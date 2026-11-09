#!/usr/bin/env python3
""" Orquestador Distribuido y Agente de Micro-VMs para el Servidor Esporádico en Proxmox VE 9.1-1.

Versión: 1.0.0
Licencia: MIT
Maintainer: Ray <elnemesisdivina@gmail.com>

Implementa la arquitectura distribuida de 5 Stripped KVM Micro-VMs sobre Proxmox VE:
  - 4 Micro-VMs de Tareas Periódicas (VM 101..104: tau1..tau4, 1 vCPU, 128 MB RAM).
  - 1 Micro-VM del Servidor Esporádico y Coordinador RM (VM 105: S_s, 1 vCPU, 256 MB RAM).

Cada Micro-VM mantiene un proceso quemador de CPU real (CPUBurnerProcess) gobernado por
señales POSIX SIGCONT (100.0% vCPU en las gráficas de Proxmox) y SIGSTOP (0.0% vCPU).
Al concluir el horizonte de simulación, el coordinador en la VM 105 construye un objeto
SimulationResult idéntico al de sporadic_server_sim.py para verificar RTA y exportar el
diagrama de Gantt (PNG), el dashboard interactivo HTML5, CSV y JSON.
"""

from __future__ import annotations

import argparse
import collections
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
import os
import re
import signal
import socket
import sys
import threading
import time
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

VERSION = "1.0.0"
MAINTAINER = "Ray <elnemesisdivina@gmail.com>"
DEFAULT_TCP_PORT = 9000
DEFAULT_HTTP_PORT = 8080


# ==============================================================================
# 1. Proceso Quemador de CPU Real controlado por SIGSTOP / SIGCONT
# ==============================================================================


def _cpu_burner_loop() -> None:
  """Bucle aritmético intensivo ejecutado en un subproceso aislado.

  Consume el 100% de 1 núcleo vCPU mientras se encuentra en estado ejecutando
  (tras recibir SIGCONT) y cae instantáneamente a 0.0% de CPU cuando el proceso
  padre le envía SIGSTOP.
  """
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  val = 1.000001
  while True:
    # Bucle desrolado en coma flotante para mantener la ALU al 100% sin syscalls
    for _ in range(10000):
      val = (val * 1.0000001 + 0.0000003) % 100000.0


class CPUBurnerProcess:
  """Gestiona un subproceso dedicado de carga de CPU mediante SIGSTOP y SIGCONT.

  Atributos:
    label: Etiqueta identificadora del nodo (ej. 'tau1' o 'S_s').
    enabled: Si es True, lanza el subproceso real del sistema operativo.
  """

  def __init__(self, label: str = "worker", enabled: bool = True) -> None:
    self.label = label
    self.enabled = enabled
    self._proc: Optional[multiprocessing.Process] = None
    self._burning = False
    self._lock = threading.Lock()
    if self.enabled:
      self._spawn_and_pause()

  def _spawn_and_pause(self) -> None:
    """Inicia el subproceso quemador e inmediatamente lo suspende con SIGSTOP."""
    ctx = multiprocessing.get_context("spawn" if sys.platform == "darwin" else "fork")
    self._proc = ctx.Process(
        target=_cpu_burner_loop,
        name=f"cpu-burner-{self.label}",
        daemon=True,
    )
    self._proc.start()
    if self._proc.pid is not None:
      os.kill(self._proc.pid, signal.SIGSTOP)
    self._burning = False

  @property
  def pid(self) -> Optional[int]:
    """Devuelve el PID del subproceso quemador de CPU."""
    return self._proc.pid if self._proc is not None else None

  @property
  def is_burning(self) -> bool:
    """Indica si el proceso quemador se encuentra activo al 100% de CPU."""
    return self._burning

  def start_burning(self) -> None:
    """Reanuda el subproceso con SIGCONT -> 100.0% vCPU en Proxmox."""
    with self._lock:
      if not self.enabled or self._proc is None or self._proc.pid is None:
        self._burning = True
        return
      if not self._burning and self._proc.is_alive():
        os.kill(self._proc.pid, signal.SIGCONT)
        self._burning = True

  def stop_burning(self) -> None:
    """Suspende el subproceso con SIGSTOP -> 0.0% vCPU en Proxmox."""
    with self._lock:
      if not self.enabled or self._proc is None or self._proc.pid is None:
        self._burning = False
        return
      if self._burning and self._proc.is_alive():
        os.kill(self._proc.pid, signal.SIGSTOP)
        self._burning = False

  def close(self) -> None:
    """Finaliza limpiamente el subproceso quemador sin dejar procesos zombis."""
    with self._lock:
      if self._proc is not None and self._proc.pid is not None:
        try:
          if self._proc.is_alive():
            os.kill(self._proc.pid, signal.SIGCONT)
            self._proc.terminate()
            self._proc.join(timeout=1.0)
            if self._proc.is_alive():
              self._proc.kill()
              self._proc.join(timeout=0.5)
        except ProcessLookupError:
          pass
      self._burning = False
      self._proc = None


# ==============================================================================
# 2. Utilidades de Normalización de Identificadores y Protocolo TCP NDJSON
# ==============================================================================


def normalize_task_id(raw_name: str) -> str:
  """Convierte nombres LaTeX como '$\\tau_1$' a identificadores CLI como 'tau1'.

  Args:
    raw_name: Nombre de la tarea (ej. '$\\tau_1$', 'tau1', '$S_s$').

  Returns:
    Identificador normalizado en minúsculas (ej. 'tau1', 'ss').
  """
  cleaned = raw_name.replace("$", "").replace("\\", "").replace("_", "").strip().lower()
  return cleaned


def send_ndjson(sock: socket.socket, payload: dict[str, Any]) -> None:
  """Envía un mensaje JSON terminado en salto de línea sobre un socket TCP."""
  raw = (json.dumps(payload) + "\n").encode("utf-8")
  sock.sendall(raw)


# ==============================================================================
# 3. Agente de Micro-VM de Tarea Periódica (--role worker)
# ==============================================================================


class PeriodicWorkerNode:
  """Agente ejecutado en las Micro-VMs 101..104 (tau1..tau4).

  Se conecta por TCP (con TCP_NODELAY) al Coordinador RM en la VM 105 y activa
  o pausa su CPUBurnerProcess inmediatamente al recibir GRANT_CPU o PREEMPT_CPU.
  """

  def __init__(
      self,
      task_id: str,
      server_host: str,
      server_port: int = DEFAULT_TCP_PORT,
      enable_burner: bool = True,
      connect_timeout: float = 15.0,
  ) -> None:
    self.task_id = normalize_task_id(task_id)
    self.server_host = server_host
    self.server_port = server_port
    self.connect_timeout = connect_timeout
    self.burner = CPUBurnerProcess(label=self.task_id, enabled=enable_burner)
    self.grants_received = 0
    self.preemptions_received = 0
    self._stop_event = threading.Event()

  def run(self) -> None:
    """Conecta al coordinador, registra el nodo y procesa comandos hasta SHUTDOWN."""
    deadline = time.monotonic() + self.connect_timeout
    sock: Optional[socket.socket] = None

    while time.monotonic() < deadline and not self._stop_event.is_set():
      try:
        candidate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        candidate.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        candidate.settimeout(2.0)
        candidate.connect((self.server_host, self.server_port))
        candidate.settimeout(None)
        sock = candidate
        break
      except OSError:
        time.sleep(0.05)

    if sock is None:
      self.burner.close()
      raise ConnectionError(
          f"No fue posible conectar el worker '{self.task_id}' a "
          f"{self.server_host}:{self.server_port}"
      )

    try:
      send_ndjson(
          sock,
          {
              "type": "REGISTER",
              "task_id": self.task_id,
              "hostname": socket.gethostname(),
              "burner_pid": self.burner.pid,
          },
      )
      reader = sock.makefile("r", encoding="utf-8")
      for line in reader:
        if not line.strip():
          continue
        msg = json.loads(line)
        mtype = msg.get("type")
        if mtype == "GRANT_CPU":
          self.burner.start_burning()
          self.grants_received += 1
          send_ndjson(sock, {"type": "ACK", "task_id": self.task_id, "state": "BURNING"})
        elif mtype == "PREEMPT_CPU":
          self.burner.stop_burning()
          self.preemptions_received += 1
          send_ndjson(sock, {"type": "ACK", "task_id": self.task_id, "state": "STOPPED"})
        elif mtype == "SHUTDOWN":
          self.burner.stop_burning()
          send_ndjson(sock, {"type": "ACK", "task_id": self.task_id, "state": "SHUTDOWN"})
          break
    finally:
      self.burner.close()
      try:
        sock.close()
      except OSError:
        pass


# ==============================================================================
# 4. Coordinador RM + Servidor Esporádico en la Micro-VM 105 (--role server)
# ==============================================================================


def _build_live_portal_html() -> str:
  """Construye el Portal Web en Vivo servido por la Micro-VM 105 en GET / (:8080)."""
  return """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <title>Proxmox VE 9.1-1 — Monitor en Vivo del Servidor Esporádico (VM 105)</title>
  <style>
    :root { --bg: #0f172a; --card: #1e293b; --text: #f8fafc; --muted: #94a3b8; --accent: #14b8a6; --warn: #ef4444; --blue: #38bdf8; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); padding: 1.5rem; }
    .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 1rem; margin-bottom: 1.25rem; flex-wrap: wrap; gap: 1rem; }
    .badge { background: #0f766e; color: #ccfbf1; padding: 0.35rem 0.75rem; border-radius: 999px; font-size: 0.85rem; font-weight: 600; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(290px, 1fr)); gap: 1rem; margin-bottom: 1.25rem; }
    .card { background: var(--card); border: 1px solid #334155; border-radius: 0.75rem; padding: 1.15rem; }
    .card h3 { margin: 0 0 0.75rem 0; font-size: 0.95rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
    .big-val { font-size: 1.65rem; font-weight: 700; margin-bottom: 0.35rem; }
    .bar-wrap { width: 100%; height: 14px; background: #0f172a; border-radius: 7px; overflow: hidden; border: 1px solid #334155; }
    .bar-fill { height: 100%; background: linear-gradient(90deg, #14b8a6, #38bdf8); transition: width 0.25s ease; }
    .vm-list { display: grid; grid-template-columns: repeat(5, 1fr); gap: 0.65rem; margin-bottom: 1.25rem; }
    .vm-box { background: var(--card); border: 2px solid #334155; border-radius: 0.65rem; padding: 0.85rem; text-align: center; transition: all 0.2s; }
    .vm-box.active { border-color: #14b8a6; background: #134e4a; box-shadow: 0 0 16px rgba(20, 184, 166, 0.35); }
    .vm-title { font-weight: 700; font-size: 0.95rem; }
    .vm-sub { font-size: 0.8rem; color: var(--muted); margin: 0.25rem 0 0.5rem 0; }
    .cpu-pill { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.8rem; font-weight: 700; background: #0f172a; color: #64748b; }
    .vm-box.active .cpu-pill { background: #10b981; color: #052e16; }
    .btn { background: var(--accent); color: #042f2e; border: none; padding: 0.6rem 1.1rem; border-radius: 0.5rem; font-weight: 700; cursor: pointer; text-decoration: none; display: inline-block; }
    .btn-blue { background: var(--blue); color: #082f49; }
    input { background: #0f172a; border: 1px solid #475569; color: var(--text); padding: 0.5rem 0.75rem; border-radius: 0.4rem; margin-right: 0.5rem; }
    #artifacts-panel { display: none; margin-top: 1.25rem; }
    @media (max-width: 900px) { .vm-list { grid-template-columns: 1fr 1fr; } }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1 style="margin:0;font-size:1.4rem;">🚀 Clúster Proxmox VE 9.1-1 — Servidor Esporádico (VM 105)</h1>
      <div style="color:var(--muted);font-size:0.9rem;margin-top:0.25rem;">Maintainer: Ray &lt;elnemesisdivina@gmail.com&gt; | 5 Stripped KVM Micro-VMs (Cores #1..#5)</div>
    </div>
    <div>
      <span id="sim-badge" class="badge">⏳ Conectando con VM 105...</span>
      <a href="/dashboard" class="btn btn-blue" style="margin-left:0.5rem;">📊 Abrir Dashboard Interactivo HTML5 (/dashboard)</a>
    </div>
  </div>

  <div class="vm-list">
    <div id="vm-tau1" class="vm-box"><div class="vm-title">VM 101 (vm1-tau1)</div><div class="vm-sub">τ₁ | P₁=5 (Máx) | Core #1</div><span class="cpu-pill">0% CPU</span></div>
    <div id="vm-tau2" class="vm-box"><div class="vm-title">VM 102 (vm2-tau2)</div><div class="vm-sub">τ₂ | P₂=4 (Alta) | Core #2</div><span class="cpu-pill">0% CPU</span></div>
    <div id="vm-ss"   class="vm-box"><div class="vm-title">VM 105 (vm5-ss)</div><div class="vm-sub">Sₛ | Pₛ=3 (Server) | Core #5</div><span class="cpu-pill">0% CPU</span></div>
    <div id="vm-tau3" class="vm-box"><div class="vm-title">VM 103 (vm3-tau3)</div><div class="vm-sub">τ₃ | P₃=2 (Media) | Core #3</div><span class="cpu-pill">0% CPU</span></div>
    <div id="vm-tau4" class="vm-box"><div class="vm-title">VM 104 (vm4-tau4)</div><div class="vm-sub">τ₄ | P₄=1 (Mín) | Core #4</div><span class="cpu-pill">0% CPU</span></div>
  </div>

  <div class="grid">
    <div class="card">
      <h3>⏱️ Reloj Lógico y Micro-VM en CPU</h3>
      <div id="val-time" class="big-val">t = 0.00 / 30.0 s</div>
      <div class="bar-wrap"><div id="bar-time" class="bar-fill" style="width:0%"></div></div>
      <div id="val-vm" style="margin-top:0.6rem;color:var(--blue);font-weight:600;">Activa: Idle</div>
    </div>
    <div class="card">
      <h3>🔋 Presupuesto del Servidor Esporádico Cₛ(t)</h3>
      <div id="val-budget" class="big-val">2.00 / 2.00 u</div>
      <div class="bar-wrap"><div id="bar-budget" class="bar-fill" style="width:100%"></div></div>
      <div id="val-repl" style="margin-top:0.6rem;color:var(--muted);font-size:0.9rem;">Ventana t_A: — | Próx. Recarga t_R: —</div>
    </div>
    <div class="card">
      <h3>📥 Cola Aperiódica FIFO y Plazos</h3>
      <div id="val-queue" class="big-val">Cola: 0 trabajos | Misses: 0</div>
      <form id="job-form" style="margin-top:0.6rem;" onsubmit="return sendJob(event);">
        <input type="text" id="jname" placeholder="Nombre (ej. J_live)" value="J_urgente" style="width:120px;" required />
        <input type="number" id="jwork" step="0.1" min="0.1" max="10.0" value="1.5" style="width:80px;" required />
        <button type="submit" class="btn">⚡ Inyectar Jₖ</button>
      </form>
      <div id="job-msg" style="font-size:0.82rem;color:#34d399;margin-top:0.35rem;"></div>
    </div>
  </div>

  <div id="artifacts-panel" class="card">
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:1rem;margin-bottom:0.5rem;">
      <h3 style="margin:0;color:#34d399;">✅ Simulación Completada — Dashboard Interactivo HTML5 Generado en VM 105</h3>
      <div>
        <a href="/dashboard" class="btn btn-blue">🔍 Ver Dashboard Interactivo HTML5 Completo (/dashboard)</a>
        <a id="png-dl-btn" href="/gantt.png" download="sporadic_server_simulation.png" class="btn" style="display:none;margin-left:0.5rem;">⬇️ Guardar PNG</a>
      </div>
    </div>
    <img id="gantt-img" src="" alt="Diagrama de Gantt y Presupuesto Cs(t)" style="display:none;width:100%;margin-top:0.75rem;border-radius:0.5rem;border:1px solid #334155;" />
  </div>

  <script>
    let artifactsLoaded = false;
    let liveJobSeq = 1;
    async function sendJob(ev) {
      ev.preventDefault();
      const inputEl = document.getElementById('jname');
      const name = inputEl.value || `J_urgente_${liveJobSeq}`;
      const work = document.getElementById('jwork').value || '1.0';
      const r = await fetch(`/job?name=${encodeURIComponent(name)}&work=${encodeURIComponent(work)}`);
      const d = await r.json();
      if (d.status === 'enqueued') {
        liveJobSeq += 1;
        inputEl.value = `J_urgente_${liveJobSeq}`;
        document.getElementById('job-msg').textContent = `✔ Trabajo ${d.name} (C=${d.compute_time}) encolado en VM 105`;
      } else {
        document.getElementById('job-msg').textContent = `❌ Error: ${d.error}`;
      }
      return false;
    }
    function highlightVM(runner, job) {
      const map = {'$\\tau_1$':'vm-tau1','tau1':'vm-tau1','$\\tau_2$':'vm-tau2','tau2':'vm-tau2','$\\tau_3$':'vm-tau3','tau3':'vm-tau3','$\\tau_4$':'vm-tau4','tau4':'vm-tau4','$S_s$':'vm-ss','S_s':'vm-ss'};
      ['vm-tau1','vm-tau2','vm-ss','vm-tau3','vm-tau4'].forEach(id => {
        const el = document.getElementById(id);
        el.classList.remove('active');
        el.querySelector('.cpu-pill').textContent = '0% CPU (SIGSTOP)';
      });
      const activeId = map[runner];
      if (activeId) {
        const el = document.getElementById(activeId);
        el.classList.add('active');
        el.querySelector('.cpu-pill').textContent = job ? `100% CPU (${job})` : '100% CPU (SIGCONT)';
      }
    }
    async function pollStatus() {
      try {
        const r = await fetch('/status');
        const s = await r.json();
        const pctTime = Math.min(100, (s.logical_time / (s.horizon || 30.0)) * 100);
        document.getElementById('val-time').textContent = `t = ${s.logical_time.toFixed(1)} / ${(s.horizon || 30.0).toFixed(1)} s`;
        document.getElementById('bar-time').style.width = `${pctTime}%`;
        document.getElementById('val-vm').textContent = `Activa: ${s.active_vm || s.active_runner}`;
        const pctBudget = Math.min(100, (s.budget / (s.max_budget || 2.0)) * 100);
        document.getElementById('val-budget').textContent = `${s.budget.toFixed(2)} / ${(s.max_budget || 2.0).toFixed(2)} u`;
        document.getElementById('bar-budget').style.width = `${pctBudget}%`;
        document.getElementById('val-repl').textContent = `Ventana t_A: ${s.t_a !== null ? s.t_a + 's' : 'Cerrada'} | Próx. Recarga t_R: ${s.next_replenishment !== null ? s.next_replenishment + 's' : '—'}`;
        document.getElementById('val-queue').textContent = `Cola: ${s.queue_len} | Misses: ${s.deadline_misses_count}`;
        document.getElementById('val-queue').style.color = s.deadline_misses_count > 0 ? '#ef4444' : '#f8fafc';
        highlightVM(s.active_runner, s.active_job);
        const badge = document.getElementById('sim-badge');
        if (s.simulation_finished) {
          badge.textContent = '✅ Simulación Concluida en VM 105';
          badge.style.background = '#065f46';
        } else {
          badge.textContent = '🟢 Ejecutando en Vivo en Clúster Proxmox';
        }
        if (s.artifacts_ready && !artifactsLoaded) {
          artifactsLoaded = true;
          document.getElementById('artifacts-panel').style.display = 'block';
          if (s.png_ready) {
            document.getElementById('png-dl-btn').style.display = 'inline-block';
            const img = document.getElementById('gantt-img');
            img.style.display = 'block';
            img.src = '/gantt.png?t=' + Date.now();
          }
        }
      } catch (e) {}
    }
    setInterval(pollStatus, 400);
    pollStatus();
  </script>
</body>
</html>"""


class SporadicServerCoordinator:
  """Coordinador RM y Servidor Esporádico (S_s) ejecutado en la Micro-VM 105.

  Coordina las 4 Micro-VMs periódicas mediante sockets TCP_NODELAY, ejecuta su
  propio CPUBurnerProcess local cuando S_s atiende peticiones aperiódicas J_k,
  expone el servidor HTTP en el puerto 8080 para inyección de trabajos en vivo y
  construye el objeto SimulationResult compatible con sporadic_server_sim.py.
  """

  def __init__(
      self,
      periodic_tasks: list[Any],
      server_config: Any,
      aperiodic_jobs: list[Any],
      horizon: float = 30.0,
      dt: float = 0.1,
      time_scale: float = 1.0,
      mode: str = "queue_aware",
      bind_host: str = "0.0.0.0",
      tcp_port: int = DEFAULT_TCP_PORT,
      http_port: int = DEFAULT_HTTP_PORT,
      enable_local_burner: bool = True,
  ) -> None:
    self.periodic_tasks = periodic_tasks
    self.server_config = server_config
    self.aperiodic_jobs = list(aperiodic_jobs)
    self.horizon = horizon
    self.dt = dt
    self.time_scale = max(0.0, time_scale)
    self.mode = mode
    self.bind_host = bind_host
    self.tcp_port = tcp_port
    self.http_port = http_port

    self.server_burner = CPUBurnerProcess(
        label="sporadic-server",
        enabled=enable_local_burner,
    )

    # Mapa de identificador normalizado ('tau1') -> objeto PeriodicTask
    self.task_map: dict[str, Any] = {
        normalize_task_id(t.name): t for t in self.periodic_tasks
    }
    self.name_to_id: dict[str, str] = {
        t.name: normalize_task_id(t.name) for t in self.periodic_tasks
    }

    # Conexiones TCP activas hacia las Micro-VMs trabajadoras: task_id -> (sock, reader)
    self._workers: dict[str, tuple[socket.socket, Any]] = {}
    self._workers_lock = threading.Lock()
    self._tcp_server: Optional[socket.socket] = None

    # Cola thread-safe de trabajos aperiódicos inyectados en vivo por HTTP
    self._live_jobs_lock = threading.Lock()
    self._live_injected_jobs: list[tuple[str, float]] = []

    # Estado en vivo consultable vía HTTP GET /status y Portal Web (GET /)
    self._status_lock = threading.Lock()
    self._live_status: dict[str, Any] = {
        "logical_time": 0.0,
        "horizon": self.horizon,
        "active_runner": "Idle",
        "active_vm": "Idle (0% CPU)",
        "active_job": "",
        "budget": self.server_config.budget,
        "max_budget": self.server_config.budget,
        "is_active": False,
        "t_a": None,
        "next_replenishment": None,
        "queue_len": 0,
        "deadline_misses_count": 0,
        "registered_workers": [],
        "simulation_finished": False,
        "artifacts_ready": False,
    }

    self._dashboard_html_path: Optional[str] = None
    self._gantt_png_path: Optional[str] = None
    self._http_server: Optional[ThreadingHTTPServer] = None
    self._http_thread: Optional[threading.Thread] = None

  def register_artifacts(
      self,
      html_path: Optional[str] = None,
      png_path: Optional[str] = None,
  ) -> None:
    """Registra las rutas del Dashboard HTML5 y PNG generados para servirlos en :8080."""
    self._dashboard_html_path = html_path
    self._gantt_png_path = png_path
    has_png = bool(png_path and os.path.isfile(png_path))
    has_html = bool(html_path and os.path.isfile(html_path))
    with self._status_lock:
      self._live_status["png_ready"] = has_png
      self._live_status["artifacts_ready"] = has_html or has_png

  def _runner_to_vm_label(self, runner_name: str, job_label: str = "") -> str:
    """Traduce el nombre de tarea al identificador de Micro-VM en Proxmox."""
    if runner_name == "Idle":
      return "Idle (Todas las VMs al 0% CPU)"
    if runner_name == self.server_config.name:
      suffix = f" -> {job_label}" if job_label else ""
      return f"VM 105 (vm5-ss{suffix}) [100% CPU]"
    tid = self.name_to_id.get(runner_name, "")
    vm_num = {"tau1": 101, "tau2": 102, "tau3": 103, "tau4": 104}.get(tid, 100)
    idx = vm_num - 100
    return f"VM {vm_num} (vm{idx}-{tid}) [100% CPU]"

  # ── Servidor HTTP de Portal en Vivo, Dashboard y API (:8080) ───────────────

  def start_http_server(self) -> int:
    """Inicia el servidor HTTP en segundo plano para /, /dashboard, /gantt.png, /job y /status.

    Returns:
      Puerto TCP real en el que escucha el servidor HTTP.
    """
    coord = self

    class _Handler(BaseHTTPRequestHandler):
      def log_message(self, format: str, *args: Any) -> None:  # pylint: disable=redefined-builtin
        pass  # Silenciar logs por petición para no ensuciar la barra de estado CLI

      def _send_json(self, code: int, data: dict[str, Any]) -> None:
        body = (json.dumps(data) + "\n").encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

      def _send_html(self, code: int, html_text: str) -> None:
        body = html_text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

      def do_GET(self) -> None:  # pylint: disable=invalid-name
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
          self._send_html(200, _build_live_portal_html())
          return

        if parsed.path == "/dashboard":
          if coord._dashboard_html_path and os.path.isfile(coord._dashboard_html_path):
            with open(coord._dashboard_html_path, "r", encoding="utf-8") as f:
              self._send_html(200, f.read())
          else:
            self._send_html(
                202,
                "<html><body style='background:#0f172a;color:#f8fafc;font-family:sans-serif;padding:2rem;'>"
                "<h2>⏳ Simulación en curso en el Clúster Proxmox...</h2>"
                "<p>El Dashboard interactivo HTML5 completo estará disponible automáticamente al concluir el horizonte. "
                "<a href='/' style='color:#38bdf8;'>Volver al Monitor en Vivo</a></p>"
                "<script>setTimeout(()=>location.reload(), 2000);</script></body></html>",
            )
          return

        if parsed.path == "/gantt.png":
          if coord._gantt_png_path and os.path.isfile(coord._gantt_png_path):
            with open(coord._gantt_png_path, "rb") as f:
              body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
          else:
            self._send_json(404, {"error": "Gantt PNG aún no generado."})
          return

        if parsed.path == "/status":
          with coord._status_lock:
            snap = dict(coord._live_status)
          self._send_json(200, snap)
          return

        if parsed.path == "/job":
          qs = parse_qs(parsed.query)
          name = qs.get("name", [f"J_live_{int(time.time() * 1000) % 10000}"])[0]
          work_str = qs.get("work", qs.get("compute_time", ["1.0"]))[0]
          try:
            work = float(work_str)
            if work <= 0:
              raise ValueError("compute_time must be > 0")
          except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
            return

          coord.inject_live_job(name=name, compute_time=work)
          self._send_json(
              200,
              {
                  "status": "enqueued",
                  "name": name,
                  "compute_time": work,
              },
          )
          return

        self._send_json(
            404,
            {"error": "Ruta no encontrada. Usar /, /dashboard, /gantt.png, /job o /status"},
        )

      def do_POST(self) -> None:  # pylint: disable=invalid-name
        parsed = urlparse(self.path)
        if parsed.path != "/job":
          self._send_json(404, {"error": "Ruta no encontrada. Usar POST /job"})
          return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
          payload = json.loads(raw)
          name = str(payload.get("name", f"J_live_{int(time.time() * 1000) % 10000}"))
          work = float(payload.get("work", payload.get("compute_time", 1.0)))
          if work <= 0:
            raise ValueError("compute_time must be > 0")
        except (ValueError, json.JSONDecodeError) as exc:
          self._send_json(400, {"error": str(exc)})
          return

        coord.inject_live_job(name=name, compute_time=work)
        self._send_json(
            200,
            {
                "status": "enqueued",
                "name": name,
                "compute_time": work,
            },
        )

    self._http_server = ThreadingHTTPServer((self.bind_host, self.http_port), _Handler)
    self.http_port = int(self._http_server.server_address[1])
    self._http_thread = threading.Thread(
        target=self._http_server.serve_forever,
        name="ss-http-injector",
        daemon=True,
    )
    self._http_thread.start()
    return self.http_port

  def stop_http_server(self) -> None:
    """Detiene limpiamente el servidor HTTP de inyección en vivo."""
    if self._http_server is not None:
      self._http_server.shutdown()
      self._http_server.server_close()
      self._http_server = None

  def inject_live_job(self, name: str, compute_time: float) -> None:
    """Encola un trabajo aperiódico de forma segura entre hilos para el próximo tick."""
    with self._live_jobs_lock:
      self._live_injected_jobs.append((name, float(compute_time)))

  # ── Servidor TCP para Registro y Control de las 4 Micro-VMs (Puerto 9000) ──

  def open_tcp_listener(self) -> int:
    """Abre el socket servidor TCP con SO_REUSEADDR y TCP_NODELAY."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((self.bind_host, self.tcp_port))
    srv.listen(16)
    self._tcp_server = srv
    self.tcp_port = int(srv.getsockname()[1])
    return self.tcp_port

  def wait_for_workers(self, timeout: float = 30.0) -> None:
    """Espera a que las Micro-VMs periódicas definidas en el JSON se registren."""
    if self._tcp_server is None:
      self.open_tcp_listener()
    assert self._tcp_server is not None

    expected_ids = set(self.task_map.keys())
    deadline = time.monotonic() + timeout

    while len(self._workers) < len(expected_ids):
      remaining = deadline - time.monotonic()
      if remaining <= 0:
        missing = sorted(expected_ids - set(self._workers.keys()))
        raise TimeoutError(
            f"Tiempo agotado esperando registro TCP de las Micro-VMs: {missing}"
        )
      self._tcp_server.settimeout(min(0.5, remaining))
      try:
        conn, _ = self._tcp_server.accept()
      except socket.timeout:
        continue

      conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
      reader = conn.makefile("r", encoding="utf-8")
      line = reader.readline()
      if not line:
        conn.close()
        continue
      msg = json.loads(line)
      if msg.get("type") == "REGISTER":
        tid = normalize_task_id(str(msg.get("task_id", "")))
        if tid in expected_ids:
          with self._workers_lock:
            self._workers[tid] = (conn, reader)
          with self._status_lock:
            self._live_status["registered_workers"] = sorted(self._workers.keys())

  def _send_worker_cmd(self, task_id: str, cmd_type: str, logical_time: float) -> None:
    """Envía GRANT_CPU, PREEMPT_CPU o SHUTDOWN a una Micro-VM trabajadora y espera ACK."""
    with self._workers_lock:
      entry = self._workers.get(task_id)
    if entry is None:
      return
    conn, reader = entry
    try:
      send_ndjson(conn, {"type": cmd_type, "logical_time": logical_time})
      _ = reader.readline()
    except OSError:
      pass

  def _dispatch_cpu_token(
      self,
      prev_runner: Optional[str],
      new_runner: str,
      logical_time: float,
  ) -> None:
    """Conmuta el token de ejecución física entre las 5 Micro-VMs."""
    if prev_runner == new_runner:
      return

    # 1. Desalojar al ejecutor previo (0.0% CPU)
    if prev_runner is not None and prev_runner != "Idle":
      if prev_runner == self.server_config.name:
        self.server_burner.stop_burning()
      elif prev_runner in self.name_to_id:
        self._send_worker_cmd(self.name_to_id[prev_runner], "PREEMPT_CPU", logical_time)

    # 2. Otorgar CPU al nuevo ejecutor (100.0% CPU)
    if new_runner == self.server_config.name:
      self.server_burner.start_burning()
    elif new_runner in self.name_to_id:
      self._send_worker_cmd(self.name_to_id[new_runner], "GRANT_CPU", logical_time)

  def close_workers_and_burner(self) -> None:
    """Cierra el quemador local y envía SHUTDOWN a las 4 Micro-VMs sin apagar el HTTP :8080."""
    self.server_burner.close()
    with self._workers_lock:
      worker_ids = list(self._workers.keys())
    for tid in worker_ids:
      self._send_worker_cmd(tid, "SHUTDOWN", self.horizon)
    with self._workers_lock:
      for conn, _ in self._workers.values():
        try:
          conn.close()
        except OSError:
          pass
      self._workers.clear()
    if self._tcp_server is not None:
      try:
        self._tcp_server.close()
      except OSError:
        pass
      self._tcp_server = None

  def close_all(self) -> None:
    """Cierra el quemador local, el servidor HTTP y envía SHUTDOWN a las 4 Micro-VMs."""
    self.close_workers_and_burner()
    self.stop_http_server()

  # ── Motor Principal de Simulación Distribuida (Sprunt-Sha-Lehoczky / Buttazzo)

  def run(self) -> Any:
    """Ejecuta la planificación RM + Servidor Esporádico sobre las 5 Micro-VMs.

    Returns:
      Objeto SimulationResult compatible con todas las funciones de reporte y
      graficación de sporadic_server_sim.py.
    """
    import sporadic_server_sim as ssim  # pylint: disable=g-import-not-at-top

    total_ticks = ssim.to_ticks(self.horizon, self.dt)
    max_budget_ticks = ssim.to_ticks(self.server_config.budget, self.dt)
    server_period_ticks = ssim.to_ticks(self.server_config.period, self.dt)

    budget_ticks = max_budget_ticks
    pending_replenishments: dict[int, int] = collections.defaultdict(int)

    aperiodic_queue: collections.deque[ssim._AperiodicJobInstance] = collections.deque()
    periodic_instances: list[ssim._PeriodicJobInstance] = []

    is_active = False
    t_a_tick: Optional[int] = None
    consumed_ticks = 0

    activations: list[ssim.ActivationEvent] = []
    replenishments: list[ssim.ReplenishmentEvent] = []
    raw_slices: list[tuple[str, str]] = []
    budget_times: list[float] = [0.0]
    budget_values: list[float] = [self.server_config.budget]

    periodic_releases: list[tuple[float, str]] = []
    periodic_deadlines: list[tuple[float, str]] = []
    aperiodic_arrivals: list[tuple[float, str]] = []
    deadline_misses: list[tuple[float, str]] = []
    scheduled_aperiodic_jobs = list(self.aperiodic_jobs)
    job_records: dict[str, ssim.JobRecord] = {
        j.name: ssim.JobRecord(job=j, remaining_time=j.compute_time)
        for j in scheduled_aperiodic_jobs
    }

    current_holder: Optional[str] = None
    tick_wall_duration = self.dt * self.time_scale

    try:
      for k in range(total_ticks):
        tick_start_wall = time.monotonic()
        current_time = k * self.dt

        # ── PASO 1: APLICAR RECARGAS PROGRAMADAS DE PRESUPUESTO (t_R = t_A + T_s)
        if k in pending_replenishments:
          amt = pending_replenishments[k]
          budget_times.append(current_time)
          budget_values.append(budget_ticks * self.dt)
          budget_ticks = min(max_budget_ticks, budget_ticks + amt)
          budget_times.append(current_time)
          budget_values.append(budget_ticks * self.dt)

        # ── PASO 2: VERIFICAR PLAZOS (DEADLINE MISSES) Y LIBERAR PERIÓDICAS ──
        for inst in periodic_instances:
          if inst.remaining_ticks > 0 and k >= inst.deadline_tick:
            deadline_misses.append((current_time, inst.task.name))

        for ptask in self.periodic_tasks:
          period_ticks = ssim.to_ticks(ptask.period, self.dt)
          deadline_ticks = ssim.to_ticks(ptask.deadline, self.dt)
          if k % period_ticks == 0:
            c_ticks = ssim.to_ticks(ptask.compute_time, self.dt)
            periodic_instances.append(
                ssim._PeriodicJobInstance(
                    task=ptask,
                    remaining_ticks=c_ticks,
                    deadline_tick=k + deadline_ticks,
                )
            )
            periodic_releases.append((current_time, ptask.name))
            if k + deadline_ticks <= total_ticks:
              periodic_deadlines.append(
                  ((k + deadline_ticks) * self.dt, ptask.name)
              )

        # ── PASO 3: ENCOLAR PETICIONES APERIÓDICAS (JSON + HTTP EN VIVO) ────
        for ajob in scheduled_aperiodic_jobs:
          if ssim.to_ticks(ajob.arrival_time, self.dt) == k:
            aperiodic_queue.append(
                ssim._AperiodicJobInstance(
                    job=ajob,
                    remaining_ticks=ssim.to_ticks(ajob.compute_time, self.dt),
                )
            )
            aperiodic_arrivals.append((current_time, ajob.name))

        with self._live_jobs_lock:
          if self._live_injected_jobs:
            for raw_name, live_work in self._live_injected_jobs:
              live_name = raw_name
              if live_name in job_records:
                seq = 2
                while f"{raw_name}_{seq}" in job_records:
                  seq += 1
                live_name = f"{raw_name}_{seq}"
              live_job = ssim.AperiodicJob(
                  name=live_name,
                  arrival_time=round(current_time, 4),
                  compute_time=live_work,
              )
              self.aperiodic_jobs.append(live_job)
              job_records[live_name] = ssim.JobRecord(
                  job=live_job,
                  remaining_time=live_work,
              )
              aperiodic_queue.append(
                  ssim._AperiodicJobInstance(
                      job=live_job,
                      remaining_ticks=max(1, ssim.to_ticks(live_work, self.dt)),
                  )
              )
              aperiodic_arrivals.append((current_time, live_name))
            self._live_injected_jobs.clear()

        periodic_instances = [
            inst for inst in periodic_instances if inst.remaining_ticks > 0
        ]

        # ── PASO 4: SELECCIÓN DEL CANDIDATO POR TASA MONÓTONA (RM) ───────────
        ready_periodic = sorted(
            periodic_instances,
            key=lambda x: x.task.priority,
            reverse=True,
        )
        highest_periodic = ready_periodic[0] if ready_periodic else None
        server_ready = ssim.is_server_ready(len(aperiodic_queue), budget_ticks)

        runner_name = "Idle"
        job_label = ""
        active_priority = 0

        if highest_periodic is not None and server_ready:
          if highest_periodic.task.priority >= self.server_config.priority:
            runner_name = highest_periodic.task.name
            active_priority = highest_periodic.task.priority
          else:
            runner_name = self.server_config.name
            job_label = aperiodic_queue[0].job.name
            active_priority = self.server_config.priority
        elif highest_periodic is not None:
          runner_name = highest_periodic.task.name
          active_priority = highest_periodic.task.priority
        elif server_ready:
          runner_name = self.server_config.name
          job_label = aperiodic_queue[0].job.name
          active_priority = self.server_config.priority

        # ── PASO 5: GESTIÓN DE LA VENTANA DE ACTIVACIÓN (t_A) ────────────────
        if self.mode == "queue_aware":
          if (
              not is_active
              and runner_name == self.server_config.name
              and budget_ticks > 0
          ):
            is_active = True
            t_a_tick = k
            consumed_ticks = 0
        else:
          if (
              not is_active
              and active_priority >= self.server_config.priority
              and budget_ticks > 0
          ):
            is_active = True
            t_a_tick = k
            consumed_ticks = 0

        # ── PASO 6: CONMUTAR EL TOKEN DE CPU ENTRE LAS 5 MICRO-VMS ───────────
        self._dispatch_cpu_token(current_holder, runner_name, current_time)
        current_holder = runner_name

        future_repls = [rt for rt in pending_replenishments if rt > k]
        next_rep_t = (
            round(min(future_repls) * self.dt, 2) if future_repls else None
        )
        vm_label = self._runner_to_vm_label(runner_name, job_label)

        with self._status_lock:
          self._live_status.update({
              "logical_time": round(current_time, 2),
              "active_runner": runner_name,
              "active_vm": vm_label,
              "active_job": job_label,
              "budget": round(budget_ticks * self.dt, 4),
              "is_active": is_active,
              "t_a": round(t_a_tick * self.dt, 2) if t_a_tick is not None else None,
              "next_replenishment": next_rep_t,
              "queue_len": len(aperiodic_queue),
              "deadline_misses_count": len(deadline_misses),
          })

        # Telemetría en vivo tick a tick en la consola de Proxmox (>_ Shell)
        if self.time_scale > 0:
          b_cur = budget_ticks * self.dt
          b_max = self.server_config.budget
          filled = int(round((b_cur / b_max) * 8)) if b_max > 0 else 0
          b_bar = "█" * filled + "░" * (8 - filled)
          ta_str = f"{t_a_tick * self.dt:4.1f}s" if t_a_tick is not None else "  —  "
          tr_str = f"{next_rep_t:4.1f}s" if next_rep_t is not None else "  —  "
          print(
              f"\r⚡ [t={current_time:5.1f}/{self.horizon:4.1f}s] "
              f"{vm_label:<32} | C_s:[{b_bar}] {b_cur:3.1f}/{b_max:3.1f} "
              f"| t_A:{ta_str} | t_R:{tr_str} | Cola:{len(aperiodic_queue)} "
              f"| Miss:{len(deadline_misses)}",
              end="",
              flush=True,
          )

        # Esperar la duración física del tick escalado (--time-scale K)
        if tick_wall_duration > 0:
          elapsed = time.monotonic() - tick_start_wall
          if elapsed < tick_wall_duration:
            time.sleep(tick_wall_duration - elapsed)

        # ── PASO 7: CONSUMIR 1 TICK DE CÓMPUTO Y ACTUALIZAR PRESUPUESTO C_s(t)
        next_time = (k + 1) * self.dt

        if runner_name == self.server_config.name:
          budget_times.append(current_time)
          budget_values.append(budget_ticks * self.dt)

          aperiodic_queue[0].remaining_ticks -= 1
          budget_ticks -= 1
          consumed_ticks += 1

          budget_times.append(next_time)
          budget_values.append(budget_ticks * self.dt)

          if aperiodic_queue[0].remaining_ticks == 0:
            finished_inst = aperiodic_queue.popleft()
            rec = job_records[finished_inst.job.name]
            rec.completion_time = round(next_time, 6)
            rec.remaining_time = 0.0
        elif highest_periodic is not None and runner_name == highest_periodic.task.name:
          highest_periodic.remaining_ticks -= 1
          budget_times.append(next_time)
          budget_values.append(budget_ticks * self.dt)
        else:
          budget_times.append(next_time)
          budget_values.append(budget_ticks * self.dt)

        raw_slices.append((runner_name, job_label))

        # ── PASO 8: CIERRE DE VENTANA Y PROGRAMACIÓN DE RECARGA (t_R = t_A + T_s)
        close_reason: Optional[str] = None
        if is_active:
          if budget_ticks == 0:
            close_reason = "budget_exhausted"
          elif self.mode == "queue_aware" and not aperiodic_queue:
            close_reason = "queue_empty"

        if close_reason is not None and t_a_tick is not None:
          if consumed_ticks > 0:
            rep_tick = t_a_tick + server_period_ticks
            pending_replenishments[rep_tick] += consumed_ticks
            replenishments.append(
                ssim.ReplenishmentEvent(
                    replenishment_time=round(rep_tick * self.dt, 6),
                    amount=round(consumed_ticks * self.dt, 6),
                )
            )
            activations.append(
                ssim.ActivationEvent(
                    activation_time=round(t_a_tick * self.dt, 6),
                    closed_time=round(next_time, 6),
                    amount_consumed=round(consumed_ticks * self.dt, 6),
                    reason=close_reason,
                )
            )
          is_active = False
          t_a_tick = None
          consumed_ticks = 0

    finally:
      self._dispatch_cpu_token(current_holder, "Idle", self.horizon)
      if self.time_scale > 0:
        print()  # Salto de línea limpio tras la barra de telemetría en vivo
      with self._status_lock:
        self._live_status.update({
            "logical_time": round(self.horizon, 2),
            "active_runner": "Idle",
            "active_vm": "Idle (Simulación Concluida)",
            "active_job": "",
            "simulation_finished": True,
        })

    # Consolidar rebanadas contiguas (ExecutionSlice) y totales de CPU
    slices: list[ssim.ExecutionSlice] = []
    cpu_totals: dict[str, float] = collections.defaultdict(float)
    for k, (rname, jlabel) in enumerate(raw_slices):
      t0 = round(k * self.dt, 6)
      t1 = round((k + 1) * self.dt, 6)
      cpu_totals[rname] = round(cpu_totals[rname] + self.dt, 6)
      if slices and slices[-1].runner_name == rname and slices[-1].job_label == jlabel:
        slices[-1].end_time = t1
      else:
        slices.append(
            ssim.ExecutionSlice(
                start_time=t0,
                end_time=t1,
                runner_name=rname,
                job_label=jlabel,
            )
        )

    for inst in aperiodic_queue:
      job_records[inst.job.name].remaining_time = round(
          inst.remaining_ticks * self.dt, 6
      )

    sched_report = ssim.compute_schedulability(
        periodic_tasks=self.periodic_tasks,
        server_config=self.server_config,
        job_records=job_records,
    )

    return ssim.SimulationResult(
        slices=slices,
        budget_trace_times=budget_times,
        budget_trace_values=budget_values,
        activations=activations,
        replenishments=replenishments,
        periodic_release_arrows=periodic_releases,
        periodic_deadline_arrows=periodic_deadlines,
        aperiodic_arrival_arrows=aperiodic_arrivals,
        cpu_totals=dict(cpu_totals),
        deadline_misses=deadline_misses,
        job_records=job_records,
        schedulability=sched_report,
    )


# ==============================================================================
# 5. Ejecución de Clúster Completo Local (5 Nodos sobre TCP Loopback) y CLI
# ==============================================================================


def run_five_node_cluster_simulation(
    tasks_json_path: str,
    time_scale: float = 0.0,
    enable_burners: bool = True,
) -> tuple[Any, SporadicServerCoordinator]:
  """Levanta el Coordinador (VM 105) y los 4 Workers (VM 101..104) sobre TCP real.

  Permite ejecutar o probar las 5 Micro-VMs tanto distribuidas en la red vmbr0 de
  Proxmox como localmente mediante hilos/procesos conectados por sockets TCP.

  Args:
    tasks_json_path: Ruta al archivo JSON (ej. proxmox_schedulable.json).
    time_scale: Factor de escala temporal K (0.0 para pruebas instantáneas).
    enable_burners: Si es True, lanza los subprocesos reales con SIGSTOP/SIGCONT.

  Returns:
    Tupla (SimulationResult, SporadicServerCoordinator).
  """
  import sporadic_server_sim as ssim  # pylint: disable=g-import-not-at-top

  periodic_tasks, server_config, aperiodic_jobs, horizon, dt = (
      ssim.load_tasks_from_json(tasks_json_path)
  )

  coord = SporadicServerCoordinator(
      periodic_tasks=periodic_tasks,
      server_config=server_config,
      aperiodic_jobs=aperiodic_jobs,
      horizon=horizon,
      dt=dt,
      time_scale=time_scale,
      bind_host="127.0.0.1",
      tcp_port=0,
      http_port=0,
      enable_local_burner=enable_burners,
  )
  tcp_port = coord.open_tcp_listener()
  coord.start_http_server()

  worker_threads: list[threading.Thread] = []
  for ptask in periodic_tasks:
    tid = normalize_task_id(ptask.name)
    wnode = PeriodicWorkerNode(
        task_id=tid,
        server_host="127.0.0.1",
        server_port=tcp_port,
        enable_burner=enable_burners,
    )
    t = threading.Thread(target=wnode.run, name=f"worker-{tid}", daemon=True)
    t.start()
    worker_threads.append(t)

  try:
    coord.wait_for_workers(timeout=10.0)
    result = coord.run()
  finally:
    coord.close_all()
    for t in worker_threads:
      t.join(timeout=2.0)

  return result, coord


def main() -> None:
  """Punto de entrada principal por línea de comandos para las 5 Micro-VMs."""
  parser = argparse.ArgumentParser(
      description=(
          "Simulador Distribuido del Servidor Esporádico (SS) para 5 Stripped KVM "
          "Micro-VMs en Proxmox VE 9.1-1."
      )
  )
  parser.add_argument(
      "--version",
      action="version",
      version=f"proxmox_ss_cluster {VERSION} ({MAINTAINER})",
  )
  parser.add_argument(
      "--role",
      choices=["server", "worker"],
      required=True,
      help="Rol de la Micro-VM: 'server' (VM 105) o 'worker' (VM 101..104).",
  )
  parser.add_argument(
      "--task-id",
      default="tau1",
      help="Identificador de tarea para --role worker (tau1, tau2, tau3, tau4).",
  )
  parser.add_argument(
      "--server-host",
      default="10.10.10.105",
      help="IP de la Micro-VM 105 del Servidor Esporádico (por defecto: 10.10.10.105).",
  )
  parser.add_argument(
      "--server-port",
      type=int,
      default=DEFAULT_TCP_PORT,
      help="Puerto TCP de coordinación (por defecto: 9000).",
  )
  parser.add_argument(
      "--http-port",
      type=int,
      default=DEFAULT_HTTP_PORT,
      help="Puerto HTTP para inyección de trabajos aperiódicos en vivo (por defecto: 8080).",
  )
  parser.add_argument(
      "--tasks-json",
      default="proxmox_schedulable.json",
      help="Ruta al archivo JSON de configuración (proxmox_schedulable.json o proxmox_unschedulable.json).",
  )
  parser.add_argument(
      "--time-scale",
      type=float,
      default=4.0,
      help="Multiplicador de tiempo real K para gráficas de Proxmox (por defecto: 4.0).",
  )
  parser.add_argument(
      "--local-workers",
      action="store_true",
      help="Levanta automáticamente los 4 workers en local si se prueba en 1 sola máquina.",
  )
  parser.add_argument(
      "--output",
      default="sporadic_server_simulation.png",
      help="Ruta para guardar el diagrama de Gantt y curva de presupuesto en PNG.",
  )
  parser.add_argument(
      "--html",
      "--interactive-html",
      dest="html",
      default="sporadic_server_interactive.html",
      help="Ruta para exportar el dashboard interactivo HTML5.",
  )
  parser.add_argument(
      "--export-csv",
      default=None,
      help="Ruta opcional para exportar la traza de ejecución a CSV.",
  )
  parser.add_argument(
      "--export-json",
      default=None,
      help="Ruta opcional para exportar todos los datos de la simulación a JSON.",
  )
  parser.add_argument(
      "--no-show",
      action="store_true",
      help="Ejecutar en modo headless sin abrir ventana GUI de Matplotlib.",
  )
  parser.add_argument(
      "--no-serve",
      action="store_true",
      help="Finalizar inmediatamente tras generar gráficas sin mantener el portal HTTP :8080 activo.",
  )
  args = parser.parse_args()

  if args.role == "worker":
    print(
        f"[*] Iniciando Micro-VM Worker '{args.task_id}' -> "
        f"conectando a {args.server_host}:{args.server_port}..."
    )
    worker = PeriodicWorkerNode(
        task_id=args.task_id,
        server_host=args.server_host,
        server_port=args.server_port,
        enable_burner=True,
    )
    worker.run()
    print(f"[✓] Micro-VM Worker '{args.task_id}' finalizado correctamente.")
    return

  # Rol: server (Micro-VM 105: Coordinador RM + Servidor Esporádico)
  import sporadic_server_sim as ssim  # pylint: disable=g-import-not-at-top

  print(f"[+] Cargando configuración de tareas para Proxmox desde: {args.tasks_json}")
  periodic_tasks, server_config, aperiodic_jobs, horizon, dt = (
      ssim.load_tasks_from_json(args.tasks_json)
  )

  # Evaluar planificabilidad previa al arranque del clúster
  pre_report = ssim.compute_schedulability(periodic_tasks, server_config)
  if pre_report.utilization_total > 1.0:
    print(
        "\033[91m[!] ADVERTENCIA CRÍTICA: U_total = "
        f"{pre_report.utilization_total:.4f} > 1.0 — Sobrecarga de CPU "
        "(Escenario NO PLANIFICABLE).\033[0m"
    )
  elif not pre_report.all_schedulable:
    print(
        "\033[91m[!] ADVERTENCIA: Algunas tareas NO cumplen RTA (R_i > D_i).\033[0m"
    )
  else:
    print(
        f"\033[92m[✓] Clúster planificable bajo RM: U_total = "
        f"{pre_report.utilization_total:.4f} <= 1.0 y todos los R_i <= D_i.\033[0m"
    )

  coord = SporadicServerCoordinator(
      periodic_tasks=periodic_tasks,
      server_config=server_config,
      aperiodic_jobs=aperiodic_jobs,
      horizon=horizon,
      dt=dt,
      time_scale=args.time_scale,
      bind_host="0.0.0.0",
      tcp_port=args.server_port,
      http_port=args.http_port,
      enable_local_burner=True,
  )

  tcp_port = coord.open_tcp_listener()
  http_port = coord.start_http_server()
  print(
      f"[+] Servidor Esporádico activo | TCP Control: :{tcp_port} | "
      f"Portal Web en Vivo: http://localhost:{http_port}/"
  )

  local_threads: list[threading.Thread] = []
  if args.local_workers:
    for ptask in periodic_tasks:
      tid = normalize_task_id(ptask.name)
      wnode = PeriodicWorkerNode(
          task_id=tid,
          server_host="127.0.0.1",
          server_port=tcp_port,
          enable_burner=True,
      )
      t = threading.Thread(target=wnode.run, name=f"worker-{tid}", daemon=True)
      t.start()
      local_threads.append(t)

  try:
    print("[*] Esperando conexión de las 4 Micro-VMs periódicas (tau1..tau4)...")
    coord.wait_for_workers(timeout=60.0)
    print(
        f"[✓] 4 Micro-VMs conectadas. Ejecutando horizonte={horizon}s "
        f"(time_scale={args.time_scale}x -> {horizon * args.time_scale:.1f}s reales)..."
    )
    result = coord.run()
  finally:
    coord.close_workers_and_burner()
    for t in local_threads:
      t.join(timeout=2.0)

  try:
    ssim.print_summary(result)
    ssim.plot_simulation(
        result,
        output_path=args.output,
        show=not args.no_show,
        horizon=horizon,
        periodic_tasks=periodic_tasks,
        server_config=server_config,
        interactive_html=args.html,
    )
    if args.export_csv:
      ssim.export_trace_to_csv(result, args.export_csv)
    if args.export_json:
      ssim.export_trace_to_json(result, args.export_json)

    coord.register_artifacts(html_path=args.html, png_path=args.output)
    if args.time_scale > 0 and not args.no_serve:
      print(
          f"\n\033[92m[🌐] Portal en Vivo, Dashboard HTML5 y Gantt servidos desde VM 105 en:\n"
          f"    - Monitor y Resumen : http://localhost:{http_port}/\n"
          f"    - Dashboard HTML5   : http://localhost:{http_port}/dashboard\n"
          f"    - Diagrama de Gantt : http://localhost:{http_port}/gantt.png\n"
          f"    (Presionar Ctrl+C para detener el servidor web)\033[0m"
      )
      try:
        while True:
          time.sleep(1.0)
      except KeyboardInterrupt:
        print("\n[*] Cerrando servidor web del Servidor Esporádico...")
  finally:
    coord.stop_http_server()


if __name__ == "__main__":
  main()
