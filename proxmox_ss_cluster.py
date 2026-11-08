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

    # Estado en vivo consultable vía HTTP GET /status
    self._status_lock = threading.Lock()
    self._live_status: dict[str, Any] = {
        "logical_time": 0.0,
        "active_runner": "Idle",
        "active_job": "",
        "budget": self.server_config.budget,
        "max_budget": self.server_config.budget,
        "is_active": False,
        "t_a": None,
        "queue_len": 0,
        "deadline_misses_count": 0,
        "registered_workers": [],
    }

    self._http_server: Optional[ThreadingHTTPServer] = None
    self._http_thread: Optional[threading.Thread] = None

  # ── Servidor HTTP de Inyección Aperiódica en Vivo (:8080) ──────────────────

  def start_http_server(self) -> int:
    """Inicia el servidor HTTP en segundo plano para recibir /job y /status.

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

      def do_GET(self) -> None:  # pylint: disable=invalid-name
        parsed = urlparse(self.path)
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

        self._send_json(404, {"error": "Ruta no encontrada. Usar /job o /status"})

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

  def close_all(self) -> None:
    """Cierra el quemador local, el servidor HTTP y envía SHUTDOWN a las 4 Micro-VMs."""
    self.server_burner.close()
    self.stop_http_server()
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
    job_records: dict[str, ssim.JobRecord] = {
        j.name: ssim.JobRecord(job=j, remaining_time=j.compute_time)
        for j in self.aperiodic_jobs
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
        for ajob in self.aperiodic_jobs:
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
            for live_name, live_work in self._live_injected_jobs:
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

        with self._status_lock:
          self._live_status.update({
              "logical_time": round(current_time, 2),
              "active_runner": runner_name,
              "active_job": job_label,
              "budget": round(budget_ticks * self.dt, 4),
              "is_active": is_active,
              "t_a": round(t_a_tick * self.dt, 2) if t_a_tick is not None else None,
              "queue_len": len(aperiodic_queue),
              "deadline_misses_count": len(deadline_misses),
          })

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
      f"HTTP Trigger: http://0.0.0.0:{http_port}/job?name=J_live&work=1.5"
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
    coord.close_all()
    for t in local_threads:
      t.join(timeout=2.0)

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


if __name__ == "__main__":
  main()
