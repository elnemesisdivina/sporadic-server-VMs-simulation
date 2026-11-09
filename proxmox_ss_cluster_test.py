#!/usr/bin/env python3
"""Suite de Pruebas Unitarias e Integración para el Clúster Proxmox del Servidor Esporádico.

Versión: 1.0.0
Licencia: MIT
Maintainer: Ray <elnemesisdivina@gmail.com>

Verifica los 10 invariantes teóricos, de red TCP/HTTP, de procesos quemadores de
CPU (SIGSTOP/SIGCONT), de escenarios planificables (U <= 1.0) y en sobrecarga
(U > 1.0), así como la sintaxis del script de aprovisionamiento para Proxmox VE 9.1-1.
"""

import json
import os
import subprocess
import tempfile
import threading
import time
import unittest
from urllib.request import urlopen

import proxmox_ss_cluster as cluster
import sporadic_server_sim as ssim

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEDULABLE_JSON = os.path.join(BASE_DIR, "proxmox_schedulable.json")
UNSCHEDULABLE_JSON = os.path.join(BASE_DIR, "proxmox_unschedulable.json")
SETUP_SCRIPT = os.path.join(BASE_DIR, "setup_proxmox_vms.sh")


class ProxmoxSporadicServerClusterTest(unittest.TestCase):
  """Suite de 10 pruebas para la simulación de 5 Micro-VMs en Proxmox VE 9.1-1."""

  def test_01_cpu_burner_sigstop_sigcont_lifecycle(self) -> None:
    """Verifica que CPUBurnerProcess inicie en SIGSTOP (0%), pase a SIGCONT (100%) y cierre."""
    burner = cluster.CPUBurnerProcess(label="test-vm", enabled=True)
    try:
      self.assertIsNotNone(burner.pid)
      self.assertFalse(burner.is_burning)

      burner.start_burning()
      self.assertTrue(burner.is_burning)
      time.sleep(0.02)

      burner.stop_burning()
      self.assertFalse(burner.is_burning)
    finally:
      burner.close()
      self.assertIsNone(burner.pid)

  def test_02_proxmox_schedulable_json_rta_and_utilization(self) -> None:
    """Verifica el caso planificable de 5 Micro-VMs (U_total = 38/60 ≈ 0.6333 <= 1.0)."""
    periodic_tasks, server_config, aperiodic_jobs, horizon, dt = (
        ssim.load_tasks_from_json(SCHEDULABLE_JSON)
    )
    self.assertEqual(len(periodic_tasks), 4)
    self.assertEqual(len(aperiodic_jobs), 6)
    self.assertAlmostEqual(horizon, 30.0)
    self.assertAlmostEqual(dt, 0.1)

    report = ssim.compute_schedulability(periodic_tasks, server_config)
    self.assertAlmostEqual(report.utilization_total, 38.0 / 60.0, places=5)
    self.assertLessEqual(report.utilization_total, report.liu_layland_bound)
    self.assertTrue(report.all_schedulable)

    rta_by_prio = {m.priority: m.response_time for m in report.task_metrics}
    self.assertAlmostEqual(rta_by_prio[5], 1.0)  # tau1 (VM 101)
    self.assertAlmostEqual(rta_by_prio[4], 2.0)  # tau2 (VM 102)
    self.assertAlmostEqual(rta_by_prio[3], 4.0)  # S_s  (VM 105)
    self.assertAlmostEqual(rta_by_prio[2], 5.5)  # tau3 (VM 103)
    self.assertAlmostEqual(rta_by_prio[1], 8.5)  # tau4 (VM 104)

  def test_03_proxmox_unschedulable_json_overload_detection(self) -> None:
    """Verifica el caso no planificable / sobrecarga (U_total = 1.1833 > 1.0)."""
    periodic_tasks, server_config, _, _, _ = ssim.load_tasks_from_json(
        UNSCHEDULABLE_JSON
    )
    report = ssim.compute_schedulability(periodic_tasks, server_config)

    self.assertAlmostEqual(report.utilization_total, 71.0 / 60.0, places=5)
    self.assertGreater(report.utilization_total, 1.0)
    self.assertFalse(report.all_schedulable)

    sched_by_prio = {m.priority: m.schedulable for m in report.task_metrics}
    # tau1 (P=5), tau2 (P=4) y S_s (P=3) siguen cumpliendo sus plazos
    self.assertTrue(sched_by_prio[5])
    self.assertTrue(sched_by_prio[4])
    self.assertTrue(sched_by_prio[3])
    # tau3 (P=2, R_3=16.5 > 15.0) y tau4 (P=1) fallan RTA por sobrecarga
    self.assertFalse(sched_by_prio[2])
    self.assertFalse(sched_by_prio[1])

  def test_04_custom_json_modification_triggers_overload(self) -> None:
    """Verifica que modificar compute_time en el JSON provoque U > 1.0 y deadline misses."""
    with open(SCHEDULABLE_JSON, "r", encoding="utf-8") as f:
      data = json.load(f)

    # Incrementar el tiempo de cómputo de tau4 (VM 104) de 2.0 a 10.0 (+0.40 de U)
    data["periodic_tasks"][3]["compute_time"] = 10.0

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
      json.dump(data, tmp)
      tmp_path = tmp.name

    try:
      result, _ = cluster.run_five_node_cluster_simulation(
          tasks_json_path=tmp_path,
          time_scale=0.0,
          enable_burners=False,
      )
      self.assertIsNotNone(result.schedulability)
      self.assertGreater(result.schedulability.utilization_total, 1.0)
      self.assertFalse(result.schedulability.all_schedulable)
      self.assertGreater(len(result.deadline_misses), 0)
    finally:
      if os.path.exists(tmp_path):
        os.remove(tmp_path)

  def test_05_distributed_5_node_cluster_schedulable_execution(self) -> None:
    """Ejecuta el clúster completo de 5 nodos con sockets TCP reales y quemadores de CPU."""
    result, _ = cluster.run_five_node_cluster_simulation(
        tasks_json_path=SCHEDULABLE_JSON,
        time_scale=0.0,
        enable_burners=True,
    )
    # Cero violaciones de plazo en el caso planificable
    self.assertEqual(len(result.deadline_misses), 0)

    # Conservación exacta del tiempo de CPU en el horizonte de 30.0s
    total_cpu = sum(result.cpu_totals.values())
    self.assertAlmostEqual(total_cpu, 30.0, places=4)

    # Con C_s=2.0 y T_s=12.0 en [0, 30], el servidor consume exactamente 6.0s de presupuesto:
    # J1..J4 se completan al 100%, J5 queda con 0.5s restante y J6 en cola (1.0s)
    for jname in ["J1", "J2", "J3", "J4"]:
      self.assertIn(jname, result.job_records)
      self.assertIsNotNone(result.job_records[jname].completion_time)
      self.assertAlmostEqual(result.job_records[jname].remaining_time, 0.0)
    self.assertAlmostEqual(result.job_records["J5"].remaining_time, 0.5)
    self.assertAlmostEqual(result.job_records["J6"].remaining_time, 1.0)

  def test_06_distributed_5_node_cluster_unschedulable_deadline_misses(self) -> None:
    """Verifica que en el clúster de 5 nodos con U > 1.0 ocurran deadline misses reales."""
    result, _ = cluster.run_five_node_cluster_simulation(
        tasks_json_path=UNSCHEDULABLE_JSON,
        time_scale=0.0,
        enable_burners=False,
    )
    self.assertGreater(len(result.deadline_misses), 0)
    missed_tasks = {tname for _, tname in result.deadline_misses}
    self.assertTrue(
        r"$\tau_3$" in missed_tasks or r"$\tau_4$" in missed_tasks,
        f"Se esperaban violaciones en tau3/tau4, se obtuvo: {missed_tasks}",
    )
    # El Servidor Esporádico sigue despachando trabajos aperiódicos cuando tau1/tau2 están libres
    self.assertGreater(result.cpu_totals.get(r"$S_s$", 0.0), 0.0)

  def test_07_sporadic_server_activation_and_replenishment_invariants(self) -> None:
    """Verifica los instantes exactos de activación (t_A) y recarga (t_R = t_A + T_s)."""
    result, _ = cluster.run_five_node_cluster_simulation(
        tasks_json_path=SCHEDULABLE_JSON,
        time_scale=0.0,
        enable_burners=False,
    )
    self.assertGreater(len(result.activations), 0)
    self.assertEqual(len(result.activations), len(result.replenishments))

    for act, rep in zip(result.activations, result.replenishments):
      # Regla 6 de Buttazzo §5.6: t_R = t_A + T_s (con T_s = 12.0)
      self.assertAlmostEqual(rep.replenishment_time, act.activation_time + 12.0, places=4)
      self.assertAlmostEqual(rep.amount, act.amount_consumed, places=4)

    # El presupuesto nunca supera C_s_max = 2.0 ni cae por debajo de 0.0
    for bval in result.budget_trace_values:
      self.assertGreaterEqual(bval, -1e-6)
      self.assertLessEqual(bval, 2.0 + 1e-6)

  def test_08_live_http_aperiodic_job_injection_and_status(self) -> None:
    """Verifica la inyección de trabajos en vivo vía HTTP (/job y /status) en la VM 105."""
    periodic_tasks, server_config, aperiodic_jobs, horizon, dt = (
        ssim.load_tasks_from_json(SCHEDULABLE_JSON)
    )
    coord = cluster.SporadicServerCoordinator(
        periodic_tasks=periodic_tasks,
        server_config=server_config,
        aperiodic_jobs=aperiodic_jobs,
        horizon=horizon,
        dt=dt,
        time_scale=0.0005,
        bind_host="127.0.0.1",
        tcp_port=0,
        http_port=0,
        enable_local_burner=False,
    )
    tcp_port = coord.open_tcp_listener()
    http_port = coord.start_http_server()

    worker_threads: list[threading.Thread] = []
    for ptask in periodic_tasks:
      tid = cluster.normalize_task_id(ptask.name)
      wnode = cluster.PeriodicWorkerNode(
          task_id=tid,
          server_host="127.0.0.1",
          server_port=tcp_port,
          enable_burner=False,
      )
      t = threading.Thread(target=wnode.run, daemon=True)
      t.start()
      worker_threads.append(t)

    try:
      coord.wait_for_workers(timeout=5.0)

      # Consultar portal en vivo (/), /status y disparar /job?name=J_urgente&work=0.5 por HTTP
      with urlopen(f"http://127.0.0.1:{http_port}/", timeout=2.0) as resp:
        portal_html = resp.read().decode("utf-8")
        self.assertIn("Clúster Proxmox VE 9.1-1", portal_html)

      with urlopen(f"http://127.0.0.1:{http_port}/status", timeout=2.0) as resp:
        status_data = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(len(status_data["registered_workers"]), 4)

      with urlopen(
          f"http://127.0.0.1:{http_port}/job?name=J_urgente&work=0.5", timeout=2.0
      ) as resp:
        job_resp = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(job_resp["status"], "enqueued")
        self.assertEqual(job_resp["name"], "J_urgente")

      with urlopen(
          f"http://127.0.0.1:{http_port}/job?name=J_urgente&work=0.5", timeout=2.0
      ) as resp:
        job_resp2 = json.loads(resp.read().decode("utf-8"))
        self.assertEqual(job_resp2["status"], "enqueued")

      result = coord.run()
      self.assertIn("J_urgente", result.job_records)
      self.assertIn("J_urgente_2", result.job_records)
      self.assertIsNotNone(result.job_records["J_urgente"].completion_time)
    finally:
      coord.close_all()
      for t in worker_threads:
        t.join(timeout=2.0)

  def test_09_export_png_html_csv_json_integration(self) -> None:
    """Verifica que la traza del clúster genere los archivos PNG, HTML5, CSV y JSON."""
    result, coord = cluster.run_five_node_cluster_simulation(
        tasks_json_path=SCHEDULABLE_JSON,
        time_scale=0.0,
        enable_burners=False,
    )
    with tempfile.TemporaryDirectory() as tmpdir:
      png_path = os.path.join(tmpdir, "proxmox_sim.png")
      html_path = os.path.join(tmpdir, "proxmox_interactive.html")
      csv_path = os.path.join(tmpdir, "proxmox_trace.csv")
      json_path = os.path.join(tmpdir, "proxmox_data.json")

      ssim.plot_simulation(
          result,
          output_path=png_path,
          show=False,
          horizon=coord.horizon,
          periodic_tasks=coord.periodic_tasks,
          server_config=coord.server_config,
          interactive_html=html_path,
      )
      ssim.export_trace_to_csv(result, csv_path)
      ssim.export_trace_to_json(result, json_path)

      self.assertTrue(os.path.isfile(png_path) and os.path.getsize(png_path) > 1000)
      self.assertTrue(os.path.isfile(html_path) and os.path.getsize(html_path) > 1000)
      self.assertTrue(os.path.isfile(csv_path) and os.path.getsize(csv_path) > 100)
      self.assertTrue(os.path.isfile(json_path) and os.path.getsize(json_path) > 100)

  def test_10_setup_proxmox_vms_shell_script_validation(self) -> None:
    """Verifica la sintaxis bash -n y banderas clave de setup_proxmox_vms.sh."""
    self.assertTrue(os.path.isfile(SETUP_SCRIPT))
    self.assertTrue(os.access(SETUP_SCRIPT, os.X_OK))

    proc = subprocess.run(
        ["bash", "-n", SETUP_SCRIPT],
        capture_output=True,
        text=True,
        check=False,
    )
    self.assertEqual(proc.returncode, 0, msg=f"Error de sintaxis en bash: {proc.stderr}")

    with open(SETUP_SCRIPT, "r", encoding="utf-8") as f:
      script_content = f.read()

    self.assertIn("--cleanup", script_content)
    self.assertIn("boxes -d critical", script_content)
    self.assertIn("import-from=", script_content)
    self.assertIn("Maintainer: Ray <elnemesisdivina@gmail.com>", script_content)


if __name__ == "__main__":
  unittest.main()
