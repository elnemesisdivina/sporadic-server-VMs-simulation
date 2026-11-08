"""Simulación autónoma y visualización interactiva del Servidor Esporádico (Sporadic Server).

Implementa el algoritmo del Servidor Esporádico (SS) bajo planificación expulsiva
(preemptive) por Tasa Monótona (Rate-Monotonic) según Sprunt, Sha y Lehoczky (1989),
tal como se presenta en el libro de Giorgio Buttazzo: "Hard Real-Time Computing Systems",
Sección 5.6.
"""

import argparse
import collections
import csv
import dataclasses
import json
import math
import os
from typing import Any, Optional

import matplotlib

if not os.environ.get("DISPLAY"):
  matplotlib.use("Agg")

# pylint: disable=g-import-not-at-top
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

# pylint: enable=g-import-not-at-top

VERSION = "2.1.0"

# Paleta de colores para asignación automática cuando se cargan tareas desde JSON
_COLOR_PALETTE = [
    "#0072B2", "#009E73", "#D55E00", "#CC79A7",
    "#E69F00", "#56B4E9", "#F0E442", "#999999",
    "#882255", "#44AA99", "#332288", "#DDCC77",
]

# ==============================================================================
# Configuración Editable de Simulación (Carga por defecto: 6 Periódicas + 1 Servidor)
# ==============================================================================


@dataclasses.dataclass
class PeriodicTask:
  """Configuración para una tarea periódica de tiempo real estricto (Hard Real-Time).

  Atributos:
    name: Identificador o nombre de la tarea (ej. r"$\\tau_1$").
    priority: Prioridad estática asignada (mayor entero = mayor prioridad en RM).
    compute_time: Tiempo de cómputo en el peor caso (WCET, C_i).
    deadline: Plazo relativo de ejecución (D_i).
    period: Período de repetición o llegada entre instancias (T_i).
    color: Código hexadecimal de color para la visualización en la gráfica.
  """

  name: str
  priority: int
  compute_time: float
  deadline: float
  period: float
  color: str


@dataclasses.dataclass
class SporadicServerConfig:
  """Configuración del Servidor Esporádico (Sporadic Server).

  Atributos:
    name: Nombre o etiqueta del servidor (ej. r"$S_s$").
    priority: Prioridad asignada según Rate-Monotonic (en función de su período T_s).
    budget: Capacidad máxima o presupuesto inicial de cómputo (C_s).
    period: Período de recarga o reposición de presupuesto (T_s).
    deadline: Plazo de ejecución asignado al servidor (típicamente D_s = T_s).
    color: Código hexadecimal de color para representar al servidor.
  """

  name: str
  priority: int
  budget: float  # Capacidad máxima C_s
  period: float  # Período de recarga T_s
  deadline: float  # Plazo del servidor D_s
  color: str


@dataclasses.dataclass
class AperiodicJob:
  """Especificación de un trabajo aperiódico individual.

  Atributos:
    name: Identificador del trabajo (ej. "J1").
    arrival_time: Instante de tiempo de llegada al sistema (r_i).
    compute_time: Tiempo de cómputo requerido para finalizar (C_i).
  """

  name: str
  arrival_time: float
  compute_time: float


# Default simulation parameters
SIMULATION_HORIZON = 30.0
DT = 0.1

PERIODIC_TASKS = [
    PeriodicTask(
        name=r"$\tau_1$",
        priority=7,
        compute_time=1.0,
        deadline=6.0,
        period=6.0,
        color="#0072B2",
    ),
    PeriodicTask(
        name=r"$\tau_2$",
        priority=6,
        compute_time=1.0,
        deadline=10.0,
        period=10.0,
        color="#009E73",
    ),
    PeriodicTask(
        name=r"$\tau_3$",
        priority=5,
        compute_time=1.0,
        deadline=12.0,
        period=12.0,
        color="#D55E00",
    ),
    PeriodicTask(
        name=r"$\tau_4$",
        priority=3,
        compute_time=1.5,
        deadline=15.0,
        period=15.0,
        color="#CC79A7",
    ),
    PeriodicTask(
        name=r"$\tau_5$",
        priority=2,
        compute_time=2.0,
        deadline=20.0,
        period=20.0,
        color="#E69F00",
    ),
    PeriodicTask(
        name=r"$\tau_6$",
        priority=1,
        compute_time=2.0,
        deadline=30.0,
        period=30.0,
        color="#56B4E9",
    ),
]

SERVER_CONFIG = SporadicServerConfig(
    name=r"$S_s$",
    priority=4,
    budget=2.0,
    period=12.0,
    deadline=12.0,
    color="#16a085",
)

APERIODIC_JOBS = [
    AperiodicJob("J1", arrival_time=2.0, compute_time=1.0),
    AperiodicJob("J2", arrival_time=5.0, compute_time=1.0),
    AperiodicJob("J3", arrival_time=9.0, compute_time=2.0),
    AperiodicJob("J4", arrival_time=13.0, compute_time=1.0),
    AperiodicJob("J5", arrival_time=17.0, compute_time=1.5),
    AperiodicJob("J6", arrival_time=22.0, compute_time=1.0),
]


# ==============================================================================
# Estructuras de Datos de la Simulación y Funciones Auxiliares
# ==============================================================================


def to_ticks(val: float, dt: float) -> int:
  """Convierte un valor de tiempo continuo a ticks discretos enteros.

  Args:
    val: Valor en tiempo continuo (segundos o unidades de tiempo).
    dt: Paso de discretización de la simulación.

  Returns:
    Número entero de ticks más cercano mediante redondeo.
  """
  return int(round(val / dt))


@dataclasses.dataclass
class ExecutionSlice:
  """Bloque contiguo de ejecución en el procesador (CPU).

  Atributos:
    start_time: Instante de inicio del bloque de ejecución.
    end_time: Instante de finalización del bloque de ejecución.
    runner_name: Nombre de la tarea o servidor en ejecución (o 'Idle' si la CPU está ociosa).
    job_label: Etiqueta del trabajo aperiódico atendido (si aplica).
  """

  start_time: float
  end_time: float
  runner_name: str
  job_label: str = ""


@dataclasses.dataclass
class ReplenishmentEvent:
  """Evento programado de recarga o reposición de presupuesto.

  Atributos:
    replenishment_time: Instante de recarga programado (t_R = t_A + T_s).
    amount: Cantidad de presupuesto C_s a reponer en dicho instante.
  """

  replenishment_time: float
  amount: float


@dataclasses.dataclass
class ActivationEvent:
  """Ventana activa de consumo del Servidor Esporádico.

  Atributos:
    activation_time: Instante de inicio de la ventana activa (t_A).
    closed_time: Instante en que se cierra la ventana de activación.
    amount_consumed: Cantidad total de presupuesto consumido durante esta ventana.
    reason: Causa de cierre ('budget_exhausted', 'queue_empty' o 'became_idle').
  """

  activation_time: float
  closed_time: float
  amount_consumed: float
  reason: str


@dataclasses.dataclass
class JobRecord:
  """Registro histórico de métricas y ejecución de un trabajo aperiódico.

  Atributos:
    job: Especificación original del trabajo (llegada r_i, cómputo C_i).
    completion_time: Instante de finalización en CPU (f_i).
    remaining_time: Cómputo pendiente en caso de no haber completado.
    response_time: Tiempo total de respuesta W_i = f_i - r_i.
    waiting_time: Tiempo de espera en cola Z_i = W_i - C_i.
  """

  job: AperiodicJob
  completion_time: Optional[float] = None
  remaining_time: float = 0.0
  response_time: Optional[float] = None
  waiting_time: Optional[float] = None


@dataclasses.dataclass
class TaskSchedulabilityMetric:
  """Resultado del análisis de planificabilidad individual para una tarea.

  Atributos:
    name: Nombre de la tarea periódica o del servidor.
    priority: Prioridad estática asignada.
    c_i: Tiempo de cómputo en el peor caso (C_i).
    t_i: Período de repetición (T_i).
    d_i: Plazo de ejecución relativo (D_i).
    u_i: Factor de utilización individual U_i = C_i / T_i.
    response_time: Tiempo de respuesta en el peor caso calculado por RTA (R_i).
    schedulable: True si R_i <= D_i (la tarea cumple su plazo); False en caso contrario.
  """

  name: str
  priority: int
  c_i: float
  t_i: float
  d_i: float
  u_i: float
  response_time: float
  schedulable: bool


@dataclasses.dataclass
class SchedulabilityReport:
  """Informe consolidado de planificabilidad en tiempo real y métricas de QoS.

  Atributos:
    utilization_periodic: Utilización acumulada de tareas periódicas U_p.
    utilization_server: Utilización del Servidor Esporádico U_s = C_s / T_s.
    utilization_total: Utilización total del sistema U_total = U_p + U_s.
    liu_layland_bound: Cota teórica de utilización de Liu & Layland para n tareas.
    task_metrics: Lista de métricas individuales de RTA para cada tarea.
    all_schedulable: True si todas las tareas cumplen R_i <= D_i.
    avg_aperiodic_response_time: Tiempo promedio de respuesta de trabajos aperiódicos.
    max_aperiodic_response_time: Tiempo máximo de respuesta de trabajos aperiódicos.
  """

  utilization_periodic: float
  utilization_server: float
  utilization_total: float
  liu_layland_bound: float
  task_metrics: list[TaskSchedulabilityMetric]
  all_schedulable: bool
  avg_aperiodic_response_time: Optional[float]
  max_aperiodic_response_time: Optional[float]


@dataclasses.dataclass
class SimulationResult:
  """Resultado completo de la simulación, trazas temporales y metadatos.

  Atributos:
    slices: Segmentos contiguos de asignación del CPU.
    budget_trace_times: Puntos temporales de la traza de presupuesto C_s(t).
    budget_trace_values: Valores correspondientes de C_s(t) en cada punto.
    activations: Lista de ventanas activas del Servidor Esporádico registradas.
    replenishments: Lista de eventos de recarga programados y efectuados.
    periodic_release_arrows: Tiempos de liberación de instancias periódicas.
    periodic_deadline_arrows: Tiempos de plazos absolutos de tareas periódicas.
    aperiodic_arrival_arrows: Tiempos de arribo de peticiones aperiódicas.
    cpu_totals: Tiempo acumulado de procesador por cada ejecutor.
    deadline_misses: Lista de violaciones de plazo registradas (tiempo, tarea).
    job_records: Diccionario de métricas detalladas por cada trabajo aperiódico.
    schedulability: Informe formal de planificabilidad matemática y RTA.
  """

  slices: list[ExecutionSlice]
  budget_trace_times: list[float]
  budget_trace_values: list[float]
  activations: list[ActivationEvent]
  replenishments: list[ReplenishmentEvent]
  periodic_release_arrows: list[tuple[float, str]]
  periodic_deadline_arrows: list[tuple[float, str]]
  aperiodic_arrival_arrows: list[tuple[float, str]]
  cpu_totals: dict[str, float]
  deadline_misses: list[tuple[float, str]]
  job_records: dict[str, JobRecord]
  schedulability: Optional[SchedulabilityReport] = None


@dataclasses.dataclass
class _PeriodicJobInstance:
  """Instancia dinámica activa de una tarea periódica en la cola del planificador."""

  task: PeriodicTask
  remaining_ticks: int
  deadline_tick: int


@dataclasses.dataclass
class _AperiodicJobInstance:
  """Instancia dinámica activa de una petición aperiódica en la cola del servidor."""

  job: AperiodicJob
  remaining_ticks: int


# ==============================================================================
# Análisis Formal de Planificabilidad y RTA (Teorema 5.1 de Buttazzo)
# ==============================================================================


def compute_schedulability(
    periodic_tasks: list[PeriodicTask],
    server_config: SporadicServerConfig,
    job_records: Optional[dict[str, JobRecord]] = None,
) -> SchedulabilityReport:
  """Calcula la utilización bajo Rate-Monotonic y el Análisis de Tiempo de Respuesta (RTA).

  Fundamento Teórico:
    Por el Teorema 5.1 de Giorgio Buttazzo ("Hard Real-Time Computing Systems"),
    un Servidor Esporádico caracterizado por (C_s, T_s) produce exactamente la misma
    interferencia en el peor caso sobre las tareas de menor prioridad que una tarea
    periódica estricta equivalente con tiempo de cómputo C = C_s y período T = T_s.
    Por lo tanto, la planificabilidad del conjunto completo se valida unificando el
    servidor como una tarea periódica más y aplicando la ecuación de punto fijo de RTA.

  Args:
    periodic_tasks: Lista de tareas periódicas del sistema.
    server_config: Configuración del Servidor Esporádico (C_s, T_s, prioridad).
    job_records: Diccionario opcional de registros de trabajos para calcular métricas de QoS.

  Returns:
    Objeto SchedulabilityReport con utilizaciones, cota de Liu & Layland, tiempos
    de respuesta individuales y estado de planificabilidad.
  """
  # ── PASO 1: UNIFICAR TAREAS PERIÓDICAS Y SERVIDOR ─────────────────────────
  # Tratamos al servidor como una tarea periódica equivalente (Teorema 5.1 Buttazzo).
  unified = [
      (p.name, p.priority, p.compute_time, p.period, p.deadline)
      for p in periodic_tasks
  ] + [(
      server_config.name,
      server_config.priority,
      server_config.budget,
      server_config.period,
      server_config.deadline,
  )]

  # ── PASO 2: ORDENAR POR PRIORIDAD DECRECIENTE ──────────────────────────────
  # Las tareas de mayor prioridad interfieren sobre las de menor prioridad.
  unified.sort(key=lambda t: t[1], reverse=True)
  n_tasks = len(unified)

  # ── PASO 3: CALCULAR FACTORES DE UTILIZACIÓN ──────────────────────────────
  # Utilización: U = Σ (C_i / T_i).
  # Cota suficiente de Liu & Layland (1973): U_LL = n * (2^(1/n) - 1).
  # Si U_total > 1.0 => El sistema es INVIABLE (sobrecarga del procesador).
  # Si U_LL < U_total <= 1.0 => La prueba de Liu & Layland no concluye; RTA es condición exacta.
  u_p = sum(p.compute_time / p.period for p in periodic_tasks)
  u_s = server_config.budget / server_config.period
  u_total = u_p + u_s
  ll_bound = n_tasks * (math.pow(2.0, 1.0 / n_tasks) - 1.0)

  metrics: list[TaskSchedulabilityMetric] = []
  all_sched = True

  # ── PASO 4: ANÁLISIS DE TIEMPO DE RESPUESTA (RTA) POR ITERACIÓN DE PUNTO FIJO
  # Para cada tarea τ_i, resolvemos la recurrencia:
  #   R_i^(k+1) = C_i + Σ_{j ∈ hp(i)} ⌈ R_i^(k) / T_j ⌉ * C_j
  # donde hp(i) representa el conjunto de tareas de mayor prioridad que τ_i.
  # La iteración se detiene cuando R_i^(k+1) == R_i^(k) o cuando R_i > D_i.
  for idx, (name, prio, c_i, t_i, d_i) in enumerate(unified):
    u_i = c_i / t_i
    hp_tasks = unified[:idx]  # Conjunto de tareas de mayor prioridad

    r_k = c_i  # Condición inicial: cómputo propio sin interferencia
    converged = False
    for _ in range(100):
      # Suma de interferencia de todas las tareas de mayor prioridad
      r_next = c_i + sum(math.ceil(r_k / t_j) * c_j for _, _, c_j, t_j, _ in hp_tasks)
      if r_next == r_k:
        converged = True
        break
      if r_next > d_i:
        r_k = r_next
        break  # Superó el plazo: la tarea no es planificable
      r_k = r_next

    sched = converged and (r_k <= d_i)
    if not sched:
      all_sched = False

    metrics.append(
        TaskSchedulabilityMetric(
            name=name,
            priority=prio,
            c_i=c_i,
            t_i=t_i,
            d_i=d_i,
            u_i=u_i,
            response_time=r_k,
            schedulable=sched,
        )
    )

  # ── PASO 5: MÉTRICAS DE CALIDAD DE SERVICIO (QoS) APERIÓDICO ───────────────
  # Tiempo de respuesta = f_i - r_i (finalización - llegada).
  # Tiempo de espera en cola = (f_i - r_i) - C_i.
  completed_resp_times = []
  if job_records:
    for rec in job_records.values():
      if rec.completion_time is not None:
        resp = rec.completion_time - rec.job.arrival_time
        rec.response_time = resp
        rec.waiting_time = resp - rec.job.compute_time
        completed_resp_times.append(resp)

  avg_resp = (
      sum(completed_resp_times) / len(completed_resp_times)
      if completed_resp_times
      else None
  )
  max_resp = max(completed_resp_times) if completed_resp_times else None

  return SchedulabilityReport(
      utilization_periodic=u_p,
      utilization_server=u_s,
      utilization_total=u_total,
      liu_layland_bound=ll_bound,
      task_metrics=metrics,
      all_schedulable=all_sched,
      avg_aperiodic_response_time=avg_resp,
      max_aperiodic_response_time=max_resp,
  )


# ==============================================================================
# Lógica de Planificación y Simulación Temporal
# ==============================================================================


def is_server_ready(queue_len: int, budget_ticks: int) -> bool:
  """Determina si el Servidor Esporádico está listo (condición 'Ready') para ejecutarse.

  Condición formal del algoritmo (Sprunt-Sha-Lehoczky / Buttazzo §5.6):
    El servidor puede competir por el procesador si y solo si:
      1. Su cola de peticiones aperiódicas no está vacía (queue_len > 0).
      2. Su presupuesto disponible C_s(t) es estrictamente positivo (budget_ticks > 0).

  Args:
    queue_len: Cantidad de tareas aperiódicas actualmente en la cola FIFO.
    budget_ticks: Presupuesto actual de capacidad C_s(t) expresado en ticks discretos.

  Returns:
    True si y solo si ambas condiciones se cumplen; False en caso contrario.
  """
  return queue_len > 0 and budget_ticks > 0


def run_simulation(
    horizon: float = SIMULATION_HORIZON,
    dt: float = DT,
    periodic_tasks: Optional[list[PeriodicTask]] = None,
    server_config: Optional[SporadicServerConfig] = None,
    aperiodic_jobs: Optional[list[AperiodicJob]] = None,
    mode: str = "queue_aware",
) -> SimulationResult:
  """Ejecuta la simulación a tiempo discreto del Servidor Esporádico (Sporadic Server).

  Args:
    horizon: Duración total de la simulación en unidades de tiempo continuo.
    dt: Paso de discretización temporal (delta t).
    periodic_tasks: Lista de tareas periódicas de tiempo real estricto (Hard Real-Time).
    server_config: Configuración y parámetros del Servidor Esporádico (C_s, T_s, prioridad).
    aperiodic_jobs: Lista de trabajos aperiódicos con instantes de llegada y cómputo.
    mode: 'queue_aware' (estándar POSIX.1d: t_A se fija cuando el servidor ejecuta un trabajo) o
      'buttazzo_pure' (SSL89 original: t_A se fija siempre que P_exe >= P_s y C_s > 0).

  Returns:
    SimulationResult con rebanadas de ejecución, trazas de presupuesto y métricas.
  """
  if periodic_tasks is None:
    periodic_tasks = PERIODIC_TASKS
  if server_config is None:
    server_config = SERVER_CONFIG
  if aperiodic_jobs is None:
    aperiodic_jobs = APERIODIC_JOBS

  total_ticks = to_ticks(horizon, dt)
  max_budget_ticks = to_ticks(server_config.budget, dt)
  server_period_ticks = to_ticks(server_config.period, dt)

  budget_ticks = max_budget_ticks
  pending_replenishments: dict[int, int] = collections.defaultdict(int)

  aperiodic_queue: collections.deque[_AperiodicJobInstance] = collections.deque()
  periodic_instances: list[_PeriodicJobInstance] = []

  is_active = False
  t_a_tick: Optional[int] = None
  consumed_ticks = 0

  activations: list[ActivationEvent] = []
  replenishments: list[ReplenishmentEvent] = []
  raw_slices: list[tuple[str, str]] = []  # (ejecutor_en_cpu, etiqueta_trabajo) por tick
  budget_times: list[float] = [0.0]
  budget_values: list[float] = [server_config.budget]

  periodic_releases: list[tuple[float, str]] = []
  periodic_deadlines: list[tuple[float, str]] = []
  aperiodic_arrivals: list[tuple[float, str]] = []
  deadline_misses: list[tuple[float, str]] = []
  job_records: dict[str, JobRecord] = {
      j.name: JobRecord(job=j, remaining_time=j.compute_time)
      for j in aperiodic_jobs
  }

  # ─────────────────────────────────────────────────────────────────────────────
  # BUCLE PRINCIPAL DE SIMULACIÓN (tick a tick, paso de tiempo dt)
  # Cada iteración k representa el intervalo [k·dt, (k+1)·dt).
  # ─────────────────────────────────────────────────────────────────────────────
  for k in range(total_ticks):
    current_time = k * dt  # Tiempo continuo correspondiente al tick actual

    # ── PASO 1: APLICAR RECARGAS PROGRAMADAS DE PRESUPUESTO ───────────────────
    # El Servidor Esporádico programa una recarga de presupuesto en t_R = t_A + T_s.
    # Cuando llegamos al tick de recarga, restauramos el presupuesto C_s hasta el
    # máximo permitido (no puede exceder el presupuesto inicial C_s_max).
    # REGLA BUTTAZZO §5.6: la recarga es igual a la cantidad consumida, no siempre
    # el presupuesto completo.
    if k in pending_replenishments:
      amt = pending_replenishments[k]
      budget_times.append(current_time)
      budget_values.append(budget_ticks * dt)
      budget_ticks = min(max_budget_ticks, budget_ticks + amt)
      budget_times.append(current_time)
      budget_values.append(budget_ticks * dt)

    # ── PASO 2: VERIFICAR DEADLINES Y LIBERAR NUEVAS INSTANCIAS PERIÓDICAS ────
    # 2a) Verificar si alguna instancia activa ha superado su deadline absoluto.
    #     Bajo RM, si una tarea aún tiene cómputo pendiente al llegar a d_i = k·dt,
    #     se registra como un "deadline miss" (violación de plazo).
    for inst in periodic_instances:
      if inst.remaining_ticks > 0 and k >= inst.deadline_tick:
        deadline_misses.append((current_time, inst.task.name))

    # 2b) Liberar nuevas instancias periódicas según su período.
    #     Bajo Rate-Monotonic, las tareas se ordenan por prioridad inversamente
    #     proporcional al período: periodo más corto → prioridad más alta.
    #     En t = 0, k·T_i == 0 para toda τ_i, por lo que todas se liberan a la vez.
    for ptask in periodic_tasks:
      p_period_ticks = to_ticks(ptask.period, dt)
      if k % p_period_ticks == 0:  # ¿Es k un múltiplo del período de la tarea?
        c_ticks = to_ticks(ptask.compute_time, dt)
        d_ticks = to_ticks(ptask.deadline, dt)
        # Crear instancia con cómputo restante = C_i y deadline absoluto = k + D_i
        periodic_instances.append(
            _PeriodicJobInstance(
                task=ptask,
                remaining_ticks=c_ticks,
                deadline_tick=k + d_ticks,
            )
        )
        periodic_releases.append((current_time, ptask.name))
        periodic_deadlines.append(((k + d_ticks) * dt, ptask.name))

    # ── PASO 3: ENCOLAR TRABAJOS APERIÓDICOS QUE LLEGAN EN ESTE TICK ──────────
    # Los trabajos aperiódicos llegan en tiempos arbitrarios (no periódicos).
    # Los encolamos en la cola FIFO del Servidor Esporádico cuando su tiempo de
    # llegada r_i coincide con el tick actual k.
    # El servidor los atenderá uno por uno en orden de llegada (FIFO).
    for ajob in aperiodic_jobs:
      if to_ticks(ajob.arrival_time, dt) == k:
        aperiodic_queue.append(
            _AperiodicJobInstance(
                job=ajob,
                remaining_ticks=to_ticks(ajob.compute_time, dt),
            )
        )
        aperiodic_arrivals.append((current_time, ajob.name))

    # ── PASO 4: REEVALUACIÓN DE ESTADO Y DESPACHO POR PRIORIDAD (DISPATCHER RM) ──
    # Se evalúa en cada tick si el Servidor Esporádico está listo (condición isReady).
    # Reevaluación inmediata: si una tarea previa terminó o el procesador queda libre,
    # el planificador evalúa de inmediato a todos los candidatos listos.
    server_ready = is_server_ready(len(aperiodic_queue), budget_ticks)

    # Identificar todas las tareas periódicas que tienen cómputo pendiente (ready)
    ready_periodics = [p for p in periodic_instances if p.remaining_ticks > 0]
    best_periodic = (
        max(ready_periodics, key=lambda p: p.task.priority)
        if ready_periodics
        else None
    )

    # ── MODO BUTTAZZO PURO (SSL89): activar ventana cuando P_exe >= P_s ────────
    # En el modo original de Sprunt-Sha-Lehoczky (1989), la ventana de activación
    # t_A se abre cuando el ejecutor en CPU tiene prioridad >= P_s, aunque el
    # servidor no esté ejecutando trabajos aperiódicos. El modo por defecto
    # (queue_aware) solo abre t_A cuando el servidor realmente ejecuta un trabajo.
    if mode == "buttazzo_pure" and not is_active and budget_ticks > 0:
      if (
          best_periodic
          and best_periodic.task.priority >= server_config.priority
      ):
        is_active = True
        t_a_tick = k
        consumed_ticks = 0

    # ── PASO 5: EJECUTAR LA TAREA SELECCIONADA (un tick de tiempo dt) ─────────
    # Comparación matemática de prioridades bajo Rate-Monotonic:
    # El Servidor Esporádico participa como un competidor más con prioridad server_config.priority.
    # Se evalúan los tres casos mutuamente excluyentes:
    chosen_runner = "Idle"  # Por defecto: CPU ociosa si nadie está listo
    chosen_label = ""       # Etiqueta del trabajo aperiódico (si aplica)

    if best_periodic and (
        not server_ready or best_periodic.task.priority > server_config.priority
    ):
      # ── CASO A: Despacho de Tarea Periódica de Mayor Prioridad ─────────────
      # La tarea periódica toma el control del procesador (preempción RM).
      # Si el servidor estaba listo (server_ready=True), queda en estado DESALOJADO
      # (preempted) sin consumir su presupuesto durante este tick.
      chosen_runner = best_periodic.task.name
      best_periodic.remaining_ticks -= 1  # Consume 1 tick de cómputo

    elif server_ready:
      # ── CASO B: Despacho del Servidor Esporádico (Transición a EJECUTANDO) ──
      # El procesador está libre de tareas periódicas de mayor prioridad y el
      # servidor cumple la condición is_server_ready (cola no vacía y presupuesto > 0).
      # Su estado transiciona inmediatamente de DESALOJADO u OCIOSO a EJECUTANDO.
      chosen_runner = server_config.name

      # Si el servidor no tenía ventana activa abierta, se registra el instante t_A
      if not is_active:
        is_active = True
        t_a_tick = k       # Guardar tick de inicio de la ventana activa (t_A)
        consumed_ticks = 0  # Reiniciar contador de ticks consumidos en esta ventana

      # Extraer y ejecutar el trabajo al frente de la cola aperiódica (FIFO)
      front_job = aperiodic_queue[0]
      chosen_label = front_job.job.name
      front_job.remaining_ticks -= 1   # Reducir cómputo pendiente del trabajo
      budget_ticks -= 1                # Reducir presupuesto del servidor en 1 tick
      consumed_ticks += 1              # Contabilizar ticks consumidos en esta ventana

      # ¿El trabajo aperiódico completó su tiempo de cómputo C_i?
      if front_job.remaining_ticks == 0:
        aperiodic_queue.popleft()  # Sacarlo de la cola FIFO
        # Registrar tiempo de finalización: al final del tick actual = (k+1)·dt
        job_records[front_job.job.name].completion_time = (k + 1) * dt
        job_records[front_job.job.name].remaining_time = 0.0

      # ── PASO 6: CERRAR VENTANA ACTIVA Y PROGRAMAR RECARGA (REPLENISHMENT) ──
      # La ventana activa se cierra cuando:
      #   (a) el presupuesto C_s se agota (budget_ticks == 0), o
      #   (b) la cola aperiódica queda vacía (todos los trabajos terminaron)
      # Al cerrar la ventana, se programa la recarga en t_R = t_A + T_s.
      # La recarga es EXACTAMENTE la cantidad consumida en esta ventana (no
      # necesariamente C_s completo), siguiendo la regla de Buttazzo §5.6.
      if budget_ticks == 0 or len(aperiodic_queue) == 0:
        r_tick = t_a_tick + server_period_ticks  # t_R = t_A + T_s (en ticks)
        pending_replenishments[r_tick] += consumed_ticks  # Programar recarga
        replenishments.append(
            ReplenishmentEvent(
                replenishment_time=r_tick * dt,    # t_R en tiempo continuo
                amount=consumed_ticks * dt,         # Cantidad a recargar = consumido
            )
        )
        activations.append(
            ActivationEvent(
                activation_time=t_a_tick * dt,       # t_A: inicio de la ventana
                closed_time=(k + 1) * dt,            # t_cierre: fin de la ventana
                amount_consumed=consumed_ticks * dt,  # Total consumido
                reason="budget_exhausted"
                if budget_ticks == 0
                else "queue_empty",
            )
        )
        # Desactivar ventana hasta la llegada del siguiente trabajo aperiódico
        is_active = False
        t_a_tick = None
        consumed_ticks = 0

    elif mode == "buttazzo_pure" and is_active:
      # En modo puro Buttazzo, si corre una tarea de menor prioridad o la CPU queda idle, el SS pasa a ocioso
      if not best_periodic or best_periodic.task.priority < server_config.priority:
        if consumed_ticks > 0:
          r_tick = t_a_tick + server_period_ticks
          pending_replenishments[r_tick] += consumed_ticks
          replenishments.append(
              ReplenishmentEvent(
                  replenishment_time=r_tick * dt,
                  amount=consumed_ticks * dt,
              )
          )
          activations.append(
              ActivationEvent(
                  activation_time=t_a_tick * dt,
                  closed_time=current_time,
                  amount_consumed=consumed_ticks * dt,
                  reason="became_idle",
              )
          )
        is_active = False
        t_a_tick = None
        consumed_ticks = 0

    raw_slices.append((chosen_runner, chosen_label))
    budget_times.append((k + 1) * dt)
    budget_values.append(budget_ticks * dt)

  # Check final replenishment at horizon boundary
  if total_ticks in pending_replenishments:
    amt = pending_replenishments[total_ticks]
    budget_times.append(horizon)
    budget_values.append(budget_ticks * dt)
    budget_ticks = min(max_budget_ticks, budget_ticks + amt)
    budget_times.append(horizon)
    budget_values.append(budget_ticks * dt)

  # Record remaining computation for incomplete aperiodic jobs
  for item in aperiodic_queue:
    job_records[item.job.name].remaining_time = item.remaining_ticks * dt

  # Merge contiguous slices
  merged_slices: list[ExecutionSlice] = []
  if raw_slices:
    cur_runner, cur_label = raw_slices[0]
    slice_start = 0
    for tick_idx in range(1, len(raw_slices)):
      r, l = raw_slices[tick_idx]
      if (r, l) != (cur_runner, cur_label):
        merged_slices.append(
            ExecutionSlice(
                start_time=slice_start * dt,
                end_time=tick_idx * dt,
                runner_name=cur_runner,
                job_label=cur_label,
            )
        )
        cur_runner, cur_label = r, l
        slice_start = tick_idx
    merged_slices.append(
        ExecutionSlice(
            start_time=slice_start * dt,
            end_time=len(raw_slices) * dt,
            runner_name=cur_runner,
            job_label=cur_label,
        )
    )

  # Calculate total execution times
  cpu_totals: dict[str, float] = collections.defaultdict(float)
  for sl in merged_slices:
    cpu_totals[sl.runner_name] += sl.end_time - sl.start_time

  sched_report = compute_schedulability(
      periodic_tasks, server_config, job_records
  )

  return SimulationResult(
      slices=merged_slices,
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
# Visualización Interactiva (Matplotlib con Hover + Exportación HTML5)
# ==============================================================================


def plot_simulation(
    result: SimulationResult,
    output_path: str = "sporadic_server_simulation.png",
    show: bool = True,
    horizon: float = SIMULATION_HORIZON,
    periodic_tasks: Optional[list[PeriodicTask]] = None,
    server_config: Optional[SporadicServerConfig] = None,
    interactive_html: Optional[str] = None,
) -> None:
  """Genera el diagrama de Gantt estilo Buttazzo con inspección interactiva por hover.

  Args:
    result: Objeto SimulationResult con las trazas y métricas de la simulación.
    output_path: Ruta de archivo donde se guardará la figura PNG en alta resolución.
    show: Si True y existe interfaz gráfica disponible, despliega la ventana interactiva.
    horizon: Horizonte temporal total a graficar.
    periodic_tasks: Configuración de tareas periódicas.
    server_config: Configuración del Servidor Esporádico.
    interactive_html: Ruta opcional para exportar el dashboard HTML interactivo.
  """
  if not show or not os.environ.get("DISPLAY"):
    plt.switch_backend("Agg")

  if periodic_tasks is None:
    periodic_tasks = PERIODIC_TASKS
  if server_config is None:
    server_config = SERVER_CONFIG

  # Orden de pistas: Tareas periódicas arriba por prioridad decreciente, Servidor abajo
  ordered_tracks = sorted(
      periodic_tasks, key=lambda t: t.priority, reverse=True
  ) + [server_config]
  track_names = [t.name for t in ordered_tracks]
  track_colors = {t.name: t.color for t in ordered_tracks}

  # Separación vertical de pistas: S_s en la parte inferior (y = 0.0)
  track_spacing = 1.35
  y_coords = {
      name: idx * track_spacing
      for idx, name in enumerate(reversed(track_names))
  }

  fig, (ax_gantt, ax_budget) = plt.subplots(
      2,
      1,
      figsize=(15, 9.5),
      sharex=True,
      gridspec_kw={"height_ratios": [3.2, 1.1], "hspace": 0.12},
  )

  # --- Subgráfico Superior: Diagrama de Gantt de Ejecución ---
  bar_height = 0.55
  slice_artist_map: list[tuple[Any, ExecutionSlice]] = []

  for sl in result.slices:
    if sl.runner_name in y_coords:
      y = y_coords[sl.runner_name] - bar_height / 2
      color = track_colors[sl.runner_name]
      rects = ax_gantt.broken_barh(
          [(sl.start_time, sl.end_time - sl.start_time)],
          (y, bar_height),
          facecolors=color,
          edgecolor="black",
          linewidth=0.8,
      )
      slice_artist_map.append((rects, sl))
      if sl.job_label:
        mid_x = (sl.start_time + sl.end_time) / 2
        ax_gantt.text(
            mid_x,
            y + bar_height / 2,
            sl.job_label,
            ha="center",
            va="center",
            color="white",
            fontweight="bold",
            fontsize=9,
        )

  # Mapear liberaciones y plazos para desplazar flechas coincidentes
  release_times_by_task = collections.defaultdict(set)
  for t_rel, name in result.periodic_release_arrows:
    release_times_by_task[name].add(round(t_rel, 4))

  deadline_times_by_task = collections.defaultdict(set)
  for t_dl, name in result.periodic_deadline_arrows:
    deadline_times_by_task[name].add(round(t_dl, 4))

  # Flechas de liberación y plazos (deadlines)
  for t_rel, name in result.periodic_release_arrows:
    if name in y_coords and t_rel <= horizon:
      y = y_coords[name]
      x_rel = (
          t_rel - 0.08
          if round(t_rel, 4) in deadline_times_by_task[name]
          else t_rel
      )
      ax_gantt.annotate(
          "",
          xy=(x_rel, y + bar_height / 2),
          xytext=(x_rel, y - bar_height / 2 - 0.25),
          arrowprops=dict(arrowstyle="->", color="black", lw=1.2),
      )

  for t_arr, label in result.aperiodic_arrival_arrows:
    if server_config.name in y_coords and t_arr <= horizon:
      y = y_coords[server_config.name]
      ax_gantt.annotate(
          "",
          xy=(t_arr, y + bar_height / 2),
          xytext=(t_arr, y - bar_height / 2 - 0.25),
          arrowprops=dict(arrowstyle="->", color="#c0392b", lw=1.2),
      )
      c_time = result.job_records[label].job.compute_time
      c_str = f"{c_time:g}"
      ax_gantt.text(
          t_arr,
          y - bar_height / 2 - 0.35,
          f"{label} ({c_str})",
          ha="center",
          va="top",
          color="#c0392b",
          fontsize=7.5,
          fontweight="bold",
      )

  for t_dl, name in result.periodic_deadline_arrows:
    if name in y_coords and t_dl <= horizon:
      y = y_coords[name]
      x_dl = (
          t_dl + 0.08 if round(t_dl, 4) in release_times_by_task[name] else t_dl
      )
      ax_gantt.annotate(
          "",
          xy=(x_dl, y - bar_height / 2),
          xytext=(x_dl, y + bar_height / 2 + 0.25),
          arrowprops=dict(
              arrowstyle="->", color="black", lw=1.0, linestyle=":"
          ),
      )

  # Líneas verticales punteadas para ventanas de activación t_A (Buttazzo Cap. 5)
  for act in result.activations:
    if act.activation_time <= horizon:
      ax_gantt.plot(
          [act.activation_time, act.activation_time],
          [-1.05, bar_height / 2],
          linestyle=":",
          color="#555555",
          lw=1.0,
          alpha=0.8,
      )
      ax_budget.axvline(
          x=act.activation_time,
          linestyle=":",
          color="#555555",
          lw=1.0,
          alpha=0.8,
      )
    if act.closed_time <= horizon:
      ax_gantt.plot(
          [act.closed_time, act.closed_time],
          [-1.05, bar_height / 2],
          linestyle=":",
          color="#555555",
          lw=1.0,
          alpha=0.8,
      )
      ax_budget.axvline(
          x=act.closed_time,
          linestyle=":",
          color="#555555",
          lw=1.0,
          alpha=0.8,
      )

  # Marcadores de recarga programada t_R
  for repl in result.replenishments:
    if repl.replenishment_time <= horizon:
      ax_gantt.plot(
          [repl.replenishment_time, repl.replenishment_time],
          [-1.05, bar_height / 2],
          linestyle="--",
          color="#c0392b",
          lw=1.2,
          alpha=0.7,
      )
      ax_budget.axvline(
          x=repl.replenishment_time,
          color="#c0392b",
          linestyle="--",
          linewidth=1.2,
          alpha=0.7,
      )
      t_a = repl.replenishment_time - server_config.period
      if t_a >= 0:
        ax_budget.annotate(
            f"+{repl.amount:.1f}",
            xy=(repl.replenishment_time, server_config.budget * 0.7),
            xytext=(t_a, server_config.budget + 0.18),
            arrowprops=dict(
                arrowstyle="->",
                linestyle=":",
                color="#c0392b",
                lw=1.1,
            ),
            color="#c0392b",
            fontweight="bold",
            fontsize=8,
            ha="center",
            va="bottom",
        )

  # Leyenda descriptiva del diagrama de Gantt
  legend_handles = [
      mpatches.Patch(
          facecolor=track_colors[name],
          edgecolor="black",
          label=name,
      )
      for name in track_names
  ] + [
      mlines.Line2D(
          [],
          [],
          color="black",
          marker=r"$\uparrow$",
          markersize=9,
          linestyle="None",
          label="Liberación Periódica",
      ),
      mlines.Line2D(
          [],
          [],
          color="black",
          marker=r"$\downarrow$",
          markersize=9,
          linestyle="None",
          label="Plazo (Deadline)",
      ),
      mlines.Line2D(
          [],
          [],
          color="#c0392b",
          marker=r"$\uparrow$",
          markersize=9,
          linestyle="None",
          label="Llegada Aperiódica",
      ),
  ]

  ax_gantt.set_yticks([y_coords[n] for n in track_names])
  ax_gantt.set_yticklabels(track_names, fontsize=11, fontweight="bold")
  ax_gantt.set_ylim(-1.05, (len(track_names) - 1) * track_spacing + 0.85)
  ax_gantt.set_ylabel("Tareas", fontsize=11, fontweight="bold")
  ax_gantt.set_title(
      "Simulación del Servidor Esporádico (SS) bajo Rate-Monotonic",
      fontsize=14,
      fontweight="bold",
      pad=44,
  )
  ax_gantt.legend(
      handles=legend_handles,
      loc="lower center",
      bbox_to_anchor=(0.5, 1.02),
      ncol=5,
      fontsize=8.5,
      framealpha=0.95,
  )
  ax_gantt.set_xticks(list(range(0, int(horizon) + 1, 2)))
  ax_gantt.set_xticks(list(range(0, int(horizon) + 1, 1)), minor=True)
  ax_gantt.grid(True, which="both", axis="x", linestyle=":", alpha=0.5)

  # --- Subgráfico Inferior: Curva de Presupuesto C_s(t) ---
  ax_budget.step(
      result.budget_trace_times,
      result.budget_trace_values,
      where="post",
      color="#16a085",
      linewidth=1.8,
      label=r"Presupuesto $C_s(t)$",
  )
  ax_budget.fill_between(
      result.budget_trace_times,
      result.budget_trace_values,
      step="post",
      color="#16a085",
      alpha=0.15,
  )

  ax_budget.set_xlim(0, horizon)
  ax_budget.set_ylim(-0.2, server_config.budget + 0.4)
  ax_budget.set_yticks([float(i) for i in range(int(server_config.budget) + 1)])
  ax_budget.set_xticks(list(range(0, int(horizon) + 1, 2)))
  ax_budget.set_xticks(list(range(0, int(horizon) + 1, 1)), minor=True)
  ax_budget.set_xlabel("Tiempo (t)", fontsize=11, fontweight="bold")
  ax_budget.set_ylabel(r"Presupuesto $C_s$", fontsize=11, fontweight="bold")
  ax_budget.grid(True, which="both", axis="x", linestyle=":", alpha=0.5)
  ax_budget.legend(loc="upper right", framealpha=0.9)
  plt.subplots_adjust(top=0.88, bottom=0.08, left=0.07, right=0.97)

  # Gancho de tooltip interactivo para la GUI de Matplotlib
  hover_annotation = ax_gantt.annotate(
      "",
      xy=(0, 0),
      xytext=(15, 15),
      textcoords="offset points",
      bbox=dict(boxstyle="round,pad=0.5", fc="#2c3e50", ec="none", alpha=0.9),
      color="white",
      fontsize=8.5,
      fontweight="bold",
      visible=False,
      zorder=100,
  )

  def on_hover(event):
    if event.inaxes == ax_gantt and event.xdata is not None and event.ydata is not None:
      x, y = event.xdata, event.ydata
      for _, sl in slice_artist_map:
        if sl.runner_name in y_coords:
          sl_y = y_coords[sl.runner_name]
          if (
              sl.start_time <= x <= sl.end_time
              and (sl_y - bar_height / 2) <= y <= (sl_y + bar_height / 2)
          ):
            job_info = f" | Trabajo: {sl.job_label}" if sl.job_label else ""
            hover_annotation.xy = (x, y)
            hover_annotation.set_text(
                f"{sl.runner_name}{job_info}\nIntervalo: [{sl.start_time:.1f}, {sl.end_time:.1f}]\nDuración: {sl.end_time - sl.start_time:.1f} u"
            )
            hover_annotation.set_visible(True)
            fig.canvas.draw_idle()
            return
      if hover_annotation.get_visible():
        hover_annotation.set_visible(False)
        fig.canvas.draw_idle()

  fig.canvas.mpl_connect("motion_notify_event", on_hover)

  plt.savefig(output_path, dpi=300, bbox_inches="tight")
  print(f"[+] Figura de simulación guardada exitosamente en: {output_path}")

  # Exportar dashboard interactivo HTML si se solicita
  if interactive_html:
    export_interactive_html(
        result,
        interactive_html,
        horizon=horizon,
        periodic_tasks=periodic_tasks,
        server_config=server_config,
    )

  if show and os.environ.get("DISPLAY"):
    plt.show()
  plt.close(fig)


# ==============================================================================
# Generador del Dashboard HTML5 / SVG Interactivo y Animado
# ==============================================================================


def export_interactive_html(
    result: SimulationResult,
    output_path: str = "sporadic_server_interactive.html",
    horizon: float = SIMULATION_HORIZON,
    periodic_tasks: Optional[list[PeriodicTask]] = None,
    server_config: Optional[SporadicServerConfig] = None,
) -> None:
  """Genera un dashboard interactivo en HTML5/SVG totalmente autónomo (cero dependencias)."""
  if periodic_tasks is None:
    periodic_tasks = PERIODIC_TASKS
  if server_config is None:
    server_config = SERVER_CONFIG

  ordered_tracks = sorted(
      periodic_tasks, key=lambda t: t.priority, reverse=True
  ) + [server_config]
  track_names = [t.name.replace("$", "").replace("\\", "") for t in ordered_tracks]
  track_colors = {
      t.name.replace("$", "").replace("\\", ""): t.color for t in ordered_tracks
  }
  server_clean_name = server_config.name.replace("$", "").replace("\\", "")
  task_priorities = {
      t.name.replace("$", "").replace("\\", ""): t.priority
      for t in ordered_tracks
  }

  slices_data = [
      {
          "start": sl.start_time,
          "end": sl.end_time,
          "duration": round(sl.end_time - sl.start_time, 2),
          "runner": sl.runner_name.replace("$", "").replace("\\", ""),
          "job": sl.job_label,
          "color": track_colors.get(
              sl.runner_name.replace("$", "").replace("\\", ""), "#7f8c8d"
          ),
      }
      for sl in result.slices
      if sl.runner_name != "Idle"
  ]

  budget_data = [
      {"time": t, "budget": b}
      for t, b in zip(result.budget_trace_times, result.budget_trace_values)
  ]

  activations_data = [
      {
          "t_A": act.activation_time,
          "end": act.closed_time,
          "consumed": act.amount_consumed,
          "reason": act.reason,
      }
      for act in result.activations
  ]

  arrivals_data = [
      {
          "time": t_arr,
          "job": label,
          "compute": result.job_records[label].job.compute_time,
      }
      for t_arr, label in result.aperiodic_arrival_arrows
  ]

  releases_data = [
      {"time": t_rel, "task": name.replace("$", "").replace("\\", "")}
      for t_rel, name in result.periodic_release_arrows
      if t_rel <= horizon
  ]

  deadlines_data = [
      {"time": t_dl, "task": name.replace("$", "").replace("\\", "")}
      for t_dl, name in result.periodic_deadline_arrows
      if t_dl <= horizon
  ]

  replenishments_data = [
      {
          "t_R": r.replenishment_time,
          "amount": r.amount,
          "t_A": round(r.replenishment_time - server_config.period, 2),
      }
      for r in result.replenishments
  ]

  aperiodic_jobs_data = [
      {
          "name": name,
          "arrival": rec.job.arrival_time,
          "compute": rec.job.compute_time,
          "completion": rec.completion_time,
          "response": rec.response_time,
          "waiting": rec.waiting_time,
          "status": "Completed" if rec.completion_time else "Incomplete",
      }
      for name, rec in sorted(result.job_records.items())
  ]

  sched_data = {}
  if result.schedulability:
    s = result.schedulability
    sched_data = {
        "u_p": round(s.utilization_periodic, 4),
        "u_s": round(s.utilization_server, 4),
        "u_total": round(s.utilization_total, 4),
        "ll_bound": round(s.liu_layland_bound, 4),
        "all_sched": s.all_schedulable,
        "avg_resp": round(s.avg_aperiodic_response_time, 2)
        if s.avg_aperiodic_response_time
        else "N/A",
        "max_resp": round(s.max_aperiodic_response_time, 2)
        if s.max_aperiodic_response_time
        else "N/A",
    }

  rows_html = []
  for j in aperiodic_jobs_data:
    comp_str = f"{j['completion']:.1f}" if j["completion"] is not None else "—"
    resp_str = f"{j['response']:.1f}" if j["response"] is not None else "—"
    wait_str = f"{j['waiting']:.1f}" if j["waiting"] is not None else "—"
    rows_html.append(
        f"<tr><td><b>{j['name']}</b></td><td>{j['arrival']:.1f}</td><td>{j['compute']:.1f}</td><td>{comp_str}</td><td>{resp_str}</td><td>{wait_str}</td><td><span class='badge badge-ok'>{j['status']}</span></td></tr>"
    )
  table_rows = "".join(rows_html)

  html_template = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>Simulador Interactivo con Animación - Sporadic Server (SS)</title>
  <style>
    :root {{
      --primary: #16a085;
      --bg: #0f172a;
      --panel: #1e293b;
      --panel-hover: #24344d;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --border: #334155;
      --accent: #38bdf8;
      --danger: #ef4444;
      --warning: #f59e0b;
      --success: #10b981;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      margin: 0;
      padding: 20px;
      background: var(--bg);
      color: var(--text);
    }}
    h1 {{
      margin: 0 0 4px 0;
      font-size: 22px;
      color: var(--accent);
    }}
    .subtitle {{
      color: var(--text-muted);
      font-size: 13px;
      margin-bottom: 16px;
    }}
    /* Animation Control Panel */
    .player-panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 14px 18px;
      margin-bottom: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }}
    .player-row-top {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }}
    .btn-group {{
      display: flex;
      gap: 6px;
      align-items: center;
    }}
    .btn {{
      background: #334155;
      color: var(--text);
      border: 1px solid #475569;
      border-radius: 6px;
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      transition: all 0.15s ease;
    }}
    .btn:hover {{ background: #475569; border-color: var(--accent); }}
    .btn-primary {{
      background: #0284c7;
      border-color: #38bdf8;
    }}
    .btn-primary:hover {{ background: #0369a1; }}
    .btn-active {{
      background: var(--primary);
      border-color: #34d399;
      color: #fff;
    }}
    .time-readout {{
      font-size: 16px;
      font-weight: bold;
      color: var(--accent);
      font-variant-numeric: tabular-nums;
      background: #0f172a;
      padding: 4px 10px;
      border-radius: 6px;
      border: 1px solid var(--border);
    }}
    .slider-container {{
      display: flex;
      align-items: center;
      gap: 12px;
      width: 100%;
    }}
    .time-slider {{
      flex: 1;
      height: 8px;
      border-radius: 4px;
      background: #334155;
      outline: none;
      -webkit-appearance: none;
      cursor: pointer;
    }}
    .time-slider::-webkit-slider-thumb {{
      -webkit-appearance: none;
      width: 18px;
      height: 18px;
      border-radius: 50%;
      background: var(--accent);
      border: 2px solid #ffffff;
      cursor: pointer;
      box-shadow: 0 0 8px rgba(56, 189, 248, 0.6);
    }}
    /* Live HUD */
    .hud-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }}
    .hud-card {{
      background: var(--panel);
      padding: 10px 14px;
      border-radius: 8px;
      border: 1px solid var(--border);
    }}
    .hud-title {{
      font-size: 10.5px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      color: var(--text-muted);
      margin-bottom: 4px;
    }}
    .hud-value {{
      font-size: 15px;
      font-weight: bold;
      color: var(--text);
      display: flex;
      align-items: center;
      gap: 6px;
    }}
    .budget-bar-bg {{
      width: 100%;
      height: 7px;
      background: #334155;
      border-radius: 4px;
      margin-top: 6px;
      overflow: hidden;
    }}
    .budget-bar-fill {{
      height: 100%;
      background: linear-gradient(90deg, #10b981, #38bdf8);
      width: 100%;
      transition: width 0.1s linear;
    }}
    /* Canvas / Chart Container */
    .chart-container {{
      background: var(--panel);
      border-radius: 8px;
      padding: 16px;
      border: 1px solid var(--border);
      position: relative;
      margin-bottom: 20px;
    }}
    svg {{
      width: 100%;
      height: 480px;
      display: block;
    }}
    .slice-bar {{
      cursor: pointer;
      transition: opacity 0.15s;
    }}
    .slice-bar:hover {{
      opacity: 0.85;
      stroke: #ffffff;
      stroke-width: 1.5px;
    }}
    .cursor-line {{
      stroke: #f43f5e;
      stroke-width: 2px;
      stroke-dasharray: 3,3;
      pointer-events: none;
    }}
    .tooltip {{
      position: absolute;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid #38bdf8;
      padding: 8px 12px;
      border-radius: 6px;
      font-size: 11.5px;
      pointer-events: none;
      display: none;
      box-shadow: 0 4px 12px rgba(0,0,0,0.5);
      z-index: 1000;
    }}
    .tooltip b {{ color: var(--accent); }}
    /* Table Styling */
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 14px;
      font-size: 12px;
    }}
    th, td {{
      padding: 8px 12px;
      text-align: left;
      border-bottom: 1px solid var(--border);
    }}
    th {{
      background: #0f172a;
      color: var(--text-muted);
      font-weight: 600;
    }}
    tr.job-active td {{ background: rgba(56, 189, 248, 0.15); }}
    tr.job-done td {{ opacity: 0.9; }}
    .badge {{
      display: inline-block;
      padding: 2px 7px;
      border-radius: 4px;
      font-size: 10.5px;
      font-weight: bold;
    }}
    .badge-ok {{ background: #065f46; color: #34d399; }}
    .badge-running {{ background: #075985; color: #38bdf8; }}
    .badge-waiting {{ background: #78350f; color: #fbbf24; }}
    .badge-suspended {{ background: #7f1d1d; color: #f87171; }}
  </style>
</head>
<body>
  <h1>Simulador Interactivo con Animación: Sporadic Server (SS) bajo RM</h1>
  <div class="subtitle">Visualización dinámica paso a paso de llegada de tareas, ejecución en CPU, consumo y recargas de presupuesto $C_s(t)$</div>

  <!-- Panel de Control de Animación -->
  <div class="player-panel">
    <div class="player-row-top">
      <div class="btn-group">
        <button id="btnPlay" class="btn btn-primary">▶ Reproducir</button>
        <button id="btnStepBack" class="btn">⏮ -0.5s</button>
        <button id="btnStepFwd" class="btn">⏭ +0.5s</button>
        <button id="btnReset" class="btn">⟲ Reiniciar</button>
      </div>

      <div class="btn-group">
        <span style="font-size: 11px; color: var(--text-muted); margin-right: 4px;">Velocidad:</span>
        <button class="btn btn-speed" data-speed="0.5">0.5x</button>
        <button class="btn btn-speed btn-active" data-speed="1.0">1.0x</button>
        <button class="btn btn-speed" data-speed="2.0">2.0x</button>
        <button class="btn btn-speed" data-speed="4.0">4.0x</button>
      </div>

      <div class="time-readout">
        t = <span id="timeVal">30.0</span> / {horizon:.1f} s
      </div>
    </div>

    <div class="slider-container">
      <span style="font-size: 11px; color: var(--text-muted);">0.0s</span>
      <input type="range" id="timeSlider" class="time-slider" min="0" max="{horizon}" step="0.1" value="{horizon}">
      <span style="font-size: 11px; color: var(--text-muted);">{horizon:.1f}s</span>
    </div>
  </div>

  <!-- HUD Dinámico en Vivo -->
  <div class="hud-grid">
    <div class="hud-card">
      <div class="hud-title">Ejecutor en CPU</div>
      <div id="hudRunner" class="hud-value"><span class="badge badge-ok">Idle</span></div>
    </div>
    <div class="hud-card">
      <div class="hud-title">Estado del Servidor $S_s$</div>
      <div id="hudServerStatus" class="hud-value"><span class="badge badge-ok">OCIOSO</span></div>
    </div>
    <div class="hud-card">
      <div class="hud-title">Presupuesto $C_s(t)$</div>
      <div id="hudBudget" class="hud-value">2.00 / 2.0 u</div>
      <div class="budget-bar-bg">
        <div id="budgetFill" class="budget-bar-fill" style="width: 100%;"></div>
      </div>
    </div>
    <div class="hud-card">
      <div class="hud-title">Cola Aperiódica Activa</div>
      <div id="hudQueue" class="hud-value" style="font-size: 12.5px; font-weight: normal;">Vacía</div>
    </div>
    <div class="hud-card">
      <div class="hud-title">Próxima Recarga ($t_R$)</div>
      <div id="hudNextRepl" class="hud-value" style="font-size: 12.5px; font-weight: normal;">Ninguna</div>
    </div>
  </div>

  <!-- Gráfica SVG -->
  <div class="chart-container">
    <div id="tooltip" class="tooltip"></div>
    <svg id="ganttSvg" viewBox="0 0 1000 450"></svg>
  </div>

  <h3>Detalle de Trabajos Aperiódicos</h3>
  <table>
    <thead>
      <tr>
        <th>Trabajo</th>
        <th>Llegada (r_i)</th>
        <th>Cómputo (C_i)</th>
        <th>Finalización (f_i)</th>
        <th>Tiempo Respuesta (W_i)</th>
        <th>Tiempo Espera (Z_i)</th>
        <th>Estado</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>

  <script>
    const slices = {json.dumps(slices_data)};
    const budgetTrace = {json.dumps(budget_data)};
    const arrivals = {json.dumps(arrivals_data)};
    const releases = {json.dumps(releases_data)};
    const deadlines = {json.dumps(deadlines_data)};
    const replenishments = {json.dumps(replenishments_data)};
    const jobs = {json.dumps(aperiodic_jobs_data)};
    const tracks = {json.dumps(track_names)};
    const horizon = {horizon};
    const serverName = {json.dumps(server_clean_name)};
    const serverBudgetMax = {server_config.budget};
    const serverPriority = {server_config.priority};
    const taskPriorities = {json.dumps(task_priorities)};

    const svg = document.getElementById("ganttSvg");
    const tooltip = document.getElementById("tooltip");
    const slider = document.getElementById("timeSlider");
    const timeVal = document.getElementById("timeVal");
    const btnPlay = document.getElementById("btnPlay");
    const btnReset = document.getElementById("btnReset");
    const btnStepBack = document.getElementById("btnStepBack");
    const btnStepFwd = document.getElementById("btnStepFwd");
    const speedButtons = document.querySelectorAll(".btn-speed");

    const hudRunner = document.getElementById("hudRunner");
    const hudServerStatus = document.getElementById("hudServerStatus");
    const hudBudget = document.getElementById("hudBudget");
    const budgetFill = document.getElementById("budgetFill");
    const hudQueue = document.getElementById("hudQueue");
    const hudNextRepl = document.getElementById("hudNextRepl");

    const margin = {{ top: 25, right: 30, bottom: 25, left: 70 }};
    const width = 1000 - margin.left - margin.right;
    const height = 450 - margin.top - margin.bottom;

    const ganttHeight = height * 0.70;
    const budgetHeight = height * 0.22;
    const budgetY = margin.top + ganttHeight + 25;

    const xScale = (t) => margin.left + (t / horizon) * width;
    const trackHeight = ganttHeight / tracks.length;

    // Elementos estáticos de la gráfica
    tracks.forEach((track, idx) => {{
      const y = margin.top + idx * trackHeight;
      const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      bg.setAttribute("x", margin.left);
      bg.setAttribute("y", y);
      bg.setAttribute("width", width);
      bg.setAttribute("height", trackHeight);
      bg.setAttribute("fill", idx % 2 === 0 ? "#1e293b" : "#172033");
      svg.appendChild(bg);

      const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
      text.setAttribute("x", margin.left - 12);
      text.setAttribute("y", y + trackHeight / 2 + 4);
      text.setAttribute("fill", "#94a3b8");
      text.setAttribute("font-size", "11");
      text.setAttribute("font-weight", "bold");
      text.setAttribute("text-anchor", "end");
      text.textContent = track;
      svg.appendChild(text);
    }});

    // Rejilla y marcas temporales
    for (let t = 0; t <= horizon; t += 2) {{
      const x = xScale(t);
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("x1", x);
      line.setAttribute("y1", margin.top);
      line.setAttribute("x2", x);
      line.setAttribute("y2", budgetY + budgetHeight);
      line.setAttribute("stroke", "#334155");
      line.setAttribute("stroke-dasharray", "2,2");
      svg.appendChild(line);

      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("x", x);
      label.setAttribute("y", budgetY + budgetHeight + 14);
      label.setAttribute("fill", "#94a3b8");
      label.setAttribute("font-size", "10");
      label.setAttribute("text-anchor", "middle");
      label.textContent = t;
      svg.appendChild(label);
    }}

    // Grupo para elementos dinámicos
    const dynamicGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
    svg.appendChild(dynamicGroup);

    // Línea de cursor de tiempo
    const cursorLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
    cursorLine.setAttribute("class", "cursor-line");
    cursorLine.setAttribute("y1", margin.top);
    cursorLine.setAttribute("y2", budgetY + budgetHeight);
    svg.appendChild(cursorLine);

    // Variables de control de animación
    let currentTime = parseFloat(slider.value);
    let isPlaying = false;
    let animSpeed = 1.0;
    let lastTimestamp = null;

    function getBudgetAt(t) {{
      let currentB = serverBudgetMax;
      for (let i = 0; i < budgetTrace.length; i++) {{
        if (budgetTrace[i].time <= t + 0.001) {{
          currentB = budgetTrace[i].budget;
        }} else {{
          break;
        }}
      }}
      return currentB;
    }}

    function updateSimulationView(t) {{
      timeVal.textContent = t.toFixed(1);
      slider.value = t;

      // Mover cursor temporal
      const cursorX = xScale(t);
      cursorLine.setAttribute("x1", cursorX);
      cursorLine.setAttribute("x2", cursorX);

      // Limpiar grupo dinámico
      while (dynamicGroup.firstChild) {{
        dynamicGroup.removeChild(dynamicGroup.firstChild);
      }}

      // 1. Dibujar bloques de ejecución recortados a t
      let activeRunner = "Idle";
      let activeJob = "";

      slices.forEach((sl) => {{
        if (sl.start < t) {{
          const trackIdx = tracks.indexOf(sl.runner);
          if (trackIdx !== -1) {{
            const visibleEnd = Math.min(sl.end, t);
            const visibleDuration = visibleEnd - sl.start;
            if (visibleDuration > 0.001) {{
              const x = xScale(sl.start);
              const w = (visibleDuration / horizon) * width;
              const y = margin.top + trackIdx * trackHeight + 4;
              const h = trackHeight - 8;

              const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
              rect.setAttribute("class", "slice-bar");
              rect.setAttribute("x", x);
              rect.setAttribute("y", y);
              rect.setAttribute("width", Math.max(w, 1.5));
              rect.setAttribute("height", h);
              rect.setAttribute("fill", sl.color);
              rect.setAttribute("rx", "3");

              rect.addEventListener("mousemove", (e) => {{
                tooltip.style.display = "block";
                tooltip.style.left = (e.pageX + 12) + "px";
                tooltip.style.top = (e.pageY - 20) + "px";
                tooltip.innerHTML = `<b>${{sl.runner}}</b> ${{sl.job ? "| Trabajo: <b>" + sl.job + "</b>" : ""}}<br/>` +
                                     `Intervalo: [${{sl.start.toFixed(1)}}, ${{sl.end.toFixed(1)}}]<br/>` +
                                     `Duración: ${{sl.duration.toFixed(1)}} u`;
              }});
              rect.addEventListener("mouseleave", () => tooltip.style.display = "none");
              dynamicGroup.appendChild(rect);

              if (sl.job && w > 14) {{
                const jobText = document.createElementNS("http://www.w3.org/2000/svg", "text");
                jobText.setAttribute("x", x + w / 2);
                jobText.setAttribute("y", y + h / 2 + 3);
                jobText.setAttribute("fill", "#ffffff");
                jobText.setAttribute("font-size", "9.5");
                jobText.setAttribute("font-weight", "bold");
                jobText.setAttribute("text-anchor", "middle");
                jobText.style.pointerEvents = "none";
                jobText.textContent = sl.job;
                dynamicGroup.appendChild(jobText);
              }}
            }}
          }}

          if (sl.start <= t && t < sl.end) {{
            activeRunner = sl.runner;
            activeJob = sl.job;
          }}
        }}
      }});

      // 2. Dibujar flechas de llegada aperiódicas para trabajos con arrival <= t
      arrivals.forEach((arr) => {{
        if (arr.time <= t) {{
          const x = xScale(arr.time);
          const ssIdx = tracks.indexOf(serverName);
          if (ssIdx !== -1) {{
            const y = margin.top + ssIdx * trackHeight + trackHeight;
            const arrow = document.createElementNS("http://www.w3.org/2000/svg", "line");
            arrow.setAttribute("x1", x);
            arrow.setAttribute("y1", y + 10);
            arrow.setAttribute("x2", x);
            arrow.setAttribute("y2", y - 2);
            arrow.setAttribute("stroke", "#c0392b");
            arrow.setAttribute("stroke-width", "1.6");
            dynamicGroup.appendChild(arrow);

            const txt = document.createElementNS("http://www.w3.org/2000/svg", "text");
            txt.setAttribute("x", x);
            txt.setAttribute("y", y + 20);
            txt.setAttribute("fill", "#c0392b");
            txt.setAttribute("font-size", "8.5");
            txt.setAttribute("font-weight", "bold");
            txt.setAttribute("text-anchor", "middle");
            txt.textContent = `${{arr.job}}(${{arr.compute}})`;
            dynamicGroup.appendChild(txt);
          }}
        }}
      }});

      // 3. Dibujar marcadores de recargas pasadas (t_R <= t)
      replenishments.forEach((repl) => {{
        if (repl.t_R <= t) {{
          const x = xScale(repl.t_R);
          const ssIdx = tracks.indexOf(serverName);
          const rLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
          rLine.setAttribute("x1", x);
          rLine.setAttribute("y1", margin.top + (ssIdx !== -1 ? ssIdx * trackHeight : 0));
          rLine.setAttribute("x2", x);
          rLine.setAttribute("y2", budgetY + budgetHeight);
          rLine.setAttribute("stroke", "#10b981");
          rLine.setAttribute("stroke-dasharray", "3,2");
          rLine.setAttribute("stroke-width", "1.2");
          dynamicGroup.appendChild(rLine);
        }}
      }});

      // 4. Dibujar traza de presupuesto C_s(t) hasta el instante t
      let pathD = "";
      let pts = budgetTrace.filter(pt => pt.time <= t + 0.001);
      if (pts.length === 0) {{
        pts = [{{ time: 0, budget: serverBudgetMax }}];
      }}
      // Punto actual proyectado a t
      const currentB = getBudgetAt(t);
      pts.push({{ time: t, budget: currentB }});

      pts.forEach((pt, i) => {{
        const x = xScale(pt.time);
        const y = budgetY + budgetHeight - (pt.budget / serverBudgetMax) * budgetHeight;
        if (i === 0) pathD += `M ${{x}} ${{y}}`;
        else pathD += ` L ${{x}} ${{y}}`;
      }});

      const budgetPath = document.createElementNS("http://www.w3.org/2000/svg", "path");
      budgetPath.setAttribute("d", pathD);
      budgetPath.setAttribute("fill", "none");
      budgetPath.setAttribute("stroke", "#16a085");
      budgetPath.setAttribute("stroke-width", "2.5");
      dynamicGroup.appendChild(budgetPath);

      // Etiqueta presupuesto fija
      const bLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
      bLabel.setAttribute("x", margin.left - 12);
      bLabel.setAttribute("y", budgetY + budgetHeight / 2 + 4);
      bLabel.setAttribute("fill", "#16a085");
      bLabel.setAttribute("font-size", "11");
      bLabel.setAttribute("font-weight", "bold");
      bLabel.setAttribute("text-anchor", "end");
      bLabel.textContent = "C_s(t)";
      dynamicGroup.appendChild(bLabel);

      // 5. Actualizar HUD
      if (activeRunner === "Idle") {{
        hudRunner.innerHTML = `<span class="badge" style="background: #334155; color: #94a3b8;">Ocioso (Idle)</span>`;
      }} else {{
        hudRunner.innerHTML = `<span class="badge badge-running">${{activeRunner}}${{activeJob ? " (" + activeJob + ")" : ""}}</span>`;
      }}

      // Calcular cola activa en t
      const activeQueue = [];
      jobs.forEach(j => {{
        if (j.arrival <= t) {{
          // Calcular cuánto ha corrido hasta t
          let executed = 0;
          slices.forEach(sl => {{
            if (sl.job === j.name && sl.start < t) {{
              executed += Math.max(0, Math.min(sl.end, t) - sl.start);
            }}
          }});
          const rem = Math.max(0, j.compute - executed);
          if (rem > 0.001) {{
            activeQueue.push(`${{j.name}} (${{rem.toFixed(1)}}u)`);
          }}
        }}
      }});

      if (activeQueue.length > 0) {{
        hudQueue.innerHTML = `<span class="badge badge-waiting">${{activeQueue.join(", ")}}</span>`;
      }} else {{
        hudQueue.textContent = "Vacía";
      }}

      // Estado dinámico del Servidor Esporádico
      const isServerExecuting = (activeRunner === serverName || activeRunner === "S_s");

      if (isServerExecuting) {{
        hudServerStatus.innerHTML = `<span class="badge badge-running">EJECUTANDO (-1/s)</span>`;
      }} else if (currentB <= 0.001) {{
        hudServerStatus.innerHTML = `<span class="badge badge-suspended">SUSPENDIDO (C_s=0)</span>`;
      }} else if (activeQueue.length === 0) {{
        hudServerStatus.innerHTML = `<span class="badge badge-ok">OCIOSO (Listo)</span>`;
      }} else {{
        // Hay peticiones en cola (activeQueue.length > 0) y presupuesto disponible (currentB > 0)
        const runnerPrio = taskPriorities[activeRunner] || 0;
        if (activeRunner !== "Idle" && runnerPrio > serverPriority) {{
          hudServerStatus.innerHTML = `<span class="badge badge-waiting">DESALOJADO (por ${{activeRunner}})</span>`;
        }} else if (activeRunner === "Idle") {{
          // Si el procesador está ocioso, el servidor listo no está siendo desalojado
          hudServerStatus.innerHTML = `<span class="badge badge-ok">OCIOSO (Listo)</span>`;
        }} else {{
          hudServerStatus.innerHTML = `<span class="badge badge-waiting">LISTO EN ESPERA</span>`;
        }}
      }}

      // Presupuesto
      hudBudget.textContent = `${{currentB.toFixed(2)}} / ${{serverBudgetMax.toFixed(1)}} u`;
      budgetFill.style.width = `${{Math.min(100, Math.max(0, (currentB / serverBudgetMax) * 100))}}%`;

      // Próxima recarga
      const nextR = replenishments.find(r => r.t_R > t + 0.001);
      if (nextR) {{
        const inSec = (nextR.t_R - t).toFixed(1);
        hudNextRepl.innerHTML = `<span style="color: #34d399;">+${{nextR.amount.toFixed(1)}} a t=${{nextR.t_R.toFixed(1)}}s (en ${{inSec}}s)</span>`;
      }} else {{
        hudNextRepl.textContent = "Ninguna restante";
      }}
    }}

    // Loop de animación con requestAnimationFrame
    function animationLoop(timestamp) {{
      if (!isPlaying) return;
      if (!lastTimestamp) lastTimestamp = timestamp;

      const delta = (timestamp - lastTimestamp) / 1000;
      lastTimestamp = timestamp;

      currentTime += delta * 2.0 * animSpeed; // 2 segundos virtuales por segundo real a 1x

      if (currentTime >= horizon) {{
        currentTime = horizon;
        isPlaying = false;
        btnPlay.textContent = "▶ Reproducir";
        btnPlay.classList.remove("btn-active");
      }}

      updateSimulationView(currentTime);

      if (isPlaying) {{
        requestAnimationFrame(animationLoop);
      }}
    }}

    // Event Listeners
    btnPlay.addEventListener("click", () => {{
      isPlaying = !isPlaying;
      if (isPlaying) {{
        if (currentTime >= horizon) currentTime = 0;
        lastTimestamp = null;
        btnPlay.textContent = "⏸ Pausar";
        btnPlay.classList.add("btn-active");
        requestAnimationFrame(animationLoop);
      }} else {{
        btnPlay.textContent = "▶ Reproducir";
        btnPlay.classList.remove("btn-active");
      }}
    }});

    btnReset.addEventListener("click", () => {{
      isPlaying = false;
      btnPlay.textContent = "▶ Reproducir";
      btnPlay.classList.remove("btn-active");
      currentTime = 0.0;
      updateSimulationView(currentTime);
    }});

    btnStepBack.addEventListener("click", () => {{
      isPlaying = false;
      btnPlay.textContent = "▶ Reproducir";
      btnPlay.classList.remove("btn-active");
      currentTime = Math.max(0, currentTime - 0.5);
      updateSimulationView(currentTime);
    }});

    btnStepFwd.addEventListener("click", () => {{
      isPlaying = false;
      btnPlay.textContent = "▶ Reproducir";
      btnPlay.classList.remove("btn-active");
      currentTime = Math.min(horizon, currentTime + 0.5);
      updateSimulationView(currentTime);
    }});

    slider.addEventListener("input", (e) => {{
      isPlaying = false;
      btnPlay.textContent = "▶ Reproducir";
      btnPlay.classList.remove("btn-active");
      currentTime = parseFloat(e.target.value);
      updateSimulationView(currentTime);
    }});

    speedButtons.forEach(btn => {{
      btn.addEventListener("click", () => {{
        speedButtons.forEach(b => b.classList.remove("btn-active"));
        btn.classList.add("btn-active");
        animSpeed = parseFloat(btn.dataset.speed);
      }});
    }});

    // Inicializar al final del horizonte por defecto
    updateSimulationView(horizon);
  </script>
</body>
</html>
"""
  with open(output_path, "w", encoding="utf-8") as f:
    f.write(html_template)
  print(f"[+] Interactive HTML dashboard generated at: {output_path}")


# ==============================================================================
# Exportadores a Formatos CSV y JSON
# ==============================================================================


def export_trace_to_csv(result: SimulationResult, output_path: str) -> None:
  """Exporta los intervalos de ejecución en procesador (CPU) a un archivo CSV.

  Args:
    result: Objeto SimulationResult obtenido tras la simulación.
    output_path: Ruta de destino del archivo CSV.
  """
  with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "indice_rebanada",
        "tiempo_inicio",
        "tiempo_fin",
        "duracion",
        "ejecutor",
        "etiqueta_trabajo",
    ])
    for idx, sl in enumerate(result.slices, 1):
      writer.writerow([
          idx,
          f"{sl.start_time:.2f}",
          f"{sl.end_time:.2f}",
          f"{sl.end_time - sl.start_time:.2f}",
          sl.runner_name.replace("$", "").replace("\\", ""),
          sl.job_label,
      ])
  print(f"[+] Traza de ejecución exportada a CSV: {output_path}")


def export_trace_to_json(result: SimulationResult, output_path: str) -> None:
  """Exporta todos los metadatos y trazas completas de la simulación a formato JSON.

  Args:
    result: Objeto SimulationResult con toda la información generada.
    output_path: Ruta de destino del archivo JSON.
  """
  data = {
      "slices": [
          {
              "start": sl.start_time,
              "end": sl.end_time,
              "runner": sl.runner_name,
              "job": sl.job_label,
          }
          for sl in result.slices
      ],
      "cpu_totals": result.cpu_totals,
      "activations": [
          {
              "t_A": act.activation_time,
              "end": act.closed_time,
              "consumed": act.amount_consumed,
              "reason": act.reason,
          }
          for act in result.activations
      ],
      "replenishments": [
          {"t_R": r.replenishment_time, "amount": r.amount}
          for r in result.replenishments
      ],
      "job_completions": {
          name: {
              "arrival": rec.job.arrival_time,
              "compute": rec.job.compute_time,
              "completion": rec.completion_time,
              "response_time": rec.response_time,
              "waiting_time": rec.waiting_time,
          }
          for name, rec in result.job_records.items()
      },
  }
  with open(output_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
  print(f"[+] Traza y metadatos exportados a JSON: {output_path}")


# ==============================================================================
# Reportes y Resumen en Consola
# ==============================================================================


def print_summary(result: SimulationResult) -> None:
  """Imprime en consola las tablas de verificación matemática y métricas de QoS.

  Args:
    result: Objeto SimulationResult con los resultados de la simulación.
  """
  print("\n" + "=" * 68)
  print("        RESUMEN DE SIMULACIÓN DEL SERVIDOR ESPORÁDICO (SS)")
  print("=" * 68)

  # Análisis de Planificabilidad y Tiempos de Respuesta (RTA)
  if result.schedulability:
    s = result.schedulability
    print("\n[+] Análisis de Planificabilidad Rate-Monotonic y RTA:")
    print(
        f"    - Utilización Periódica (U_p) : {s.utilization_periodic:.4f} ({s.utilization_periodic * 100:.1f}%)"
    )
    print(
        f"    - Utilización del Servidor (U_s): {s.utilization_server:.4f} ({s.utilization_server * 100:.1f}%)"
    )
    print(
        f"    - Utilización Total (U_total) : {s.utilization_total:.4f} ({s.utilization_total * 100:.1f}%) <= 1.0"
    )
    print(
        f"    - Cota de Liu & Layland U_LL  : {s.liu_layland_bound:.4f} ({s.liu_layland_bound * 100:.1f}%)"
    )
    print("    - Tiempos de Respuesta en Peor Caso (RTA):")
    print("      Tarea   | Prioridad | C_i  | T_i  | D_i  | U_i    | R_i  | ¿Planificable?")
    print("      " + "-" * 66)
    for tm in s.task_metrics:
      status = "SÍ" if tm.schedulable else "NO"
      print(
          f"      {tm.name:7s} | {tm.priority:9d} | {tm.c_i:4.1f} | {tm.t_i:4.1f} | {tm.d_i:4.1f} | {tm.u_i:6.3f} | {tm.response_time:4.1f} | {status}"
      )

  print("\n[+] Tiempo Total de CPU por Ejecutor:")
  for runner, duration in sorted(result.cpu_totals.items()):
    print(f"    - {runner:12s}: {duration:5.1f} unidades")
  print(f"    - Tiempo Total: {sum(result.cpu_totals.values()):5.1f} unidades")

  print("\n[+] Ventanas de Activación (t_A) del Servidor:")
  print("    Núm | t_A   | Fin Ventana | Consumido | Causa de Cierre")
  print("    " + "-" * 55)
  for idx, act in enumerate(result.activations, 1):
    print(
        f"    {idx:3d} | {act.activation_time:5.1f} | {act.closed_time:11.1f} |"
        f" {act.amount_consumed:9.1f} | {act.reason}"
    )

  print("\n[+] Recargas Programadas de Presupuesto (t_R):")
  print("    Núm | t_R   | Monto de Recarga")
  print("    " + "-" * 35)
  for idx, rep in enumerate(result.replenishments, 1):
    print(f"    {idx:3d} | {rep.replenishment_time:5.1f} | +{rep.amount:.1f}")

  print("\n[+] Estado de Trabajos Aperiódicos y Métricas de QoS:")
  print("    Trabajo | Llegada | Cómputo | Fin      | T. Resp | T. Espera | Estado")
  print("    " + "-" * 68)
  for name, rec in sorted(result.job_records.items()):
    if rec.completion_time is not None:
      resp = rec.response_time if rec.response_time is not None else (rec.completion_time - rec.job.arrival_time)
      wait = rec.waiting_time if rec.waiting_time is not None else (resp - rec.job.compute_time)
      print(
          f"    {name:7s} | {rec.job.arrival_time:7.1f} | {rec.job.compute_time:7.1f} | {rec.completion_time:8.1f} | {resp:7.1f} | {wait:9.1f} | Completado"
      )
    else:
      print(
          f"    {name:7s} | {rec.job.arrival_time:7.1f} | {rec.job.compute_time:7.1f} | {'—':>8s} | {'—':>7s} | {'—':>9s} | Incompleto ({rec.remaining_time:.1f} u rest.)"
      )

  if result.schedulability and result.schedulability.avg_aperiodic_response_time:
    print(
        f"\n    [*] Tiempo Medio de Respuesta Aperiódico (W_avg): {result.schedulability.avg_aperiodic_response_time:.2f} unidades"
    )
    print(
        f"    [*] Tiempo Máximo de Respuesta Aperiódico (W_max): {result.schedulability.max_aperiodic_response_time:.2f} unidades"
    )

  if result.deadline_misses:
    print(f"\n[!] ADVERTENCIA: ¡{len(result.deadline_misses)} violaciones de plazos (deadlines perdidos)!")
  else:
    print("\n[+] Plazos Periódicos: Todos los plazos cumplidos satisfactoriamente (0 violaciones).")
  print("=" * 68 + "\n")


# ==============================================================================
# Cargador de Tareas desde Archivo JSON
# ==============================================================================


def load_tasks_from_json(
    path: str,
) -> tuple[list[PeriodicTask], SporadicServerConfig, list[AperiodicJob], float, float]:
  """Carga la configuración de tareas y parámetros desde un archivo JSON.

  Estructura esperada del JSON:
    {
      "horizon": 30.0,          (opcional, horizonte de simulación en unidades)
      "dt": 0.1,                (opcional, paso delta t)
      "periodic_tasks": [
        {
          "name": "$\\tau_1$",
          "priority": 7,
          "compute_time": 1.0,
          "deadline": 6.0,
          "period": 6.0,
          "color": "#0072B2"    (opcional)
        }, ...
      ],
      "server": {
        "name": "$S_s$",        (opcional, default="$S_s$")
        "priority": 4,
        "budget": 2.0,
        "period": 12.0,
        "deadline": 12.0,
        "color": "#16a085"      (opcional)
      },
      "aperiodic_jobs": [
        {
          "name": "J1",
          "arrival_time": 2.0,
          "compute_time": 1.0
        }, ...
      ]
    }

  Args:
    path: Ruta del archivo JSON a cargar.

  Returns:
    Tupla con (periodic_tasks, server_config, aperiodic_jobs, horizon, dt).
  """
  with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

  horizon = float(data.get("horizon", SIMULATION_HORIZON))
  dt = float(data.get("dt", DT))

  # Cargar tareas periódicas con asignación automática de colores
  periodic_tasks: list[PeriodicTask] = []
  for idx, td in enumerate(data.get("periodic_tasks", [])):
    color = td.get("color", _COLOR_PALETTE[idx % len(_COLOR_PALETTE)])
    periodic_tasks.append(
        PeriodicTask(
            name=td["name"],
            priority=int(td["priority"]),
            compute_time=float(td["compute_time"]),
            deadline=float(td["deadline"]),
            period=float(td["period"]),
            color=color,
        )
    )

  # Cargar parámetros del Servidor Esporádico
  sd = data.get("server", {})
  server_color = sd.get("color", "#16a085")
  server_config = SporadicServerConfig(
      name=sd.get("name", r"$S_s$"),
      priority=int(sd["priority"]),
      budget=float(sd["budget"]),
      period=float(sd["period"]),
      deadline=float(sd.get("deadline", sd["period"])),
      color=server_color,
  )

  # Cargar peticiones aperiódicas
  aperiodic_jobs: list[AperiodicJob] = []
  for jd in data.get("aperiodic_jobs", []):
    aperiodic_jobs.append(
        AperiodicJob(
            name=jd["name"],
            arrival_time=float(jd["arrival_time"]),
            compute_time=float(jd["compute_time"]),
        )
    )

  return periodic_tasks, server_config, aperiodic_jobs, horizon, dt


# ==============================================================================
# Punto de Entrada de Línea de Comandos (CLI)
# ==============================================================================


def main() -> None:
  """Función principal para la ejecución por línea de comandos (CLI)."""
  parser = argparse.ArgumentParser(
      description="Simulación y visualización interactiva del algoritmo Servidor Esporádico (SS)."
  )
  parser.add_argument(
      "--version",
      action="version",
      version=f"sporadic_server_sim {VERSION}",
  )
  parser.add_argument(
      "--output",
      default="sporadic_server_simulation.png",
      help="Ruta para guardar la imagen generada de la simulación.",
  )
  parser.add_argument(
      "--html",
      default="sporadic_server_interactive.html",
      help="Ruta para exportar el dashboard HTML interactivo.",
  )
  parser.add_argument(
      "--export-csv",
      default=None,
      help="Ruta opcional para exportar las rebanadas de ejecución a CSV.",
  )
  parser.add_argument(
      "--export-json",
      default=None,
      help="Ruta opcional para exportar toda la traza a JSON.",
  )
  parser.add_argument(
      "--mode",
      choices=["queue_aware", "buttazzo_pure"],
      default="queue_aware",
      help="Modo de regla de reposición del servidor (queue_aware o buttazzo_pure).",
  )
  parser.add_argument(
      "--dt",
      type=float,
      default=DT,
      help="Paso de tiempo de discretización (por defecto: 0.1).",
  )
  parser.add_argument(
      "--horizon",
      type=float,
      default=SIMULATION_HORIZON,
      help="Horizonte temporal de la simulación (por defecto: 30.0).",
  )
  parser.add_argument(
      "--no-show",
      action="store_true",
      help="Desactiva la apertura de la ventana gráfica interactiva de Matplotlib.",
  )
  parser.add_argument(
      "--tasks-json",
      default=None,
      metavar="ARCHIVO",
      help=(
          "Ruta a un archivo JSON con la configuración de tareas. "
          "Si no se especifica, se usa el conjunto por defecto (7 tareas). "
          "Ver example_schedulable.json y example_unschedulable.json."
      ),
  )
  args = parser.parse_args()

  # Cargar tareas desde JSON o recurrir a los valores por defecto
  periodic_tasks = None
  server_config = None
  aperiodic_jobs = None
  horizon = args.horizon
  dt = args.dt

  if args.tasks_json:
    print(f"[+] Cargando configuración de tareas desde: {args.tasks_json}")
    periodic_tasks, server_config, aperiodic_jobs, json_horizon, json_dt = (
        load_tasks_from_json(args.tasks_json)
    )
    if args.horizon == SIMULATION_HORIZON:
      horizon = json_horizon
    if args.dt == DT:
      dt = json_dt
    print(f"    → {len(periodic_tasks)} tarea(s) periódica(s) cargada(s)")
    print(f"    → Horizonte: {horizon:.1f} | dt: {dt}")

  result = run_simulation(
      horizon=horizon,
      dt=dt,
      periodic_tasks=periodic_tasks,
      server_config=server_config,
      aperiodic_jobs=aperiodic_jobs,
      mode=args.mode,
  )

  # ── ADVERTENCIA DE PLANIFICABILIDAD ──────────────────────────────────────────
  # Si la utilización total supera 1.0 o alguna tarea no pasa el RTA,
  # se imprime una advertencia en rojo para que el usuario pueda identificar
  # el caso de conjunto de tareas NO PLANIFICABLE.
  if result.schedulability:
    s = result.schedulability
    if s.utilization_total > 1.0:
      print(
          "\033[91m[!] ADVERTENCIA: U_total = "
          f"{s.utilization_total:.4f} > 1.0 — El sistema DEFINITIVAMENTE"
          " no es planificable (sobrecarga de CPU).\033[0m"
      )
    elif not s.all_schedulable:
      print(
          "\033[91m[!] ADVERTENCIA: Algunas tareas NO pasan el RTA "
          "(R_i > D_i). El sistema tiene riesgo de incumplimiento de plazos.\033[0m"
      )
    else:
      print(
          f"\033[92m[✓] Sistema planificable bajo RM: U_total = "
          f"{s.utilization_total:.4f} ≤ 1.0 y todos los R_i ≤ D_i.\033[0m"
      )
  print_summary(result)

  plot_simulation(
      result,
      output_path=args.output,
      show=not args.no_show,
      horizon=horizon,
      periodic_tasks=periodic_tasks,
      server_config=server_config,
      interactive_html=args.html,
  )

  if args.export_csv:
    export_trace_to_csv(result, args.export_csv)
  if args.export_json:
    export_trace_to_json(result, args.export_json)


if __name__ == "__main__":
  main()
