# Simulador del Servidor Esporádico sobre Micro-VMs en Proxmox VE

[![Versión](https://img.shields.io/badge/versión-1.0.0-blue.svg)](setup_proxmox_vms.sh)
[![Proxmox VE](https://img.shields.io/badge/Proxmox_VE-9.1--1-E57000.svg)](https://www.proxmox.com/)
[![Micro-VMs](https://img.shields.io/badge/Micro--VMs-5_Stripped_KVM_(768MB_RAM)-brightgreen.svg)](#1-dimensionamiento-del-servidor-host-proxmox-ve-91-1-y-selección-de-micro-vm-cero-overprovisioning)
[![Licencia](https://img.shields.io/badge/licencia-MIT-green.svg)](LICENSE)

**Proyecto Base:** [`elnemesisdivina/sporadic-server-simulation` (v2.1.0)](https://github.com/elnemesisdivina/sporadic-server-simulation)  
**Fundamento Teórico:** Algoritmo del Servidor Esporádico (*Sporadic Server*, SS) bajo Planificación Expulsiva por Tasa Monótona (*Rate-Monotonic*, RM) — *Sprunt, Sha & Lehoczky (1989)* / *Giorgio Buttazzo, Hard Real-Time Computing Systems, Cap. 5, Sección 5.6*.  
**Arquitectura del Sistema:** 4 Micro-VMs de Tareas Periódicas ($\tau_1 \dots \tau_4$) + 1 Micro-VM para el Servidor Esporádico y Coordinador RM ($S_s$) sobre **Proxmox VE 9.1-1**.  
**Maintainer:** Ray (`<elnemesisdivina@gmail.com>`)

---

## Índice de Contenidos

1. [Dimensionamiento del Servidor Host (Proxmox VE 9.1-1) y Selección de Micro-VM (Cero Overprovisioning)](#1-dimensionamiento-del-servidor-host-proxmox-ve-91-1-y-selección-de-micro-vm-cero-overprovisioning)
   * [1.1 Especificaciones del Servidor Físico (Host) y Acceso Remoto (Sanitizado)](#11-especificaciones-del-servidor-físico-host-y-acceso-remoto-sanitizado)
   * [1.2 Elección de la Micro-VM Ideal: Proxmox Stripped KVM Micro-VMs (`qm` con Alpine Linux `virt`)](#12-elección-de-la-micro-vm-ideal-proxmox-stripped-kvm-micro-vms-qm-con-alpine-linux-virt)
   * [1.3 Presupuesto de Recursos y Garantía de Cero Overprovisioning](#13-presupuesto-de-recursos-y-garantía-de-cero-overprovisioning)
   * [1.4 Detalle de Configuración por Micro-VM (Stripped KVM) en Proxmox 9.1-1](#14-detalle-de-configuración-por-micro-vm-stripped-kvm-en-proxmox-91-1)
2. [Resumen Ejecutivo y Clarificación Conceptual](#2-resumen-ejecutivo-y-clarificación-conceptual)
3. [Estrategia de Visibilidad en las Gráficas de CPU de Proxmox (`--time-scale`)](#3-estrategia-de-visibilidad-en-las-gráficas-de-cpu-de-proxmox---time-scale)
4. [Arquitectura del Clúster de 5 Micro-VMs (Stripped KVM) en Proxmox VE 9.1-1](#4-arquitectura-del-clúster-de-5-micro-vms-stripped-kvm-en-proxmox-ve-91-1)
5. [Diseño Matemático del Conjunto de Tareas para 5 Micro-VMs](#5-diseño-matemático-del-conjunto-de-tareas-para-5-micro-vms)
   * [5.1 Escenario Planificable ($U_{\text{total}} = 0.6333 \le 1.0$, `proxmox_schedulable.json`)](#51-escenario-planificable-u_texttotal--06333-le-10-proxmox_schedulablejson)
   * [5.2 Escenario No Planificable / Sobrecarga ($U_{\text{total}} = 1.1833 > 1.0$, `proxmox_unschedulable.json`)](#52-escenario-no-planificable--sobrecarga-u_texttotal--11833--10-proxmox_unschedulablejson)
   * [5.3 Estructura Dinámica del JSON y Cómo Modificarlo para Provocar Sobrecarga ($U > 1.0$)](#53-estructura-dinámica-del-json-y-cómo-modificarlo-para-provocar-sobrecarga-u--10)
   * [5.4 Secuencia de Trabajos Aperiódicos ($J_1 \dots J_6$) y Eventos Clave a Observar](#54-secuencia-de-trabajos-aperiódicos-j_1-dots-j_6-y-eventos-clave-a-observar)
6. [Especificación Técnica de los Componentes de Software](#6-especificación-técnica-de-los-componentes-de-software)
   * [6.1 Proceso Quemador de CPU (`CPUBurnerProcess`)](#61-proceso-quemador-de-cpu-cpuburnerprocess)
   * [6.2 Protocolo de Control en Tiempo Real (TCP `NDJSON` con `TCP_NODELAY`)](#62-protocolo-de-control-en-tiempo-real-tcp-ndjson-con-tcp_nodelay)
   * [6.3 Inyección de Trabajos Aperiódicos ($J_k$): Automática + En Vivo](#63-inyección-de-trabajos-aperiódicos-j_k-automática--en-vivo)
   * [6.4 Integración Directa con el Visualizador del Repositorio Base](#64-integración-directa-con-el-visualizador-del-repositorio-base)
7. [Estructura de Archivos a Desarrollar](#7-estructura-de-archivos-a-desarrollar)
8. [Creación del Clúster Desde Cero y Ejecución Remota (Host Proxmox 9.1-1 Vacío)](#8-creación-del-clúster-desde-cero-y-ejecución-remota-host-proxmox-91-1-vacío)
   * [Paso 0: Establecer el Túnel SSH y Subir los Archivos al Host Proxmox Remoto](#paso-0-establecer-el-túnel-ssh-y-subir-los-archivos-al-host-proxmox-remoto)
   * [Paso 1: Creación Automatizada desde Cero en el Host Proxmox 9.1-1 (`setup_proxmox_vms.sh`)](#paso-1-creación-automatizada-desde-cero-en-el-host-proxmox-91-1-setup_proxmox_vmssh)
   * [Paso 2: Despliegue Automático del Código desde el Host Proxmox hacia las 5 Micro-VMs](#paso-2-despliegue-automático-del-código-desde-el-host-proxmox-hacia-las-5-micro-vms)
   * [Paso 3: Iniciar el Servidor Esporádico y Coordinador en la Micro-VM 105](#paso-3-iniciar-el-servidor-esporádico-y-coordinador-en-la-micro-vm-105)
   * [Paso 4: Visualización en Vivo desde Proxmox (`:1234`) y el Dashboard Web de la VM 105 (`:8080`)](#paso-4-visualización-en-vivo-desde-proxmox-1234-y-el-dashboard-web-de-la-vm-105-8080)
   * [Paso 5: Limpieza Total del Clúster en Proxmox (`--cleanup`)](#paso-5-limpieza-total-del-clúster-en-proxmox---cleanup)
9. [Autoría y Licencia](#9-autoría-y-licencia)

---

## 1. Dimensionamiento del Servidor Host (Proxmox VE 9.1-1) y Selección de Micro-VM (Cero Overprovisioning)

### 1.1 Especificaciones del Servidor Físico (Host) y Acceso Remoto (Sanitizado)
* **Sistema Operativo Hipervisor:** Proxmox VE `9.1-1` (basado en Debian 13 *Trixie* / Kernel Linux 6.14+)
* **Procesadores (CPUs lógicos):** `12 núcleos`
* **Memoria RAM Total:** `31 GB`
* **Almacenamiento Disponible:** `500 GB`
* **Parámetros de Conexión Remota (Sanitizados):**
  * **Usuario (`Login`):** `root`
  * **Dirección IP:** `<IP_HOST_SERVER>`
  * **Puerto SSH:** `<PORT_HOST_SERVER>`
  * **Protocolo:** `SSH` (con reenvío de puertos locales para la GUI Web de Proxmox y el disparador HTTP del Servidor Esporádico)

```bash
# 1. Crear el Túnel SSH hacia la Interfaz Web de Proxmox (https://localhost:1234)
#    y hacia la API de trabajos aperiódicos de la VM 105 (http://localhost:8080):
ssh -p <PORT_HOST_SERVER> -L 1234:localhost:8006 -L 8080:10.10.10.105:8080 -N -C root@<IP_HOST_SERVER>

# 2. Abrir sesión interactiva en la consola del Host Proxmox VE 9.1-1:
ssh -p <PORT_HOST_SERVER> root@<IP_HOST_SERVER>
```

### 1.2 Elección de la Micro-VM Ideal: **Proxmox Stripped KVM Micro-VMs (`qm` con Alpine Linux `virt`)**
Para evitar cualquier degradación de rendimiento, ruido de CPU en segundo plano (*jitter*), limitaciones de memoria o sobreaprovisionamiento (*overprovisioning*), se utilizan **KVM Micro-VMs nativas de Proxmox (`qm`) con hardware emulado recortado (*Stripped KVM*)** ejecutando **Alpine Linux 3.21 Edición `virt` (`alpine-virt`)**:

1. **Aislamiento real de máquina virtual KVM + Gráficas nativas en Proxmox Web GUI:** A diferencia de `cloud-hypervisor` (que no se registra en la interfaz web de Proxmox), utilizar `qm` con perfil recortado (`--vga serial0`, sin audio, sin tableta USB, `--balloon 0`, `virtio-scsi-single`) mantiene el 100% de las gráficas individuales de CPU en Proxmox reduciendo la sobrecarga de QEMU al mínimo.
2. **Kernel `virt` ultraligero sin demonios ruidosos:** Alpine utiliza `OpenRC` y un kernel compilado exclusivamente para KVM (`virtio`). Cuando una tarea se encuentra desalojada (`SIGSTOP`), el uso de CPU de la Micro-VM en Proxmox se mantiene en **`0.00%`**, garantizando gráficas limpias.
3. **Huella ultracompacta de RAM (`768 MB` en total para las 5 Micro-VMs):** Cada Micro-VM trabajadora (`vm1..vm4`) consume apenas `~35 MB` en reposo y se configura con **`128 MB de RAM fija`**, mientras que `vm5-sporadic-server` recibe **`256 MB de RAM fija`**.
4. **Clonación instantánea mediante Plantilla Base (*Linked Clones*):** Se importa una única imagen base Alpine Cloud (`VM 9000`) y las 5 Micro-VMs (`VM 101` a `VM 105`) se clonan en menos de 3 segundos consumiendo menos de **1.5 GB de disco en total** de los 500 GB disponibles.

### 1.3 Presupuesto de Recursos y Garantía de Cero Overprovisioning

Se asignan recursos físicos estrictamente acotados con **anclaje de núcleos (*CPU Pinning / Affinity*)** para aislar el hipervisor Proxmox 9.1-1 de las 5 Micro-VMs:

| Recurso del Host | Capacidad Total (Proxmox 9.1-1) | Asignación por Micro-VM (Stripped KVM) | Total Asignado (5 Micro-VMs) | Libre para el Host Proxmox | % de Uso del Host | ¿Existe Overprovisioning? |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **CPU (Núcleos)** | `12 Cores` | `1 vCPU` (`cpu: host`, anclado 1:1) | **`5 vCPUs`** (Cores `1–5`) | **`7 Cores`** (Core `0` + Cores `6–11`) | **41.7% asignado** *(8.3% activo)* | **NO** (Relación 1:1 física + 7 núcleos libres) |
| **Memoria RAM** | `31 GB` | `128 MB` (`vm1..4`) / `256 MB` (`vm5`) | **`0.75 GB`** (`768 MB`) | **`30.25 GB`** | **2.4%** | **NO** (97.6% de RAM libre, `--balloon 0`) |
| **Almacenamiento** | `500 GB` | `2 GB` (`vm1..4`) / `4 GB` (`vm5`) | **`12 GB` nominal** *(< 1.5 GB real con Linked Clone)* | **`> 488 GB`** | **< 0.3% real** | **NO** (> 97.6% libre, `virtio-scsi-single`) |

### 1.4 Detalle de Configuración por Micro-VM (Stripped KVM) en Proxmox 9.1-1

| VMID | Nombre en Proxmox | Rol en la Simulación | vCPUs (`cores`) | Núcleo Físico Anclado (`affinity`) | RAM Fija (`memory`) | Perfil Hardware Recortado |
| :---: | :--- | :--- | :---: | :---: | :---: | :---: |
| **`101`** | `vm1-tau1` | Tarea Periódica $\tau_1$ ($P_1 = 5$, Máxima) | `1 vCPU` (`host`) | **Core Físico #1** | **`128 MB`** | `--vga serial0 --balloon 0` |
| **`102`** | `vm2-tau2` | Tarea Periódica $\tau_2$ ($P_2 = 4$, Alta) | `1 vCPU` (`host`) | **Core Físico #2** | **`128 MB`** | `--vga serial0 --balloon 0` |
| **`103`** | `vm3-tau3` | Tarea Periódica $\tau_3$ ($P_3 = 2$, Media-Baja) | `1 vCPU` (`host`) | **Core Físico #3** | **`128 MB`** | `--vga serial0 --balloon 0` |
| **`104`** | `vm4-tau4` | Tarea Periódica $\tau_4$ ($P_4 = 1$, Mínima) | `1 vCPU` (`host`) | **Core Físico #4** | **`128 MB`** | `--vga serial0 --balloon 0` |
| **`105`** | `vm5-sporadic-server` | **Servidor Esporádico $S_s$ ($P_s = 3$) + Coordinador RM + Generador PNG/HTML** | `1 vCPU` (`host`) | **Core Físico #5** | **`256 MB`** | `--vga serial0 --balloon 0` |

> [!TIP]
> **¿Cómo logra una Stripped KVM Micro-VM ejecutarse en `128 MB` de RAM en Proxmox 9.1-1?**  
> Al configurar `--vga serial0` (consola serie en lugar de tarjeta gráfica emulada con VRAM), deshabilitar el *Memory Ballooning* (`--balloon 0`), eliminar controladores USB/Audio innecesarios y emplear el kernel `alpine-virt`, QEMU/KVM reduce su huella de memoria al mínimo e inicia en menos de 2 segundos manteniendo soporte completo en la GUI de Proxmox.

---

## 2. Resumen Ejecutivo y Clarificación Conceptual

Al trasladar el simulador discreto en Python ([`sporadic_server_sim.py`](https://github.com/elnemesisdivina/sporadic-server-simulation/blob/main/sporadic_server_sim.py)) a un entorno real de virtualización en **Proxmox VE**, resulta indispensable resolver dos diferencias físicas entre una simulación de un solo hilo y un clúster de 5 Máquinas Virtuales (VMs):

1. **¿Por qué un simple `ping` no basta como carga de trabajo?**
   * Un paquete ICMP `ping` tarda menos de `0.5 ms` y no consume CPU de forma sostenida.
   * En el algoritmo del Servidor Esporádico, tanto las tareas periódicas ($\tau_i$) como las peticiones aperiódicas ($J_k$) poseen un **tiempo de cómputo medible ($C_i, C_k$)** durante el cual:
     * El presupuesto $C_s(t)$ se consume a razón de $-1/\text{s}$ **únicamente mientras $S_s$ se encuentra ejecutando en CPU**.
     * Una tarea periódica de mayor prioridad ($\tau_1, \tau_2$) puede **desalojar (*preempt*)** al Servidor Esporádico a mitad de un trabajo $J_k$, congelando el gasto de presupuesto $C_s(t)$ pero conservando abierta la ventana de activación $t_A$.
   * **Solución:** Cuando una petición aperiódica $J_k$ arriba a la **VM 5 (`vm5-sporadic-server`)**, ésta encola el trabajo y, cuando el planificador RM le otorga el turno de CPU, **la propia VM 5 ejecuta un proceso intensivo de CPU al 100% (`CPU Burner`)** durante el tiempo asignado. De este modo, el monitor de CPU de Proxmox en la VM 5 refleja directamente el consumo real del presupuesto $C_s(t)$.

2. **¿Cómo garantizar exclusión mutua y desalojo (*preemption*) entre 5 VMs?**
   * Por defecto, 5 VMs en Proxmox se ejecutan en paralelo sobre distintos núcleos físicos del hipervisor. Si todas corren libremente, ninguna tarea desaloja a otra.
   * **Solución (Coordinación de Token de CPU con `SIGSTOP`/`SIGCONT`):** Cada una de las 5 VMs se configura con **1 único vCPU (`cores: 1`)** y mantiene un proceso trabajador de carga real (`CPU Burner`). El Coordinador RM en la VM 5 otorga el **Token Único de CPU** a **una sola VM a la vez**:
     * La VM activa recibe `SIGCONT` $\rightarrow$ su uso de CPU sube instantáneamente al **100.0%** en Proxmox.
     * Las 4 VMs restantes (desalojadas o en espera) reciben `SIGSTOP` $\rightarrow$ su uso de CPU cae instantáneamente al **0.0%** en Proxmox.

---

## 3. Estrategia de Visibilidad en las Gráficas de CPU de Proxmox (`--time-scale`)

### El reto del muestreo en Proxmox VE
El demonio de estadísticas de Proxmox (`pvestatd`) actualiza las barras de uso de CPU en vivo cada **2 a 3 segundos** y registra puntos en las gráficas RRD (`Summary` $\rightarrow$ `CPU usage`) cada **10 segundos**.
* Si se ejecuta la simulación a escala $1:1$ ($1\text{ unidad lógica} = 1\text{ segundo real}$), una ráfaga de $C_1 = 1.0\text{ s}$ se promedia con los segundos de inactividad y apenas se distingue en la interfaz web de Proxmox.

### Solución: Factor de Escala Temporal (`--time-scale K`)
Se mantienen intactos los parámetros matemáticos del modelo ($C_i, T_i, D_i, C_s, T_s$) y se incorpora un multiplicador de tiempo real `--time-scale K`:

| Modo de Ejecución | Parámetro | Equivalencia de $1.0$ unidad lógica | Duración de $C_s = 2.0$ en CPU al 100% | Duración Total (Horizonte $30\text{ u}$) | Uso Recomendado |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Prueba Rápida (CLI)** | `--time-scale 1.0` | `1.0 s` reales | `2.0 s` al 100% CPU | `30 segundos` | Verificación rápida y pruebas automatizadas |
| **Demo en Vivo (`htop` / Consola)** | `--time-scale 3.0` | `3.0 s` reales | `6.0 s` al 100% CPU | `90 segundos` (1.5 min) | Observar desalojos en vivo en `htop`/`btop` y resumen de Proxmox |
| **Gráficas Web RRD de Proxmox** | `--time-scale 5.0` | `5.0 s` reales | `10.0 s` al 100% CPU | `150 segundos` (2.5 min) | Capturar picos limpios del 100% en las gráficas históricas de cada VM en Proxmox GUI |

---

## 4. Arquitectura del Clúster de 5 Micro-VMs (Stripped KVM) en Proxmox VE 9.1-1

```text
+======================================================================================+
| NODO HIPERVISOR: Proxmox VE 9.1-1 (12 CPUs | 31 GB RAM | 500 GB Disk | Bridge vmbr0) |
| Recursos Libres para el Host: 7 CPUs (Core 0, Cores 6..11) | 30.25 GB RAM (97.6%)    |
+======================================================================================+
                                           |
   [Inyector de Trabajos Aperiodicos J_k]  |  (Tunel SSH -L 8080:10.10.10.105:8080)
   1. Replay automatico JSON (J1..J6)      |  2. Trigger en vivo: curl :8080/job?work=2.0
                                           v
+--------------------------------------------------------------------------------------+
| VM 105: vm5-sporadic-server (10.10.10.105) | Core Fisico #5 | 1 vCPU | 256 MB RAM    |
|--------------------------------------------------------------------------------------|
|  +----------------------------------------+   +-----------------------------------+  |
|  | Coordinador RM + Servidor Esporadico   |   | Local CPU Burner (Trabajos J_k)   |  |
|  | Prioridad P_s = 3 | C_s = 2.0 | T_s=12 |-->| SIGCONT => 100% vCPU en VM 105    |  |
|  | - Reloj logico (dt = 0.1 * K)          |   | SIGSTOP =>   0% vCPU (C_s=0/Idle) |  |
|  | - Cola FIFO, ventana t_A, t_R = t_A+T_s|   +-----------------------------------+  |
|  +----------------------------------------+                                          |
+--------------------------------------------------------------------------------------+
            ^                     ^                     ^                     ^
            |                     |                     |                     |
            | Sockets TCP_NODELAY (Puerto 9000 en vmbr0: GRANT_CPU / PREEMPT_CPU)
            |                     |                     |                     |
            v                     v                     v                     v
+-------------------+ +-------------------+ +-------------------+ +-------------------+
| VM 101: vm1-tau1  | | VM 102: vm2-tau2  | | VM 103: vm3-tau3  | | VM 104: vm4-tau4  |
| IP: 10.10.10.101  | | IP: 10.10.10.102  | | IP: 10.10.10.103  | | IP: 10.10.10.104  |
|-------------------| |-------------------| |-------------------| |-------------------|
| Core Fisico #1    | | Core Fisico #2    | | Core Fisico #3    | | Core Fisico #4    |
| 1 vCPU | 128MB RAM| | 1 vCPU | 128MB RAM| | 1 vCPU | 128MB RAM| | 1 vCPU | 128MB RAM|
| Tarea: tau_1      | | Tarea: tau_2      | | Tarea: tau_3      | | Tarea: tau_4      |
| P_1 = 5 (Maxima)  | | P_2 = 4 (Alta)    | | P_3 = 2 (Media)   | | P_4 = 1 (Minima)  |
| C_1=1.0 | T_1=6.0 | | C_2=1.0 | T_2=10.0| | C_3=1.5 | T_3=15.0| | C_4=2.0 | T_4=20.0|
|-------------------| |-------------------| |-------------------| |-------------------|
| Desaloja a S_s y  | | Desaloja a S_s y  | | Desalojada por    | | Carga periodica   |
| preserva ventana  | | a tareas menores  | | S_s si C_s(t) > 0 | | de fondo          |
+-------------------+ +-------------------+ +-------------------+ +-------------------+
```

### Mapeo Exacto entre la Teoría (Buttazzo) y Proxmox

| Concepto Teórico (`sporadic_server_sim.py`) | Implementación Física en las 5 Micro-VMs de Proxmox | Comportamiento en el Monitor de CPU de Proxmox |
| :--- | :--- | :--- |
| **Procesador Único Compartido** | Token de ejecución exclusivo gestionado por `vm5-sporadic-server` cada tick $dt$. | En cualquier instante $t$, **exactamente una** de las 5 VMs se encuentra al **100% de CPU** (o ninguna si el sistema está `Idle`). |
| **Tarea Periódica $\tau_i$ ($C_i, T_i, D_i$)** | Demonio `worker` en `vm1`..`vm4`. Cada $T_i$ unidades solicita $C_i$ unidades de CPU. | `vm1`..`vm4` exhiben pulsos periódicos al **100% CPU** de duración $C_i \times K$ segundos. |
| **Servidor Esporádico $S_s$ ($C_s, T_s, P_s$)** | Motor de presupuesto y cola FIFO dentro de `vm5-sporadic-server`. | `vm5` compite con prioridad $P_s = 3$ únicamente cuando `cola > 0` y $C_s(t) > 0$. |
| **Trabajo Aperiódico $J_k$ ($r_k, C_k$)** | Petición recibida por `vm5` (vía JSON programado o petición `curl` al puerto `8080`). | Mientras $S_s$ atiende $J_k$, **`vm5-sporadic-server` sube al 100% de CPU** en Proxmox. |
| **Desalojo (*Preemption*) de $S_s$ por $\tau_1$** | Si $\tau_1$ arriba mientras `vm5` ejecuta $J_k$, `vm5` pausa su propio `CPU Burner` (`SIGSTOP`) y envía `GRANT_CPU` a `vm1`. | En Proxmox, **`vm5` cae del 100% al 0% CPU** e inmediatamente **`vm1` sube del 0% al 100% CPU**. El presupuesto $C_s(t)$ deja de disminuir y $t_A$ se conserva. |
| **Agotamiento ($C_s = 0$) y Recarga ($t_R = t_A + T_s$)** | Cuando `vm5` consume $C_s = 2.0$, pausa su `CPU Burner` (`SIGSTOP`), cede el CPU a `vm3`/`vm4` y agenda recarga en $t_R = t_A + 12.0$. | **`vm5` permanece en 0% CPU** aunque existan trabajos en cola, y vuelve a subir al **100% CPU** exactamente en el instante $t_R$. |

---

## 5. Diseño Matemático del Conjunto de Tareas para 5 Micro-VMs

Se adapta el escenario del repositorio base a **4 Tareas Periódicas ($\tau_1 \dots \tau_4$) + 1 Servidor Esporádico ($S_s$)**, ubicando la prioridad del servidor en el centro ($P_s = 3$) para demostrar tanto desalojos sufridos por el servidor (por $\tau_1, \tau_2$) como desalojos provocados por el servidor sobre tareas de menor prioridad ($\tau_3, \tau_4$).

### 5.1 Escenario Planificable ($U_{\text{total}} = 0.6333 \le 1.0$, `proxmox_schedulable.json`)

| VM en Proxmox | Tarea | Prioridad ($P_i$) | Cómputo ($C_i$) | Período ($T_i$) | Plazo ($D_i$) | Utilización ($U_i$) | Peor Caso RTA ($R_i$) | ¿Cumple $R_i \le D_i$? |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`vm1-tau1` (101)** | $\tau_1$ | **5** (Máxima) | `1.0` | `6.0` | `6.0` | `0.1667` | `1.0` | **SÍ** ($1.0 \le 6.0$) |
| **`vm2-tau2` (102)** | $\tau_2$ | **4** | `1.0` | `10.0` | `10.0` | `0.1000` | `2.0` | **SÍ** ($2.0 \le 10.0$) |
| **`vm5-ss` (105)** | **$S_s$** | **3 (Servidor)** | **`2.0`** | **`12.0`** | **`12.0`** | **`0.1667`** | **`4.0`** | **SÍ ($4.0 \le 12.0$)** |
| **`vm3-tau3` (103)** | $\tau_3$ | **2** | `1.5` | `15.0` | `15.0` | `0.1000` | `5.5` | **SÍ** ($5.5 \le 15.0$) |
| **`vm4-tau4` (104)** | $\tau_4$ | **1** (Mínima) | `2.0` | `20.0` | `20.0` | `0.1000` | `8.5` | **SÍ** ($8.5 \le 20.0$) |

* **Utilización Total del Clúster (Planificable):**
  $$U_{\text{total}} = \sum_{i=1}^{4} \frac{C_i}{T_i} + \frac{C_s}{T_s} = \frac{1}{6} + \frac{1}{10} + \frac{2}{12} + \frac{1.5}{15} + \frac{2}{20} = \frac{38}{60} \approx 0.6333 \le 1.0$$
* **Cota de Liu & Layland para $n = 5$:**
  $$U_{LL}(5) = 5 \cdot (2^{1/5} - 1) \approx 0.7435$$
  Dado que $U_{\text{total}} = 0.6333 \le 0.7435$, el sistema es **estrictamente planificable** tanto por la condición suficiente de Liu & Layland como por el Análisis Exacto de Tiempo de Respuesta (RTA, Teorema 5.1 de Buttazzo), produciendo **0 violaciones de plazo**.

---

### 5.2 Escenario No Planificable / Sobrecarga ($U_{\text{total}} = 1.1833 > 1.0$, `proxmox_unschedulable.json`)

Al igual que `example_unschedulable.json` en el repositorio base, se define un segundo escenario con sobrecarga del procesador ($U_{\text{total}} > 1.0$) incrementando los tiempos de cómputo $C_i$ de las tareas periódicas sin alterar sus períodos $T_i$:

| VM en Proxmox | Tarea | Prioridad ($P_i$) | Cómputo ($C_i$) | Período ($T_i$) | Plazo ($D_i$) | Utilización ($U_i$) | Peor Caso RTA ($R_i$) | ¿Cumple $R_i \le D_i$? |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`vm1-tau1` (101)** | $\tau_1$ | **5** (Máxima) | `1.5` | `6.0` | `6.0` | `0.2500` | `1.5` | **SÍ** ($1.5 \le 6.0$) |
| **`vm2-tau2` (102)** | $\tau_2$ | **4** | `2.0` | `10.0` | `10.0` | `0.2000` | `3.5` | **SÍ** ($3.5 \le 10.0$) |
| **`vm5-ss` (105)** | **$S_s$** | **3 (Servidor)** | **`2.0`** | **`12.0`** | **`12.0`** | **`0.1667`** | **`5.5`** | **SÍ ($5.5 \le 12.0$)** |
| **`vm3-tau3` (103)** | $\tau_3$ | **2** | `4.0` | `15.0` | `15.0` | `0.2667` | `16.5` | **NO ($16.5 > 15.0$ — Violación de Plazo)** |
| **`vm4-tau4` (104)** | $\tau_4$ | **1** (Mínima) | `6.0` | `20.0` | `20.0` | `0.3000` | `> 20.0` | **NO ($> 20.0$ — Violación / Inanición)** |

* **Utilización Total en Sobrecarga:**
  $$U_{\text{total}} = \frac{1.5}{6} + \frac{2.0}{10} + \frac{2.0}{12} + \frac{4.0}{15} + \frac{6.0}{20} = 0.2500 + 0.2000 + 0.1667 + 0.2667 + 0.3000 = \mathbf{1.1833 > 1.0}$$
* **Comportamiento Observado en Proxmox bajo Sobrecarga ($U_{\text{total}} > 1.0$):**
  1. **Aislamiento de Prioridades Superiores ($\tau_1, \tau_2$) y del Servidor Esporádico ($S_s$):** Dado que la utilización acumulada hasta el nivel de prioridad $P_s = 3$ es $U_{1..s} = 0.25 + 0.20 + 0.1667 = 0.6167 \le 1.0$, tanto `vm1-tau1` como `vm2-tau2` y el Servidor Esporádico `vm5-ss` continúan cumpliendo sus plazos ($R_s = 5.5 \le 12.0$). Cuando la CPU queda libre de $\tau_1$ y $\tau_2$, `vm5-ss` despacha los trabajos aperiódicos $J_k$ al **100% CPU**, consume presupuesto $C_s(t)$ y programa recargas $t_R = t_A + 12.0$.
  2. **Violación de Plazos (*Deadline Misses*) e Inanición en Tareas Inferiores ($\tau_3, \tau_4$):** La interferencia combinada de $\tau_1, \tau_2$ y $S_s$ provoca que `vm3-tau3` exceda su plazo ($R_3 = 16.5 > 15.0$) y que `vm4-tau4` sufra inanición severa (*starvation*). En el monitor web de Proxmox se observa cómo `vm4-tau4` casi no recibe tiempo de CPU al 100%, y la consola de `vm5-ss` emite las alertas en color rojo de **SISTEMA NO PLANIFICABLE ($U_{\text{total}} > 1.0$)** y registra cada `deadline_miss` en el diagrama de Gantt y en el dashboard HTML5.

---

### 5.3 Estructura Dinámica del JSON y Cómo Modificarlo para Provocar Sobrecarga ($U > 1.0$)

Los demonios trabajadores en las Micro-VMs `101..104` no tienen tiempos de cómputo ni períodos cableados (*hardcoded*); actúan como ejecutores puros gobernados en tiempo real por el archivo JSON cargado en `vm5-sporadic-server` (`--tasks-json`). Por lo tanto, **para provocar o eliminar una condición de sobrecarga ($U > 1.0$) basta con editar los valores de `compute_time`, `period` o `budget` en el archivo JSON antes de lanzar la corrida**, sin necesidad de reiniciar ni modificar las Micro-VMs:

```json
{
  "_comment": "Archivo editable de configuracion de cargas para las 5 Micro-VMs en Proxmox VE 9.1-1",
  "horizon": 30.0,
  "dt": 0.1,
  "periodic_tasks": [
    {"name": "$\\tau_1$", "task_id": "tau1", "vm_ip": "10.10.10.101", "priority": 5, "compute_time": 1.5, "deadline": 6.0,  "period": 6.0,  "color": "#0072B2"},
    {"name": "$\\tau_2$", "task_id": "tau2", "vm_ip": "10.10.10.102", "priority": 4, "compute_time": 2.0, "deadline": 10.0, "period": 10.0, "color": "#009E73"},
    {"name": "$\\tau_3$", "task_id": "tau3", "vm_ip": "10.10.10.103", "priority": 2, "compute_time": 4.0, "deadline": 15.0, "period": 15.0, "color": "#CC79A7"},
    {"name": "$\\tau_4$", "task_id": "tau4", "vm_ip": "10.10.10.104", "priority": 1, "compute_time": 6.0, "deadline": 20.0, "period": 20.0, "color": "#E69F00"}
  ],
  "server": {
    "name": "$S_s$",
    "vm_ip": "10.10.10.105",
    "priority": 3,
    "budget": 2.0,
    "period": 12.0,
    "deadline": 12.0,
    "color": "#16a085"
  },
  "aperiodic_jobs": [
    {"name": "J1", "arrival_time": 2.0,  "compute_time": 1.0},
    {"name": "J2", "arrival_time": 5.0,  "compute_time": 1.0},
    {"name": "J3", "arrival_time": 9.0,  "compute_time": 2.0},
    {"name": "J4", "arrival_time": 13.0, "compute_time": 1.0},
    {"name": "J5", "arrival_time": 17.0, "compute_time": 1.5},
    {"name": "J6", "arrival_time": 22.0, "compute_time": 1.0}
  ]
}
```

> **Fórmula rápida para experimentar con el archivo JSON:**  
> Al modificar cualquier `"compute_time"` ($C_i$), `"period"` ($T_i$) o `"budget"` ($C_s$), el analizador en `vm5-sporadic-server` evalúa automáticamente antes de iniciar:
> * **Caso 1 ($U_{\text{total}} \le U_{LL}(5) \approx 0.7435$):** Garantizado planificable por Liu & Layland y RTA (alerta verde en consola).
> * **Caso 2 ($0.7435 < U_{\text{total}} \le 1.0$):** Zona intermedia; `compute_schedulability()` ejecuta la iteración de punto fijo RTA ($R_i^{(k+1)}$) e indica exactamente qué tarea cumple ($R_i \le D_i$) o falla ($R_i > D_i$).
> * **Caso 3 ($U_{\text{total}} > 1.0$):** Sobrecarga estricta; emite alerta crítica en consola y permite observar en vivo en las gráficas de Proxmox qué Micro-VMs de baja prioridad sufren violación de plazo o inanición.

---

### 5.4 Secuencia de Trabajos Aperiódicos ($J_1 \dots J_6$) y Eventos Clave a Observar
Con la secuencia de trabajos aperiódicos del repositorio base (`J1` en $t=2.0$, `J2` en $t=5.0$, `J3` en $t=9.0$, `J4` en $t=13.0$, `J5` en $t=17.0$, `J6` en $t=22.0$), se producirán los siguientes eventos clave en las gráficas de Proxmox:
1. **$t = 2.0$ (`J1` arriba con $C=1.0$):** $\tau_1$ y $\tau_2$ ya concluyeron su primera instancia ($t=0..2$). `vm5-ss` activa su ventana ($t_A = 2.0$), **desaloja a `vm3-tau3`**, sube al **100% CPU** durante $1.0$ unidad ($t=2.0 \to 3.0$), vacía la cola y programa recarga de $+1.0$ en $t_R = 2.0 + 12.0 = 14.0$.
2. **$t = 5.0$ (`J2` arriba con $C=1.0$, presupuesto restante $C_s = 1.0$):** `vm5-ss` abre ventana ($t_A = 5.0$), **desaloja a `vm4-tau4`**, sube al **100% CPU** ($t=5.0 \to 6.0$), agota su presupuesto ($C_s = 0.0$) y programa recarga de $+1.0$ en $t_R = 5.0 + 12.0 = 17.0$.
3. **$t = 9.0$ (`J3` arriba con $C=2.0$, pero $C_s = 0.0$):** `vm5-ss` se encuentra **SUSPENDIDO ($C_s = 0$)**. Aunque `J3` permanece en cola, `vm5-ss` se mantiene al **0% CPU** respetando la ejecución de `vm3` y `vm4` hasta que se efectúa la primera recarga en $t_R = 14.0$.

---

## 6. Especificación Técnica de los Componentes de Software

### 6.1 Proceso Quemador de CPU (`CPUBurnerProcess`)
En cada una de las 5 Micro-VMs se inicia un subproceso dedicado en Python puro (sin dependencias externas) que ejecuta un bucle aritmético intensivo en coma flotante, controlado por señales POSIX del sistema operativo:
* **Al iniciar:** El subproceso se crea e inmediatamente recibe `os.kill(pid, signal.SIGSTOP)`. El kernel de Linux lo retira de la cola de ejecución $\rightarrow$ **0.0% CPU**.
* **Al recibir turno (`start_burning()`):** El agente envía `os.kill(pid, signal.SIGCONT)` $\rightarrow$ el vCPU de la VM sube inmediatamente al **100.0% CPU**.
* **Al ser desalojado o terminar (`stop_burning()`):** El agente envía `os.kill(pid, signal.SIGSTOP)` $\rightarrow$ el vCPU cae instantáneamente al **0.0% CPU**.

### 6.2 Protocolo de Control en Tiempo Real (TCP `NDJSON` con `TCP_NODELAY`)
* **Puerto TCP `9000` en `vm5-sporadic-server`:**
  1. **Fase de Registro (`Handshake`):** Cada VM periódica (`vm1`..`vm4`) se conecta al iniciar y transmite:
     ```json
     {"type": "REGISTER", "task_id": "tau1", "hostname": "vm1-tau1"}
     ```
  2. **Comandos en cada cambio de contexto (sin sobrecarga de red en ticks sin cambio):**
     * `{"type": "GRANT_CPU", "logical_time": 6.0, "slice_label": "tau1"}` $\rightarrow$ La VM destino activa su `CPUBurnerProcess` (`SIGCONT`).
     * `{"type": "PREEMPT_CPU", "logical_time": 6.0}` $\rightarrow$ La VM desalojada congela su `CPUBurnerProcess` (`SIGSTOP`).
     * `{"type": "SHUTDOWN"}` $\rightarrow$ Al finalizar el horizonte de simulación, detiene limpiamente los demonios.

### 6.3 Portal Web en Vivo, Dashboard HTML5 e Inyección de Trabajos Aperiódicos ($J_k$)
1. **Modo Escenario JSON Automático:** Lee `aperiodic_jobs` desde `proxmox_schedulable.json` (o `proxmox_unschedulable.json`) e inyecta $J_1 \dots J_6$ en los instantes exactos $r_k$.
2. **Portal Web en Vivo y Dashboard Integrado en la Micro-VM 105 (`http://localhost:8080/`):** `vm5-sporadic-server` levanta un servidor web ligero en el puerto `8080` accesible directamente desde el navegador local a través del túnel SSH (`-L 8080:10.10.10.105:8080`), sin necesidad de descargar archivos manualmente por `scp`:
   * **`GET /` (`http://localhost:8080/`):** Portal en tiempo real que muestra las 5 Micro-VMs (`101..105`) con su estado `100% CPU` vs. `0% CPU`, barra en vivo del presupuesto $C_s(t)$, ventana activa $t_A$, próxima recarga $t_R = t_A + T_s$, contador de *Deadline Misses* y un formulario interactivo para **inyectar trabajos aperiódicos ($J_k$) con un clic**.
   * **`GET /dashboard` (`http://localhost:8080/dashboard`):** Sirve directamente en el navegador el **Dashboard Interactivo HTML5/SVG** generado en la VM 105.
   * **`GET /gantt.png` (`http://localhost:8080/gantt.png`):** Sirve directamente el diagrama de Gantt y la curva de presupuesto $C_s(t)$ en alta resolución.
   * **`GET /job` y `GET /status`:** Endpoints JSON para automatización o disparo por CLI (`curl "http://localhost:8080/job?name=J_extra&work=1.5"`).

### 6.4 Telemetría CLI en Vivo (`>_ Shell` de Proxmox) e Integración con `sporadic_server_sim.py`
Durante la corrida en vivo (`--time-scale K`), `vm5-sporadic-server` actualiza tick a tick una barra de estado en la consola de Proxmox (`>_ Shell`) indicando el reloj lógico $t$, la Micro-VM activa al `100% CPU`, la barra de presupuesto $C_s(t)$, los instantes $t_A$ / $t_R$ y las violaciones de plazo. Al concluir el horizonte, empaqueta la traza real en un objeto `SimulationResult` idéntico al de [`sporadic_server_sim.py`](https://github.com/elnemesisdivina/sporadic-server-simulation/blob/main/sporadic_server_sim.py), imprime la tabla RTA en consola y publica automáticamente el Dashboard HTML5 y el diagrama PNG en `http://localhost:8080/`.

---

## 7. Estructura de Archivos a Desarrollar

| Archivo | Rol y Descripción |
| :--- | :--- |
| **`proxmox_ss_cluster.py`** | Script principal unificado: opera como **Coordinador + Servidor Esporádico** (`--role server` en VM 105) o como **Nodo de Tarea Periódica** (`--role worker --task-id tau1..tau4` en VMs 101–104), incluye el quemador de CPU real (`SIGSTOP`/`SIGCONT`), telemetría CLI en vivo y el portal web en el puerto `:8080` (`/`, `/dashboard`, `/gantt.png`, `/job`, `/status`). |
| **`proxmox_schedulable.json`** | Configuración JSON para las 4 Micro-VMs periódicas ($\tau_1 \dots \tau_4$) + Servidor Esporádico ($S_s$) + trabajos aperiódicos ($J_1 \dots J_6$) con $U_{\text{total}} = 0.6333 \le 1.0$ (0 violaciones de plazo). |
| **`proxmox_unschedulable.json`** | Configuración JSON de sobrecarga ($U_{\text{total}} > 1.0$) para demostrar violaciones de plazo y saturación de CPU en las 5 Micro-VMs de Proxmox. |
| **`setup_proxmox_vms.sh`** | Script automatizado para **Proxmox VE 9.1-1** (`qm`) con interfaz CLI enriquecida mediante [`boxes`](https://github.com/ascii-boxes/boxes), emojis UTF-8 (`🚀`, `✅`, `🧹`) y barra de progreso dinámica (`0%` a `100%`). Soporta aprovisionamiento completo desde cero y modo de destrucción/limpieza total mediante la bandera `--cleanup`. |
| **`proxmox_ss_cluster_test.py`** | Suite de pruebas automatizadas (`unittest`) que levanta los 5 nodos en procesos locales mediante sockets reales (`127.0.0.1`), valida el protocolo TCP, el quemador de CPU (`SIGSTOP`/`SIGCONT`), el portal HTTP `:8080` y los invariantes matemáticos de `sporadic_server_sim.py`. |

---

## 8. Creación del Clúster Desde Cero y Ejecución Remota (Host Proxmox 9.1-1 Vacío)

Cuando el servidor remoto **Proxmox VE 9.1-1 se encuentra completamente vacío (0 VMs creadas inicialmente)**, **no se requiere instalar ningún sistema operativo de forma manual ni abrir 5 consolas por separado**. Todo el flujo se controla de manera centralizada hacia `<IP_HOST_SERVER>` a través del puerto `<PORT_HOST_SERVER>`.

### Flujo Automatizado "Zero-to-Cluster"

```text
[Equipo Local]                    [Host Proxmox VE 9.1-1]                [Plantilla VM 9000]       [5 Micro-VMs (101..105)]
       |                                     |                                    |                            |
       |-- 0. Tunel SSH (:1234, :8080) ----->|                                    |                            |
       |-- 1. scp scripts + ejecutar ------->|                                    |                            |
       |      ./setup_proxmox_vms.sh         |-- 2. wget Alpine 3.21 Virt .qcow2  |                            |
       |                                     |-- 3. qm create + import-from ----->|                            |
       |                                     |      + cloudinit + qm template     |                            |
       |                                     |-- 4. qm clone 9000 -> 101..105 --->|--- Linked Clones (< 3s) -->|
       |                                     |      (Cores 1..5, RAM 128/256MB,   |                            |
       |                                     |       IPs 10.10.10.101..105)       |                            |
       |                                     |-- 5. qm start + desplegar codigo ->|--------------------------->|
       |<-- 6. Visualizar en vivo en --------|<-----------------------------------|----------------------------|
       |       https://localhost:1234 (Proxmox GUI + >_ Shell)                    |                            |
       |       http://localhost:8080  (Portal en Vivo + /dashboard + /gantt.png)  |                            |
```

### Paso 0: Establecer el Túnel SSH y Subir los Archivos al Host Proxmox Remoto
> Ejecutar desde una terminal en el equipo local para habilitar el acceso web a Proxmox (`https://localhost:1234`), el portal en vivo del Servidor Esporádico (`http://localhost:8080`) y transferir los scripts al servidor remoto (o descargarlos directamente desde GitHub sin requerir `git` en Proxmox):

```bash
# 1. En una pestaña de terminal en tu equipo local, levantar el túnel SSH (-L 1234 y -L 8080):
#    - Puerto local 1234 -> Interfaz Web de Proxmox (https://localhost:8006)
#    - Puerto local 8080 -> Portal Web y Dashboard en VM 105 (10.10.10.105:8080)
ssh -p <PORT_HOST_SERVER> -L 1234:localhost:8006 -L 8080:10.10.10.105:8080 -N -C root@<IP_HOST_SERVER>

# 2. Abrir en el navegador local:
#    - Interfaz Web de Proxmox VE 9.1-1 : https://localhost:1234
#    - Portal en Vivo y Dashboard VM 105: http://localhost:8080/

# 3. En la consola del Host Proxmox (por SSH o >_ Shell), descargar la última versión del repositorio sin requerir git:
mkdir -p /root/sporadic-server-VMs-simulation
wget -qO- https://github.com/elnemesisdivina/sporadic-server-VMs-simulation/archive/refs/heads/main.tar.gz | tar -xz -C /tmp
cp -f /tmp/sporadic-server-VMs-simulation-main/* /root/sporadic-server-VMs-simulation/
rm -rf /tmp/sporadic-server-VMs-simulation-main
cd /root/sporadic-server-VMs-simulation
```

### Paso 1: Creación Automatizada desde Cero en el Host Proxmox 9.1-1 (`setup_proxmox_vms.sh`)
> Conectarse por SSH al servidor (`ssh -p <PORT_HOST_SERVER> root@<IP_HOST_SERVER>`) y ejecutar el script [`setup_proxmox_vms.sh`](setup_proxmox_vms.sh) incluido en este repositorio. Dado que Proxmox VE 9.1-1 se basa en Debian 13 (*Trixie*) con soporte UTF-8 nativo en consola SSH y `xterm.js`, el script instala automáticamente el paquete oficial [`boxes`](https://github.com/ascii-boxes/boxes) (`apt-get install -y boxes`), despliega encabezados estilizados en ASCII, muestra una **barra de progreso dinámica (`0%` a `100%`)** en cada etapa y soporta la opción `--cleanup` para revertir todos los cambios:

```bash
# Conectarse al Host Proxmox VE 9.1-1 y aprovisionar las 5 Micro-VMs:
ssh -p <PORT_HOST_SERVER> root@<IP_HOST_SERVER> "chmod +x /root/setup_proxmox_vms.sh && bash /root/setup_proxmox_vms.sh"
```

> Se visualizará en la consola del Host Proxmox VE 9.1-1 la salida con el estilo `boxes -d critical` (pestaña `CRITICAL` con marco exterior y bloque sólido ANSI) junto con las barras dinámicas del `0%` al `100%`:

```text
  ▄▄▄▄▄▄▄▄▄▄
  CRITICAL  ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
██                                                                               ██
██  DESPLIEGUE CLUSTER SERVIDOR ESPORADICO (5 Micro-VMs Stripped KVM)            ██
██  Maintainer: Ray <elnemesisdivina@gmail.com> | Host: Proxmox VE 9.1-1         ██
██                                                                               ██
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
✅ Verificando llave SSH del Host Proxmox       [████████████████████████████] 100%
✅ Descargando Alpine 3.21 Virt Cloud (.qcow2)  [████████████████████████████] 100%
✅ Creando Plantilla Base Stripped KVM (VM 9000)[████████████████████████████] 100%
✅ Clonando e iniciando VM 101 (vm1-tau1 | #1)  [████████████████████████████] 100%
✅ Clonando e iniciando VM 102 (vm2-tau2 | #2)  [████████████████████████████] 100%
✅ Clonando e iniciando VM 103 (vm3-tau3 | #3)  [████████████████████████████] 100%
✅ Clonando e iniciando VM 104 (vm4-tau4 | #4)  [████████████████████████████] 100%
✅ Clonando e iniciando VM 105 (vm5-ss   | #5)  [████████████████████████████] 100%
  ▄▄▄▄▄▄▄▄▄▄
  CRITICAL  ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
██                                                                               ██
██  Cluster de 5 Micro-VMs activo (768 MB RAM total | Cores 1..5 anclados)       ██
██                                                                               ██
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
```

> [!IMPORTANT]
> **¿Cómo se comunican las 5 Micro-VMs si no existe un router DHCP en la red?**  
> Mediante `--ipconfig0 "ip=10.10.10.101/24"` de Cloud-Init sobre el switch virtual `vmbr0` de Proxmox, las 5 VMs quedan conectadas en Capa 2 con IPs fijas (`10.10.10.101` a `10.10.10.105`) desde el primer segundo de arranque, sin depender de un servidor DHCP externo. Adicionalmente, para instalar `python3` vía `apk`, el script configura automáticamente el enlace de salida o preinstala el paquete en la imagen base antes de clonar.

### Paso 2: Despliegue Automático del Código desde el Host Proxmox hacia las 5 Micro-VMs
> Una vez iniciadas las 5 Micro-VMs (`10.10.10.101` .. `10.10.10.105`, que ya incluyen `python3` preinstalado de fábrica en la imagen Alpine 3.21 Cloud sin requerir salida a Internet), transferir los archivos (`scp`) desde el Host Proxmox y levantar los 4 demonios trabajadores en segundo plano (`ssh` con `killall python3` y redirección `< /dev/null`):

```bash
cd /root/sporadic-server-VMs-simulation

# 1. Copiar archivos del Servidor Esporádico a la Micro-VM 105:
scp -o StrictHostKeyChecking=no \
  proxmox_ss_cluster.py sporadic_server_sim.py \
  proxmox_schedulable.json proxmox_unschedulable.json \
  root@10.10.10.105:/root/

# 2. Copiar proxmox_ss_cluster.py a las 4 Micro-VMs periódicas (101..104) e iniciar los workers:
for i in 1 2 3 4; do
  VM_IP="10.10.10.$((100 + i))"
  scp -o StrictHostKeyChecking=no proxmox_ss_cluster.py root@${VM_IP}:/root/
  ssh -o StrictHostKeyChecking=no root@${VM_IP} "killall python3 2>/dev/null || true"
  ssh -o StrictHostKeyChecking=no root@${VM_IP} \
    "nohup python3 /root/proxmox_ss_cluster.py --role worker --task-id tau${i} --server-host 10.10.10.105 < /dev/null > /root/worker.log 2>&1 &"
done
```

### Paso 3: Iniciar el Servidor Esporádico y Coordinador en la Micro-VM 105 (Caso Planificable vs. Caso No Planificable)
> Ejecutar desde el Host Proxmox hacia `vm5-sporadic-server` (`10.10.10.105`) seleccionando la escala temporal (`--time-scale`) y el archivo JSON deseado:
> * **`--time-scale 4.0` (Corrida rápida de 2 minutos):** Ideal para verificar rápidamente las tablas RTA, el portal en vivo `http://localhost:8080/` y el Dashboard HTML5 `http://localhost:8080/dashboard`.
> * **`--time-scale 15.0` (Modo Visualización de CPU en Proxmox VE):** Dado que el demonio `pvestatd` de Proxmox VE muestrea el uso de CPU cada **10 segundos** (y las gráficas RRD promedian cada **60 segundos**), usar `--time-scale 15.0` hace que cada $1.0\text{ u}$ de cómputo dure **15 segundos reales al 100% CPU**, permitiendo ver los picos al **100% CPU** con total nitidez directamente en los paneles de CPU de Proxmox.

```bash
# Opción A: Modo Visualización de CPU en Proxmox VE (--time-scale 15.0 -> 1.0u = 15s reales al 100% CPU):
ssh -t -o StrictHostKeyChecking=no root@10.10.10.105 \
  "python3 /root/proxmox_ss_cluster.py --role server --tasks-json /root/proxmox_schedulable.json --time-scale 15.0 --no-show --export-csv /root/proxmox_schedulable_trace.csv --export-json /root/proxmox_schedulable_data.json"

# Opción B: Corrida Rápida Caso Planificable (U_total = 0.6333 <= 1.0, --time-scale 4.0):
ssh -t -o StrictHostKeyChecking=no root@10.10.10.105 \
  "python3 /root/proxmox_ss_cluster.py --role server --tasks-json /root/proxmox_schedulable.json --time-scale 4.0 --no-show --export-csv /root/proxmox_schedulable_trace.csv --export-json /root/proxmox_schedulable_data.json"

# Opción C: Caso No Planificable / Sobrecarga (U_total = 1.1833 > 1.0, violaciones en tau3 y tau4):
#    (Reiniciar antes los 4 workers del Paso 2 y lanzar con proxmox_unschedulable.json):
ssh -t -o StrictHostKeyChecking=no root@10.10.10.105 \
  "python3 /root/proxmox_ss_cluster.py --role server --tasks-json /root/proxmox_unschedulable.json --time-scale 15.0 --no-show --export-csv /root/proxmox_unschedulable_trace.csv --export-json /root/proxmox_unschedulable_data.json"
```

### Paso 4: Cómo Visualizar el Consumo de CPU (`0%` vs. `100%`) y el Presupuesto $C_s(t)$ Directamente en Proxmox VE y en el Portal Web
> Para observar de forma concluyente cómo las Micro-VMs `101..104` ejecutan las tareas periódicas ($\tau_1 \dots \tau_4$) según su prioridad Rate-Monotonic y cómo `vm5-sporadic-server` (`105`) sólo consume CPU cuando tiene trabajos aperiódicos ($J_k$) y presupuesto $C_s(t) > 0$, seguir estos pasos mientras corre el comando con `--time-scale 15.0`:
>
> 1. **En el Panel de CPU de Proxmox VE (`https://localhost:1234`):**
>    * **Vista comparativa en tiempo real de las 5 Micro-VMs (`Datacenter` $\rightarrow$ `Search`):** En el árbol izquierdo de Proxmox, hacer clic en **`Datacenter`** (o en el nodo `pve`) y seleccionar **`Search`**. Se mostrarán las barras de **`CPU Usage`** de las 5 VMs (`101..105`) una debajo de la otra actualizándose en vivo:
>      * En $t = 0.0 \to 1.0\text{ u}$ (`15s` reales): **`101 (vm1-tau1)`** sube al **`100% CPU`** mientras `102, 103, 104, 105` están al **`0% CPU`**.
>      * En $t = 1.0 \to 2.0\text{ u}$ (`15s` reales): **`101`** cae al **`0%`** y **`102 (vm2-tau2)`** sube al **`100% CPU`**.
>      * En $t = 2.0 \to 3.0\text{ u}$ (llega `J1` o un trabajo aperiódico inyectado): **`105 (vm5-sporadic-server)`** desaloja a `103`/`104` (porque $P_s=3 > P_3=2 > P_4=1$), sube al **`100% CPU`** y descuenta su presupuesto $C_s(t)$ de `2.0` a `1.0`.
>      * En cuanto $C_s(t)$ llega a **`0.0`**, **`105 (vm5-sporadic-server)`** cae inmediatamente al **`0% CPU`** (`SIGSTOP`), incluso si hay decenas de trabajos aperiódicos esperando en la cola FIFO, y permanece en `0%` hasta el instante exacto de su recarga $t_R = t_A + 12.0$.
>    * **Gráfica histórica de CPU (`105 (vm5-sporadic-server)` $\rightarrow$ `Summary`):** En el desplegable superior derecho de la gráfica **`CPU usage`**, cambiar de **`Average`** a **`Maximum`** (junto a `Hour`). Esto evita que Proxmox promedie bloques de 60 segundos y muestra los escalones cuadrados de activación del Servidor Esporádico.
> 2. **En la Consola `>_ Shell` donde corre el Paso 3:**
>    * Se actualiza tick a tick la barra de telemetría sincronizada con Proxmox:
>      ```text
>      ⚡ [t= 14.0/30.0s] VM 105 (vm5-ss -> J3) [100% CPU] | C_s:[████░░░░] 1.0/2.0 | t_A:14.0s | t_R:17.0s | Cola:2 | Miss:0
>      ```
> 3. **En el Portal Web y Dashboard servidos desde la Micro-VM 105 (`http://localhost:8080/`):**
>    * **`http://localhost:8080/`:** Muestra los 5 indicadores de las Micro-VMs (`100% CPU` en verde cuando tienen el token vs. `0% CPU` en reposo), la barra de presupuesto $C_s(t)$ y el botón **`⚡ Inyectar Jₖ`** (que numera automáticamente cada trabajo inyectado como `J_urgente_1`, `J_urgente_2`, etc.).
>    * **`http://localhost:8080/dashboard`:** Al concluir los $30.0\text{ s}$ lógicos, abre el reproductor interactivo HTML5 con el diagrama de Gantt de las 5 VMs, la curva $C_s(t)$ diente de sierra y la tabla de trabajos aperiódicos diferenciando en verde los trabajos **`Completed`** y en ámbar los **`Incomplete`** que quedaron esperando recarga de presupuesto al cierre del horizonte.

### Paso 5: Limpieza Total del Clúster en Proxmox (`--cleanup`)
> Una vez concluidas las pruebas, invocar el mismo script con la bandera `--cleanup` en el Host Proxmox para detener y purgar las 5 Micro-VMs (`101..105`), la plantilla base (`9000`) y los discos asociados, devolviendo el hipervisor a su estado original (`0 VMs`):

```bash
bash /root/setup_proxmox_vms.sh --cleanup
```

> Se visualizará en la CLI del Host Proxmox VE 9.1-1 el progreso de desmantelamiento con `boxes -d critical` y barras al `100%`:

```text
  ▄▄▄▄▄▄▄▄▄▄
  CRITICAL  ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
██                                                                               ██
██  MODO LIMPIEZA PROXMOX VE 9.1-1 | Maintainer: Ray <elnemesisdivina@gmail.com> ██
██                                                                               ██
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
✅ Eliminando VM/Plantilla 101                  [████████████████████████████] 100%
✅ Eliminando VM/Plantilla 102                  [████████████████████████████] 100%
✅ Eliminando VM/Plantilla 103                  [████████████████████████████] 100%
✅ Eliminando VM/Plantilla 104                  [████████████████████████████] 100%
✅ Eliminando VM/Plantilla 105                  [████████████████████████████] 100%
✅ Eliminando VM/Plantilla 9000                 [████████████████████████████] 100%
✅ Eliminando imagen temporal en /tmp           [████████████████████████████] 100%
  ▄▄▄▄▄▄▄▄▄▄
  CRITICAL  ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
██                                                                               ██
██  Limpieza completada: 0 Micro-VMs activas en el Host Proxmox                  ██
██                                                                               ██
▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
```

---

## 9. Autoría y Licencia

* **Versión:** `1.0.0`
* **Maintainer:** Ray (`<elnemesisdivina@gmail.com>`)
* **Repositorio Base:** [`elnemesisdivina/sporadic-server-simulation`](https://github.com/elnemesisdivina/sporadic-server-simulation)
* **Licencia:** Distribuido bajo la **Licencia MIT**. Consultar el archivo [`LICENSE`](LICENSE) para más información.
