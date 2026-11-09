#!/usr/bin/env bash
# ==============================================================================
# Script: setup_proxmox_vms.sh
# Versión: 1.0.0
# Licencia: MIT
# Maintainer: Ray <elnemesisdivina@gmail.com>
# Descripción: Aprovisionamiento automatizado desde cero y limpieza (--cleanup)
#              de 5 Stripped KVM Micro-VMs (Alpine Linux 3.21 Virt) en
#              Proxmox VE 9.1-1 para la simulación del Servidor Esporádico.
# Uso:
#   Crear clúster : bash setup_proxmox_vms.sh
#   Limpiar todo  : bash setup_proxmox_vms.sh --cleanup
# ==============================================================================
set -euo pipefail

BRIDGE="${BRIDGE:-vmbr0}"
SUBNET_PREFIX="10.10.10"
HOST_GW_IP="${SUBNET_PREFIX}.1"
ALPINE_IMG="generic_alpine-3.21.2-x86_64-bios-cloudinit-r0.qcow2"
ALPINE_URL="https://dl-cdn.alpinelinux.org/alpine/v3.21/releases/cloud/${ALPINE_IMG}"
ALL_VMIDS=(101 102 103 104 105 9000)
STEP_LOG="/tmp/proxmox_ss_step.log"

# ── 1. Verificar ejecución en nodo Proxmox VE como root ──────────────────────
if [[ "${EUID}" -ne 0 ]]; then
  echo "❌ Error: Ejecutar este script como usuario root en el Host Proxmox VE." >&2
  exit 1
fi

if ! command -v qm >/dev/null 2>&1; then
  echo "❌ Error: No se encontró el comando 'qm'. Ejecutar dentro del Host Proxmox VE 9.1-1." >&2
  exit 1
fi

# ── 2. Asegurar soporte de ascii-boxes (https://github.com/ascii-boxes/boxes) ─
if ! command -v boxes >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq >/dev/null 2>&1 || true
  apt-get install -y -qq boxes >/dev/null 2>&1 || true
fi

render_box() {
  if command -v boxes >/dev/null 2>&1 && printf "test\n" | boxes -d critical >/dev/null 2>&1; then
    printf "%b\n" "$1" | boxes -d critical
  else
    printf "\033[38;5;203m  ▄▄▄▄▄▄▄▄▄▄\033[0m\n"
    printf "\033[48;5;203;97;1m  CRITICAL  \033[0m\033[38;5;203m▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄\033[0m\n"
    while IFS= read -r line; do
      printf "\033[48;5;203m \033[48;5;52;97m  %-74s  \033[48;5;203m \033[0m\n" "${line}"
    done < <(printf "%b\n" "$1")
    printf "\033[38;5;203m▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀\033[0m\n"
  fi
}

# ── 3. Barra de progreso dinámica (0% -> 100%) con captura de errores ────────
run_with_progress() {
  local label="$1"
  shift
  local bar_width=28
  local pct=0

  : > "${STEP_LOG}"
  "$@" >"${STEP_LOG}" 2>&1 &
  local cmd_pid=$!

  while kill -0 "${cmd_pid}" 2>/dev/null; do
    if [[ "${pct}" -lt 95 ]]; then
      pct=$((pct + 5))
    fi
    local filled=$((pct * bar_width / 100))
    local empty=$((bar_width - filled))
    local filled_str=""
    local empty_str=""
    [[ "${filled}" -gt 0 ]] && printf -v filled_str "%${filled}s" ""
    [[ "${empty}" -gt 0 ]] && printf -v empty_str "%${empty}s" ""
    local bar="${filled_str// /█}${empty_str// /░}"
    printf "\r⏳ %-46s [%s] %3d%%" "${label}" "${bar}" "${pct}"
    sleep 0.15
  done

  if ! wait "${cmd_pid}"; then
    printf "\r❌ %-46s [FALLO]     \n" "${label}" >&2
    echo "── Detalle del error (${STEP_LOG}) ──" >&2
    cat "${STEP_LOG}" >&2
    exit 1
  fi

  local full_str=""
  printf -v full_str "%${bar_width}s" ""
  local full_bar="${full_str// /█}"
  printf "\r✅ %-46s [%s] 100%%\n" "${label}" "${full_bar}"
}

# ── 4. Modo Limpieza (--cleanup): Eliminar VMs 101..105, Plantilla 9000 y Red ─
if [[ "${1:-}" == "--cleanup" ]]; then
  render_box "MODO LIMPIEZA PROXMOX VE 9.1-1 | Maintainer: Ray <elnemesisdivina@gmail.com>"
  for vmid in "${ALL_VMIDS[@]}"; do
    if qm status "${vmid}" >/dev/null 2>&1; then
      run_with_progress "Eliminando VM/Plantilla ${vmid}" \
        bash -c "qm stop ${vmid} --skiplock 1 >/dev/null 2>&1 || true; qm destroy ${vmid} --purge 1 --destroy-unreferenced-disks 1"
    fi
  done
  run_with_progress "Limpiando subred interna ${HOST_GW_IP}/24 en ${BRIDGE}" \
    bash -c "ip addr del ${HOST_GW_IP}/24 dev ${BRIDGE} 2>/dev/null || true"
  run_with_progress "Eliminando archivos temporales en /tmp" \
    rm -f "/tmp/${ALPINE_IMG}" "${STEP_LOG}"
  render_box "Limpieza completada: 0 Micro-VMs activas en el Host Proxmox"
  exit 0
fi

# ── 5. Auto-detección de Almacenamiento Activo en Proxmox (local-lvm/zfs/local)
STORAGE="${STORAGE:-}"
if [[ -z "${STORAGE}" ]]; then
  if pvesm status -content images 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "local-lvm"; then
    STORAGE="local-lvm"
  elif pvesm status -content images 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "local-zfs"; then
    STORAGE="local-zfs"
  else
    STORAGE=$(pvesm status -content images 2>/dev/null | awk 'NR==2 {print $1}')
    STORAGE="${STORAGE:-local}"
  fi
fi

# ── 6. Modo Creación del Clúster (Por defecto) ───────────────────────────────
render_box "DESPLIEGUE CLUSTER SERVIDOR ESPORADICO (5 Micro-VMs Stripped KVM)\nMaintainer: Ray <elnemesisdivina@gmail.com> | Storage: ${STORAGE} | Bridge: ${BRIDGE}"

# Paso A: Configurar llave SSH en /root/.ssh
run_with_progress "Configurando llave SSH en /root/.ssh" \
  bash -c 'mkdir -p /root/.ssh && chmod 700 /root/.ssh && ([ -f /root/.ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N "" -f /root/.ssh/id_ed25519)'

# Paso B: Configurar IP puerta de enlace (10.10.10.1/24) y NAT en vmbr0
run_with_progress "Configurando gateway ${HOST_GW_IP}/24 y NAT en ${BRIDGE}" \
  bash -c "
    ip addr show dev ${BRIDGE} | grep -q '${HOST_GW_IP}/24' || ip addr add ${HOST_GW_IP}/24 dev ${BRIDGE}
    sysctl -w net.ipv4.ip_forward=1 >/dev/null
    iptables -t nat -C POSTROUTING -s ${SUBNET_PREFIX}.0/24 -o ${BRIDGE} -j MASQUERADE 2>/dev/null || \
      iptables -t nat -A POSTROUTING -s ${SUBNET_PREFIX}.0/24 -o ${BRIDGE} -j MASQUERADE
  "

# Paso C: Descargar imagen Alpine 3.21 Virt Cloud (.qcow2) si no existe en caché
run_with_progress "Descargando Alpine 3.21 Virt Cloud (.qcow2)" \
  bash -c "[ -s '/tmp/${ALPINE_IMG}' ] || wget -q -O '/tmp/${ALPINE_IMG}' '${ALPINE_URL}'"

# Paso D: Crear Plantilla Base Stripped KVM (VM 9000) con importación atómica
run_with_progress "Creando Plantilla Base Stripped KVM (VM 9000)" \
  bash -c "
    if qm status 9000 >/dev/null 2>&1; then
      qm stop 9000 --skiplock 1 >/dev/null 2>&1 || true
      qm destroy 9000 --purge 1 --destroy-unreferenced-disks 1
    fi
    qm create 9000 --name alpine-virt-microvm-tpl --ostype l26 \
      --cpu host --sockets 1 --cores 1 --memory 128 --balloon 0 \
      --vga serial0 --serial0 socket --scsihw virtio-scsi-single \
      --net0 virtio,bridge=${BRIDGE}
    qm set 9000 --scsi0 ${STORAGE}:0,import-from=/tmp/${ALPINE_IMG}
    qm set 9000 --boot order=scsi0 --ide2 ${STORAGE}:cloudinit
    qm resize 9000 scsi0 2G
    qm template 9000
  "

# Paso E: Clonar e iniciar las 4 Micro-VMs Periódicas (VM 101..104 -> Cores 1..4, 128 MB RAM)
for i in 1 2 3 4; do
  VMID=$((100 + i))
  run_with_progress "Clonando e iniciando VM ${VMID} (vm${i}-tau${i} | Core #${i})" \
    bash -c "
      if qm status ${VMID} >/dev/null 2>&1; then
        qm stop ${VMID} --skiplock 1 >/dev/null 2>&1 || true
        qm destroy ${VMID} --purge 1 --destroy-unreferenced-disks 1
      fi
      qm clone 9000 ${VMID} --name vm${i}-tau${i} --full 0 || qm clone 9000 ${VMID} --name vm${i}-tau${i} --full 1
      qm set ${VMID} --cpu host --sockets 1 --cores 1 --affinity ${i} --memory 128 --balloon 0 --vga serial0
      qm set ${VMID} --ciuser root --sshkeys /root/.ssh/id_ed25519.pub \
        --ipconfig0 ip=${SUBNET_PREFIX}.${VMID}/24,gw=${HOST_GW_IP} --nameserver 8.8.8.8
      qm start ${VMID}
    "
done

# Paso F: Clonar e iniciar la Micro-VM 105 del Servidor Esporádico (Core 5, 256 MB RAM)
run_with_progress "Clonando e iniciando VM 105 (vm5-ss | Core #5)" \
  bash -c "
    if qm status 105 >/dev/null 2>&1; then
      qm stop 105 --skiplock 1 >/dev/null 2>&1 || true
      qm destroy 105 --purge 1 --destroy-unreferenced-disks 1
    fi
    qm clone 9000 105 --name vm5-sporadic-server --full 0 || qm clone 9000 105 --name vm5-sporadic-server --full 1
    qm set 105 --cpu host --sockets 1 --cores 1 --affinity 5 --memory 256 --balloon 0 --vga serial0
    qm set 105 --ciuser root --sshkeys /root/.ssh/id_ed25519.pub \
      --ipconfig0 ip=${SUBNET_PREFIX}.105/24,gw=${HOST_GW_IP} --nameserver 8.8.8.8
    qm start 105
  "

render_box "Cluster de 5 Micro-VMs activo (768 MB RAM total | Cores 1..5 anclados)"
