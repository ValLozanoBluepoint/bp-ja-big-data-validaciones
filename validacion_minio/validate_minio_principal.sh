#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster MinIO Principal (DC Principal)
#  Nodos : stg-1 | stg-2 | stg-3
#  Versión MinIO : RELEASE.2026-02-07T07-43-34Z
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 9000 (API S3) | 9001 (Console)
#  Directorios: /opt/minio  |  /data/minio  |  /var/log/minio
#  Buckets requeridos: raw | bronze | silver | gold
#
#  Uso:
#    chmod +x validate_minio_principal.sh
#    sudo ./validate_minio_principal.sh [--alias ALIAS] [--skip-write-test]
#
#  Parámetros opcionales:
#    --alias    Alias mc configurado para este cluster (default: minio-principal)
#    --skip-write-test   Omitir prueba de escritura/lectura (solo lectura estructural)
# =============================================================================

set -euo pipefail

# ── Colores y helpers ──────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS="[${GREEN}PASS${NC}]"
FAIL="[${RED}FAIL${NC}]"
WARN="[${YELLOW}WARN${NC}]"
INFO="[${CYAN}INFO${NC}]"

ERRORS=0
WARNINGS=0

pass()  { echo -e "${PASS} $*"; }
fail()  { echo -e "${FAIL} $*"; ERRORS=$((ERRORS+1)); }
warn()  { echo -e "${WARN} $*"; WARNINGS=$((WARNINGS+1)); }
info()  { echo -e "${INFO} $*"; }
header(){ echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════════${NC}"; \
          echo -e "${BOLD}${CYAN}  $*${NC}"; \
          echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════${NC}"; }

# ── Parámetros ─────────────────────────────────────────────────────────────────
MC_ALIAS="minio-principal"
SKIP_WRITE_TEST=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --alias)          MC_ALIAS="$2"; shift 2 ;;
    --skip-write-test) SKIP_WRITE_TEST=true;  shift   ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

# ── Configuración del cluster ──────────────────────────────────────────────────
CLUSTER_NAME="MinIO Principal (DC Principal)"
NODES=("stg-1" "stg-2" "stg-3")
CONTAINER_PATTERN="minio"        # nombre o prefijo del contenedor Podman
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
REQUIRED_DIRS=("/opt/minio" "/data/minio" "/var/log/minio")
REQUIRED_BUCKETS=("raw" "bronze" "silver" "gold")
HOSTNAME_ACTUAL=$(hostname -s)

# Detectar en qué nodo estamos ejecutando
NODE_ROLE="desconocido"
for n in "${NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n"; break; fi
done

# ── Banner ─────────────────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  ██████╗ ██╗     ██╗   ██╗███████╗██████╗  ██████╗ ██╗███╗   ██╗████████╗"
echo "  ██╔══██╗██║     ██║   ██║██╔════╝██╔══██╗██╔═══██╗██║████╗  ██║╚══██╔══╝"
echo "  ██████╔╝██║     ██║   ██║█████╗  ██████╔╝██║   ██║██║██╔██╗ ██║   ██║   "
echo "  ██╔══██╗██║     ██║   ██║██╔══╝  ██╔═══╝ ██║   ██║██║██║╚██╗██║   ██║   "
echo "  ██████╔╝███████╗╚██████╔╝███████╗██║     ╚██████╔╝██║██║ ╚████║   ██║   "
echo "  ╚═════╝ ╚══════╝ ╚═════╝ ╚══════╝╚═╝      ╚═════╝ ╚═╝╚═╝  ╚═══╝   ╚═╝  "
echo -e "${NC}"
echo -e "${BOLD}  Cooperativa Jardín Azuayo — Proyecto Big Data${NC}"
echo -e "  Cluster: ${BOLD}${CYAN}${CLUSTER_NAME}${NC}"
echo -e "  Nodo actual: ${BOLD}${HOSTNAME_ACTUAL}${NC}  (rol detectado: ${NODE_ROLE})"
echo -e "  Alias mc: ${MC_ALIAS}"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""

# =============================================================================
# SECCIÓN 1 — SISTEMA OPERATIVO Y RUNTIME
# =============================================================================
header "1. SISTEMA OPERATIVO Y RUNTIME"

# 1.1 Rocky Linux 10.x
info "1.1 Verificando sistema operativo..."
if grep -qi "rocky linux 10" /etc/os-release 2>/dev/null; then
  OS_VER=$(grep "VERSION=" /etc/os-release | head -1 | tr -d '"' | cut -d= -f2)
  pass "SO: Rocky Linux — $OS_VER"
else
  OS_VER=$(grep "PRETTY_NAME" /etc/os-release 2>/dev/null | cut -d= -f2 | tr -d '"' || echo "no detectado")
  fail "SO esperado: Rocky Linux 10.x — encontrado: $OS_VER"
fi

# 1.2 Kernel
info "1.2 Versión del kernel..."
KERNEL=$(uname -r)
pass "Kernel: $KERNEL"

# 1.3 Podman instalado y versión >= 5.x
info "1.3 Verificando Podman..."
if command -v podman &>/dev/null; then
  POD_VER=$(podman --version | awk '{print $3}')
  POD_MAJ=$(echo "$POD_VER" | cut -d. -f1)
  if [[ "$POD_MAJ" -ge 5 ]]; then
    pass "Podman $POD_VER instalado (>= 5.x requerido)"
  else
    fail "Podman $POD_VER < 5.x requerido"
  fi
else
  fail "Podman no encontrado en PATH"
fi

# 1.4 systemd activo
info "1.4 Verificando systemd..."
if systemctl is-system-running &>/dev/null || systemctl is-system-running | grep -qE "running|degraded"; then
  SYSSTATE=$(systemctl is-system-running 2>/dev/null || echo "unknown")
  [[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"
else
  fail "systemd no está activo"
fi

# 1.5 NTP activo (sincronización obligatoria según guía)
info "1.5 Verificando sincronización NTP..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado"
elif chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización"
fi

# =============================================================================
# SECCIÓN 2 — ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA
# =============================================================================
header "2. ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA"

for dir in "${REQUIRED_DIRS[@]}"; do
  info "Verificando $dir ..."
  if [[ -d "$dir" ]]; then
    OWNER=$(stat -c '%U:%G' "$dir")
    PERMS=$(stat -c '%a' "$dir")
    USAGE=$(df -h "$dir" 2>/dev/null | awk 'NR==2{print $3"/"$2" usado ("$5")"}' || echo "N/A")
    pass "Directorio $dir existe — owner: $OWNER — perms: $PERMS — uso: $USAGE"

    # Verificar que /data/minio es bind mount externo (no overlay)
    if [[ "$dir" == "/data/minio" ]]; then
      FSTYPE=$(df -T "$dir" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
      if [[ "$FSTYPE" == "overlay" ]]; then
        fail "/data/minio usa overlay filesystem — debe ser bind mount sobre partición dedicada"
      else
        pass "/data/minio filesystem: $FSTYPE (no overlay)"
      fi
    fi
  else
    fail "Directorio $dir NO existe"
  fi
done

# Verificar partición /data dedicada
info "Verificando partición /data independiente..."
DATA_MOUNT=$(findmnt -n -o TARGET /data 2>/dev/null || findmnt -n -o TARGET --target /data/minio 2>/dev/null || echo "")
if [[ -n "$DATA_MOUNT" ]]; then
  pass "Partición /data montada en: $DATA_MOUNT"
else
  warn "/data puede no ser una partición independiente — revisar fstab"
fi

# =============================================================================
# SECCIÓN 3 — SERVICIO SYSTEMD Y CONTENEDOR PODMAN
# =============================================================================
header "3. SERVICIO SYSTEMD Y CONTENEDOR PODMAN"

# 3.1 Servicio systemd MinIO habilitado y activo
info "3.1 Verificando unidades systemd de MinIO..."
SYSTEMD_UNITS=$(systemctl list-units --type=service 2>/dev/null | grep -i "minio\|container-minio" | awk '{print $1}' || true)

if [[ -n "$SYSTEMD_UNITS" ]]; then
  while IFS= read -r unit; do
    SVC_STATE=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    SVC_ENABLED=$(systemctl is-enabled "$unit" 2>/dev/null || echo "unknown")
    if [[ "$SVC_STATE" == "active" ]]; then
      pass "Unidad systemd: $unit — activa — habilitada: $SVC_ENABLED"
    else
      fail "Unidad systemd: $unit — estado: $SVC_STATE — habilitada: $SVC_ENABLED"
    fi
  done <<< "$SYSTEMD_UNITS"
else
  warn "No se encontraron unidades systemd con nombre 'minio' — verificar manualmente"
fi

# 3.2 Contenedor Podman corriendo
info "3.2 Verificando contenedor Podman MinIO..."
CONTAINERS=$(podman ps --format "{{.Names}}\t{{.Status}}\t{{.Image}}" 2>/dev/null | grep -i "minio" || true)

if [[ -n "$CONTAINERS" ]]; then
  while IFS=$'\t' read -r cname cstatus cimage; do
    if echo "$cstatus" | grep -qi "up"; then
      pass "Contenedor: $cname — estado: $cstatus — imagen: $cimage"
    else
      fail "Contenedor: $cname — estado: $cstatus (esperado: Up)"
    fi
  done <<< "$CONTAINERS"
else
  fail "No se encontró ningún contenedor MinIO corriendo (podman ps)"
fi

# 3.3 Naming convention: minio-<índice>
info "3.3 Verificando naming convention de contenedores..."
if podman ps --format "{{.Names}}" 2>/dev/null | grep -qE "^minio-[0-9]+$"; then
  CNAME=$(podman ps --format "{{.Names}}" | grep -E "^minio-[0-9]+$")
  pass "Naming convention correcta: $CNAME"
else
  warn "Naming convention esperada: minio-<índice> — revisar nombre del contenedor"
fi

# 3.4 Reinicio automático configurado (--restart=always o RemainAfterExit)
info "3.4 Verificando política de reinicio..."
RESTART_POL=$(podman inspect --format "{{.HostConfig.RestartPolicy.Name}}" \
  $(podman ps -q --filter "name=minio" 2>/dev/null | head -1) 2>/dev/null || echo "N/A")
if [[ "$RESTART_POL" == "always" ]]; then
  pass "Política de reinicio: always"
else
  warn "Política de reinicio: $RESTART_POL (recomendado: always o gestión via systemd)"
fi

# =============================================================================
# SECCIÓN 4 — CONECTIVIDAD DE RED (puertos API y Console)
# =============================================================================
header "4. CONECTIVIDAD DE RED"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || \
     netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando — MinIO puede no estar activo"
  fi
}

info "4.1 Puerto API S3 (${MINIO_API_PORT})..."
check_port_local $MINIO_API_PORT "API S3"

info "4.2 Puerto Console (${MINIO_CONSOLE_PORT})..."
check_port_local $MINIO_CONSOLE_PORT "Console Web"

# 4.3 Conectividad entre nodos del cluster (cross-node)
info "4.3 Conectividad entre nodos del cluster..."
for NODE in "${NODES[@]}"; do
  if [[ "$NODE" == "$NODE_ROLE" ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE: OK (rtt: ${PING_RTT}ms)"
  else
    warn "Ping a $NODE: no resuelve o no responde — verificar DNS/resolución entre nodos"
  fi

  # TCP al puerto API del nodo remoto
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${MINIO_API_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${MINIO_API_PORT} alcanzable"
  else
    fail "TCP ${NODE}:${MINIO_API_PORT} no alcanzable — revisar firewall/networking"
  fi
done

# 4.4 HTTP health check local
info "4.4 Health check HTTP MinIO API..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:${MINIO_API_PORT}/minio/health/live" \
  --connect-timeout 5 --max-time 10 2>/dev/null || echo "000")
if [[ "$HTTP_CODE" == "200" ]]; then
  pass "Health check /minio/health/live: HTTP $HTTP_CODE"
elif [[ "$HTTP_CODE" == "403" ]]; then
  warn "Health check devolvió 403 — MinIO responde pero puede requerir auth"
else
  fail "Health check /minio/health/live: HTTP $HTTP_CODE (esperado 200)"
fi

# Cluster readiness
HTTP_CLUSTER=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:${MINIO_API_PORT}/minio/health/cluster" \
  --connect-timeout 5 --max-time 10 2>/dev/null || echo "000")
if [[ "$HTTP_CLUSTER" == "200" ]]; then
  pass "Cluster health /minio/health/cluster: HTTP $HTTP_CLUSTER — QUÓRUM OK"
else
  fail "Cluster health: HTTP $HTTP_CLUSTER — posible pérdida de quórum o nodo faltante"
fi

# =============================================================================
# SECCIÓN 5 — VALIDACIÓN FUNCIONAL CON mc (MinIO Client)
# =============================================================================
header "5. VALIDACIÓN FUNCIONAL (mc — MinIO Client)"

if ! command -v mc &>/dev/null && ! command -v mcli &>/dev/null; then
  warn "mc/mcli no encontrado en PATH — omitiendo validaciones funcionales con mc"
  warn "Instalar: https://dl.min.io/client/mc/release/linux-amd64/mc"
  SKIP_MC=true
else
  SKIP_MC=false
  MC_CMD=$(command -v mcli 2>/dev/null || command -v mc)
  info "mc encontrado: $($MC_CMD --version 2>/dev/null | head -1)"
fi

if [[ "$SKIP_MC" == false ]]; then

  # 5.1 Alias configurado
  info "5.1 Verificando alias '$MC_ALIAS' configurado..."
  if $MC_CMD alias ls 2>/dev/null | grep -q "^${MC_ALIAS}"; then
    pass "Alias '$MC_ALIAS' existe en configuración mc"
  else
    warn "Alias '$MC_ALIAS' no encontrado — se intentará con URL local"
    info "  Crear con: mc alias set ${MC_ALIAS} http://localhost:9000 <ACCESS_KEY> <SECRET_KEY>"
    MC_ALIAS="http://localhost:${MINIO_API_PORT}"
  fi

  # 5.2 Admin info (estado del cluster distribuido)
  info "5.2 Información del cluster distribuido..."
  ADMIN_INFO=$($MC_CMD admin info "${MC_ALIAS}" 2>/dev/null || echo "ERROR")
  if echo "$ADMIN_INFO" | grep -qi "online"; then
    DRIVES_ONLINE=$(echo "$ADMIN_INFO" | grep -i "drives" | head -1 || echo "ver output")
    pass "Cluster MinIO: nodos online — $DRIVES_ONLINE"
  elif [[ "$ADMIN_INFO" == "ERROR" ]]; then
    warn "No se pudo obtener admin info — verificar credenciales del alias mc"
  else
    fail "Cluster MinIO: estado inesperado — revisar output: $ADMIN_INFO"
  fi

  # 5.3 Erasure coding status
  info "5.3 Verificando erasure coding..."
  EC_INFO=$($MC_CMD admin info "${MC_ALIAS}" 2>/dev/null | grep -i "erasure\|EC:" || echo "no encontrado")
  if echo "$EC_INFO" | grep -qi "erasure\|EC:"; then
    pass "Erasure coding configurado: $EC_INFO"
  else
    warn "No se pudo confirmar erasure coding — verificar con 'mc admin info ${MC_ALIAS}'"
  fi

  # 5.4 Verificar buckets requeridos
  info "5.4 Verificando buckets requeridos: ${REQUIRED_BUCKETS[*]}..."
  for BUCKET in "${REQUIRED_BUCKETS[@]}"; do
    if $MC_CMD ls "${MC_ALIAS}/${BUCKET}" &>/dev/null; then
      pass "Bucket '${BUCKET}' existe y es accesible"
    else
      fail "Bucket '${BUCKET}' NO existe o no es accesible en ${MC_ALIAS}"
    fi
  done

  # 5.5 Prueba de escritura/lectura (si no se omite)
  if [[ "$SKIP_WRITE_TEST" == false ]]; then
    info "5.5 Prueba de escritura/lectura en bucket 'raw'..."
    TEST_FILE="/tmp/bluepoint_minio_validation_$(date +%s).tmp"
    TEST_OBJECT="bluepoint-validation/test-$(date +%s).txt"

    echo "Bluepoint MinIO validation - $(date)" > "$TEST_FILE"

    if $MC_CMD cp "$TEST_FILE" "${MC_ALIAS}/raw/${TEST_OBJECT}" &>/dev/null; then
      pass "Escritura: ${MC_ALIAS}/raw/${TEST_OBJECT} — OK"

      # Verificar con stat
      STAT_OUT=$($MC_CMD stat "${MC_ALIAS}/raw/${TEST_OBJECT}" 2>/dev/null | grep "Size:" || echo "N/A")
      pass "Stat del objeto: $STAT_OUT"

      # Lectura de vuelta
      READBACK=$(mktemp)
      if $MC_CMD cp "${MC_ALIAS}/raw/${TEST_OBJECT}" "$READBACK" &>/dev/null; then
        pass "Lectura del objeto: OK"
      else
        fail "Lectura del objeto: FALLO"
      fi

      # Limpieza
      $MC_CMD rm "${MC_ALIAS}/raw/${TEST_OBJECT}" &>/dev/null && \
        info "Objeto de prueba eliminado."
      rm -f "$TEST_FILE" "$READBACK"
    else
      fail "Escritura: ${MC_ALIAS}/raw/${TEST_OBJECT} — FALLO"
      rm -f "$TEST_FILE"
    fi
  else
    info "5.5 Prueba de escritura omitida (--skip-write-test)"
  fi

fi

# =============================================================================
# SECCIÓN 6 — RESILIENCIA: VERIFICACIÓN DE NODOS DEL CLUSTER
# =============================================================================
header "6. RESILIENCIA Y ESTADO DEL CLUSTER"

info "6.1 Estado de MinIO en cada nodo del cluster..."
for NODE in "${NODES[@]}"; do
  HC=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://${NODE}:${MINIO_API_PORT}/minio/health/live" \
    --connect-timeout 5 --max-time 8 2>/dev/null || echo "000")
  if [[ "$HC" == "200" ]]; then
    pass "Nodo $NODE: /minio/health/live HTTP $HC — UP"
  else
    fail "Nodo $NODE: /minio/health/live HTTP $HC — DOWN o inaccesible"
  fi
done

info "6.2 Verificando que todos los nodos tienen el volumen /data/minio accesible..."
# Solo verificamos en el nodo local (los remotos requieren SSH)
DATA_MINIO_SIZE=$(df -h /data/minio 2>/dev/null | awk 'NR==2{print $2}' || echo "N/A")
DATA_MINIO_AVAIL=$(df -h /data/minio 2>/dev/null | awk 'NR==2{print $4}' || echo "N/A")
if [[ "$DATA_MINIO_SIZE" != "N/A" ]]; then
  pass "/data/minio — tamaño: $DATA_MINIO_SIZE — disponible: $DATA_MINIO_AVAIL"
else
  fail "/data/minio no disponible en este nodo"
fi

# =============================================================================
# SECCIÓN 7 — LOGS Y OBSERVABILIDAD
# =============================================================================
header "7. LOGS Y OBSERVABILIDAD"

# 7.1 Logs en /var/log/minio
info "7.1 Verificando logs en /var/log/minio..."
if [[ -d "/var/log/minio" ]]; then
  LOG_COUNT=$(find /var/log/minio -name "*.log" 2>/dev/null | wc -l)
  if [[ "$LOG_COUNT" -gt 0 ]]; then
    LAST_LOG=$(ls -t /var/log/minio/*.log 2>/dev/null | head -1)
    LAST_MODIFIED=$(stat -c '%y' "$LAST_LOG" 2>/dev/null | cut -d. -f1 || echo "N/A")
    pass "Logs en /var/log/minio: $LOG_COUNT archivo(s) — último modificado: $LAST_MODIFIED"
  else
    warn "Directorio /var/log/minio existe pero sin archivos .log"
  fi
else
  fail "Directorio /var/log/minio NO existe"
fi

# 7.2 Grafana Alloy (agente de observabilidad en nodos stg)
info "7.2 Verificando Grafana Alloy (agente de observabilidad)..."
ALLOY_UNITS=$(systemctl list-units --type=service 2>/dev/null | grep -i "alloy" | awk '{print $1}' || true)
if [[ -n "$ALLOY_UNITS" ]]; then
  while IFS= read -r unit; do
    ALLOY_STATE=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    [[ "$ALLOY_STATE" == "active" ]] && pass "Alloy: $unit — activo" || fail "Alloy: $unit — $ALLOY_STATE"
  done <<< "$ALLOY_UNITS"
else
  # Verificar como contenedor Podman
  ALLOY_CTR=$(podman ps --format "{{.Names}}\t{{.Status}}" 2>/dev/null | grep -i "alloy" || true)
  if [[ -n "$ALLOY_CTR" ]]; then
    pass "Alloy corriendo como contenedor Podman: $ALLOY_CTR"
  else
    warn "Grafana Alloy no encontrado — verificar despliegue del agente de observabilidad"
  fi
fi

# 7.3 Node Exporter (métricas de nodo)
info "7.3 Verificando node-exporter..."
if ss -tlnp 2>/dev/null | grep -q ":9100 "; then
  pass "node-exporter: puerto 9100 escuchando"
elif curl -s -o /dev/null -w "%{http_code}" http://localhost:9100/metrics \
     --connect-timeout 3 --max-time 5 2>/dev/null | grep -q "200"; then
  pass "node-exporter: métricas accesibles en :9100"
else
  warn "node-exporter: no detectado en puerto 9100 — verificar despliegue"
fi

# =============================================================================
# SECCIÓN 8 — VERIFICACIÓN DE VERSIÓN
# =============================================================================
header "8. VERSIÓN DE MinIO"

info "8.1 Verificando versión instalada..."
MINIO_VER=$($MC_CMD admin info "${MC_ALIAS}" 2>/dev/null | grep -i "version" | head -1 || \
  podman exec $(podman ps -q --filter "name=minio" 2>/dev/null | head -1) \
    minio --version 2>/dev/null | head -1 || echo "no disponible")
if echo "$MINIO_VER" | grep -q "2026-02-07"; then
  pass "Versión MinIO: $MINIO_VER (RELEASE.2026-02-07T07-43-34Z)"
elif [[ "$MINIO_VER" != "no disponible" ]]; then
  warn "Versión MinIO: $MINIO_VER — esperada: RELEASE.2026-02-07T07-43-34Z"
else
  warn "No se pudo determinar la versión de MinIO instalada"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN"

echo -e "  Cluster  : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo     : ${BOLD}${HOSTNAME_ACTUAL}${NC}"
echo -e "  Fecha    : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""
if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN COMPLETADA SIN ERRORES NI ADVERTENCIAS${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN COMPLETADA CON ${WARNINGS} ADVERTENCIA(S) — revisar antes de producción${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${RED}      Corregir errores antes de operar el cluster en producción.${NC}"
fi
echo ""

exit $ERRORS
