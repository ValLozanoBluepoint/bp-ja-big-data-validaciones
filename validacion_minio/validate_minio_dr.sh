#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster MinIO DR (Datacenter Alterno)
#  Nodos : dr-stg-1 | dr-stg-2 | dr-stg-3
#  Versión MinIO : RELEASE.2026-02-07T07-43-34Z
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 9000 (API S3) | 9001 (Console)
#  Directorios: /opt/minio  |  /data/minio  |  /var/log/minio
#  Buckets requeridos: raw | bronze | silver | gold
#  Rol DR: preservación íntegra del Data Lake ante fallo del DC principal
#
#  Uso:
#    chmod +x validate_minio_dr.sh
#    sudo ./validate_minio_dr.sh [--alias ALIAS] [--principal-alias ALIAS_PRINCIPAL]
#                                [--skip-write-test] [--skip-replication-check]
#
#  Parámetros opcionales:
#    --alias                  Alias mc del cluster DR (default: minio-dr)
#    --principal-alias        Alias mc del cluster principal (default: minio-principal)
#    --skip-write-test        Omitir prueba de escritura/lectura
#    --skip-replication-check Omitir verificación de replicación desde cluster principal
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
MC_ALIAS="minio-dr"
MC_ALIAS_PRINCIPAL="minio-principal"
SKIP_WRITE_TEST=false
SKIP_REPLICATION_CHECK=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --alias)                   MC_ALIAS="$2";            shift 2 ;;
    --principal-alias)         MC_ALIAS_PRINCIPAL="$2";  shift 2 ;;
    --skip-write-test)         SKIP_WRITE_TEST=true;     shift   ;;
    --skip-replication-check)  SKIP_REPLICATION_CHECK=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

# ── Configuración del cluster DR ───────────────────────────────────────────────
CLUSTER_NAME="MinIO DR (Datacenter Alterno)"
NODES_DR=("dr-stg-1" "dr-stg-2" "dr-stg-3")
NODES_PRINCIPAL=("stg-1" "stg-2" "stg-3")
CONTAINER_PATTERN="minio"
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
REQUIRED_DIRS=("/opt/minio" "/data/minio" "/var/log/minio")
REQUIRED_BUCKETS=("raw" "bronze" "silver" "gold")
HOSTNAME_ACTUAL=$(hostname -s)

# Detectar rol del nodo actual
NODE_ROLE="desconocido"
for n in "${NODES_DR[@]}"; do
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
echo -e "  Alias mc DR: ${MC_ALIAS}  |  Alias mc Principal: ${MC_ALIAS_PRINCIPAL}"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  ${YELLOW}${BOLD}Nota: Este cluster es DR — dimensionado para preservación de datos,${NC}"
echo -e "  ${YELLOW}no para carga analítica intensiva en condiciones normales.${NC}"
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

# 1.3 Podman >= 5.x
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

# 1.4 systemd
info "1.4 Verificando systemd..."
SYSSTATE=$(systemctl is-system-running 2>/dev/null || echo "unknown")
[[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"

# 1.5 NTP
info "1.5 Verificando sincronización NTP..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado (obligatorio para cluster distribuido)"
elif chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización"
fi

# 1.6 Verificar que el nodo es efectivamente DR (hostname dr-stg-*)
info "1.6 Verificando naming convention del nodo DR..."
if echo "$HOSTNAME_ACTUAL" | grep -qE "^dr-stg-[0-9]+"; then
  pass "Hostname del nodo DR correcto: $HOSTNAME_ACTUAL (patrón dr-stg-<N>)"
else
  warn "Hostname '$HOSTNAME_ACTUAL' no sigue el patrón esperado 'dr-stg-<N>' — verificar"
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

    # /data/minio no debe ser overlay
    if [[ "$dir" == "/data/minio" ]]; then
      FSTYPE=$(df -T "$dir" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
      if [[ "$FSTYPE" == "overlay" ]]; then
        fail "/data/minio usa overlay filesystem — debe ser bind mount sobre partición dedicada"
      else
        pass "/data/minio filesystem: $FSTYPE (no overlay)"
      fi
      # DR especial: verificar espacio disponible mínimo (nodos DR tienen 500 GB)
      AVAIL_GB=$(df -BG "$dir" 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4}' || echo "0")
      if [[ "$AVAIL_GB" -gt 100 ]]; then
        pass "/data/minio disponible: ${AVAIL_GB} GB (umbral mínimo: 100 GB)"
      else
        warn "/data/minio disponible: ${AVAIL_GB} GB — espacio reducido para DR"
      fi
    fi
  else
    fail "Directorio $dir NO existe"
  fi
done

# Partición /data independiente
info "Verificando partición /data independiente..."
DATA_MOUNT=$(findmnt -n -o TARGET /data 2>/dev/null || \
             findmnt -n -o TARGET --target /data/minio 2>/dev/null || echo "")
if [[ -n "$DATA_MOUNT" ]]; then
  pass "Partición /data montada en: $DATA_MOUNT"
else
  warn "/data puede no ser partición independiente — revisar fstab"
fi

# =============================================================================
# SECCIÓN 3 — SERVICIO SYSTEMD Y CONTENEDOR PODMAN
# =============================================================================
header "3. SERVICIO SYSTEMD Y CONTENEDOR PODMAN"

# 3.1 Unidades systemd
info "3.1 Verificando unidades systemd de MinIO..."
SYSTEMD_UNITS=$(systemctl list-units --type=service 2>/dev/null | \
  grep -i "minio\|container-minio" | awk '{print $1}' || true)

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
  warn "No se encontraron unidades systemd con nombre 'minio'"
fi

# 3.2 Contenedor Podman
info "3.2 Verificando contenedor Podman MinIO..."
CONTAINERS=$(podman ps --format "{{.Names}}\t{{.Status}}\t{{.Image}}" 2>/dev/null | \
  grep -i "minio" || true)

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

# 3.3 Naming convention
info "3.3 Verificando naming convention..."
if podman ps --format "{{.Names}}" 2>/dev/null | grep -qE "^minio-[0-9]+$"; then
  CNAME=$(podman ps --format "{{.Names}}" | grep -E "^minio-[0-9]+$")
  pass "Naming convention correcta: $CNAME"
else
  warn "Naming convention esperada: minio-<índice>"
fi

# 3.4 Política de reinicio
info "3.4 Verificando política de reinicio..."
RESTART_POL=$(podman inspect --format "{{.HostConfig.RestartPolicy.Name}}" \
  $(podman ps -q --filter "name=minio" 2>/dev/null | head -1) 2>/dev/null || echo "N/A")
[[ "$RESTART_POL" == "always" ]] && \
  pass "Política de reinicio: always" || \
  warn "Política de reinicio: $RESTART_POL (recomendado: always o gestión via systemd)"

# =============================================================================
# SECCIÓN 4 — CONECTIVIDAD DE RED
# =============================================================================
header "4. CONECTIVIDAD DE RED"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || \
     netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando"
  fi
}

info "4.1 Puerto API S3 (${MINIO_API_PORT})..."
check_port_local $MINIO_API_PORT "API S3"

info "4.2 Puerto Console (${MINIO_CONSOLE_PORT})..."
check_port_local $MINIO_CONSOLE_PORT "Console Web"

# 4.3 Conectividad intra-cluster DR
info "4.3 Conectividad entre nodos del cluster DR..."
for NODE in "${NODES_DR[@]}"; do
  if [[ "$NODE" == "$NODE_ROLE" ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE (DR): OK (rtt: ${PING_RTT}ms)"
  else
    warn "Ping a $NODE (DR): no responde — verificar red interna DC alterno"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${MINIO_API_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${MINIO_API_PORT} (DR) alcanzable"
  else
    fail "TCP ${NODE}:${MINIO_API_PORT} (DR) no alcanzable"
  fi
done

# 4.4 Conectividad hacia cluster principal (para replicación)
info "4.4 Conectividad hacia cluster MinIO Principal (replicación)..."
PRINCIPAL_REACHABLE=0
for NODE in "${NODES_PRINCIPAL[@]}"; do
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${MINIO_API_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${MINIO_API_PORT} (Principal) alcanzable desde DR"
    PRINCIPAL_REACHABLE=$((PRINCIPAL_REACHABLE+1))
  else
    warn "TCP ${NODE}:${MINIO_API_PORT} (Principal) NO alcanzable — normal en contingencia total"
  fi
done
if [[ $PRINCIPAL_REACHABLE -eq 0 ]]; then
  warn "Ningún nodo del cluster principal es alcanzable — DC principal puede estar caído (escenario DR activo)"
fi

# 4.5 Health check local
info "4.5 Health check HTTP MinIO DR..."
HTTP_LIVE=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:${MINIO_API_PORT}/minio/health/live" \
  --connect-timeout 5 --max-time 10 2>/dev/null || echo "000")
if [[ "$HTTP_LIVE" == "200" ]]; then
  pass "Health check /minio/health/live: HTTP $HTTP_LIVE"
elif [[ "$HTTP_LIVE" == "403" ]]; then
  warn "Health check devolvió 403 — MinIO responde pero puede requerir auth"
else
  fail "Health check /minio/health/live: HTTP $HTTP_LIVE (esperado 200)"
fi

HTTP_CLUSTER=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:${MINIO_API_PORT}/minio/health/cluster" \
  --connect-timeout 5 --max-time 10 2>/dev/null || echo "000")
if [[ "$HTTP_CLUSTER" == "200" ]]; then
  pass "Cluster health /minio/health/cluster: HTTP $HTTP_CLUSTER — QUÓRUM OK"
else
  fail "Cluster health: HTTP $HTTP_CLUSTER — posible pérdida de quórum en cluster DR"
fi

# =============================================================================
# SECCIÓN 5 — VALIDACIÓN FUNCIONAL CON mc
# =============================================================================
header "5. VALIDACIÓN FUNCIONAL (mc — MinIO Client)"

if ! command -v mc &>/dev/null && ! command -v mcli &>/dev/null; then
  warn "mc/mcli no encontrado en PATH — omitiendo validaciones funcionales"
  SKIP_MC=true
else
  SKIP_MC=false
  MC_CMD=$(command -v mcli 2>/dev/null || command -v mc)
  info "mc encontrado: $($MC_CMD --version 2>/dev/null | head -1)"
fi

if [[ "$SKIP_MC" == false ]]; then

  # 5.1 Alias DR configurado
  info "5.1 Verificando alias '$MC_ALIAS' (DR) configurado..."
  if $MC_CMD alias ls 2>/dev/null | grep -q "^${MC_ALIAS}"; then
    pass "Alias '$MC_ALIAS' existe en configuración mc"
  else
    warn "Alias '$MC_ALIAS' no encontrado — usando URL local"
    info "  Crear con: mc alias set ${MC_ALIAS} http://localhost:9000 <ACCESS_KEY> <SECRET_KEY>"
    MC_ALIAS="http://localhost:${MINIO_API_PORT}"
  fi

  # 5.2 Admin info cluster DR
  info "5.2 Información del cluster DR..."
  ADMIN_INFO=$($MC_CMD admin info "${MC_ALIAS}" 2>/dev/null || echo "ERROR")
  if echo "$ADMIN_INFO" | grep -qi "online"; then
    pass "Cluster MinIO DR: nodos online"
  elif [[ "$ADMIN_INFO" == "ERROR" ]]; then
    warn "No se pudo obtener admin info del DR — verificar credenciales"
  else
    fail "Cluster MinIO DR: estado inesperado"
  fi

  # 5.3 Verificar buckets
  info "5.3 Verificando buckets requeridos en DR: ${REQUIRED_BUCKETS[*]}..."
  MISSING_BUCKETS=0
  for BUCKET in "${REQUIRED_BUCKETS[@]}"; do
    if $MC_CMD ls "${MC_ALIAS}/${BUCKET}" &>/dev/null; then
      pass "Bucket '${BUCKET}' existe en DR"
    else
      fail "Bucket '${BUCKET}' NO existe en DR — puede no haberse replicado aún"
      ((MISSING_BUCKETS++))
    fi
  done
  if [[ $MISSING_BUCKETS -gt 0 ]]; then
    warn "Buckets faltantes pueden deberse a que la replicación no se ha completado"
  fi

  # 5.4 Prueba de escritura/lectura en DR
  if [[ "$SKIP_WRITE_TEST" == false ]]; then
    info "5.4 Prueba de escritura/lectura en bucket 'raw' (DR)..."
    TEST_FILE="/tmp/bluepoint_minio_dr_validation_$(date +%s).tmp"
    TEST_OBJECT="bluepoint-validation-dr/test-$(date +%s).txt"

    echo "Bluepoint MinIO DR validation - $(date) - Cluster: DR" > "$TEST_FILE"

    if $MC_CMD cp "$TEST_FILE" "${MC_ALIAS}/raw/${TEST_OBJECT}" &>/dev/null; then
      pass "Escritura en DR: ${MC_ALIAS}/raw/${TEST_OBJECT} — OK"

      READBACK=$(mktemp)
      if $MC_CMD cp "${MC_ALIAS}/raw/${TEST_OBJECT}" "$READBACK" &>/dev/null; then
        pass "Lectura del objeto en DR: OK"
      else
        fail "Lectura del objeto en DR: FALLO"
      fi

      $MC_CMD rm "${MC_ALIAS}/raw/${TEST_OBJECT}" &>/dev/null && \
        info "Objeto de prueba DR eliminado."
      rm -f "$TEST_FILE" "$READBACK"
    else
      fail "Escritura en DR: ${MC_ALIAS}/raw/${TEST_OBJECT} — FALLO"
      rm -f "$TEST_FILE"
    fi
  else
    info "5.4 Prueba de escritura omitida (--skip-write-test)"
  fi

  # 5.5 Erasure coding
  info "5.5 Verificando erasure coding en DR..."
  EC_INFO=$($MC_CMD admin info "${MC_ALIAS}" 2>/dev/null | grep -i "erasure\|EC:" || echo "no encontrado")
  if echo "$EC_INFO" | grep -qi "erasure\|EC:"; then
    pass "Erasure coding configurado en DR"
  else
    warn "No se pudo confirmar erasure coding en DR"
  fi

fi

# =============================================================================
# SECCIÓN 6 — VERIFICACIÓN DE REPLICACIÓN DESDE CLUSTER PRINCIPAL
# =============================================================================
header "6. VERIFICACIÓN DE REPLICACIÓN (Principal → DR)"

if [[ "$SKIP_REPLICATION_CHECK" == true ]]; then
  info "Verificación de replicación omitida (--skip-replication-check)"
elif [[ "$SKIP_MC" == true ]]; then
  warn "mc no disponible — no se puede verificar replicación"
else

  # 6.1 Estado de las reglas de replicación en el cluster DR
  info "6.1 Verificando reglas de replicación site-to-site configuradas..."
  for BUCKET in "${REQUIRED_BUCKETS[@]}"; do
    REP_STATUS=$($MC_CMD replicate ls "${MC_ALIAS}/${BUCKET}" 2>/dev/null | \
      head -5 || echo "ERROR")
    if echo "$REP_STATUS" | grep -qi "enabled\|active\|ENABLED"; then
      pass "Bucket '$BUCKET': replicación activa"
    elif [[ "$REP_STATUS" == "ERROR" ]]; then
      warn "Bucket '$BUCKET': no se pudo verificar estado de replicación"
    else
      warn "Bucket '$BUCKET': replicación no configurada o inactiva — revisar"
    fi
  done

  # 6.2 Verificar con mc admin replicate (site replication)
  info "6.2 Estado de site replication MinIO..."
  SITE_REP=$($MC_CMD admin replicate info "${MC_ALIAS}" 2>/dev/null || echo "ERROR")
  if echo "$SITE_REP" | grep -qi "enabled\|principal\|source"; then
    pass "Site replication configurada"
    info "  Detalle: $(echo "$SITE_REP" | head -5)"
  elif [[ "$SITE_REP" == "ERROR" ]]; then
    warn "No se pudo obtener info de site replication — verificar credenciales o config"
  else
    warn "Site replication: no detectada activa — verificar configuración"
    info "  Para configurar site replication:"
    info "  mc admin replicate add ${MC_ALIAS_PRINCIPAL} ${MC_ALIAS}"
  fi

  # 6.3 Comparar conteo de objetos entre principal y DR en bucket raw (si ambos accesibles)
  info "6.3 Comparando objetos en bucket 'raw' entre Principal y DR..."
  if $MC_CMD alias ls 2>/dev/null | grep -q "^${MC_ALIAS_PRINCIPAL}"; then
    COUNT_PRINCIPAL=$($MC_CMD ls --recursive "${MC_ALIAS_PRINCIPAL}/raw" 2>/dev/null | \
      wc -l || echo "N/A")
    COUNT_DR=$($MC_CMD ls --recursive "${MC_ALIAS}/raw" 2>/dev/null | \
      wc -l || echo "N/A")

    if [[ "$COUNT_PRINCIPAL" != "N/A" && "$COUNT_DR" != "N/A" ]]; then
      info "  Objetos en Principal/raw: $COUNT_PRINCIPAL"
      info "  Objetos en DR/raw:        $COUNT_DR"
      DIFF=$((COUNT_PRINCIPAL - COUNT_DR))
      if [[ $DIFF -le 10 && $DIFF -ge 0 ]]; then
        pass "Replicación raw: diferencia aceptable ($DIFF objetos)"
      elif [[ $DIFF -gt 10 ]]; then
        warn "Replicación raw: hay $DIFF objetos sin replicar (replicación en progreso o rezagada)"
      else
        warn "Replicación raw: DR tiene más objetos que Principal — revisar"
      fi
    else
      warn "No se pudo comparar conteo de objetos — cluster principal puede no ser alcanzable"
    fi
  else
    warn "Alias '${MC_ALIAS_PRINCIPAL}' no configurado — no se puede comparar con cluster principal"
    info "  Si el DC principal está caído, este escenario es esperado."
  fi

  # 6.4 Verificar lag de replicación (metric endpoint)
  info "6.4 Verificando métricas de lag de replicación..."
  REP_METRICS=$(curl -s "http://localhost:${MINIO_API_PORT}/minio/v2/metrics/replication" \
    --connect-timeout 5 --max-time 10 2>/dev/null | head -20 || echo "")
  if echo "$REP_METRICS" | grep -q "minio_replication"; then
    PENDING=$(echo "$REP_METRICS" | grep "minio_replication_sent_bytes" | head -1 || echo "no data")
    pass "Métricas de replicación accesibles"
    info "  minio_replication_sent_bytes: $PENDING"
  else
    warn "Métricas de replicación no accesibles en /minio/v2/metrics/replication"
    info "  Nota: requiere auth token o IP allowlist configurada"
  fi

fi

# =============================================================================
# SECCIÓN 7 — RESILIENCIA DEL CLUSTER DR
# =============================================================================
header "7. RESILIENCIA DEL CLUSTER DR"

info "7.1 Estado de MinIO en cada nodo del cluster DR..."
UP_NODES=0
for NODE in "${NODES_DR[@]}"; do
  HC=$(curl -s -o /dev/null -w "%{http_code}" \
    "http://${NODE}:${MINIO_API_PORT}/minio/health/live" \
    --connect-timeout 5 --max-time 8 2>/dev/null || echo "000")
  if [[ "$HC" == "200" ]]; then
    pass "Nodo DR $NODE: /minio/health/live HTTP $HC — UP"
    UP_NODES=$((UP_NODES+1))
  else
    fail "Nodo DR $NODE: /minio/health/live HTTP $HC — DOWN o inaccesible"
  fi
done

# Quórum mínimo: >= 2 nodos de 3
if [[ $UP_NODES -ge 2 ]]; then
  pass "Quórum DR: $UP_NODES/3 nodos activos — quórum mantenido"
else
  fail "Quórum DR: $UP_NODES/3 nodos activos — SIN QUÓRUM — cluster DR degradado"
fi

info "7.2 Verificando /data/minio en nodo local..."
DATA_MINIO_SIZE=$(df -h /data/minio 2>/dev/null | awk 'NR==2{print $2}' || echo "N/A")
DATA_MINIO_AVAIL=$(df -h /data/minio 2>/dev/null | awk 'NR==2{print $4}' || echo "N/A")
DATA_MINIO_PCT=$(df /data/minio 2>/dev/null | awk 'NR==2{print $5}' || echo "N/A")
if [[ "$DATA_MINIO_SIZE" != "N/A" ]]; then
  pass "/data/minio — tamaño: $DATA_MINIO_SIZE — disponible: $DATA_MINIO_AVAIL — uso: $DATA_MINIO_PCT"
  # Alerta si uso > 85%
  PCT_NUM=$(echo "$DATA_MINIO_PCT" | tr -d '%')
  if [[ "$PCT_NUM" =~ ^[0-9]+$ ]] && [[ $PCT_NUM -gt 85 ]]; then
    warn "/data/minio uso > 85% — considerar expansión de almacenamiento DR"
  fi
else
  fail "/data/minio no disponible en este nodo DR"
fi

# =============================================================================
# SECCIÓN 8 — LOGS Y OBSERVABILIDAD
# =============================================================================
header "8. LOGS Y OBSERVABILIDAD"

# 8.1 Logs
info "8.1 Verificando logs en /var/log/minio..."
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

# 8.2 Grafana Alloy
info "8.2 Verificando Grafana Alloy (observabilidad)..."
ALLOY_UNITS=$(systemctl list-units --type=service 2>/dev/null | grep -i "alloy" | awk '{print $1}' || true)
if [[ -n "$ALLOY_UNITS" ]]; then
  while IFS= read -r unit; do
    ALLOY_STATE=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    [[ "$ALLOY_STATE" == "active" ]] && \
      pass "Alloy: $unit — activo" || fail "Alloy: $unit — $ALLOY_STATE"
  done <<< "$ALLOY_UNITS"
else
  ALLOY_CTR=$(podman ps --format "{{.Names}}\t{{.Status}}" 2>/dev/null | grep -i "alloy" || true)
  if [[ -n "$ALLOY_CTR" ]]; then
    pass "Alloy corriendo como contenedor Podman: $ALLOY_CTR"
  else
    warn "Grafana Alloy no encontrado"
  fi
fi

# 8.3 Node Exporter
info "8.3 Verificando node-exporter..."
if ss -tlnp 2>/dev/null | grep -q ":9100 "; then
  pass "node-exporter: puerto 9100 escuchando"
else
  warn "node-exporter: no detectado en puerto 9100"
fi

# =============================================================================
# SECCIÓN 9 — SIMULACIÓN DE FAILOVER (solo lectura estructural)
# =============================================================================
header "9. VERIFICACIÓN DE CAPACIDAD DE FAILOVER"

info "9.1 Verificando que MinIO DR puede servir peticiones independientemente del Principal..."
# El cluster DR es operativo si puede responder al health check con quórum propio
if [[ $UP_NODES -ge 2 ]]; then
  pass "Capacidad de failover: cluster DR puede operar de forma autónoma ($UP_NODES/3 nodos)"
else
  fail "Capacidad de failover: cluster DR NO tiene quórum para operar autónomamente"
fi

info "9.2 Verificando acceso a Console Web DR (puerto ${MINIO_CONSOLE_PORT})..."
CONSOLE_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:${MINIO_CONSOLE_PORT}" \
  --connect-timeout 5 --max-time 10 2>/dev/null || echo "000")
if [[ "$CONSOLE_CODE" =~ ^(200|301|302)$ ]]; then
  pass "Console Web DR accesible: HTTP $CONSOLE_CODE en :${MINIO_CONSOLE_PORT}"
else
  warn "Console Web DR: HTTP $CONSOLE_CODE en :${MINIO_CONSOLE_PORT}"
fi

info "9.3 Verificando versión de MinIO en cluster DR..."
MINIO_VER=$($MC_CMD admin info "${MC_ALIAS}" 2>/dev/null | grep -i "version" | head -1 || \
  podman exec $(podman ps -q --filter "name=minio" 2>/dev/null | head -1) \
    minio --version 2>/dev/null | head -1 || echo "no disponible")
if echo "$MINIO_VER" | grep -q "2026-02-07"; then
  pass "Versión MinIO DR: $MINIO_VER (RELEASE.2026-02-07T07-43-34Z — igual a Principal)"
elif [[ "$MINIO_VER" != "no disponible" ]]; then
  warn "Versión MinIO DR: $MINIO_VER — verificar homologación con cluster Principal"
else
  warn "No se pudo determinar versión de MinIO en DR"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN — CLUSTER DR"

echo -e "  Cluster  : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo     : ${BOLD}${HOSTNAME_ACTUAL}${NC}"
echo -e "  Nodos DR : ${NODES_DR[*]}"
echo -e "  Nodos UP : ${UP_NODES}/3"
echo -e "  Fecha    : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""
echo -e "  ${BOLD}Notas de diseño DR (Bluepoint):${NC}"
echo -e "  • Los nodos DR tienen menor capacidad (2 vCPU / 6 GB RAM) que el Principal"
echo -e "  • El objetivo del DR es preservación de datos, no carga analítica"
echo -e "  • La replicación hacia el DC alterno puede estar activa incluso si el Principal está caído"
echo ""

if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN DR COMPLETADA SIN ERRORES NI ADVERTENCIAS${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN DR COMPLETADA CON ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${YELLOW}      Revisar advertencias — algunas pueden ser normales en escenario DR activo.${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN DR FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${RED}      Corregir errores críticos — el cluster DR no está listo para contingencia.${NC}"
fi
echo ""

exit $ERRORS
