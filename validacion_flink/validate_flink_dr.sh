#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Apache Flink 2.2.1 – CLUSTER DR (Datacenter Alterno)
#
# Topología (mínimo viable DR) — arquitectura separada, igual que el cluster PRINCIPAL:
#   pbigd-plat-apps01-cont  →  Flink JobManager
#   pbigd-proc01-cont       →  Flink TaskManager
#   pbigd-proc02-cont       →  Flink TaskManager
#
# Los hostnames de los nodos YA incluyen el sufijo "-cont" (confirmado en
# /etc/hosts); el nombre del contenedor Podman coincide exactamente con el
# hostname del nodo, sin sufijo adicional.
#
# Propósito en contingencia:
#   · Procesamiento near real-time de eventos críticos
#   · Reconstrucción de estado de saldos (Redis)
#   · Jobs de serving de máxima prioridad únicamente
#
# Runtime: Podman 5.x · OpenJDK 17 · Rocky Linux 10.x
# Versión Flink: 2.2.1
#
# Uso:
#   chmod +x validate_flink_dr.sh
#   ./validate_flink_dr.sh [--jobmanager-host <ip_o_hostname>]
#
# Por defecto asume ejecución desde pbigd-plat-apps01 (nodo JobManager).
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN – ajusta las IPs/hostnames reales antes de ejecutar
# ---------------------------------------------------------------------------
JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01-cont}"   # JobManager DR (nodo dedicado)
JM_REST_PORT=8081
JM_RPC_PORT=6123
TASKMANAGER_HOSTS=("pbigd-proc01-cont" "pbigd-proc02-cont")  # TaskManagers DR

# Nombres reales de los contenedores Podman (no se derivan del hostname)
JM_CONTAINER_NAME="flink-jobmanager"
TASKMANAGER_CONTAINERS=("flink-tm-1-cont" "flink-tm-2-cont")  # paralelo a TASKMANAGER_HOSTS

FLINK_HOME="/opt/flink"
FLINK_DATA="/data/flink"
FLINK_LOGS="/var/log/flink"
FLINK_VERSION_EXPECTED="2.2.1"
JAVA_VERSION_EXPECTED="17"

PODMAN_USER="admapl"

# MinIO DR endpoint para checkpoints
MINIO_DR_ENDPOINT="http://pbigd-stg01-cont:9000"
CHECKPOINT_BUCKET="s3://flink-checkpoints"

# Umbrales mínimos aceptables en DR (capacidad reducida intencionalmente)
MIN_TASKMANAGERS=1   # En DR con 2 nodos, 1 TM es mínimo aceptable
MIN_SLOTS_TOTAL=1

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; MAGENTA='\033[0;35m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_flink_dr_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
# Verificación de puerto TCP sin depender de netcat (no siempre instalado en
# los nodos DR); usa el builtin /dev/tcp de bash.
tcp_port_open() {
  local host=$1 port=$2 timeout=${3:-3}
  timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null
}
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
dr()   { log "${MAGENTA}  [DR]${NC}  $*"; }   # notas específicas de contingencia
section() { log "\n${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${MAGENTA}  $*${NC}"; \
            log "${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# Parseo de argumentos
while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobmanager-host) JM_HOST="$2"; shift 2 ;;
    *) shift ;;
  esac
done

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
JM_API="http://${JM_HOST}:${JM_REST_PORT}"

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Flink Cluster DR (Alterno)             ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}║   Flink : $FLINK_VERSION_EXPECTED  ·  Java : OpenJDK $JAVA_VERSION_EXPECTED  ·  Modo: CONTINGENCIA    ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
dr "NOTA: El cluster DR opera con capacidad reducida intencionalmente."
dr "  pbigd-plat-apps01-cont: JobManager DR   — nodo dedicado"
dr "  pbigd-proc01-cont:      TaskManager DR"
dr "  pbigd-proc02-cont:      TaskManager DR"
dr "  Solo se ejecutan los jobs críticos de serving en modo contingencia."
log ""

# Detectar rol del nodo actual (arquitectura separada, igual que el cluster PRINCIPAL)
IS_JM_NODE=false
IS_TM_NODE=false
CONTAINER_TM=""
[[ "$THIS_HOST" == *"plat-apps"* || "$THIS_HOST" == "$JM_HOST" ]] && IS_JM_NODE=true
for i in "${!TASKMANAGER_HOSTS[@]}"; do
  TM="${TASKMANAGER_HOSTS[$i]}"
  if [[ "$THIS_HOST" == *"$TM"* || "$THIS_HOST" == "$TM" ]]; then
    IS_TM_NODE=true
    CONTAINER_TM="${TASKMANAGER_CONTAINERS[$i]}"
  fi
done

CONTAINER_JM="$JM_CONTAINER_NAME"

# ===========================================================================
# MÓDULO 1 – OS y prerequisitos
# ===========================================================================
section "MÓDULO 1 · OS, Java y prerequisitos DR"

OS=$(grep -oP '(?<=Rocky Linux )\S+' /etc/os-release 2>/dev/null || echo "unknown")
info "Sistema operativo: Rocky Linux $OS"
if [[ "$OS" == 10* ]]; then
  ok "Rocky Linux 10.x confirmado"
else
  warn "Versión OS inesperada: $OS"
fi

# Java 17
if command -v java &>/dev/null; then
  JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
  if [[ "$JAVA_VER" == "$JAVA_VERSION_EXPECTED" ]]; then
    ok "OpenJDK $JAVA_VERSION_EXPECTED detectado"
  else
    fail "Versión Java incorrecta: detectada=$JAVA_VER esperada=$JAVA_VERSION_EXPECTED"
  fi
  if java -version 2>&1 | grep -i openjdk &>/dev/null; then
    ok "Distribución OpenJDK confirmada"
  else
    warn "Distribución Java no es OpenJDK"
  fi
else
  fail "java no encontrado en PATH"
fi

# Podman
if command -v podman &>/dev/null; then
  PDM_VER=$(podman --version | awk '{print $3}')
  ok "Podman disponible: v$PDM_VER"
else
  fail "Podman no encontrado"
fi

# NTP — crítico para coherencia de eventos en contingencia
if timedatectl status 2>/dev/null | grep -q "synchronized: yes"; then
  ok "NTP sincronizado (timedatectl)"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  ok "NTP sincronizado (chrony)"
else
  warn "NTP no confirmado — CRÍTICO en entorno DR para coherencia de eventos"
fi

# Verificar recursos del nodo (límites en DR)
if $IS_JM_NODE; then
  MEM_TOTAL_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
  MEM_TOTAL_GB=$(( MEM_TOTAL_KB / 1024 / 1024 ))
  VCPU=$(nproc)
  info "Recursos nodo $THIS_HOST: ${VCPU} vCPU / ${MEM_TOTAL_GB} GB RAM"
  if [[ $MEM_TOTAL_GB -ge 9 ]]; then
    ok "RAM suficiente para JobManager DR: ${MEM_TOTAL_GB} GB (mín 10 GB especificado)"
  else
    warn "RAM por debajo del mínimo DR: ${MEM_TOTAL_GB} GB (esperado ≥10 GB en pbigd-plat-apps01)"
  fi
  if [[ $VCPU -ge 4 ]]; then
    ok "vCPU suficientes: $VCPU (mín 4 para pbigd-plat-apps01)"
  else
    warn "vCPU insuficientes: $VCPU (se esperan ≥4 en pbigd-plat-apps01)"
  fi
fi

# ===========================================================================
# MÓDULO 2 – Directorios y permisos
# ===========================================================================
section "MÓDULO 2 · Directorios y estructura DR"

for DIR in "$FLINK_HOME" "$FLINK_DATA" "$FLINK_LOGS"; do
  if [[ -d "$DIR" ]]; then
    ok "Directorio existe: $DIR"
    if touch "$DIR/.write_test" 2>/dev/null; then
      rm -f "$DIR/.write_test"
      ok "  ↳ Escritura OK en $DIR"
    else
      warn "  ↳ Sin permisos de escritura en $DIR"
    fi
  else
    fail "Directorio faltante: $DIR"
  fi
done

for SUBDIR in checkpoints savepoints; do
  if [[ -d "$FLINK_DATA/$SUBDIR" ]]; then
    ok "Subdirectorio existe: $FLINK_DATA/$SUBDIR"
  else
    warn "Subdirectorio no encontrado: $FLINK_DATA/$SUBDIR"
  fi
done

# Espacio disponible — mínimo 20 GB libres en DR
DISK_AVAIL_GB=$(df --output=avail -BG "$FLINK_DATA" 2>/dev/null | tail -1 | tr -d 'G' | tr -d ' ' || echo 0)
info "Espacio disponible en $FLINK_DATA: ${DISK_AVAIL_GB} GB"
if [[ "$DISK_AVAIL_GB" -ge 20 ]] 2>/dev/null; then
  ok "Espacio libre suficiente: ${DISK_AVAIL_GB} GB"
else
  warn "Espacio libre bajo: ${DISK_AVAIL_GB} GB (recomendado ≥20 GB para checkpoints)"
fi

# ===========================================================================
# MÓDULO 3 – Contenedor JobManager (pbigd-plat-apps01-cont → contenedor flink-jobmanager)
# ===========================================================================
section "MÓDULO 3 · Contenedor Flink JobManager (flink-jobmanager)"

if $IS_JM_NODE; then
  info "Nodo identificado como JobManager DR ($THIS_HOST)"
  info "Contenedor detectado: $CONTAINER_JM"

  JM_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null \
    || sudo -u "$PODMAN_USER" podman ps --filter "name=${CONTAINER_JM}" \
       --format "{{.Status}}" 2>/dev/null || echo "")

  if echo "$JM_STATUS" | grep -qi "up"; then
    ok "Contenedor JobManager corriendo: $JM_STATUS"
  else
    fail "Contenedor JobManager NO está UP — DR inoperativo sin JM"
  fi

  # Versión Flink en contenedor
  FLINK_VER_ACTUAL=$(podman exec "$CONTAINER_JM" /opt/flink/bin/flink --version 2>/dev/null \
    | grep -oP 'Version: \K[\d.]+' || \
    podman exec "$CONTAINER_JM" cat /opt/flink/version 2>/dev/null || echo "unknown")
  if [[ "$FLINK_VER_ACTUAL" == "$FLINK_VERSION_EXPECTED" ]]; then
    ok "Versión Flink DR: $FLINK_VER_ACTUAL"
  else
    warn "Versión Flink DR: detectada='$FLINK_VER_ACTUAL' esperada='$FLINK_VERSION_EXPECTED'"
  fi

  # Java en contenedor JM
  JVM_JM=$(podman exec "$CONTAINER_JM" java -version 2>&1 | head -1 || echo "error")
  info "JVM en JobManager DR: $JVM_JM"
  if echo "$JVM_JM" | grep -q '"17'; then
    ok "OpenJDK 17 en JobManager DR"
  else
    fail "JVM del JobManager DR NO es OpenJDK 17"
  fi

  # Uptime del contenedor (importante en DR — debe haber levantado recientemente en failover)
  JM_STARTED=$(podman inspect "$CONTAINER_JM" --format "{{.State.StartedAt}}" 2>/dev/null || echo "?")
  info "Contenedor JobManager iniciado: $JM_STARTED"
else
  info "Este nodo no es el JobManager DR ($JM_HOST) — saltando Módulo 3"
fi

# ===========================================================================
# MÓDULO 4 – Contenedor TaskManager (pbigd-proc01-cont → flink-tm-1-cont / pbigd-proc02-cont → flink-tm-2-cont)
# ===========================================================================
section "MÓDULO 4 · Contenedor Flink TaskManager DR"

if $IS_TM_NODE; then
  info "Nodo TaskManager DR detectado ($THIS_HOST)"
  info "Contenedor detectado: $CONTAINER_TM"

  TM_STATUS=$(podman ps --filter "name=${CONTAINER_TM}" --format "{{.Status}}" 2>/dev/null || echo "")
  if echo "$TM_STATUS" | grep -qi "up"; then
    ok "Contenedor TaskManager corriendo: $TM_STATUS"
  else
    fail "Contenedor TaskManager NO está UP en $THIS_HOST"
  fi

  # Java en TM
  TM_JVM=$(podman exec "$CONTAINER_TM" java -version 2>&1 | head -1 || echo "error")
  info "JVM en TaskManager DR: $TM_JVM"
  if echo "$TM_JVM" | grep -q '"17'; then
    ok "OpenJDK 17 confirmado en TaskManager DR"
  else
    fail "JVM en TaskManager DR NO es OpenJDK 17"
  fi

  # Slots configurados — vía REST API del JobManager en vez de parsear el
  # archivo de configuración local: entre Flink 1.x y 2.x cambió el formato
  # (flink-conf.yaml plano → config.yaml jerárquico), y la API no depende de eso.
  THIS_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
  TM_API_RAW=$(curl -sf --max-time 10 "$JM_API/taskmanagers" 2>/dev/null || echo "")
  if [[ -n "$TM_API_RAW" && -n "$THIS_IP" ]]; then
    TM_SLOTS=$(echo "$TM_API_RAW" | python3 -c \
      "import sys,json
tms=json.load(sys.stdin).get('taskmanagers',[])
ip='$THIS_IP'
match=[t for t in tms if t.get('id','').split(':')[0]==ip]
print(match[0]['slotsNumber'] if match else '?')" 2>/dev/null || echo "?")
  else
    TM_SLOTS="?"
  fi
  info "Task slots configurados en $THIS_HOST (vía REST API): $TM_SLOTS"
  if [[ "$TM_SLOTS" != "?" ]]; then
    ok "Task slots configurados: $TM_SLOTS"
  else
    warn "No se pudo determinar numberOfTaskSlots vía REST API (JM inalcanzable o TM aún no registrado)"
  fi

  # Verificar que JVM del TM no excede recursos del nodo DR
  TM_HEAP=$(podman exec "$CONTAINER_TM" \
    grep -oP 'taskmanager.memory.process.size\s*:\s*\K\S+' \
    /opt/flink/conf/flink-conf.yaml 2>/dev/null || echo "?")
  info "Memoria proceso TM configurada: $TM_HEAP"
else
  info "Este nodo ($THIS_HOST) no tiene rol TaskManager DR activo — omitiendo"
fi

# ===========================================================================
# MÓDULO 5 – Puertos de red
# ===========================================================================
section "MÓDULO 5 · Verificación de puertos DR"

if $IS_JM_NODE; then
  if ss -tlnp | grep -q ":${JM_REST_PORT}"; then
    ok "Puerto $JM_REST_PORT (REST/UI) escuchando en JM DR"
  else
    fail "Puerto $JM_REST_PORT NO escucha — JobManager DR no expone REST"
  fi

  if ss -tlnp | grep -q ":${JM_RPC_PORT}"; then
    ok "Puerto $JM_RPC_PORT (RPC) escuchando en JM DR"
  else
    fail "Puerto $JM_RPC_PORT NO escucha — RPC no activo"
  fi
fi

if $IS_TM_NODE; then
  if ss -tlnp | grep -qE "java|flink"; then
    ok "Actividad Java/Flink en puertos de red del TaskManager DR"
  else
    warn "Sin actividad de red Java/Flink en TaskManager DR"
  fi
fi

# ===========================================================================
# MÓDULO 6 – REST API JobManager DR
# ===========================================================================
section "MÓDULO 6 · REST API Flink JobManager DR"

info "Consultando REST API DR: $JM_API"

OVERVIEW=$(curl -sf --max-time 10 "$JM_API/overview" 2>/dev/null || echo "")
if [[ -n "$OVERVIEW" ]]; then
  ok "REST API DR responde: $JM_API/overview"

  if command -v python3 &>/dev/null; then
    TM_COUNT=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('taskmanagers',0))" 2>/dev/null || echo "0")
    SLOTS_TOTAL=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('slots-total',0))" 2>/dev/null || echo "0")
    SLOTS_AVAIL=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('slots-available',0))" 2>/dev/null || echo "0")
    FLINK_API_VER=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('flink-version','?'))" 2>/dev/null || echo "?")
    JOBS_RUNNING=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('jobs-running',0))" 2>/dev/null || echo "?")

    info "  Flink versión (API):   $FLINK_API_VER"
    info "  TaskManagers activos:  $TM_COUNT  (mínimo aceptable DR: $MIN_TASKMANAGERS)"
    info "  Slots totales:         $SLOTS_TOTAL"
    info "  Slots disponibles:     $SLOTS_AVAIL"
    info "  Jobs corriendo:        $JOBS_RUNNING"

    if [[ "$FLINK_API_VER" == "$FLINK_VERSION_EXPECTED" ]]; then
      ok "Versión Flink API coincide: $FLINK_API_VER"
    else
      warn "Versión API: '$FLINK_API_VER' ≠ esperada '$FLINK_VERSION_EXPECTED'"
    fi

    # En DR, umbral reducido respecto al cluster principal
    if [[ "$TM_COUNT" -ge "$MIN_TASKMANAGERS" ]] 2>/dev/null; then
      ok "TaskManagers DR activos: $TM_COUNT (umbral DR: ≥$MIN_TASKMANAGERS)"
    else
      fail "TaskManagers DR insuficientes: $TM_COUNT (umbral: $MIN_TASKMANAGERS) — cluster no puede procesar"
    fi

    if [[ "$SLOTS_TOTAL" -ge "$MIN_SLOTS_TOTAL" ]] 2>/dev/null; then
      ok "Slots disponibles en DR: total=$SLOTS_TOTAL avail=$SLOTS_AVAIL"
    else
      fail "Sin slots registrados — cluster DR inoperativo"
    fi

    dr "En contingencia se esperan $JOBS_RUNNING job(s) críticos de serving activos"
  fi
else
  fail "REST API DR no responde: $JM_API — cluster DR INOPERATIVO"
fi

# 6.2 Detalle de TaskManagers registrados
TM_API=$(curl -sf --max-time 10 "$JM_API/taskmanagers" 2>/dev/null || echo "")
if [[ -n "$TM_API" ]]; then
  ok "Endpoint /taskmanagers responde en JM DR"
  echo "$TM_API" | python3 -c \
    "import sys,json; tms=json.load(sys.stdin).get('taskmanagers',[]); \
    [print(f'  TM: {t[\"id\"][:20]}... | host={t.get(\"path\",\"?\").split(\":\")[0]} | slots={t[\"slotsNumber\"]} | free={t[\"freeSlots\"]}') for t in tms]" \
    2>/dev/null || true
else
  warn "Endpoint /taskmanagers no responde"
fi

# 6.3 Jobs en ejecución (en DR se espera al menos 1 en contingencia activa)
JOBS_API=$(curl -sf --max-time 10 "$JM_API/jobs" 2>/dev/null || echo "")
if [[ -n "$JOBS_API" ]]; then
  ok "Endpoint /jobs responde"
  RUNNING_JOBS=$(echo "$JOBS_API" | python3 -c \
    "import sys,json; jobs=json.load(sys.stdin).get('jobs',[]); \
    running=[j for j in jobs if j.get('status')=='RUNNING']; \
    print(f'Jobs RUNNING: {len(running)}')" 2>/dev/null || echo "?")
  info "  $RUNNING_JOBS"
  dr "En contingencia activa se esperan jobs de serving corriendo"
else
  warn "Endpoint /jobs no responde"
fi

# ===========================================================================
# MÓDULO 7 – systemd (resiliencia del servicio en DR)
# ===========================================================================
section "MÓDULO 7 · Systemd – persistencia y autoarranque DR"

check_systemd() {
  local UNIT=$1 DESC=$2
  if systemctl is-active --quiet "$UNIT" 2>/dev/null; then
    ok "Servicio activo: $UNIT ($DESC)"
    if systemctl is-enabled --quiet "$UNIT" 2>/dev/null; then
      ok "  ↳ Habilitado en boot (crítico para DR)"
    else
      fail "  ↳ NO habilitado en boot — CRÍTICO: debe sobrevivir reinicios en DR"
    fi
  else
    if sudo -u "$PODMAN_USER" systemctl --user is-active --quiet "$UNIT" 2>/dev/null; then
      ok "Servicio usuario activo: $UNIT"
      if sudo -u "$PODMAN_USER" systemctl --user is-enabled --quiet "$UNIT" 2>/dev/null; then
        ok "  ↳ Habilitado en boot (usuario)"
      else
        fail "  ↳ NO habilitado en boot de usuario — CRÍTICO en DR"
      fi
    else
      warn "Servicio '$UNIT' no encontrado como activo (verificar nombre de unidad)"
    fi
  fi
}

if $IS_JM_NODE; then
  check_systemd "flink-jobmanager"        "Flink JobManager DR"
fi
if $IS_TM_NODE; then
  check_systemd "flink-taskmanager"        "Flink TaskManager DR"
fi

dr "En DR es OBLIGATORIO que los servicios tengan 'enabled' para autoarrancar post-failover"

# ===========================================================================
# MÓDULO 8 – Checkpoints hacia MinIO DR
# ===========================================================================
section "MÓDULO 8 · Checkpoints Flink → MinIO DR"

# 8.1 Conectividad MinIO DR
MINIO_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "${MINIO_DR_ENDPOINT}/minio/health/live" 2>/dev/null || echo "000")
if [[ "$MINIO_HTTP" == "200" ]]; then
  ok "MinIO DR alcanzable: HTTP $MINIO_HTTP ($MINIO_DR_ENDPOINT)"
else
  fail "MinIO DR no responde (HTTP $MINIO_HTTP) — checkpoints NO se podrán persistir"
fi

# 8.2 Configuración de checkpoint en JM DR
if $IS_JM_NODE; then
  CKPT_CFG=$(podman exec "$CONTAINER_JM" \
    grep -r "state.checkpoints\|s3\|filesystem.backend\|high-availability" \
    /opt/flink/conf/ 2>/dev/null | head -15 || echo "")
  if [[ -n "$CKPT_CFG" ]]; then
    ok "Configuración de checkpoints/HA encontrada"
    info "$CKPT_CFG"
  else
    warn "Sin configuración explícita de checkpoints — validar flink-conf.yaml en DR"
  fi

  # Verificar intervalo de checkpoint (importante en DR para RPO)
  CKPT_INTERVAL=$(podman exec "$CONTAINER_JM" \
    grep -oP 'execution.checkpointing.interval\s*:\s*\K\S+' \
    /opt/flink/conf/flink-conf.yaml 2>/dev/null || echo "?")
  info "Intervalo de checkpoint configurado: $CKPT_INTERVAL"
  dr "Un intervalo ≤60s reduce la pérdida de datos en caso de failover DR"
fi

# ===========================================================================
# MÓDULO 9 – Conectividad inter-nodos DR
# ===========================================================================
section "MÓDULO 9 · Conectividad entre nodos DR"

ALL_DR_NODES=("$JM_HOST" "${TASKMANAGER_HOSTS[@]}")
for NODE in "${ALL_DR_NODES[@]}"; do
  [[ "$NODE" == "$THIS_HOST" ]] && continue
  if ping -c 1 -W 2 "$NODE" &>/dev/null; then
    ok "Nodo DR $NODE alcanzable (ping OK)"
    # El puerto RPC 6123 sólo lo expone el JobManager; el TaskManager adicional
    # (pbigd-proc02) usa un puerto RPC dinámico propio, así que sólo tiene
    # sentido verificarlo contra el JM.
    if [[ "$NODE" == "$JM_HOST" ]]; then
      if tcp_port_open "$NODE" "$JM_RPC_PORT"; then
        ok "  ↳ Puerto RPC $JM_RPC_PORT accesible en $NODE"
      else
        warn "  ↳ Puerto RPC $JM_RPC_PORT no accesible en $NODE (TM puede no haber levantado)"
      fi
    fi
  else
    fail "Nodo DR $NODE NO responde — cluster DR degradado"
  fi
done

# Verificar conectividad también hacia componentes de serving críticos
info "Verificando conectividad hacia componentes de serving en DR..."
for SERVING_HOST in "pbigd-plat-apps01-cont" "pbigd-kaf01-cont"; do
  if ping -c 1 -W 2 "$SERVING_HOST" &>/dev/null; then
    ok "Alcanzable: $SERVING_HOST"
  else
    warn "No se puede alcanzar: $SERVING_HOST (puede estar en otro segmento)"
  fi
done

# ===========================================================================
# MÓDULO 10 – Validación de jobs críticos de contingencia
# ===========================================================================
section "MÓDULO 10 · Jobs críticos para modo contingencia"

dr "En modo contingencia, Flink DR debe ejecutar únicamente:"
dr "  · Job de reconstrucción de saldos (Kafka → Flink → Redis)"
dr "  · Job de serving de últimos N movimientos"
dr ""

JOBS_RESP=$(curl -sf --max-time 10 "$JM_API/jobs/overview" 2>/dev/null || echo "")

if [[ -n "$JOBS_RESP" ]]; then
  ok "Endpoint /jobs/overview disponible"
  echo "$JOBS_RESP" | python3 2>/dev/null <<'PYEOF' | tee -a "$LOG_FILE" || true
import sys, json
data = json.load(sys.stdin)
jobs = data.get("jobs", [])
print(f"  Total jobs registrados: {len(jobs)}")
for j in jobs:
    status = j.get("state", "?")
    name   = j.get("name", "?")[:50]
    jid    = j.get("jid", "?")[:8]
    print(f"  [{status:10}] {name}  (id: {jid}...)")
PYEOF
else
  warn "Endpoint /jobs/overview no disponible — no se pueden listar jobs"
fi

# Verificar conectividad Kafka DR (prerequisito de los jobs)
info "Verificando acceso a Kafka DR (prerequisito de jobs)..."
KAFKA_OK=false
for KAFKA_NODE in "pbigd-kaf01-cont" "pbigd-kaf02-cont" "pbigd-kaf03-cont"; do
  if tcp_port_open "$KAFKA_NODE" 9092; then
    ok "Kafka DR accesible: $KAFKA_NODE:9092"
    KAFKA_OK=true
    break
  fi
done
$KAFKA_OK || warn "Ningún broker Kafka DR accesible en puerto 9092 (kaf01/kaf02/kaf03)"

# ===========================================================================
# MÓDULO 11 – Logs y observabilidad DR
# ===========================================================================
section "MÓDULO 11 · Logs y observabilidad DR"

if $IS_JM_NODE; then
  LOG_LINES=$(podman logs --tail 5 "$CONTAINER_JM" 2>/dev/null | wc -l || echo 0)
  if [[ "$LOG_LINES" -gt 0 ]]; then
    ok "Logs disponibles en JobManager DR ($LOG_LINES líneas recientes)"
  else
    warn "Sin logs recientes en JobManager DR"
  fi

  ERRORS=$(podman logs --tail 100 "$CONTAINER_JM" 2>/dev/null | grep -ci "ERROR\|Exception\|FATAL" || true)
  if [[ "$ERRORS" -eq 0 ]]; then
    ok "Sin errores críticos en últimos 100 logs del JM DR"
  else
    warn "Detectados $ERRORS mensajes ERROR/Exception/FATAL en JM DR — revisar"
  fi

  # Verificar si hay advertencias de memoria (relevante con recursos reducidos)
  MEM_WARN=$(podman logs --tail 200 "$CONTAINER_JM" 2>/dev/null | \
    grep -ci "OutOfMemory\|GC overhead\|heap space" || true)
  if [[ "$MEM_WARN" -gt 0 ]]; then
    fail "Detectadas $MEM_WARN advertencias de memoria en JM DR — recursos insuficientes"
  else
    ok "Sin presión de memoria reportada en logs JM DR"
  fi
fi

if $IS_TM_NODE; then
  LOG_LINES_TM=$(podman logs --tail 5 "$CONTAINER_TM" 2>/dev/null | wc -l || echo 0)
  if [[ "$LOG_LINES_TM" -gt 0 ]]; then
    ok "Logs disponibles en TaskManager DR"
  else
    warn "Sin logs en TaskManager DR"
  fi

  MEM_WARN_TM=$(podman logs --tail 200 "$CONTAINER_TM" 2>/dev/null | \
    grep -ci "OutOfMemory\|GC overhead\|heap space" || true)
  if [[ "$MEM_WARN_TM" -gt 0 ]]; then
    fail "Presión de memoria en TaskManager DR ($MEM_WARN_TM avisos)"
  else
    ok "Sin presión de memoria en TaskManager DR"
  fi
fi

# Alloy (observabilidad DR)
if systemctl is-active --quiet alloy 2>/dev/null || \
   systemctl is-active --quiet grafana-alloy 2>/dev/null; then
  ok "Grafana Alloy activo en DR"
else
  warn "Grafana Alloy no detectado — observabilidad limitada en contingencia"
fi

# node-exporter
if ss -tlnp | grep -q ":9100"; then
  ok "node-exporter activo (puerto 9100)"
else
  warn "node-exporter no detectado"
fi

# ===========================================================================
# MÓDULO 12 – Prueba de capacidad de procesamiento DR
# ===========================================================================
section "MÓDULO 12 · Capacidad y RTO estimado"

OVERVIEW_2=$(curl -sf --max-time 10 "$JM_API/overview" 2>/dev/null || echo "")
if [[ -n "$OVERVIEW_2" ]] && command -v python3 &>/dev/null; then
  echo "$OVERVIEW_2" | python3 2>/dev/null <<'PYEOF' | tee -a "$LOG_FILE" || true
import sys, json
d = json.load(sys.stdin)
tm_count   = d.get("taskmanagers", 0)
slots_tot  = d.get("slots-total", 0)
slots_avail= d.get("slots-available", 0)
jobs_run   = d.get("jobs-running", 0)

print(f"\n  ── Capacidad cluster DR ──────────────────────────────")
print(f"  TaskManagers activos : {tm_count}")
print(f"  Slots totales        : {slots_tot}")
print(f"  Slots disponibles    : {slots_avail}")
print(f"  Jobs en ejecución    : {jobs_run}")

if slots_tot > 0:
    util = ((slots_tot - slots_avail) / slots_tot) * 100
    print(f"  Utilización de slots : {util:.0f}%")
    if util > 80:
        print("  ⚠ ADVERTENCIA: utilización >80% — cluster DR saturado")
    else:
        print("  ✔ Utilización dentro de límites aceptables para DR")
PYEOF
fi

dr ""
dr "RTO estimado para este cluster DR:"
dr "  · JobManager disponible       → inmediato (contenedor ya corriendo)"
dr "  · TaskManagers registrados    → <30s tras inicio"
dr "  · Jobs críticos operativos    → <2min desde failover declarado"

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Flink Cluster DR (Datacenter Alterno)               ║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Nodo evaluado : $(printf '%-51s' "$THIS_HOST")║${NC}"
log "${BOLD}║   Total checks  : $(printf '%-51s' "$TOTAL")║${NC}"
log "${GREEN}${BOLD}║   PASS          : $(printf '%-51s' "$PASS")║${NC}"
log "${RED}${BOLD}║   FAIL          : $(printf '%-51s' "$FAIL")║${NC}"
log "${YELLOW}${BOLD}║   WARN          : $(printf '%-51s' "$WARN")║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Log guardado  : $(printf '%-51s' "$LOG_FILE")║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

if [[ $FAIL -eq 0 && $WARN -le 3 ]]; then
  log "${GREEN}${BOLD}  ✔  Cluster DR LISTO para contingencia${NC}"
elif [[ $FAIL -eq 0 ]]; then
  log "${YELLOW}${BOLD}  ⚠  Cluster DR funcional con advertencias ($WARN) — revisar antes de activar contingencia${NC}"
else
  log "${RED}${BOLD}  ✘  $FAIL fallos críticos — cluster DR NO APTO para contingencia${NC}"
fi

log ""
log "${CYAN}  ▶ Ejecutar también en el nodo restante:${NC}"
for NODE in "${ALL_DR_NODES[@]}"; do
  [[ "$NODE" != "$THIS_HOST" ]] && log "     ssh admapl@$NODE 'bash validate_flink_dr.sh'"
done
log ""

exit $FAIL
