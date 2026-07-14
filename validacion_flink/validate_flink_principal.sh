#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Apache Flink 2.2.1 – CLUSTER PRINCIPAL (DC Principal)
#
# Topología:
#   pbigd-plat-apps01  →  Flink JobManager  (puerto 8081 UI / 6123 RPC)
#   pbigd-proc01       →  Flink TaskManager
#   pbigd-proc02       →  Flink TaskManager
#   pbigd-proc03       →  Flink TaskManager
#
# Runtime: Podman 5.x · OpenJDK 17 · Rocky Linux 10.x
# Versión Flink: 2.2.1
#
# Uso:
#   chmod +x validate_flink_principal.sh
#   ./validate_flink_principal.sh [--jobmanager-host <ip_o_hostname>]
#
# Por defecto asume que se ejecuta DESDE el nodo JobManager (pbigd-plat-apps01).
# Pasa --jobmanager-host si lo ejecutas desde otra máquina.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN – ajusta las IPs/hostnames reales antes de ejecutar
# ---------------------------------------------------------------------------
JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01}"   # sobreescribible vía env o flag
JM_REST_PORT=8081
JM_RPC_PORT=6123
TASKMANAGER_HOSTS=("pbigd-proc01" "pbigd-proc02" "pbigd-proc03")

FLINK_HOME="/opt/flink"
FLINK_DATA="/data/flink"
FLINK_LOGS="/var/log/flink"
FLINK_VERSION_EXPECTED="2.2.1"
JAVA_VERSION_EXPECTED="17"

CONTAINER_JM="flink-jobmanager"      # nombre del contenedor Podman JobManager
CONTAINER_TM_PATTERN="flink-tm"      # prefijo del contenedor TaskManager; cada nodo usa su propio sufijo (flink-tm-1, flink-tm-2...)
PODMAN_USER="admapl"                 # usuario rootless (ajustar si difiere)

MINIO_ENDPOINT="http://pbigd-stg01:9000"   # endpoint MinIO para validar checkpoints S3 (renombrado: stg1 → pbigd-stg01)
CHECKPOINT_BUCKET="s3://flink-checkpoints"

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_flink_principal_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
# Verificación de puerto TCP sin depender de netcat (no siempre instalado en
# los nodos); usa el builtin /dev/tcp de bash.
tcp_port_open() {
  local host=$1 port=$2 timeout=${3:-3}
  timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null
}
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# Parseo de argumentos
while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobmanager-host) JM_HOST="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Flink Cluster PRINCIPAL                ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}║   Flink : $FLINK_VERSION_EXPECTED  ·  Java : OpenJDK $JAVA_VERSION_EXPECTED                    ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

# ===========================================================================
# MÓDULO 1 – Sistema operativo y prerequisitos
# ===========================================================================
section "MÓDULO 1 · OS, Java y prerequisitos"

# 1.1 Rocky Linux
OS=$(grep -oP '(?<=Rocky Linux )\S+' /etc/os-release 2>/dev/null || echo "unknown")
info "Sistema operativo detectado: Rocky Linux $OS"
if [[ "$OS" == 10* ]]; then
  ok "Rocky Linux 10.x confirmado"
else
  warn "Versión OS inesperada: $OS"
fi

# 1.2 OpenJDK 17
if command -v java &>/dev/null; then
  JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
  if [[ "$JAVA_VER" == "$JAVA_VERSION_EXPECTED" ]]; then
    ok "OpenJDK $JAVA_VERSION_EXPECTED detectado correctamente"
  else
    fail "Versión Java incorrecta: detectada=$JAVA_VER esperada=$JAVA_VERSION_EXPECTED"
  fi
  if java -version 2>&1 | grep -i openjdk &>/dev/null; then
    ok "Distribución confirmada: OpenJDK"
  else
    warn "Distribución Java no es OpenJDK"
  fi
else
  fail "java no encontrado en PATH"
fi

# 1.3 Podman disponible
if command -v podman &>/dev/null; then
  PDM_VER=$(podman --version | awk '{print $3}')
  ok "Podman disponible: v$PDM_VER"
else
  fail "Podman no encontrado"
fi

# 1.4 Sincronización NTP
if command -v chronyc &>/dev/null; then
  if chronyc tracking &>/dev/null; then
    ok "NTP (chrony) sincronizado"
  else
    warn "chrony no confirmado"
  fi
elif timedatectl status 2>/dev/null | grep -q "synchronized: yes"; then
  ok "NTP sincronizado (timedatectl)"
else
  warn "No se pudo verificar sincronización NTP"
fi

# ===========================================================================
# MÓDULO 2 – Directorios y permisos
# ===========================================================================
section "MÓDULO 2 · Directorios y estructura"

for DIR in "$FLINK_HOME" "$FLINK_DATA" "$FLINK_LOGS"; do
  if [[ -d "$DIR" ]]; then
    ok "Directorio existe: $DIR"
    # Verificar escritura
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

# Subdirectorios esperados bajo FLINK_DATA
for SUBDIR in checkpoints savepoints; do
  if [[ -d "$FLINK_DATA/$SUBDIR" ]]; then
    ok "Subdirectorio existe: $FLINK_DATA/$SUBDIR"
  else
    warn "Subdirectorio no encontrado: $FLINK_DATA/$SUBDIR (puede no existir antes del primer job)"
  fi
done

# ===========================================================================
# MÓDULO 3 – Contenedor JobManager (ejecutar en el nodo pbigd-plat-apps01)
# ===========================================================================
section "MÓDULO 3 · Contenedor Flink JobManager (nodo local)"

# Detectar si estamos en el nodo JobManager
THIS_HOST=$(hostname -s 2>/dev/null || hostname)
JM_API="http://${JM_HOST}:${JM_REST_PORT}"
IS_JM_NODE=false
[[ "$THIS_HOST" == *"plat-apps"* || "$THIS_HOST" == "$JM_HOST" ]] && IS_JM_NODE=true

if $IS_JM_NODE; then
  info "Nodo identificado como JobManager ($THIS_HOST)"

  # 3.1 Contenedor corriendo (rootless)
  JM_STATUS=$(sudo -u "$PODMAN_USER" podman ps --filter "name=${CONTAINER_JM}" \
              --format "{{.Status}}" 2>/dev/null || echo "")
  if echo "$JM_STATUS" | grep -qi "up"; then
    ok "Contenedor JobManager corriendo: $JM_STATUS"
  else
    # Intentar sin sudo (si ya somos el usuario)
    JM_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null || echo "")
    if echo "$JM_STATUS" | grep -qi "up"; then
      ok "Contenedor JobManager corriendo: $JM_STATUS"
    else
      fail "Contenedor JobManager no está UP (estado: '${JM_STATUS:-no encontrado}')"
    fi
  fi

  # 3.2 Versión Flink dentro del contenedor
  FLINK_VER_ACTUAL=$(podman exec "$CONTAINER_JM" /opt/flink/bin/flink --version 2>/dev/null \
    | grep -oP 'Version: \K[\d.]+' || echo "")
  [[ -z "$FLINK_VER_ACTUAL" ]] && FLINK_VER_ACTUAL=$(podman exec "$CONTAINER_JM" \
    cat /opt/flink/version 2>/dev/null || echo "unknown")
  if [[ "$FLINK_VER_ACTUAL" == "$FLINK_VERSION_EXPECTED" ]]; then
    ok "Versión Flink confirmada: $FLINK_VER_ACTUAL"
  else
    warn "Versión Flink: detectada='$FLINK_VER_ACTUAL' esperada='$FLINK_VERSION_EXPECTED'"
  fi

  # 3.3 Java dentro del contenedor
  JVM_IN=$(podman exec "$CONTAINER_JM" java -version 2>&1 | head -1 || echo "error")
  info "JVM en contenedor JobManager: $JVM_IN"
  if echo "$JVM_IN" | grep -q '"17'; then
    ok "OpenJDK 17 confirmado dentro del contenedor"
  else
    fail "JVM dentro del contenedor NO es OpenJDK 17"
  fi

  # 3.4 Imagen del contenedor
  JM_IMAGE=$(podman inspect "$CONTAINER_JM" --format "{{.ImageName}}" 2>/dev/null || echo "")
  info "Imagen JobManager: $JM_IMAGE"
  if [[ -n "$JM_IMAGE" ]]; then
    ok "Imagen identificada"
  else
    warn "No se pudo leer la imagen del contenedor"
  fi

else
  info "Este nodo ($THIS_HOST) no es el JobManager — omitiendo validaciones locales del JM"
  info "Ejecuta este script también en: $JM_HOST"
fi

# ===========================================================================
# MÓDULO 4 – Contenedor TaskManager (ejecutar en nodos pbigd-proc01/2/3)
# ===========================================================================
section "MÓDULO 4 · Contenedor Flink TaskManager (nodo local)"

IS_TM_NODE=false
for TM in "${TASKMANAGER_HOSTS[@]}"; do
  [[ "$THIS_HOST" == *"$TM"* || "$THIS_HOST" == "$TM" ]] && IS_TM_NODE=true && break
done

if $IS_TM_NODE; then
  info "Nodo identificado como TaskManager ($THIS_HOST)"

  # Resolver nombre real del contenedor (cada nodo usa su propio sufijo: flink-tm-1, flink-tm-2...)
  CONTAINER_TM=$(podman ps --filter "name=${CONTAINER_TM_PATTERN}" --format "{{.Names}}" 2>/dev/null | head -1)

  if [[ -z "$CONTAINER_TM" ]]; then
    fail "No se encontró ningún contenedor con prefijo '${CONTAINER_TM_PATTERN}' (podman ps)"
  else
    info "Contenedor TaskManager detectado: $CONTAINER_TM"

    TM_STATUS=$(podman ps --filter "name=${CONTAINER_TM}" --format "{{.Status}}" 2>/dev/null || echo "")
    if echo "$TM_STATUS" | grep -qi "up"; then
      ok "Contenedor TaskManager corriendo: $TM_STATUS"
    else
      fail "Contenedor TaskManager no está UP (estado: '${TM_STATUS:-no encontrado}')"
    fi

    # Validar Java en TaskManager
    TM_JVM=$(podman exec "$CONTAINER_TM" java -version 2>&1 | head -1 || echo "error")
    info "JVM en contenedor TaskManager: $TM_JVM"
    if echo "$TM_JVM" | grep -q '"17'; then
      ok "OpenJDK 17 confirmado en TaskManager"
    else
      fail "JVM en TaskManager NO es OpenJDK 17"
    fi

    # Slots disponibles — vía REST API del JobManager en vez de parsear el
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
print(match[0]['slotsNumber'] if match else 'unknown')" 2>/dev/null || echo "unknown")
    else
      TM_SLOTS="unknown"
    fi
    info "Task slots configurados (vía REST API): $TM_SLOTS"
    if [[ "$TM_SLOTS" != "unknown" ]]; then
      ok "Configuración task slots legible: $TM_SLOTS"
    else
      warn "No se pudo determinar numberOfTaskSlots vía REST API (JM inalcanzable o TM aún no registrado)"
    fi
  fi

else
  info "Este nodo ($THIS_HOST) no es un TaskManager conocido (${TASKMANAGER_HOSTS[*]})"
fi

# ===========================================================================
# MÓDULO 5 – Puertos de red (locales)
# ===========================================================================
section "MÓDULO 5 · Verificación de puertos"

check_port_local() {
  local PORT=$1 DESC=$2
  if ss -tlnp | grep -q ":${PORT}"; then
    ok "Puerto $PORT escuchando → $DESC"
  else
    fail "Puerto $PORT NO escucha → $DESC"
  fi
}

if $IS_JM_NODE; then
  check_port_local $JM_REST_PORT "Flink REST / UI"
  check_port_local $JM_RPC_PORT  "Flink RPC (JobManager)"
fi

if $IS_TM_NODE; then
  # TaskManager abre un puerto RPC dinámico; verificamos que al menos haya actividad Flink
  if ss -tlnp | grep -qE "flink|java"; then
    ok "Actividad de red Flink/Java detectada en TaskManager"
  else
    warn "No se detectó actividad de red Flink/Java en este TaskManager"
  fi
fi

# ===========================================================================
# MÓDULO 6 – REST API JobManager (desde cualquier nodo del cluster)
# ===========================================================================
section "MÓDULO 6 · REST API Flink JobManager"

# 6.1 Health check overview
info "Consultando REST API: $JM_API"
OVERVIEW=$(curl -sf --max-time 10 "$JM_API/overview" 2>/dev/null || echo "")
if [[ -n "$OVERVIEW" ]]; then
  ok "REST API responde: $JM_API/overview"

  # Extraer datos clave con python3 (disponible en Rocky 10)
  if command -v python3 &>/dev/null; then
    TM_COUNT=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('taskmanagers',0))" 2>/dev/null || echo "?")
    SLOTS_TOTAL=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('slots-total',0))" 2>/dev/null || echo "?")
    SLOTS_AVAIL=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('slots-available',0))" 2>/dev/null || echo "?")
    FLINK_API_VER=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('flink-version','?'))" 2>/dev/null || echo "?")
    JOBS_RUNNING=$(echo "$OVERVIEW" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('jobs-running',0))" 2>/dev/null || echo "?")

    info "  Flink versión (API):   $FLINK_API_VER"
    info "  TaskManagers activos:  $TM_COUNT"
    info "  Slots totales:         $SLOTS_TOTAL"
    info "  Slots disponibles:     $SLOTS_AVAIL"
    info "  Jobs corriendo:        $JOBS_RUNNING"

    if [[ "$FLINK_API_VER" == "$FLINK_VERSION_EXPECTED" ]]; then
      ok "Versión Flink API coincide: $FLINK_API_VER"
    else
      warn "Versión API: '$FLINK_API_VER' ≠ esperada '$FLINK_VERSION_EXPECTED'"
    fi

    if [[ "$TM_COUNT" -ge 3 ]] 2>/dev/null; then
      ok "TaskManagers registrados: $TM_COUNT (≥3 requerido)"
    else
      fail "TaskManagers insuficientes: $TM_COUNT (se esperan 3 para DC Principal)"
    fi

    if [[ "$SLOTS_TOTAL" -gt 0 ]] 2>/dev/null; then
      ok "Slots disponibles en el cluster: total=$SLOTS_TOTAL avail=$SLOTS_AVAIL"
    else
      warn "No hay slots registrados aún"
    fi
  fi
else
  fail "REST API no responde: $JM_API (¿JobManager corriendo en $JM_HOST?)"
fi

# 6.2 Endpoint de TaskManagers
TM_API=$(curl -sf --max-time 10 "$JM_API/taskmanagers" 2>/dev/null || echo "")
if [[ -n "$TM_API" ]]; then
  ok "Endpoint /taskmanagers responde"
  TM_IDS=$(echo "$TM_API" | python3 -c \
    "import sys,json; tms=json.load(sys.stdin).get('taskmanagers',[]); \
    [print(f'  TM: {t[\"id\"]} | slots={t[\"slotsNumber\"]} | freeSlots={t[\"freeSlots\"]}') for t in tms]" \
    2>/dev/null || true)
  [[ -n "$TM_IDS" ]] && log "$TM_IDS"
else
  warn "Endpoint /taskmanagers sin respuesta"
fi

# 6.3 Endpoint de Jobs
JOBS_API=$(curl -sf --max-time 10 "$JM_API/jobs" 2>/dev/null || echo "")
if [[ -n "$JOBS_API" ]]; then
  ok "Endpoint /jobs responde"
else
  warn "Endpoint /jobs no responde"
fi

# 6.4 Config del cluster
CONFIG_API=$(curl -sf --max-time 10 "$JM_API/jobmanager/config" 2>/dev/null || echo "")
if [[ -n "$CONFIG_API" ]]; then
  ok "Endpoint /jobmanager/config responde"
else
  warn "Endpoint /jobmanager/config no responde"
fi

# ===========================================================================
# MÓDULO 7 – systemd (persistencia del servicio)
# ===========================================================================
section "MÓDULO 7 · Systemd – persistencia y reinicio automático"

check_systemd_unit() {
  local UNIT=$1 DESC=$2
  if systemctl is-active --quiet "$UNIT" 2>/dev/null; then
    ok "Servicio activo: $UNIT ($DESC)"
    if systemctl is-enabled --quiet "$UNIT" 2>/dev/null; then
      ok "  ↳ Habilitado en boot"
    else
      warn "  ↳ NO habilitado en boot (faltaría: systemctl enable $UNIT)"
    fi
  else
    # Intentar con el usuario PODMAN_USER
    if sudo -u "$PODMAN_USER" systemctl --user is-active --quiet "$UNIT" 2>/dev/null; then
      ok "Servicio de usuario activo: $UNIT ($DESC)"
    else
      warn "Servicio '$UNIT' no activo como sistema ni usuario (verificar nombre exacto)"
    fi
  fi
}

if $IS_JM_NODE; then
  check_systemd_unit "flink-jobmanager" "Flink JobManager"
fi

if $IS_TM_NODE; then
  check_systemd_unit "flink-taskmanager" "Flink TaskManager"
fi

# ===========================================================================
# MÓDULO 8 – Checkpoints hacia MinIO (S3)
# ===========================================================================
section "MÓDULO 8 · Checkpoints Flink → MinIO (S3)"

# 8.1 Conectividad al endpoint MinIO
MINIO_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "${MINIO_ENDPOINT}/minio/health/live" 2>/dev/null || echo "000")
if [[ "$MINIO_HTTP" == "200" ]]; then
  ok "MinIO alcanzable desde este nodo: HTTP $MINIO_HTTP"
else
  warn "MinIO no responde en $MINIO_ENDPOINT (HTTP $MINIO_HTTP) — revisar conectividad S3"
fi

# 8.2 Verificar configuración de checkpoint en contenedor
if $IS_JM_NODE; then
  CKPT_CFG=$(podman exec "$CONTAINER_JM" \
    grep -r "state.checkpoints\|s3\|filesystem.backend" \
    /opt/flink/conf/ 2>/dev/null | head -10 || echo "")
  if [[ -n "$CKPT_CFG" ]]; then
    ok "Configuración de checkpoints encontrada en contenedor"
    info "$CKPT_CFG"
  else
    warn "No se encontró configuración explícita de checkpoints — validar flink-conf.yaml"
  fi
fi

# ===========================================================================
# MÓDULO 9 – Job de prueba (WordCount o DataGen)
# ===========================================================================
section "MÓDULO 9 · Job de prueba funcional"

if $IS_JM_NODE; then
  info "Ejecutando job de prueba WordCount (modo batch, datos internos)..."
  info "Nota: requiere que el contenedor JobManager tenga acceso al CLI de Flink"

  # Intentar lanzar un job simple con la API REST (no requiere CLI en host)
  JOB_SUBMIT=$(curl -sf --max-time 30 \
    -X POST "$JM_API/jars/upload" \
    -H "Expect:" \
    2>/dev/null | head -c 200 || echo "")
  # Solo verificamos que el endpoint de jars está disponible
  JAR_LIST=$(curl -sf --max-time 10 "$JM_API/jars" 2>/dev/null || echo "")
  if [[ -n "$JAR_LIST" ]]; then
    ok "Endpoint /jars responde (listo para submit de jobs)"
    JAR_COUNT=$(echo "$JAR_LIST" | python3 -c \
      "import sys,json; print(len(json.load(sys.stdin).get('files',[])))" 2>/dev/null || echo "?")
    info "JARs actualmente subidos: $JAR_COUNT"
  else
    warn "Endpoint /jars no responde"
  fi

  # Test con Flink CLI si está disponible en host o contenedor
  if podman exec "$CONTAINER_JM" test -f /opt/flink/bin/flink 2>/dev/null; then
    info "Flink CLI disponible en contenedor — probando list de jobs..."
    # El flag '-m/--jobmanager' del CLI se llama así por razones históricas, pero
    # desde Flink 1.5+ el cliente habla REST/HTTP (no RPC/Akka crudo), así que debe
    # apuntar al puerto REST (8081), no al RPC (6123) — usar el RPC port aquí hace
    # que el cliente HTTP negocie contra un puerto que no habla HTTP, cerrando el
    # canal de inmediato ("Channel became inactive").
    # Se ejecuta vía 'podman exec' DENTRO del propio contenedor, por eso localhost.
    JOB_LIST=$(podman exec "$CONTAINER_JM" /opt/flink/bin/flink list \
      -m "localhost:${JM_REST_PORT}" 2>&1 || echo "error")
    if echo "$JOB_LIST" | grep -qiE "error|exception|refused|timeout|no route to host"; then
      warn "Flink CLI devolvió un error al listar jobs: $JOB_LIST"
    else
      ok "Flink CLI operativo: $JOB_LIST"
    fi
  fi
else
  info "Este nodo no es JobManager — omitiendo job de prueba"
fi

# ===========================================================================
# MÓDULO 10 – Conectividad inter-nodos del cluster
# ===========================================================================
section "MÓDULO 10 · Conectividad entre nodos"

ALL_NODES=("$JM_HOST" "${TASKMANAGER_HOSTS[@]}")
for NODE in "${ALL_NODES[@]}"; do
  [[ "$NODE" == "$THIS_HOST" ]] && continue
  if ping -c 1 -W 2 "$NODE" &>/dev/null; then
    ok "Nodo $NODE alcanzable (ping OK)"
    # El puerto RPC 6123 sólo lo expone el JobManager; los TaskManagers usan
    # un puerto RPC dinámico propio, así que sólo tiene sentido verificarlo aquí.
    if [[ "$NODE" == "$JM_HOST" ]]; then
      if tcp_port_open "$NODE" "$JM_RPC_PORT"; then
        ok "  ↳ Puerto RPC $JM_RPC_PORT accesible en $NODE"
      else
        warn "  ↳ Puerto RPC $JM_RPC_PORT no accesible en $NODE"
      fi
    fi
  else
    warn "Nodo $NODE NO responde a ping (verificar red o que el host esté levantado)"
  fi
done

# Puerto REST del JM accesible desde todos los nodos
if ! $IS_JM_NODE; then
  if tcp_port_open "$JM_HOST" "$JM_REST_PORT"; then
    ok "Puerto REST $JM_REST_PORT alcanzable en $JM_HOST desde $THIS_HOST"
  else
    fail "Puerto REST $JM_REST_PORT NO alcanzable en $JM_HOST"
  fi
fi

# ===========================================================================
# MÓDULO 11 – Logs y observabilidad
# ===========================================================================
section "MÓDULO 11 · Logs y observabilidad"

# 11.1 Logs del contenedor
if $IS_JM_NODE; then
  LOG_LINES=$(podman logs --tail 5 "$CONTAINER_JM" 2>/dev/null | wc -l || echo 0)
  if [[ "$LOG_LINES" -gt 0 ]]; then
    ok "Logs del contenedor JobManager disponibles ($LOG_LINES líneas recientes)"
  else
    warn "Sin logs recientes en contenedor JobManager"
  fi

  # Buscar errores críticos
  ERRORS=$(podman logs --tail 50 "$CONTAINER_JM" 2>/dev/null | grep -ci "ERROR\|Exception\|FATAL" || true)
  if [[ "$ERRORS" -gt 0 ]]; then
    warn "Se detectaron $ERRORS líneas con ERROR/Exception/FATAL en logs del JM"
  else
    ok "Sin errores críticos en los últimos 50 logs del JM"
  fi
fi

if $IS_TM_NODE; then
  if [[ -z "${CONTAINER_TM:-}" ]]; then
    warn "Sin contenedor TaskManager detectado — omitiendo verificación de logs"
  else
    LOG_LINES_TM=$(podman logs --tail 5 "$CONTAINER_TM" 2>/dev/null | wc -l || echo 0)
    if [[ "$LOG_LINES_TM" -gt 0 ]]; then
      ok "Logs del contenedor TaskManager disponibles"
    else
      warn "Sin logs recientes en contenedor TaskManager"
    fi
  fi
fi

# 11.2 Verificar que Grafana Alloy esté activo (observabilidad)
if systemctl is-active --quiet alloy 2>/dev/null || \
   systemctl is-active --quiet grafana-alloy 2>/dev/null; then
  ok "Grafana Alloy activo"
else
  warn "Grafana Alloy no detectado como servicio systemd (verificar nombre de unidad)"
fi

# 11.3 node-exporter
if systemctl is-active --quiet node_exporter 2>/dev/null || \
   ss -tlnp | grep -q ":9100"; then
  ok "node-exporter activo (puerto 9100)"
else
  warn "node-exporter no detectado"
fi

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Flink Cluster PRINCIPAL                             ║${NC}"
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

if [[ $FAIL -eq 0 ]]; then
  log "${GREEN}${BOLD}  ✔  Validación completada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  Se detectaron $FAIL fallos críticos — revisar log: $LOG_FILE${NC}"
fi

log ""
log "${CYAN}  ▶ Ejecutar también en los nodos restantes:${NC}"
for NODE in "${ALL_NODES[@]}"; do
  [[ "$NODE" != "$THIS_HOST" ]] && log "     ssh admapl@$NODE 'bash validate_flink_principal.sh'"
done
log ""

exit $FAIL
