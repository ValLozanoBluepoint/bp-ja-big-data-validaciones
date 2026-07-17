#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Kafka ↔ Flink (DC Principal) — LADO FLINK
#
#  Reemplaza la parte automática de validate_kafka_flink_principal.sh
#  (deprecado). No depende de SSH hacia los nodos Kafka: el topic de prueba
#  debe existir de antemano (creado por validate_kafka_flink_principal_kafka.sh,
#  ejecutado manualmente en un nodo Kafka) y se pasa aquí con --topic.
#
#  Flujo completo: ver preparacion_integracion_kafka_flink.md
#
#  Topología (DC Principal):
#    JobManager   : pbigd-plat-apps01   (contenedor: flink-jobmanager)
#    TaskManagers : pbigd-proc01 | pbigd-proc02 | pbigd-proc03
#    Brokers Kafka: pbigd-kaf01 | pbigd-kaf02 | pbigd-kaf03 (contenedor: kafka)
#
#  Versiones validadas: Kafka 4.0.1 (KRaft) · Flink 2.2.1 (mismas que fijan
#  validate_kafka_principal.sh / validate_flink_principal.sh).
#
#  Requisitos previos (ver preparacion_integracion_kafka_flink.md):
#    - Conector Kafka instalado en Flink (se verifica en Módulo 1, no se instala aquí).
#    - Topic de prueba ya creado y con mensajes producidos por el script del lado Kafka.
#    - Ejecutar desde el nodo JobManager (pbigd-plat-apps01) o pasar --jobmanager-host.
#    - SSH sin contraseña (clave) del usuario admapl entre los nodos Flink
#      (JobManager → TaskManagers) para el chequeo de conectividad multi-nodo
#      del Módulo 2. Esto es SSH interno de Flink, no hacia Kafka.
#    - python3 disponible (parseo de JSON de la REST API de Flink).
#
#  Uso:
#    chmod +x validate_kafka_flink_principal_flink.sh
#    ./validate_kafka_flink_principal_flink.sh --topic <topic> \
#        [--jobmanager-host host] [--skip-functional-test] [--skip-tm-connectivity]
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01}"
JM_REST_PORT=8081
KAFKA_BROKER_PORT=9092
TASKMANAGER_HOSTS=("pbigd-proc01" "pbigd-proc02" "pbigd-proc03")
KAFKA_NODES=("pbigd-kaf01" "pbigd-kaf02" "pbigd-kaf03")

CONTAINER_JM="flink-jobmanager"
CONTAINER_TM_PATTERN="flink-tm"
FLINK_HOME="/opt/flink"
PODMAN_USER="admapl"

TEST_TOPIC=""
TEST_MESSAGE_PREFIX="bluepoint-it"
LATENCY_TIMEOUT_S=20
SQL_JOB_STARTUP_WAIT_S=15

SKIP_FUNCTIONAL_TEST=false
SKIP_TM_CONNECTIVITY=false

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_kafka_flink_principal_flink_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

tcp_port_open() {
  local host=$1 port=$2 timeout=${3:-3}
  timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --topic)                 TEST_TOPIC="$2"; shift 2 ;;
    --jobmanager-host)       JM_HOST="$2"; shift 2 ;;
    --skip-functional-test)  SKIP_FUNCTIONAL_TEST=true; shift ;;
    --skip-tm-connectivity)  SKIP_TM_CONNECTIVITY=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

if [[ -z "$TEST_TOPIC" ]]; then
  echo "ERROR: --topic es obligatorio. Ejecutar primero validate_kafka_flink_principal_kafka.sh en un nodo Kafka"
  echo "y pasar aquí el nombre de topic que imprime al final."
  exit 1
fi

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
JM_API="http://${JM_HOST}:${JM_REST_PORT}"

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Integración Kafka↔Flink — PRINCIPAL · LADO FLINK   ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución: $THIS_HOST"
info "JobManager Flink : $JM_HOST ($JM_API)"
info "TaskManagers     : ${TASKMANAGER_HOSTS[*]}"
info "Brokers Kafka    : ${KAFKA_NODES[*]}"
info "Topic de prueba (creado previamente en Kafka): $TEST_TOPIC"
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_kafka_principal.sh,${NC}"
log "${CYAN}  validate_flink_principal.sh y validate_kafka_flink_principal_kafka.sh${NC}"
log "${CYAN}  ya se ejecutaron. Aquí solo se valida el lado Flink de la integración.${NC}"
log ""

# =============================================================================
# MÓDULO 1 — Prerequisito: conector Kafka presente en Flink
# =============================================================================
section "MÓDULO 1 · Conector Kafka en el contenedor JobManager"

CONNECTOR_OK=false
JM_CONTAINER_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null || echo "")

if echo "$JM_CONTAINER_STATUS" | grep -qi "up"; then
  ok "Contenedor JobManager local corriendo: $JM_CONTAINER_STATUS"

  CONNECTOR_JAR=$(podman exec "$CONTAINER_JM" sh -c "ls ${FLINK_HOME}/lib/ 2>/dev/null | grep -i kafka" || echo "")
  if [[ -n "$CONNECTOR_JAR" ]]; then
    ok "Conector Kafka encontrado en ${FLINK_HOME}/lib/: $CONNECTOR_JAR"
    CONNECTOR_OK=true
  else
    fail "No se encontró flink-sql-connector-kafka-*.jar en ${FLINK_HOME}/lib/ — el módulo 3 (integración funcional) no podrá ejecutarse. Ver preparacion_integracion_kafka_flink.md (Prerequisito 1)"
  fi
else
  warn "Contenedor JobManager ($CONTAINER_JM) no detectado localmente en $THIS_HOST — verificando vía REST API en su lugar"
  if curl -sf --max-time 10 "$JM_API/jars" &>/dev/null; then
    warn "Ejecutar este script en $JM_HOST para validar el conector Kafka localmente"
  else
    fail "No se pudo verificar el conector Kafka (ni contenedor local ni REST API accesible)"
  fi
fi

# =============================================================================
# MÓDULO 2 — Conectividad Flink → Kafka (JobManager + cada TaskManager)
# =============================================================================
section "MÓDULO 2 · Conectividad Flink → Kafka (puerto ${KAFKA_BROKER_PORT})"

info "Verificando alcance a los ${#KAFKA_NODES[@]} brokers Kafka desde $THIS_HOST (JobManager)..."
KAFKA_REACHABLE_COUNT=0
for BROKER in "${KAFKA_NODES[@]}"; do
  if tcp_port_open "$BROKER" "$KAFKA_BROKER_PORT"; then
    ok "TCP ${BROKER}:${KAFKA_BROKER_PORT} alcanzable desde $THIS_HOST"
    KAFKA_REACHABLE_COUNT=$((KAFKA_REACHABLE_COUNT+1))
  else
    fail "TCP ${BROKER}:${KAFKA_BROKER_PORT} NO alcanzable desde $THIS_HOST — revisar firewall/red entre segmento compute y segmento Kafka"
  fi
done

if [[ "$KAFKA_REACHABLE_COUNT" -eq "${#KAFKA_NODES[@]}" ]]; then
  ok "Los ${#KAFKA_NODES[@]} brokers Kafka son alcanzables desde el JobManager"
else
  fail "Solo $KAFKA_REACHABLE_COUNT/${#KAFKA_NODES[@]} brokers Kafka alcanzables desde el JobManager"
fi

# Cobertura completa: repetir el mismo chequeo TCP en cada TaskManager vía SSH
# interno entre nodos Flink (no hacia Kafka; mismo patrón de
# preparacion_validacion_flink.md para correr validate_flink_principal.sh).
if $SKIP_TM_CONNECTIVITY; then
  info "Chequeo de conectividad en TaskManagers omitido (--skip-tm-connectivity)"
else
  info "Verificando conectividad TCP ${KAFKA_BROKER_PORT} desde cada TaskManager (SSH interno Flink)..."
  for TM in "${TASKMANAGER_HOSTS[@]}"; do
    TM_REACHABLE_COUNT=0
    for BROKER in "${KAFKA_NODES[@]}"; do
      CHECK_CMD="timeout 3 bash -c 'cat < /dev/null > /dev/tcp/${BROKER}/${KAFKA_BROKER_PORT}' 2>/dev/null && echo OPEN || echo CLOSED"
      RESULT=$(ssh -o ConnectTimeout=5 -o BatchMode=yes "${PODMAN_USER}@${TM}" "$CHECK_CMD" 2>/dev/null || echo "SSH_ERROR")
      if [[ "$RESULT" == "OPEN" ]]; then
        TM_REACHABLE_COUNT=$((TM_REACHABLE_COUNT+1))
      elif [[ "$RESULT" == "SSH_ERROR" ]]; then
        warn "No se pudo conectar por SSH a $TM para probar TCP hacia $BROKER"
      else
        fail "TCP ${BROKER}:${KAFKA_BROKER_PORT} NO alcanzable desde TaskManager $TM"
      fi
    done
    if [[ "$TM_REACHABLE_COUNT" -eq "${#KAFKA_NODES[@]}" ]]; then
      ok "TaskManager $TM alcanza los ${#KAFKA_NODES[@]} brokers Kafka"
    else
      fail "TaskManager $TM solo alcanza $TM_REACHABLE_COUNT/${#KAFKA_NODES[@]} brokers Kafka"
    fi
  done
fi

# =============================================================================
# MÓDULO 3 — Integración funcional end-to-end (topic ya creado por el lado Kafka)
# =============================================================================
section "MÓDULO 3 · Integración funcional Kafka → Flink (end-to-end)"

if $SKIP_FUNCTIONAL_TEST; then
  info "Módulo 3 omitido (--skip-functional-test)"
elif ! $CONNECTOR_OK; then
  warn "Módulo 3 omitido: conector Kafka no confirmado en el contenedor JobManager (ver Módulo 1)"
elif [[ "$KAFKA_REACHABLE_COUNT" -eq 0 ]]; then
  warn "Módulo 3 omitido: ningún broker Kafka alcanzable desde el JobManager (ver Módulo 2)"
else
  SQL_SCRIPT_LOCAL="/tmp/${TEST_TOPIC}_flinkside.sql"
  SQL_SCRIPT_CONTAINER="/tmp/${TEST_TOPIC}_flinkside.sql"

  info "Generando job Flink SQL (fuente Kafka → sink print) contra topic existente '$TEST_TOPIC'..."
  cat > "$SQL_SCRIPT_LOCAL" <<SQLEOF
CREATE TABLE kafka_it_source (
  mensaje STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = '${TEST_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_NODES[0]}:${KAFKA_BROKER_PORT},${KAFKA_NODES[1]}:${KAFKA_BROKER_PORT},${KAFKA_NODES[2]}:${KAFKA_BROKER_PORT}',
  'properties.group.id'          = 'bluepoint-kafka-flink-it',
  'scan.startup.mode'            = 'earliest-offset',
  'format'                       = 'raw'
);

CREATE TABLE print_sink (
  mensaje STRING
) WITH (
  'connector' = 'print'
);

INSERT INTO print_sink SELECT * FROM kafka_it_source;
SQLEOF

  if podman cp "$SQL_SCRIPT_LOCAL" "${CONTAINER_JM}:${SQL_SCRIPT_CONTAINER}" 2>/dev/null; then
    ok "Script SQL copiado al contenedor JobManager"

    info "Sometiendo job Flink SQL (background)..."
    podman exec -d "$CONTAINER_JM" "${FLINK_HOME}/bin/sql-client.sh" -f "$SQL_SCRIPT_CONTAINER" \
      > /tmp/${TEST_TOPIC}_flinkside_sqlclient.log 2>&1

    info "Esperando ${SQL_JOB_STARTUP_WAIT_S}s a que el job quede RUNNING..."
    sleep "$SQL_JOB_STARTUP_WAIT_S"

    JOBS_JSON=$(curl -sf --max-time 10 "$JM_API/jobs" 2>/dev/null || echo "")
    JOB_ID=""
    if [[ -n "$JOBS_JSON" ]] && command -v python3 &>/dev/null; then
      JOB_ID=$(echo "$JOBS_JSON" | python3 -c \
        "import sys,json
jobs=json.load(sys.stdin).get('jobs',[])
running=[j for j in jobs if j.get('status')=='RUNNING']
print(running[-1]['id'] if running else '')" 2>/dev/null || echo "")
    fi

    if [[ -n "$JOB_ID" ]]; then
      ok "Job Flink SQL RUNNING confirmado vía REST API (job id: $JOB_ID)"
    else
      fail "No se detectó ningún job RUNNING tras ${SQL_JOB_STARTUP_WAIT_S}s — ver /tmp/${TEST_TOPIC}_flinkside_sqlclient.log en $THIS_HOST"
    fi

    info "Verificando llegada de mensajes al sink 'print' (timeout ${LATENCY_TIMEOUT_S}s)..."
    MESSAGE_SEEN=false
    DEADLINE=$(( $(date +%s) + LATENCY_TIMEOUT_S ))
    TM_CONTAINER=$(podman ps --filter "name=${CONTAINER_TM_PATTERN}" --format "{{.Names}}" 2>/dev/null | head -1)
    while [[ $(date +%s) -lt $DEADLINE ]]; do
      if [[ -n "$TM_CONTAINER" ]] && podman logs "$TM_CONTAINER" 2>/dev/null | grep -q "${TEST_MESSAGE_PREFIX}-"; then
        MESSAGE_SEEN=true
        break
      fi
      sleep 2
    done

    if $MESSAGE_SEEN; then
      ok "Mensajes de prueba (producidos por el lado Kafka) detectados en el sink (TaskManager: $TM_CONTAINER)"
    else
      fail "Los mensajes de prueba NO aparecieron en el sink dentro de ${LATENCY_TIMEOUT_S}s — integración Kafka→Flink no confirmada"
    fi

    info "Cancelando job de prueba..."
    if [[ -n "$JOB_ID" ]]; then
      curl -sf --max-time 10 -X PATCH "$JM_API/jobs/${JOB_ID}" &>/dev/null && \
        ok "Job $JOB_ID cancelado vía REST API" || \
        warn "No se pudo cancelar el job $JOB_ID vía REST API — cancelar manualmente"
    fi

    rm -f "$SQL_SCRIPT_LOCAL"
    podman exec "$CONTAINER_JM" rm -f "$SQL_SCRIPT_CONTAINER" 2>/dev/null || true
  else
    fail "No se pudo copiar el script SQL al contenedor JobManager ($CONTAINER_JM) — ejecutar este script en $JM_HOST"
  fi
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Lado Flink · Integración Kafka↔Flink · PRINCIPAL     ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Integración Kafka↔Flink validada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  Se detectaron $FAIL fallos críticos en la integración — revisar log: $LOG_FILE${NC}"
fi

log ""
log "${CYAN}  Siguiente paso: volver al nodo Kafka y ejecutar:${NC}"
log "${CYAN}    ./validate_kafka_flink_principal_kafka.sh --cleanup ${TEST_TOPIC}${NC}"
log ""

exit $FAIL
