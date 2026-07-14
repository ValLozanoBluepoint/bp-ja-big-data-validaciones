#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Kafka ↔ Flink (DR — Datacenter Alterno)
#
#  Complementa a validacion_kafka/validate_kafka_dr.sh y
#  validacion_flink/validate_flink_dr.sh, que validan cada cluster DR por
#  separado y declaran explícitamente que la integración entre ambos
#  queda fuera de su alcance. Este script cubre ese espacio en el
#  datacenter alterno.
#
#  Topología DR (capacidad reducida en Flink, completa en Kafka — decisión
#  de diseño de Bluepoint: Kafka es el punto de entrada crítico del flujo
#  de datos y no se reduce en contingencia):
#    JobManager   : pbigd-plat-apps01-cont   (contenedor: flink-jobmanager)
#    TaskManagers : pbigd-proc01-cont | pbigd-proc02-cont  (solo 2 nodos)
#    Brokers Kafka: pbigd-kaf01-cont | pbigd-kaf02-cont | pbigd-kaf03-cont
#                   (contenedor: kafka-broker-cont-<índice>, capacidad
#                   completa 3/3, igual que Principal)
#
#  Versiones validadas: Kafka 4.0.1 (KRaft) · Flink 2.2.1 (mismas que fijan
#  validate_kafka_dr.sh / validate_flink_dr.sh).
#
#  Requisitos:
#    - Ejecutar desde el nodo JobManager DR (pbigd-plat-apps01-cont) o pasar
#      --jobmanager-host / --kafka-node si se ejecuta desde otro nodo.
#    - SSH sin contraseña (clave) del usuario admapl hacia el nodo Kafka DR
#      usado para crear/producir/borrar el topic de prueba.
#    - python3 disponible (parseo de JSON de la REST API de Flink).
#
#  Uso:
#    chmod +x validate_kafka_flink_dr.sh
#    ./validate_kafka_flink_dr.sh [--jobmanager-host host] \
#        [--kafka-node host] [--skip-functional-test]
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DR
# ---------------------------------------------------------------------------
JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01-cont}"
JM_REST_PORT=8081
KAFKA_BROKER_PORT=9092
TASKMANAGER_HOSTS=("pbigd-proc01-cont" "pbigd-proc02-cont")   # DR: solo 2 nodos (capacidad reducida)
KAFKA_NODES=("pbigd-kaf01-cont" "pbigd-kaf02-cont" "pbigd-kaf03-cont")  # DR: capacidad completa
KAFKA_NODE_FOR_TEST="pbigd-kaf01-cont"

CONTAINER_JM="flink-jobmanager"
CONTAINER_TM_PATTERN="flink-tm"
KAFKA_HOME="/opt/kafka"
FLINK_HOME="/opt/flink"
PODMAN_USER="admapl"

# Umbrales DR — igual criterio que validate_flink_dr.sh (capacidad Flink
# reducida intencionalmente) y validate_kafka_dr.sh (Kafka sin reducir).
MIN_KAFKA_BROKERS_REACHABLE=3   # Kafka DR exige capacidad completa, igual que Principal
MIN_FLINK_NODES_REACHABLE=1     # Flink DR acepta degradación (mínimo 1 TM operativo)

TEST_TOPIC="bluepoint-kafka-flink-it-dr-$(date +%s)"
TEST_MESSAGE_PREFIX="bluepoint-it-dr"
MESSAGE_COUNT=3
LATENCY_TIMEOUT_S=25             # ventana algo mayor que Principal: recursos DR más limitados
SQL_JOB_STARTUP_WAIT_S=20

SKIP_FUNCTIONAL_TEST=false

# ---------------------------------------------------------------------------
# Colores y helpers (mismo estilo que validate_kafka_dr.sh / validate_flink_dr.sh)
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; MAGENTA='\033[0;35m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_kafka_flink_dr_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
dr()   { log "${MAGENTA}  [DR]${NC}  $*"; }
section() { log "\n${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${MAGENTA}  $*${NC}"; \
            log "${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

tcp_port_open() {
  local host=$1 port=$2 timeout=${3:-3}
  timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobmanager-host)      JM_HOST="$2"; shift 2 ;;
    --kafka-node)           KAFKA_NODE_FOR_TEST="$2"; shift 2 ;;
    --skip-functional-test) SKIP_FUNCTIONAL_TEST=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
JM_API="http://${JM_HOST}:${JM_REST_PORT}"

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Integración Kafka ↔ Flink — DR (Alterno)║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
dr "NOTA: Flink DR opera con capacidad reducida (2 TaskManagers); Kafka DR"
dr "mantiene capacidad completa (3/3) — es el punto de entrada crítico del flujo."
log ""
info "Nodo de ejecución: $THIS_HOST"
info "JobManager Flink DR : $JM_HOST ($JM_API)"
info "TaskManagers DR      : ${TASKMANAGER_HOSTS[*]}"
info "Brokers Kafka DR     : ${KAFKA_NODES[*]}"
info "Nodo usado para crear/producir/borrar el topic de prueba: $KAFKA_NODE_FOR_TEST"
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_kafka_dr.sh y${NC}"
log "${CYAN}  validate_flink_dr.sh ya se ejecutaron y ambos clusters DR están${NC}"
log "${CYAN}  operativos por separado. Aquí solo se valida la integración entre ambos.${NC}"
log ""

# =============================================================================
# MÓDULO 1 — Prerequisito: conector Kafka presente en Flink DR
# =============================================================================
section "MÓDULO 1 · Conector Kafka en el contenedor JobManager DR"

CONNECTOR_OK=false
JM_CONTAINER_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null || echo "")

if echo "$JM_CONTAINER_STATUS" | grep -qi "up"; then
  ok "Contenedor JobManager DR local corriendo: $JM_CONTAINER_STATUS"

  CONNECTOR_JAR=$(podman exec "$CONTAINER_JM" sh -c "ls ${FLINK_HOME}/lib/ 2>/dev/null | grep -i kafka" || echo "")
  if [[ -n "$CONNECTOR_JAR" ]]; then
    ok "Conector Kafka encontrado en ${FLINK_HOME}/lib/: $CONNECTOR_JAR"
    CONNECTOR_OK=true
  else
    fail "No se encontró flink-sql-connector-kafka-*.jar en ${FLINK_HOME}/lib/ — el módulo 3 no podrá ejecutarse — CRÍTICO en DR"
  fi
else
  warn "Contenedor JobManager DR ($CONTAINER_JM) no detectado localmente en $THIS_HOST"
  if curl -sf --max-time 10 "$JM_API/jars" &>/dev/null; then
    warn "Ejecutar este script en $JM_HOST para validar el conector Kafka localmente"
  else
    fail "No se pudo verificar el conector Kafka (ni contenedor local ni REST API accesible) — DR podría no estar operativo"
  fi
fi

# =============================================================================
# MÓDULO 2 — Conectividad Flink DR → Kafka DR
# =============================================================================
section "MÓDULO 2 · Conectividad Flink DR → Kafka DR (puerto ${KAFKA_BROKER_PORT})"

info "Verificando alcance a los ${#KAFKA_NODES[@]} brokers Kafka DR desde $THIS_HOST..."
KAFKA_REACHABLE_COUNT=0
for BROKER in "${KAFKA_NODES[@]}"; do
  if tcp_port_open "$BROKER" "$KAFKA_BROKER_PORT"; then
    ok "TCP ${BROKER}:${KAFKA_BROKER_PORT} alcanzable desde $THIS_HOST"
    KAFKA_REACHABLE_COUNT=$((KAFKA_REACHABLE_COUNT+1))
  else
    fail "TCP ${BROKER}:${KAFKA_BROKER_PORT} NO alcanzable desde $THIS_HOST"
  fi
done

# Kafka DR no se reduce: se exige el mismo umbral pleno que Principal (3/3)
if [[ "$KAFKA_REACHABLE_COUNT" -ge "$MIN_KAFKA_BROKERS_REACHABLE" ]]; then
  ok "Brokers Kafka DR alcanzables: $KAFKA_REACHABLE_COUNT/${#KAFKA_NODES[@]} (umbral: $MIN_KAFKA_BROKERS_REACHABLE — sin reducir)"
else
  fail "Brokers Kafka DR alcanzables: $KAFKA_REACHABLE_COUNT/${#KAFKA_NODES[@]} — por debajo del umbral pleno exigido ($MIN_KAFKA_BROKERS_REACHABLE)"
fi

info "Nota: para cobertura completa, ejecutar este módulo también desde cada TaskManager DR (${TASKMANAGER_HOSTS[*]})."

# =============================================================================
# MÓDULO 3 — Integración funcional end-to-end en DR
# =============================================================================
section "MÓDULO 3 · Integración funcional Kafka DR → Flink DR (end-to-end)"

if $SKIP_FUNCTIONAL_TEST; then
  info "Módulo 3 omitido (--skip-functional-test)"
elif ! $CONNECTOR_OK; then
  warn "Módulo 3 omitido: conector Kafka no confirmado en el contenedor JobManager DR (ver Módulo 1)"
elif [[ "$KAFKA_REACHABLE_COUNT" -eq 0 ]]; then
  warn "Módulo 3 omitido: ningún broker Kafka DR alcanzable (ver Módulo 2)"
else
  SQL_SCRIPT_LOCAL="/tmp/${TEST_TOPIC}.sql"
  SQL_SCRIPT_CONTAINER="/tmp/${TEST_TOPIC}.sql"

  # 3.1 Crear topic de prueba en Kafka DR
  info "3.1 Creando topic de prueba '$TEST_TOPIC' en $KAFKA_NODE_FOR_TEST..."
  CREATE_OUT=$(ssh -o ConnectTimeout=5 -o BatchMode=yes "${PODMAN_USER}@${KAFKA_NODE_FOR_TEST}" \
    "podman exec kafka ${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server localhost:${KAFKA_BROKER_PORT} \
       --create --topic ${TEST_TOPIC} --partitions 3 --replication-factor 3" 2>&1 || echo "ERROR")

  if echo "$CREATE_OUT" | grep -qi "created topic\|already exists"; then
    ok "Topic de prueba creado en DR: $TEST_TOPIC"
    TOPIC_CREATED=true
  else
    fail "No se pudo crear el topic de prueba en $KAFKA_NODE_FOR_TEST — salida: $CREATE_OUT"
    TOPIC_CREATED=false
  fi

  if $TOPIC_CREATED; then
    # 3.2 Generar script SQL: tabla fuente Kafka + sink 'print'
    info "3.2 Generando job Flink SQL DR (fuente Kafka → sink print)..."
    KAFKA_BOOTSTRAP_LIST="${KAFKA_NODES[0]}:${KAFKA_BROKER_PORT},${KAFKA_NODES[1]}:${KAFKA_BROKER_PORT},${KAFKA_NODES[2]}:${KAFKA_BROKER_PORT}"
    cat > "$SQL_SCRIPT_LOCAL" <<SQLEOF
CREATE TABLE kafka_it_source_dr (
  mensaje STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = '${TEST_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP_LIST}',
  'properties.group.id'          = 'bluepoint-kafka-flink-it-dr',
  'scan.startup.mode'            = 'latest-offset',
  'format'                       = 'raw'
);

CREATE TABLE print_sink_dr (
  mensaje STRING
) WITH (
  'connector' = 'print'
);

INSERT INTO print_sink_dr SELECT * FROM kafka_it_source_dr;
SQLEOF

    if podman cp "$SQL_SCRIPT_LOCAL" "${CONTAINER_JM}:${SQL_SCRIPT_CONTAINER}" 2>/dev/null; then
      ok "Script SQL copiado al contenedor JobManager DR"

      # 3.3 Someter el job en background
      info "3.3 Sometiendo job Flink SQL DR (background)..."
      podman exec -d "$CONTAINER_JM" "${FLINK_HOME}/bin/sql-client.sh" -f "$SQL_SCRIPT_CONTAINER" \
        > /tmp/${TEST_TOPIC}_sqlclient.log 2>&1

      info "Esperando ${SQL_JOB_STARTUP_WAIT_S}s a que el job quede RUNNING (DR: recursos más limitados)..."
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
        fail "No se detectó ningún job RUNNING tras ${SQL_JOB_STARTUP_WAIT_S}s — ver /tmp/${TEST_TOPIC}_sqlclient.log en $THIS_HOST — CRÍTICO en DR"
      fi

      # 3.4 Producir mensajes de prueba
      info "3.4 Produciendo $MESSAGE_COUNT mensajes de prueba en $TEST_TOPIC..."
      PRODUCE_START_EPOCH=$(date +%s)
      for i in $(seq 1 "$MESSAGE_COUNT"); do
        echo "${TEST_MESSAGE_PREFIX}-${i}-$(date +%s%N)" | \
          ssh -o ConnectTimeout=5 -o BatchMode=yes "${PODMAN_USER}@${KAFKA_NODE_FOR_TEST}" \
          "podman exec -i kafka ${KAFKA_HOME}/bin/kafka-console-producer.sh \
             --bootstrap-server localhost:${KAFKA_BROKER_PORT} --topic ${TEST_TOPIC}" &>/dev/null || \
          warn "Fallo produciendo el mensaje de prueba #$i"
      done
      ok "Mensajes de prueba producidos (o intentados): $MESSAGE_COUNT"

      # 3.5 Verificar llegada de mensajes al sink
      info "3.5 Verificando llegada de mensajes al sink 'print' (timeout ${LATENCY_TIMEOUT_S}s)..."
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
      PRODUCE_END_EPOCH=$(date +%s)
      ELAPSED=$((PRODUCE_END_EPOCH - PRODUCE_START_EPOCH))

      if $MESSAGE_SEEN; then
        ok "Mensajes de prueba detectados en el sink DR (TaskManager: $TM_CONTAINER) — latencia aproximada: ${ELAPSED}s"
        dr "RTO de integración observado: ${ELAPSED}s (referencia: <2min desde failover declarado, según diseño DR)"
      else
        fail "Los mensajes de prueba NO aparecieron en el sink DR dentro de ${LATENCY_TIMEOUT_S}s — integración Kafka↔Flink NO confirmada en contingencia"
      fi

      # 3.6 Limpieza
      info "3.6 Limpieza — cancelando job y borrando topic de prueba..."
      if [[ -n "$JOB_ID" ]]; then
        curl -sf --max-time 10 -X PATCH "$JM_API/jobs/${JOB_ID}" &>/dev/null && \
          ok "Job $JOB_ID cancelado vía REST API" || \
          warn "No se pudo cancelar el job $JOB_ID vía REST API — cancelar manualmente"
      fi

      ssh -o ConnectTimeout=5 -o BatchMode=yes "${PODMAN_USER}@${KAFKA_NODE_FOR_TEST}" \
        "podman exec kafka ${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server localhost:${KAFKA_BROKER_PORT} \
           --delete --topic ${TEST_TOPIC}" &>/dev/null && \
        ok "Topic de prueba '$TEST_TOPIC' eliminado" || \
        warn "No se pudo confirmar el borrado del topic de prueba — verificar manualmente en $KAFKA_NODE_FOR_TEST"

      rm -f "$SQL_SCRIPT_LOCAL"
      podman exec "$CONTAINER_JM" rm -f "$SQL_SCRIPT_CONTAINER" 2>/dev/null || true
    else
      fail "No se pudo copiar el script SQL al contenedor JobManager DR ($CONTAINER_JM) — ejecutar este script en $JM_HOST"
    fi
  fi
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Integración Kafka ↔ Flink · DR (Datacenter Alterno)  ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Integración Kafka↔Flink DR validada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  $FAIL fallos críticos — integración Kafka↔Flink NO APTA para contingencia${NC}"
fi

log ""
log "${CYAN}  Nota de alcance: la salud individual de cada cluster DR (Kafka, Flink) NO se${NC}"
log "${CYAN}  revalida aquí — usar validate_kafka_dr.sh y validate_flink_dr.sh.${NC}"
log ""

exit $FAIL
