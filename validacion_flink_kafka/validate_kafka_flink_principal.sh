#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Kafka ↔ Flink (DC Principal)
#
#  Complementa a validacion_kafka/validate_kafka_principal.sh y
#  validacion_flink/validate_flink_principal.sh, que validan cada cluster
#  por separado y declaran explícitamente que la integración entre ambos
#  queda fuera de su alcance. Este script cubre ese espacio: automatiza lo
#  que en validacion_flink/validacion_funcional_flink_tarea24.md era un
#  procedimiento manual (Pruebas 2, 2b y 3), usando los hostnames finales
#  de hostnames.txt.
#
#  Topología (DC Principal):
#    JobManager   : pbigd-plat-apps01   (contenedor: flink-jobmanager)
#    TaskManagers : pbigd-proc01 | pbigd-proc02 | pbigd-proc03
#    Brokers Kafka: pbigd-kaf01 | pbigd-kaf02 | pbigd-kaf03 (contenedor: kafka)
#
#  Versiones validadas: Kafka 4.0.1 (KRaft) · Flink 2.2.1 (mismas que fijan
#  validate_kafka_principal.sh / validate_flink_principal.sh — no se
#  redefinen aquí, se asumen ya confirmadas por esos scripts).
#
#  Requisitos:
#    - Ejecutar desde el nodo JobManager (pbigd-plat-apps01) o pasar
#      --jobmanager-host / --kafka-node si se ejecuta desde otro nodo.
#    - SSH sin contraseña (clave) del usuario admapl hacia el nodo Kafka
#      usado para crear/producir/borrar el topic de prueba.
#    - python3 disponible (parseo de JSON de la REST API de Flink).
#
#  Uso:
#    chmod +x validate_kafka_flink_principal.sh
#    ./validate_kafka_flink_principal.sh [--jobmanager-host host] \
#        [--kafka-node host] [--skip-functional-test]
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
KAFKA_NODE_FOR_TEST="pbigd-kaf01"   # nodo usado para crear/producir/borrar el topic de prueba

CONTAINER_JM="flink-jobmanager"
CONTAINER_TM_PATTERN="flink-tm"
KAFKA_HOME="/opt/kafka"
FLINK_HOME="/opt/flink"
PODMAN_USER="admapl"

TEST_TOPIC="bluepoint-kafka-flink-it-$(date +%s)"
TEST_MESSAGE_PREFIX="bluepoint-it"
MESSAGE_COUNT=3
LATENCY_TIMEOUT_S=20            # ventana máxima para ver el mensaje reflejado en el sink
SQL_JOB_STARTUP_WAIT_S=15        # tiempo de gracia para que el job SQL quede RUNNING

SKIP_FUNCTIONAL_TEST=false

# ---------------------------------------------------------------------------
# Colores y helpers (mismo estilo que validate_kafka_*.sh / validate_flink_*.sh)
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_kafka_flink_principal_$(date +%Y%m%d_%H%M%S).log"

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
    --jobmanager-host)      JM_HOST="$2"; shift 2 ;;
    --kafka-node)           KAFKA_NODE_FOR_TEST="$2"; shift 2 ;;
    --skip-functional-test) SKIP_FUNCTIONAL_TEST=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
JM_API="http://${JM_HOST}:${JM_REST_PORT}"
ALL_FLINK_NODES=("$JM_HOST" "${TASKMANAGER_HOSTS[@]}")

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Integración Kafka ↔ Flink — PRINCIPAL   ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución: $THIS_HOST"
info "JobManager Flink : $JM_HOST ($JM_API)"
info "TaskManagers     : ${TASKMANAGER_HOSTS[*]}"
info "Brokers Kafka    : ${KAFKA_NODES[*]}"
info "Nodo usado para crear/producir/borrar el topic de prueba: $KAFKA_NODE_FOR_TEST"
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_kafka_principal.sh y${NC}"
log "${CYAN}  validate_flink_principal.sh ya se ejecutaron y ambos clusters están${NC}"
log "${CYAN}  operativos por separado. Aquí solo se valida la integración entre ambos.${NC}"
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
    fail "No se encontró flink-sql-connector-kafka-*.jar en ${FLINK_HOME}/lib/ — el módulo 3 (integración funcional) no podrá ejecutarse"
  fi
else
  warn "Contenedor JobManager ($CONTAINER_JM) no detectado localmente en $THIS_HOST — verificando vía REST API en su lugar"
  if curl -sf --max-time 10 "$JM_API/jars" &>/dev/null; then
    info "REST API accesible; no se puede inspeccionar /opt/flink/lib remotamente sin acceso podman al nodo $JM_HOST"
    warn "Ejecutar este script en $JM_HOST para validar el conector Kafka localmente"
  else
    fail "No se pudo verificar el conector Kafka (ni contenedor local ni REST API accesible)"
  fi
fi

# =============================================================================
# MÓDULO 2 — Conectividad Flink → Kafka (automatiza Prueba 2 de tarea24.md)
# =============================================================================
section "MÓDULO 2 · Conectividad Flink → Kafka (puerto ${KAFKA_BROKER_PORT})"

info "Verificando alcance a los ${#KAFKA_NODES[@]} brokers Kafka desde $THIS_HOST..."
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
  ok "Los ${#KAFKA_NODES[@]} brokers Kafka son alcanzables desde este nodo Flink"
else
  fail "Solo $KAFKA_REACHABLE_COUNT/${#KAFKA_NODES[@]} brokers Kafka alcanzables — la integración puede degradarse ante el broker no accesible"
fi

info "Nota: para cobertura completa, ejecutar este módulo también desde cada TaskManager (${TASKMANAGER_HOSTS[*]}), no solo desde el JobManager."

# =============================================================================
# MÓDULO 3 — Integración funcional end-to-end (automatiza Prueba 3 de tarea24.md)
# =============================================================================
section "MÓDULO 3 · Integración funcional Kafka → Flink (end-to-end)"

if $SKIP_FUNCTIONAL_TEST; then
  info "Módulo 3 omitido (--skip-functional-test)"
elif ! $CONNECTOR_OK; then
  warn "Módulo 3 omitido: conector Kafka no confirmado en el contenedor JobManager (ver Módulo 1)"
elif [[ "$KAFKA_REACHABLE_COUNT" -eq 0 ]]; then
  warn "Módulo 3 omitido: ningún broker Kafka alcanzable (ver Módulo 2)"
else
  SQL_SCRIPT_LOCAL="/tmp/${TEST_TOPIC}.sql"
  SQL_SCRIPT_CONTAINER="/tmp/${TEST_TOPIC}.sql"

  # 3.1 Crear topic de prueba en Kafka
  info "3.1 Creando topic de prueba '$TEST_TOPIC' en $KAFKA_NODE_FOR_TEST..."
  CREATE_OUT=$(ssh -o ConnectTimeout=5 -o BatchMode=yes "${PODMAN_USER}@${KAFKA_NODE_FOR_TEST}" \
    "podman exec kafka ${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server localhost:${KAFKA_BROKER_PORT} \
       --create --topic ${TEST_TOPIC} --partitions 3 --replication-factor 3" 2>&1 || echo "ERROR")

  if echo "$CREATE_OUT" | grep -qi "created topic\|already exists"; then
    ok "Topic de prueba creado: $TEST_TOPIC"
    TOPIC_CREATED=true
  else
    fail "No se pudo crear el topic de prueba en $KAFKA_NODE_FOR_TEST — salida: $CREATE_OUT"
    TOPIC_CREATED=false
  fi

  if $TOPIC_CREATED; then
    # 3.2 Generar script SQL: tabla fuente Kafka + sink 'print' (no requiere sesión interactiva)
    info "3.2 Generando job Flink SQL (fuente Kafka → sink print)..."
    cat > "$SQL_SCRIPT_LOCAL" <<SQLEOF
CREATE TABLE kafka_it_source (
  mensaje STRING
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = '${TEST_TOPIC}',
  'properties.bootstrap.servers' = '${KAFKA_NODES[0]}:${KAFKA_BROKER_PORT},${KAFKA_NODES[1]}:${KAFKA_BROKER_PORT},${KAFKA_NODES[2]}:${KAFKA_BROKER_PORT}',
  'properties.group.id'          = 'bluepoint-kafka-flink-it',
  'scan.startup.mode'            = 'latest-offset',
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

      # 3.3 Someter el job en background (INSERT INTO es streaming continuo — no bloquea con print sink)
      info "3.3 Sometiendo job Flink SQL (background)..."
      podman exec -d "$CONTAINER_JM" "${FLINK_HOME}/bin/sql-client.sh" -f "$SQL_SCRIPT_CONTAINER" \
        > /tmp/${TEST_TOPIC}_sqlclient.log 2>&1

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
        fail "No se detectó ningún job RUNNING tras ${SQL_JOB_STARTUP_WAIT_S}s — ver /tmp/${TEST_TOPIC}_sqlclient.log en $THIS_HOST"
      fi

      # 3.4 Producir mensajes de prueba y medir aparición en el sink (log del TaskManager)
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

      # 3.5 Verificar, con timeout, que los mensajes llegan al sink (log del TaskManager con el operador print)
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
        ok "Mensajes de prueba detectados en el sink (TaskManager: $TM_CONTAINER) — latencia aproximada: ${ELAPSED}s"
      else
        fail "Los mensajes de prueba NO aparecieron en el sink dentro de ${LATENCY_TIMEOUT_S}s — integración Kafka→Flink no confirmada"
      fi

      # 3.6 Limpieza: cancelar job y borrar topic
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
      fail "No se pudo copiar el script SQL al contenedor JobManager ($CONTAINER_JM) — ejecutar este script en $JM_HOST"
    fi
  fi
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Integración Kafka ↔ Flink · PRINCIPAL                ║${NC}"
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
log "${CYAN}  Nota de alcance: la salud individual de cada cluster (Kafka, Flink) NO se${NC}"
log "${CYAN}  revalida aquí — usar validate_kafka_principal.sh y validate_flink_principal.sh.${NC}"
log ""

exit $FAIL
