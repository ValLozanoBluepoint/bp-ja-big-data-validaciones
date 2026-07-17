#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Kafka ↔ Flink (DC Principal) — LADO KAFKA
#
#  Reemplaza la parte SSH de validate_kafka_flink_principal.sh (deprecado):
#  no existe acceso SSH sin contraseña del usuario admapl desde los nodos
#  Flink hacia los nodos Kafka, así que este script se ejecuta MANUALMENTE,
#  de forma local, en un nodo Kafka (ej. pbigd-kaf01).
#
#  Flujo (ver preparacion_integracion_kafka_flink.md):
#    1. Ejecutar este script en un nodo Kafka (modo por defecto): crea un
#       topic de prueba, produce mensajes, e imprime el nombre del topic.
#    2. Copiar el nombre del topic impreso y pasarlo con --topic a
#       validate_kafka_flink_principal_flink.sh, ejecutado en el JobManager.
#    3. Una vez confirmado el resultado en el paso 2, volver a este nodo
#       Kafka y ejecutar este script con --cleanup <topic> para borrarlo.
#
#  Uso:
#    chmod +x validate_kafka_flink_principal_kafka.sh
#    ./validate_kafka_flink_principal_kafka.sh                # crea + produce
#    ./validate_kafka_flink_principal_kafka.sh --cleanup <topic>  # borra
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
KAFKA_BROKER_PORT=9092
CONTAINER_KAFKA="kafka"
KAFKA_HOME="/opt/kafka"

TEST_TOPIC="bluepoint-kafka-flink-it-$(date +%s)"
TEST_MESSAGE_PREFIX="bluepoint-it"
MESSAGE_COUNT=3

CLEANUP_TOPIC=""

# ---------------------------------------------------------------------------
# Colores y helpers (mismo estilo que el resto de scripts de validación)
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_kafka_flink_principal_kafka_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cleanup) CLEANUP_TOPIC="$2"; shift 2 ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

THIS_HOST=$(hostname -s 2>/dev/null || hostname)

log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Integración Kafka↔Flink — PRINCIPAL · LADO KAFKA   ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución: $THIS_HOST"
log ""

# =============================================================================
# MODO CLEANUP
# =============================================================================
if [[ -n "$CLEANUP_TOPIC" ]]; then
  section "CLEANUP · Borrando topic de prueba '$CLEANUP_TOPIC'"

  DELETE_OUT=$(podman exec "$CONTAINER_KAFKA" "${KAFKA_HOME}/bin/kafka-topics.sh" \
    --bootstrap-server "localhost:${KAFKA_BROKER_PORT}" \
    --delete --topic "$CLEANUP_TOPIC" 2>&1 || echo "ERROR")

  if echo "$DELETE_OUT" | grep -qiv "error"; then
    ok "Topic de prueba '$CLEANUP_TOPIC' eliminado"
  else
    fail "No se pudo borrar el topic '$CLEANUP_TOPIC' — salida: $DELETE_OUT"
  fi

  log ""
  log "${BOLD}  Cleanup finalizado. PASS=$PASS FAIL=$FAIL${NC}"
  log ""
  exit $FAIL
fi

# =============================================================================
# MÓDULO 1 — Verificación local del contenedor Kafka
# =============================================================================
section "MÓDULO 1 · Contenedor Kafka local"

KAFKA_STATUS=$(podman ps --filter "name=${CONTAINER_KAFKA}" --format "{{.Status}}" 2>/dev/null || echo "")
if echo "$KAFKA_STATUS" | grep -qi "up"; then
  ok "Contenedor Kafka corriendo en $THIS_HOST: $KAFKA_STATUS"
else
  fail "Contenedor Kafka ('$CONTAINER_KAFKA') no está corriendo en $THIS_HOST — abortando"
  log ""
  exit 1
fi

# =============================================================================
# MÓDULO 2 — Creación de topic de prueba
# =============================================================================
section "MÓDULO 2 · Creación de topic de prueba"

info "Creando topic de prueba '$TEST_TOPIC'..."
CREATE_OUT=$(podman exec "$CONTAINER_KAFKA" "${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server "localhost:${KAFKA_BROKER_PORT}" \
  --create --topic "$TEST_TOPIC" --partitions 3 --replication-factor 3 2>&1 || echo "ERROR")

TOPIC_CREATED=false
if echo "$CREATE_OUT" | grep -qi "created topic\|already exists"; then
  ok "Topic de prueba creado: $TEST_TOPIC"
  TOPIC_CREATED=true
else
  fail "No se pudo crear el topic de prueba — salida: $CREATE_OUT"
fi

# =============================================================================
# MÓDULO 3 — Producción de mensajes de prueba
# =============================================================================
if $TOPIC_CREATED; then
  section "MÓDULO 3 · Producción de mensajes de prueba"

  info "Produciendo $MESSAGE_COUNT mensajes de prueba en '$TEST_TOPIC'..."
  {
    for i in $(seq 1 "$MESSAGE_COUNT"); do
      echo "${TEST_MESSAGE_PREFIX}-${i}-$(date +%s%N)"
    done
  } | podman exec -i "$CONTAINER_KAFKA" "${KAFKA_HOME}/bin/kafka-console-producer.sh" \
        --bootstrap-server "localhost:${KAFKA_BROKER_PORT}" --topic "$TEST_TOPIC" &>/dev/null

  if [[ $? -eq 0 ]]; then
    ok "Mensajes de prueba producidos: $MESSAGE_COUNT"
  else
    fail "Fallo al producir mensajes de prueba en '$TEST_TOPIC'"
  fi
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Lado Kafka · Integración Kafka↔Flink · PRINCIPAL     ║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Nodo evaluado : $(printf '%-51s' "$THIS_HOST")║${NC}"
log "${BOLD}║   Total checks  : $(printf '%-51s' "$TOTAL")║${NC}"
log "${GREEN}${BOLD}║   PASS          : $(printf '%-51s' "$PASS")║${NC}"
log "${RED}${BOLD}║   FAIL          : $(printf '%-51s' "$FAIL")║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

if [[ $FAIL -eq 0 ]] && $TOPIC_CREATED; then
  log "${GREEN}${BOLD}  ✔  Topic de prueba listo: ${TEST_TOPIC}${NC}"
  log "${CYAN}  Siguiente paso: ejecutar en el JobManager (pbigd-plat-apps01):${NC}"
  log "${CYAN}    ./validate_kafka_flink_principal_flink.sh --topic ${TEST_TOPIC}${NC}"
  log "${CYAN}  Al finalizar, volver aquí y ejecutar:${NC}"
  log "${CYAN}    ./validate_kafka_flink_principal_kafka.sh --cleanup ${TEST_TOPIC}${NC}"
else
  log "${RED}${BOLD}  ✘  No se pudo dejar listo el topic de prueba — revisar log: $LOG_FILE${NC}"
fi
log ""

exit $FAIL
