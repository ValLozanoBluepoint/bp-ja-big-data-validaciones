#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Kafka ↔ Flink (DR — Datacenter Alterno)
#  LADO KAFKA
#
#  Reemplaza la parte SSH de validate_kafka_flink_dr.sh (deprecado): no
#  existe acceso SSH sin contraseña del usuario admapl desde los nodos
#  Flink hacia los nodos Kafka. Este script se ejecuta MANUALMENTE, de
#  forma local, en un nodo Kafka DR (ej. pbigd-kaf01-cont).
#
#  Flujo (ver preparacion_integracion_kafka_flink.md):
#    1. Ejecutar este script en un nodo Kafka DR (modo por defecto): crea
#       un topic de prueba, produce mensajes, e imprime el nombre del topic.
#    2. Copiar el nombre del topic y pasarlo con --topic a
#       validate_kafka_flink_dr_flink.sh, ejecutado en el JobManager DR.
#    3. Una vez confirmado el resultado en el paso 2, volver a este nodo
#       Kafka DR y ejecutar este script con --cleanup <topic> para borrarlo.
#
#  Kafka DR mantiene capacidad completa (3/3), igual que Principal — es el
#  punto de entrada crítico del flujo de datos y no se reduce en contingencia.
#
#  Uso:
#    chmod +x validate_kafka_flink_dr_kafka.sh
#    sh ./validate_kafka_flink_dr_kafka.sh                # crea + produce
#    sh ./validate_kafka_flink_dr_kafka.sh --cleanup <topic>  # borra
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN DR
# ---------------------------------------------------------------------------
KAFKA_BROKER_PORT=9092
CONTAINER_KAFKA="kafka-broker-cont-01"
KAFKA_HOME="/opt/kafka"

TEST_TOPIC="bluepoint-kafka-flink-it-dr-$(date +%s)"
TEST_MESSAGE_PREFIX="bluepoint-it-dr"
MESSAGE_COUNT=3

CLEANUP_TOPIC=""

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; MAGENTA='\033[0;35m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_kafka_flink_dr_kafka_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
dr()   { log "${MAGENTA}  [DR]${NC}  $*"; }
section() { log "\n${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${MAGENTA}  $*${NC}"; \
            log "${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

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
log "${BOLD}║   BLUEPOINT · Integración Kafka↔Flink — DR · LADO KAFKA          ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
dr "Kafka DR mantiene capacidad completa (3/3) — no se reduce en contingencia."
info "Nodo de ejecución: $THIS_HOST"
log ""

# =============================================================================
# MODO CLEANUP
# =============================================================================
if [[ -n "$CLEANUP_TOPIC" ]]; then
  section "CLEANUP · Borrando topic de prueba DR '$CLEANUP_TOPIC'"

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
# MÓDULO 1 — Verificación local del contenedor Kafka DR
# =============================================================================
section "MÓDULO 1 · Contenedor Kafka DR local"

KAFKA_STATUS=$(podman ps --filter "name=${CONTAINER_KAFKA}" --format "{{.Status}}" 2>/dev/null || echo "")
if echo "$KAFKA_STATUS" | grep -qi "up"; then
  ok "Contenedor Kafka DR corriendo en $THIS_HOST: $KAFKA_STATUS"
else
  fail "Contenedor Kafka ('$CONTAINER_KAFKA') no está corriendo en $THIS_HOST — abortando"
  log ""
  exit 1
fi

# =============================================================================
# MÓDULO 2 — Creación de topic de prueba
# =============================================================================
section "MÓDULO 2 · Creación de topic de prueba DR"

info "Creando topic de prueba '$TEST_TOPIC'..."
CREATE_OUT=$(podman exec "$CONTAINER_KAFKA" "${KAFKA_HOME}/bin/kafka-topics.sh" \
  --bootstrap-server "localhost:${KAFKA_BROKER_PORT}" \
  --create --topic "$TEST_TOPIC" --partitions 3 --replication-factor 3 2>&1 || echo "ERROR")

TOPIC_CREATED=false
if echo "$CREATE_OUT" | grep -qi "created topic\|already exists"; then
  ok "Topic de prueba creado en DR: $TEST_TOPIC"
  TOPIC_CREATED=true
else
  fail "No se pudo crear el topic de prueba en DR — salida: $CREATE_OUT"
fi

# =============================================================================
# MÓDULO 3 — Producción de mensajes de prueba
# =============================================================================
if $TOPIC_CREATED; then
  section "MÓDULO 3 · Producción de mensajes de prueba DR"

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
log "${BOLD}║   RESUMEN – Lado Kafka · Integración Kafka↔Flink · DR             ║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Nodo evaluado : $(printf '%-51s' "$THIS_HOST")║${NC}"
log "${BOLD}║   Total checks  : $(printf '%-51s' "$TOTAL")║${NC}"
log "${GREEN}${BOLD}║   PASS          : $(printf '%-51s' "$PASS")║${NC}"
log "${RED}${BOLD}║   FAIL          : $(printf '%-51s' "$FAIL")║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

if [[ $FAIL -eq 0 ]] && $TOPIC_CREATED; then
  log "${GREEN}${BOLD}  ✔  Topic de prueba DR listo: ${TEST_TOPIC}${NC}"
  log "${CYAN}  Siguiente paso: ejecutar en el JobManager DR (pbigd-plat-apps01-cont):${NC}"
  log "${CYAN}    sh ./validate_kafka_flink_dr_flink.sh --topic ${TEST_TOPIC}${NC}"
  log "${CYAN}  Al finalizar, volver aquí y ejecutar:${NC}"
  log "${CYAN}    sh ./validate_kafka_flink_dr_kafka.sh --cleanup ${TEST_TOPIC}${NC}"
else
  log "${RED}${BOLD}  ✘  No se pudo dejar listo el topic de prueba DR — revisar log: $LOG_FILE${NC}"
fi
log ""

exit $FAIL
