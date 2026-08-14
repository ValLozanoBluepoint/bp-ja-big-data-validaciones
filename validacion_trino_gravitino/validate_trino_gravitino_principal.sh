#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Trino ↔ Gravitino (DC Principal)
#  Metadata/catálogo Iceberg compartido (NO archivos de datos)
#
#  Alcance: la conexión Trino → Gravitino (metadata/catálogo REST de Iceberg)
#  y, como prueba funcional cruzada, el mismo catálogo visto desde Flink. NO
#  valida la conexión directa Trino → AIStor para archivos de datos (eso es
#  validacion_trino_minio/) ni la salud de Trino o Gravitino como tecnologías
#  en sí mismas (eso es validacion_trino/ y validacion_gravitino/, que deben
#  correrse antes).
#
#  Por qué es un script separado de validacion_trino_minio/: un correo interno
#  del equipo de arquitectura (context/Contexto-extra-correos.md, 2026-08-06)
#  confirma que "Gravitino solo maneja metadata (catálogo de tablas), nunca
#  los archivos de datos -por eso Flink y Trino tienen una conexión hacia
#  Gravitino (metadata) y otra directa hacia AIStor (archivos), sin pasar por
#  Gravitino." Trino tiene DOS conexiones independientes; este script valida
#  la primera (metadata), no la segunda (datos, ver validacion_trino_minio/).
#
#  No existe un script hermano exacto: la integración Flink↔Gravitino tampoco
#  tiene validación propia todavía (hueco de cobertura detectado en una
#  revisión anterior). El Módulo 2 de este script es, hasta donde se pudo
#  confirmar en el repo, la primera prueba funcional real del catálogo REST
#  compartido — validacion_flink_minio/ dejó Iceberg/Gravitino explícitamente
#  fuera de alcance (su Módulo 3 usa un job datagen→print efímero sin
#  catálogo), así que no hay ninguna tabla de prueba ya creada por Flink que
#  se pueda reutilizar aquí.
#
#  Topología (DC Principal — Trino Coordinator, Flink JobManager y Gravitino
#  comparten el mismo nodo platform-apps-1, ver validate_trino_principal.sh /
#  validate_gravitino_principal.sh / validate_flink_principal.sh):
#    Coordinator/JM/Gravitino : pbigd-plat-apps01
#      (contenedores: trino-coordinator, flink-jobmanager, patrón *gravitino*)
#
#  DR: NO se genera validate_trino_gravitino_dr.sh en esta entrega. Se hereda
#  la conclusión ya documentada, sin re-investigar, de los dos scripts base:
#    - validate_trino_principal.sh: Trino "No aplica en DR por diseño",
#      pendiente de confirmación en sitio (podman ps | grep -i trino en los
#      3 nodos -cont).
#    - validate_gravitino_principal.sh: Gravitino queda como "PREGUNTA
#      ABIERTA" sin resolver — ni siquiera se documenta como "no aplica".
#  Ninguno de los dos componentes tiene su despliegue en DR confirmado, así
#  que esta integración tampoco aplica ahí hasta que ambos se resuelvan. No
#  investigar esto de nuevo aquí.
#
#  Nombre real del catálogo/metalake Iceberg — NO documentado en ningún lugar
#  del repo (ni el plan de implementación, ni los correos, ni
#  validacion_gravitino/, ni validacion_trino_minio/). Mismo vacío documental
#  ya señalado para el catálogo S3 en validacion_trino_minio/. Este script lo
#  DESCUBRE en runtime (Módulo 1.1): no asume ninguno de antemano.
#
#  Requisitos previos (ver preparacion_validacion_trino_gravitino.md):
#    - validacion_trino/validate_trino_principal.sh y
#      validacion_gravitino/validate_gravitino_principal.sh ya corridos.
#    - Ejecutar desde pbigd-plat-apps01 (o pasar --coordinator-host /
#      --gravitino-host / --flink-jobmanager-host).
#    - python3 disponible (parseo de JSON de las REST API de Trino/Gravitino/Flink).
#
#  NO usa SSH: mismo criterio que el resto de la familia de scripts de
#  integración. Todos los chequeos automáticos asumen que el script corre en
#  el nodo donde viven los tres contenedores (colocados en platform-apps-1
#  según el plan de implementación); si alguno no está local, el chequeo que
#  dependa de él degrada a WARN con instrucción manual, no FAIL.
#
#  Fuera de alcance: conexión Trino → AIStor para datos (ver
#  validacion_trino_minio/), salud de Trino/Gravitino/Flink como tecnologías
#  en sí mismas (ver sus scripts respectivos), DR (ver arriba).
#
#  Uso:
#    chmod +x validate_trino_gravitino_principal.sh
#    ./validate_trino_gravitino_principal.sh \
#        [--coordinator-host host] [--gravitino-host host] \
#        [--flink-jobmanager-host host] [--skip-functional-test]
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
COORDINATOR_HOST="${COORDINATOR_HOST:-pbigd-plat-apps01}"
COORDINATOR_PORT=8080
TRINO_HOME="/opt/trino"
CONTAINER_COORDINATOR="trino"
TRINO_USER="admapl"   # usuario enviado en X-Trino-User, igual que validate_trino_minio_principal.sh

GRAVITINO_HOST="${GRAVITINO_HOST:-pbigd-plat-apps01}"
GRAVITINO_PORT=8090
# Gravitino expone DOS puertos propios en el mismo contenedor: 8090 (API
# general de metalakes/catálogos) y 9001 (Iceberg REST Catalog específico,
# /iceberg/v1/...). La URI que configuran Trino/Flink para el catálogo
# Iceberg apunta a este segundo puerto, no al 8090 — confirmado en sitio
# (curl http://localhost:9001/iceberg/v1/config responde el config real).
GRAVITINO_ICEBERG_PORT=9001
GRAVITINO_CONTAINER_PATTERN="gravitino"

JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01}"
JM_REST_PORT=8081
CONTAINER_JM="flink-jobmanager"
FLINK_HOME="/opt/flink"

# Config del iceberg-rest-server de Gravitino — rutas candidatas dentro del
# contenedor, para el Módulo 1.5 (warehouse real).
GRAVITINO_CONF_CANDIDATES=("/root/gravitino/conf/gravitino-iceberg-rest-server.conf" "/root/gravitino/conf/gravitino.conf")

# Rutas candidatas donde puede vivir la config de catálogos de Trino — mismo
# criterio de descubrimiento que validate_trino_minio_principal.sh.
CATALOG_DIR_CANDIDATES=("${TRINO_HOME}/etc/catalog" "/etc/trino/catalog")

# Nombres de propiedad candidatos para la URI del catálogo REST — ninguno
# confirmado en el repo, se prueban todos y se reporta cuál existe.
REST_URI_KEY_CANDIDATES=("iceberg.rest-catalog.uri" "iceberg.rest.uri" "iceberg.catalog.uri")

TEST_SCHEMA="bluepoint_validacion_tg"
TEST_TABLE_FLINK="tabla_flink"
TEST_TABLE_TRINO="tabla_trino"
SQL_JOB_STARTUP_WAIT_S=15         # tiempo máximo esperando a que el sql-client reporte el Job ID
JOB_POLL_INTERVAL_S=5             # intervalo de sondeo del estado real del job vía REST de Flink
JOB_POLL_MAX_WAIT_S=90            # tiempo máximo esperando a que el job llegue a estado terminal

STATEMENT_MAX_WAIT_S=30
STATEMENT_POLL_INTERVAL_S=2

SKIP_FUNCTIONAL_TEST=false

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_trino_gravitino_principal_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# Ejecuta una sentencia SQL contra Trino vía REST API (/v1/statement), igual
# helper que validate_trino_minio_principal.sh.
run_statement() {
  local sql="$1"
  local resp
  resp=$(curl -sf --max-time 15 \
    -X POST "$COORDINATOR_API/v1/statement" \
    -H "X-Trino-User: ${TRINO_USER}" \
    --data "$sql" 2>/dev/null || echo "")
  [[ -z "$resp" ]] && { echo ""; return 1; }

  local elapsed=0
  while [[ "$elapsed" -lt "$STATEMENT_MAX_WAIT_S" ]]; do
    local next_uri
    next_uri=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('nextUri',''))" 2>/dev/null || echo "")
    local err
    err=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','') if d.get('error') else '')" 2>/dev/null || echo "")
    if [[ -n "$err" ]]; then
      echo "$resp"
      return 1
    fi
    if [[ -z "$next_uri" ]]; then
      echo "$resp"
      return 0
    fi
    resp=$(curl -sf --max-time 15 "$next_uri" -H "X-Trino-User: ${TRINO_USER}" 2>/dev/null || echo "")
    [[ -z "$resp" ]] && { echo ""; return 1; }
    sleep "$STATEMENT_POLL_INTERVAL_S"
    elapsed=$((elapsed + STATEMENT_POLL_INTERVAL_S))
  done
  echo "$resp"
  return 1
}

statement_error() {
  echo "$1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','') if d.get('error') else '')" 2>/dev/null || echo ""
}

# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --coordinator-host)        COORDINATOR_HOST="$2"; shift 2 ;;
    --gravitino-host)          GRAVITINO_HOST="$2"; shift 2 ;;
    --flink-jobmanager-host)   JM_HOST="$2"; shift 2 ;;
    --skip-functional-test)    SKIP_FUNCTIONAL_TEST=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
COORDINATOR_API="http://${COORDINATOR_HOST}:${COORDINATOR_PORT}"
GRAVITINO_API="http://${GRAVITINO_HOST}:${GRAVITINO_PORT}"
JM_API="http://${JM_HOST}:${JM_REST_PORT}"

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Integración Trino ↔ Gravitino — PRINCIPAL          ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución    : $THIS_HOST"
info "Coordinator Trino    : $COORDINATOR_HOST ($COORDINATOR_API)"
info "Gravitino            : $GRAVITINO_HOST ($GRAVITINO_API)"
info "JobManager Flink     : $JM_HOST ($JM_API)"
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_trino_principal.sh y${NC}"
log "${CYAN}  validate_gravitino_principal.sh ya se ejecutaron. Aquí solo se valida${NC}"
log "${CYAN}  la conexión de metadata Trino → Gravitino y, como prueba cruzada, el${NC}"
log "${CYAN}  mismo catálogo visto desde Flink — NO la conexión Trino → AIStor para${NC}"
log "${CYAN}  archivos de datos (ver validacion_trino_minio/).${NC}"
log ""

# =============================================================================
# MÓDULO 1 — Conectividad base Trino → Gravitino
# =============================================================================
section "MÓDULO 1 · Conectividad base Trino → Gravitino"

# 1.0 Detectar contenedor Coordinator local (la config del catálogo vive ahí)
CRD_CONTAINER_STATUS=$(podman ps --filter "name=${CONTAINER_COORDINATOR}" --format "{{.Status}}" 2>/dev/null || echo "")
CRD_LOCAL=false
if echo "$CRD_CONTAINER_STATUS" | grep -qi "up"; then
  CRD_LOCAL=true
  ok "Contenedor Coordinator local corriendo: $CRD_CONTAINER_STATUS"
else
  warn "Contenedor Coordinator ($CONTAINER_COORDINATOR) no detectado localmente en $THIS_HOST — ejecutar este script en $COORDINATOR_HOST para validar la config del catálogo"
fi

CATALOG_PROPS_FILE=""
CATALOG_NAME=""
REST_URI_KEY=""
TRINO_REST_URI=""

if $CRD_LOCAL; then
  # 1.1 Descubrimiento de catálogo Iceberg — mismo mecanismo que
  # validate_trino_minio_principal.sh (no asumir nombre ni ruta).
  for CAND_DIR in "${CATALOG_DIR_CANDIDATES[@]}"; do
    FOUND=$(podman exec "$CONTAINER_COORDINATOR" sh -c "find '$CAND_DIR' -maxdepth 1 -iname '*.properties' 2>/dev/null" || echo "")
    if [[ -n "$FOUND" ]]; then
      while IFS= read -r PROP_FILE; do
        CONNECTOR=$(podman exec "$CONTAINER_COORDINATOR" grep -oP '^\s*connector\.name\s*=\s*\K\S+' "$PROP_FILE" 2>/dev/null || echo "")
        if [[ "$CONNECTOR" == "iceberg" ]]; then
          CATALOG_PROPS_FILE="$PROP_FILE"
          CATALOG_NAME=$(basename "$PROP_FILE" .properties)
          break
        fi
      done <<< "$FOUND"
      [[ -n "$CATALOG_PROPS_FILE" ]] && break
    fi
  done

  if [[ -n "$CATALOG_PROPS_FILE" ]]; then
    ok "Catálogo Iceberg detectado: '$CATALOG_NAME' ($CATALOG_PROPS_FILE, connector.name=iceberg)"

    CATALOG_TYPE=$(podman exec "$CONTAINER_COORDINATOR" grep -oP '^\s*iceberg\.catalog\.type\s*=\s*\K\S+' "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
    if [[ "$CATALOG_TYPE" == "rest" ]]; then
      ok "iceberg.catalog.type=rest confirmado en '$CATALOG_NAME' — el catálogo apunta a un servicio REST (Gravitino), no a Hive Metastore/Glue/JDBC"
    elif [[ -n "$CATALOG_TYPE" ]]; then
      fail "iceberg.catalog.type='$CATALOG_TYPE' en '$CATALOG_NAME' — se esperaba 'rest' para integrar con Gravitino; el catálogo de Trino NO está configurado para usar Gravitino"
    else
      fail "No se encontró iceberg.catalog.type en $(basename "$CATALOG_PROPS_FILE") — no se puede confirmar que Trino use el catálogo REST de Gravitino"
    fi

    # 1.1 (cont.) URI del catálogo REST — nombre de propiedad no confirmado en
    # el repo, se prueban los candidatos conocidos.
    for KEY in "${REST_URI_KEY_CANDIDATES[@]}"; do
      VAL=$(podman exec "$CONTAINER_COORDINATOR" grep -oP "^\s*${KEY}\s*=\s*\K\S+" "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
      if [[ -n "$VAL" ]]; then
        REST_URI_KEY="$KEY"
        TRINO_REST_URI="$VAL"
        break
      fi
    done
    if [[ -n "$TRINO_REST_URI" ]]; then
      ok "URI del catálogo REST configurada en Trino ($REST_URI_KEY): $TRINO_REST_URI"
    else
      fail "No se encontró ninguna de las propiedades candidatas (${REST_URI_KEY_CANDIDATES[*]}) en $(basename "$CATALOG_PROPS_FILE") — no se puede confirmar a qué endpoint apunta Trino"
    fi
  else
    fail "No se encontró ningún catálogo con connector.name=iceberg en ${CATALOG_DIR_CANDIDATES[*]} — no se puede validar la integración con Gravitino sin saber qué archivo leer"
  fi
fi

# 1.2 Gravitino responde y la URI configurada en Trino coincide con el
# endpoint real (comparación de texto, con fallback a resolución DNS dentro
# del contenedor para tolerar nombre corto vs FQDN, mismo patrón que
# validate_trino_minio_principal.sh Módulo 1.2).
GRV_VER_JSON=$(curl -sf --max-time 10 "$GRAVITINO_API/api/version" 2>/dev/null || echo "")
if [[ -n "$GRV_VER_JSON" ]]; then
  ok "Gravitino REST API responde desde $THIS_HOST: $GRAVITINO_API/api/version"
else
  fail "Gravitino REST API no responde en $GRAVITINO_API — el Módulo 2 (funcional) no podrá completarse"
fi

# Puerto específico del Iceberg REST Catalog (distinto del API general de
# metalakes en $GRAVITINO_PORT) — ver nota de GRAVITINO_ICEBERG_PORT arriba.
GRAVITINO_ICEBERG_API="http://${GRAVITINO_HOST}:${GRAVITINO_ICEBERG_PORT}"
GRV_ICEBERG_CONFIG_JSON=$(curl -sf --max-time 10 "${GRAVITINO_ICEBERG_API}/iceberg/v1/config" 2>/dev/null || echo "")
if [[ -n "$GRV_ICEBERG_CONFIG_JSON" ]]; then
  ok "Iceberg REST Catalog de Gravitino responde desde $THIS_HOST: ${GRAVITINO_ICEBERG_API}/iceberg/v1/config"
else
  warn "Iceberg REST Catalog de Gravitino no respondió en ${GRAVITINO_ICEBERG_API}/iceberg/v1/config — si Trino usa otro puerto/ruta para el catálogo, confirmarlo manualmente (ver check funcional de la URI de Trino más abajo)"
fi

if [[ -n "$TRINO_REST_URI" ]]; then
  # Comparación por host/IP: informativa únicamente. En Podman rootless con
  # red pasta, es normal y correcto que el catálogo use el alias
  # "host.containers.internal" (resuelve a la IP de gateway del netns, p.ej.
  # 169.254.1.2) en vez del hostname real del nodo — ambos llegan al mismo
  # servicio, pero NUNCA van a coincidir por IP. Comparar por IP aquí
  # produce falsos FAIL; se deja solo como dato adicional, no como el check
  # que decide OK/FAIL.
  if echo "$TRINO_REST_URI" | grep -qF "$GRAVITINO_HOST"; then
    info "La URI de Trino ($TRINO_REST_URI) referencia textualmente el host de Gravitino ($GRAVITINO_HOST)"
  else
    TRINO_URI_HOST=$(echo "$TRINO_REST_URI" | sed -E 's#^[a-zA-Z]+://##; s#:[0-9]+.*$##; s#/.*$##')
    TRINO_URI_IP=$(podman exec "$CONTAINER_COORDINATOR" getent hosts "$TRINO_URI_HOST" 2>/dev/null | awk '{print $1}' | head -1)
    GRAVITINO_IP=$(podman exec "$CONTAINER_COORDINATOR" getent hosts "$GRAVITINO_HOST" 2>/dev/null | awk '{print $1}' | head -1)
    if [[ -n "$TRINO_URI_IP" && "$TRINO_URI_IP" == "$GRAVITINO_IP" ]]; then
      info "La URI de Trino ($TRINO_REST_URI) resuelve a la misma IP ($TRINO_URI_IP) que $GRAVITINO_HOST"
    else
      info "La URI de Trino ($TRINO_REST_URI${TRINO_URI_IP:+ → $TRINO_URI_IP}) no coincide textualmente ni por IP con $GRAVITINO_HOST${GRAVITINO_IP:+ → $GRAVITINO_IP} — normal si usa el alias 'host.containers.internal' de Podman rootless; el check decisivo es la respuesta funcional de abajo, no esto"
    fi
  fi

  # Check decisivo: ¿la URI exacta que Trino tiene configurada responde
  # realmente como Iceberg REST Catalog, consultada DESDE el propio
  # contenedor de Trino (mismo netns/resolución que usa Trino en runtime)?
  # Esto reemplaza la comparación de host/IP como criterio de OK/FAIL: es
  # una prueba funcional directa en vez de inferir por nombre.
  TRINO_REST_URI_CONFIG="${TRINO_REST_URI%/}/v1/config"
  TRINO_SIDE_CONFIG_JSON=$(podman exec "$CONTAINER_COORDINATOR" curl -sf --max-time 10 "$TRINO_REST_URI_CONFIG" 2>/dev/null || echo "")
  if [[ -n "$TRINO_SIDE_CONFIG_JSON" ]] && echo "$TRINO_SIDE_CONFIG_JSON" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    ok "La URI configurada en Trino ($TRINO_REST_URI) responde como Iceberg REST Catalog válido, consultada desde el propio contenedor de Trino"
  else
    fail "La URI configurada en Trino ($TRINO_REST_URI) NO respondió un config JSON válido en $TRINO_REST_URI_CONFIG consultada desde el contenedor de Trino — Trino no podría usar este catálogo en runtime"
  fi
fi

# 1.3 Si Gravitino tiene 2+ instancias (mismo hallazgo de HA "de papel" que
# validate_gravitino_principal.sh 2.3), confirmar que la URI de Trino usa el
# mecanismo de failover real y no un nodo individual. Y, por separado, solo
# presencia de credenciales/token de autenticación — nunca el valor.
mapfile -t GRAVITINO_CONTAINERS < <(podman ps --filter "name=${GRAVITINO_CONTAINER_PATTERN}" --format "{{.Names}}" 2>/dev/null || true)
N_GRAVITINO_INSTANCES=${#GRAVITINO_CONTAINERS[@]}
if [[ "$N_GRAVITINO_INSTANCES" -ge 2 ]]; then
  HA_EVIDENCE=""
  if podman ps --format "{{.Names}}" 2>/dev/null | grep -qiE "haproxy|keepalived"; then
    HA_EVIDENCE="contenedor HAProxy/Keepalived detectado en este nodo"
  fi
  if [[ -n "$HA_EVIDENCE" ]]; then
    ok "2+ instancias de Gravitino con evidencia de HA ($HA_EVIDENCE) — confirmar que la URI de Trino ($TRINO_REST_URI) apunta a ese mecanismo, no a una instancia individual"
  else
    warn "Gravitino tiene $N_GRAVITINO_INSTANCES instancias pero sin evidencia de HAProxy/Keepalived (mismo hallazgo ya reportado en validate_gravitino_principal.sh 2.3) — si Trino apunta directo a una instancia individual ($TRINO_REST_URI), pierde el catálogo entero si esa instancia cae, sin failover real"
  fi
else
  info "Gravitino detectado con $N_GRAVITINO_INSTANCES instancia(s) — sin escenario de HA que validar contra la URI de Trino"
fi

if [[ -n "$CATALOG_PROPS_FILE" ]]; then
  AUTH_KEYS=$(podman exec "$CONTAINER_COORDINATOR" grep -oP '^\s*iceberg\.rest-catalog\.(security|oauth2[.\w-]*|token)\s*=' "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
  if [[ -n "$AUTH_KEYS" ]]; then
    ok "Trino tiene configurado un mecanismo de autenticación hacia el catálogo REST (claves presentes; valores no inspeccionados)"
  else
    info "No se encontraron claves de autenticación explícitas (iceberg.rest-catalog.security/oauth2/token) en $(basename "$CATALOG_PROPS_FILE") — Gravitino podría estar sin autenticación delante; confirmar manualmente el mecanismo real, no asumir que falta"
  fi
fi

# 1.4 Consistencia entre componentes: comparar el catálogo/warehouse que usa
# Flink contra el de Trino. Sin SSH entre nodos, este check solo puede
# automatizarse si el JobManager corre localmente (colocado con Trino/Gravitino
# en platform-apps-1 según el plan); si no, degrada a WARN con instrucción manual.
JM_CONTAINER_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null || echo "")
JM_LOCAL=false
if echo "$JM_CONTAINER_STATUS" | grep -qi "up"; then
  JM_LOCAL=true
  ok "Contenedor JobManager local corriendo: $JM_CONTAINER_STATUS"
else
  warn "Contenedor JobManager ($CONTAINER_JM) no detectado localmente en $THIS_HOST — no se puede cruzar automáticamente la config de catálogo de Flink contra la de Trino"
fi

if $JM_LOCAL; then
  FLINK_GRV_LINE=$(podman exec "$CONTAINER_JM" sh -c "grep -riE 'gravitino|rest-catalog' ${FLINK_HOME}/conf/*.yaml 2>/dev/null" || echo "")
  if [[ -n "$FLINK_GRV_LINE" ]]; then
    ok "Referencia a Gravitino/rest-catalog encontrada en la config de Flink:"
    while IFS= read -r LN; do info "    $LN"; done <<< "$FLINK_GRV_LINE"
    if echo "$FLINK_GRV_LINE" | grep -qF "$GRAVITINO_HOST"; then
      ok "La config de Flink referencia el mismo host de Gravitino ($GRAVITINO_HOST) que Trino"
    else
      warn "La config de Flink no menciona textualmente '$GRAVITINO_HOST' — confirmar manualmente que Flink y Trino usan el MISMO catálogo/metalake y no dos configuraciones distintas que coincidentemente no fallan"
    fi
  else
    # No es una anomalía: por diseño, el catálogo de Flink en este proyecto
    # se define vía CREATE CATALOG en el SQL de sesión (ver Módulo 2), no en
    # conf/*.yaml. Es el comportamiento esperado, no un hallazgo a corregir.
    info "Sin referencia a Gravitino/rest-catalog en ${FLINK_HOME}/conf/*.yaml dentro del JobManager — esperado: el catálogo de Flink se define vía CREATE CATALOG en el SQL de sesión (Módulo 2), no en la config estática"
  fi
fi

# 1.5 [PRIORITARIO] Warehouse real del iceberg-rest-server de Gravitino — el
# check 1.1/1.2 arriba solo confirma que la URI REST responde, no a dónde
# escribe realmente los archivos de datos. Con catalog-backend=memory (no
# persistente) y/o warehouse apuntando a un path local (p. ej. /tmp) en vez de
# s3://, cualquier PASS del Módulo 2 (Flink/Trino escriben y leen de vuelta)
# es un falso positivo respecto a la integración con AIStor: el catálogo REST
# funciona perfectamente bien contra almacenamiento efímero del propio
# contenedor. Hallazgo de sitio 2026-08-14 — ver hallazgos_transversales.md
# (H1) en la raíz del repo. Este es el chequeo correcto para confirmarlo:
# validate_trino_minio_principal.sh, por su propio alcance declarado ("NO
# valida la conexión Trino → Gravitino"), no debe tocar el contenedor
# Gravitino — ese script solo confirma (Módulo 1.5 suyo) que Trino no tiene
# config S3 propia y redirige aquí.
GRV_WAREHOUSE=""
GRV_BACKEND=""
if [[ "$N_GRAVITINO_INSTANCES" -ge 1 ]]; then
  GRV_CONF_FOUND=""
  for CAND in "${GRAVITINO_CONF_CANDIDATES[@]}"; do
    if podman exec "${GRAVITINO_CONTAINERS[0]}" test -f "$CAND" 2>/dev/null; then
      GRV_CONF_FOUND="$CAND"
      GRV_WAREHOUSE=$(podman exec "${GRAVITINO_CONTAINERS[0]}" grep -oP '^\s*gravitino\.iceberg-rest\.warehouse\s*=\s*\K\S+' "$CAND" 2>/dev/null || echo "")
      GRV_BACKEND=$(podman exec "${GRAVITINO_CONTAINERS[0]}" grep -oP '^\s*gravitino\.iceberg-rest\.catalog-backend\s*=\s*\K\S+' "$CAND" 2>/dev/null || echo "")
      [[ -n "$GRV_WAREHOUSE" ]] && break
    fi
  done
  if [[ -z "$GRV_CONF_FOUND" ]]; then
    warn "No se encontró config del iceberg-rest-server de Gravitino en ninguna ruta candidata (${GRAVITINO_CONF_CANDIDATES[*]}) dentro de '${GRAVITINO_CONTAINERS[0]}' — confirmar manualmente el warehouse real antes de confiar en el resultado del Módulo 2"
  elif [[ -z "$GRV_WAREHOUSE" ]]; then
    warn "No se encontró gravitino.iceberg-rest.warehouse en $(basename "$GRV_CONF_FOUND") de '${GRAVITINO_CONTAINERS[0]}' — no se puede confirmar si el warehouse apunta a AIStor real antes de confiar en el resultado del Módulo 2"
  elif echo "$GRV_WAREHOUSE" | grep -qP '^s3a?://'; then
    ok "Warehouse de Gravitino apunta a S3 real: $GRV_WAREHOUSE (catalog-backend=${GRV_BACKEND:-desconocido}) — el Módulo 2 sí es evidencia válida de integración con AIStor"
    if [[ "$GRV_BACKEND" != "jdbc" && -n "$GRV_BACKEND" ]]; then
      warn "catalog-backend de Gravitino es '$GRV_BACKEND' (no 'jdbc') pese a tener warehouse S3 — con backend no persistente el catálogo no sobrevive un reinicio del contenedor, aunque los archivos de datos sí queden en S3; confirmar si esto es intencional"
    fi
  else
    fail "Warehouse de Gravitino NO apunta a S3/AIStor: '$GRV_WAREHOUSE' (catalog-backend='${GRV_BACKEND:-desconocido}') en $(basename "$GRV_CONF_FOUND") de '${GRAVITINO_CONTAINERS[0]}' — cualquier PASS del Módulo 2 de este script (y de validacion_flink_gravitino/) NO es evidencia de integración con AIStor: las tablas se escriben en almacenamiento local efímero del contenedor Gravitino. Esto también explica por qué validacion_trino_minio/ no encuentra ninguna tabla real (SHOW SCHEMAS FROM <catálogo> solo devuelve information_schema/system). Ver hallazgos_transversales.md (H1) en la raíz del repo."
  fi
else
  warn "No se detectó ningún contenedor Gravitino local — no se puede confirmar el warehouse real antes de correr el Módulo 2"
fi

# =============================================================================
# MÓDULO 2 — Validación funcional cruzada (núcleo de esta integración)
# =============================================================================
section "MÓDULO 2 · Validación funcional cruzada Trino ↔ Gravitino ↔ Flink"

if $SKIP_FUNCTIONAL_TEST; then
  info "Módulo 2 omitido (--skip-functional-test)"
elif [[ -z "$CATALOG_NAME" || -z "$TRINO_REST_URI" ]]; then
  warn "Módulo 2 omitido: no se pudo confirmar el catálogo Iceberg/REST de Trino en el Módulo 1 — no hay un catalogo.schema válido donde crear tablas de prueba sin asumirlo"
elif ! $JM_LOCAL; then
  warn "Módulo 2 (2.1, escritura desde Flink) omitido: contenedor JobManager no detectado localmente. Se continúa solo con 2.3 (escritura desde Trino), que no depende de Flink"
else
  FULL_TABLE_FLINK="${CATALOG_NAME}.${TEST_SCHEMA}.${TEST_TABLE_FLINK}"
  FULL_TABLE_TRINO="${CATALOG_NAME}.${TEST_SCHEMA}.${TEST_TABLE_TRINO}"

  # 2.1 Crear tabla de prueba DESDE FLINK sobre el catálogo Iceberg/Gravitino
  # descubierto en el Módulo 1, vía sql-client.sh en el contenedor JobManager
  # (mismo patrón que validate_kafka_flink_principal_flink.sh Módulo 3).
  info "Creando tabla de prueba desde Flink sobre el catálogo compartido: $FULL_TABLE_FLINK"
  SQL_SCRIPT_LOCAL="/tmp/trino_gravitino_flinkside.sql"
  SQL_SCRIPT_CONTAINER="/tmp/trino_gravitino_flinkside.sql"

  cat > "$SQL_SCRIPT_LOCAL" <<SQLEOF
CREATE CATALOG ${CATALOG_NAME} WITH (
  'type'     = 'iceberg',
  'catalog-type' = 'rest',
  'uri'      = '${TRINO_REST_URI}'
);
USE CATALOG ${CATALOG_NAME};
CREATE DATABASE IF NOT EXISTS ${TEST_SCHEMA};
CREATE TABLE IF NOT EXISTS ${TEST_SCHEMA}.${TEST_TABLE_FLINK} (
  id BIGINT,
  origen STRING
);
INSERT INTO ${TEST_SCHEMA}.${TEST_TABLE_FLINK} SELECT 1, 'bluepoint_validacion_flink';
SQLEOF

  if podman cp "$SQL_SCRIPT_LOCAL" "${CONTAINER_JM}:${SQL_SCRIPT_CONTAINER}" 2>/dev/null; then
    ok "Script SQL copiado al contenedor JobManager"
    nohup podman exec "$CONTAINER_JM" "${FLINK_HOME}/bin/sql-client.sh" -f "$SQL_SCRIPT_CONTAINER" \
      > "/tmp/trino_gravitino_flinkside_sqlclient.log" 2>&1 &

    # El sql-client se desconecta apenas el job queda SUBMITTED — un log
    # limpio en ese punto no dice nada sobre si el job realmente terminó.
    # Se extrae el Job ID y se sondea su estado real vía REST de Flink
    # (/jobs/<id>) hasta un estado terminal, en vez de un sleep fijo seguido
    # de grep sobre el log del cliente.
    info "Esperando a que el sql-client reporte el Job ID de inserción (máx. ${SQL_JOB_STARTUP_WAIT_S}s)..."
    FLINK_JOB_ID=""
    ELAPSED=0
    while [[ "$ELAPSED" -lt "$SQL_JOB_STARTUP_WAIT_S" ]]; do
      FLINK_JOB_ID=$(grep -oP '(?<=Job ID: )[0-9a-f]+' "/tmp/trino_gravitino_flinkside_sqlclient.log" 2>/dev/null | head -1 || echo "")
      [[ -n "$FLINK_JOB_ID" ]] && break
      sleep 2
      ELAPSED=$((ELAPSED + 2))
    done

    if [[ -z "$FLINK_JOB_ID" ]]; then
      FLINK_SQL_ERR=$(grep -iE "ERROR|Exception" "/tmp/trino_gravitino_flinkside_sqlclient.log" 2>/dev/null | head -1 || echo "")
      fail "El sql-client no reportó un Job ID de inserción en ${SQL_JOB_STARTUP_WAIT_S}s${FLINK_SQL_ERR:+ — error visible en el log: $FLINK_SQL_ERR} (ver /tmp/trino_gravitino_flinkside_sqlclient.log en $THIS_HOST)"
    else
      info "Job de inserción enviado: $FLINK_JOB_ID — sondeando estado real vía REST de Flink ($JM_API, máx. ${JOB_POLL_MAX_WAIT_S}s)"
      JOB_STATE=""
      POLL_ELAPSED=0
      while [[ "$POLL_ELAPSED" -lt "$JOB_POLL_MAX_WAIT_S" ]]; do
        JOB_JSON=$(curl -sf --max-time 10 "$JM_API/jobs/${FLINK_JOB_ID}" 2>/dev/null || echo "")
        JOB_STATE=$(echo "$JOB_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
        [[ "$JOB_STATE" == "FINISHED" || "$JOB_STATE" == "FAILED" || "$JOB_STATE" == "CANCELED" ]] && break
        sleep "$JOB_POLL_INTERVAL_S"
        POLL_ELAPSED=$((POLL_ELAPSED + JOB_POLL_INTERVAL_S))
      done

      case "$JOB_STATE" in
        FINISHED)
          ok "Job Flink de inserción ($FLINK_JOB_ID) terminó FINISHED"
          ;;
        FAILED|CANCELED)
          FLINK_EXC_MSG=$(curl -sf --max-time 10 "$JM_API/jobs/${FLINK_JOB_ID}/exceptions" 2>/dev/null \
            | python3 -c "
import sys, json
d = json.load(sys.stdin)
msg = d.get('root-exception') or ''
if not msg:
    hist = d.get('all-exceptions') or []
    msg = hist[0].get('exception', '') if hist else ''
print((msg or '').splitlines()[0] if msg else '')
" 2>/dev/null || echo "")
          fail "Job Flink de inserción ($FLINK_JOB_ID) terminó en estado $JOB_STATE${FLINK_EXC_MSG:+ — causa: $FLINK_EXC_MSG} (detalle completo: curl $JM_API/jobs/${FLINK_JOB_ID}/exceptions)"
          ;;
        *)
          warn "Job Flink de inserción ($FLINK_JOB_ID) no llegó a estado terminal en ${JOB_POLL_MAX_WAIT_S}s — último estado observado: '${JOB_STATE:-desconocido}' (puede seguir reiniciando en background, p. ej. por falta de recursos/slots). Revisar manualmente: curl $JM_API/jobs/${FLINK_JOB_ID}"
          ;;
      esac
    fi
    rm -f "$SQL_SCRIPT_LOCAL"
    podman exec "$CONTAINER_JM" rm -f "$SQL_SCRIPT_CONTAINER" 2>/dev/null || true
  else
    fail "No se pudo copiar el script SQL al contenedor JobManager ($CONTAINER_JM)"
  fi

  # 2.2 Consultar esa misma tabla DESDE TRINO — confirma que el catálogo
  # compartido realmente lo es, no dos catálogos distintos por error de
  # configuración que coincidentemente no fallan por separado.
  info "Consultando desde Trino la tabla creada por Flink: $FULL_TABLE_FLINK"
  READ_RESULT=$(run_statement "SELECT count(*) FROM ${FULL_TABLE_FLINK}")
  READ_ERR=$(statement_error "$READ_RESULT")
  if [[ -z "$READ_RESULT" ]]; then
    fail "SELECT contra $FULL_TABLE_FLINK no obtuvo respuesta de Trino (timeout o error de red)"
  elif [[ -n "$READ_ERR" ]]; then
    fail "Trino NO ve la tabla creada por Flink ($FULL_TABLE_FLINK): $READ_ERR — evidencia de que el catálogo compartido NO está realmente unificado entre Flink y Trino (HALLAZGO PRIORITARIO, ver informe)"
  else
    ROW_COUNT=$(echo "$READ_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); data=d.get('data',[]); print(data[0][0] if data else '')" 2>/dev/null || echo "")
    if [[ "$ROW_COUNT" -gt 0 ]] 2>/dev/null; then
      ok "Trino leyó correctamente la tabla creada por Flink vía el catálogo Gravitino compartido: $ROW_COUNT fila(s)"
    else
      fail "Trino ve la tabla $FULL_TABLE_FLINK en el catálogo pero devuelve 0 filas — la metadata está registrada en Gravitino pero los datos escritos por Flink no son visibles desde Trino (posible inconsistencia de catálogo, HALLAZGO PRIORITARIO)"
    fi
  fi

  # 2.3 A la inversa: crear tabla DESDE TRINO (CTAS) y confirmar el registro
  # en Gravitino vía su REST API — sin levantar un job Flink completo para
  # esto (basta confirmar el registro de metadata).
  info "Creando tabla de prueba desde Trino (CTAS): $FULL_TABLE_TRINO"
  run_statement "CREATE SCHEMA IF NOT EXISTS ${CATALOG_NAME}.${TEST_SCHEMA}" >/dev/null
  CTAS_RESULT=$(run_statement "CREATE TABLE ${FULL_TABLE_TRINO} AS SELECT 1 AS id, 'bluepoint_validacion_trino' AS origen")
  CTAS_ERR=$(statement_error "$CTAS_RESULT")
  if [[ -z "$CTAS_RESULT" ]]; then
    warn "No se pudo confirmar el resultado del CTAS de prueba desde Trino (timeout o error de red)"
  elif [[ -n "$CTAS_ERR" ]]; then
    fail "CREATE TABLE desde Trino falló: $CTAS_ERR — revisar si Trino tiene permisos de escritura de metadata en el catálogo compartido"
  else
    ok "CREATE TABLE AS SELECT desde Trino aceptado: $FULL_TABLE_TRINO"
    GRV_TABLES_JSON=$(curl -sf --max-time 10 \
      "$GRAVITINO_API/api/metalakes/${CATALOG_NAME}/catalogs/${CATALOG_NAME}/schemas/${TEST_SCHEMA}/tables" 2>/dev/null || echo "")
    if [[ -n "$GRV_TABLES_JSON" ]] && echo "$GRV_TABLES_JSON" | grep -qi "$TEST_TABLE_TRINO"; then
      ok "Gravitino registró la tabla creada por Trino ($TEST_TABLE_TRINO) — metadata visible vía REST API"
    else
      warn "No se pudo confirmar vía REST API de Gravitino el registro de $TEST_TABLE_TRINO (endpoint de metalake/catálogo probado con nombres asumidos igual a '$CATALOG_NAME' — puede no coincidir con la jerarquía real de metalake/catálogo). Confirmar manualmente con: curl $GRAVITINO_API/api/metalakes/<metalake>/catalogs/<catalogo>/schemas/${TEST_SCHEMA}/tables"
    fi
  fi

  # 2.4 Limpieza — desde Trino, que tiene acceso SQL síncrono directo al
  # catálogo compartido (evita depender de otro job Flink en background).
  info "Limpiando objetos de prueba..."
  for T in "$FULL_TABLE_FLINK" "$FULL_TABLE_TRINO"; do
    DROP_RESULT=$(run_statement "DROP TABLE IF EXISTS ${T}")
    DROP_ERR=$(statement_error "$DROP_RESULT")
    if [[ -z "$DROP_ERR" ]]; then
      ok "Tabla de prueba $T eliminada correctamente"
    else
      warn "No se pudo confirmar el DROP TABLE de $T ('$DROP_ERR') — eliminar manualmente para no dejar residuos de la validación"
    fi
  done
  DROP_SCHEMA_RESULT=$(run_statement "DROP SCHEMA IF EXISTS ${CATALOG_NAME}.${TEST_SCHEMA}")
  DROP_SCHEMA_ERR=$(statement_error "$DROP_SCHEMA_RESULT")
  if [[ -z "$DROP_SCHEMA_ERR" ]]; then
    ok "Schema de prueba ${TEST_SCHEMA} eliminado correctamente"
  else
    warn "No se pudo confirmar el DROP SCHEMA de ${TEST_SCHEMA} ('$DROP_SCHEMA_ERR') — eliminar manualmente"
  fi
fi

# =============================================================================
# MÓDULO 3 — Logs
# =============================================================================
section "MÓDULO 3 · Logs del coordinator de Trino"

if $CRD_LOCAL; then
  TRINO_GRV_ERRORS=$(podman logs --tail 200 "$CONTAINER_COORDINATOR" 2>/dev/null \
    | grep -iE "rest.?catalog|gravitino" | grep -iE "error|exception|timeout|refused|failed" || true)
  if [[ -n "$TRINO_GRV_ERRORS" ]]; then
    warn "Se detectaron líneas de error relacionadas al catálogo REST/Gravitino en los últimos 200 logs del coordinator:"
    while IFS= read -r LN; do warn "    $LN"; done <<< "$TRINO_GRV_ERRORS"
  else
    ok "Sin errores de conexión al catálogo REST/Gravitino en los últimos 200 logs del coordinator"
  fi
else
  warn "Contenedor Coordinator no local — no se pudo revisar logs de Trino automáticamente"
fi

# =============================================================================
# MÓDULO 4 — DR
# =============================================================================
section "MÓDULO 4 · DR"
info "No se genera validate_trino_gravitino_dr.sh en esta entrega. Se hereda, sin"
info "re-investigar, la conclusión de validate_trino_principal.sh ('No aplica en"
info "DR por diseño', pendiente de confirmación en sitio) y de"
info "validate_gravitino_principal.sh ('PREGUNTA ABIERTA', sin resolver). Ninguno"
info "de los dos componentes tiene su despliegue en DR confirmado, por lo que"
info "esta integración tampoco aplica en DR hasta que ambos se aclaren."

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Integración Trino ↔ Gravitino · PRINCIPAL            ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Integración Trino ↔ Gravitino validada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  Se detectaron $FAIL fallos críticos en la integración — revisar log: $LOG_FILE${NC}"
fi
log ""

if [[ -n "$GRV_WAREHOUSE" ]] && ! echo "$GRV_WAREHOUSE" | grep -qP '^s3a?://'; then
  log "${RED}${BOLD}  ⚠  ADVERTENCIA: el warehouse de Gravitino ('$GRV_WAREHOUSE') no es S3 — ningún PASS${NC}"
  log "${RED}${BOLD}     del Módulo 2 de este run es evidencia de integración real con AIStor.${NC}"
  log ""
fi

exit $FAIL
