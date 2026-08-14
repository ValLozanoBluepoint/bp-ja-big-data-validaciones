#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Flink ↔ Gravitino (DC Principal)
#  Catálogo REST Iceberg de Gravitino usado directamente por Flink (sin Trino)
#
#  Alcance: la conexión Flink → Gravitino (metadata/catálogo REST de Iceberg),
#  validada de forma standalone, sin pasar por Trino. NO valida la conexión
#  directa Flink → AIStor para checkpoints/savepoints (eso es
#  validacion_flink_minio/), la integración Trino↔Gravitino (eso es
#  validacion_trino_gravitino/, que ya cubre un caso de uso cruzado parcial),
#  ni la salud de Flink o Gravitino como tecnologías en sí mismas (eso es
#  validacion_flink/ y validacion_gravitino/, que deben correrse antes).
#
#  Por qué es un script separado: un correo interno del equipo de arquitectura
#  (context/Contexto-extra-correos.md, 2026-08-06) confirma que "Gravitino
#  solo maneja metadata (catálogo de tablas), nunca los archivos de datos -por
#  eso Flink y Trino tienen una conexión hacia Gravitino (metadata) y otra
#  directa hacia AIStor (archivos), sin pasar por Gravitino." Flink tiene DOS
#  conexiones independientes; este script valida la primera (metadata), no la
#  segunda (checkpoints/savepoints, ver validacion_flink_minio/).
#
#  Hueco de cobertura cerrado por este script: validate_trino_gravitino_principal.sh
#  documenta explícitamente en su propio encabezado que "la integración
#  Flink↔Gravitino tampoco tiene validación propia todavía" — su Módulo 2 solo
#  la ejercita como prueba cruzada (confirmar que Trino y Flink comparten el
#  mismo catálogo), sin reemplazar una validación dedicada. Este script es esa
#  validación dedicada: Flink usa el catálogo REST de Gravitino directamente,
#  sin que Trino esté involucrado en ningún paso.
#
#  JAR requerido: el mismo correo (línea 69) confirma que "Flink JobManager
#  (platformapps-1) y TaskManagers (cmp-1/2/3)... sí requieren el JAR
#  iceberg-flink-runtime-1.20-1.9.2.jar en el directorio lib/ de Flink" — sin
#  este JAR, un CREATE CATALOG de tipo iceberg falla en Flink SQL. El Módulo 1
#  lo verifica explícitamente (chequeo nuevo, no presente en ningún script
#  hermano existente).
#
#  Topología (DC Principal — JobManager y Gravitino comparten el mismo nodo
#  platform-apps-1, ver validate_flink_principal.sh / validate_gravitino_principal.sh):
#    JobManager/Gravitino : pbigd-plat-apps01
#      (contenedores: flink-jobmanager, patrón *gravitino*)
#
#  DR: NO se genera validate_flink_gravitino_dr.sh en esta entrega. A
#  diferencia de validate_trino_gravitino_principal.sh (donde ambos lados
#  estaban sin confirmar en DR), aquí hay una asimetría: Flink SÍ tiene DR
#  confirmado desplegado (validacion_flink/validate_flink_dr.sh documenta una
#  topología DR completa: JobManager en pbigd-plat-apps01-cont, TaskManagers
#  en pbigd-proc01-cont/pbigd-proc02-cont). Gravitino, en cambio, sigue como
#  "PREGUNTA ABIERTA" en validate_gravitino_principal.sh — no aparece en la
#  tabla de asignación de recursos del datacenter alterno. Es Gravitino, no
#  Flink, el componente que bloquea esta integración en DR.
#
#  Requisitos previos (ver preparacion_validacion_flink_gravitino.md):
#    - validacion_flink/validate_flink_principal.sh y
#      validacion_gravitino/validate_gravitino_principal.sh ya corridos.
#    - Ejecutar desde pbigd-plat-apps01 (o pasar --gravitino-host /
#      --flink-jobmanager-host).
#    - python3 disponible (parseo de JSON de las REST API de Gravitino/Flink).
#
#  NO usa SSH: mismo criterio que el resto de la familia de scripts de
#  integración. Todos los chequeos automáticos asumen que el script corre en
#  el nodo donde viven los dos contenedores (colocados en platform-apps-1
#  según el plan de implementación); si alguno no está local, el chequeo que
#  dependa de él degrada a WARN con instrucción manual, no FAIL.
#
#  Fuera de alcance: conexión Flink → AIStor para checkpoints/savepoints (ver
#  validacion_flink_minio/), integración Trino↔Gravitino (ver
#  validacion_trino_gravitino/), salud de Flink/Gravitino como tecnologías en
#  sí mismas (ver sus scripts respectivos), DR (ver arriba).
#
#  Uso:
#    chmod +x validate_flink_gravitino_principal.sh
#    ./validate_flink_gravitino_principal.sh \
#        [--gravitino-host host] [--flink-jobmanager-host host] \
#        [--skip-functional-test]
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
GRAVITINO_HOST="${GRAVITINO_HOST:-pbigd-plat-apps01}"
GRAVITINO_PORT=8090
# Gravitino expone DOS puertos propios en el mismo contenedor: 8090 (API
# general de metalakes/catálogos) y 9001 (Iceberg REST Catalog específico,
# /iceberg/v1/...). Este es el endpoint que Flink usará directamente en el
# CREATE CATALOG del Módulo 2 — confirmado en validate_trino_gravitino_principal.sh
# (curl http://localhost:9001/iceberg/v1/config responde el config real).
GRAVITINO_ICEBERG_PORT=9001
GRAVITINO_CONTAINER_PATTERN="gravitino"

JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01}"
JM_REST_PORT=8081
CONTAINER_JM="flink-jobmanager"
FLINK_HOME="/opt/flink"

TEST_CATALOG="bluepoint_fg"
TEST_SCHEMA="bluepoint_validacion_fg"
TEST_TABLE="tabla_flink_gravitino"

SQL_JOB_STARTUP_WAIT_S=15         # tiempo máximo esperando a que el sql-client reporte el Job ID
JOB_POLL_INTERVAL_S=5             # intervalo de sondeo del estado real del job vía REST de Flink
JOB_POLL_MAX_WAIT_S=90            # tiempo máximo esperando a que el job llegue a estado terminal

SKIP_FUNCTIONAL_TEST=false

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_flink_gravitino_principal_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# Envía un job Flink SQL (background, vía sql-client.sh) y espera a que el
# estado real del último job llegue a un estado terminal, sondeando la REST
# API de Flink en vez de un sleep fijo. Devuelve 0 si terminó FINISHED.
run_flink_sql() {
  local sql_content="$1"
  local run_id="$2"
  local sql_local="/tmp/${run_id}.sql"
  local sql_container="/tmp/${run_id}.sql"
  local log_local="/tmp/${run_id}_sqlclient.log"

  echo "$sql_content" > "$sql_local"

  if ! podman cp "$sql_local" "${CONTAINER_JM}:${sql_container}" 2>/dev/null; then
    fail "No se pudo copiar el script SQL '$run_id' al contenedor JobManager ($CONTAINER_JM)"
    rm -f "$sql_local"
    return 1
  fi

  nohup podman exec "$CONTAINER_JM" "${FLINK_HOME}/bin/sql-client.sh" -f "$sql_container" \
    > "$log_local" 2>&1 &

  local job_id=""
  local elapsed=0
  while [[ "$elapsed" -lt "$SQL_JOB_STARTUP_WAIT_S" ]]; do
    job_id=$(grep -oP '(?<=Job ID: )[0-9a-f]+' "$log_local" 2>/dev/null | head -1 || echo "")
    [[ -n "$job_id" ]] && break
    sleep 2
    elapsed=$((elapsed + 2))
  done

  rm -f "$sql_local"
  podman exec "$CONTAINER_JM" rm -f "$sql_container" 2>/dev/null || true

  if [[ -z "$job_id" ]]; then
    local sql_err
    sql_err=$(grep -iE "ERROR|Exception" "$log_local" 2>/dev/null | head -1 || echo "")
    fail "El sql-client no reportó un Job ID para '$run_id' en ${SQL_JOB_STARTUP_WAIT_S}s${sql_err:+ — error visible en el log: $sql_err} (ver $log_local en $THIS_HOST)"
    return 1
  fi

  info "Job Flink enviado ($run_id): $job_id — sondeando estado real vía REST de Flink ($JM_API, máx. ${JOB_POLL_MAX_WAIT_S}s)"
  local job_state=""
  local poll_elapsed=0
  while [[ "$poll_elapsed" -lt "$JOB_POLL_MAX_WAIT_S" ]]; do
    local job_json
    job_json=$(curl -sf --max-time 10 "$JM_API/jobs/${job_id}" 2>/dev/null || echo "")
    job_state=$(echo "$job_json" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
    [[ "$job_state" == "FINISHED" || "$job_state" == "FAILED" || "$job_state" == "CANCELED" ]] && break
    sleep "$JOB_POLL_INTERVAL_S"
    poll_elapsed=$((poll_elapsed + JOB_POLL_INTERVAL_S))
  done

  case "$job_state" in
    FINISHED)
      ok "Job Flink '$run_id' ($job_id) terminó FINISHED"
      return 0
      ;;
    FAILED|CANCELED)
      local exc_msg
      exc_msg=$(curl -sf --max-time 10 "$JM_API/jobs/${job_id}/exceptions" 2>/dev/null \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
msg = d.get('root-exception') or ''
if not msg:
    hist = d.get('all-exceptions') or []
    msg = hist[0].get('exception', '') if hist else ''
print((msg or '').splitlines()[0] if msg else '')
" 2>/dev/null || echo "")
      fail "Job Flink '$run_id' ($job_id) terminó en estado $job_state${exc_msg:+ — causa: $exc_msg} (detalle completo: curl $JM_API/jobs/${job_id}/exceptions)"
      return 1
      ;;
    *)
      warn "Job Flink '$run_id' ($job_id) no llegó a estado terminal en ${JOB_POLL_MAX_WAIT_S}s — último estado observado: '${job_state:-desconocido}' (puede seguir reiniciando en background, p. ej. por falta de recursos/slots). Revisar manualmente: curl $JM_API/jobs/${job_id}"
      return 1
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gravitino-host)          GRAVITINO_HOST="$2"; shift 2 ;;
    --flink-jobmanager-host)   JM_HOST="$2"; shift 2 ;;
    --skip-functional-test)    SKIP_FUNCTIONAL_TEST=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
GRAVITINO_API="http://${GRAVITINO_HOST}:${GRAVITINO_PORT}"
GRAVITINO_ICEBERG_API="http://${GRAVITINO_HOST}:${GRAVITINO_ICEBERG_PORT}"
JM_API="http://${JM_HOST}:${JM_REST_PORT}"

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Integración Flink ↔ Gravitino — PRINCIPAL          ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución    : $THIS_HOST"
info "Gravitino            : $GRAVITINO_HOST ($GRAVITINO_API, Iceberg REST: $GRAVITINO_ICEBERG_API)"
info "JobManager Flink     : $JM_HOST ($JM_API)"
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_flink_principal.sh y${NC}"
log "${CYAN}  validate_gravitino_principal.sh ya se ejecutaron. Aquí solo se valida${NC}"
log "${CYAN}  la conexión de metadata Flink → Gravitino, de forma standalone y sin${NC}"
log "${CYAN}  pasar por Trino (ver validacion_trino_gravitino/ para esa integración).${NC}"
log ""

# =============================================================================
# MÓDULO 1 — Conectividad base Flink → Gravitino
# =============================================================================
section "MÓDULO 1 · Conectividad base Flink → Gravitino"

# 1.1 Detectar contenedor JobManager local
JM_CONTAINER_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null || echo "")
JM_LOCAL=false
if echo "$JM_CONTAINER_STATUS" | grep -qi "up"; then
  JM_LOCAL=true
  ok "Contenedor JobManager local corriendo: $JM_CONTAINER_STATUS"
else
  warn "Contenedor JobManager ($CONTAINER_JM) no detectado localmente en $THIS_HOST — ejecutar este script en $JM_HOST para validar el resto del Módulo 1 y el Módulo 2"
fi

# 1.2 Gravitino API general responde
GRV_VER_JSON=$(curl -sf --max-time 10 "$GRAVITINO_API/api/version" 2>/dev/null || echo "")
if [[ -n "$GRV_VER_JSON" ]]; then
  ok "Gravitino REST API responde desde $THIS_HOST: $GRAVITINO_API/api/version"
else
  fail "Gravitino REST API no responde en $GRAVITINO_API — el Módulo 2 no podrá completarse"
fi

# 1.3 Iceberg REST Catalog de Gravitino responde — este es el endpoint que
# Flink usará directamente en el CREATE CATALOG del Módulo 2 (a diferencia de
# validate_trino_gravitino_principal.sh, aquí no existe una URI ya configurada
# de antemano en ningún archivo estático que se pueda descubrir, así que este
# chequeo es bloqueante, no informativo).
GRV_ICEBERG_CONFIG_JSON=$(curl -sf --max-time 10 "${GRAVITINO_ICEBERG_API}/iceberg/v1/config" 2>/dev/null || echo "")
if [[ -n "$GRV_ICEBERG_CONFIG_JSON" ]] && echo "$GRV_ICEBERG_CONFIG_JSON" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
  ok "Iceberg REST Catalog de Gravitino responde con config JSON válido: ${GRAVITINO_ICEBERG_API}/iceberg/v1/config"
else
  fail "Iceberg REST Catalog de Gravitino NO respondió un config JSON válido en ${GRAVITINO_ICEBERG_API}/iceberg/v1/config — el Módulo 2 no podrá crear el catálogo en Flink sin este endpoint"
fi

# 1.4 JAR iceberg-flink-runtime presente en el JobManager — requisito
# documentado explícitamente en context/Contexto-extra-correos.md (línea 69).
# Sin este JAR, CREATE CATALOG de tipo iceberg falla en Flink SQL.
ICEBERG_JAR=""
if $JM_LOCAL; then
  ICEBERG_JAR=$(podman exec "$CONTAINER_JM" sh -c "find ${FLINK_HOME}/lib -maxdepth 1 -iname 'iceberg-flink-runtime*.jar' 2>/dev/null" || echo "")
  if [[ -n "$ICEBERG_JAR" ]]; then
    ok "JAR iceberg-flink-runtime encontrado en el JobManager: $ICEBERG_JAR"
  else
    fail "No se encontró ningún JAR iceberg-flink-runtime*.jar en ${FLINK_HOME}/lib/ del JobManager — requisito confirmado en context/Contexto-extra-correos.md; sin este JAR el CREATE CATALOG del Módulo 2 fallará"
  fi
fi

# 1.5 Si Gravitino tiene 2+ instancias, confirmar evidencia de un mecanismo
# real de HA delante (mismo hallazgo de HA "de papel" ya reportado en
# validate_gravitino_principal.sh 2.3 y validate_trino_gravitino_principal.sh 1.3).
mapfile -t GRAVITINO_CONTAINERS < <(podman ps --filter "name=${GRAVITINO_CONTAINER_PATTERN}" --format "{{.Names}}" 2>/dev/null || true)
N_GRAVITINO_INSTANCES=${#GRAVITINO_CONTAINERS[@]}
if [[ "$N_GRAVITINO_INSTANCES" -ge 2 ]]; then
  HA_EVIDENCE=""
  if podman ps --format "{{.Names}}" 2>/dev/null | grep -qiE "haproxy|keepalived"; then
    HA_EVIDENCE="contenedor HAProxy/Keepalived detectado en este nodo"
  fi
  if [[ -n "$HA_EVIDENCE" ]]; then
    ok "2+ instancias de Gravitino con evidencia de HA ($HA_EVIDENCE) — confirmar que Flink apunta a ese mecanismo, no a una instancia individual"
  else
    warn "Gravitino tiene $N_GRAVITINO_INSTANCES instancias pero sin evidencia de HAProxy/Keepalived (mismo hallazgo ya reportado en validate_gravitino_principal.sh 2.3) — si Flink apunta directo a una instancia individual ($GRAVITINO_ICEBERG_API), pierde el catálogo entero si esa instancia cae, sin failover real"
  fi
else
  info "Gravitino detectado con $N_GRAVITINO_INSTANCES instancia(s) — sin escenario de HA que validar"
fi

# 1.6 Confirmar ausencia de referencia estática a Gravitino en conf/*.yaml del
# JobManager — comportamiento esperado, no una anomalía: el catálogo de Flink
# en este proyecto se define vía CREATE CATALOG en el SQL de sesión (Módulo 2).
if $JM_LOCAL; then
  FLINK_GRV_LINE=$(podman exec "$CONTAINER_JM" sh -c "grep -riE 'gravitino|rest-catalog' ${FLINK_HOME}/conf/*.yaml 2>/dev/null" || echo "")
  if [[ -n "$FLINK_GRV_LINE" ]]; then
    info "Referencia a Gravitino/rest-catalog encontrada en la config estática de Flink (no esperado por diseño, pero no es un error):"
    while IFS= read -r LN; do info "    $LN"; done <<< "$FLINK_GRV_LINE"
  else
    info "Sin referencia a Gravitino/rest-catalog en ${FLINK_HOME}/conf/*.yaml dentro del JobManager — esperado: el catálogo de Flink se define vía CREATE CATALOG en el SQL de sesión (Módulo 2), no en la config estática"
  fi
fi

# 1.7 [PRIORITARIO] Confirmar que el warehouse real del iceberg-rest-server de
# Gravitino apunta a S3/AIStor, no a almacenamiento local efímero del
# contenedor. Sin este check, un PASS del Módulo 2 (Flink escribe/lee de
# vuelta) es un falso positivo: con catalog-backend=memory + warehouse=/tmp
# (config de ejemplo de la doc de Gravitino, nunca completada), Flink puede
# escribir/leer perfectamente bien contra disco local del contenedor sin que
# ni un byte llegue al lake real. Hallazgo de sitio 2026-08-14 confirmado
# desde validate_trino_minio_principal.sh (Módulo 1.5) — ver
# hallazgos_transversales.md en la raíz del repo.
GRAVITINO_CONF_CANDIDATES=("/root/gravitino/conf/gravitino-iceberg-rest-server.conf" "/root/gravitino/conf/gravitino.conf")
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
    fail "Warehouse de Gravitino NO apunta a S3/AIStor: '$GRV_WAREHOUSE' (catalog-backend='${GRV_BACKEND:-desconocido}') en $(basename "$GRV_CONF_FOUND") de '${GRAVITINO_CONTAINERS[0]}' — un PASS del Módulo 2 de este script NO es evidencia de integración con AIStor: Flink estaría escribiendo/leyendo contra almacenamiento local efímero del contenedor Gravitino. Ver hallazgos_transversales.md en la raíz del repo."
  fi
else
  warn "No se detectó ningún contenedor Gravitino local ($GRAVITINO_CONTAINER_PATTERN) — no se puede confirmar el warehouse real antes de correr el Módulo 2"
fi

# =============================================================================
# MÓDULO 2 — Validación funcional Flink → Gravitino (núcleo de esta integración)
# =============================================================================
section "MÓDULO 2 · Validación funcional Flink → Gravitino (sin Trino)"

if $SKIP_FUNCTIONAL_TEST; then
  info "Módulo 2 omitido (--skip-functional-test)"
elif ! $JM_LOCAL; then
  warn "Módulo 2 omitido: contenedor JobManager no detectado localmente (ver Módulo 1)"
elif [[ -z "$GRV_ICEBERG_CONFIG_JSON" ]]; then
  warn "Módulo 2 omitido: el Iceberg REST Catalog de Gravitino no respondió en el Módulo 1 — no hay endpoint válido contra el cual crear el catálogo en Flink"
else
  FULL_TABLE="${TEST_CATALOG}.${TEST_SCHEMA}.${TEST_TABLE}"
  RUN_PREFIX="flink_gravitino_$(date +%s)"

  # 2.1 + 2.2 Crear catálogo/schema/tabla e insertar una fila de prueba, todo
  # en un solo job Flink SQL. Nombre de catálogo/schema arbitrarios: Gravitino
  # no exige que el nombre del catálogo de Flink coincida con un metalake
  # pre-existente para el REST Iceberg Catalog genérico (ver nota en
  # preparacion_validacion_flink_gravitino.md).
  info "Creando catálogo/schema/tabla de prueba desde Flink sobre Gravitino: $FULL_TABLE"
  CREATE_SQL=$(cat <<SQLEOF
CREATE CATALOG ${TEST_CATALOG} WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'rest',
  'uri'          = '${GRAVITINO_ICEBERG_API}/iceberg/v1'
);
USE CATALOG ${TEST_CATALOG};
CREATE DATABASE IF NOT EXISTS ${TEST_SCHEMA};
CREATE TABLE IF NOT EXISTS ${TEST_SCHEMA}.${TEST_TABLE} (
  id BIGINT,
  origen STRING
);
INSERT INTO ${TEST_SCHEMA}.${TEST_TABLE} SELECT 1, 'bluepoint_validacion_flink_gravitino';
SQLEOF
)
  CREATE_OK=false
  if run_flink_sql "$CREATE_SQL" "${RUN_PREFIX}_create"; then
    CREATE_OK=true
  fi

  # 2.3 Lectura de vuelta — confirma lectura-tras-escritura contra el mismo
  # catálogo, íntegramente con Flink (a diferencia de validate_trino_gravitino_principal.sh,
  # que delega esta lectura a Trino).
  if $CREATE_OK; then
    info "Leyendo de vuelta la tabla de prueba desde Flink: $FULL_TABLE"
    READ_SQL=$(cat <<SQLEOF
CREATE CATALOG ${TEST_CATALOG} WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'rest',
  'uri'          = '${GRAVITINO_ICEBERG_API}/iceberg/v1'
);
USE CATALOG ${TEST_CATALOG};
SELECT * FROM ${TEST_SCHEMA}.${TEST_TABLE};
SQLEOF
)
    if run_flink_sql "$READ_SQL" "${RUN_PREFIX}_read"; then
      ok "Flink leyó de vuelta correctamente la tabla que escribió sobre el catálogo Gravitino: $FULL_TABLE"
    else
      fail "Flink NO pudo leer de vuelta $FULL_TABLE tras escribirla — evidencia de inconsistencia entre la escritura y la lectura sobre el mismo catálogo REST (HALLAZGO PRIORITARIO, ver informe)"
    fi

    # 2.4 Confirmar el registro de metadata en Gravitino vía su REST API — la
    # jerarquía metalake/catálogo/schema exacta no está confirmada en el repo
    # (mismo vacío documental ya señalado para Trino↔Gravitino), se prueba con
    # el nombre usado en el CREATE CATALOG.
    GRV_TABLES_JSON=$(curl -sf --max-time 10 \
      "$GRAVITINO_API/api/metalakes/${TEST_CATALOG}/catalogs/${TEST_CATALOG}/schemas/${TEST_SCHEMA}/tables" 2>/dev/null || echo "")
    if [[ -n "$GRV_TABLES_JSON" ]] && echo "$GRV_TABLES_JSON" | grep -qi "$TEST_TABLE"; then
      ok "Gravitino registró la tabla creada por Flink ($TEST_TABLE) — metadata visible vía REST API"
    else
      warn "No se pudo confirmar vía REST API de Gravitino el registro de $TEST_TABLE (endpoint de metalake/catálogo probado con nombres asumidos iguales a '$TEST_CATALOG' — puede no coincidir con la jerarquía real de metalake/catálogo). Confirmar manualmente con: curl $GRAVITINO_API/api/metalakes/<metalake>/catalogs/<catalogo>/schemas/${TEST_SCHEMA}/tables"
    fi

    # 2.5 Limpieza
    info "Limpiando objetos de prueba..."
    DROP_SQL=$(cat <<SQLEOF
CREATE CATALOG ${TEST_CATALOG} WITH (
  'type'         = 'iceberg',
  'catalog-type' = 'rest',
  'uri'          = '${GRAVITINO_ICEBERG_API}/iceberg/v1'
);
USE CATALOG ${TEST_CATALOG};
DROP TABLE IF EXISTS ${TEST_SCHEMA}.${TEST_TABLE};
DROP DATABASE IF EXISTS ${TEST_SCHEMA};
SQLEOF
)
    if run_flink_sql "$DROP_SQL" "${RUN_PREFIX}_cleanup"; then
      ok "Objetos de prueba ($FULL_TABLE y schema ${TEST_SCHEMA}) eliminados correctamente"
    else
      warn "No se pudo confirmar la limpieza de los objetos de prueba — eliminar manualmente $FULL_TABLE y el schema ${TEST_SCHEMA} para no dejar residuos de la validación"
    fi
  else
    warn "Lectura de vuelta (2.3), confirmación en Gravitino (2.4) y limpieza (2.5) omitidas: la creación/inserción inicial (2.1/2.2) no terminó FINISHED"
  fi
fi

# =============================================================================
# MÓDULO 3 — Logs
# =============================================================================
section "MÓDULO 3 · Logs del JobManager de Flink"

if $JM_LOCAL; then
  FLINK_GRV_ERRORS=$(podman logs --tail 200 "$CONTAINER_JM" 2>/dev/null \
    | grep -iE "gravitino|rest.?catalog|iceberg" | grep -iE "error|exception|timeout|refused|failed" || true)
  if [[ -n "$FLINK_GRV_ERRORS" ]]; then
    warn "Se detectaron líneas de error relacionadas al catálogo REST/Gravitino en los últimos 200 logs del JobManager:"
    while IFS= read -r LN; do warn "    $LN"; done <<< "$FLINK_GRV_ERRORS"
  else
    ok "Sin errores de conexión al catálogo REST/Gravitino en los últimos 200 logs del JobManager"
  fi
else
  warn "Contenedor JobManager no local — no se pudo revisar logs de Flink automáticamente"
fi

# =============================================================================
# MÓDULO 4 — DR
# =============================================================================
section "MÓDULO 4 · DR"
info "No se genera validate_flink_gravitino_dr.sh en esta entrega. A diferencia de"
info "validate_trino_gravitino_principal.sh (donde ambos lados estaban sin confirmar"
info "en DR), aquí hay una asimetría: Flink SÍ tiene DR confirmado desplegado"
info "(validate_flink_dr.sh documenta JobManager en pbigd-plat-apps01-cont y"
info "TaskManagers en pbigd-proc01-cont/pbigd-proc02-cont). Gravitino, en cambio,"
info "sigue como 'PREGUNTA ABIERTA' en validate_gravitino_principal.sh — no aparece"
info "en la tabla de asignación de recursos del datacenter alterno. Es Gravitino,"
info "no Flink, el componente que bloquea esta integración en DR."

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Integración Flink ↔ Gravitino · PRINCIPAL            ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Integración Flink ↔ Gravitino validada SIN fallos críticos${NC}"
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
