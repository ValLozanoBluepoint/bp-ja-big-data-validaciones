#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Flink → MinIO (DC Principal)
#  Checkpoints/savepoints de Flink persistidos en MinIO (S3)
#
#  A diferencia de validacion_flink_kafka/ (que necesita dos scripts porque
#  el lado "productor" vive en otro clúster), aquí no hace falta split: la
#  confirmación de que el checkpoint llegó a MinIO se obtiene consultando la
#  REST API de Flink (GET /jobs/:id/checkpoints), que reporta el estado
#  COMPLETED y el external_path en S3 directamente desde el JobManager.
#
#  Flujo completo: ver preparacion_integracion_flink_minio.md
#
#  Topología (DC Principal):
#    JobManager   : pbigd-plat-apps01   (contenedor: flink-jobmanager)
#    TaskManagers : pbigd-proc01 | pbigd-proc02 | pbigd-proc03
#    MinIO        : pbigd-stg01 (endpoint S3, bucket flink-checkpoints)
#
#  Requisitos previos (ver preparacion_integracion_flink_minio.md):
#    - Bucket flink-checkpoints (con prefijos checkpoints/ y savepoints/) ya
#      creado en MinIO.
#    - Plugin flink-s3-fs-hadoop (o -presto) instalado en /opt/flink/plugins/
#      en los 4 nodos Flink (JM + 3 TM).
#    - config.yaml (Flink 2.x) o flink-conf.yaml (Flink 1.x) con
#      state.checkpoints.dir / state.savepoints.dir / credenciales S3
#      consistente en los 4 nodos. El script detecta automáticamente cuál
#      archivo usa el JobManager.
#    - Ejecutar desde el nodo JobManager (pbigd-plat-apps01) o pasar
#      --jobmanager-host.
#    - python3 disponible (parseo de JSON de la REST API de Flink).
#
#  NO usa SSH: no existe acceso SSH sin contraseña del usuario admapl entre
#  ningún par de nodos del entorno. Por eso:
#    - Módulo 1 (plugin S3 / config) solo se verifica automáticamente en el
#      JobManager; en cada TaskManager se deja como paso MANUAL (el script
#      imprime el comando exacto — ver preparacion_integracion_flink_minio.md,
#      Prerequisitos 2 y 3).
#
#  Extensión (deriva de config. HAProxy/MinIO): este script se escribió antes
#  de que existiera el VIP único de HAProxy delante de MinIO, validado aparte
#  en validacion_haproxy_minio/. --vip/--dns (sin valor por defecto — no hay
#  VIP/DNS estático confirmado en el repo, igual criterio que
#  validate_haproxy_minio_principal.sh) cumplen doble función aquí:
#    1. Si se pasan, el propio MINIO_ENDPOINT que este script usa para probar
#       conectividad (Módulo 1.1/1.2) pasa a ser la VIP/DNS, no el nodo
#       individual por defecto (se puede forzar un valor distinto con
#       --minio-endpoint).
#    2. El Módulo 1.5 compara el s3.endpoint real de Flink contra esa
#       VIP/DNS esperada.
#  Si NO se pasan, el script sigue validando contra el nodo individual
#  pbigd-stg01, pero lo marca con un WARN explícito (no silencioso) en el
#  encabezado del reporte, y el Módulo 1.5 queda en WARN documentando la
#  posible deriva en vez de asumir que ya está resuelta.
#
#  Fuera de alcance: JAR iceberg-flink-runtime, catálogo REST Gravitino y test
#  funcional de tablas Iceberg — este script valida checkpoints/savepoints de
#  Flink sobre MinIO, no la integración Flink↔Iceberg (ciclo de validación
#  separado, pendiente de datos confirmados por el equipo).
#
#  Uso:
#    chmod +x validate_flink_minio_principal.sh
#    ./validate_flink_minio_principal.sh \
#        [--jobmanager-host host] [--skip-functional-test] \
#        [--checkpoint-wait-seconds N] [--vip IP] [--dns nombre] \
#        [--minio-endpoint url] [--tm-manual-ok N]
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
JM_HOST="${JOBMANAGER_HOST:-pbigd-plat-apps01}"
JM_REST_PORT=8081
TASKMANAGER_HOSTS=("pbigd-proc01" "pbigd-proc02" "pbigd-proc03")
TM_CONTAINER_NAMES=("flink-tm-1" "flink-tm-2" "flink-tm-3")   # 1:1 con TASKMANAGER_HOSTS

CONTAINER_JM="flink-jobmanager"
FLINK_HOME="/opt/flink"

MINIO_NODE_DEFAULT="pbigd-stg01"
MINIO_PORT=9000
MINIO_ENDPOINT="http://${MINIO_NODE_DEFAULT}:${MINIO_PORT}"
MINIO_ENDPOINT_EXPLICIT=false
CHECKPOINT_BUCKET="flink-checkpoints"

CHECKPOINT_WAIT_S=90              # override con --checkpoint-wait-seconds; default > intervalo típico de 60s
SQL_JOB_STARTUP_WAIT_S=15
CHECKPOINT_POLL_INTERVAL_S=5

SKIP_FUNCTIONAL_TEST=false

# VIP/DNS esperado del HAProxy delante de MinIO (sin valor por defecto — no
# existe uno estático confirmado en el repo; mismo criterio que --vip/--dns
# en validate_haproxy_minio_principal.sh).
VIP=""
VIP_DNS=""

# --tm-manual-ok N: cantidad de TaskManagers ya confirmados manualmente
# (plugin S3 + config OK, ver Módulo 2) en una corrida previa del operador.
# Sin este flag, el Módulo 2 solo imprime instrucciones (no afecta PASS/FAIL).
TM_MANUAL_OK=""

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_flink_minio_principal_$(date +%Y%m%d_%H%M%S).log"

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
    --jobmanager-host)          JM_HOST="$2"; shift 2 ;;
    --skip-functional-test)     SKIP_FUNCTIONAL_TEST=true; shift ;;
    --checkpoint-wait-seconds)  CHECKPOINT_WAIT_S="$2"; shift 2 ;;
    --vip)                      VIP="$2"; shift 2 ;;
    --dns)                      VIP_DNS="$2"; shift 2 ;;
    --minio-endpoint)           MINIO_ENDPOINT="$2"; MINIO_ENDPOINT_EXPLICIT=true; shift 2 ;;
    --tm-manual-ok)              TM_MANUAL_OK="$2"; shift 2 ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

# Si se pasó --vip/--dns y no se pasó --minio-endpoint explícito, el endpoint
# a validar pasa a ser la VIP/DNS de HAProxy (mismo criterio que
# validate_haproxy_minio_principal.sh) en vez del nodo individual por defecto.
if [[ "$MINIO_ENDPOINT_EXPLICIT" == false && ( -n "$VIP" || -n "$VIP_DNS" ) ]]; then
  MINIO_ENDPOINT="http://${VIP_DNS:-$VIP}:${MINIO_PORT}"
fi

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
JM_API="http://${JM_HOST}:${JM_REST_PORT}"

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Integración Flink → MinIO — PRINCIPAL              ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución : $THIS_HOST"
info "JobManager Flink  : $JM_HOST ($JM_API)"
info "TaskManagers      : ${TASKMANAGER_HOSTS[*]}"
info "Endpoint MinIO    : $MINIO_ENDPOINT"
info "Bucket checkpoints: $CHECKPOINT_BUCKET"
if [[ -n "$VIP" || -n "$VIP_DNS" ]]; then
  info "VIP/DNS esperado  : ${VIP_DNS:-$VIP}"
else
  warn "No se pasó --vip ni --dns: este script está validando contra un endpoint de UN SOLO NODO ($MINIO_ENDPOINT), no contra la VIP/DNS de alta disponibilidad de HAProxy. Pasar --vip <IP> o --dns <nombre> para validar contra la VIP real."
fi
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_flink_principal.sh y${NC}"
log "${CYAN}  validate_minio_principal.sh ya se ejecutaron. Aquí solo se valida la${NC}"
log "${CYAN}  integración funcional Flink → MinIO (checkpoints reales).${NC}"
log ""

# =============================================================================
# MÓDULO 1 — Prerequisitos automatizables desde el JobManager
# =============================================================================
section "MÓDULO 1 · Prerequisitos (bucket, plugin S3, config) — JobManager"

# 1.1 MinIO alcanzable
# Nota: sin -f (fail-silent). Con -f, curl igual imprime el %{http_code} de
# un 4xx/5xx antes de salir con exit!=0, y el "|| echo 000" de abajo se
# agregaba a ese output ya impreso en vez de reemplazarlo (p.ej. "403000"
# en vez de "403") — queremos el código real, no que curl lo oculte.
MINIO_HTTP=$(curl -s --max-time 5 -o /dev/null -w "%{http_code}" \
  "${MINIO_ENDPOINT}/minio/health/live" 2>/dev/null)
MINIO_HTTP="${MINIO_HTTP:-000}"
if [[ "$MINIO_HTTP" == "200" ]]; then
  ok "MinIO alcanzable desde $THIS_HOST: HTTP $MINIO_HTTP"
else
  fail "MinIO no responde en $MINIO_ENDPOINT (HTTP $MINIO_HTTP) — el Módulo 3 no podrá completar checkpoints"
fi

# 1.2 Bucket de checkpoints accesible (sin mc: HEAD al bucket vía API S3 path-style)
BUCKET_HTTP=$(curl -s --max-time 5 -o /dev/null -w "%{http_code}" \
  "${MINIO_ENDPOINT}/${CHECKPOINT_BUCKET}" 2>/dev/null)
BUCKET_HTTP="${BUCKET_HTTP:-000}"
if [[ "$BUCKET_HTTP" =~ ^(200|403)$ ]]; then
  ok "Bucket '$CHECKPOINT_BUCKET' responde en MinIO (HTTP $BUCKET_HTTP)"
elif [[ "$BUCKET_HTTP" == "404" ]]; then
  fail "Bucket '$CHECKPOINT_BUCKET' NO existe en $MINIO_ENDPOINT (HTTP 404) — crearlo con los prefijos checkpoints/ y savepoints/"
else
  warn "No se pudo confirmar el bucket '$CHECKPOINT_BUCKET' (HTTP $BUCKET_HTTP) — verificar manualmente con 'mc ls' en un nodo MinIO"
fi

# Detectar contenedor JobManager local
JM_CONTAINER_STATUS=$(podman ps --filter "name=${CONTAINER_JM}" --format "{{.Status}}" 2>/dev/null || echo "")
JM_LOCAL=false
if echo "$JM_CONTAINER_STATUS" | grep -qi "up"; then
  JM_LOCAL=true
  ok "Contenedor JobManager local corriendo: $JM_CONTAINER_STATUS"
else
  warn "Contenedor JobManager ($CONTAINER_JM) no detectado localmente en $THIS_HOST — ejecutar este script en $JM_HOST para validar plugin/config"
fi

# Detectar archivo de configuración real: Flink 2.x renombró flink-conf.yaml
# (plano) a config.yaml (jerárquico, aunque acepta claves en formato plano —
# mismo criterio ya usado en validacion_flink/validate_flink_principal.sh).
FLINK_CONF_FILE="${FLINK_HOME}/conf/flink-conf.yaml"
if $JM_LOCAL; then
  if podman exec "$CONTAINER_JM" test -f "${FLINK_HOME}/conf/config.yaml" 2>/dev/null; then
    FLINK_CONF_FILE="${FLINK_HOME}/conf/config.yaml"
    info "Archivo de configuración detectado: config.yaml (formato Flink 2.x)"
  elif podman exec "$CONTAINER_JM" test -f "${FLINK_HOME}/conf/flink-conf.yaml" 2>/dev/null; then
    info "Archivo de configuración detectado: flink-conf.yaml (formato Flink 1.x)"
  else
    warn "No se encontró config.yaml ni flink-conf.yaml en ${FLINK_HOME}/conf/ del JobManager"
  fi
fi

# 1.3 Plugin S3 presente en el JobManager
PLUGIN_OK=false
if $JM_LOCAL; then
  S3_PLUGIN=$(podman exec "$CONTAINER_JM" find /opt/flink/plugins -iname "flink-s3-fs-*.jar" 2>/dev/null || echo "")
  if [[ -n "$S3_PLUGIN" ]]; then
    ok "Plugin S3 encontrado en JobManager: $S3_PLUGIN"
    PLUGIN_OK=true
  else
    fail "No se encontró flink-s3-fs-hadoop/-presto en /opt/flink/plugins/ del JobManager — el Módulo 3 no podrá completar checkpoints"
  fi
fi

# 1.4 Configuración de checkpoint/S3 en el archivo de config del JobManager
CKPT_INTERVAL="?"
if $JM_LOCAL; then
  CKPT_CFG=$(podman exec "$CONTAINER_JM" \
    grep -E "state.checkpoints.dir|state.savepoints.dir|s3.endpoint" \
    "$FLINK_CONF_FILE" 2>/dev/null || echo "")
  if [[ -n "$CKPT_CFG" ]]; then
    ok "Configuración de checkpoints/S3 encontrada en $(basename "$FLINK_CONF_FILE")"
    info "$CKPT_CFG"
  else
    fail "No se encontró state.checkpoints.dir/state.savepoints.dir/s3.endpoint en $(basename "$FLINK_CONF_FILE")"
  fi

  CKPT_INTERVAL=$(podman exec "$CONTAINER_JM" \
    grep -oP 'execution.checkpointing.interval\s*:\s*\K\S+' \
    "$FLINK_CONF_FILE" 2>/dev/null || echo "?")
  info "Intervalo de checkpoint configurado: $CKPT_INTERVAL ms"

  # 1.5 Endpoint S3 de Flink vs. VIP/DNS esperado (deriva de config. HAProxy)
  FLINK_S3_ENDPOINT=$(podman exec "$CONTAINER_JM" \
    grep -oP 's3\.endpoint\s*:\s*\K\S+' \
    "$FLINK_CONF_FILE" 2>/dev/null || echo "")
  if [[ -z "$FLINK_S3_ENDPOINT" ]]; then
    warn "No se pudo extraer s3.endpoint de $(basename "$FLINK_CONF_FILE") — no se puede confirmar si apunta al VIP o a un nodo individual"
  elif [[ -n "$VIP" || -n "$VIP_DNS" ]]; then
    TARGET="${VIP_DNS:-$VIP}"
    if echo "$FLINK_S3_ENDPOINT" | grep -qF "$TARGET"; then
      ok "s3.endpoint de Flink ($FLINK_S3_ENDPOINT) coincide con el VIP/DNS esperado ($TARGET)"
    else
      # Coincidencia de texto falló (p.ej. nombre corto "itaca" en s3.endpoint
      # vs. FQDN "itaca.jardinazuayo.fin.ec" pasado en --dns): antes de marcar
      # FAIL, resolver ambos por DNS dentro del propio contenedor (respeta el
      # mismo search domain que usa Flink) y comparar IPs.
      FLINK_S3_HOST=$(echo "$FLINK_S3_ENDPOINT" | sed -E 's#^[a-zA-Z]+://##; s#:[0-9]+$##; s#/.*$##')
      FLINK_S3_IP=$(podman exec "$CONTAINER_JM" getent hosts "$FLINK_S3_HOST" 2>/dev/null | awk '{print $1}' | head -1)
      TARGET_IP=$(podman exec "$CONTAINER_JM" getent hosts "$TARGET" 2>/dev/null | awk '{print $1}' | head -1)
      if [[ -n "$FLINK_S3_IP" && "$FLINK_S3_IP" == "$TARGET_IP" ]]; then
        ok "s3.endpoint de Flink ($FLINK_S3_ENDPOINT) resuelve a la misma IP ($FLINK_S3_IP) que el VIP/DNS esperado ($TARGET) — el nombre difiere (nombre corto vs FQDN) pero apunta al mismo destino"
      else
        fail "s3.endpoint de Flink ($FLINK_S3_ENDPOINT${FLINK_S3_IP:+ → $FLINK_S3_IP}) NO coincide con el VIP/DNS esperado ($TARGET${TARGET_IP:+ → $TARGET_IP}) — deriva de configuración confirmada, actualizar $(basename "$FLINK_CONF_FILE") para apuntar al VIP de HAProxy"
      fi
    fi
  elif echo "$FLINK_S3_ENDPOINT" | grep -qP 'pbigd-stg0\d'; then
    warn "s3.endpoint de Flink ($FLINK_S3_ENDPOINT) apunta a un nodo MinIO individual, no a un VIP — posible deriva de configuración respecto al VIP de HAProxy validado en validacion_haproxy_minio/. Pasar --vip/--dns para confirmar o descartar"
  else
    warn "s3.endpoint de Flink ($FLINK_S3_ENDPOINT) no coincide con el patrón de nodo pbigd-stg0N ni se pasó --vip/--dns — no se pudo clasificar como VIP o nodo individual; confirmar manualmente (valor detectado: $FLINK_S3_ENDPOINT)"
  fi

  # 1.6 path-style access habilitado
  PATH_STYLE=$(podman exec "$CONTAINER_JM" \
    grep -oP 's3\.path[.-]style[.-]access\s*:\s*\K\S+' \
    "$FLINK_CONF_FILE" 2>/dev/null || echo "")
  if [[ "$PATH_STYLE" == "true" ]]; then
    ok "path-style access habilitado explícitamente (s3.path.style.access: true)"
  elif [[ -n "$PATH_STYLE" ]]; then
    fail "s3.path.style.access está presente pero en '$PATH_STYLE' (se esperaba 'true') en $(basename "$FLINK_CONF_FILE")"
  else
    fail "No se encontró s3.path.style.access en $(basename "$FLINK_CONF_FILE") — requerido para acceso path-style a MinIO"
  fi

  # 1.7 Mecanismo de credenciales S3 (solo presencia, nunca el valor)
  CRED_KEYS=$(podman exec "$CONTAINER_JM" \
    grep -oP '^\s*s3\.(access-key|secret-key)\s*:' \
    "$FLINK_CONF_FILE" 2>/dev/null || echo "")
  if [[ -n "$CRED_KEYS" ]]; then
    ok "Credenciales S3 configuradas en $(basename "$FLINK_CONF_FILE") (s3.access-key/s3.secret-key presentes; valores no inspeccionados)"
    CONF_PERMS=$(podman exec "$CONTAINER_JM" stat -c "%a" "$FLINK_CONF_FILE" 2>/dev/null || echo "")
    if [[ -n "$CONF_PERMS" ]] && [[ "${CONF_PERMS: -1}" -ge 4 ]]; then
      warn "$(basename "$FLINK_CONF_FILE") tiene permisos $CONF_PERMS (legible por 'otros') y contiene credenciales en texto plano — considerar restringir permisos o migrar a Podman secrets"
    elif [[ -n "$CONF_PERMS" ]]; then
      ok "$(basename "$FLINK_CONF_FILE") no es legible por 'otros' (permisos $CONF_PERMS)"
    fi
  else
    fail "No se encontraron s3.access-key/s3.secret-key en $(basename "$FLINK_CONF_FILE") — confirmar mecanismo de credenciales usado por Flink hacia MinIO"
  fi
fi

# =============================================================================
# MÓDULO 2 — Plugin S3 y configuración en cada TaskManager (MANUAL, sin SSH)
# =============================================================================
section "MÓDULO 2 · Plugin S3 y configuración — TaskManagers (manual)"

TOTAL_TM=${#TASKMANAGER_HOSTS[@]}
if [[ -n "$TM_MANUAL_OK" ]]; then
  if ! [[ "$TM_MANUAL_OK" =~ ^[0-9]+$ ]]; then
    fail "--tm-manual-ok recibió un valor no numérico ('$TM_MANUAL_OK') — no se puede evaluar el Módulo 2"
  elif [[ "$TM_MANUAL_OK" -ge "$TOTAL_TM" ]]; then
    ok "TaskManagers confirmados manualmente por el operador: $TM_MANUAL_OK/$TOTAL_TM con plugin S3 + config OK"
  else
    fail "TaskManagers confirmados manualmente por el operador: $TM_MANUAL_OK/$TOTAL_TM — no alcanza el umbral Principal (3/3)"
  fi
else
  info "Sin SSH disponible entre el JobManager y los TaskManagers, este chequeo"
  info "no se puede automatizar desde aquí. Correr localmente en CADA TaskManager"
  info "(${TASKMANAGER_HOSTS[*]}) el contenedor correspondiente. El archivo de"
  info "config puede ser config.yaml (Flink 2.x) o flink-conf.yaml (Flink 1.x) —"
  info "el comando prueba ambos:"
  for i in "${!TASKMANAGER_HOSTS[@]}"; do
    info "    [${TASKMANAGER_HOSTS[$i]}]  podman exec ${TM_CONTAINER_NAMES[$i]} find /opt/flink/plugins -iname 'flink-s3-fs-*.jar'"
    info "    [${TASKMANAGER_HOSTS[$i]}]  podman exec ${TM_CONTAINER_NAMES[$i]} sh -c \"grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint' ${FLINK_HOME}/conf/config.yaml 2>/dev/null || grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint' ${FLINK_HOME}/conf/flink-conf.yaml\""
  done
  info "Los $TOTAL_TM TaskManagers deben mostrar el JAR del plugin y las 3 claves de configuración."
  info "Umbral Principal: se exige $TOTAL_TM/$TOTAL_TM TM con plugin+config OK."
  info "Una vez confirmado, volver a correr con --tm-manual-ok $TOTAL_TM para que quede reflejado en el resumen."
fi

# =============================================================================
# MÓDULO 3 — Integración funcional end-to-end (checkpoint real)
# =============================================================================
section "MÓDULO 3 · Integración funcional Flink → MinIO (checkpoint real)"

if $SKIP_FUNCTIONAL_TEST; then
  info "Módulo 3 omitido (--skip-functional-test)"
elif ! $JM_LOCAL; then
  warn "Módulo 3 omitido: contenedor JobManager no detectado localmente (ver Módulo 1)"
elif ! $PLUGIN_OK; then
  warn "Módulo 3 omitido: plugin S3 no confirmado en el JobManager (ver Módulo 1)"
else
  RUN_ID="flink-minio-it-$(date +%s)"
  SQL_SCRIPT_LOCAL="/tmp/${RUN_ID}.sql"
  SQL_SCRIPT_CONTAINER="/tmp/${RUN_ID}.sql"

  info "Generando job Flink SQL (fuente datagen → sink print, checkpointing habilitado)..."
  cat > "$SQL_SCRIPT_LOCAL" <<SQLEOF
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE datagen_source (
  id BIGINT,
  ts AS PROCTIME()
) WITH (
  'connector'          = 'datagen',
  'rows-per-second'    = '5'
);

CREATE TABLE print_sink (
  id BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO print_sink SELECT id FROM datagen_source;
SQLEOF

  if podman cp "$SQL_SCRIPT_LOCAL" "${CONTAINER_JM}:${SQL_SCRIPT_CONTAINER}" 2>/dev/null; then
    ok "Script SQL copiado al contenedor JobManager"

    info "Sometiendo job Flink SQL (background)..."
    nohup podman exec "$CONTAINER_JM" "${FLINK_HOME}/bin/sql-client.sh" -f "$SQL_SCRIPT_CONTAINER" \
      > "/tmp/${RUN_ID}_sqlclient.log" 2>&1 &

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

      info "Esperando checkpoint COMPLETED (hasta ${CHECKPOINT_WAIT_S}s, polling cada ${CHECKPOINT_POLL_INTERVAL_S}s)..."
      EXTERNAL_PATH=""
      ELAPSED=0
      while [[ "$ELAPSED" -lt "$CHECKPOINT_WAIT_S" ]]; do
        CKPT_JSON=$(curl -sf --max-time 10 "$JM_API/jobs/${JOB_ID}/checkpoints" 2>/dev/null || echo "")
        if [[ -n "$CKPT_JSON" ]] && command -v python3 &>/dev/null; then
          EXTERNAL_PATH=$(echo "$CKPT_JSON" | python3 -c \
            "import sys,json
d=json.load(sys.stdin)
completed=[c for c in d.get('history',[]) if c.get('status')=='COMPLETED']
print(completed[-1].get('external_path','') if completed else '')" 2>/dev/null || echo "")
        fi
        if [[ -n "$EXTERNAL_PATH" ]]; then
          break
        fi
        sleep "$CHECKPOINT_POLL_INTERVAL_S"
        ELAPSED=$((ELAPSED + CHECKPOINT_POLL_INTERVAL_S))
      done

      if [[ -n "$EXTERNAL_PATH" ]]; then
        ok "Checkpoint COMPLETED confirmado vía REST API — external_path: $EXTERNAL_PATH"
        ok "Integración Flink → MinIO confirmada: el checkpoint quedó persistido en el bucket '$CHECKPOINT_BUCKET'"
      else
        fail "No se detectó ningún checkpoint COMPLETED tras ${CHECKPOINT_WAIT_S}s — revisar GET $JM_API/jobs/${JOB_ID}/checkpoints y logs del JobManager"
      fi
    else
      fail "No se detectó ningún job RUNNING tras ${SQL_JOB_STARTUP_WAIT_S}s — ver /tmp/${RUN_ID}_sqlclient.log en $THIS_HOST"
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
log "${BOLD}║   RESUMEN – Integración Flink → MinIO · PRINCIPAL                ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Integración Flink → MinIO validada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  Se detectaron $FAIL fallos críticos en la integración — revisar log: $LOG_FILE${NC}"
fi

if [[ -z "$TM_MANUAL_OK" ]]; then
  log ""
  log "${CYAN}  Recordar completar el Módulo 2 (pasos manuales en cada TaskManager)${NC}"
  log "${CYAN}  antes de dar por validada la integración completa.${NC}"
fi
log ""

exit $FAIL
