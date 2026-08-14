#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Integración Trino → MinIO/AIStor (DC Principal)
#  Lectura/escritura directa de archivos de datos Iceberg (NO metadata)
#
#  Alcance: la conexión directa Trino → AIStor para archivos de datos Iceberg.
#  NO valida la conexión Trino → Gravitino (eso es validacion_trino_gravitino/,
#  aún no existe) ni la salud del clúster Trino en sí (eso es
#  validacion_trino/validate_trino_principal.sh, que debe correrse antes).
#
#  Por qué es un script separado de validacion_trino_gravitino/: un correo
#  interno del equipo de arquitectura (context/Contexto-extra-correos.md,
#  2026-08-06) confirma que "Gravitino solo maneja metadata (catálogo de
#  tablas), nunca los archivos de datos -por eso Flink y Trino tienen una
#  conexión hacia Gravitino (metadata) y otra directa hacia AIStor (archivos),
#  sin pasar por Gravitino." Trino tiene DOS conexiones independientes; este
#  script valida solo la segunda (datos), no la primera (metadata/catálogo).
#
#  Topología (DC Principal — compartida con validacion_trino/ y
#  validacion_flink_minio/, mismo endpoint MinIO que usa Flink):
#    Coordinator : pbigd-plat-apps01   (contenedor: trino-coordinator)
#    Workers     : pbigd-proc01 | pbigd-proc02 | pbigd-proc03
#    MinIO       : pbigd-stg01 (endpoint S3; bucket a confirmar — ver abajo)
#
#  DR: NO se genera validate_trino_minio_dr.sh en esta entrega. Se hereda la
#  conclusión ya documentada en validacion_trino/validate_trino_principal.sh:
#  Trino no figura en la tabla de asignación de recursos del datacenter
#  alterno del plan de implementación, y su presencia en DR está documentada
#  como "No aplica en DR por diseño" pendiente de confirmación en sitio
#  (podman ps | grep -i trino en los 3 nodos -cont). Si Trino no está
#  confirmado en DR, su integración con MinIO tampoco aplica ahí — no
#  investigar esto de nuevo, ver el script citado. Ver también
#  preparacion_integracion_trino_minio.md.
#
#  Catálogo Iceberg de Trino — NO documentado en el repo (ni
#  validacion_gravitino/ ni el plan de implementación citan el nombre del
#  catálogo, la ruta del .properties, ni si el conector S3 en uso es el
#  legacy basado en Hive (hive.s3.*) o el filesystem nativo S3 de Trino 481
#  (fs.native-s3.* / s3.*). Este script lo DESCUBRE en runtime (Módulo 1.0):
#  no asume ninguno de antemano.
#
#  Requisitos previos (ver preparacion_integracion_trino_minio.md):
#    - validacion_trino/validate_trino_principal.sh ya corrido (clúster Trino
#      sano) y validacion_minio/validate_minio_principal.sh ya corrido (MinIO
#      sano).
#    - Ejecutar desde el nodo Coordinator (pbigd-plat-apps01) o pasar
#      --coordinator-host.
#    - python3 disponible (parseo de JSON de la REST API de Trino).
#
#  NO usa SSH: no existe acceso SSH sin contraseña del usuario admapl entre
#  ningún par de nodos del entorno (mismo criterio que el resto de la familia
#  de scripts de integración). El Módulo 1 (config del catálogo) solo se
#  verifica automáticamente en el Coordinator, que es donde vive la config de
#  catálogos consultada por los workers.
#
#  --vip/--dns: mismo mecanismo que validate_flink_minio_principal.sh. El VIP
#  DNS de HAProxy delante de MinIO (itaca.jardinazuayo.fin.ec) ya está
#  confirmado como real en Principal (resuelve a 172.17.210.62, ver
#  logs/haproxy_minio/) pero NO se hardcodea aquí como default silencioso —
#  hay que pasarlo explícitamente para que el Módulo 1.2 lo use como valor
#  esperado. Sin estos flags, el script sigue validando contra el nodo
#  individual pbigd-stg01, con WARN explícito.
#
#  Fuera de alcance: contenido/permisos del catálogo REST de Gravitino (ver
#  validacion_trino_gravitino/, aún no existe), salud del clúster Trino en sí
#  (ver validacion_trino/), DR (ver arriba).
#
#  Uso:
#    chmod +x validate_trino_minio_principal.sh
#    ./validate_trino_minio_principal.sh \
#        [--coordinator-host host] [--vip IP] [--dns nombre] \
#        [--minio-endpoint url] [--test-table catalogo.schema.tabla] \
#        [--expected-count N] [--skip-write-test] [--write-test-schema catalogo.schema]
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
COORDINATOR_HOST="${COORDINATOR_HOST:-pbigd-plat-apps01}"
COORDINATOR_PORT=8080
TRINO_HOME="/opt/trino"
CONTAINER_COORDINATOR="trino"
TRINO_USER="admapl"   # usuario enviado en X-Trino-User, igual que validate_trino_principal.sh

MINIO_NODE_DEFAULT="pbigd-stg01"
MINIO_PORT=9000
MINIO_ENDPOINT="http://${MINIO_NODE_DEFAULT}:${MINIO_PORT}"
MINIO_ENDPOINT_EXPLICIT=false

# VIP/DNS esperado del HAProxy delante de MinIO — sin valor por defecto, igual
# criterio que validate_flink_minio_principal.sh y
# validate_haproxy_minio_principal.sh.
VIP=""
VIP_DNS=""

# Rutas candidatas donde puede vivir la config de catálogos de Trino —
# ninguna confirmada en el repo, se prueban ambas (Módulo 1.0).
CATALOG_DIR_CANDIDATES=("${TRINO_HOME}/etc/catalog" "/etc/trino/catalog")

# Tabla Iceberg real para la prueba de lectura (Módulo 2) — no hay ninguna
# confirmada en el repo (creada por Flink u otro proceso), debe pasarse
# explícitamente. Formato: catalogo.schema.tabla
TEST_TABLE=""
EXPECTED_COUNT=""

SKIP_WRITE_TEST=false
# Catálogo.schema donde crear la tabla de prueba de escritura (Módulo 3). Si
# no se pasa, se reutiliza el schema de --test-table cuando esté disponible.
WRITE_TEST_SCHEMA=""
WRITE_TEST_TABLE_NAME="bluepoint_validacion_trino_test"

STATEMENT_MAX_WAIT_S=30
STATEMENT_POLL_INTERVAL_S=2

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_trino_minio_principal_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# Ejecuta una sentencia SQL contra Trino vía REST API (/v1/statement),
# siguiendo la cadena de "nextUri" hasta que no queda más (igual protocolo que
# usa trino-cli). Devuelve por stdout el JSON del último estado, y dentro de
# ese JSON pueden venir columns/data si la sentencia produjo resultados.
# Uso: RESULT_JSON=$(run_statement "SELECT 1")
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

# ---------------------------------------------------------------------------
# Parseo de argumentos
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --coordinator-host)   COORDINATOR_HOST="$2"; shift 2 ;;
    --vip)                VIP="$2"; shift 2 ;;
    --dns)                VIP_DNS="$2"; shift 2 ;;
    --minio-endpoint)     MINIO_ENDPOINT="$2"; MINIO_ENDPOINT_EXPLICIT=true; shift 2 ;;
    --test-table)         TEST_TABLE="$2"; shift 2 ;;
    --expected-count)     EXPECTED_COUNT="$2"; shift 2 ;;
    --skip-write-test)    SKIP_WRITE_TEST=true; shift ;;
    --write-test-schema)  WRITE_TEST_SCHEMA="$2"; shift 2 ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

if [[ "$MINIO_ENDPOINT_EXPLICIT" == false && ( -n "$VIP" || -n "$VIP_DNS" ) ]]; then
  MINIO_ENDPOINT="http://${VIP_DNS:-$VIP}:${MINIO_PORT}"
fi

if [[ -z "$WRITE_TEST_SCHEMA" && -n "$TEST_TABLE" ]]; then
  # catalogo.schema.tabla -> catalogo.schema
  WRITE_TEST_SCHEMA=$(echo "$TEST_TABLE" | awk -F. '{print $1"."$2}')
fi

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
COORDINATOR_API="http://${COORDINATOR_HOST}:${COORDINATOR_PORT}"
IS_COORDINATOR=false
[[ "$THIS_HOST" == *"plat-apps"* || "$THIS_HOST" == "$COORDINATOR_HOST" ]] && IS_COORDINATOR=true

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Integración Trino → MinIO/AIStor — PRINCIPAL       ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Nodo de ejecución  : $THIS_HOST"
info "Coordinator Trino   : $COORDINATOR_HOST ($COORDINATOR_API)"
info "Endpoint MinIO      : $MINIO_ENDPOINT"
if [[ -n "$VIP" || -n "$VIP_DNS" ]]; then
  info "VIP/DNS esperado    : ${VIP_DNS:-$VIP}"
else
  warn "No se pasó --vip ni --dns: este script está validando contra un endpoint de UN SOLO NODO ($MINIO_ENDPOINT), no contra la VIP/DNS de alta disponibilidad de HAProxy. Pasar --vip <IP> o --dns <nombre> para validar contra la VIP real (ya confirmada en Principal para Flink: itaca.jardinazuayo.fin.ec)."
fi
log ""
log "${CYAN}  Nota de alcance: este script asume que validate_trino_principal.sh y${NC}"
log "${CYAN}  validate_minio_principal.sh ya se ejecutaron. Aquí solo se valida la${NC}"
log "${CYAN}  conexión directa Trino → AIStor para archivos de datos Iceberg — NO la${NC}"
log "${CYAN}  conexión Trino → Gravitino (metadata), que es un script separado.${NC}"
log ""

# =============================================================================
# MÓDULO 1 — Conectividad base S3/MinIO desde el catálogo de Trino
# =============================================================================
section "MÓDULO 1 · Conectividad base S3/MinIO — Coordinator"

# 1.0 MinIO alcanzable (igual patrón que validate_flink_minio_principal.sh —
# sin -f: con -f, un 4xx/5xx puede llegar concatenado al "|| echo 000" de
# abajo en vez de reemplazarlo).
MINIO_HTTP=$(curl -s --max-time 5 -o /dev/null -w "%{http_code}" \
  "${MINIO_ENDPOINT}/minio/health/live" 2>/dev/null)
MINIO_HTTP="${MINIO_HTTP:-000}"
if [[ "$MINIO_HTTP" == "200" ]]; then
  ok "MinIO alcanzable desde $THIS_HOST: HTTP $MINIO_HTTP"
else
  fail "MinIO no responde en $MINIO_ENDPOINT (HTTP $MINIO_HTTP) — los Módulos 2/3 no podrán completar pruebas funcionales"
fi

# Detectar contenedor Coordinator local (la config del catálogo vive ahí)
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
S3_KEY_PREFIX=""   # prefijo de clave realmente usado en el .properties encontrado: "s3", "hive.s3" o "fs.native-s3"

if $CRD_LOCAL; then
  # 1.0 Descubrimiento de catálogo — ni el nombre ni la ruta del .properties
  # están confirmados en el repo (ver cabecera del script). Se prueban las
  # rutas candidatas conocidas de Trino y se reporta cuál existe.
  for CAND_DIR in "${CATALOG_DIR_CANDIDATES[@]}"; do
    FOUND=$(podman exec "$CONTAINER_COORDINATOR" sh -c "find '$CAND_DIR' -maxdepth 1 -iname '*.properties' 2>/dev/null" || echo "")
    if [[ -n "$FOUND" ]]; then
      info "Directorio de catálogos detectado: $CAND_DIR"
      info "Archivos .properties encontrados:"
      while IFS= read -r F; do info "    $F"; done <<< "$FOUND"
      # De entre los .properties encontrados, quedarse con el primero cuyo
      # connector.name sea iceberg (o, si ninguno lo es, reportarlo como WARN
      # en vez de asumir uno).
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
  else
    fail "No se encontró ningún catálogo con connector.name=iceberg en ${CATALOG_DIR_CANDIDATES[*]} — no se puede validar la config S3 sin saber qué archivo leer"
  fi
fi

# 1.1 fs.native-s3.enabled / conector S3 habilitado explícitamente
if [[ -n "$CATALOG_PROPS_FILE" ]]; then
  NATIVE_S3_LINE=$(podman exec "$CONTAINER_COORDINATOR" grep -E "fs\.native-s3\.enabled|fs\.hadoop\.enabled|hive\.s3\.enabled" "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
  if [[ -n "$NATIVE_S3_LINE" ]]; then
    ok "Filesystem S3 habilitado explícitamente en '$CATALOG_NAME': $NATIVE_S3_LINE"
    info "$NATIVE_S3_LINE"
  else
    warn "No se encontró fs.native-s3.enabled/fs.hadoop.enabled/hive.s3.enabled en $(basename "$CATALOG_PROPS_FILE") — confirmar manualmente qué filesystem S3 usa esta versión de Trino (481)"
  fi

  # Detectar qué prefijo de clave usa realmente este catálogo, probando las
  # 3 familias conocidas (nativo S3, Hadoop/Hive S3 legacy, genérico s3.*) —
  # no asumir de antemano cuál aplica a Trino 481 en este entorno.
  for PREFIX in "fs\.native-s3" "hive\.s3" "s3"; do
    if podman exec "$CONTAINER_COORDINATOR" grep -qP "^\s*${PREFIX}\.endpoint\s*=" "$CATALOG_PROPS_FILE" 2>/dev/null; then
      S3_KEY_PREFIX="${PREFIX//\\/}"
      break
    fi
  done
  if [[ -n "$S3_KEY_PREFIX" ]]; then
    info "Prefijo de claves S3 detectado en '$CATALOG_NAME': ${S3_KEY_PREFIX}.*"
  else
    warn "No se pudo determinar el prefijo de claves S3 (fs.native-s3.* / hive.s3.* / s3.*) en $(basename "$CATALOG_PROPS_FILE") — los checks 1.2/1.3/1.4 no podrán extraer valores"
  fi
fi

# 1.2 [PRIORITARIO] Endpoint S3 del catálogo Trino vs. VIP/DNS de HAProxy —
# misma lógica que validate_flink_minio_principal.sh Módulo 1.5, portada
# verbatim (comparación de texto, luego resolución de IP vía getent hosts
# dentro del contenedor para tolerar nombre corto vs FQDN).
if [[ -n "$CATALOG_PROPS_FILE" && -n "$S3_KEY_PREFIX" ]]; then
  TRINO_S3_ENDPOINT=$(podman exec "$CONTAINER_COORDINATOR" \
    grep -oP "^\s*${S3_KEY_PREFIX}\.endpoint\s*=\s*\K\S+" \
    "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
  if [[ -z "$TRINO_S3_ENDPOINT" ]]; then
    warn "No se pudo extraer ${S3_KEY_PREFIX}.endpoint de $(basename "$CATALOG_PROPS_FILE") — no se puede confirmar si Trino apunta al VIP o a un nodo individual"
  elif [[ -n "$VIP" || -n "$VIP_DNS" ]]; then
    TARGET="${VIP_DNS:-$VIP}"
    if echo "$TRINO_S3_ENDPOINT" | grep -qF "$TARGET"; then
      ok "${S3_KEY_PREFIX}.endpoint de Trino ($TRINO_S3_ENDPOINT) coincide con el VIP/DNS esperado ($TARGET)"
    else
      TRINO_S3_HOST=$(echo "$TRINO_S3_ENDPOINT" | sed -E 's#^[a-zA-Z]+://##; s#:[0-9]+$##; s#/.*$##')
      TRINO_S3_IP=$(podman exec "$CONTAINER_COORDINATOR" getent hosts "$TRINO_S3_HOST" 2>/dev/null | awk '{print $1}' | head -1)
      TARGET_IP=$(podman exec "$CONTAINER_COORDINATOR" getent hosts "$TARGET" 2>/dev/null | awk '{print $1}' | head -1)
      if [[ -n "$TRINO_S3_IP" && "$TRINO_S3_IP" == "$TARGET_IP" ]]; then
        ok "${S3_KEY_PREFIX}.endpoint de Trino ($TRINO_S3_ENDPOINT) resuelve a la misma IP ($TRINO_S3_IP) que el VIP/DNS esperado ($TARGET) — el nombre difiere (nombre corto vs FQDN) pero apunta al mismo destino"
      else
        fail "${S3_KEY_PREFIX}.endpoint de Trino ($TRINO_S3_ENDPOINT${TRINO_S3_IP:+ → $TRINO_S3_IP}) NO coincide con el VIP/DNS esperado ($TARGET${TARGET_IP:+ → $TARGET_IP}) — deriva de configuración confirmada, actualizar $(basename "$CATALOG_PROPS_FILE") para apuntar al VIP de HAProxy"
      fi
    fi
  elif echo "$TRINO_S3_ENDPOINT" | grep -qP 'pbigd-stg0\d'; then
    warn "${S3_KEY_PREFIX}.endpoint de Trino ($TRINO_S3_ENDPOINT) apunta a un nodo MinIO individual, no a un VIP — posible deriva respecto al VIP de HAProxy validado en validacion_haproxy_minio/. Pasar --vip/--dns para confirmar o descartar"
  else
    warn "${S3_KEY_PREFIX}.endpoint de Trino ($TRINO_S3_ENDPOINT) no coincide con el patrón de nodo pbigd-stg0N ni se pasó --vip/--dns — no se pudo clasificar como VIP o nodo individual; confirmar manualmente"
  fi

  # Cruce explícito con Flink: ambos comparten el mismo endpoint MinIO según
  # el correo del 2026-08-01 ("los 4 componentes deben apuntar exactamente al
  # mismo endpoint"). Este script no lee la config de Flink directamente
  # (responsabilidad de validate_flink_minio_principal.sh); se deja como nota
  # explícita para que el operador cruce manualmente ambos logs.
  info "Cruzar este valor (${S3_KEY_PREFIX}.endpoint=$TRINO_S3_ENDPOINT) contra el s3.endpoint reportado por validate_flink_minio_principal.sh — deben coincidir exactamente según el correo de arquitectura del 2026-08-01"
fi

# 1.3 path-style access habilitado explícitamente — regex tolerante a
# separador '.' o '-' en el nombre de la clave, igual idioma que Flink.
if [[ -n "$CATALOG_PROPS_FILE" && -n "$S3_KEY_PREFIX" ]]; then
  PATH_STYLE=$(podman exec "$CONTAINER_COORDINATOR" \
    grep -oP "^\s*${S3_KEY_PREFIX}\.path[.-]style[.-]access\s*=\s*\K\S+" \
    "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
  if [[ "$PATH_STYLE" == "true" ]]; then
    ok "path-style access habilitado explícitamente (${S3_KEY_PREFIX}.path-style-access: true)"
  elif [[ -n "$PATH_STYLE" ]]; then
    fail "${S3_KEY_PREFIX}.path-style-access está presente pero en '$PATH_STYLE' (se esperaba 'true') en $(basename "$CATALOG_PROPS_FILE")"
  else
    fail "No se encontró ${S3_KEY_PREFIX}.path-style-access en $(basename "$CATALOG_PROPS_FILE") — requerido para acceso path-style a MinIO"
  fi
fi

# 1.4 Credenciales S3: solo presencia, nunca el valor (sin \K sobre el
# secreto) + permisos del archivo — igual patrón que Flink Módulo 1.7.
if [[ -n "$CATALOG_PROPS_FILE" && -n "$S3_KEY_PREFIX" ]]; then
  CRED_KEYS=$(podman exec "$CONTAINER_COORDINATOR" \
    grep -oP "^\s*${S3_KEY_PREFIX}\.(aws-access-key|aws-secret-key|access-key|secret-key)\s*=" \
    "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
  if [[ -n "$CRED_KEYS" ]]; then
    ok "Credenciales S3 configuradas en $(basename "$CATALOG_PROPS_FILE") (claves presentes; valores no inspeccionados)"
    CONF_PERMS=$(podman exec "$CONTAINER_COORDINATOR" stat -c "%a" "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
    if [[ -n "$CONF_PERMS" ]] && [[ "${CONF_PERMS: -1}" -ge 4 ]]; then
      warn "$(basename "$CATALOG_PROPS_FILE") tiene permisos $CONF_PERMS (legible por 'otros') y contiene credenciales en texto plano — considerar restringir permisos o migrar a Podman secrets"
    elif [[ -n "$CONF_PERMS" ]]; then
      ok "$(basename "$CATALOG_PROPS_FILE") no es legible por 'otros' (permisos $CONF_PERMS)"
    fi
  else
    warn "No se encontraron claves de credenciales S3 explícitas (${S3_KEY_PREFIX}.access-key/secret-key) en $(basename "$CATALOG_PROPS_FILE") — puede usar un mecanismo de credenciales basado en IAM/rol en vez de claves estáticas; confirmar manualmente el mecanismo real, no asumir que faltan"
  fi
fi

# 1.5 [PRIORITARIO] Si el catálogo Iceberg de Trino es tipo REST, la premisa
# de este script (conexión directa Trino→AIStor, independiente de Gravitino)
# NO se cumple: Trino no tiene ninguna config S3 propia por diseño, delega
# toda la resolución de archivos al servidor REST. Ese es el resultado de
# este chequeo — un FAIL, porque el objeto que este script debía validar (la
# conexión directa) no existe tal como está configurado el entorno.
#
# Deliberadamente NO se investiga aquí si el servidor REST (Gravitino) llega
# a su vez a AIStor — eso es la config de Gravitino, fuera de alcance de este
# script por su propio encabezado ("NO valida la conexión Trino → Gravitino,
# eso es validacion_trino_gravitino/"). Ver ese script (Módulo 1.5) para la
# confirmación del warehouse real.
CATALOG_TYPE=""
if [[ -n "$CATALOG_PROPS_FILE" ]]; then
  CATALOG_TYPE=$(podman exec "$CONTAINER_COORDINATOR" grep -oP '^\s*iceberg\.catalog\.type\s*=\s*\K\S+' "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
fi

if [[ "$CATALOG_TYPE" == "rest" ]]; then
  fail "El catálogo Iceberg de Trino ('$CATALOG_NAME') es tipo REST (iceberg.catalog.type=rest) — no tiene ninguna config S3/AIStor propia (consistente con los WARN 1.1/1.2 de arriba). La premisa de una conexión directa Trino→AIStor independiente de Gravitino NO se cumple en este entorno: toda la resolución de archivos pasa por el servidor REST. Este script no puede validar una conexión directa que no existe — la integración real (Trino→Gravitino→AIStor) se valida en validacion_trino_gravitino/, no aquí."
else
  info "iceberg.catalog.type no es 'rest' (o no se pudo determinar) — la premisa de conexión directa Trino→AIStor sigue siendo aplicable; ver checks 1.1-1.4 arriba"
fi

# =============================================================================
# MÓDULO 2 — Validación funcional: lectura
# =============================================================================
section "MÓDULO 2 · Validación funcional — lectura de tabla Iceberg"

if [[ -z "$TEST_TABLE" ]]; then
  warn "Módulo 2 omitido: no se pasó --test-table catalogo.schema.tabla — no hay ninguna tabla Iceberg de referencia confirmada en el repo para probar lectura sin asumir una"
else
  info "Tabla de prueba: $TEST_TABLE"
  READ_RESULT=$(run_statement "SELECT count(*) FROM ${TEST_TABLE}")
  if [[ -z "$READ_RESULT" ]]; then
    fail "SELECT contra $TEST_TABLE no obtuvo respuesta del coordinator (timeout o error de red)"
  else
    ROW_COUNT=$(echo "$READ_RESULT" | python3 -c \
      "import sys,json
d=json.load(sys.stdin)
data=d.get('data',[])
print(data[0][0] if data else '')" 2>/dev/null || echo "")
    ERR_MSG=$(echo "$READ_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','') if d.get('error') else '')" 2>/dev/null || echo "")
    if [[ -n "$ERR_MSG" ]]; then
      fail "SELECT contra $TEST_TABLE falló: $ERR_MSG"
    elif [[ -z "$ROW_COUNT" ]]; then
      warn "No se pudo extraer el conteo de filas de la respuesta de Trino — confirmar manualmente con: trino --server $COORDINATOR_API --execute 'SELECT count(*) FROM ${TEST_TABLE}'"
    elif [[ "$ROW_COUNT" -gt 0 ]] 2>/dev/null; then
      ok "Trino leyó $TEST_TABLE correctamente vía S3/MinIO: $ROW_COUNT filas"
      if [[ -n "$EXPECTED_COUNT" ]]; then
        if [[ "$ROW_COUNT" == "$EXPECTED_COUNT" ]]; then
          ok "Conteo de filas coincide con el valor esperado ($EXPECTED_COUNT)"
        else
          fail "Conteo de filas ($ROW_COUNT) NO coincide con el valor esperado (--expected-count $EXPECTED_COUNT)"
        fi
      fi
    else
      fail "SELECT contra $TEST_TABLE devolvió 0 filas — la tabla existe en el catálogo pero Trino no está leyendo datos reales de MinIO (revisar si los archivos Iceberg realmente están en el bucket, o si el problema es de metadata en Gravitino, no de S3)"
    fi
  fi
fi

# =============================================================================
# MÓDULO 3 — Validación funcional: escritura (condicional)
# =============================================================================
section "MÓDULO 3 · Validación funcional — escritura (condicional)"

if $SKIP_WRITE_TEST; then
  info "Módulo 3 omitido (--skip-write-test)"
elif [[ -z "$WRITE_TEST_SCHEMA" ]]; then
  warn "Módulo 3 omitido: no se pasó --write-test-schema (ni se pudo derivar de --test-table) — no hay un catalogo.schema confirmado donde probar CTAS sin asumir uno"
else
  # 3.1 No asumir que Trino es solo lectura pese a que el plan lo describe
  # como "acceso ocasional de Power BI" — probar con un CTAS real. Si el
  # catálogo/schema es de solo lectura, Trino devuelve un error explícito de
  # permisos, no un fallo de conectividad — distinguimos ambos casos abajo.
  FULL_TEST_TABLE="${WRITE_TEST_SCHEMA}.${WRITE_TEST_TABLE_NAME}"
  info "Probando escritura con CTAS de prueba: $FULL_TEST_TABLE"
  CTAS_RESULT=$(run_statement "CREATE TABLE ${FULL_TEST_TABLE} AS SELECT 1 AS id, 'bluepoint_validacion' AS origen")
  CTAS_ERR=$(echo "$CTAS_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','') if d.get('error') else '')" 2>/dev/null || echo "")

  if [[ -z "$CTAS_RESULT" ]]; then
    warn "No se pudo confirmar el resultado del CTAS de prueba (timeout o error de red) — no se puede determinar si Trino tiene escritura habilitada"
  elif [[ -n "$CTAS_ERR" ]] && echo "$CTAS_ERR" | grep -qiE "permission|denied|read.only|not.*allowed|access.*denied"; then
    info "CREATE TABLE rechazado por permisos ('$CTAS_ERR') — catálogo confirmado como solo lectura, consistente con el uso documentado (acceso ocasional de Power BI). No es un FAIL, es diseño esperado."
  elif [[ -n "$CTAS_ERR" ]]; then
    fail "CREATE TABLE falló con un error que NO parece ser de permisos: '$CTAS_ERR' — revisar si es un problema real de conectividad S3/credenciales, no asumir que es 'solo lectura por diseño'"
  else
    ok "CREATE TABLE AS SELECT aceptado — el catálogo '${WRITE_TEST_SCHEMA%%.*}' tiene escritura habilitada"

    # 3.2 Confirmar que el archivo realmente llegó a MinIO — mismo truco HTTP
    # que Flink Módulo 1.2 (curl GET/HEAD sobre el bucket, sin depender de mc).
    # El nombre exacto del bucket/prefijo del warehouse Iceberg no está
    # confirmado en el repo; se reporta como manual si no se puede derivar.
    if [[ -n "$CATALOG_PROPS_FILE" ]]; then
      WAREHOUSE_LOCATION=$(podman exec "$CONTAINER_COORDINATOR" grep -oP '^\s*iceberg\.catalog\.warehouse\s*=\s*\K\S+' "$CATALOG_PROPS_FILE" 2>/dev/null || echo "")
      if [[ -n "$WAREHOUSE_LOCATION" ]]; then
        BUCKET_NAME=$(echo "$WAREHOUSE_LOCATION" | sed -E 's#^s3a?://##; s#/.*$##')
        BUCKET_HTTP=$(curl -s --max-time 5 -o /dev/null -w "%{http_code}" "${MINIO_ENDPOINT}/${BUCKET_NAME}" 2>/dev/null)
        BUCKET_HTTP="${BUCKET_HTTP:-000}"
        if [[ "$BUCKET_HTTP" =~ ^(200|403)$ ]]; then
          ok "Bucket de warehouse Iceberg '$BUCKET_NAME' responde en MinIO tras el CTAS (HTTP $BUCKET_HTTP)"
        else
          warn "No se pudo confirmar el bucket de warehouse '$BUCKET_NAME' vía HTTP (código $BUCKET_HTTP) — confirmar manualmente con 'mc ls' que los archivos del CTAS llegaron a MinIO"
        fi
      else
        warn "No se encontró iceberg.catalog.warehouse en $(basename "$CATALOG_PROPS_FILE") — no se pudo derivar el bucket para confirmar los archivos vía HTTP; confirmar manualmente con 'mc ls'"
      fi
    fi

    # Limpieza — siempre, incluso si la confirmación en MinIO fue un WARN.
    info "Limpiando tabla de prueba..."
    DROP_RESULT=$(run_statement "DROP TABLE IF EXISTS ${FULL_TEST_TABLE}")
    DROP_ERR=$(echo "$DROP_RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('error',{}).get('message','') if d.get('error') else '')" 2>/dev/null || echo "")
    if [[ -z "$DROP_ERR" ]]; then
      ok "Tabla de prueba $FULL_TEST_TABLE eliminada correctamente"
    else
      warn "No se pudo confirmar el DROP TABLE de $FULL_TEST_TABLE ('$DROP_ERR') — eliminar manualmente para no dejar residuos de la validación"
    fi
  fi
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Integración Trino → MinIO/AIStor · PRINCIPAL         ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Integración Trino → MinIO/AIStor validada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  Se detectaron $FAIL fallos críticos en la integración — revisar log: $LOG_FILE${NC}"
fi

if [[ -z "$TEST_TABLE" ]]; then
  log ""
  log "${CYAN}  Recordar volver a correr con --test-table catalogo.schema.tabla${NC}"
  log "${CYAN}  (idealmente una tabla ya escrita por Flink) antes de dar por validada${NC}"
  log "${CYAN}  la lectura funcional.${NC}"
fi
log ""

exit $FAIL
