#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Agentes de Observabilidad: Grafana Alloy 1.6.0 + node-exporter
# SCRIPT UNIVERSAL – Ejecutar en TODOS los nodos de DC Principal y DC Alterno
#
# Nodos DC Principal (todos deben tener Alloy + node-exporter):
#   pbigd-kaf01/02/03            → Kafka
#   pbigd-stg01/02/03            → MinIO Storage
#   pbigd-dlh01/02/03            → PostgreSQL DW HA
#   pbigd-proc01/02/03           → Flink Compute
#   pbigd-plat-apps01            → Flink JM + Redis + Trino Coordinator
#   pbigd-bd-plat-apps01         → Postgres meta + Redis Replica
#   pbigd-plat-obs01             → Stack observabilidad
#
# Nodos DC Alterno (todos deben tener Alloy, sufijo -cont):
#   pbigd-kaf01/02/03-cont, pbigd-stg01/02/03-cont
#   pbigd-dlh01/02/03-cont, pbigd-proc01/02-cont
#   pbigd-plat-apps01-cont, pbigd-bd-plat-apps01-cont, pbigd-plat-obs01-cont
#
# Componentes obligatorios (mínimos indispensables del proyecto):
#   · Grafana Alloy 1.6.0    → agente unificado (logs + métricas hacia
#                              pbigd-plat-obs01 en Principal, o
#                              pbigd-plat-obs01-cont si el nodo es DR)
#   · node-exporter          → métricas de sistema (CPU, RAM, disco, red);
#                              standalone en :9100 o embebido en Alloy
#                              (prometheus.exporter.unix) en :17935
#
# Runtime: Rocky Linux 10.x · Podman 5.x · systemd
#
# Uso:
#   chmod +x validate_obs_agents.sh
#   ./validate_obs_agents.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
ALLOY_VERSION_EXPECTED="1.6.0"
NODE_EXPORTER_PORT=9100
NODE_EXPORTER_EMBEDDED_PORT=17935   # exporter unix embebido en Alloy (prometheus.exporter.unix)
# ALLOY_HTTP_PORT: 12345 es el default de Alloy, pero este despliegue lo
# sobreescribe vía CUSTOM_ARGS="--server.http.listen-addr=0.0.0.0:17935" en
# /etc/sysconfig/alloy (confirmado en campo el 2026-07-14) — es el MISMO
# puerto que NODE_EXPORTER_EMBEDDED_PORT, porque Alloy solo corre un único
# servidor HTTP (self-metrics, UI y API de componentes conviven en él).
ALLOY_HTTP_PORT=17935        # puerto HTTP real de Alloy (métricas propias + UI + API de componentes)
ALLOY_GRPC_PORT=4317         # OpenTelemetry gRPC (OTLP)

# Rutas reales confirmadas en campo el 2026-07-14 en pbigd-plat-apps01, a partir
# de /etc/sysconfig/alloy (CONFIG_FILE) y de `systemctl cat alloy` (--storage.path
# en ExecStart). Antes apuntaban a las rutas de plan (/opt/alloy, /data/alloy,
# /var/log/alloy), que nunca existieron en el despliegue real — de ahí los WARN.
# Alloy no escribe a un directorio de logs propio: su log va a journald
# (ver check en MÓDULO 4 más abajo), por eso no hay ALLOY_LOG_DIR.
ALLOY_CONFIG_DIR="/etc/alloy"
ALLOY_DATA_DIR="/var/lib/alloy/data"

# Destino del stack de observabilidad (donde Alloy envía métricas/logs).
# Los nodos DR (hostname con sufijo -cont) reportan al Prometheus LOCAL
# de pbigd-plat-obs01-cont, no al pbigd-plat-obs01 de Principal.
#
# NOTA: "pbigd-plat-obs01" es la etiqueta del plan (hostnames.txt), pero el
# nombre real registrado en el DNS interno de la Cooperativa (jardinazuayo.fin.ec)
# es "escila" — confirmado en campo el 2026-07-14 (IP 172.17.210.89). Se usa
# el FQDN real aquí porque el hostname de plan nunca fue dado de alta en DNS.
# Pendiente: confirmar el nombre real del nodo DR (pbigd-plat-obs01-cont) antes
# de reemplazarlo aquí también — mientras tanto puede sobreescribirse al vuelo
# con la variable de entorno OBS_HOST.
if [[ -z "${OBS_HOST:-}" ]]; then
  if [[ "$(hostname -s 2>/dev/null || hostname)" == *-cont ]]; then
    OBS_HOST="pbigd-plat-obs01-cont"   # TODO: confirmar nombre DNS real (ver nota arriba)
  else
    OBS_HOST="escila.jardinazuayo.fin.ec"
  fi
fi
PROMETHEUS_PORT=9090
LOKI_PORT=3100

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
THIS_HOST=$(hostname -s 2>/dev/null || hostname)
LOG_FILE="/tmp/validate_obs_agents_${THIS_HOST}_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() {
  log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  log "${BOLD}${CYAN}  $*${NC}"
  log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Agentes Observabilidad (UNIVERSAL)     ║${NC}"
log "${BOLD}║   Alloy 1.6.0 + node-exporter  ·  Todos los nodos              ║${NC}"
log "${BOLD}║   Nodo   : $(printf '%-55s' "$THIS_HOST")║${NC}"
log "${BOLD}║   Fecha  : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
info "Este script es mandatorio en TODOS los nodos de la plataforma."
info "Alloy + node-exporter son los componentes mínimos indispensables de observabilidad."

# ===========================================================================
# MÓDULO 1 – OS y prerequisitos
# ===========================================================================
section "MÓDULO 1 · OS y prerequisitos del nodo"

OS=$(grep -oP '(?<=Rocky Linux )\S+' /etc/os-release 2>/dev/null || echo "unknown")
[[ "$OS" == 10* ]] \
  && ok "Rocky Linux 10.x confirmado" \
  || warn "Versión OS: $OS (esperado Rocky Linux 10.x)"

command -v podman &>/dev/null \
  && ok "Podman disponible: v$(podman --version | awk '{print $3}')" \
  || warn "Podman no encontrado (requerido para contenedores)"

# NTP — crítico para correlación de métricas/logs
if timedatectl status 2>/dev/null | grep -q "synchronized: yes"; then
  ok "NTP sincronizado (timedatectl)"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  ok "NTP sincronizado (chrony)"
else
  fail "NTP NO sincronizado — correlación de logs/métricas será incorrecta"
fi

# Verificar hostname resolución (Alloy necesita identificarse correctamente)
HOSTNAME_FQDN=$(hostname -f 2>/dev/null || echo "unknown")
info "Hostname FQDN: $HOSTNAME_FQDN"
[[ "$HOSTNAME_FQDN" != "unknown" && "$HOSTNAME_FQDN" != "localhost" ]] \
  && ok "Hostname resoluble: $HOSTNAME_FQDN" \
  || warn "Hostname no resoluble — las métricas pueden no identificar correctamente el nodo"

# ===========================================================================
# MÓDULO 2 – node-exporter (standalone :9100 o embebido en Alloy :17935)
# ===========================================================================
section "MÓDULO 2 · node-exporter"

# 2.1 Detectar modalidad: embebido en Alloy (prometheus.exporter.unix) tiene
# prioridad, ya que es la forma de despliegue real del proyecto; standalone
# es una alternativa igualmente válida.
NE_PORT=""
NE_MODE=""
if ss -tlnp | grep -q ":${NODE_EXPORTER_EMBEDDED_PORT}"; then
  NE_PORT="$NODE_EXPORTER_EMBEDDED_PORT"
  NE_MODE="embebido en Alloy"
  ok "node-exporter embebido en Alloy activo (puerto $NE_PORT)"
elif pgrep -x "node_exporter" &>/dev/null; then
  NE_PORT="$NODE_EXPORTER_PORT"
  NE_MODE="standalone"
  ok "Proceso node_exporter standalone corriendo"
  NE_PID=$(pgrep -x "node_exporter" | head -1)
  info "  PID: $NE_PID"
elif ss -tlnp | grep -q ":${NODE_EXPORTER_PORT}"; then
  NE_PORT="$NODE_EXPORTER_PORT"
  NE_MODE="standalone"
  ok "Puerto $NODE_EXPORTER_PORT escuchando (node-exporter standalone activo)"
else
  fail "node-exporter NO está corriendo (ni embebido :$NODE_EXPORTER_EMBEDDED_PORT ni standalone :$NODE_EXPORTER_PORT)"
fi

if [[ "$NE_MODE" == "embebido en Alloy" ]]; then
  # En modo embebido, Alloy 1.17.x NO re-expone las métricas node_* en un
  # /metrics local con formato Prometheus estándar (confirmado en campo: ese
  # puerto solo sirve las métricas propias de Alloy, alloy_*). El componente
  # prometheus.exporter.unix alimenta directo a scrape→relabel→remote_write
  # sin dejar rastro en /metrics. La validación LOCAL correcta es la API de
  # componentes de Alloy (/api/v0/web/components), que reporta el estado real
  # ("health.state") de cada bloque del pipeline sin depender de red hacia
  # Prometheus central.
  COMPONENTS_JSON=$(curl -sf --max-time 5 "http://localhost:${NE_PORT}/api/v0/web/components" 2>/dev/null || echo "")

  if [[ -n "$COMPONENTS_JSON" ]] && command -v python3 &>/dev/null; then
    ok "API de componentes de Alloy responde (:$NE_PORT/api/v0/web/components)"

    UNIX_HEALTH=$(echo "$COMPONENTS_JSON" | python3 -c \
      "import sys,json
data=json.load(sys.stdin)
m=[c for c in data if c.get('name')=='prometheus.exporter.unix']
print(m[0]['health']['state'] if m else 'not_found')" 2>/dev/null || echo "parse_error")

    case "$UNIX_HEALTH" in
      healthy)    ok   "Componente prometheus.exporter.unix: healthy (recolectando métricas de host)" ;;
      not_found)  fail "No se encontró ningún componente prometheus.exporter.unix en Alloy — métricas de host NO se están recolectando" ;;
      *)          warn "Componente prometheus.exporter.unix en estado '$UNIX_HEALTH' (esperado: healthy)" ;;
    esac

    RW_HEALTH=$(echo "$COMPONENTS_JSON" | python3 -c \
      "import sys,json
data=json.load(sys.stdin)
m=[c for c in data if c.get('name')=='prometheus.remote_write']
print(m[0]['health']['state'] if m else 'not_found')" 2>/dev/null || echo "parse_error")

    case "$RW_HEALTH" in
      healthy)    ok   "Componente prometheus.remote_write: healthy (enviando métricas al Prometheus central)" ;;
      not_found)  warn "No se encontró componente prometheus.remote_write — métricas locales, pero sin envío a central" ;;
      *)          warn "Componente prometheus.remote_write en estado '$RW_HEALTH' (esperado: healthy)" ;;
    esac

    info "Nota: no se listan métricas node_* individuales — en este modo no existen en ningún endpoint local; la salud de componentes de arriba es la validación local equivalente. Para confirmar arribo end-to-end, consultar Prometheus central (ver Módulo 5)."
  else
    warn "No se pudo consultar la API de componentes de Alloy (:$NE_PORT/api/v0/web/components) — verificar que esta versión de Alloy la soporte"
  fi

  info "Versión node-exporter: N/A (modo embebido — la versión relevante es la de Alloy, ver MÓDULO 3)"

elif [[ -n "$NE_PORT" ]]; then
  # Modo standalone: un node_exporter real expone /metrics en formato
  # Prometheus estándar — aquí sí aplica el grep de texto plano.
  NE_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${NE_PORT}/metrics" 2>/dev/null || echo "000")
  [[ "$NE_HTTP" == "200" ]] \
    && ok "Endpoint /metrics responde ($NE_MODE, :$NE_PORT): HTTP $NE_HTTP" \
    || fail "Endpoint /metrics no responde ($NE_MODE, :$NE_PORT): HTTP $NE_HTTP"

  if [[ "$NE_HTTP" == "200" ]]; then
    METRICS_RAW=$(curl -sf --max-time 5 "http://localhost:${NE_PORT}/metrics" 2>/dev/null || echo "")
    for METRIC in "node_cpu_seconds_total" "node_memory_MemTotal_bytes" \
                  "node_filesystem_size_bytes" "node_network_receive_bytes_total" \
                  "node_load1"; do
      echo "$METRICS_RAW" | grep -q "^${METRIC}" \
        && ok "  Métrica presente: $METRIC" \
        || warn "  Métrica ausente: $METRIC"
    done
  fi

  NE_VERSION=$(curl -sf --max-time 5 "http://localhost:${NE_PORT}/metrics" 2>/dev/null \
    | grep 'node_exporter_build_info' | grep -oP 'version="\K[^"]+' | head -1 || echo "unknown")
  info "Versión node-exporter: $NE_VERSION"
  [[ "$NE_VERSION" != "unknown" ]] \
    && ok "Versión node-exporter identificada: $NE_VERSION" \
    || warn "No se pudo identificar versión de node-exporter"
fi

# 2.5 Servicio systemd — solo aplica al modo standalone; en modo embebido
# el ciclo de vida lo controla el servicio de Alloy (ver MÓDULO 3).
if [[ "$NE_MODE" == "standalone" ]]; then
  for UNIT_NAME in "node_exporter" "node-exporter" "prometheus-node-exporter"; do
    if systemctl is-active --quiet "$UNIT_NAME" 2>/dev/null; then
      ok "Servicio systemd activo: $UNIT_NAME"
      systemctl is-enabled --quiet "$UNIT_NAME" 2>/dev/null \
        && ok "  ↳ Habilitado en boot" \
        || warn "  ↳ NO habilitado en boot — no sobrevivirá reinicios"
      break
    fi
  done
else
  info "Modo embebido en Alloy: el autoarranque se valida vía el servicio de Alloy (MÓDULO 3)"
fi

# ===========================================================================
# MÓDULO 3 – Grafana Alloy
# ===========================================================================
section "MÓDULO 3 · Grafana Alloy 1.6.0"

# 3.1 Binario o proceso
ALLOY_BIN=""
for BIN_PATH in /usr/bin/alloy /usr/local/bin/alloy /opt/alloy/bin/alloy; do
  [[ -x "$BIN_PATH" ]] && ALLOY_BIN="$BIN_PATH" && break
done

if [[ -n "$ALLOY_BIN" ]]; then
  ok "Binario Alloy encontrado: $ALLOY_BIN"
  ALLOY_VER=$("$ALLOY_BIN" --version 2>/dev/null | grep -oP 'v\K[\d.]+' | head -1 || echo "unknown")
  info "Versión Alloy: $ALLOY_VER"
  # La versión homologada (1.6.0) es una recomendación, no un requisito duro
  # de compatibilidad — por eso una versión distinta es INFO, no WARN.
  [[ "$ALLOY_VER" == "$ALLOY_VERSION_EXPECTED"* ]] \
    && ok "Versión Alloy correcta: $ALLOY_VER" \
    || info "Versión Alloy: detectada='$ALLOY_VER', recomendada='$ALLOY_VERSION_EXPECTED' (no bloqueante)"
elif pgrep -x "alloy" &>/dev/null; then
  ok "Proceso alloy corriendo (binario en ruta no estándar)"
else
  fail "Alloy NO encontrado — sin agente de observabilidad en este nodo"
fi

# 3.2 Proceso activo
if pgrep -x "alloy" &>/dev/null; then
  ok "Proceso alloy en ejecución"
  ALLOY_PID=$(pgrep -x "alloy" | head -1)
  info "  PID: $ALLOY_PID"
  ALLOY_UPTIME=$(ps -o etimes= -p "$ALLOY_PID" 2>/dev/null | tr -d ' ' || echo "?")
  info "  Uptime (segundos): $ALLOY_UPTIME"
else
  fail "Proceso alloy NO está corriendo"
fi

# 3.3 Puerto HTTP de Alloy
ss -tlnp | grep -q ":${ALLOY_HTTP_PORT}" \
  && ok "Puerto $ALLOY_HTTP_PORT escuchando (Alloy HTTP/UI)" \
  || warn "Puerto $ALLOY_HTTP_PORT no escucha (Alloy puede usar puerto distinto)"

# 3.4 Alloy health check
ALLOY_HEALTH=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${ALLOY_HTTP_PORT}/-/ready" 2>/dev/null || echo "000")
[[ "$ALLOY_HEALTH" == "200" ]] \
  && ok "Alloy health check OK: HTTP $ALLOY_HEALTH" \
  || warn "Alloy health check: HTTP $ALLOY_HEALTH (puede estar en puerto diferente)"

# 3.5 Métricas propias de Alloy
ALLOY_METRICS_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${ALLOY_HTTP_PORT}/metrics" 2>/dev/null || echo "000")
[[ "$ALLOY_METRICS_HTTP" == "200" ]] \
  && ok "Endpoint /metrics de Alloy responde: HTTP $ALLOY_METRICS_HTTP" \
  || warn "Endpoint /metrics de Alloy no accesible en puerto $ALLOY_HTTP_PORT"

# 3.6 Configuración de Alloy
# El nombre real y ruta confirmados en campo (CONFIG_FILE en /etc/sysconfig/alloy)
# son $ALLOY_CONFIG_DIR/config.alloy. Se prueba primero con un stat directo
# ([[ -f ]]), que solo requiere permiso de TRÁNSITO (x) sobre el directorio
# padre — a diferencia de `find`, que necesita permiso de LISTADO (r) sobre
# el propio directorio y por eso fallaba en silencio cuando /etc/alloy no es
# legible por el usuario que corre el script (mismo caso ya visto en MÓDULO 4).
ALLOY_KNOWN_CFG="${ALLOY_CONFIG_DIR}/config.alloy"
if [[ -f "$ALLOY_KNOWN_CFG" ]]; then
  ok "Archivo de configuración encontrado: $ALLOY_KNOWN_CFG"
  if CFG_CONTENT=$(cat "$ALLOY_KNOWN_CFG" 2>/dev/null); then
    echo "$CFG_CONTENT" | grep -q "prometheus\|loki\|$OBS_HOST\|9090\|3100" \
      && ok "  ↳ Referencia al stack de observabilidad ($OBS_HOST) detectada en configuración" \
      || warn "  ↳ No se detecta referencia explícita al stack de observabilidad ($OBS_HOST)"
  else
    info "  ↳ Archivo existe pero su contenido no es legible por este usuario (permisos) — no se puede verificar la referencia al stack; no es evidencia de que falte"
  fi
else
  # Fallback: puede que el nombre de archivo real difiera del confirmado en
  # pbigd-plat-apps01 — buscar por extensión antes de declarar FAIL. Si esto
  # también falla por falta de permiso de listado, el resultado es el mismo
  # "no encontrado" que antes, pero ya se agotó la vía más confiable primero.
  ALLOY_CFG_FILES=$(find "$ALLOY_CONFIG_DIR" -name "*.alloy" -o -name "*.river" 2>/dev/null | head -5 || true)
  if [[ -n "$ALLOY_CFG_FILES" ]]; then
    ok "Archivo(s) de configuración Alloy encontrado(s) (nombre no estándar)"
    info "  $ALLOY_CFG_FILES"
  else
    ALT_CFG=$(find /etc/alloy /etc/grafana-agent /opt/alloy \
      -name "*.alloy" -o -name "*.river" -o -name "config*" 2>/dev/null | head -3 || true)
    if [[ -n "$ALT_CFG" ]]; then
      ok "Configuración Alloy encontrada en ruta alternativa"
      info "  $ALT_CFG"
    else
      warn "No se pudo confirmar el archivo de configuración de Alloy ($ALLOY_KNOWN_CFG no accesible ni por stat ni por listado) — puede ser falta de permisos de $USER, verificar con sudo antes de escalar como hallazgo real"
    fi
  fi
fi

# 3.7 Servicio systemd Alloy
ALLOY_SVC_ACTIVE=false
for UNIT_NAME in "alloy" "grafana-alloy" "grafana-agent"; do
  if systemctl is-active --quiet "$UNIT_NAME" 2>/dev/null; then
    ok "Servicio systemd activo: $UNIT_NAME"
    systemctl is-enabled --quiet "$UNIT_NAME" 2>/dev/null \
      && ok "  ↳ Habilitado en boot" \
      || fail "  ↳ NO habilitado en boot — CRÍTICO: sin observabilidad tras reinicio"
    ALLOY_SVC_ACTIVE=true
    break
  fi
done
$ALLOY_SVC_ACTIVE || warn "No se detectó servicio systemd de Alloy (verificar nombre de unidad)"

# ===========================================================================
# MÓDULO 4 – Directorios de Alloy
# ===========================================================================
section "MÓDULO 4 · Directorios y persistencia de Alloy"

for DIR in "$ALLOY_CONFIG_DIR" "$ALLOY_DATA_DIR"; do
  if [[ -d "$DIR" ]]; then
    ok "Directorio existe: $DIR"
    touch "$DIR/.write_test" 2>/dev/null && rm -f "$DIR/.write_test" \
      && ok "  ↳ Escritura OK" \
      || warn "  ↳ Sin permisos de escritura (esperado en $ALLOY_CONFIG_DIR — la config puede traer credenciales de remote_write)"
  else
    warn "Directorio no encontrado: $DIR (puede estar en ruta alternativa)"
  fi
done

# Logs de Alloy — no hay ALLOY_LOG_DIR: este despliegue no escribe logs a un
# directorio propio (no vino con --log.file en ExecStart), va todo a journald.
info "Verificando logs de Alloy en journald..."
JOURNAL_LOG_COUNT=$(journalctl -u alloy --no-pager -n 1 2>/dev/null | wc -l)
[[ "$JOURNAL_LOG_COUNT" -gt 0 ]] \
  && ok "Logs de Alloy disponibles vía journald (journalctl -u alloy)" \
  || warn "Sin logs recientes de Alloy en journald — verificar 'journalctl -u alloy'"

# WAL (Write-Ahead Log) de Alloy para métricas — evita pérdida en reinicios.
# Nota: si $ALLOY_DATA_DIR no es legible por el usuario que corre este script
# (mismo caso de permisos que $ALLOY_CONFIG_DIR), `find` fallará en silencio
# y esto dará WARN aunque el WAL exista — no asumir que "no encontrado" es
# igual a "no existe" sin antes descartar un problema de permisos.
WAL_DIR=$(find "$ALLOY_DATA_DIR" /var/lib/alloy /data/alloy /opt/alloy -name "wal" -type d 2>/dev/null | head -1 || true)
[[ -n "$WAL_DIR" ]] \
  && ok "WAL de Alloy encontrado: $WAL_DIR (métricas persisten en reinicios)" \
  || warn "WAL de Alloy no encontrado en $ALLOY_DATA_DIR — puede ser que no exista, o que el usuario actual no tenga permisos para leer ahí (verificar con sudo antes de escalar como hallazgo real)"

# ===========================================================================
# MÓDULO 5 – Conectividad hacia el stack de observabilidad
# ===========================================================================
section "MÓDULO 5 · Conectividad hacia $OBS_HOST (stack observabilidad)"

# 5.0 Resolución de nombre — se verifica ANTES de cualquier curl/nc, porque un
# fallo de DNS y un servicio caído producen el mismo síntoma superficial
# ("no alcanzable") pero tienen causa raíz y acción correctiva totalmente
# distintas. Sin este check, un problema de DNS aparece disfrazado de 3
# WARNs independientes (Prometheus, Loki, OTLP) en vez de señalarse como
# la causa común única que realmente es.
info "Verificando resolución DNS de $OBS_HOST..."
# || true: si getent no resuelve el host, su fallo se propaga como el status
# del pipeline completo (pipefail) aunque awk/head terminen bien — sin este
# guard, set -e mataría el script aquí mismo en vez de dejar que el if/else
# de abajo maneje el caso "no resuelve".
OBS_HOST_IP=$(getent hosts "$OBS_HOST" 2>/dev/null | awk '{print $1}' | head -1 || true)
if [[ -n "$OBS_HOST_IP" ]]; then
  ok "DNS resuelve $OBS_HOST → $OBS_HOST_IP"
  OBS_DNS_OK=true
else
  fail "DNS NO resuelve '$OBS_HOST' desde $THIS_HOST — bloquea TODO el envío a observabilidad (Prometheus, Loki, OTLP), aunque los servicios estén sanos. Corregir agregando el registro en el DNS interno (o en /etc/hosts) antes de investigar los servicios."
  OBS_DNS_OK=false
fi

if $OBS_DNS_OK; then
  info "Verificando acceso a Prometheus en $OBS_HOST:$PROMETHEUS_PORT"
  PROM_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://${OBS_HOST}:${PROMETHEUS_PORT}/-/healthy" 2>/dev/null || echo "000")
  [[ "$PROM_HTTP" == "200" ]] \
    && ok "Prometheus alcanzable desde $THIS_HOST: HTTP $PROM_HTTP" \
    || warn "DNS resuelve, pero Prometheus no responde en $OBS_HOST:$PROMETHEUS_PORT (HTTP $PROM_HTTP) — revisar el servicio o firewall, no el DNS"

  info "Verificando acceso a Loki en $OBS_HOST:$LOKI_PORT"
  LOKI_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://${OBS_HOST}:${LOKI_PORT}/ready" 2>/dev/null || echo "000")
  [[ "$LOKI_HTTP" == "200" ]] \
    && ok "Loki alcanzable desde $THIS_HOST: HTTP $LOKI_HTTP" \
    || warn "DNS resuelve, pero Loki no responde en $OBS_HOST:$LOKI_PORT (HTTP $LOKI_HTTP) — logs no centralizados; revisar el servicio o firewall, no el DNS"

  # Puerto OTLP gRPC
  nc -z -w 3 "$OBS_HOST" 4317 2>/dev/null \
    && ok "OTLP gRPC (4317) alcanzable en $OBS_HOST" \
    || warn "DNS resuelve, pero OTLP gRPC (4317) no responde en $OBS_HOST — revisar el servicio o firewall, no el DNS"
else
  warn "Prometheus/Loki/OTLP omitidos — sin resolución DNS de $OBS_HOST no tiene sentido probar puertos individuales (ver FAIL de arriba)"
fi

# ===========================================================================
# MÓDULO 6 – Validación de recolección activa
# ===========================================================================
section "MÓDULO 6 · Verificación de recolección activa"

# Verificar que Alloy está efectivamente enviando a Prometheus (remote_write)
if [[ "$ALLOY_METRICS_HTTP" == "200" ]]; then
  ALLOY_RW_SENT=$(curl -sf --max-time 5 "http://localhost:${ALLOY_HTTP_PORT}/metrics" 2>/dev/null \
    | grep "prometheus_remote_storage_samples_total" | head -1 || echo "")
  [[ -n "$ALLOY_RW_SENT" ]] \
    && ok "Remote write activo: métricas siendo enviadas a Prometheus" \
    || warn "No se detectó actividad de remote_write — ¿Alloy configurado para enviar a Prometheus?"

  # Verificar envío de logs a Loki
  ALLOY_LOKI_SENT=$(curl -sf --max-time 5 "http://localhost:${ALLOY_HTTP_PORT}/metrics" 2>/dev/null \
    | grep "loki_write_sent_entries_total\|loki_process_" | head -1 || echo "")
  [[ -n "$ALLOY_LOKI_SENT" ]] \
    && ok "Envío de logs a Loki activo" \
    || warn "No se detectó actividad de escritura a Loki (puede ser normal si Loki es opcional)"
fi

# Verificar la fuente de métricas de nodo: en modo embebido, Alloy genera las
# métricas él mismo (prometheus.exporter.unix), no las "scrapea" de un
# node-exporter externo en :9100.
if [[ "${NE_MODE:-}" == "embebido en Alloy" ]]; then
  # Reutiliza $COMPONENTS_JSON ya obtenido en el MÓDULO 2 (misma sesión de
  # script, no subshell). Antes este check hacía grep sobre archivos de
  # config en /etc/alloy, que requiere permiso de LISTADO sobre ese
  # directorio — el mismo permiso que ya confirmamos que falta para
  # $ALLOY_CONFIG_DIR (MÓDULO 3.6/4), por eso daba WARN aunque el componente
  # sí existiera (confirmado por la API en MÓDULO 2). La API de componentes
  # no tiene ese problema porque no depende de leer archivos en disco.
  if [[ -n "${COMPONENTS_JSON:-}" ]]; then
    [[ "${UNIX_HEALTH:-}" == "healthy" ]] \
      && ok "Alloy configurado con exporter.unix embebido y healthy (node-exporter :$NODE_EXPORTER_EMBEDDED_PORT) — confirmado vía API de componentes" \
      || warn "Componente exporter.unix no confirmado como healthy vía API de componentes (ver detalle en MÓDULO 2)"
  else
    warn "No se pudo confirmar el componente exporter.unix — API de componentes no disponible en MÓDULO 2"
  fi
else
  ALLOY_SCRAPES_NE=false
  for CFG in $(find /etc/alloy /opt/alloy /etc/grafana-agent \
    -name "*.alloy" -o -name "*.river" 2>/dev/null); do
    grep -q "node.exporter\|node_exporter\|9100" "$CFG" 2>/dev/null \
      && ALLOY_SCRAPES_NE=true && break
  done
  $ALLOY_SCRAPES_NE \
    && ok "Alloy configurado para scraping de node-exporter (:9100)" \
    || warn "No se detecta scraping de node-exporter en la configuración de Alloy"
fi

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Agentes Observabilidad                              ║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Nodo         : $(printf '%-50s' "$THIS_HOST")║${NC}"
log "${BOLD}║   Total checks : $(printf '%-50s' "$TOTAL")║${NC}"
log "${GREEN}${BOLD}║   PASS         : $(printf '%-50s' "$PASS")║${NC}"
log "${RED}${BOLD}║   FAIL         : $(printf '%-50s' "$FAIL")║${NC}"
log "${YELLOW}${BOLD}║   WARN         : $(printf '%-50s' "$WARN")║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Log          : $(printf '%-50s' "$LOG_FILE")║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

[[ $FAIL -eq 0 ]] \
  && log "${GREEN}${BOLD}  ✔  Agentes OK en $THIS_HOST — nodo visible en stack de observabilidad${NC}" \
  || log "${RED}${BOLD}  ✘  $FAIL fallos críticos en $THIS_HOST — nodo puede estar CIEGO para observabilidad${NC}"

log ""
exit $FAIL
