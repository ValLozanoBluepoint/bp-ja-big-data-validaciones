#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Stack Observabilidad COMPLETO – DC Principal (pbigd-plat-obs01)
#
# Nodo: pbigd-plat-obs01  (4 vCPU / 8 GB RAM / 500 GB disco)
# Componentes:
#   · Prometheus 3.5.1              → métricas (mandatorio)
#   · Grafana 12.3.4                → dashboards (mandatorio)
#   · Redis Sentinel 7.4.x          → también presente en pbigd-plat-obs01
#   · Loki 3.6.5                    → logs centralizados (opcional/recomendado)
#   · Tempo 2.10.1                  → trazas distribuidas (opcional)
#   · OpenTelemetry Collector 0.146.1 → receptor OTLP (opcional)
#   · Grafana Alloy 1.6.0           → agente local en pbigd-plat-obs01
#   · node-exporter                 → métricas del propio nodo (standalone :9100
#                                      o embebido en Alloy :17935)
#
# Validaciones cubiertas:
#   1. OS y prerequisitos
#   2. Prometheus: proceso, puerto, API, targets, scrape activo
#   3. Grafana: proceso, puerto, API, datasources
#   4. Loki: proceso, puerto, health, ingestión
#   5. Tempo: proceso, puerto, health
#   6. OpenTelemetry Collector: proceso, puerto
#   7. Redis Sentinel (en pbigd-plat-obs01)
#   8. Alloy + node-exporter locales (standalone o embebido)
#   9. Visibilidad de todos los nodos del cluster (scrape coverage)
#  10. Alertas básicas configuradas
#  11. Persistencia y directorios
#  12. systemd y reinicio automático
#
# Runtime: Rocky Linux 10.x · Podman 5.x · systemd
#
# Uso:
#   chmod +x validate_obs_stack_principal.sh
#   ./validate_obs_stack_principal.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN – versiones esperadas
# ---------------------------------------------------------------------------
PROMETHEUS_VERSION_EXPECTED="3.5.1"
GRAFANA_VERSION_EXPECTED="12.3.4"
LOKI_VERSION_EXPECTED="3.6.5"
TEMPO_VERSION_EXPECTED="2.10.1"
OTELCOL_VERSION_EXPECTED="0.146.1"
ALLOY_VERSION_EXPECTED="1.6.0"

# Puertos
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
LOKI_PORT=3100
LOKI_GRPC_PORT=9095
TEMPO_PORT=3200
TEMPO_OTLP_GRPC=4317
TEMPO_OTLP_HTTP=4318
OTELCOL_GRPC=4317
OTELCOL_HTTP=4318
OTELCOL_METRICS=8888
NODE_EXPORTER_PORT=9100
NODE_EXPORTER_EMBEDDED_PORT=17935   # exporter unix embebido en Alloy (prometheus.exporter.unix)
ALLOY_PORT=12345
REDIS_SENTINEL_PORT=26379

# Directorios
PROMETHEUS_DIR="/opt/prometheus"
PROMETHEUS_DATA="/data/prometheus"
GRAFANA_DIR="/opt/grafana"
GRAFANA_DATA="/data/grafana"
LOKI_DIR="/opt/loki"
LOKI_DATA="/data/loki"
TEMPO_DIR="/opt/tempo"
TEMPO_DATA="/data/tempo"
OBS_LOG_BASE="/var/log"

# Todos los nodos del DC Principal que deben ser visibles en Prometheus
PRINCIPAL_NODES=(
  "pbigd-kaf01" "pbigd-kaf02" "pbigd-kaf03"
  "pbigd-stg01" "pbigd-stg02" "pbigd-stg03"
  "pbigd-dlh01" "pbigd-dlh02" "pbigd-dlh03"
  "pbigd-proc01" "pbigd-proc02" "pbigd-proc03"
  "pbigd-plat-apps01" "pbigd-bd-plat-apps01" "pbigd-plat-obs01"
)

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; BLUE='\033[0;34m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
THIS_HOST=$(hostname -s 2>/dev/null || hostname)
LOG_FILE="/tmp/validate_obs_stack_principal_$(date +%Y%m%d_%H%M%S).log"

log()     { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()      { log "${GREEN}  [OK]${NC}  $*";  ((PASS++)); }
fail()    { log "${RED}  [FAIL]${NC} $*"; ((FAIL++)); }
warn()    { log "${YELLOW}  [WARN]${NC} $*"; ((WARN++)); }
info()    { log "${CYAN}  [INFO]${NC} $*"; }
opt()     { log "${BLUE}  [OPT]${NC}  $*"; }   # componente opcional
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
log "${BOLD}║   BLUEPOINT · Stack Observabilidad DC Principal  (pbigd-plat-obs01)        ║${NC}"
log "${BOLD}║   Prometheus 3.5.1 · Grafana 12.3.4 · Loki 3.6.5              ║${NC}"
log "${BOLD}║   Tempo 2.10.1 · OTel Collector 0.146.1                        ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

# Verificar que estamos en el nodo correcto
if [[ "$THIS_HOST" != "pbigd-plat-obs01" ]]; then
  warn "Este script está diseñado para pbigd-plat-obs01. Nodo actual: $THIS_HOST"
  warn "Continuando de todas formas — algunos módulos pueden no aplicar"
fi

# ===========================================================================
# MÓDULO 1 – OS y prerequisitos
# ===========================================================================
section "MÓDULO 1 · OS y prerequisitos"

OS=$(grep -oP '(?<=Rocky Linux )\S+' /etc/os-release 2>/dev/null || echo "unknown")
[[ "$OS" == 10* ]] && ok "Rocky Linux 10.x confirmado" || warn "OS: $OS"

command -v podman &>/dev/null \
  && ok "Podman disponible: v$(podman --version | awk '{print $3}')" \
  || warn "Podman no encontrado"

if timedatectl status 2>/dev/null | grep -q "synchronized: yes"; then
  ok "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  ok "NTP sincronizado (chrony)"
else
  fail "NTP NO sincronizado — timestamps de métricas incorrectos"
fi

# Disco disponible — pbigd-plat-obs01 tiene 500 GB, requiere bastante para TSDB + Loki + Tempo
DISK_DATA=$(df --output=avail -BG /data 2>/dev/null | tail -1 | tr -d 'G ' || echo "0")
info "Espacio libre en /data: ${DISK_DATA} GB (pbigd-plat-obs01 tiene 500 GB asignados)"
[[ "$DISK_DATA" -ge 100 ]] 2>/dev/null \
  && ok "Espacio libre suficiente: ${DISK_DATA} GB" \
  || warn "Espacio libre bajo: ${DISK_DATA} GB — TSDB y Loki pueden saturarse"

# ===========================================================================
# MÓDULO 2 – Prometheus 3.5.1
# ===========================================================================
section "MÓDULO 2 · Prometheus 3.5.1 (MANDATORIO)"

# 2.1 Proceso
if pgrep -x "prometheus" &>/dev/null; then
  ok "Proceso prometheus corriendo"
elif ss -tlnp | grep -q ":${PROMETHEUS_PORT}"; then
  ok "Puerto $PROMETHEUS_PORT escuchando (prometheus activo)"
else
  fail "Prometheus NO está corriendo — sin recolección de métricas"
fi

# 2.2 Puerto
ss -tlnp | grep -q ":${PROMETHEUS_PORT}" \
  && ok "Puerto $PROMETHEUS_PORT escuchando" \
  || fail "Puerto $PROMETHEUS_PORT no escucha"

# 2.3 Health check
PROM_HEALTH=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${PROMETHEUS_PORT}/-/healthy" 2>/dev/null || echo "000")
[[ "$PROM_HEALTH" == "200" ]] \
  && ok "Prometheus health: HTTP $PROM_HEALTH" \
  || fail "Prometheus health check fallido: HTTP $PROM_HEALTH"

PROM_READY=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${PROMETHEUS_PORT}/-/ready" 2>/dev/null || echo "000")
[[ "$PROM_READY" == "200" ]] \
  && ok "Prometheus ready: HTTP $PROM_READY" \
  || warn "Prometheus ready check: HTTP $PROM_READY"

# 2.4 Versión
PROM_VER=$(curl -sf --max-time 5 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/status/buildinfo" 2>/dev/null \
  | python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['data'].get('version','?'))" 2>/dev/null || echo "unknown")
info "Versión Prometheus: $PROM_VER"
[[ "$PROM_VER" == "$PROMETHEUS_VERSION_EXPECTED"* ]] \
  && ok "Versión Prometheus correcta: $PROM_VER" \
  || warn "Versión Prometheus: detectada='$PROM_VER' esperada='$PROMETHEUS_VERSION_EXPECTED'"

# 2.5 Targets activos
TARGETS_JSON=$(curl -sf --max-time 10 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/targets" 2>/dev/null || echo "")
if [[ -n "$TARGETS_JSON" ]]; then
  ok "API /targets responde"
  TARGETS_UP=$(echo "$TARGETS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    t=d['data']['activeTargets']; \
    up=[x for x in t if x['health']=='up']; \
    down=[x for x in t if x['health']!='up']; \
    print(f'Targets UP: {len(up)}  DOWN/UNKNOWN: {len(down)}  TOTAL: {len(t)}')" \
    2>/dev/null || echo "?")
  info "  $TARGETS_UP"

  # Listar targets DOWN
  echo "$TARGETS_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
down = [x for x in d['data']['activeTargets'] if x['health'] != 'up']
if down:
    print('  Targets con problemas:')
    for t in down[:10]:
        print(f'    [{t[\"health\"]}] {t[\"labels\"].get(\"instance\",\"?\")}')" \
    2>/dev/null | tee -a "$LOG_FILE" || true

  # Contar targets up vs esperados
  N_UP=$(echo "$TARGETS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    print(len([x for x in d['data']['activeTargets'] if x['health']=='up']))" \
    2>/dev/null || echo "0")
  [[ "$N_UP" -ge ${#PRINCIPAL_NODES[@]} ]] 2>/dev/null \
    && ok "Suficientes targets activos: $N_UP (nodos esperados: ${#PRINCIPAL_NODES[@]})" \
    || warn "Targets activos ($N_UP) < nodos esperados (${#PRINCIPAL_NODES[@]})"
else
  warn "No se pudo consultar /api/v1/targets"
fi

# 2.6 Verificar que métricas de componentes clave están presentes
if [[ "$PROM_HEALTH" == "200" ]]; then
  for QUERY_CHECK in \
    "up" \
    "node_cpu_seconds_total" \
    "node_memory_MemTotal_bytes"; do
    QUERY_RESULT=$(curl -sf --max-time 5 \
      "http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=${QUERY_CHECK}" \
      2>/dev/null | python3 -c \
      "import sys,json; d=json.load(sys.stdin); \
      r=d.get('data',{}).get('result',[]); print(len(r))" 2>/dev/null || echo "0")
    [[ "$QUERY_RESULT" -gt 0 ]] 2>/dev/null \
      && ok "  Métrica activa: $QUERY_CHECK ($QUERY_RESULT series)" \
      || warn "  Métrica sin datos: $QUERY_CHECK"
  done
fi

# 2.7 Retention configurada
PROM_FLAGS=$(curl -sf --max-time 5 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/status/flags" 2>/dev/null || echo "")
RETENTION=$(echo "$PROM_FLAGS" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); \
  print(d['data'].get('storage.tsdb.retention.time','?'))" 2>/dev/null || echo "?")
info "Retención TSDB configurada: $RETENTION"

# ===========================================================================
# MÓDULO 3 – Grafana 12.3.4
# ===========================================================================
section "MÓDULO 3 · Grafana 12.3.4 (MANDATORIO)"

# 3.1 Proceso / Puerto
if pgrep -x "grafana\|grafana-server" &>/dev/null || \
   ss -tlnp | grep -q ":${GRAFANA_PORT}"; then
  ok "Grafana activo (proceso o puerto $GRAFANA_PORT)"
else
  fail "Grafana NO está corriendo"
fi

ss -tlnp | grep -q ":${GRAFANA_PORT}" \
  && ok "Puerto $GRAFANA_PORT escuchando (Grafana UI)" \
  || fail "Puerto $GRAFANA_PORT no escucha"

# 3.2 Health check
GRAFANA_HEALTH=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${GRAFANA_PORT}/api/health" 2>/dev/null || echo "000")
[[ "$GRAFANA_HEALTH" == "200" ]] \
  && ok "Grafana health check OK: HTTP $GRAFANA_HEALTH" \
  || fail "Grafana health check fallido: HTTP $GRAFANA_HEALTH"

# 3.3 Versión via API
GRAFANA_VER=$(curl -sf --max-time 5 \
  "http://localhost:${GRAFANA_PORT}/api/health" 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('version','?'))" \
  2>/dev/null || echo "unknown")
info "Versión Grafana: $GRAFANA_VER"
[[ "$GRAFANA_VER" == "$GRAFANA_VERSION_EXPECTED"* ]] \
  && ok "Versión Grafana correcta: $GRAFANA_VER" \
  || warn "Versión Grafana: detectada='$GRAFANA_VER' esperada='$GRAFANA_VERSION_EXPECTED'"

# 3.4 Datasources configurados (anon o con credenciales de admin)
DS_RESP=$(curl -sf --max-time 5 -u "admin:admin" \
  "http://localhost:${GRAFANA_PORT}/api/datasources" 2>/dev/null || \
  curl -sf --max-time 5 \
  "http://localhost:${GRAFANA_PORT}/api/datasources" 2>/dev/null || echo "")
if [[ -n "$DS_RESP" ]]; then
  DS_COUNT=$(echo "$DS_RESP" | python3 -c \
    "import sys,json; ds=json.load(sys.stdin); \
    [print(f'  DS: {d[\"name\"]} ({d[\"type\"]})') for d in ds]; \
    print(f'Total datasources: {len(ds)}')" 2>/dev/null || echo "?")
  ok "Datasources accesibles"
  log "$DS_COUNT"

  # Verificar que Prometheus está como datasource
  echo "$DS_RESP" | python3 -c \
    "import sys,json; ds=json.load(sys.stdin); \
    prom=[d for d in ds if d['type']=='prometheus']; \
    print('ok' if prom else 'missing')" 2>/dev/null | grep -q "^ok" \
    && ok "  Datasource Prometheus configurado en Grafana" \
    || warn "  Datasource Prometheus NO configurado en Grafana"

  echo "$DS_RESP" | python3 -c \
    "import sys,json; ds=json.load(sys.stdin); \
    loki=[d for d in ds if d['type']=='loki']; \
    print('ok' if loki else 'missing')" 2>/dev/null | grep -q "^ok" \
    && ok "  Datasource Loki configurado en Grafana" \
    || warn "  Datasource Loki NO configurado (opcional pero recomendado)"
else
  warn "No se pudo consultar datasources (puede requerir credenciales no por defecto)"
fi

# 3.5 Dashboards disponibles
DASH_RESP=$(curl -sf --max-time 5 -u "admin:admin" \
  "http://localhost:${GRAFANA_PORT}/api/search?type=dash-db" 2>/dev/null || echo "")
if [[ -n "$DASH_RESP" ]]; then
  DASH_COUNT=$(echo "$DASH_RESP" | python3 -c \
    "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
  info "Dashboards configurados: $DASH_COUNT"
  [[ "$DASH_COUNT" -gt 0 ]] 2>/dev/null \
    && ok "Dashboards disponibles: $DASH_COUNT" \
    || warn "Sin dashboards configurados — visibilidad operacional limitada"
fi

# ===========================================================================
# MÓDULO 4 – Loki 3.6.5 (recomendado / opcional)
# ===========================================================================
section "MÓDULO 4 · Loki 3.6.5 (recomendado)"

LOKI_PRESENT=false
if pgrep -x "loki" &>/dev/null || ss -tlnp | grep -q ":${LOKI_PORT}"; then
  LOKI_PRESENT=true
fi

if $LOKI_PRESENT; then
  ok "Loki detectado en este nodo"

  ss -tlnp | grep -q ":${LOKI_PORT}" \
    && ok "Puerto $LOKI_PORT escuchando (Loki HTTP)" \
    || warn "Puerto $LOKI_PORT no escucha"

  LOKI_READY=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${LOKI_PORT}/ready" 2>/dev/null || echo "000")
  [[ "$LOKI_READY" == "200" ]] \
    && ok "Loki ready: HTTP $LOKI_READY" \
    || warn "Loki ready check: HTTP $LOKI_READY"

  # Versión
  LOKI_VER=$(curl -sf --max-time 5 \
    "http://localhost:${LOKI_PORT}/loki/api/v1/status/buildinfo" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('version','?'))" \
    2>/dev/null || echo "unknown")
  info "Versión Loki: $LOKI_VER"
  [[ "$LOKI_VER" == "$LOKI_VERSION_EXPECTED"* ]] \
    && ok "Versión Loki correcta: $LOKI_VER" \
    || warn "Versión Loki: detectada='$LOKI_VER' esperada='$LOKI_VERSION_EXPECTED'"

  # Métricas de ingesta
  LOKI_METRICS=$(curl -sf --max-time 5 \
    "http://localhost:${LOKI_PORT}/metrics" 2>/dev/null \
    | grep "loki_ingester_streams_created_total" | head -1 || echo "")
  [[ -n "$LOKI_METRICS" ]] \
    && ok "Loki ingestando logs (streams creados detectados)" \
    || warn "No se detecta ingesta activa en Loki"

  # Labels disponibles (hay streams de logs?)
  LOKI_LABELS=$(curl -sf --max-time 5 \
    "http://localhost:${LOKI_PORT}/loki/api/v1/labels" 2>/dev/null \
    | python3 -c "import sys,json; d=json.load(sys.stdin); \
    labels=d.get('data',[]); \
    print(f'Labels: {labels[:10]}')" 2>/dev/null || echo "")
  [[ -n "$LOKI_LABELS" ]] \
    && { ok "Loki tiene streams de logs activos"; info "  $LOKI_LABELS"; } \
    || warn "Loki sin streams activos — ¿Alloy configurado para enviar a Loki?"
else
  opt "Loki NO detectado en pbigd-plat-obs01 (componente opcional/recomendado)"
  opt "Instalar para centralizar logs de todos los nodos"
fi

# ===========================================================================
# MÓDULO 5 – Tempo 2.10.1 (opcional)
# ===========================================================================
section "MÓDULO 5 · Tempo 2.10.1 (opcional)"

if pgrep -x "tempo" &>/dev/null || ss -tlnp | grep -q ":${TEMPO_PORT}"; then
  ok "Tempo detectado en pbigd-plat-obs01"

  TEMPO_READY=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${TEMPO_PORT}/ready" 2>/dev/null || echo "000")
  [[ "$TEMPO_READY" == "200" ]] \
    && ok "Tempo ready: HTTP $TEMPO_READY" \
    || warn "Tempo ready: HTTP $TEMPO_READY"

  TEMPO_VER=$(curl -sf --max-time 5 \
    "http://localhost:${TEMPO_PORT}/status/buildinfo" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('version','?'))" \
    2>/dev/null || echo "unknown")
  info "Versión Tempo: $TEMPO_VER"
  [[ "$TEMPO_VER" == "$TEMPO_VERSION_EXPECTED"* ]] \
    && ok "Versión Tempo correcta: $TEMPO_VER" \
    || warn "Versión Tempo: '$TEMPO_VER' (esperada $TEMPO_VERSION_EXPECTED)"

  # Puerto OTLP de Tempo
  ss -tlnp | grep -q ":${TEMPO_OTLP_GRPC}" \
    && ok "Puerto OTLP gRPC $TEMPO_OTLP_GRPC escuchando (Tempo)" \
    || warn "Puerto OTLP gRPC $TEMPO_OTLP_GRPC no escucha"
else
  opt "Tempo NO detectado (componente opcional para trazas distribuidas)"
fi

# ===========================================================================
# MÓDULO 6 – OpenTelemetry Collector 0.146.1 (opcional)
# ===========================================================================
section "MÓDULO 6 · OpenTelemetry Collector 0.146.1 (opcional)"

OTEL_PROC=$(pgrep -x "otelcol\|otelcol-contrib\|otel-collector" 2>/dev/null | head -1 || echo "")
if [[ -n "$OTEL_PROC" ]] || ss -tlnp | grep -q ":${OTELCOL_METRICS}"; then
  ok "OpenTelemetry Collector detectado"

  ss -tlnp | grep -q ":${OTELCOL_GRPC}" \
    && ok "Puerto OTLP gRPC $OTELCOL_GRPC escuchando" \
    || warn "Puerto OTLP gRPC $OTELCOL_GRPC no escucha"

  ss -tlnp | grep -q ":${OTELCOL_HTTP}" \
    && ok "Puerto OTLP HTTP $OTELCOL_HTTP escuchando" \
    || warn "Puerto OTLP HTTP $OTELCOL_HTTP no escucha"

  OTEL_HEALTH=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:13133/" 2>/dev/null || echo "000")
  [[ "$OTEL_HEALTH" == "200" ]] \
    && ok "OTel Collector health check OK" \
    || warn "OTel Collector health: HTTP $OTEL_HEALTH"

  OTELCOL_VER=$(pgrep -ax "otelcol" 2>/dev/null | grep -oP '\-\-version\s+\K\S+' || echo "unknown")
  info "Versión OTel Collector: $OTELCOL_VER"
else
  opt "OpenTelemetry Collector NO detectado (componente opcional)"
fi

# ===========================================================================
# MÓDULO 7 – Redis Sentinel en pbigd-plat-obs01
# ===========================================================================
section "MÓDULO 7 · Redis Sentinel (en pbigd-plat-obs01)"

if ss -tlnp | grep -q ":${REDIS_SENTINEL_PORT}" || \
   pgrep -x "redis-sentinel" &>/dev/null; then
  ok "Redis Sentinel activo en pbigd-plat-obs01 (puerto $REDIS_SENTINEL_PORT)"

  SENTINEL_PING=$(redis-cli -p "$REDIS_SENTINEL_PORT" PING 2>/dev/null || echo "")
  [[ "$SENTINEL_PING" == "PONG" ]] \
    && ok "Redis Sentinel responde PONG" \
    || warn "Redis Sentinel no responde a PING"

  SENTINEL_MASTERS=$(redis-cli -p "$REDIS_SENTINEL_PORT" \
    SENTINEL masters 2>/dev/null | grep -c "name" || echo "0")
  info "Masters monitoreados por Sentinel en pbigd-plat-obs01: $SENTINEL_MASTERS"
else
  warn "Redis Sentinel no detectado en pbigd-plat-obs01 (según diseño debe estar presente)"
fi

# ===========================================================================
# MÓDULO 8 – Alloy + node-exporter locales en pbigd-plat-obs01
# ===========================================================================
section "MÓDULO 8 · Alloy + node-exporter locales (pbigd-plat-obs01)"

# node-exporter — puede correr standalone (:9100) o embebido en Alloy
# (prometheus.exporter.unix en :17935). Ambos son válidos; solo se falla
# si ninguno de los dos responde.
if ss -tlnp | grep -q ":${NODE_EXPORTER_EMBEDDED_PORT}"; then
  ok "node-exporter embebido en Alloy activo en pbigd-plat-obs01 (puerto $NODE_EXPORTER_EMBEDDED_PORT)"
  NE_SELF=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${NODE_EXPORTER_EMBEDDED_PORT}/metrics" 2>/dev/null || echo "000")
  [[ "$NE_SELF" == "200" ]] \
    && ok "node-exporter embebido responde /metrics" \
    || warn "node-exporter embebido /metrics: HTTP $NE_SELF"
elif ss -tlnp | grep -q ":${NODE_EXPORTER_PORT}"; then
  ok "node-exporter standalone activo en pbigd-plat-obs01 (puerto $NODE_EXPORTER_PORT)"
  NE_SELF=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${NODE_EXPORTER_PORT}/metrics" 2>/dev/null || echo "000")
  [[ "$NE_SELF" == "200" ]] \
    && ok "node-exporter standalone responde /metrics" \
    || warn "node-exporter standalone /metrics: HTTP $NE_SELF"
else
  fail "node-exporter NO activo en pbigd-plat-obs01 (ni embebido :$NODE_EXPORTER_EMBEDDED_PORT ni standalone :$NODE_EXPORTER_PORT) — nodo de obs ciego a sí mismo"
fi

# Alloy
pgrep -x "alloy" &>/dev/null \
  && ok "Alloy corriendo en pbigd-plat-obs01" \
  || warn "Alloy no corriendo en pbigd-plat-obs01"

ALLOY_SELF=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${ALLOY_PORT}/-/ready" 2>/dev/null || echo "000")
[[ "$ALLOY_SELF" == "200" ]] \
  && ok "Alloy en pbigd-plat-obs01 ready: HTTP $ALLOY_SELF" \
  || warn "Alloy en pbigd-plat-obs01: HTTP $ALLOY_SELF"

# ===========================================================================
# MÓDULO 9 – Visibilidad de todos los nodos (scrape coverage)
# ===========================================================================
section "MÓDULO 9 · Cobertura de scraping – todos los nodos del DC Principal"

if [[ "$PROM_HEALTH" == "200" ]]; then
  info "Verificando visibilidad de cada nodo en Prometheus..."
  MISSING_NODES=()

  for NODE in "${PRINCIPAL_NODES[@]}"; do
    # Consultar si hay al menos 1 serie 'up' para este nodo
    QUERY="up{instance=~\"${NODE}.*\"}"
    RESULT=$(curl -sf --max-time 5 \
      "http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=$(python3 -c \
      "import urllib.parse; print(urllib.parse.quote('${QUERY}'))" 2>/dev/null)" \
      2>/dev/null | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(len(d.get('data',{}).get('result',[])))" \
      2>/dev/null || echo "0")

    if [[ "$RESULT" -gt 0 ]] 2>/dev/null; then
      ok "Nodo visible en Prometheus: $NODE ($RESULT serie(s))"
    else
      warn "Nodo SIN datos en Prometheus: $NODE — ¿Alloy/node-exporter instalado?"
      MISSING_NODES+=("$NODE")
    fi
  done

  if [[ ${#MISSING_NODES[@]} -eq 0 ]]; then
    ok "Todos los nodos del DC Principal son visibles en Prometheus"
  else
    warn "${#MISSING_NODES[@]} nodo(s) sin visibilidad: ${MISSING_NODES[*]}"
  fi
else
  warn "Prometheus no disponible — no se puede verificar cobertura de scraping"
fi

# ===========================================================================
# MÓDULO 10 – Alertas básicas configuradas
# ===========================================================================
section "MÓDULO 10 · Alertas básicas"

ALERTS_JSON=$(curl -sf --max-time 5 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/rules" 2>/dev/null || echo "")
if [[ -n "$ALERTS_JSON" ]]; then
  ALERT_COUNT=$(echo "$ALERTS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    rules=[r for g in d['data']['groups'] for r in g['rules'] if r['type']=='alerting']; \
    print(len(rules))" 2>/dev/null || echo "0")
  info "Reglas de alerta configuradas: $ALERT_COUNT"
  [[ "$ALERT_COUNT" -gt 0 ]] 2>/dev/null \
    && ok "Alertas configuradas: $ALERT_COUNT reglas" \
    || warn "Sin reglas de alerta — sin notificación automática de fallos"

  # Alertas disparadas actualmente
  FIRING=$(echo "$ALERTS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    rules=[r for g in d['data']['groups'] for r in g['rules'] \
           if r['type']=='alerting' and r.get('state')=='firing']; \
    print(f'Alertas FIRING: {len(rules)}')" 2>/dev/null || echo "?")
  info "  $FIRING"

  # Alertas de base esperadas para Kafka lag, Flink jobs, MinIO
  PROM_ALERTS=$(curl -sf --max-time 5 \
    "http://localhost:${PROMETHEUS_PORT}/api/v1/rules" 2>/dev/null | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    names=[r['name'] for g in d['data']['groups'] for r in g['rules'] \
           if r['type']=='alerting']; print('\n'.join(names))" 2>/dev/null || echo "")

  for EXPECTED_ALERT in "NodeDown" "HighCpuUsage" "HighMemoryUsage" "DiskSpaceLow"; do
    echo "$PROM_ALERTS" | grep -qi "$EXPECTED_ALERT" \
      && ok "  Alerta presente: $EXPECTED_ALERT" \
      || warn "  Alerta no configurada: $EXPECTED_ALERT (recomendada)"
  done
else
  warn "No se pudo consultar reglas de Prometheus"
fi

# ===========================================================================
# MÓDULO 11 – Directorios y retención de datos
# ===========================================================================
section "MÓDULO 11 · Directorios y retención"

for DIR in "$PROMETHEUS_DATA" "$GRAFANA_DATA"; do
  if [[ -d "$DIR" ]]; then
    SIZE=$(du -sh "$DIR" 2>/dev/null | cut -f1 || echo "?")
    ok "Directorio existe: $DIR  (uso: $SIZE)"
  else
    fail "Directorio de datos faltante: $DIR"
  fi
done

for DIR in "$LOKI_DATA" "$TEMPO_DATA"; do
  if [[ -d "$DIR" ]]; then
    SIZE=$(du -sh "$DIR" 2>/dev/null | cut -f1 || echo "?")
    ok "Directorio existe: $DIR  (uso: $SIZE)"
  else
    opt "Directorio opcional no encontrado: $DIR (Loki/Tempo pueden no estar instalados)"
  fi
done

# Espacio restante en /data
DISK_AVAIL=$(df -h /data 2>/dev/null | tail -1 | awk '{print $4}' || echo "?")
info "Espacio disponible en /data: $DISK_AVAIL"

# ===========================================================================
# MÓDULO 12 – systemd persistencia de todos los servicios
# ===========================================================================
section "MÓDULO 12 · systemd – persistencia de servicios"

check_svc() {
  local UNIT=$1 DESC=$2 MANDATORY=${3:-true}
  if systemctl is-active --quiet "$UNIT" 2>/dev/null; then
    ok "Servicio activo: $UNIT ($DESC)"
    systemctl is-enabled --quiet "$UNIT" 2>/dev/null \
      && ok "  ↳ Habilitado en boot" \
      || { $MANDATORY && fail "  ↳ NO habilitado en boot — no sobrevivirá reinicio" \
           || warn "  ↳ NO habilitado en boot"; }
  else
    $MANDATORY \
      && warn "Servicio '$UNIT' no activo como sistema (probar también container-$UNIT)" \
      || opt "Servicio opcional '$UNIT' no activo ($DESC)"
  fi
}

check_svc "prometheus"           "Prometheus métricas"           true
check_svc "grafana-server"       "Grafana dashboards"            true
check_svc "grafana"              "Grafana (alt naming)"          true
check_svc "loki"                 "Loki logs"                     false
check_svc "tempo"                "Tempo trazas"                  false
check_svc "otelcol"              "OTel Collector"                false
check_svc "otelcol-contrib"      "OTel Collector contrib"        false
check_svc "node_exporter"        "node-exporter"                 true
check_svc "alloy"                "Grafana Alloy agente"          true
check_svc "redis-sentinel"       "Redis Sentinel"                true

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Stack Observabilidad DC Principal (pbigd-plat-obs01)           ║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Total checks  : $(printf '%-50s' "$TOTAL")║${NC}"
log "${GREEN}${BOLD}║   PASS          : $(printf '%-50s' "$PASS")║${NC}"
log "${RED}${BOLD}║   FAIL          : $(printf '%-50s' "$FAIL")║${NC}"
log "${YELLOW}${BOLD}║   WARN          : $(printf '%-50s' "$WARN")║${NC}"
log "${BOLD}╠══════════════════════════════════════════════════════════════════╣${NC}"
log "${BOLD}║   Log           : $(printf '%-50s' "$LOG_FILE")║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

if [[ $FAIL -eq 0 && $WARN -le 5 ]]; then
  log "${GREEN}${BOLD}  ✔  Stack de observabilidad DC Principal OPERATIVO${NC}"
elif [[ $FAIL -eq 0 ]]; then
  log "${YELLOW}${BOLD}  ⚠  Stack funcional con $WARN advertencias — revisar componentes opcionales${NC}"
else
  log "${RED}${BOLD}  ✘  $FAIL fallos críticos — observabilidad degradada o inoperativa${NC}"
fi

log ""
exit $FAIL
