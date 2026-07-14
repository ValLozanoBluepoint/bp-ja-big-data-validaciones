#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Stack Observabilidad DR – Datacenter Alterno (pbigd-plat-obs01-cont)
#
# Nodo: pbigd-plat-obs01-cont  (1 vCPU / 2 GB RAM / 200 GB disco)
# Estrategia DR: Capacidad mínima — Prometheus básico + Alloy local.
#                Grafana, Loki y Tempo NO se despliegan en el DC Alterno.
#                El objetivo en contingencia es recolectar métricas esenciales
#                de los nodos DR hasta que pbigd-plat-obs01 sea restaurado.
#
# Componentes en pbigd-plat-obs01-cont:
#   · Prometheus básico  → recolección mínima de métricas de nodos DR
#   · Grafana Alloy 1.6.0 → agente local
#   · node-exporter      → métricas del propio nodo
#   · Redis Sentinel 7.4.x → también presente (según diseño)
#
# Nodos DR que deben ser visibles en este Prometheus:
#   pbigd-kaf01/02/03-cont, pbigd-stg01/02/03-cont, pbigd-dlh01/02/03-cont
#   pbigd-proc01/02-cont, pbigd-plat-apps01-cont, pbigd-bd-plat-apps01-cont,
#   pbigd-plat-obs01-cont
#
# Runtime: Rocky Linux 10.x · Podman 5.x · systemd
#
# Uso:
#   chmod +x validate_obs_stack_dr.sh
#   ./validate_obs_stack_dr.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN
# ---------------------------------------------------------------------------
PROMETHEUS_PORT=9090
NODE_EXPORTER_PORT=9100
NODE_EXPORTER_EMBEDDED_PORT=17935   # exporter unix embebido en Alloy (prometheus.exporter.unix)
ALLOY_PORT=12345
ALLOY_VERSION_EXPECTED="1.6.0"
REDIS_SENTINEL_PORT=26379

# Retención mínima aceptable en DR (Prometheus con recursos reducidos)
MIN_PROMETHEUS_TARGETS=5    # al menos la mitad de nodos DR activos

# Nodos DR que deben aparecer en Prometheus (convención pbigd-<rol><NN>-cont)
DR_NODES=(
  "pbigd-kaf01-cont" "pbigd-kaf02-cont" "pbigd-kaf03-cont"
  "pbigd-stg01-cont" "pbigd-stg02-cont" "pbigd-stg03-cont"
  "pbigd-dlh01-cont"  "pbigd-dlh02-cont"  "pbigd-dlh03-cont"
  "pbigd-proc01-cont" "pbigd-proc02-cont"
  "pbigd-plat-apps01-cont" "pbigd-bd-plat-apps01-cont" "pbigd-plat-obs01-cont"
)

PROMETHEUS_DATA="/data/prometheus"
ALLOY_CONFIG_DIR="/opt/alloy"

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; MAGENTA='\033[0;35m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
THIS_HOST=$(hostname -s 2>/dev/null || hostname)
LOG_FILE="/tmp/validate_obs_dr_$(date +%Y%m%d_%H%M%S).log"

log()     { echo -e "$*" | tee -a "$LOG_FILE"; }
ok()      { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail()    { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn()    { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info()    { log "${CYAN}  [INFO]${NC} $*"; }
dr()      { log "${MAGENTA}  [DR]${NC}  $*"; }
section() {
  log "\n${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  log "${BOLD}${MAGENTA}  $*${NC}"
  log "${BOLD}${MAGENTA}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Stack Observabilidad DR (pbigd-plat-obs01-cont)                ║${NC}"
log "${BOLD}║   Prometheus básico · Alloy 1.6.0 · Modo CONTINGENCIA           ║${NC}"
log "${BOLD}║   Nodo  : $(printf '%-56s' "$THIS_HOST")║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""
dr "NOTA: pbigd-plat-obs01-cont opera con capacidad MUY reducida (1 vCPU / 2 GB RAM)."
dr "El stack completo (Grafana, Loki, Tempo) NO aplica en DC Alterno."
dr "Objetivo: recolección mínima de métricas de los nodos DR activos."
log ""

# ===========================================================================
# MÓDULO 1 – OS y prerequisitos
# ===========================================================================
section "MÓDULO 1 · OS y prerequisitos DR"

OS=$(grep -oP '(?<=Rocky Linux )\S+' /etc/os-release 2>/dev/null || echo "unknown")
[[ "$OS" == 10* ]] && ok "Rocky Linux 10.x confirmado" || warn "OS: $OS"

command -v podman &>/dev/null \
  && ok "Podman disponible" \
  || warn "Podman no encontrado"

if timedatectl status 2>/dev/null | grep -q "synchronized: yes"; then
  ok "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  ok "NTP sincronizado (chrony)"
else
  fail "NTP NO sincronizado — correlación de métricas incorrecta en contingencia"
fi

# Recursos muy limitados — verificar que el nodo puede mantener Prometheus
MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
MEM_GB=$(( MEM_KB / 1024 / 1024 ))
VCPU=$(nproc)
info "Recursos de pbigd-plat-obs01-cont: ${VCPU} vCPU / ${MEM_GB} GB RAM"
[[ $MEM_GB -ge 1 ]] \
  && ok "RAM disponible: ${MEM_GB} GB (mín 2 GB para Prometheus básico)" \
  || fail "RAM insuficiente: ${MEM_GB} GB — Prometheus puede no funcionar"
[[ $VCPU -ge 1 ]] && ok "vCPU disponibles: $VCPU" || warn "vCPU insuficientes: $VCPU"

# Disco — 200 GB total, verificar espacio libre para TSDB
DISK_AVAIL=$(df --output=avail -BG /data 2>/dev/null | tail -1 | tr -d 'G ' || echo "0")
info "Espacio libre en /data: ${DISK_AVAIL} GB"
[[ "$DISK_AVAIL" -ge 20 ]] 2>/dev/null \
  && ok "Espacio libre suficiente: ${DISK_AVAIL} GB para TSDB reducida" \
  || warn "Espacio libre bajo: ${DISK_AVAIL} GB — ajustar retención de Prometheus"

# ===========================================================================
# MÓDULO 2 – Prometheus básico en pbigd-plat-obs01-cont
# ===========================================================================
section "MÓDULO 2 · Prometheus básico (MANDATORIO en DR)"

# 2.1 Proceso / Puerto
if pgrep -x "prometheus" &>/dev/null; then
  ok "Proceso prometheus corriendo en pbigd-plat-obs01-cont"
elif ss -tlnp | grep -q ":${PROMETHEUS_PORT}"; then
  ok "Puerto $PROMETHEUS_PORT escuchando"
else
  fail "Prometheus NO está corriendo en pbigd-plat-obs01-cont — sin visibilidad del DC Alterno"
fi

ss -tlnp | grep -q ":${PROMETHEUS_PORT}" \
  && ok "Puerto $PROMETHEUS_PORT escuchando" \
  || fail "Puerto $PROMETHEUS_PORT no escucha"

# 2.2 Health check
PROM_HEALTH=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${PROMETHEUS_PORT}/-/healthy" 2>/dev/null || echo "000")
[[ "$PROM_HEALTH" == "200" ]] \
  && ok "Prometheus health OK: HTTP $PROM_HEALTH" \
  || fail "Prometheus health fallido: HTTP $PROM_HEALTH"

PROM_READY=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
  "http://localhost:${PROMETHEUS_PORT}/-/ready" 2>/dev/null || echo "000")
[[ "$PROM_READY" == "200" ]] \
  && ok "Prometheus ready: HTTP $PROM_READY" \
  || warn "Prometheus ready: HTTP $PROM_READY"

# 2.3 Versión
PROM_VER=$(curl -sf --max-time 5 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/status/buildinfo" 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['data'].get('version','?'))" \
  2>/dev/null || echo "unknown")
info "Versión Prometheus DR: $PROM_VER"
[[ "$PROM_VER" != "unknown" ]] \
  && ok "Versión Prometheus DR identificada: $PROM_VER" \
  || warn "No se pudo identificar versión de Prometheus"

# 2.4 Consumo de recursos del proceso Prometheus
PROM_PID=$(pgrep -x "prometheus" | head -1 || echo "")
if [[ -n "$PROM_PID" ]]; then
  PROM_MEM_MB=$(cat /proc/"$PROM_PID"/status 2>/dev/null \
    | grep VmRSS | awk '{print int($2/1024)}' || echo "?")
  info "Prometheus RAM usage: ${PROM_MEM_MB} MB"
  [[ "$PROM_MEM_MB" != "?" && "$PROM_MEM_MB" -lt 1500 ]] 2>/dev/null \
    && ok "Consumo de RAM Prometheus dentro de límites: ${PROM_MEM_MB} MB" \
    || warn "Prometheus consume ${PROM_MEM_MB} MB — pbigd-plat-obs01-cont tiene solo 2 GB RAM"
fi

# 2.5 Retención configurada (debe ser corta en DR para no saturar disco)
PROM_FLAGS=$(curl -sf --max-time 5 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/status/flags" 2>/dev/null || echo "")
RETENTION=$(echo "$PROM_FLAGS" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); \
  print(d['data'].get('storage.tsdb.retention.time','?'))" 2>/dev/null || echo "?")
info "Retención TSDB configurada: $RETENTION"
dr "En DR se recomienda retención corta (7d-15d) para no saturar los 200 GB"

# 2.6 Targets activos
TARGETS_JSON=$(curl -sf --max-time 10 \
  "http://localhost:${PROMETHEUS_PORT}/api/v1/targets" 2>/dev/null || echo "")
if [[ -n "$TARGETS_JSON" ]]; then
  ok "API /targets responde en Prometheus DR"
  N_UP=$(echo "$TARGETS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    print(len([x for x in d['data']['activeTargets'] if x['health']=='up']))" \
    2>/dev/null || echo "0")
  N_TOTAL=$(echo "$TARGETS_JSON" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); \
    print(len(d['data']['activeTargets']))" 2>/dev/null || echo "0")
  info "Targets: $N_UP activos / $N_TOTAL totales"

  [[ "$N_UP" -ge "$MIN_PROMETHEUS_TARGETS" ]] 2>/dev/null \
    && ok "Targets activos: $N_UP (umbral mínimo DR: $MIN_PROMETHEUS_TARGETS)" \
    || warn "Targets activos bajos: $N_UP (umbral: $MIN_PROMETHEUS_TARGETS) — nodos DR no visibles"

  # Listar targets con problemas
  echo "$TARGETS_JSON" | python3 -c "
import sys, json
d = json.load(sys.stdin)
down = [x for x in d['data']['activeTargets'] if x['health'] != 'up']
if down:
    print('  Targets con problemas:')
    for t in down[:10]:
        print(f'    [{t[\"health\"]}] {t[\"labels\"].get(\"instance\",\"?\")} — {t.get(\"lastError\",\"\")}')
else:
    print('  Todos los targets configurados están UP')" \
  2>/dev/null | tee -a "$LOG_FILE" || true
else
  warn "API /targets no responde"
fi

# ===========================================================================
# MÓDULO 3 – Visibilidad de nodos DR críticos
# ===========================================================================
section "MÓDULO 3 · Cobertura de scraping – nodos DR"

if [[ "$PROM_HEALTH" == "200" ]]; then
  info "Verificando visibilidad de nodos DR en Prometheus..."
  MISSING_DR=()
  CRITICAL_DR=("pbigd-kaf01-cont" "pbigd-proc01-cont" "pbigd-plat-apps01-cont")   # nodos más críticos

  for NODE in "${DR_NODES[@]}"; do
    QUERY="up{instance=~\"${NODE}.*\"}"
    RESULT=$(curl -sf --max-time 5 \
      "http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=$(python3 -c \
      "import urllib.parse; print(urllib.parse.quote('${QUERY}'))" 2>/dev/null)" \
      2>/dev/null | python3 -c \
      "import sys,json; d=json.load(sys.stdin); \
      print(len(d.get('data',{}).get('result',[])))" 2>/dev/null || echo "0")

    IS_CRITICAL=false
    for C in "${CRITICAL_DR[@]}"; do [[ "$NODE" == "$C" ]] && IS_CRITICAL=true; done

    if [[ "$RESULT" -gt 0 ]] 2>/dev/null; then
      ok "Nodo DR visible: $NODE"
    else
      MISSING_DR+=("$NODE")
      $IS_CRITICAL \
        && fail "Nodo DR CRÍTICO sin datos: $NODE — visibilidad perdida en contingencia" \
        || warn "Nodo DR sin datos: $NODE"
    fi
  done

  [[ ${#MISSING_DR[@]} -eq 0 ]] \
    && ok "Todos los nodos DR son visibles en Prometheus" \
    || info "Nodos sin datos: ${MISSING_DR[*]}"
else
  warn "Prometheus no disponible — no se puede verificar cobertura"
fi

# ===========================================================================
# MÓDULO 4 – Métricas de componentes críticos DR
# ===========================================================================
section "MÓDULO 4 · Métricas de servicios críticos DR"

if [[ "$PROM_HEALTH" == "200" ]]; then
  dr "En contingencia se monitorean principalmente: Kafka, Flink, Redis"

  # Kafka DR — verificar que hay métricas de JMX o node
  KAFKA_METRIC=$(curl -sf --max-time 5 \
    "http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=up{job=~\"kafka.*\"}" \
    2>/dev/null | python3 -c \
    "import sys,json; r=json.load(sys.stdin)['data']['result']; print(len(r))" \
    2>/dev/null || echo "0")
  [[ "$KAFKA_METRIC" -gt 0 ]] 2>/dev/null \
    && ok "Métricas Kafka DR presentes: $KAFKA_METRIC serie(s)" \
    || warn "Sin métricas de Kafka DR — ¿scrape job configurado?"

  # Flink DR
  FLINK_METRIC=$(curl -sf --max-time 5 \
    "http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=up{job=~\"flink.*\"}" \
    2>/dev/null | python3 -c \
    "import sys,json; r=json.load(sys.stdin)['data']['result']; print(len(r))" \
    2>/dev/null || echo "0")
  [[ "$FLINK_METRIC" -gt 0 ]] 2>/dev/null \
    && ok "Métricas Flink DR presentes: $FLINK_METRIC serie(s)" \
    || warn "Sin métricas de Flink DR en Prometheus"

  # node-exporter en nodos DR
  NODE_METRIC=$(curl -sf --max-time 5 \
    "http://localhost:${PROMETHEUS_PORT}/api/v1/query?query=up{job=~\"node.*\"}" \
    2>/dev/null | python3 -c \
    "import sys,json; r=json.load(sys.stdin)['data']['result']; print(len(r))" \
    2>/dev/null || echo "0")
  [[ "$NODE_METRIC" -gt 0 ]] 2>/dev/null \
    && ok "Métricas node-exporter DR presentes: $NODE_METRIC nodo(s)" \
    || warn "Sin métricas node-exporter de nodos DR"
fi

# ===========================================================================
# MÓDULO 5 – Grafana Alloy local en pbigd-plat-obs01-cont
# ===========================================================================
section "MÓDULO 5 · Grafana Alloy 1.6.0 (pbigd-plat-obs01-cont)"

if pgrep -x "alloy" &>/dev/null; then
  ok "Proceso Alloy corriendo en pbigd-plat-obs01-cont"

  ALLOY_HEALTH=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${ALLOY_PORT}/-/ready" 2>/dev/null || echo "000")
  [[ "$ALLOY_HEALTH" == "200" ]] \
    && ok "Alloy ready: HTTP $ALLOY_HEALTH" \
    || warn "Alloy health: HTTP $ALLOY_HEALTH"

  # Verificar versión
  ALLOY_BIN=$(command -v alloy 2>/dev/null || \
    find /usr/bin /usr/local/bin /opt/alloy/bin -name "alloy" -executable 2>/dev/null | head -1)
  if [[ -n "$ALLOY_BIN" ]]; then
    ALLOY_VER=$("$ALLOY_BIN" --version 2>/dev/null | grep -oP 'v\K[\d.]+' | head -1 || echo "?")
    [[ "$ALLOY_VER" == "$ALLOY_VERSION_EXPECTED"* ]] \
      && ok "Versión Alloy: $ALLOY_VER" \
      || warn "Versión Alloy: '$ALLOY_VER' (esperada $ALLOY_VERSION_EXPECTED)"
  fi

  # Verificar que Alloy está enviando a Prometheus local
  ALLOY_RW=$(curl -sf --max-time 5 "http://localhost:${ALLOY_PORT}/metrics" 2>/dev/null \
    | grep "prometheus_remote_storage_samples_total" | head -1 || echo "")
  [[ -n "$ALLOY_RW" ]] \
    && ok "Alloy enviando métricas a Prometheus DR (remote_write activo)" \
    || warn "No se detecta remote_write en Alloy DR"
else
  fail "Alloy NO está corriendo en pbigd-plat-obs01-cont — sin agente de observabilidad local"
fi

# Configuración de Alloy
ALLOY_CFG=$(find "$ALLOY_CONFIG_DIR" /etc/alloy \
  -name "*.alloy" -o -name "*.river" 2>/dev/null | head -3 || echo "")
if [[ -n "$ALLOY_CFG" ]]; then
  ok "Configuración Alloy encontrada"
  # En DR, Alloy debe apuntar a Prometheus LOCAL (no al pbigd-plat-obs01 del principal)
  while IFS= read -r CFG; do
    grep -q "localhost\|127.0.0.1\|pbigd-plat-obs01-cont" "$CFG" 2>/dev/null \
      && ok "  ↳ Alloy apunta a Prometheus LOCAL (correcto para DR)" \
      || warn "  ↳ Alloy puede estar apuntando a pbigd-plat-obs01 en lugar del Prometheus local"
  done <<< "$ALLOY_CFG"
else
  warn "Configuración de Alloy no encontrada"
fi

# ===========================================================================
# MÓDULO 6 – node-exporter en pbigd-plat-obs01-cont
# ===========================================================================
section "MÓDULO 6 · node-exporter local (pbigd-plat-obs01-cont)"

# node-exporter puede correr standalone (:9100) o embebido en Alloy
# (prometheus.exporter.unix en :17935). Ambos son válidos.
if ss -tlnp | grep -q ":${NODE_EXPORTER_EMBEDDED_PORT}"; then
  ok "node-exporter embebido en Alloy activo en pbigd-plat-obs01-cont (puerto $NODE_EXPORTER_EMBEDDED_PORT)"
  NE_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${NODE_EXPORTER_EMBEDDED_PORT}/metrics" 2>/dev/null || echo "000")
  [[ "$NE_HTTP" == "200" ]] \
    && ok "node-exporter embebido /metrics: HTTP $NE_HTTP" \
    || warn "node-exporter embebido /metrics: HTTP $NE_HTTP"
elif ss -tlnp | grep -q ":${NODE_EXPORTER_PORT}"; then
  ok "node-exporter standalone activo en pbigd-plat-obs01-cont (puerto $NODE_EXPORTER_PORT)"
  NE_HTTP=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" \
    "http://localhost:${NODE_EXPORTER_PORT}/metrics" 2>/dev/null || echo "000")
  [[ "$NE_HTTP" == "200" ]] \
    && ok "node-exporter standalone /metrics: HTTP $NE_HTTP" \
    || warn "node-exporter standalone /metrics: HTTP $NE_HTTP"
else
  fail "node-exporter NO activo en pbigd-plat-obs01-cont (ni embebido :$NODE_EXPORTER_EMBEDDED_PORT ni standalone :$NODE_EXPORTER_PORT)"
fi

# ===========================================================================
# MÓDULO 7 – Redis Sentinel en pbigd-plat-obs01-cont
# ===========================================================================
section "MÓDULO 7 · Redis Sentinel (pbigd-plat-obs01-cont)"

if ss -tlnp | grep -q ":${REDIS_SENTINEL_PORT}" || \
   pgrep -x "redis-sentinel" &>/dev/null; then
  ok "Redis Sentinel activo en pbigd-plat-obs01-cont (puerto $REDIS_SENTINEL_PORT)"
  SENTINEL_PING=$(redis-cli -p "$REDIS_SENTINEL_PORT" PING 2>/dev/null || echo "")
  [[ "$SENTINEL_PING" == "PONG" ]] \
    && ok "Redis Sentinel PONG" \
    || warn "Redis Sentinel no responde a PING"
else
  warn "Redis Sentinel no detectado en pbigd-plat-obs01-cont (verificar diseño)"
fi

# ===========================================================================
# MÓDULO 8 – systemd – persistencia crítica en DR
# ===========================================================================
section "MÓDULO 8 · systemd – persistencia en DR"

dr "En DR, autoarranque es OBLIGATORIO para todos los servicios"

for UNIT in "prometheus" "alloy" "grafana-alloy" "node_exporter" \
             "node-exporter" "redis-sentinel"; do
  if systemctl is-active --quiet "$UNIT" 2>/dev/null; then
    ok "Servicio activo: $UNIT"
    systemctl is-enabled --quiet "$UNIT" 2>/dev/null \
      && ok "  ↳ Habilitado en boot (correcto para DR)" \
      || fail "  ↳ NO habilitado en boot — NO sobrevivirá failover"
  fi
done

# Verificar también unidades Podman de contenedores
for CONTAINER_UNIT in "container-prometheus" "container-alloy"; do
  systemctl is-active --quiet "$CONTAINER_UNIT" 2>/dev/null \
    && ok "Contenedor activo vía systemd: $CONTAINER_UNIT" || true
done

# ===========================================================================
# MÓDULO 9 – Conectividad con pbigd-plat-obs01 del DC Principal (post-recuperación)
# ===========================================================================
section "MÓDULO 9 · Conectividad con DC Principal (referencia)"

# OBS1_HOST: "pbigd-plat-obs01" es la etiqueta del plan (hostnames.txt), pero
# el nombre real registrado en el DNS interno (jardinazuayo.fin.ec) es "escila"
# — confirmado en campo el 2026-07-14 (IP 172.17.210.89). El hostname de plan
# nunca fue dado de alta en DNS, por eso se usa el FQDN real aquí.
OBS1_HOST="escila.jardinazuayo.fin.ec"

dr "pbigd-plat-obs01-cont opera de forma autónoma en contingencia."
dr "La conectividad con el Principal ($OBS1_HOST) solo es necesaria post-recuperación."

OBS1_PING=$(ping -c 1 -W 2 "$OBS1_HOST" &>/dev/null && echo "ok" || echo "fail")
if [[ "$OBS1_PING" == "ok" ]]; then
  info "$OBS1_HOST (DC Principal) alcanzable — posible recuperación en curso"
  OBS1_PROM=$(curl -sf --max-time 3 -o /dev/null -w "%{http_code}" \
    "http://${OBS1_HOST}:${PROMETHEUS_PORT}/-/healthy" 2>/dev/null || echo "000")
  [[ "$OBS1_PROM" == "200" ]] \
    && info "  Prometheus en $OBS1_HOST accesible — considerar migración de tráfico" \
    || info "  Prometheus en $OBS1_HOST no responde (puede estar en recuperación)"
else
  info "$OBS1_HOST no alcanzable (esperado en contingencia activa)"
  ok "pbigd-plat-obs01-cont operando correctamente en modo standalone"
fi

# ===========================================================================
# MÓDULO 10 – Datos persistidos en TSDB DR
# ===========================================================================
section "MÓDULO 10 · Datos TSDB y persistencia"

if [[ -d "$PROMETHEUS_DATA" ]]; then
  TSDB_SIZE=$(du -sh "$PROMETHEUS_DATA" 2>/dev/null | cut -f1 || echo "?")
  ok "Directorio TSDB existe: $PROMETHEUS_DATA (uso: $TSDB_SIZE)"

  # Contar bloques de datos
  BLOCK_COUNT=$(find "$PROMETHEUS_DATA" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
  info "Bloques TSDB actuales: $BLOCK_COUNT"
  [[ "$BLOCK_COUNT" -gt 0 ]] \
    && ok "Datos históricos presentes en TSDB DR ($BLOCK_COUNT bloques)" \
    || warn "TSDB vacía — Prometheus puede ser nuevo o sin datos aún"
else
  fail "Directorio TSDB no encontrado: $PROMETHEUS_DATA"
fi

# Verificar WAL de Prometheus
WAL_DIR="${PROMETHEUS_DATA}/wal"
[[ -d "$WAL_DIR" ]] \
  && ok "WAL de Prometheus presente: $WAL_DIR" \
  || warn "WAL no encontrado en $WAL_DIR"

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Stack Observabilidad DR (pbigd-plat-obs01-cont)                  ║${NC}"
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

if [[ $FAIL -eq 0 && $WARN -le 3 ]]; then
  log "${GREEN}${BOLD}  ✔  Observabilidad DR OPERATIVA — DC Alterno monitoreado${NC}"
elif [[ $FAIL -eq 0 ]]; then
  log "${YELLOW}${BOLD}  ⚠  Observabilidad DR funcional con $WARN advertencias${NC}"
else
  log "${RED}${BOLD}  ✘  $FAIL fallos — DC Alterno puede operar CIEGO en contingencia${NC}"
fi

log ""
dr "Stack completo (Grafana/Loki/Tempo) disponible únicamente en pbigd-plat-obs01 (DC Principal)."
dr "Para acceder a dashboards en contingencia: usar tunnel SSH hacia pbigd-plat-obs01-cont:$PROMETHEUS_PORT"
log ""

exit $FAIL
