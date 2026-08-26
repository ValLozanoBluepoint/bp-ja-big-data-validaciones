#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster PostgreSQL HA DR (Datacenter Alterno)
#  Nodos : pbigd-dlh01-cont | pbigd-dlh02-cont | pbigd-dlh03-cont
#  Stack : etcd 3.5.16 + PostgreSQL 17.10 + Patroni 4.1 en LOS 3 NODOS
#          + HAProxy 2.8.6 + Keepalived en dlh01-cont/dlh02-cont + Grafana Alloy
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 5432 (Postgres) | 8008 (Patroni REST API) | 2379/2380 (etcd)
#            6432 (PgBouncer) | 15432 (HAProxy WRITE) | 15433 (HAProxy READ)
#            | 18404 (HAProxy Stats UI)
#
#  Rol DR (mismo modelo que Principal — ver context/ha arquitectura.png,
#  ajustado a DR contra context/hostnames.txt): LOS 3 NODOS corren el stack
#  completo de datos (etcd+Postgres+Patroni) y participan en la replicación
#  síncrona; ningún nodo es un witness liviano de solo-etcd. Solo dlh01-cont
#  y dlh02-cont llevan además la capa de acceso (PgBouncer+HAProxy+Keepalived).
#  A diferencia de Kafka/Redis (que NO se degradan en DR), el clúster
#  PostgreSQL HA sí opera con recursos reducidos en el DC Alterno según el
#  plan de implementación — pendiente confirmar el dimensionamiento exacto
#  por nodo ahora que dlh03-cont deja de tratarse como witness.
#
#  Nota de alcance: este script valida ÚNICAMENTE el clúster PostgreSQL HA
#  DR (pbigd-dlh01-cont/02-cont/03-cont). La instancia PostgreSQL de
#  METADATOS (platform-db-dr) queda fuera de alcance — ver
#  postgresql/provision_metadatos_postgresql.sh.
#
#  Uso:
#    chmod +x validate_postgresql_dr.sh
#    ./validate_postgresql_dr.sh [--pg-port puerto] [--patroni-port puerto]
#                                 [--skip-failover-drill]
# =============================================================================

set -uo pipefail

# ── Colores y helpers ────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS="[${GREEN}PASS${NC}]"
FAIL="[${RED}FAIL${NC}]"
WARN="[${YELLOW}WARN${NC}]"
INFO="[${CYAN}INFO${NC}]"

ERRORS=0
WARNINGS=0
PASS_COUNT=0

pass()  { echo -e "${PASS} $*"; PASS_COUNT=$((PASS_COUNT+1)); }
fail()  { echo -e "${FAIL} $*"; ERRORS=$((ERRORS+1)); }
warn()  { echo -e "${WARN} $*"; WARNINGS=$((WARNINGS+1)); }
info()  { echo -e "${INFO} $*"; }
header(){ echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════════${NC}"; \
          echo -e "${BOLD}${CYAN}  $*${NC}"; \
          echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════${NC}"; }

# ── Parámetros ───────────────────────────────────────────────────────────────
PG_PORT=5432
PATRONI_PORT=8008
SKIP_FAILOVER_DRILL=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pg-port)             PG_PORT="$2"; shift 2 ;;
    --patroni-port)         PATRONI_PORT="$2"; shift 2 ;;
    --skip-failover-drill)  SKIP_FAILOVER_DRILL=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

# ── Configuración del clúster DR ────────────────────────────────────────────
# Nombres de host confirmados contra context/hostnames.txt (sección DR).
CLUSTER_NAME="PostgreSQL HA DR (Datacenter Alterno)"
ALL_NODES=("pbigd-dlh01-cont" "pbigd-dlh02-cont" "pbigd-dlh03-cont")  # los 3 corren etcd+Postgres+Patroni
ACCESS_NODES=("pbigd-dlh01-cont" "pbigd-dlh02-cont")                 # además llevan PgBouncer+HAProxy+Keepalived
ETCD_CLIENT_PORT=2379
ETCD_PEER_PORT=2380
HAPROXY_STATS_PORT=18404
HAPROXY_WRITE_PORT=15432
HAPROXY_READ_PORT=15433
PGBOUNCER_PORT=6432
PG_DATA="/var/lib/postgresql/17/data"
ETCD_DATA="/var/lib/etcd"
PG_LOGS="/var/log/postgresql"
PG_VERSION_EXPECTED="17.10"
PATRONI_VERSION_EXPECTED="4.1"
MIN_PG_REPLICAS_REQUIRED=2          # dlh02-cont + dlh03-cont: 2 réplicas síncronas requeridas
MIN_ETCD_MEMBERS_REQUIRED=3
HOSTNAME_ACTUAL=$(hostname -s)

# Detectar rol del nodo actual: todos corren Postgres/Patroni/etcd; solo
# ACCESS_NODES llevan además la capa de acceso (PgBouncer/HAProxy/Keepalived).
NODE_ROLE="desconocido"
IS_ACCESS_NODE=false
for n in "${ACCESS_NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n (Postgres/Patroni/etcd + acceso PgBouncer/HAProxy, DR)"; IS_ACCESS_NODE=true; break; fi
done
if [[ "$NODE_ROLE" == "desconocido" ]]; then
  for n in "${ALL_NODES[@]}"; do
    if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n (Postgres/Patroni/etcd, sin capa de acceso, DR)"; break; fi
  done
fi

CONTAINER_PG=$(podman ps --format "{{.Names}}\t{{.Image}}" 2>/dev/null | \
  awk -F'\t' '$2!="" && ($1 ~ /postgres|patroni/ || $2 ~ /postgres|patroni/) {print $1; exit}')

# ── Banner ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  PostgreSQL HA + Patroni + etcd · Clúster DR (Datacenter Alterno)"
echo -e "${NC}"
echo -e "${BOLD}  Cooperativa Jardín Azuayo — Proyecto Big Data${NC}"
echo -e "  Cluster: ${BOLD}${CYAN}${CLUSTER_NAME}${NC}"
echo -e "  Nodo actual: ${BOLD}${HOSTNAME_ACTUAL}${NC}  (rol detectado: ${NODE_ROLE})"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  ${YELLOW}${BOLD}Nota: los 3 nodos llevan réplica real de Postgres en DR (a diferencia de Kafka/Redis${NC}"
echo -e "  ${YELLOW}que no se degradan). Solo dlh01-cont/dlh02-cont exponen la capa de acceso.${NC}"
echo ""

# =============================================================================
# SECCIÓN 1 — SISTEMA OPERATIVO Y RUNTIME
# =============================================================================
header "1. SISTEMA OPERATIVO Y RUNTIME DR"

info "1.1 Verificando sistema operativo..."
if grep -qi "rocky linux 10" /etc/os-release 2>/dev/null; then
  OS_VER=$(grep "VERSION=" /etc/os-release | head -1 | tr -d '"' | cut -d= -f2)
  pass "SO: Rocky Linux — $OS_VER"
else
  OS_VER=$(grep "PRETTY_NAME" /etc/os-release 2>/dev/null | cut -d= -f2 | tr -d '"' || echo "no detectado")
  fail "SO esperado: Rocky Linux 10.x — encontrado: $OS_VER"
fi

info "1.2 Versión del kernel..."
pass "Kernel: $(uname -r)"

info "1.3 Verificando Podman..."
if command -v podman &>/dev/null; then
  POD_VER=$(podman --version | awk '{print $3}')
  POD_MAJ=$(echo "$POD_VER" | cut -d. -f1)
  [[ "$POD_MAJ" -ge 5 ]] && pass "Podman $POD_VER instalado (>= 5.x requerido)" || fail "Podman $POD_VER < 5.x requerido"
else
  warn "Podman no encontrado en PATH — el stack puede correr en bare metal"
fi

info "1.4 Verificando systemd..."
SYSSTATE=$(systemctl is-system-running 2>/dev/null) || true
[[ -z "$SYSSTATE" ]] && SYSSTATE="unknown"
[[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"

info "1.5 Verificando sincronización NTP (crítico para consenso Raft de etcd en DR)..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización — CRÍTICO en DR para consenso etcd"
fi

# 1.6 Recursos del nodo — los 3 nodos corren el stack de datos completo en
# DR (etcd+Postgres+Patroni), spec reducida frente a Principal. El
# dimensionamiento exacto por nodo ahora que dlh03-cont deja de ser witness
# está pendiente de confirmar contra el inventario real; se usa aquí la
# spec DR de "nodo completo" (2 vCPU/8GB) como umbral mínimo para los 3.
info "1.6 Verificando recursos del nodo (spec DR reducida frente a Principal)..."
MEM_TOTAL_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
MEM_TOTAL_GB=$(( MEM_TOTAL_KB / 1024 / 1024 ))
VCPU=$(nproc)
info "Recursos nodo ${HOSTNAME_ACTUAL}: ${VCPU} vCPU / ${MEM_TOTAL_GB} GB RAM"
[[ $MEM_TOTAL_GB -ge 7 ]] && pass "RAM acorde a spec DR reducida: ${MEM_TOTAL_GB} GB (spec: 8 GB)" \
  || warn "RAM por debajo de lo especificado para DR: ${MEM_TOTAL_GB} GB (esperado ~8 GB)"
[[ $VCPU -ge 2 ]] && pass "vCPU acordes a spec DR reducida: $VCPU (spec: 2)" \
  || warn "vCPU por debajo de lo especificado para DR: $VCPU (esperado 2)"

# =============================================================================
# SECCIÓN 2 — ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA
# =============================================================================
header "2. ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA DR"

REQUIRED_DIRS=("$PG_DATA" "$ETCD_DATA" "$PG_LOGS")

for dir in "${REQUIRED_DIRS[@]}"; do
  info "Verificando $dir ..."
  if [[ -d "$dir" ]]; then
    OWNER=$(stat -c '%U:%G' "$dir")
    PERMS=$(stat -c '%a' "$dir")
    USAGE=$(df -h "$dir" 2>/dev/null | awk 'NR==2{print $3"/"$2" usado ("$5")"}' || echo "N/A")
    pass "Directorio $dir existe — owner: $OWNER — perms: $PERMS — uso: $USAGE"

    FSTYPE=$(df -T "$dir" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
    [[ "$FSTYPE" == "overlay" ]] \
      && fail "$dir usa overlay filesystem — debe ser bind mount sobre partición dedicada" \
      || pass "$dir filesystem: $FSTYPE (no overlay)"
  else
    fail "Directorio $dir NO existe"
  fi
done

# =============================================================================
# SECCIÓN 3 — SERVICIOS SYSTEMD Y CONTENEDORES PODMAN
# =============================================================================
header "3. SERVICIOS SYSTEMD Y CONTENEDORES PODMAN DR"

check_systemd_unit() {
  local scope="$1" unit="$2" state enabled
  if [[ "$scope" == "usuario" ]]; then
    state=$(systemctl --user is-active "$unit" 2>/dev/null || echo "unknown")
    enabled=$(systemctl --user is-enabled "$unit" 2>/dev/null || echo "unknown")
  else
    state=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    enabled=$(systemctl is-enabled "$unit" 2>/dev/null || echo "unknown")
  fi
  if [[ "$state" == "active" ]]; then
    pass "Unidad systemd (${scope}): $unit — activa — habilitada: $enabled"
    if [[ "$scope" == "sistema" && "$enabled" != "enabled" ]]; then
      fail "  ↳ Unidad '$unit' (${scope}) NO habilitada en boot — CRÍTICO en DR: debe sobrevivir reinicios"
    elif [[ "$scope" == "usuario" && "$enabled" != "generated" && "$enabled" != "enabled" ]]; then
      fail "  ↳ Unidad '$unit' (${scope}) en estado inesperado '$enabled' — verificar sección [Install] en el Quadlet"
    fi
  else
    fail "Unidad systemd (${scope}): $unit — estado: $state — habilitada: $enabled"
  fi
}

if [[ "$IS_ACCESS_NODE" == true ]]; then
  SERVICE_PATTERN="etcd|patroni|postgres|haproxy|keepalived|pgbouncer"
else
  SERVICE_PATTERN="etcd|patroni|postgres"
  info "Nodo sin capa de acceso: se omiten unidades de haproxy/keepalived/pgbouncer"
fi

info "3.1 Verificando unidades systemd del stack HA DR (sistema y usuario/Quadlet)..."
SYSTEMD_UNITS_SYSTEM=$(systemctl list-units --type=service 2>/dev/null | grep -iE "$SERVICE_PATTERN" | awk '{print $1}' || true)
SYSTEMD_UNITS_USER=$(systemctl --user list-units --type=service 2>/dev/null | grep -iE "$SERVICE_PATTERN" | awk '{print $1}' || true)

if [[ -n "$SYSTEMD_UNITS_SYSTEM" ]]; then
  while IFS= read -r unit; do check_systemd_unit "sistema" "$unit"; done <<< "$SYSTEMD_UNITS_SYSTEM"
fi
if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  while IFS= read -r unit; do check_systemd_unit "usuario" "$unit"; done <<< "$SYSTEMD_UNITS_USER"
fi
if [[ -z "$SYSTEMD_UNITS_SYSTEM" && -z "$SYSTEMD_UNITS_USER" ]]; then
  fail "No se encontraron unidades systemd (sistema ni usuario) para el stack HA DR ($SERVICE_PATTERN)"
fi

if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  info "3.2 Verificando linger de systemd para persistencia de servicios de usuario (DR)..."
  LINGER_USER=$(whoami)
  LINGER_STATE=$(loginctl show-user "$LINGER_USER" -p Linger --value 2>/dev/null || echo "unknown")
  [[ "$LINGER_STATE" == "yes" ]] \
    && pass "Linger habilitado para usuario $LINGER_USER" \
    || fail "Linger NO habilitado para usuario $LINGER_USER (Linger=$LINGER_STATE) — corregir con: loginctl enable-linger $LINGER_USER"
fi

if [[ -n "$CONTAINER_PG" ]]; then
  info "3.3 Verificando contenedor Podman Postgres/Patroni DR..."
  CSTATUS=$(podman ps --format "{{.Status}}" --filter "name=$CONTAINER_PG" 2>/dev/null || echo "")
  echo "$CSTATUS" | grep -qi "up" \
    && pass "Contenedor: $CONTAINER_PG — estado: $CSTATUS" \
    || fail "Contenedor: $CONTAINER_PG — estado: $CSTATUS (esperado: Up)"
else
  info "3.3 Sin contenedor Podman detectado — se asume despliegue directo sobre systemd"
fi

# =============================================================================
# SECCIÓN 4 — CONECTIVIDAD DE RED
# =============================================================================
header "4. CONECTIVIDAD DE RED DR"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando — clúster DR puede no estar activo"
  fi
}

info "4.1 Puerto PostgreSQL (${PG_PORT})..."
check_port_local "$PG_PORT" "PostgreSQL"
info "4.2 Puerto Patroni REST API (${PATRONI_PORT})..."
check_port_local "$PATRONI_PORT" "Patroni REST API"

if [[ "$IS_ACCESS_NODE" == true ]]; then
  info "4.3 Puerto PgBouncer (${PGBOUNCER_PORT})..."
  check_port_local "$PGBOUNCER_PORT" "PgBouncer"
  info "4.4 Puerto HAProxy WRITE (${HAPROXY_WRITE_PORT})..."
  check_port_local "$HAPROXY_WRITE_PORT" "HAProxy WRITE → líder"
  info "4.5 Puerto HAProxy READ (${HAPROXY_READ_PORT})..."
  check_port_local "$HAPROXY_READ_PORT" "HAProxy READ → réplicas"
  info "4.6 Puerto HAProxy Stats UI (${HAPROXY_STATS_PORT})..."
  check_port_local "$HAPROXY_STATS_PORT" "HAProxy Stats UI"
else
  info "4.3-4.6 Nodo sin capa de acceso: se omiten puertos de PgBouncer/HAProxy"
fi

info "4.7 Puerto etcd cliente (${ETCD_CLIENT_PORT})..."
check_port_local "$ETCD_CLIENT_PORT" "etcd client"
info "4.8 Puerto etcd peer (${ETCD_PEER_PORT})..."
check_port_local "$ETCD_PEER_PORT" "etcd peer"

info "4.9 Conectividad entre nodos del clúster DR (${ALL_NODES[*]})..."
for NODE in "${ALL_NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$NODE"* ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE (DR): OK (rtt: ${PING_RTT}ms)"
  else
    fail "Ping a $NODE (DR): no responde — clúster DR degradado"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${ETCD_CLIENT_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${ETCD_CLIENT_PORT} (etcd, DR) alcanzable"
  else
    fail "TCP ${NODE}:${ETCD_CLIENT_PORT} (etcd, DR) no alcanzable — riesgo de pérdida de quórum"
  fi
done

# =============================================================================
# SECCIÓN 5 — VALIDACIÓN FUNCIONAL: ETCD Y PATRONI (DR)
# =============================================================================
header "5. VALIDACIÓN FUNCIONAL DR — ETCD Y PATRONI"

info "5.1 Verificando salud de etcd (quórum) en DR..."
if command -v etcdctl &>/dev/null; then
  ETCD_HEALTH=$(etcdctl endpoint health --cluster 2>&1 || echo "ERROR")
  if echo "$ETCD_HEALTH" | grep -qi "is healthy"; then
    HEALTHY_COUNT=$(echo "$ETCD_HEALTH" | grep -ci "is healthy" || true)
    if [[ "$HEALTHY_COUNT" -ge "$MIN_ETCD_MEMBERS_REQUIRED" ]]; then
      pass "etcd DR: $HEALTHY_COUNT/$MIN_ETCD_MEMBERS_REQUIRED miembros saludables (quórum completo)"
    else
      fail "etcd DR: solo $HEALTHY_COUNT/$MIN_ETCD_MEMBERS_REQUIRED miembros saludables — quórum en riesgo"
    fi
  else
    fail "etcd endpoint health (DR) no reportó miembros saludables: $ETCD_HEALTH"
  fi
else
  warn "etcdctl no disponible en PATH — no se pudo verificar salud de etcd directamente"
fi

info "5.2 Verificando estado del clúster vía Patroni REST API (DR, :${PATRONI_PORT}/cluster)..."
PATRONI_CLUSTER=$(curl -sf --max-time 5 "http://localhost:${PATRONI_PORT}/cluster" 2>/dev/null || echo "")
if [[ -n "$PATRONI_CLUSTER" ]]; then
  pass "Patroni REST API (DR) responde en :${PATRONI_PORT}"
  LEADER_COUNT=$(echo "$PATRONI_CLUSTER" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(len([m for m in d.get('members',[]) if m.get('role')=='leader']))" \
    2>/dev/null || echo "0")
  REPLICA_STREAMING=$(echo "$PATRONI_CLUSTER" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(len([m for m in d.get('members',[]) if m.get('role')=='replica' and m.get('state')=='streaming']))" \
    2>/dev/null || echo "0")
  info "Líder DR detectado: $LEADER_COUNT | Réplicas en streaming: $REPLICA_STREAMING"
  if [[ "$LEADER_COUNT" -eq 1 ]]; then
    pass "Exactamente un líder Patroni activo en DR (sin split-brain)"
  elif [[ "$LEADER_COUNT" -eq 0 ]]; then
    fail "Sin líder Patroni activo en DR — clúster DR sin escritura"
  else
    fail "Múltiples líderes Patroni detectados en DR ($LEADER_COUNT) — posible split-brain"
  fi
  [[ "$REPLICA_STREAMING" -ge "$MIN_PG_REPLICAS_REQUIRED" ]] 2>/dev/null \
    && pass "Réplicas en streaming en DR: $REPLICA_STREAMING (mínimo: $MIN_PG_REPLICAS_REQUIRED)" \
    || warn "Réplicas en streaming insuficientes en DR: $REPLICA_STREAMING (mínimo: $MIN_PG_REPLICAS_REQUIRED)"
else
  fail "Patroni REST API no respondió en DR (:${PATRONI_PORT}) — no se pudo evaluar el estado del clúster"
fi

if [[ "$SKIP_FAILOVER_DRILL" == false ]]; then
  info "5.3 Verificando que el failover automático NO está pausado en DR..."
  PATRONI_CONFIG=$(curl -sf --max-time 5 "http://localhost:${PATRONI_PORT}/config" 2>/dev/null || echo "")
  PAUSED=$(echo "$PATRONI_CONFIG" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pause', False))" 2>/dev/null || echo "unknown")
  if [[ "$PAUSED" == "False" || "$PAUSED" == "false" ]]; then
    pass "Failover automático de Patroni HABILITADO en DR (pause=false)"
  elif [[ "$PAUSED" == "unknown" ]]; then
    warn "No se pudo determinar el estado de pausa de Patroni en DR"
  else
    fail "Failover automático de Patroni PAUSADO en DR — el clúster DR no promoverá una réplica ante una caída del líder"
  fi
else
  info "5.3 Verificación de failover omitida (--skip-failover-drill)"
fi

# =============================================================================
# SECCIÓN 6 — REPLICACIÓN POSTGRESQL (DR)
# =============================================================================
header "6. REPLICACIÓN POSTGRESQL DR"

if command -v psql &>/dev/null; then
  info "6.1 Verificando pg_stat_replication en DR (solo aplica en el nodo líder)..."
  REPL_STATE=$(psql -X -A -t -q -h localhost -p "$PG_PORT" -U postgres -d postgres \
    -c "SELECT application_name, state, sync_state, COALESCE(replay_lag::text, '0') FROM pg_stat_replication;" 2>&1 || echo "ERROR")
  if [[ "$REPL_STATE" == "ERROR" || -z "$REPL_STATE" ]]; then
    info "Sin datos de pg_stat_replication en este nodo DR (probablemente es la réplica) o sin permisos de conexión"
  else
    pass "pg_stat_replication (líder DR) — filas:"
    info "$REPL_STATE"
  fi

  info "6.2 Verificando modo de recuperación local (líder vs réplica) en DR..."
  IS_REPLICA=$(psql -X -A -t -q -h localhost -p "$PG_PORT" -U postgres -d postgres -c "SELECT pg_is_in_recovery();" 2>&1 || echo "ERROR")
  info "pg_is_in_recovery() en DR: $IS_REPLICA"
else
  info "6. Verificación de replicación omitida (psql no disponible)"
fi

# =============================================================================
# SECCIÓN 7 — HAPROXY (DR)
# =============================================================================
header "7. HAPROXY (VIP) DR"

if [[ "$IS_ACCESS_NODE" == true ]]; then
  info "7.1 Verificando HAProxy stats (DR, :${HAPROXY_STATS_PORT})..."
  HAPROXY_STATS=$(curl -sf --max-time 5 -o /dev/null -w "%{http_code}" "http://localhost:${HAPROXY_STATS_PORT}/" 2>/dev/null || echo "000")
  [[ "$HAPROXY_STATS" == "200" ]] \
    && pass "HAProxy stats page (DR) responde: HTTP $HAPROXY_STATS" \
    || warn "HAProxy stats page (DR): HTTP $HAPROXY_STATS"

  info "7.2 Verificando frontend HAProxy WRITE (DR, :${HAPROXY_WRITE_PORT})..."
  if curl -sf --max-time 3 -o /dev/null "http://localhost:${HAPROXY_WRITE_PORT}/" 2>/dev/null || \
     timeout 3 bash -c "echo >/dev/tcp/localhost/${HAPROXY_WRITE_PORT}" 2>/dev/null; then
    pass "HAProxy WRITE (DR, :${HAPROXY_WRITE_PORT}) alcanzable"
  else
    fail "HAProxy WRITE (DR, :${HAPROXY_WRITE_PORT}) NO alcanzable — sin ruta de escritura al líder"
  fi

  info "7.3 Verificando frontend HAProxy READ (DR, :${HAPROXY_READ_PORT})..."
  if curl -sf --max-time 3 -o /dev/null "http://localhost:${HAPROXY_READ_PORT}/" 2>/dev/null || \
     timeout 3 bash -c "echo >/dev/tcp/localhost/${HAPROXY_READ_PORT}" 2>/dev/null; then
    pass "HAProxy READ (DR, :${HAPROXY_READ_PORT}) alcanzable"
  else
    fail "HAProxy READ (DR, :${HAPROXY_READ_PORT}) NO alcanzable — sin ruta de lectura a las réplicas"
  fi

  info "7.4 Verificando Keepalived (VIP, DR)..."
  if systemctl is-active --quiet keepalived 2>/dev/null || pgrep -x keepalived &>/dev/null; then
    pass "Keepalived activo en este nodo DR"
  else
    warn "Keepalived no detectado en DR — el VIP puede no tener failover automático real"
  fi

  info "7.5 Verificando PgBouncer (pool de conexiones, DR)..."
  if ss -tlnp 2>/dev/null | grep -q ":${PGBOUNCER_PORT} "; then
    pass "PgBouncer escuchando en :${PGBOUNCER_PORT} (DR)"
  else
    warn "PgBouncer no detectado en :${PGBOUNCER_PORT} (DR) — puede ser opcional en este despliegue"
  fi
else
  info "7. Nodo sin capa de acceso: no aplica HAProxy/PgBouncer en este nodo"
fi

# =============================================================================
# SECCIÓN 8 — RESILIENCIA Y QUÓRUM (DR)
# =============================================================================
header "8. RESILIENCIA Y QUÓRUM DEL CLÚSTER DR"

info "8.1 Estado de cada nodo del clúster DR (TCP etcd client port)..."
UP_NODES=0
for NODE in "${ALL_NODES[@]}"; do
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${ETCD_CLIENT_PORT}" 2>/dev/null; then
    pass "Nodo DR $NODE: puerto ${ETCD_CLIENT_PORT} alcanzable — UP"
    UP_NODES=$((UP_NODES+1))
  else
    fail "Nodo DR $NODE: puerto ${ETCD_CLIENT_PORT} NO alcanzable — DOWN o inaccesible"
  fi
done

if [[ $UP_NODES -ge $MIN_ETCD_MEMBERS_REQUIRED ]]; then
  pass "Nodos del clúster DR: $UP_NODES/3 — quórum completo (requerido: $MIN_ETCD_MEMBERS_REQUIRED/3)"
elif [[ $UP_NODES -ge 2 ]]; then
  warn "Nodos del clúster DR: $UP_NODES/3 — quórum mínimo de etcd (2/3) mantenido, pero degradado"
else
  fail "Nodos del clúster DR: $UP_NODES/3 — POR DEBAJO del quórum mínimo de etcd — DR en riesgo"
fi

# =============================================================================
# SECCIÓN 9 — LOGS Y OBSERVABILIDAD (DR)
# =============================================================================
header "9. LOGS Y OBSERVABILIDAD DR"

info "9.1 Verificando logs en $PG_LOGS (DR)..."
if [[ -d "$PG_LOGS" ]]; then
  LOG_COUNT=$(find "$PG_LOGS" -name "*.log" 2>/dev/null | wc -l)
  if [[ "$LOG_COUNT" -gt 0 ]]; then
    LAST_LOG=$(ls -t "$PG_LOGS"/*.log 2>/dev/null | head -1)
    LAST_MODIFIED=$(stat -c '%y' "$LAST_LOG" 2>/dev/null | cut -d. -f1 || echo "N/A")
    pass "Logs en $PG_LOGS: $LOG_COUNT archivo(s) — último modificado: $LAST_MODIFIED"
    ERRORS_LOG=$(grep -ciE '\bERROR\b|\bFATAL\b|\bPANIC\b' "$LAST_LOG" 2>/dev/null || true)
    [[ "$ERRORS_LOG" -eq 0 ]] \
      && pass "Sin errores críticos en el log más reciente (DR)" \
      || warn "Se detectaron $ERRORS_LOG líneas con ERROR/FATAL/PANIC en el log más reciente (DR)"
  else
    warn "Directorio $PG_LOGS existe pero sin archivos .log"
  fi
else
  warn "Directorio $PG_LOGS NO existe — verificar log_directory en postgresql.conf"
fi

info "9.2 Verificando Grafana Alloy (observabilidad, DR)..."
ALLOY_UNITS=$(systemctl list-units --type=service 2>/dev/null | grep -i "alloy" | awk '{print $1}' || true)
if [[ -n "$ALLOY_UNITS" ]]; then
  while IFS= read -r unit; do
    ALLOY_STATE=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    [[ "$ALLOY_STATE" == "active" ]] && pass "Alloy: $unit — activo" || fail "Alloy: $unit — $ALLOY_STATE"
  done <<< "$ALLOY_UNITS"
else
  warn "Grafana Alloy no encontrado como unidad systemd en DR"
fi

info "9.3 Verificando node-exporter (DR)..."
ss -tlnp 2>/dev/null | grep -q ":9100 " \
  && pass "node-exporter: puerto 9100 escuchando" \
  || warn "node-exporter: no detectado en puerto 9100"

# =============================================================================
# SECCIÓN 10 — VERIFICACIÓN DE VERSIONES (DR)
# =============================================================================
header "10. VERSIÓN DE COMPONENTES (DR)"

if command -v psql &>/dev/null; then
  PG_VER=$(psql -X -A -t -q -h localhost -p "$PG_PORT" -U postgres -d postgres -c "SHOW server_version;" 2>/dev/null | tr -d ' ' || echo "no disponible")
  if [[ "$PG_VER" == "$PG_VERSION_EXPECTED"* ]]; then
    pass "Versión PostgreSQL DR: $PG_VER (igual a Principal: $PG_VERSION_EXPECTED)"
  elif [[ "$PG_VER" != "no disponible" ]]; then
    warn "Versión PostgreSQL DR: $PG_VER — verificar homologación con Principal ($PG_VERSION_EXPECTED)"
  else
    warn "No se pudo determinar la versión de PostgreSQL en DR"
  fi
fi

if command -v patronictl &>/dev/null; then
  PATRONI_VER=$(patronictl version 2>/dev/null | awk '{print $NF}' || echo "no disponible")
  [[ "$PATRONI_VER" == "$PATRONI_VERSION_EXPECTED"* ]] \
    && pass "Versión Patroni DR: $PATRONI_VER (esperado: $PATRONI_VERSION_EXPECTED)" \
    || warn "Versión Patroni DR: $PATRONI_VER — verificar homologación con $PATRONI_VERSION_EXPECTED"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN — CLÚSTER DR"

TOTAL_CHECKS=$((PASS_COUNT + ERRORS + WARNINGS))

echo -e "  Cluster       : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo          : ${BOLD}${HOSTNAME_ACTUAL}${NC} (${NODE_ROLE})"
echo -e "  Nodos DR      : ${ALL_NODES[*]}"
echo -e "  Nodos UP      : ${UP_NODES}/3"
echo -e "  Fecha         : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  Total checks  : ${BOLD}${TOTAL_CHECKS}${NC}"
echo -e "  ${GREEN}${BOLD}PASS${NC}          : ${PASS_COUNT}"
echo -e "  ${RED}${BOLD}FAIL${NC}          : ${ERRORS}"
echo -e "  ${YELLOW}${BOLD}WARN${NC}          : ${WARNINGS}"
echo ""
echo -e "  ${BOLD}Notas de diseño DR (Bluepoint):${NC}"
echo -e "  • PostgreSQL HA SÍ reduce capacidad en DR frente a Principal"
echo -e "  • Los 3 nodos llevan réplica real de Postgres — dlh03-cont NO es witness"
echo -e "  • Solo dlh01-cont/dlh02-cont exponen la capa de acceso (PgBouncer/HAProxy/Keepalived)"
echo -e "  • Objetivo en contingencia: mantener el Outbox transaccional disponible con recursos mínimos"
echo ""

if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN DR COMPLETADA SIN ERRORES NI ADVERTENCIAS${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN DR COMPLETADA CON ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${YELLOW}      Revisar advertencias antes de depender del DR en contingencia.${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN DR FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${RED}      Corregir errores críticos — el clúster DR no está listo para contingencia.${NC}"
fi
echo ""

exit $ERRORS
