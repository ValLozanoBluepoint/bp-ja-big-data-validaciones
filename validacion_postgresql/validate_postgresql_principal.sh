#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster PostgreSQL HA Principal (Data Warehouse / Outbox)
#  Nodos : pbigd-dlh01 | pbigd-dlh02 | pbigd-dlh03
#  Stack : etcd 3.5.16 + PostgreSQL 17.10 + Patroni 4.1 en LOS 3 NODOS
#          + HAProxy 2.8.6 + Keepalived + PgBouncer 1.24 SOLO en dlh01/dlh02
#          + Grafana Alloy
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 5432 (Postgres) | 8008 (Patroni REST API) | 2379/2380 (etcd)
#            6432 (PgBouncer) | 15432 (HAProxy WRITE) | 15433 (HAProxy READ)
#            | 18404 (HAProxy Stats UI) — puertos altos, HAProxy corre sin
#            privilegios bajo el usuario admapl
#  Directorios: /var/lib/postgresql (o equivalente) | /var/lib/etcd | logs
#
#  Rol (según diagrama de arquitectura — context/ha arquitectura.png):
#  LOS 3 NODOS (dlh01/dlh02/dlh03) corren el stack completo de datos —
#  etcd + PostgreSQL + Patroni — y participan en la replicación síncrona:
#  dlh01 = LEADER (primario), dlh02 y dlh03 = REPLICA (SÍNCRONA). Ningún
#  nodo es un witness liviano de solo-etcd; los 3 llevan réplica real de
#  datos. Solo dlh01 y dlh02 llevan además la capa de acceso (PgBouncer +
#  HAProxy + Keepalived) — dlh03 NO expone esa capa.
#
#  Este clúster sirve el Transactional Outbox con replicación síncrona. Por
#  diseño NO debe ser consultado directamente por BI/Reportería (Pentaho) —
#  ese tráfico debe ir vía Trino+Iceberg o una réplica aislada fuera del
#  esquema de replicación síncrona (ver hallazgos de arquitectura, sección 7).
#
#  Nota de alcance: este script valida ÚNICAMENTE el clúster PostgreSQL HA
#  Principal (dlh01/dlh02/dlh03). La instancia PostgreSQL de METADATOS
#  (platform-db-1, bd_bigd_meta) tiene su propio script de aprovisionamiento
#  con verificación incluida (ver postgresql/provision_metadatos_postgresql.sh)
#  y queda fuera de alcance aquí.
#
#  Uso:
#    chmod +x validate_postgresql_principal.sh
#    ./validate_postgresql_principal.sh [--pg-port puerto] [--patroni-port puerto]
#                                        [--skip-failover-drill]
#
#  Parámetros opcionales:
#    --pg-port           Puerto de PostgreSQL local (default: 5432)
#    --patroni-port       Puerto de la REST API de Patroni (default: 8008)
#    --skip-failover-drill  Omitir la verificación de que el failover automático
#                            está habilitado (solo lectura de configuración, no
#                            ejecuta un failover real)
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

# ── Configuración del clúster Principal ─────────────────────────────────────
# Nombres de host y topología confirmados contra context/hostnames.txt y el
# diagrama de arquitectura (context/ha arquitectura.png).
CLUSTER_NAME="PostgreSQL HA Principal (Data Warehouse / Outbox)"
ALL_NODES=("pbigd-dlh01" "pbigd-dlh02" "pbigd-dlh03")   # los 3 corren etcd+Postgres+Patroni
ACCESS_NODES=("pbigd-dlh01" "pbigd-dlh02")              # además llevan PgBouncer+HAProxy+Keepalived
ETCD_CLIENT_PORT=2379
ETCD_PEER_PORT=2380
HAPROXY_STATS_PORT=18404
HAPROXY_WRITE_PORT=15432
HAPROXY_READ_PORT=15433
PGBOUNCER_PORT=6432
PG_DATA="/data/postgres/pgdata"   # confirmado en dlh01: bind mount real (host) del contenedor postgres-ha
ETCD_DATA="/data/postgres/etcd"  # confirmado en dlh01: bind mount real (host) del contenedor etcd
PG_LOGS="/var/log/postgresql"
PG_VERSION_EXPECTED="17.10"
PATRONI_VERSION_EXPECTED="4.1"
ETCD_VERSION_EXPECTED="3.5.16"
HAPROXY_VERSION_EXPECTED="2.8.6"
PGBOUNCER_VERSION_EXPECTED="1.24.0"
MIN_PG_REPLICAS_REQUIRED=2          # dlh02 + dlh03: 2 réplicas síncronas requeridas (diagrama)
MIN_ETCD_MEMBERS_REQUIRED=3         # dlh01 + dlh02 + dlh03 — quórum completo
HOSTNAME_ACTUAL=$(hostname -s)

# Detectar rol del nodo actual: todos corren Postgres/Patroni/etcd; solo
# ACCESS_NODES llevan además la capa de acceso (PgBouncer/HAProxy/Keepalived).
NODE_ROLE="desconocido"
IS_ACCESS_NODE=false
for n in "${ACCESS_NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n (Postgres/Patroni/etcd + acceso PgBouncer/HAProxy)"; IS_ACCESS_NODE=true; break; fi
done
if [[ "$NODE_ROLE" == "desconocido" ]]; then
  for n in "${ALL_NODES[@]}"; do
    if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n (Postgres/Patroni/etcd, sin capa de acceso)"; break; fi
  done
fi

# Resolver contenedores locales (si el stack corre en Podman). Cada componente
# puede vivir en un contenedor separado, por lo que se detectan por su cuenta
# en vez de asumir un único contenedor para todo el stack.
# El nombre de imagen se reduce a su componente base (sin registry/namespace
# ni tag) antes de comparar: el namespace del registry de este proyecto es
# "postgres-ha" para TODAS las imágenes (ej. postgres-ha/etcd:3.5.21-1), así
# que matchear contra la cadena completa de imagen hace que "etcd" también
# matchee /postgres/ y se detecte el contenedor equivocado.
resolve_container() {
  local pattern="$1"
  podman ps --format "{{.Names}}\t{{.Image}}" 2>/dev/null | \
    awk -F'\t' -v pat="$pattern" '{
      img=$2; sub(/:[^:]*$/,"",img); sub(/.*\//,"",img);
      if ($1 ~ pat || img ~ pat) { print $1; exit }
    }'
}
CONTAINER_PG=$(resolve_container "postgres|patroni")
CONTAINER_ETCD=$(resolve_container "etcd")
CONTAINER_HAPROXY=$(resolve_container "haproxy")
CONTAINER_PGBOUNCER=$(resolve_container "pgbouncer")

# ── Banner ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  PostgreSQL HA + Patroni + etcd · Clúster Principal (Data Warehouse / Outbox)"
echo -e "${NC}"
echo -e "${BOLD}  Cooperativa Jardín Azuayo — Proyecto Big Data${NC}"
echo -e "  Cluster: ${BOLD}${CYAN}${CLUSTER_NAME}${NC}"
echo -e "  Nodo actual: ${BOLD}${HOSTNAME_ACTUAL}${NC}  (rol detectado: ${NODE_ROLE})"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""

# =============================================================================
# SECCIÓN 1 — SISTEMA OPERATIVO Y RUNTIME
# =============================================================================
header "1. SISTEMA OPERATIVO Y RUNTIME"

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
  if [[ "$POD_MAJ" -ge 5 ]]; then
    pass "Podman $POD_VER instalado (>= 5.x requerido)"
  else
    fail "Podman $POD_VER < 5.x requerido"
  fi
else
  warn "Podman no encontrado en PATH — el stack puede correr en bare metal (systemd directo)"
fi

info "1.4 Verificando systemd..."
SYSSTATE=$(systemctl is-system-running 2>/dev/null) || true
[[ -z "$SYSSTATE" ]] && SYSSTATE="unknown"
[[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"

# 1.5 NTP — crítico para el consenso Raft de etcd y para la coherencia de
# timestamps de replicación síncrona de Postgres
info "1.5 Verificando sincronización NTP (crítico para consenso Raft de etcd)..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización — CRÍTICO para consenso etcd"
fi

# 1.6 Recursos del nodo — los 3 nodos corren el stack de datos completo
# (etcd+Postgres+Patroni), por lo que comparten la misma spec mínima. El
# diagrama de arquitectura no desglosa vCPU/RAM por nodo individual — el
# umbral usado aquí es el de un nodo Postgres completo, igual para los 3.
info "1.6 Verificando recursos del nodo..."
MEM_TOTAL_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
MEM_TOTAL_GB=$(( MEM_TOTAL_KB / 1024 / 1024 ))
VCPU=$(nproc)
info "Recursos nodo ${HOSTNAME_ACTUAL}: ${VCPU} vCPU / ${MEM_TOTAL_GB} GB RAM"
if [[ $MEM_TOTAL_GB -ge 12 ]]; then
  pass "RAM acorde a spec de nodo Postgres completo: ${MEM_TOTAL_GB} GB (spec: 14 GB)"
else
  warn "RAM por debajo de lo especificado: ${MEM_TOTAL_GB} GB (esperado ~14 GB)"
fi
if [[ $VCPU -ge 4 ]]; then
  pass "vCPU acordes a spec de nodo Postgres completo: $VCPU (spec: 4)"
else
  warn "vCPU por debajo de lo especificado: $VCPU (esperado 4)"
fi

# =============================================================================
# SECCIÓN 2 — VERSIÓN DE COMPONENTES
# =============================================================================
header "2. VERSIÓN DE COMPONENTES"

# Verifica versión de un componente: intenta primero podman exec dentro del
# contenedor detectado para ese componente (despliegue containerizado, el caso
# real de este proyecto); si no hay contenedor o falla, cae al binario en el
# PATH del host (despliegue bare-metal). El binario se invoca SIN shell
# intermedia (podman exec "$container" "$bin" "${args[@]}"), porque algunas
# imágenes (ej. etcd) son mínimas y no incluyen bash/sh — el parseo de la
# salida (head/awk/grep/tr) siempre corre del lado del host, que sí garantiza
# tener esas herramientas.
check_component_version() {
  local label="$1" host_bin="$2" expected="$3" container="$4" parse_cmd="$5"
  shift 5
  local -a bin_args=("$@")
  local raw="" ver="" source=""

  if [[ -n "$container" ]] && command -v podman &>/dev/null; then
    raw=$(podman exec "$container" "$host_bin" "${bin_args[@]}" 2>/dev/null)
    if [[ -n "$raw" ]]; then
      ver=$(printf '%s\n' "$raw" | eval "$parse_cmd" 2>/dev/null)
      [[ -n "$ver" ]] && source="contenedor: $container"
    fi
  fi
  if [[ -z "$ver" ]] && command -v "$host_bin" &>/dev/null; then
    raw=$("$host_bin" "${bin_args[@]}" 2>/dev/null)
    ver=$(printf '%s\n' "$raw" | eval "$parse_cmd" 2>/dev/null)
    [[ -n "$ver" ]] && source="host"
  fi

  if [[ -z "$ver" ]]; then
    if [[ -n "$container" ]]; then
      warn "$label: no se pudo obtener versión (falló podman exec en $container y binario ausente en host)"
    elif command -v podman &>/dev/null && [[ "$(id -u)" -ne 0 ]]; then
      warn "$label: no se detectó contenedor (podman rootful requiere privilegios elevados) y binario ausente en host — reintentar con sudo"
    else
      warn "$label: no se detectó contenedor y binario ausente en PATH del host — verificar despliegue"
    fi
    return
  fi

  # Compatibilidad por semver: mismo major.minor se acepta aunque el patch
  # difiera (ej. 3.5.21 vs 3.5.16 esperado — ambos son 3.5.x).
  local ver_mm exp_mm
  ver_mm=$(echo "$ver" | cut -d. -f1,2)
  exp_mm=$(echo "$expected" | cut -d. -f1,2)
  if [[ "$ver_mm" == "$exp_mm" ]]; then
    pass "Versión $label: $ver (compatible con $expected) — fuente: $source"
  else
    warn "Versión $label: $ver — verificar homologación con $expected — fuente: $source"
  fi
}

check_component_version "PostgreSQL" "psql" "$PG_VERSION_EXPECTED" "$CONTAINER_PG" \
  "awk '{print \$1}'" \
  -X -A -t -q -h localhost -p "${PG_PORT}" -U postgres -d postgres -c "SHOW server_version;"

check_component_version "Patroni" "patronictl" "$PATRONI_VERSION_EXPECTED" "$CONTAINER_PG" \
  "awk '{print \$NF}'" \
  version

check_component_version "etcd" "etcd" "$ETCD_VERSION_EXPECTED" "$CONTAINER_ETCD" \
  "head -1 | awk '{print \$NF}'" \
  --version

# HAProxy solo corre en los ACCESS_NODES (dlh01/dlh02) — dlh03 no lleva capa
# de acceso, así que no tiene sentido verificar su versión ahí.
if [[ "$IS_ACCESS_NODE" == true ]]; then
  check_component_version "HAProxy" "haproxy" "$HAPROXY_VERSION_EXPECTED" "$CONTAINER_HAPROXY" \
    "head -1 | grep -oE '[0-9]+\\.[0-9]+\\.[0-9]+' | head -1" \
    -v
else
  info "HAProxy: nodo sin capa de acceso — no aplica"
fi

# =============================================================================
# SECCIÓN 3 — ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA
# =============================================================================
header "3. ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA"

REQUIRED_DIRS=("$PG_DATA" "$ETCD_DATA" "$PG_LOGS")

for dir in "${REQUIRED_DIRS[@]}"; do
  info "Verificando $dir ..."
  if [[ -d "$dir" ]]; then
    OWNER=$(stat -c '%U:%G' "$dir")
    PERMS=$(stat -c '%a' "$dir")
    USAGE=$(df -h "$dir" 2>/dev/null | awk 'NR==2{print $3"/"$2" usado ("$5")"}' || echo "N/A")
    pass "Directorio $dir existe — owner: $OWNER — perms: $PERMS — uso: $USAGE"

    FSTYPE=$(df -T "$dir" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
    if [[ "$FSTYPE" == "overlay" ]]; then
      fail "$dir usa overlay filesystem — debe ser bind mount sobre partición dedicada"
    else
      pass "$dir filesystem: $FSTYPE (no overlay)"
    fi
  else
    fail "Directorio $dir NO existe"
  fi
done

# =============================================================================
# SECCIÓN 4 — SERVICIOS SYSTEMD Y CONTENEDORES PODMAN
# =============================================================================
header "4. SERVICIOS SYSTEMD Y CONTENEDORES PODMAN"

# 3.1 Unidades systemd — se revisan TANTO la instancia de sistema (root) COMO la
# instancia de usuario (rootless/Quadlet), igual que en el resto de scripts del
# proyecto (ver validate_kafka_dr.sh).
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
      fail "  ↳ Unidad '$unit' (${scope}) NO habilitada en boot — CRÍTICO: debe sobrevivir reinicios"
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

info "4.1 Verificando unidades systemd del stack HA (sistema y usuario/Quadlet)..."
SYSTEMD_UNITS_SYSTEM=$(systemctl list-units --type=service 2>/dev/null | grep -iE "$SERVICE_PATTERN" | awk '{print $1}' || true)
SYSTEMD_UNITS_USER=$(systemctl --user list-units --type=service 2>/dev/null | grep -iE "$SERVICE_PATTERN" | awk '{print $1}' || true)

if [[ -n "$SYSTEMD_UNITS_SYSTEM" ]]; then
  while IFS= read -r unit; do check_systemd_unit "sistema" "$unit"; done <<< "$SYSTEMD_UNITS_SYSTEM"
fi
if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  while IFS= read -r unit; do check_systemd_unit "usuario" "$unit"; done <<< "$SYSTEMD_UNITS_USER"
fi
if [[ -z "$SYSTEMD_UNITS_SYSTEM" && -z "$SYSTEMD_UNITS_USER" ]]; then
  fail "No se encontraron unidades systemd (sistema ni usuario) para el stack HA ($SERVICE_PATTERN)"
fi

# 3.2 Linger de systemd --user (rootless/Quadlet)
if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  info "4.2 Verificando linger de systemd para persistencia de servicios de usuario..."
  LINGER_USER=$(whoami)
  LINGER_STATE=$(loginctl show-user "$LINGER_USER" -p Linger --value 2>/dev/null || echo "unknown")
  if [[ "$LINGER_STATE" == "yes" ]]; then
    pass "Linger habilitado para usuario $LINGER_USER — el servicio persiste sin sesión activa"
  else
    fail "Linger NO habilitado para usuario $LINGER_USER (Linger=$LINGER_STATE) — corregir con: loginctl enable-linger $LINGER_USER"
  fi
fi

# 3.3 Contenedores Podman (si el stack corre containerizado)
if [[ -n "$CONTAINER_PG" ]]; then
  info "4.3 Verificando contenedor Podman Postgres/Patroni..."
  CSTATUS=$(podman ps --format "{{.Status}}" --filter "name=$CONTAINER_PG" 2>/dev/null || echo "")
  if echo "$CSTATUS" | grep -qi "up"; then
    pass "Contenedor: $CONTAINER_PG — estado: $CSTATUS"
  else
    fail "Contenedor: $CONTAINER_PG — estado: $CSTATUS (esperado: Up)"
  fi
elif command -v podman &>/dev/null && [[ "$(id -u)" -ne 0 ]]; then
  info "4.3 Sin contenedor Podman detectado para Postgres/Patroni (podman rootful requiere privilegios elevados) — reintentar con sudo"
else
  info "4.3 Sin contenedor Podman detectado para Postgres/Patroni — se asume despliegue directo sobre systemd"
fi

# =============================================================================
# SECCIÓN 5 — CONECTIVIDAD DE RED
# =============================================================================
header "5. CONECTIVIDAD DE RED"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || \
     netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando"
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

info "4.9 Conectividad entre nodos del clúster (${ALL_NODES[*]})..."
for NODE in "${ALL_NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$NODE"* ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE: OK (rtt: ${PING_RTT}ms)"
  else
    fail "Ping a $NODE: no responde — clúster degradado"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${ETCD_CLIENT_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${ETCD_CLIENT_PORT} (etcd) alcanzable"
  else
    fail "TCP ${NODE}:${ETCD_CLIENT_PORT} (etcd) no alcanzable — riesgo de pérdida de quórum"
  fi
done

# =============================================================================
# SECCIÓN 6 — VALIDACIÓN FUNCIONAL: ETCD Y PATRONI
# =============================================================================
header "6. VALIDACIÓN FUNCIONAL — ETCD Y PATRONI"

# 6.1 Salud de etcd — intenta etcdctl en el contenedor de etcd primero
# (despliegue containerizado), luego en el PATH del host (bare-metal).
info "6.1 Verificando salud de etcd (quórum)..."
ETCD_HEALTH=""
if [[ -n "$CONTAINER_ETCD" ]] && command -v podman &>/dev/null; then
  ETCD_HEALTH=$(podman exec "$CONTAINER_ETCD" etcdctl endpoint health --cluster 2>&1 || echo "")
fi
if [[ -z "$ETCD_HEALTH" ]] && command -v etcdctl &>/dev/null; then
  ETCD_HEALTH=$(etcdctl endpoint health --cluster 2>&1 || echo "")
fi

if [[ -n "$ETCD_HEALTH" ]]; then
  if echo "$ETCD_HEALTH" | grep -qi "is healthy"; then
    HEALTHY_COUNT=$(echo "$ETCD_HEALTH" | grep -ci "is healthy" || true)
    if [[ "$HEALTHY_COUNT" -ge "$MIN_ETCD_MEMBERS_REQUIRED" ]]; then
      pass "etcd: $HEALTHY_COUNT/$MIN_ETCD_MEMBERS_REQUIRED miembros saludables (quórum completo)"
    else
      fail "etcd: solo $HEALTHY_COUNT/$MIN_ETCD_MEMBERS_REQUIRED miembros saludables — quórum en riesgo"
    fi
  else
    fail "etcd endpoint health no reportó miembros saludables: $ETCD_HEALTH"
  fi
elif command -v podman &>/dev/null && [[ "$(id -u)" -ne 0 ]]; then
  warn "etcdctl no disponible (podman rootful requiere privilegios elevados) — reintentar con sudo"
else
  warn "etcdctl no disponible (ni en contenedor etcd ni en PATH del host) — no se pudo verificar salud de etcd directamente"
fi

# 6.2 Estado del clúster Patroni (líder, réplicas, lag) — Patroni corre en
# los 3 nodos, se puede consultar desde cualquiera.
info "6.2 Verificando estado del clúster vía Patroni REST API (:${PATRONI_PORT}/cluster)..."
PATRONI_CLUSTER=$(curl -sf --max-time 5 "http://localhost:${PATRONI_PORT}/cluster" 2>/dev/null || echo "")
if [[ -n "$PATRONI_CLUSTER" ]]; then
  pass "Patroni REST API responde en :${PATRONI_PORT}"
  LEADER_COUNT=$(echo "$PATRONI_CLUSTER" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(len([m for m in d.get('members',[]) if m.get('role')=='leader']))" \
    2>/dev/null || echo "0")
  # 'replica' no es el único rol de standby: en replicación síncrona Patroni
  # reporta 'sync_standby' (y también existe 'quorum_standby'). Se cuenta
  # cualquier miembro que no sea el leader y esté en streaming.
  REPLICA_STREAMING=$(echo "$PATRONI_CLUSTER" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(len([m for m in d.get('members',[]) if m.get('role')!='leader' and m.get('state')=='streaming']))" \
    2>/dev/null || echo "0")
  info "Líder detectado: $LEADER_COUNT | Réplicas en streaming: $REPLICA_STREAMING"
  if [[ "$LEADER_COUNT" -eq 1 ]]; then
    pass "Exactamente un líder Patroni activo (sin split-brain)"
  elif [[ "$LEADER_COUNT" -eq 0 ]]; then
    fail "Sin líder Patroni activo — clúster sin escritura"
  else
    fail "Múltiples líderes Patroni detectados ($LEADER_COUNT) — posible split-brain"
  fi
  if [[ "$REPLICA_STREAMING" -ge "$MIN_PG_REPLICAS_REQUIRED" ]] 2>/dev/null; then
    pass "Réplicas en streaming: $REPLICA_STREAMING (mínimo requerido: $MIN_PG_REPLICAS_REQUIRED)"
  else
    warn "Réplicas en streaming insuficientes: $REPLICA_STREAMING (mínimo: $MIN_PG_REPLICAS_REQUIRED)"
  fi
else
  fail "Patroni REST API no respondió en :${PATRONI_PORT} — no se pudo evaluar el estado del clúster"
fi

# 6.3 Failover automático habilitado (solo lectura de config, no ejecuta failover real)
if [[ "$SKIP_FAILOVER_DRILL" == false ]]; then
  info "6.3 Verificando que el failover automático NO está pausado..."
  PATRONI_CONFIG=$(curl -sf --max-time 5 "http://localhost:${PATRONI_PORT}/config" 2>/dev/null || echo "")
  PAUSED=$(echo "$PATRONI_CONFIG" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pause', False))" 2>/dev/null || echo "unknown")
  if [[ "$PAUSED" == "False" || "$PAUSED" == "false" ]]; then
    pass "Failover automático de Patroni HABILITADO (pause=false)"
  elif [[ "$PAUSED" == "unknown" ]]; then
    warn "No se pudo determinar el estado de pausa de Patroni"
  else
    fail "Failover automático de Patroni PAUSADO — el clúster no promoverá una réplica ante una caída del líder"
  fi
else
  info "5.3 Verificación de failover omitida (--skip-failover-drill)"
fi

# =============================================================================
# SECCIÓN 7 — REPLICACIÓN POSTGRESQL
# =============================================================================
header "7. REPLICACIÓN POSTGRESQL"

if command -v psql &>/dev/null; then
  info "6.1 Verificando pg_stat_replication (solo aplica en el nodo líder)..."
  REPL_STATE=$(psql -X -A -t -q -h localhost -p "$PG_PORT" -U postgres -d postgres \
    -c "SELECT application_name, state, sync_state, COALESCE(replay_lag::text, '0') FROM pg_stat_replication;" 2>&1 || echo "ERROR")
  if [[ "$REPL_STATE" == "ERROR" || -z "$REPL_STATE" ]]; then
    info "Sin datos de pg_stat_replication en este nodo (probablemente es la réplica, no el líder) o sin permisos de conexión"
  else
    pass "pg_stat_replication (líder) — filas:"
    info "$REPL_STATE"
    if echo "$REPL_STATE" | grep -q "sync"; then
      pass "Replicación síncrona confirmada (sync_state=sync) — consistente con el diseño del Outbox"
    else
      warn "No se detectó una réplica en modo 'sync' — verificar synchronous_standby_names"
    fi
  fi

  info "6.2 Verificando modo de recuperación local (líder vs réplica)..."
  IS_REPLICA=$(psql -X -A -t -q -h localhost -p "$PG_PORT" -U postgres -d postgres \
    -c "SELECT pg_is_in_recovery();" 2>&1 || echo "ERROR")
  info "pg_is_in_recovery(): $IS_REPLICA"
else
  info "6. Verificación de replicación omitida (psql no disponible)"
fi

# =============================================================================
# SECCIÓN 8 — HAPROXY Y PGBOUNCER
# =============================================================================
header "8. HAPROXY (VIP) Y PGBOUNCER"

if [[ "$IS_ACCESS_NODE" == true ]]; then
  info "7.1 Verificando HAProxy stats (:${HAPROXY_STATS_PORT})..."
  # La raíz "/" no es la página de stats — HAProxy la enruta como tráfico
  # normal a un backend (sin servidores ahí, de ahí el 503 "No server is
  # available"). La página de stats real vive en "/stats".
  # Sin -f: con -f + -w "%{http_code}" + "|| echo 000" en el mismo $(...), un
  # HTTP >=400 hace que curl imprima el código real Y falle, y el "|| echo 000"
  # se concatena al resultado (ej. "503000"). Sin -f, curl siempre imprime el
  # código real con exit 0; solo queda vacío si la conexión falla de plano.
  HAPROXY_STATS=$(curl -s --max-time 5 -o /dev/null -w "%{http_code}" "http://localhost:${HAPROXY_STATS_PORT}/stats" 2>/dev/null)
  [[ -z "$HAPROXY_STATS" ]] && HAPROXY_STATS="000"
  if [[ "$HAPROXY_STATS" == "200" ]]; then
    pass "HAProxy stats page responde: HTTP $HAPROXY_STATS"
  elif [[ "$HAPROXY_STATS" == "401" ]]; then
    pass "HAProxy stats page activa y protegida con autenticación: HTTP 401 (comportamiento esperado)"
  else
    warn "HAProxy stats page: HTTP $HAPROXY_STATS — verificar que HAProxy esté activo en :${HAPROXY_STATS_PORT}"
  fi

  # Frontends HAProxy WRITE (→ líder únicamente) y READ (→ réplicas), según
  # el diagrama de arquitectura — separados del puerto directo de PgBouncer.
  info "7.2 Verificando frontend HAProxy WRITE (:${HAPROXY_WRITE_PORT})..."
  if curl -sf --max-time 3 -o /dev/null "http://localhost:${HAPROXY_WRITE_PORT}/" 2>/dev/null || \
     timeout 3 bash -c "echo >/dev/tcp/localhost/${HAPROXY_WRITE_PORT}" 2>/dev/null; then
    pass "HAProxy WRITE (:${HAPROXY_WRITE_PORT}) alcanzable"
  else
    fail "HAProxy WRITE (:${HAPROXY_WRITE_PORT}) NO alcanzable — sin ruta de escritura al líder"
  fi

  info "7.3 Verificando frontend HAProxy READ (:${HAPROXY_READ_PORT})..."
  if curl -sf --max-time 3 -o /dev/null "http://localhost:${HAPROXY_READ_PORT}/" 2>/dev/null || \
     timeout 3 bash -c "echo >/dev/tcp/localhost/${HAPROXY_READ_PORT}" 2>/dev/null; then
    pass "HAProxy READ (:${HAPROXY_READ_PORT}) alcanzable"
  else
    fail "HAProxy READ (:${HAPROXY_READ_PORT}) NO alcanzable — sin ruta de lectura a las réplicas"
  fi

  # Hallazgo de arquitectura: el VIP de PgBouncer requiere Keepalived con
  # failover real. Se reporta como WARN (no FAIL) porque depende de una
  # confirmación pendiente del equipo de virtualización sobre las condiciones
  # VMware/ESXi necesarias para que el VIP se mueva correctamente entre nodos.
  info "7.4 Verificando Keepalived (VIP de PgBouncer/HAProxy)..."
  if systemctl is-active --quiet keepalived 2>/dev/null || pgrep -x keepalived &>/dev/null; then
    pass "Keepalived activo en este nodo"
  else
    warn "Keepalived no detectado — el VIP de PgBouncer puede no tener failover automático real"
  fi

  info "7.5 Verificando PgBouncer (pool de conexiones)..."
  if ss -tlnp 2>/dev/null | grep -q ":${PGBOUNCER_PORT} "; then
    pass "PgBouncer escuchando en :${PGBOUNCER_PORT}"
    if command -v psql &>/dev/null; then
      POOLS=$(PGPASSWORD="${PGBOUNCER_ADMIN_PW:-}" psql -X -A -t -q -h localhost -p "$PGBOUNCER_PORT" -U pgbouncer pgbouncer \
        -c "SHOW POOLS;" 2>&1 || echo "")
      [[ -n "$POOLS" ]] && info "SHOW POOLS: $POOLS"
    fi
  else
    warn "PgBouncer no detectado en :${PGBOUNCER_PORT} — puede ser opcional en este despliegue"
  fi

  # Hallazgo de arquitectura: PgBouncer no debe autenticar aplicaciones con el
  # superusuario 'postgres'. Se lee auth_user de pgbouncer.ini directamente
  # (vía podman exec, sin exponer contraseñas/hashes de userlist.txt).
  info "7.6 Verificando que PgBouncer no autentica con el superusuario 'postgres'..."
  if [[ -n "$CONTAINER_PGBOUNCER" ]] && command -v podman &>/dev/null; then
    PGB_AUTH_USER=$(podman exec "$CONTAINER_PGBOUNCER" grep -E '^\s*auth_user\s*=' /etc/pgbouncer/pgbouncer.ini 2>/dev/null | \
      awk -F= '{gsub(/ /,"",$2); print $2}')
    if [[ -z "$PGB_AUTH_USER" ]]; then
      warn "No se pudo leer auth_user de pgbouncer.ini en $CONTAINER_PGBOUNCER — verificar manualmente"
    elif [[ "$PGB_AUTH_USER" == "postgres" ]]; then
      fail "PgBouncer usa auth_user=postgres (superusuario) para autenticar aplicaciones — riesgo de seguridad"
    else
      pass "PgBouncer usa un usuario de autenticación dedicado (auth_user=$PGB_AUTH_USER), no el superusuario 'postgres'"
    fi
  else
    warn "Recordatorio de hallazgo pendiente: confirmar que PgBouncer NO usa el superusuario 'postgres' para autenticar aplicaciones (sin contenedor detectado para verificar automáticamente)"
  fi
else
  info "7. Nodo sin capa de acceso: no aplica HAProxy/PgBouncer en este nodo"
fi

# =============================================================================
# SECCIÓN 9 — RESILIENCIA Y QUÓRUM
# =============================================================================
header "9. RESILIENCIA Y QUÓRUM DEL CLÚSTER"

info "8.1 Estado de cada nodo del clúster (TCP etcd client port)..."
UP_NODES=0
for NODE in "${ALL_NODES[@]}"; do
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${ETCD_CLIENT_PORT}" 2>/dev/null; then
    pass "Nodo $NODE: puerto ${ETCD_CLIENT_PORT} alcanzable — UP"
    UP_NODES=$((UP_NODES+1))
  else
    fail "Nodo $NODE: puerto ${ETCD_CLIENT_PORT} NO alcanzable — DOWN o inaccesible"
  fi
done

if [[ $UP_NODES -ge $MIN_ETCD_MEMBERS_REQUIRED ]]; then
  pass "Nodos del clúster: $UP_NODES/3 — quórum completo (requerido: $MIN_ETCD_MEMBERS_REQUIRED/3)"
elif [[ $UP_NODES -ge 2 ]]; then
  warn "Nodos del clúster: $UP_NODES/3 — quórum mínimo de etcd (2/3) mantenido, pero degradado"
else
  fail "Nodos del clúster: $UP_NODES/3 — POR DEBAJO del quórum mínimo de etcd — clúster en riesgo de indisponibilidad"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN — CLÚSTER PRINCIPAL"

TOTAL_CHECKS=$((PASS_COUNT + ERRORS + WARNINGS))

echo -e "  Cluster       : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo          : ${BOLD}${HOSTNAME_ACTUAL}${NC} (${NODE_ROLE})"
echo -e "  Nodos         : ${ALL_NODES[*]}"
echo -e "  Nodos UP      : ${UP_NODES}/3"
echo -e "  Fecha         : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  Total checks  : ${BOLD}${TOTAL_CHECKS}${NC}"
echo -e "  ${GREEN}${BOLD}PASS${NC}          : ${PASS_COUNT}"
echo -e "  ${RED}${BOLD}FAIL${NC}          : ${ERRORS}"
echo -e "  ${YELLOW}${BOLD}WARN${NC}          : ${WARNINGS}"
echo ""

if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN COMPLETADA SIN ERRORES NI ADVERTENCIAS${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN COMPLETADA CON ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${YELLOW}      Revisar advertencias antes de pasar a producción.${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${RED}      Corregir errores críticos — el clúster no está listo para producción.${NC}"
fi
echo ""

exit $ERRORS
