#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster Redis HA Principal (Serving Layer)
#  Nodos : pbigd-plat-apps01 (Primary+Sentinel) | pbigd-bd-plat-apps01 (Replica+Sentinel)
#          | pbigd-plat-obs01 (Sentinel)  [ver context/hostnames.txt]
#  Versión Redis : 7.4.x  +  Redis Sentinel 7.4.x
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 6379 (Redis) | 26379 (Sentinel)
#
#  Rol: Redis es la capa de serving online (saldos y movimientos recientes,
#  baja latencia). Igual que Kafka — a diferencia de PostgreSQL HA/MinIO —
#  el diseño de Bluepoint mantiene capacidad COMPLETA en el datacenter
#  alterno (ver validate_redis_dr.sh): es una función crítica.
#
#  Topología: pbigd-plat-apps01 corre el Primary (colocado con Flink JM/Trino
#  Coordinator/Gravitino); pbigd-bd-plat-apps01 corre una Réplica; los 3
#  Sentinels (pbigd-plat-apps01, pbigd-bd-plat-apps01, pbigd-plat-obs01)
#  forman el quórum de failover automático distribuido en distintos nodos.
#
#  Nota de alcance: este script valida ÚNICAMENTE el clúster Redis+Sentinel
#  Principal. La integración Redis↔API de serving queda fuera de alcance.
#
#  Uso:
#    chmod +x validate_redis_principal.sh
#    ./validate_redis_principal.sh [--redis-port puerto] [--sentinel-port puerto]
#                                   [--master-name nombre] [--node-role nombre]
#                                   [--redis-password password] [--sentinel-password password]
#
#  Parámetros opcionales:
#    --redis-port         Puerto de Redis local (default: 6379)
#    --sentinel-port      Puerto de Sentinel local (default: 26379)
#    --master-name        Nombre del master monitoreado por Sentinel (default: redis-master,
#                          confirmado vía SENTINEL masters en pbigd-plat-obs01)
#    --redis-password     Password de Redis (requirepass) para las validaciones funcionales
#                          de la sección 6. También puede pasarse vía REDISCLI_AUTH
#                          (mismo mecanismo nativo de redis-cli). Confirmado NOAUTH en
#                          pbigd-plat-apps01 — Redis exige auth.
#    --sentinel-password  Password de Sentinel, si lo tuviera (default: vacío — también vía
#                          SENTINEL_REDISCLI_AUTH). Distinto de --redis-password: confirmado
#                          que Sentinel en pbigd-plat-apps01 NO tiene auth configurada;
#                          reusar el password de Redis rompía el PING (AUTH failed).
#    --node-role       Fuerza el rol del nodo (pbigd-plat-apps01|pbigd-bd-plat-apps01|
#                       pbigd-plat-obs01) cuando `hostname -s` difiere del inventario
#                       real (context/hostnames.txt). Evita falsos PASS/FAIL.
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
REDIS_PORT=6379
SENTINEL_PORT=26379
MASTER_NAME="redis-master"
NODE_ROLE_OVERRIDE=""
REDIS_PASSWORD="${REDISCLI_AUTH:-}"
SENTINEL_PASSWORD="${SENTINEL_REDISCLI_AUTH:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --redis-port)        REDIS_PORT="$2"; shift 2 ;;
    --sentinel-port)     SENTINEL_PORT="$2"; shift 2 ;;
    --master-name)       MASTER_NAME="$2"; shift 2 ;;
    --node-role)         NODE_ROLE_OVERRIDE="$2"; shift 2 ;;
    --redis-password)    REDIS_PASSWORD="$2"; shift 2 ;;
    --sentinel-password) SENTINEL_PASSWORD="$2"; shift 2 ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

# ── Configuración del clúster Principal ─────────────────────────────────────
# Nombres de host reales (context/hostnames.txt, bloque PRINCIPAL):
#   pbigd-plat-apps01    = platform-apps-1 (Primary+Sentinel)
#   pbigd-bd-plat-apps01 = platform-db-1   (Replica+Sentinel)
#   pbigd-plat-obs01     = obs-1           (Sentinel only)
CLUSTER_NAME="Redis HA Principal (Serving Layer)"
NODE_PRIMARY="pbigd-plat-apps01"
NODE_REPLICA="pbigd-bd-plat-apps01"
NODE_SENTINEL_ONLY="pbigd-plat-obs01"
ALL_NODES=("$NODE_PRIMARY" "$NODE_REPLICA" "$NODE_SENTINEL_ONLY")
REDIS_DATA="/var/lib/redis"
REDIS_LOGS="/var/log/redis"
REDIS_VERSION_EXPECTED="7.4"
SENTINEL_VERSION_EXPECTED="7.4"
MIN_SENTINELS_REQUIRED=3
HOSTNAME_ACTUAL=$(hostname -s)

NODE_ROLE="desconocido"
for n in "${ALL_NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n"; break; fi
done
if [[ -n "$NODE_ROLE_OVERRIDE" ]]; then
  NODE_ROLE="$NODE_ROLE_OVERRIDE"
fi
if [[ "$NODE_ROLE" == "desconocido" ]]; then
  echo -e "${YELLOW}${BOLD}[ADVERTENCIA]${NC} No se pudo determinar el rol de este nodo a partir del hostname '${HOSTNAME_ACTUAL}'."
  echo -e "  Nombres esperados (según plan): ${ALL_NODES[*]}"
  echo -e "  Use --node-role <nombre> para indicarlo explícitamente y evitar falsos PASS/FAIL en las secciones 2, 4 y 5."
fi

CONTAINER_REDIS=$(podman ps --format "{{.Names}}\t{{.Image}}" 2>/dev/null | \
  awk -F'\t' '$2!="" && ($1 ~ /redis/ || $2 ~ /redis/) {print $1; exit}')

# Modo de acceso a redis-cli: prioriza el binario del host; si no existe,
# usa el contenedor Podman ya detectado (despliegue containerizado).
REDIS_CLI_MODE="none"
if command -v redis-cli &>/dev/null; then
  REDIS_CLI_MODE="host"
elif [[ -n "$CONTAINER_REDIS" ]]; then
  REDIS_CLI_MODE="container"
fi

# NOTA: redis-cli decide si manda AUTH según si REDISCLI_AUTH está *definida*,
# no según si tiene contenido — una variable vacía igual dispara el AUTH y
# rompe contra un usuario sin password (visto en Sentinel). Por eso acá NUNCA
# se define la variable cuando el password está vacío, en vez de definirla "".
redis_cli() {
  case "$REDIS_CLI_MODE" in
    host)
      if [[ -n "$REDIS_PASSWORD" ]]; then
        REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli -p "$REDIS_PORT" "$@" 2>&1
      else
        env -u REDISCLI_AUTH redis-cli -p "$REDIS_PORT" "$@" 2>&1
      fi ;;
    container)
      if [[ -n "$REDIS_PASSWORD" ]]; then
        podman exec -e REDISCLI_AUTH="$REDIS_PASSWORD" "$CONTAINER_REDIS" redis-cli -p "$REDIS_PORT" "$@" 2>&1
      else
        podman exec "$CONTAINER_REDIS" redis-cli -p "$REDIS_PORT" "$@" 2>&1
      fi ;;
    *) echo "" ;;
  esac
}
sentinel_cli() {
  case "$REDIS_CLI_MODE" in
    host)
      if [[ -n "$SENTINEL_PASSWORD" ]]; then
        REDISCLI_AUTH="$SENTINEL_PASSWORD" redis-cli -p "$SENTINEL_PORT" "$@" 2>&1
      else
        env -u REDISCLI_AUTH redis-cli -p "$SENTINEL_PORT" "$@" 2>&1
      fi ;;
    container)
      if [[ -n "$SENTINEL_PASSWORD" ]]; then
        podman exec -e REDISCLI_AUTH="$SENTINEL_PASSWORD" "$CONTAINER_REDIS" redis-cli -p "$SENTINEL_PORT" "$@" 2>&1
      else
        podman exec "$CONTAINER_REDIS" redis-cli -p "$SENTINEL_PORT" "$@" 2>&1
      fi ;;
    *) echo "" ;;
  esac
}

# Autodetección del path real de persistencia cuando Redis corre containerizado:
# REDIS_DATA es solo un valor por defecto documentado — puede no coincidir con el
# bind mount real del despliegue (confirmado en pbigd-plat-apps01: /data/redis/data
# en el host -> /data en el contenedor, distinto de /var/lib/redis).
if [[ "$NODE_ROLE" != "$NODE_SENTINEL_ONLY" && "$REDIS_CLI_MODE" == "container" ]]; then
  CONTAINER_DATA_DIR=$(redis_cli CONFIG GET dir 2>/dev/null | tail -1)
  if [[ -n "$CONTAINER_DATA_DIR" && "$CONTAINER_DATA_DIR" == /* ]]; then
    HOST_DATA_DIR=$(podman inspect "$CONTAINER_REDIS" \
      --format '{{range .Mounts}}{{if eq .Destination "'"$CONTAINER_DATA_DIR"'"}}{{.Source}}{{end}}{{end}}' 2>/dev/null)
    if [[ -n "$HOST_DATA_DIR" ]]; then
      REDIS_DATA="$HOST_DATA_DIR"
    fi
  fi
fi

# ── Banner ───────────────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  Redis HA + Sentinel · Capa de Serving Principal"
echo -e "${NC}"
echo -e "${BOLD}  Cooperativa Jardín Azuayo — Proyecto Big Data${NC}"
echo -e "  Cluster: ${BOLD}${CYAN}${CLUSTER_NAME}${NC}"
echo -e "  Nodo actual: ${BOLD}${HOSTNAME_ACTUAL}${NC}  (rol detectado: ${NODE_ROLE})"
echo -e "  Master monitoreado: ${MASTER_NAME}"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  ${YELLOW}${BOLD}Nota: Redis mantiene capacidad COMPLETA en DR (sin degradar),${NC}"
echo -e "  ${YELLOW}es la capa de serving online crítica para saldos y movimientos.${NC}"
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
  [[ "$POD_MAJ" -ge 5 ]] && pass "Podman $POD_VER instalado (>= 5.x requerido)" || fail "Podman $POD_VER < 5.x requerido"
else
  warn "Podman no encontrado en PATH — Redis puede correr en bare metal"
fi

info "1.4 Verificando systemd..."
SYSSTATE=$(systemctl is-system-running 2>/dev/null) || true
[[ -z "$SYSSTATE" ]] && SYSSTATE="unknown"
[[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"

info "1.5 Verificando sincronización NTP (relevante para correlación de eventos de failover Sentinel)..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización"
fi

MEM_TOTAL_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
MEM_TOTAL_GB=$(( MEM_TOTAL_KB / 1024 / 1024 ))
VCPU=$(nproc)
info "1.6 Recursos nodo ${HOSTNAME_ACTUAL}: ${VCPU} vCPU / ${MEM_TOTAL_GB} GB RAM"
pass "Recursos detectados: ${VCPU} vCPU / ${MEM_TOTAL_GB} GB RAM (verificar contra spec del nodo en la tabla de dimensionamiento)"

# =============================================================================
# SECCIÓN 2 — VERIFICACIÓN DE VERSIÓN
# =============================================================================
header "2. VERSIÓN DE REDIS Y SENTINEL"

# Nota: la versión es una recomendación de homologación, no un requisito
# bloqueante — se reporta como INFO, nunca como WARN/FAIL.
if [[ "$REDIS_CLI_MODE" != "none" ]]; then
  REDIS_VER=$(redis_cli INFO server 2>/dev/null | grep -oP '(?<=^redis_version:)\S+' | tr -d '\r')
  if [[ "$REDIS_VER" == "$REDIS_VERSION_EXPECTED"* ]]; then
    pass "Versión Redis: $REDIS_VER (esperado: ${REDIS_VERSION_EXPECTED}.x)"
  elif [[ -n "$REDIS_VER" ]]; then
    info "Versión Redis: $REDIS_VER — recomendado homologar con ${REDIS_VERSION_EXPECTED}.x"
  else
    info "No se pudo determinar la versión de Redis"
  fi

  SENTINEL_VER=$(sentinel_cli INFO server 2>/dev/null | grep -oP '(?<=^redis_version:)\S+' | tr -d '\r')
  if [[ "$SENTINEL_VER" == "$SENTINEL_VERSION_EXPECTED"* ]]; then
    pass "Versión Sentinel: $SENTINEL_VER (esperado: ${SENTINEL_VERSION_EXPECTED}.x)"
  elif [[ -n "$SENTINEL_VER" ]]; then
    info "Versión Sentinel: $SENTINEL_VER — recomendado homologar con ${SENTINEL_VERSION_EXPECTED}.x"
  else
    info "No se pudo determinar la versión de Sentinel"
  fi
else
  info "redis-cli no disponible (ni en host ni en contenedor Podman) — no se pudo determinar la versión de Redis/Sentinel"
fi

# =============================================================================
# SECCIÓN 3 — ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA
# =============================================================================
header "3. ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA"

if [[ "$NODE_ROLE" != "$NODE_SENTINEL_ONLY" ]]; then
  info "Verificando $REDIS_DATA (RDB/AOF)..."
  if [[ -d "$REDIS_DATA" ]]; then
    OWNER=$(stat -c '%U:%G' "$REDIS_DATA")
    PERMS=$(stat -c '%a' "$REDIS_DATA")
    USAGE=$(df -h "$REDIS_DATA" 2>/dev/null | awk 'NR==2{print $3"/"$2" usado ("$5")"}' || echo "N/A")
    pass "Directorio $REDIS_DATA existe — owner: $OWNER — perms: $PERMS — uso: $USAGE"

    FSTYPE=$(df -T "$REDIS_DATA" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
    [[ "$FSTYPE" == "overlay" ]] \
      && warn "$REDIS_DATA usa overlay filesystem — se recomienda bind mount dedicado para persistencia RDB/AOF" \
      || pass "$REDIS_DATA filesystem: $FSTYPE (no overlay)"

    RDB_COUNT=$(find "$REDIS_DATA" -name "*.rdb" 2>/dev/null | wc -l)
    AOF_COUNT=$(find "$REDIS_DATA" -name "*.aof" -o -name "appendonly*" 2>/dev/null | wc -l)
    info "Archivos de persistencia: ${RDB_COUNT} RDB, ${AOF_COUNT} AOF"
  else
    fail "Directorio $REDIS_DATA NO existe"
  fi
else
  info "Nodo solo-Sentinel ($NODE_SENTINEL_ONLY): se omite verificación de persistencia RDB/AOF"
fi

# =============================================================================
# SECCIÓN 4 — SERVICIOS SYSTEMD Y CONTENEDORES PODMAN
# =============================================================================
header "4. SERVICIOS SYSTEMD Y CONTENEDORES PODMAN"

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

info "4.1 Verificando unidades systemd de Redis/Sentinel (sistema y usuario/Quadlet)..."
SYSTEMD_UNITS_SYSTEM=$(systemctl list-units --type=service 2>/dev/null | grep -iE "redis|sentinel" | awk '{print $1}' || true)
SYSTEMD_UNITS_USER=$(systemctl --user list-units --type=service 2>/dev/null | grep -iE "redis|sentinel" | awk '{print $1}' || true)

if [[ -n "$SYSTEMD_UNITS_SYSTEM" ]]; then
  while IFS= read -r unit; do check_systemd_unit "sistema" "$unit"; done <<< "$SYSTEMD_UNITS_SYSTEM"
fi
if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  while IFS= read -r unit; do check_systemd_unit "usuario" "$unit"; done <<< "$SYSTEMD_UNITS_USER"
fi
if [[ -z "$SYSTEMD_UNITS_SYSTEM" && -z "$SYSTEMD_UNITS_USER" ]]; then
  fail "No se encontraron unidades systemd (sistema ni usuario) con nombre 'redis'/'sentinel'"
fi

if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  info "4.2 Verificando linger de systemd para persistencia de servicios de usuario..."
  LINGER_USER=$(whoami)
  LINGER_STATE=$(loginctl show-user "$LINGER_USER" -p Linger --value 2>/dev/null || echo "unknown")
  [[ "$LINGER_STATE" == "yes" ]] \
    && pass "Linger habilitado para usuario $LINGER_USER" \
    || fail "Linger NO habilitado para usuario $LINGER_USER (Linger=$LINGER_STATE) — corregir con: loginctl enable-linger $LINGER_USER"
fi

if [[ -n "$CONTAINER_REDIS" ]]; then
  info "4.3 Verificando contenedor Podman Redis..."
  # Filtro anclado (^...$): --filter "name=X" en Podman hace match por substring,
  # lo que mezclaba el estado de contenedores distintos (ej. "redis" y "redis-sentinel").
  CSTATUS=$(podman ps --format "{{.Status}}" --filter "name=^${CONTAINER_REDIS}\$" 2>/dev/null || echo "")
  echo "$CSTATUS" | grep -qi "up" \
    && pass "Contenedor: $CONTAINER_REDIS — estado: $CSTATUS" \
    || fail "Contenedor: $CONTAINER_REDIS — estado: $CSTATUS (esperado: Up)"
else
  info "4.3 Sin contenedor Podman detectado — se asume despliegue directo sobre systemd"
fi

# =============================================================================
# SECCIÓN 5 — CONECTIVIDAD DE RED
# =============================================================================
header "5. CONECTIVIDAD DE RED"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando"
  fi
}

if [[ "$NODE_ROLE" != "$NODE_SENTINEL_ONLY" ]]; then
  info "5.1 Puerto Redis (${REDIS_PORT})..."
  check_port_local "$REDIS_PORT" "Redis"
fi
info "5.2 Puerto Sentinel (${SENTINEL_PORT})..."
check_port_local "$SENTINEL_PORT" "Sentinel"

info "5.3 Conectividad entre nodos del clúster (${ALL_NODES[*]})..."
for NODE in "${ALL_NODES[@]}"; do
  if [[ "$NODE" == "$NODE_ROLE" ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE: OK (rtt: ${PING_RTT}ms)"
  else
    fail "Ping a $NODE: no responde — visibilidad del quórum de Sentinel en riesgo"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${SENTINEL_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${SENTINEL_PORT} (Sentinel) alcanzable"
  else
    fail "TCP ${NODE}:${SENTINEL_PORT} (Sentinel) no alcanzable — quórum de failover en riesgo"
  fi
done

# =============================================================================
# SECCIÓN 6 — VALIDACIÓN FUNCIONAL REDIS (PING, replicación)
# =============================================================================
header "6. VALIDACIÓN FUNCIONAL REDIS"

if [[ "$NODE_ROLE" == "$NODE_SENTINEL_ONLY" ]]; then
  info "6. Módulo funcional Redis omitido — nodo Sentinel-only ($NODE_SENTINEL_ONLY)"
elif [[ "$REDIS_CLI_MODE" == "none" ]]; then
  warn "6. Módulo funcional Redis omitido — redis-cli no disponible (ni en host ni en contenedor Podman)"
else
  info "6.1 Verificando PING a Redis local..."
  PING_OUT=$(redis_cli PING)
  if [[ "$PING_OUT" == "PONG" ]]; then
    pass "Redis responde PING: PONG"
  else
    fail "Redis no respondió PING correctamente: $PING_OUT"
  fi

  info "6.2 Verificando rol y estado de replicación (INFO replication)..."
  ROLE=$(redis_cli INFO replication | grep -oP '(?<=^role:)\S+' | tr -d '\r')
  info "Rol detectado en este nodo: ${ROLE:-desconocido}"
  if [[ "$ROLE" == "master" ]]; then
    CONNECTED_SLAVES=$(redis_cli INFO replication | grep -oP '(?<=^connected_slaves:)\d+' | tr -d '\r')
    CONNECTED_SLAVES=${CONNECTED_SLAVES:-0}
    if [[ "$CONNECTED_SLAVES" -ge 1 ]]; then
      pass "Master con $CONNECTED_SLAVES réplica(s) conectada(s)"
    else
      fail "Master SIN réplicas conectadas — sin redundancia de datos"
    fi
  elif [[ "$ROLE" == "slave" ]]; then
    MASTER_LINK=$(redis_cli INFO replication | grep -oP '(?<=^master_link_status:)\S+' | tr -d '\r')
    if [[ "$MASTER_LINK" == "up" ]]; then
      pass "Réplica conectada al master (master_link_status: up)"
    else
      fail "Réplica DESCONECTADA del master (master_link_status: ${MASTER_LINK:-desconocido})"
    fi
    OFFSET_MASTER=$(redis_cli INFO replication | grep -oP '(?<=^master_repl_offset:)\d+' | tr -d '\r')
    OFFSET_SLAVE=$(redis_cli INFO replication | grep -oP '(?<=^slave_repl_offset:)\d+' | tr -d '\r')
    info "Offset de replicación — master: ${OFFSET_MASTER:-N/A} | slave: ${OFFSET_SLAVE:-N/A}"
    if [[ -n "$OFFSET_MASTER" && -n "$OFFSET_SLAVE" ]]; then
      LAG=$(( OFFSET_MASTER - OFFSET_SLAVE ))
      [[ "$LAG" -lt 0 ]] && LAG=0
      if [[ "$LAG" -eq 0 ]]; then
        pass "Réplica sincronizada — sin lag de replicación (offset idéntico)"
      elif [[ "$LAG" -lt 1000000 ]]; then
        pass "Réplica con lag menor (offset diff: ${LAG} bytes) — dentro de rango normal"
      else
        warn "Réplica con lag considerable (offset diff: ${LAG} bytes) — verificar carga de red/CPU"
      fi
    fi
  else
    fail "No se pudo determinar el rol de este nodo Redis"
  fi

  info "6.3 Verificando memoria y política de eviction..."
  MAXMEM=$(redis_cli CONFIG GET maxmemory | tail -1)
  MAXMEM_POLICY=$(redis_cli CONFIG GET maxmemory-policy | tail -1)
  info "maxmemory: ${MAXMEM:-N/A} bytes | maxmemory-policy: ${MAXMEM_POLICY:-N/A}"
  USED_MEM=$(redis_cli INFO memory | grep -oP '(?<=^used_memory_human:)\S+' | tr -d '\r')
  info "Memoria en uso: ${USED_MEM:-N/A}"
  pass "Configuración de memoria reportada — validar manualmente contra el spec del nodo"
fi

# =============================================================================
# SECCIÓN 7 — VALIDACIÓN FUNCIONAL SENTINEL (quórum, failover)
# =============================================================================
header "7. VALIDACIÓN FUNCIONAL SENTINEL"

if [[ "$REDIS_CLI_MODE" != "none" ]]; then
  info "7.1 Verificando PING a Sentinel local..."
  SENTINEL_PING=$(sentinel_cli PING)
  [[ "$SENTINEL_PING" == "PONG" ]] \
    && pass "Sentinel responde PING: PONG" \
    || fail "Sentinel no respondió PING correctamente: $SENTINEL_PING"

  info "7.2 Verificando master monitoreado (SENTINEL masters)..."
  MASTERS_OUT=$(sentinel_cli SENTINEL masters)
  MONITORED_NAMES=$(echo "$MASTERS_OUT" | grep -A1 "^name$" | grep -v "^name$" | grep -v "^--$")
  if echo "$MONITORED_NAMES" | grep -qxi "$MASTER_NAME"; then
    pass "Sentinel monitorea el master '$MASTER_NAME'"
    MASTER_IP=$(echo "$MASTERS_OUT" | grep -A1 "^ip$" | tail -1)
    MASTER_STATUS=$(echo "$MASTERS_OUT" | grep -A1 "^flags$" | tail -1)
    info "IP del master reportada por Sentinel: ${MASTER_IP:-N/A} | flags: ${MASTER_STATUS:-N/A}"
    if echo "$MASTER_STATUS" | grep -qi "master"; then
      pass "Sentinel confirma el nodo como 'master' (sin down flags: o_down/s_down)"
    else
      warn "Flags del master reportadas por Sentinel: ${MASTER_STATUS:-N/A} — verificar si hay down flags activas"
    fi
  elif [[ -n "$MONITORED_NAMES" ]]; then
    fail "Sentinel NO monitorea ningún master llamado '$MASTER_NAME' — master(s) detectado(s): $(echo "$MONITORED_NAMES" | tr '\n' ' ') — reintentar con --master-name <nombre>"
  else
    fail "Sentinel NO monitorea ningún master — SENTINEL masters no devolvió resultados — verificar sentinel.conf"
  fi

  info "7.3 Verificando quórum de Sentinels visibles (SENTINEL sentinels)..."
  SENTINELS_OUT=$(sentinel_cli SENTINEL sentinels "$MASTER_NAME")
  if echo "$SENTINELS_OUT" | grep -qi "^ERR"; then
    fail "No se pudo obtener el listado de Sentinels: $SENTINELS_OUT — verificar --master-name"
  else
    SENTINEL_COUNT=$(echo "$SENTINELS_OUT" | grep -c "^name$" || true)
    # +1 porque SENTINEL sentinels no incluye el propio Sentinel consultado
    TOTAL_SENTINELS=$((SENTINEL_COUNT + 1))
    info "Sentinels visibles desde este nodo: $TOTAL_SENTINELS (esperado: $MIN_SENTINELS_REQUIRED)"
    if [[ "$TOTAL_SENTINELS" -ge "$MIN_SENTINELS_REQUIRED" ]]; then
      pass "Quórum de Sentinels completo: $TOTAL_SENTINELS/$MIN_SENTINELS_REQUIRED"
    else
      fail "Quórum de Sentinels insuficiente: $TOTAL_SENTINELS/$MIN_SENTINELS_REQUIRED — failover automático en riesgo"
    fi
  fi

  info "7.4 Verificando quórum de decisión de failover (SENTINEL ckquorum)..."
  CKQUORUM_OUT=$(sentinel_cli SENTINEL CKQUORUM "$MASTER_NAME")
  if echo "$CKQUORUM_OUT" | grep -qi "^OK"; then
    pass "CKQUORUM OK — $CKQUORUM_OUT"
  else
    fail "CKQUORUM falló — el quórum necesario para decidir un failover puede no alcanzarse: $CKQUORUM_OUT"
  fi
else
  warn "redis-cli no disponible (ni en host ni en contenedor Podman) — no se pudo validar Sentinel funcionalmente"
fi

# =============================================================================
# SECCIÓN 8 — RESILIENCIA DEL CLÚSTER
# =============================================================================
header "8. RESILIENCIA Y ESTADO DEL CLÚSTER"

info "8.1 Estado de cada nodo del clúster (TCP Sentinel port)..."
UP_NODES=0
for NODE in "${ALL_NODES[@]}"; do
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${SENTINEL_PORT}" 2>/dev/null; then
    pass "Nodo $NODE: puerto ${SENTINEL_PORT} alcanzable — UP"
    UP_NODES=$((UP_NODES+1))
  else
    fail "Nodo $NODE: puerto ${SENTINEL_PORT} NO alcanzable — DOWN o inaccesible"
  fi
done

# Redis/Sentinel es función crítica: exige capacidad completa, igual que Kafka
if [[ $UP_NODES -ge $MIN_SENTINELS_REQUIRED ]]; then
  pass "Nodos del clúster: $UP_NODES/3 — capacidad completa (requerido: $MIN_SENTINELS_REQUIRED/3)"
else
  fail "Nodos del clúster: $UP_NODES/3 — POR DEBAJO del mínimo exigido ($MIN_SENTINELS_REQUIRED/3)"
fi

# =============================================================================
# SECCIÓN 9 — LOGS Y OBSERVABILIDAD
# =============================================================================
header "9. LOGS Y OBSERVABILIDAD"

if [[ "$NODE_ROLE" != "$NODE_SENTINEL_ONLY" ]]; then
  info "9.1 Verificando logs en $REDIS_LOGS..."
  if [[ -d "$REDIS_LOGS" ]]; then
    LOG_COUNT=$(find "$REDIS_LOGS" -name "*.log" 2>/dev/null | wc -l)
    if [[ "$LOG_COUNT" -gt 0 ]]; then
      LAST_LOG=$(ls -t "$REDIS_LOGS"/*.log 2>/dev/null | head -1)
      LAST_MODIFIED=$(stat -c '%y' "$LAST_LOG" 2>/dev/null | cut -d. -f1 || echo "N/A")
      pass "Logs en $REDIS_LOGS: $LOG_COUNT archivo(s) — último modificado: $LAST_MODIFIED"

      OOM_WARN=$(grep -ci "oom\|out of memory\|maxmemory" "$LAST_LOG" 2>/dev/null || true)
      [[ "$OOM_WARN" -eq 0 ]] \
        && pass "Sin advertencias de memoria (OOM/maxmemory) en el log más reciente" \
        || warn "Se detectaron $OOM_WARN líneas relacionadas a memoria/OOM en el log más reciente"

      EVICTED=$(redis_cli INFO stats 2>/dev/null | grep -oP '(?<=^evicted_keys:)\d+' | tr -d '\r' || echo "0")
      [[ "${EVICTED:-0}" -eq 0 ]] \
        && pass "Sin keys evictadas por presión de memoria (evicted_keys=0)" \
        || warn "evicted_keys=${EVICTED} — el dataset puede estar excediendo maxmemory"
    else
      warn "Directorio $REDIS_LOGS existe pero sin archivos .log"
    fi
  else
    warn "Directorio $REDIS_LOGS NO existe — verificar logfile en redis.conf"
  fi
else
  info "Nodo solo-Sentinel: se omite verificación de logs de Redis"
fi

info "9.2 Verificando Grafana Alloy (observabilidad)..."
ALLOY_UNITS=$(systemctl list-units --type=service 2>/dev/null | grep -i "alloy" | awk '{print $1}' || true)
if [[ -n "$ALLOY_UNITS" ]]; then
  while IFS= read -r unit; do
    ALLOY_STATE=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    [[ "$ALLOY_STATE" == "active" ]] && pass "Alloy: $unit — activo" || fail "Alloy: $unit — $ALLOY_STATE"
  done <<< "$ALLOY_UNITS"
else
  warn "Grafana Alloy no encontrado como unidad systemd"
fi

info "9.3 Verificando node-exporter..."
ss -tlnp 2>/dev/null | grep -q ":9100 " \
  && pass "node-exporter: puerto 9100 escuchando" \
  || warn "node-exporter: no detectado en puerto 9100"

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN — CLÚSTER PRINCIPAL"

TOTAL_CHECKS=$((PASS_COUNT + ERRORS + WARNINGS))

echo -e "  Cluster       : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo          : ${BOLD}${HOSTNAME_ACTUAL}${NC} (${NODE_ROLE})"
echo -e "  Nodos         : ${ALL_NODES[*]}"
echo -e "  Nodos UP      : ${UP_NODES}/3"
echo -e "  Master        : ${MASTER_NAME}"
echo -e "  Fecha         : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  Total checks  : ${BOLD}${TOTAL_CHECKS}${NC}"
echo -e "  ${GREEN}${BOLD}PASS${NC}          : ${PASS_COUNT}"
echo -e "  ${RED}${BOLD}FAIL${NC}          : ${ERRORS}"
echo -e "  ${YELLOW}${BOLD}WARN${NC}          : ${WARNINGS}"
echo ""
echo -e "  ${BOLD}Notas de diseño (Bluepoint):${NC}"
echo -e "  • Redis NO reduce capacidad en DR — mismos recursos y umbrales que Principal"
echo -e "  • Es la capa de serving online crítica (saldos y movimientos, baja latencia)"
echo -e "  • Un quórum de Sentinel incompleto (<3/3) se reporta como FALLO, no como advertencia"
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
