#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster Kafka DR (Datacenter Alterno)
#  Nodos : pbigd-kaf01-cont | pbigd-kaf02-cont | pbigd-kaf03-cont
#  Versión Kafka : 4.0.1 (KRaft — sin Zookeeper)
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 9092 (broker / client) | 9093 (controller KRaft)
#  Directorios: /opt/kafka  |  /data/kafka  |  /var/log/kafka
#
#  Rol DR: Kafka es el punto de entrada de todo el flujo de datos, por lo
#  que — a diferencia de Flink y MinIO en DR — el diseño de Bluepoint
#  mantiene su capacidad COMPLETA en el datacenter alterno (mismos
#  recursos y mismos umbrales que el cluster Principal: 3 nodos, 3
#  réplicas). Este script por lo tanto NO reduce umbrales de quórum.
#
#  Nota de alcance: este script valida ÚNICAMENTE el cluster Kafka DR.
#  La validación de integración Kafka↔Flink queda fuera de alcance
#  (se realizará en una validación separada).
#
#  Uso:
#    chmod +x validate_kafka_dr.sh
#    ./validate_kafka_dr.sh [--bootstrap-server host:puerto] [--skip-topic-test]
#
#  Parámetros opcionales:
#    --bootstrap-server   Bootstrap server a usar en las pruebas funcionales
#                          (default: localhost:9092)
#    --skip-topic-test    Omitir el ciclo completo de creación/producción/
#                          consumo/borrado de topic de prueba
# =============================================================================

set -euo pipefail

# ── Colores y helpers ──────────────────────────────────────────────────────────
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

# Filtra el ruido de logging propio de los clientes Java de Kafka (dump de
# AdminClientConfig/ProducerConfig/etc., líneas de housekeeping de Metrics/AppInfoParser)
# antes de mostrar la salida de un comando kafka-*.sh. No se usa para el matching
# funcional (grep sobre la variable original), solo para lo que se imprime en pantalla.
clean_kafka_output() {
  grep -vE '^\s*[a-zA-Z][a-zA-Z0-9._-]*\s*=|Config values:|^\s*\(org\.apache\.kafka|^\[[0-9]{4}-[0-9]{2}-[0-9]{2}[^]]*\]\s+INFO\s' | sed '/^\s*$/d'
}

# ── Parámetros ─────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVER="localhost:9092"
SKIP_TOPIC_TEST=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap-server) BOOTSTRAP_SERVER="$2"; shift 2 ;;
    --skip-topic-test)  SKIP_TOPIC_TEST=true;  shift   ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

# ── Configuración del cluster DR ───────────────────────────────────────────────
CLUSTER_NAME="Kafka DR (Datacenter Alterno)"
NODES_DR=("pbigd-kaf01-cont" "pbigd-kaf02-cont" "pbigd-kaf03-cont")
CONTAINER_PATTERN="kafka"        # patrón de nombre/imagen del contenedor real de Kafka (naming: kafka-broker-<índice>)
KAFKA_BROKER_PORT=9092
KAFKA_CONTROLLER_PORT=9093
KAFKA_HOME="/opt/kafka"          # vive DENTRO de la imagen del contenedor (no se espera bind-mount en el host)
KAFKA_DATA="/data/kafka"
KAFKA_LOGS="/var/log/kafka"
KAFKA_VERSION_EXPECTED="4.0.1"
JAVA_VERSION_EXPECTED="25"
JAVA_VERSION_MIN="17"            # mínimo exigido por Kafka 4.x (dejó de soportar Java 8/11)
REPLICATION_FACTOR=3           # Kafka NO reduce capacidad en DR — igual que Principal
MIN_BROKERS_REQUIRED=3          # umbral pleno, sin degradar (a diferencia de Flink/MinIO DR)
TEST_TOPIC="bluepoint-validation-dr-$(date +%s)"
HOSTNAME_ACTUAL=$(hostname -s)

# Detectar rol del nodo actual
NODE_ROLE="desconocido"
for n in "${NODES_DR[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n"; break; fi
done

# Resolver contenedor Kafka local. Se excluyen contenedores infra de pod (imagen vacía,
# p.ej. "pod-kafka-01-infra") exigiendo que el campo Image no esté vacío.
CONTAINER_KAFKA=$(podman ps --format "{{.Names}}\t{{.Image}}" 2>/dev/null | \
  awk -F'\t' -v pat="$CONTAINER_PATTERN" '$2!="" && ($1 ~ pat || $2 ~ pat) {print $1; exit}')

# ── Banner ─────────────────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  ██████╗ ██╗     ██╗   ██╗███████╗██████╗  ██████╗ ██╗███╗   ██╗████████╗"
echo "  ██╔══██╗██║     ██║   ██║██╔════╝██╔══██╗██╔═══██╗██║████╗  ██║╚══██╔══╝"
echo "  ██████╔╝██║     ██║   ██║█████╗  ██████╔╝██║   ██║██║██╔██╗ ██║   ██║   "
echo "  ██╔══██╗██║     ██║   ██║██╔══╝  ██╔═══╝ ██║   ██║██║██║╚██╗██║   ██║   "
echo "  ██████╔╝███████╗╚██████╔╝███████╗██║     ╚██████╔╝██║██║ ╚████║   ██║   "
echo "  ╚═════╝ ╚══════╝ ╚═════╝ ╚══════╝╚═╝      ╚═════╝ ╚═╝╚═╝  ╚═══╝   ╚═╝  "
echo -e "${NC}"
echo -e "${BOLD}  Cooperativa Jardín Azuayo — Proyecto Big Data${NC}"
echo -e "  Cluster: ${BOLD}${CYAN}${CLUSTER_NAME}${NC}"
echo -e "  Nodo actual: ${BOLD}${HOSTNAME_ACTUAL}${NC}  (rol detectado: ${NODE_ROLE})"
echo -e "  Bootstrap server: ${BOOTSTRAP_SERVER}"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  ${YELLOW}${BOLD}Nota: Kafka mantiene capacidad COMPLETA en DR (sin degradar umbrales),${NC}"
echo -e "  ${YELLOW}a diferencia de Flink/MinIO — es el punto de entrada crítico del flujo de datos.${NC}"
echo ""

# =============================================================================
# SECCIÓN 1 — SISTEMA OPERATIVO Y RUNTIME
# =============================================================================
header "1. SISTEMA OPERATIVO Y RUNTIME DR"

# 1.1 Rocky Linux 10.x
info "1.1 Verificando sistema operativo..."
if grep -qi "rocky linux 10" /etc/os-release 2>/dev/null; then
  OS_VER=$(grep "VERSION=" /etc/os-release | head -1 | tr -d '"' | cut -d= -f2)
  pass "SO: Rocky Linux — $OS_VER"
else
  OS_VER=$(grep "PRETTY_NAME" /etc/os-release 2>/dev/null | cut -d= -f2 | tr -d '"' || echo "no detectado")
  fail "SO esperado: Rocky Linux 10.x — encontrado: $OS_VER"
fi

# 1.2 Kernel
info "1.2 Versión del kernel..."
KERNEL=$(uname -r)
pass "Kernel: $KERNEL"

# 1.3 Podman >= 5.x
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
  fail "Podman no encontrado en PATH"
fi

# 1.4 systemd
info "1.4 Verificando systemd..."
SYSSTATE=$(systemctl is-system-running 2>/dev/null) || true
[[ -z "$SYSSTATE" ]] && SYSSTATE="unknown"
[[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"

# 1.5 NTP — crítico para el consenso Raft del quórum de controllers KRaft
info "1.5 Verificando sincronización NTP (crítico para consenso KRaft/Raft)..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización — CRÍTICO en DR para consenso KRaft"
fi

# 1.6 Naming convention del hostname (pbigd-kaf<NN>-cont)
info "1.6 Verificando naming convention del nodo DR..."
if echo "$HOSTNAME_ACTUAL" | grep -qE "^pbigd-kaf[0-9]+-cont$"; then
  pass "Hostname del nodo DR correcto: $HOSTNAME_ACTUAL (patrón pbigd-kaf<NN>-cont)"
else
  warn "Hostname '$HOSTNAME_ACTUAL' no sigue el patrón esperado 'pbigd-kaf<NN>-cont' — verificar"
fi

# 1.7 Recursos del nodo — deben ser equivalentes al Principal (sin reducción)
info "1.7 Verificando recursos del nodo (Kafka DR no se reduce respecto a Principal)..."
MEM_TOTAL_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
MEM_TOTAL_GB=$(( MEM_TOTAL_KB / 1024 / 1024 ))
VCPU=$(nproc)
info "Recursos nodo ${HOSTNAME_ACTUAL}: ${VCPU} vCPU / ${MEM_TOTAL_GB} GB RAM"
if [[ $MEM_TOTAL_GB -ge 7 ]]; then
  pass "RAM equivalente al Principal: ${MEM_TOTAL_GB} GB (spec: 8 GB)"
else
  warn "RAM por debajo de lo especificado para Kafka DR: ${MEM_TOTAL_GB} GB (esperado ~8 GB, igual a Principal)"
fi
if [[ $VCPU -ge 4 ]]; then
  pass "vCPU equivalentes al Principal: $VCPU (spec: 4)"
else
  warn "vCPU por debajo de lo especificado: $VCPU (esperado 4, igual a Principal)"
fi

# =============================================================================
# SECCIÓN 2 — ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA
# =============================================================================
header "2. ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA DR"

# Nota: $KAFKA_HOME (/opt/kafka) NO se valida como directorio de host — vive dentro
# de la imagen del contenedor apache/kafka. Solo se exigen en el host los paths
# persistentes (datos y logs), que sí deben ser bind mounts dedicados.
REQUIRED_DIRS=("$KAFKA_DATA" "$KAFKA_LOGS")
for dir in "${REQUIRED_DIRS[@]}"; do
  info "Verificando $dir ..."
  if [[ -d "$dir" ]]; then
    OWNER=$(stat -c '%U:%G' "$dir")
    PERMS=$(stat -c '%a' "$dir")
    USAGE=$(df -h "$dir" 2>/dev/null | awk 'NR==2{print $3"/"$2" usado ("$5")"}' || echo "N/A")
    pass "Directorio $dir existe — owner: $OWNER — perms: $PERMS — uso: $USAGE"

    if [[ "$dir" == "$KAFKA_DATA" ]]; then
      FSTYPE=$(df -T "$dir" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
      if [[ "$FSTYPE" == "overlay" ]]; then
        fail "$KAFKA_DATA usa overlay filesystem — debe ser bind mount sobre partición dedicada"
      else
        pass "$KAFKA_DATA filesystem: $FSTYPE (no overlay)"
      fi

      AVAIL_GB=$(df -BG "$dir" 2>/dev/null | awk 'NR==2{gsub(/G/,"",$4); print $4}' || echo "0")
      # Margen del 5% sobre el mínimo esperado (100 GB): el overhead normal del
      # filesystem (metadata, bloques reservados, redondeo GB/GiB de df) hace
      # que una partición de 100 GB casi nunca reporte >100 GB libres aunque
      # esté prácticamente vacía. Sin este margen, el check falla siempre.
      KAFKA_DATA_MIN_GB=100
      MIN_AVAIL_GB=$(( KAFKA_DATA_MIN_GB * 95 / 100 ))
      if [[ "$AVAIL_GB" -ge "$MIN_AVAIL_GB" ]] 2>/dev/null; then
        pass "$KAFKA_DATA disponible: ${AVAIL_GB} GB (umbral mínimo: ${MIN_AVAIL_GB} GB)"
      else
        warn "$KAFKA_DATA disponible: ${AVAIL_GB} GB — por debajo de lo especificado para DR (mínimo: ${MIN_AVAIL_GB} GB)"
      fi
    fi
  else
    fail "Directorio $dir NO existe"
  fi
done

# Partición /data independiente
info "Verificando partición /data independiente..."
DATA_MOUNT=$(findmnt -n -o TARGET /data 2>/dev/null || findmnt -n -o TARGET --target "$KAFKA_DATA" 2>/dev/null || echo "")
if [[ -n "$DATA_MOUNT" ]]; then
  pass "Partición /data montada en: $DATA_MOUNT"
else
  warn "/data puede no ser partición independiente — revisar fstab"
fi

# KAFKA_HOME (/opt/kafka) se valida DENTRO del contenedor, no en el host
info "Verificando $KAFKA_HOME dentro del contenedor..."
if [[ -n "$CONTAINER_KAFKA" ]]; then
  if podman exec "$CONTAINER_KAFKA" test -d "$KAFKA_HOME" 2>/dev/null; then
    pass "KAFKA_HOME ($KAFKA_HOME) presente dentro del contenedor $CONTAINER_KAFKA"
  else
    fail "KAFKA_HOME ($KAFKA_HOME) NO encontrado dentro del contenedor $CONTAINER_KAFKA"
  fi
else
  warn "No se pudo verificar KAFKA_HOME dentro del contenedor — contenedor Kafka no detectado aún"
fi

# =============================================================================
# SECCIÓN 3 — SERVICIO SYSTEMD Y CONTENEDOR PODMAN
# =============================================================================
header "3. SERVICIO SYSTEMD Y CONTENEDOR PODMAN DR"

# 3.1 Unidades systemd — se revisan TANTO la instancia de sistema (root) COMO la
# instancia de usuario (rootless), ya que Quadlet genera unidades en ~/.config/
# containers/systemd/ bajo systemctl --user, invisibles para systemctl a secas.
info "3.1 Verificando unidades systemd de Kafka (sistema y usuario/Quadlet)..."

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
    # Unidades Quadlet (scope=usuario) nunca reportan "enabled": is-enabled
    # devuelve "generated" porque el arranque real depende de [Install] en el
    # .container/.pod + linger del usuario, no de systemctl enable. El linger
    # ya se valida aparte (ver LINGER_STATE más abajo); aquí solo se marca FAIL
    # si el scope es de sistema (unidad tradicional) y no está enabled.
    if [[ "$scope" == "sistema" && "$enabled" != "enabled" ]]; then
      fail "  ↳ Unidad '$unit' (${scope}) NO habilitada en boot — CRÍTICO: Kafka DR debe sobrevivir reinicios"
    elif [[ "$scope" == "usuario" && "$enabled" != "generated" && "$enabled" != "enabled" ]]; then
      fail "  ↳ Unidad '$unit' (${scope}) en estado inesperado '$enabled' — verificar sección [Install] en el Quadlet"
    fi
  else
    fail "Unidad systemd (${scope}): $unit — estado: $state — habilitada: $enabled"
  fi
}

SYSTEMD_UNITS_SYSTEM=$(systemctl list-units --type=service 2>/dev/null | grep -i "kafka\|container-kafka" | awk '{print $1}' || true)
SYSTEMD_UNITS_USER=$(systemctl --user list-units --type=service 2>/dev/null | grep -i "kafka\|container-kafka\|pod-kafka" | awk '{print $1}' || true)

if [[ -n "$SYSTEMD_UNITS_SYSTEM" ]]; then
  while IFS= read -r unit; do check_systemd_unit "sistema" "$unit"; done <<< "$SYSTEMD_UNITS_SYSTEM"
fi

if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  while IFS= read -r unit; do check_systemd_unit "usuario" "$unit"; done <<< "$SYSTEMD_UNITS_USER"
fi

if [[ -z "$SYSTEMD_UNITS_SYSTEM" && -z "$SYSTEMD_UNITS_USER" ]]; then
  fail "No se encontraron unidades systemd (sistema ni usuario) con nombre 'kafka' — CRÍTICO en DR: sin gestión de resiliencia"
fi

# 3.2 Contenedor Podman (excluye contenedores infra de pod, sin imagen)
info "3.2 Verificando contenedor Podman Kafka DR..."
CONTAINERS=$(podman ps --format "{{.Names}}\t{{.Status}}\t{{.Image}}" 2>/dev/null | \
  awk -F'\t' '$3!="" && ($1 ~ /kafka/ || $3 ~ /kafka/)' || true)

if [[ -n "$CONTAINERS" ]]; then
  while IFS=$'\t' read -r cname cstatus cimage; do
    if echo "$cstatus" | grep -qi "up"; then
      pass "Contenedor: $cname — estado: $cstatus — imagen: $cimage"
    else
      fail "Contenedor: $cname — estado: $cstatus (esperado: Up)"
    fi
  done <<< "$CONTAINERS"
else
  fail "No se encontró ningún contenedor Kafka corriendo (podman ps) — DR inoperativo"
fi

# 3.3 Naming convention — en DR el contenedor lleva el sufijo "-cont"
# (coherente con el naming de host pbigd-kaf0N-cont), a diferencia de Principal.
info "3.3 Verificando naming convention de contenedores DR..."
if [[ -n "$CONTAINER_KAFKA" ]] && echo "$CONTAINER_KAFKA" | grep -qE "^kafka-broker-cont-[0-9]+$"; then
  pass "Naming convention correcta: $CONTAINER_KAFKA"
else
  warn "Naming convention esperada: kafka-broker-cont-<índice> — nombre actual: ${CONTAINER_KAFKA:-N/A}"
fi

# 3.4 Política de reinicio — con Quadlet, el reinicio lo gestiona systemd
# (Restart= en la unidad), NO el flag nativo de Podman (--restart), por lo que
# hay que revisar ambos antes de concluir que no hay política de resiliencia.
info "3.4 Verificando política de reinicio (Podman nativo y systemd/Quadlet)..."
RESTART_POL=$(podman inspect --format "{{.HostConfig.RestartPolicy.Name}}" "$CONTAINER_KAFKA" 2>/dev/null || echo "N/A")

QUADLET_UNIT="${CONTAINER_KAFKA}.service"
SYSTEMD_RESTART=$(systemctl --user show "$QUADLET_UNIT" -p Restart --value 2>/dev/null || echo "")
[[ -z "$SYSTEMD_RESTART" ]] && SYSTEMD_RESTART=$(systemctl show "$QUADLET_UNIT" -p Restart --value 2>/dev/null || echo "")

if [[ "$RESTART_POL" == "always" ]]; then
  pass "Política de reinicio (Podman nativo): always"
elif [[ "$SYSTEMD_RESTART" == "always" ]]; then
  pass "Política de reinicio gestionada por systemd/Quadlet: Restart=always (unidad: $QUADLET_UNIT) — Podman nativo reporta '$RESTART_POL' porque el reinicio lo gestiona systemd, no Podman"
else
  fail "Política de reinicio: Podman='$RESTART_POL', systemd Restart='${SYSTEMD_RESTART:-N/A}' — CRÍTICO en DR: ninguno garantiza reinicio automático"
fi

# 3.5 Linger de systemd --user — sin esto, las unidades Quadlet rootless se
# detienen al cerrar la sesión del usuario y no arrancan solas tras reiniciar
# el host, dejando el broker DR caído sin que nadie lo note.
if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  info "3.5 Verificando linger de systemd para persistencia de servicios de usuario (Quadlet rootless)..."
  LINGER_USER=$(whoami)
  LINGER_STATE=$(loginctl show-user "$LINGER_USER" -p Linger --value 2>/dev/null || echo "unknown")
  if [[ "$LINGER_STATE" == "yes" ]]; then
    pass "Linger habilitado para usuario $LINGER_USER — el servicio persiste sin sesión activa y arranca en el boot"
  else
    fail "Linger NO habilitado para usuario $LINGER_USER (Linger=$LINGER_STATE) — CRÍTICO en DR: el broker puede detenerse al cerrar sesión o no levantar tras reiniciar el host. Corregir con: loginctl enable-linger $LINGER_USER"
  fi
fi

# =============================================================================
# SECCIÓN 4 — CONECTIVIDAD DE RED
# =============================================================================
header "4. CONECTIVIDAD DE RED DR"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || \
     netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando — Kafka DR puede no estar activo"
  fi
}

info "4.1 Puerto broker / client (${KAFKA_BROKER_PORT})..."
check_port_local $KAFKA_BROKER_PORT "Broker / Client API"

info "4.2 Puerto controller KRaft (${KAFKA_CONTROLLER_PORT})..."
check_port_local $KAFKA_CONTROLLER_PORT "Controller KRaft"

# 4.3 Conectividad intra-cluster DR
info "4.3 Conectividad entre nodos del cluster DR..."
for NODE in "${NODES_DR[@]}"; do
  if [[ "$NODE" == "$NODE_ROLE" ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE (DR): OK (rtt: ${PING_RTT}ms)"
  else
    fail "Ping a $NODE (DR): no responde — cluster DR degradado (Kafka exige capacidad completa)"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${KAFKA_BROKER_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${KAFKA_BROKER_PORT} (DR) alcanzable"
  else
    fail "TCP ${NODE}:${KAFKA_BROKER_PORT} (DR) no alcanzable"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${KAFKA_CONTROLLER_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${KAFKA_CONTROLLER_PORT} (controller, DR) alcanzable"
  else
    warn "TCP ${NODE}:${KAFKA_CONTROLLER_PORT} (controller, DR) no alcanzable — revisar listeners internos"
  fi
done

# =============================================================================
# SECCIÓN 5 — VALIDACIÓN FUNCIONAL (KRaft + topics)
# =============================================================================
header "5. VALIDACIÓN FUNCIONAL DR (KRaft y topics)"

if [[ -z "$CONTAINER_KAFKA" ]]; then
  warn "No se detectó contenedor Kafka local — omitiendo validaciones funcionales"
  SKIP_FUNCTIONAL=true
else
  SKIP_FUNCTIONAL=false
  info "Contenedor Kafka detectado: $CONTAINER_KAFKA"
fi

kexec() { podman exec "$CONTAINER_KAFKA" "$@" 2>&1; }

if [[ "$SKIP_FUNCTIONAL" == false ]]; then

  # 5.1 Confirmar modo KRaft (sin Zookeeper)
  info "5.1 Verificando que el cluster DR opera en modo KRaft (sin Zookeeper)..."
  if ss -tlnp 2>/dev/null | grep -q ":2181 "; then
    fail "Puerto 2181 (Zookeeper) detectado escuchando — el cluster DR debería operar sin Zookeeper (KRaft)"
  else
    pass "Sin puerto 2181 (Zookeeper) activo — consistente con modo KRaft"
  fi
  if kexec pgrep -f "zookeeper" &>/dev/null; then
    fail "Proceso Zookeeper detectado dentro del contenedor — inconsistente con KRaft"
  else
    pass "Sin proceso Zookeeper dentro del contenedor"
  fi

  # 5.2 Estado del quórum de controllers KRaft
  info "5.2 Verificando estado del quórum de metadata (KRaft) en DR..."
  QUORUM_STATUS=$(kexec "${KAFKA_HOME}/bin/kafka-metadata-quorum.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" describe --status || echo "ERROR")
  if echo "$QUORUM_STATUS" | grep -qiE "leaderid|currentleader"; then
    pass "Quórum KRaft DR respondiendo — detalle:"
    info "$(echo "$QUORUM_STATUS" | clean_kafka_output)"
  else
    fail "No se pudo obtener el estado del quórum KRaft en DR (kafka-metadata-quorum.sh)"
  fi

  # 5.3 Brokers registrados — Kafka DR exige el mismo quórum pleno que Principal
  info "5.3 Verificando brokers registrados en el cluster DR..."
  API_VERSIONS=$(kexec "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" || echo "ERROR")
  BROKER_COUNT=$(echo "$API_VERSIONS" | grep -cE "^[a-zA-Z0-9._-]+:[0-9]+ \(id: [0-9]+" || true)
  info "Brokers detectados respondiendo a API versions: $BROKER_COUNT (umbral DR: $MIN_BROKERS_REQUIRED — sin reducir)"
  if [[ "$BROKER_COUNT" -ge "$MIN_BROKERS_REQUIRED" ]]; then
    pass "Brokers activos en DR: $BROKER_COUNT (≥$MIN_BROKERS_REQUIRED requerido — capacidad completa)"
  else
    fail "Brokers activos insuficientes en DR: $BROKER_COUNT (se requieren $MIN_BROKERS_REQUIRED, Kafka DR NO se reduce)"
  fi

  # 5.4 Ciclo funcional de topic de prueba
  if [[ "$SKIP_TOPIC_TEST" == false ]]; then
    info "5.4 Ejecutando ciclo de prueba de topic en DR: crear → producir → consumir → borrar..."

    CREATE_OUT=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
      --create --topic "$TEST_TOPIC" --partitions 3 --replication-factor "$REPLICATION_FACTOR" || echo "ERROR")
    if echo "$CREATE_OUT" | grep -qi "created topic\|already exists"; then
      pass "Topic de prueba creado en DR: $TEST_TOPIC (particiones=3, replication-factor=$REPLICATION_FACTOR)"

      PRODUCE_OUT=$(echo "bluepoint-validation-dr-$(date '+%Y-%m-%d %H:%M:%S')" | \
        kexec "${KAFKA_HOME}/bin/kafka-console-producer.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TEST_TOPIC" || echo "ERROR")
      if [[ "$PRODUCE_OUT" != "ERROR" ]]; then
        pass "Producción de mensaje de prueba en DR: OK"
      else
        fail "Producción de mensaje de prueba en DR: FALLO"
      fi

      CONSUME_OUT=$(kexec timeout 15 "${KAFKA_HOME}/bin/kafka-console-consumer.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TEST_TOPIC" \
        --from-beginning --max-messages 1 --timeout-ms 10000 || echo "")
      if echo "$CONSUME_OUT" | grep -q "bluepoint-validation-dr"; then
        pass "Consumo de mensaje de prueba en DR: OK"
      else
        fail "Consumo de mensaje de prueba en DR: FALLO o timeout"
      fi

      DESCRIBE_OUT=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe --topic "$TEST_TOPIC" || echo "ERROR")
      info "$(echo "$DESCRIBE_OUT" | clean_kafka_output)"
      ISR_COUNT=$(echo "$DESCRIBE_OUT" | grep -oP 'Isr: \K[0-9,]+' | head -1 | awk -F',' '{print NF}' || echo 0)
      if [[ "$ISR_COUNT" -ge "$REPLICATION_FACTOR" ]] 2>/dev/null; then
        pass "Replicación interna en DR: ISR completo ($ISR_COUNT/$REPLICATION_FACTOR réplicas sincronizadas)"
      else
        fail "Replicación interna incompleta en DR: ISR=$ISR_COUNT (esperado $REPLICATION_FACTOR)"
      fi

      kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
        --delete --topic "$TEST_TOPIC" &>/dev/null && \
        info "Topic de prueba '$TEST_TOPIC' eliminado."
    else
      fail "Creación de topic de prueba en DR: FALLO — $(echo "$CREATE_OUT" | clean_kafka_output)"
    fi
  else
    info "5.4 Ciclo de prueba de topic omitido (--skip-topic-test)"
  fi

else
  info "Módulo funcional omitido — sin contenedor Kafka local disponible"
fi

# =============================================================================
# SECCIÓN 6 — RESILIENCIA DEL CLUSTER DR
# =============================================================================
header "6. RESILIENCIA Y ESTADO DEL CLUSTER DR"

if [[ "$SKIP_FUNCTIONAL" == false ]]; then
  info "6.1 Verificando particiones sub-replicadas en el cluster DR..."
  UNDER_REPLICATED=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe --under-replicated-partitions || echo "")
  # La decisión se toma sobre la salida YA FILTRADA de ruido de logging — la variable
  # cruda casi nunca está vacía (siempre trae logging del cliente), lo que daría falso positivo.
  # || true: si clean_kafka_output filtra el 100% del ruido (nada real que reportar),
  # su grep -v interno sale con status 1 — sin este guard, pipefail+set -e mataría el script aquí.
  UNDER_REPLICATED_CLEAN=$(echo "$UNDER_REPLICATED" | clean_kafka_output || true)
  if [[ -z "$UNDER_REPLICATED_CLEAN" ]]; then
    pass "Sin particiones sub-replicadas detectadas en DR"
  else
    fail "Particiones sub-replicadas detectadas en DR:"
    info "$UNDER_REPLICATED_CLEAN"
  fi
else
  warn "6.1 No evaluado — sin acceso funcional al contenedor"
fi

info "6.2 Estado de Kafka en cada nodo del cluster DR (TCP broker port)..."
UP_NODES=0
for NODE in "${NODES_DR[@]}"; do
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${KAFKA_BROKER_PORT}" 2>/dev/null; then
    pass "Nodo DR $NODE: puerto ${KAFKA_BROKER_PORT} alcanzable — UP"
    UP_NODES=$((UP_NODES+1))
  else
    fail "Nodo DR $NODE: puerto ${KAFKA_BROKER_PORT} NO alcanzable — DOWN o inaccesible"
  fi
done

# Kafka DR exige capacidad completa: 3/3, no quórum degradado como en MinIO/Flink DR
if [[ $UP_NODES -ge $MIN_BROKERS_REQUIRED ]]; then
  pass "Nodos del cluster DR: $UP_NODES/3 — capacidad completa (requerido: $MIN_BROKERS_REQUIRED/3)"
else
  fail "Nodos del cluster DR: $UP_NODES/3 — POR DEBAJO del mínimo exigido ($MIN_BROKERS_REQUIRED/3) — Kafka DR degradado"
fi

# =============================================================================
# SECCIÓN 7 — LOGS Y OBSERVABILIDAD
# =============================================================================
header "7. LOGS Y OBSERVABILIDAD DR"

# 7.1 Logs
info "7.1 Verificando logs en $KAFKA_LOGS..."
if [[ -d "$KAFKA_LOGS" ]]; then
  LOG_COUNT=$(find "$KAFKA_LOGS" -name "*.log" 2>/dev/null | wc -l)
  if [[ "$LOG_COUNT" -gt 0 ]]; then
    LAST_LOG=$(ls -t "$KAFKA_LOGS"/*.log 2>/dev/null | head -1)
    LAST_MODIFIED=$(stat -c '%y' "$LAST_LOG" 2>/dev/null | cut -d. -f1 || echo "N/A")
    pass "Logs en $KAFKA_LOGS: $LOG_COUNT archivo(s) — último modificado: $LAST_MODIFIED"
  else
    warn "Directorio $KAFKA_LOGS existe pero sin archivos .log"
  fi
else
  fail "Directorio $KAFKA_LOGS NO existe"
fi

# 7.2 Errores recientes en logs del contenedor
if [[ "$SKIP_FUNCTIONAL" == false ]]; then
  # Excluye falsos positivos conocidos: WARN benignos cuyo texto explicativo
  # contiene la palabra "error" (ej. UNKNOWN_TOPIC_ID transitorio en
  # __consumer_offsets — ver 7.2b), y exige que el match sea sobre el nivel
  # de log real (ERROR/FATAL) o "Exception", no sobre cualquier substring.
  ERRORS_LOG=$(podman logs --tail 100 "$CONTAINER_KAFKA" 2>/dev/null | \
    grep -v "UNKNOWN_TOPIC_ID" | grep -ciE '\](\s*(ERROR|FATAL)\s|.*Exception)' || true)
  if [[ "$ERRORS_LOG" -eq 0 ]]; then
    pass "Sin errores críticos en los últimos 100 logs del contenedor DR"
  else
    warn "Se detectaron $ERRORS_LOG líneas con ERROR/Exception/FATAL en logs del contenedor DR"
  fi

  # 7.2b UNKNOWN_TOPIC_ID en particiones internas (ej. __consumer_offsets) —
  # patrón transitorio documentado por Kafka durante creación/reasignación de
  # particiones. Se usa "podman logs --since" (no parseo manual de timestamps)
  # para evitar desajustes de zona horaria entre el reloj del host y el del
  # contenedor.
  UNKNOWN_TOPIC_ID_STALE_WINDOW="5m"
  info "7.2b Verificando warnings UNKNOWN_TOPIC_ID (transitorios esperados en __consumer_offsets)..."
  UTID_TOTAL=$(podman logs --tail 500 "$CONTAINER_KAFKA" 2>/dev/null | grep -c "UNKNOWN_TOPIC_ID" || true)
  if [[ "$UTID_TOTAL" -eq 0 ]]; then
    pass "Sin warnings UNKNOWN_TOPIC_ID en los últimos 500 logs del contenedor DR"
  else
    UTID_RECENT=$(podman logs --since "$UNKNOWN_TOPIC_ID_STALE_WINDOW" "$CONTAINER_KAFKA" 2>/dev/null | grep -c "UNKNOWN_TOPIC_ID" || true)
    if [[ "$UTID_RECENT" -eq 0 ]]; then
      pass "UNKNOWN_TOPIC_ID: $UTID_TOTAL ocurrencia(s) históricas, ninguna en los últimos $UNKNOWN_TOPIC_ID_STALE_WINDOW — transitorio y resuelto (creación/reasignación de particiones)"
    else
      warn "UNKNOWN_TOPIC_ID: $UTID_RECENT ocurrencia(s) en los últimos $UNKNOWN_TOPIC_ID_STALE_WINDOW — aún activo, verificar si persiste"
    fi
  fi

  MEM_WARN=$(podman logs --tail 200 "$CONTAINER_KAFKA" 2>/dev/null | grep -ci "OutOfMemory\|GC overhead\|heap space" || true)
  if [[ "$MEM_WARN" -eq 0 ]]; then
    pass "Sin presión de memoria reportada en logs del contenedor DR"
  else
    fail "Detectadas $MEM_WARN advertencias de memoria en el contenedor DR — recursos insuficientes"
  fi
fi

# 7.3 Grafana Alloy
info "7.3 Verificando Grafana Alloy (observabilidad)..."
ALLOY_UNITS=$(systemctl list-units --type=service 2>/dev/null | grep -i "alloy" | awk '{print $1}' || true)
if [[ -n "$ALLOY_UNITS" ]]; then
  while IFS= read -r unit; do
    ALLOY_STATE=$(systemctl is-active "$unit" 2>/dev/null || echo "unknown")
    [[ "$ALLOY_STATE" == "active" ]] && pass "Alloy: $unit — activo" || fail "Alloy: $unit — $ALLOY_STATE"
  done <<< "$ALLOY_UNITS"
else
  ALLOY_CTR=$(podman ps --format "{{.Names}}\t{{.Status}}" 2>/dev/null | grep -i "alloy" || true)
  if [[ -n "$ALLOY_CTR" ]]; then
    pass "Alloy corriendo como contenedor Podman: $ALLOY_CTR"
  else
    warn "Grafana Alloy no encontrado — observabilidad limitada en contingencia"
  fi
fi

# 7.4 node-exporter
info "7.4 Verificando node-exporter..."
if ss -tlnp 2>/dev/null | grep -q ":9100 "; then
  pass "node-exporter: puerto 9100 escuchando"
else
  warn "node-exporter: no detectado en puerto 9100"
fi

# =============================================================================
# SECCIÓN 8 — VERIFICACIÓN DE VERSIÓN
# =============================================================================
header "8. VERSIÓN DE KAFKA Y JAVA (DR)"

if [[ "$SKIP_FUNCTIONAL" == false ]]; then
  info "8.1 Verificando versión de Kafka en DR..."
  # Método preferido: los kafka-*.sh soportan --version nativamente (no requiere tocar el broker)
  KAFKA_VER=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --version 2>/dev/null | head -1 | awk '{print $1}')
  # Fallback: parsear el jar de kafka en libs/ (grep -E, no -P: la imagen trae BusyBox grep sin PCRE)
  if [[ -z "$KAFKA_VER" ]]; then
    KAFKA_VER=$(kexec bash -c "ls ${KAFKA_HOME}/libs 2>/dev/null | grep -E '^kafka_[0-9.]+-[0-9.]+\.jar' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1")
  fi
  KAFKA_VER=${KAFKA_VER:-"no disponible"}
  if [[ "$KAFKA_VER" == "$KAFKA_VERSION_EXPECTED" ]]; then
    pass "Versión Kafka DR: $KAFKA_VER (igual a Principal: $KAFKA_VERSION_EXPECTED)"
  elif [[ "$KAFKA_VER" != "no disponible" ]]; then
    warn "Versión Kafka DR: $KAFKA_VER — verificar homologación con Principal ($KAFKA_VERSION_EXPECTED)"
  else
    warn "No se pudo determinar la versión de Kafka en DR"
  fi

  info "8.2 Verificando versión de Java dentro del contenedor DR..."
  JVM_IN=$(kexec java -version 2>&1 | head -1 || echo "error")
  info "JVM en contenedor Kafka DR: $JVM_IN"
  JAVA_MAJOR=$(echo "$JVM_IN" | grep -oP '"\K[0-9]+' | head -1 || true)
  JAVA_MAJOR=${JAVA_MAJOR:-0}
  if [[ "$JAVA_MAJOR" -ge "$JAVA_VERSION_MIN" ]] 2>/dev/null; then
    if [[ "$JAVA_MAJOR" == "$JAVA_VERSION_EXPECTED" ]]; then
      pass "OpenJDK $JAVA_VERSION_EXPECTED confirmado dentro del contenedor DR (versión homologada)"
    else
      pass "JDK $JAVA_MAJOR compatible con Kafka 4.x (mínimo Java $JAVA_VERSION_MIN) — no es la versión homologada ($JAVA_VERSION_EXPECTED) pero es funcional"
    fi
  else
    fail "JDK dentro del contenedor DR por debajo del mínimo requerido por Kafka 4.x (Java $JAVA_VERSION_MIN+) — detectado: $JVM_IN"
  fi
else
  warn "8. Verificación de versión omitida — sin acceso funcional al contenedor"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN — CLUSTER DR"

TOTAL_CHECKS=$((PASS_COUNT + ERRORS + WARNINGS))

echo -e "  Cluster       : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo          : ${BOLD}${HOSTNAME_ACTUAL}${NC}"
echo -e "  Nodos DR      : ${NODES_DR[*]}"
echo -e "  Nodos UP      : ${UP_NODES}/3"
echo -e "  Fecha         : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  Total checks  : ${BOLD}${TOTAL_CHECKS}${NC}"
echo -e "  ${GREEN}${BOLD}PASS${NC}          : ${PASS_COUNT}"
echo -e "  ${RED}${BOLD}FAIL${NC}          : ${ERRORS}"
echo -e "  ${YELLOW}${BOLD}WARN${NC}          : ${WARNINGS}"
echo ""
echo -e "  ${BOLD}Notas de diseño DR (Bluepoint):${NC}"
echo -e "  • Kafka NO reduce capacidad en DR — mismos recursos y umbrales que Principal"
echo -e "  • Es el punto de entrada crítico de todo el flujo de datos (outbox y replicación)"
echo -e "  • Un quórum incompleto (<3/3) se reporta como FALLO, no como advertencia"
echo ""

if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN DR COMPLETADA SIN ERRORES NI ADVERTENCIAS${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN DR COMPLETADA CON ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${YELLOW}      Revisar advertencias antes de depender del DR en contingencia.${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN DR FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${RED}      Corregir errores críticos — el cluster DR no está listo para contingencia.${NC}"
fi
echo ""

exit $ERRORS
