#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: Cluster Kafka Principal (DC Principal)
#  Nodos : pbigd-kaf01 | pbigd-kaf02 | pbigd-kaf03
#  Versión Kafka : 4.0.1 (KRaft — sin Zookeeper)
#  SO base : Rocky Linux 10.x
#  Runtime : Podman 5.x  +  systemd
#  Puertos : 9092 (broker / client) | 9093 (controller KRaft)
#  Directorios: /opt/kafka  |  /data/kafka  |  /var/log/kafka
#  Rol: backbone de eventos — transporte de mensajes para Transactional
#       Outbox (near-realtime) y replicación diferida.
#
#  Nota de alcance: este script valida ÚNICAMENTE el cluster Kafka.
#  La validación de integración Kafka↔Flink queda fuera de alcance
#  (se realizará en una validación separada).
#
#  Uso:
#    chmod +x validate_kafka_principal.sh
#    ./validate_kafka_principal.sh [--bootstrap-server host:puerto] [--skip-topic-test]
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

# ── Configuración del cluster ──────────────────────────────────────────────────
CLUSTER_NAME="Kafka Principal (DC Principal)"
NODES=("pbigd-kaf01" "pbigd-kaf02" "pbigd-kaf03")
CONTAINER_PATTERN="kafka"        # patrón de nombre/imagen del contenedor real de Kafka (naming: kafka-broker-<índice>)
KAFKA_BROKER_PORT=9092
KAFKA_CONTROLLER_PORT=9093
KAFKA_HOME="/opt/kafka"          # vive DENTRO de la imagen del contenedor (no se espera bind-mount en el host)
KAFKA_DATA="/data/kafka"
KAFKA_LOGS="/var/log/kafka"
KAFKA_VERSION_EXPECTED="4.0.1"
JAVA_VERSION_EXPECTED="25"
JAVA_VERSION_MIN="17"            # mínimo exigido por Kafka 4.x (dejó de soportar Java 8/11)
REPLICATION_FACTOR=3
TEST_TOPIC="bluepoint-validation-$(date +%s)"
HOSTNAME_ACTUAL=$(hostname -s)

# Detectar en qué nodo estamos ejecutando
NODE_ROLE="desconocido"
for n in "${NODES[@]}"; do
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
echo ""

# =============================================================================
# SECCIÓN 1 — SISTEMA OPERATIVO Y RUNTIME
# =============================================================================
header "1. SISTEMA OPERATIVO Y RUNTIME"

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

# 1.4 systemd activo
info "1.4 Verificando systemd..."
SYSSTATE=$(systemctl is-system-running 2>/dev/null || echo "unknown")
[[ "$SYSSTATE" == "running" ]] && pass "systemd: $SYSSTATE" || warn "systemd estado: $SYSSTATE"

# 1.5 NTP — crítico para el consenso Raft del quórum de controllers KRaft
info "1.5 Verificando sincronización NTP (crítico para consenso KRaft/Raft)..."
if timedatectl status 2>/dev/null | grep -q "NTP service: active\|synchronized: yes"; then
  pass "NTP sincronizado"
elif command -v chronyc &>/dev/null && chronyc tracking &>/dev/null; then
  OFFSET=$(chronyc tracking 2>/dev/null | grep "System time" | awk '{print $4, $5}' || echo "N/A")
  pass "chrony activo — offset: $OFFSET"
else
  warn "NTP/chrony: no se pudo verificar sincronización — el desfase de reloj afecta al quórum KRaft"
fi

# 1.6 Naming convention del hostname (pbigd-kaf<NN>)
info "1.6 Verificando naming convention del nodo..."
if echo "$HOSTNAME_ACTUAL" | grep -qE "^pbigd-kaf[0-9]+$"; then
  pass "Hostname correcto: $HOSTNAME_ACTUAL (patrón pbigd-kaf<NN>)"
else
  warn "Hostname '$HOSTNAME_ACTUAL' no sigue el patrón esperado 'pbigd-kaf<NN>' — verificar"
fi

# =============================================================================
# SECCIÓN 2 — ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA
# =============================================================================
header "2. ESTRUCTURA DE DIRECTORIOS Y PERSISTENCIA"

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

    # /data/kafka no debe ser overlay — debe ser bind mount sobre partición dedicada
    if [[ "$dir" == "$KAFKA_DATA" ]]; then
      FSTYPE=$(df -T "$dir" 2>/dev/null | awk 'NR==2{print $2}' || echo "unknown")
      if [[ "$FSTYPE" == "overlay" ]]; then
        fail "$KAFKA_DATA usa overlay filesystem — debe ser bind mount sobre partición dedicada"
      else
        pass "$KAFKA_DATA filesystem: $FSTYPE (no overlay)"
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
  warn "/data puede no ser una partición independiente — revisar fstab"
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
header "3. SERVICIO SYSTEMD Y CONTENEDOR PODMAN"

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
  warn "No se encontraron unidades systemd (sistema ni usuario) con nombre 'kafka' — verificar manualmente"
fi

# 3.2 Contenedor Podman corriendo (excluye contenedores infra de pod, sin imagen)
info "3.2 Verificando contenedor Podman Kafka..."
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
  fail "No se encontró ningún contenedor Kafka corriendo (podman ps)"
fi

# 3.3 Naming convention: kafka-broker-<índice>
info "3.3 Verificando naming convention de contenedores..."
if [[ -n "$CONTAINER_KAFKA" ]] && echo "$CONTAINER_KAFKA" | grep -qE "^kafka-broker-[0-9]+$"; then
  pass "Naming convention correcta: $CONTAINER_KAFKA"
else
  warn "Naming convention esperada: kafka-broker-<índice> — nombre actual: ${CONTAINER_KAFKA:-N/A}"
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
  warn "Política de reinicio: Podman='$RESTART_POL', systemd Restart='${SYSTEMD_RESTART:-N/A}' — ninguno garantiza reinicio automático"
fi

# 3.5 Linger de systemd --user — sin esto, las unidades Quadlet rootless se
# detienen al cerrar la sesión del usuario y no arrancan solas tras reiniciar
# el host, dejando el broker caído sin que nadie lo note.
if [[ -n "$SYSTEMD_UNITS_USER" ]]; then
  info "3.5 Verificando linger de systemd para persistencia de servicios de usuario (Quadlet rootless)..."
  LINGER_USER=$(whoami)
  LINGER_STATE=$(loginctl show-user "$LINGER_USER" -p Linger --value 2>/dev/null || echo "unknown")
  if [[ "$LINGER_STATE" == "yes" ]]; then
    pass "Linger habilitado para usuario $LINGER_USER — el servicio persiste sin sesión activa y arranca en el boot"
  else
    fail "Linger NO habilitado para usuario $LINGER_USER (Linger=$LINGER_STATE) — el broker puede detenerse al cerrar sesión o no levantar tras reiniciar el host. Corregir con: loginctl enable-linger $LINGER_USER"
  fi
fi

# =============================================================================
# SECCIÓN 4 — CONECTIVIDAD DE RED
# =============================================================================
header "4. CONECTIVIDAD DE RED"

check_port_local(){
  local PORT=$1 DESC=$2
  if ss -tlnp 2>/dev/null | grep -q ":${PORT} " || \
     netstat -tlnp 2>/dev/null | grep -q ":${PORT} "; then
    pass "Puerto $PORT ($DESC) ESCUCHANDO localmente"
  else
    fail "Puerto $PORT ($DESC) NO escuchando — Kafka puede no estar activo"
  fi
}

info "4.1 Puerto broker / client (${KAFKA_BROKER_PORT})..."
check_port_local $KAFKA_BROKER_PORT "Broker / Client API"

info "4.2 Puerto controller KRaft (${KAFKA_CONTROLLER_PORT})..."
check_port_local $KAFKA_CONTROLLER_PORT "Controller KRaft"

# 4.3 Conectividad entre nodos del cluster
info "4.3 Conectividad entre nodos del cluster..."
for NODE in "${NODES[@]}"; do
  if [[ "$NODE" == "$NODE_ROLE" ]]; then continue; fi
  if ping -c1 -W2 "$NODE" &>/dev/null 2>&1; then
    PING_RTT=$(ping -c1 -W2 "$NODE" 2>/dev/null | grep "time=" | awk -F'time=' '{print $2}' | awk '{print $1}')
    pass "Ping a $NODE: OK (rtt: ${PING_RTT}ms)"
  else
    warn "Ping a $NODE: no resuelve o no responde — verificar DNS/resolución entre nodos"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${KAFKA_BROKER_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${KAFKA_BROKER_PORT} alcanzable"
  else
    fail "TCP ${NODE}:${KAFKA_BROKER_PORT} no alcanzable — revisar firewall/networking"
  fi

  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${KAFKA_CONTROLLER_PORT}" 2>/dev/null; then
    pass "TCP ${NODE}:${KAFKA_CONTROLLER_PORT} (controller) alcanzable"
  else
    warn "TCP ${NODE}:${KAFKA_CONTROLLER_PORT} (controller) no alcanzable — revisar listeners internos"
  fi
done

# =============================================================================
# SECCIÓN 5 — VALIDACIÓN FUNCIONAL (KRaft + topics)
# =============================================================================
header "5. VALIDACIÓN FUNCIONAL (KRaft y topics)"

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
  info "5.1 Verificando que el cluster opera en modo KRaft (sin Zookeeper)..."
  if ss -tlnp 2>/dev/null | grep -q ":2181 "; then
    fail "Puerto 2181 (Zookeeper) detectado escuchando — el cluster debería operar sin Zookeeper (KRaft)"
  else
    pass "Sin puerto 2181 (Zookeeper) activo — consistente con modo KRaft"
  fi
  if kexec pgrep -f "zookeeper" &>/dev/null; then
    fail "Proceso Zookeeper detectado dentro del contenedor — inconsistente con KRaft"
  else
    pass "Sin proceso Zookeeper dentro del contenedor"
  fi

  # 5.2 Estado del quórum de controllers KRaft
  info "5.2 Verificando estado del quórum de metadata (KRaft)..."
  QUORUM_STATUS=$(kexec "${KAFKA_HOME}/bin/kafka-metadata-quorum.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" describe --status || echo "ERROR")
  if echo "$QUORUM_STATUS" | grep -qiE "leaderid|currentleader"; then
    pass "Quórum KRaft respondiendo — detalle:"
    info "$(echo "$QUORUM_STATUS" | clean_kafka_output)"
  else
    fail "No se pudo obtener el estado del quórum KRaft (kafka-metadata-quorum.sh)"
  fi

  # 5.3 Brokers registrados
  info "5.3 Verificando brokers registrados en el cluster..."
  API_VERSIONS=$(kexec "${KAFKA_HOME}/bin/kafka-broker-api-versions.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVER" || echo "ERROR")
  BROKER_COUNT=$(echo "$API_VERSIONS" | grep -cE "^[a-zA-Z0-9._-]+:[0-9]+ \(id: [0-9]+" || true)
  info "Brokers detectados respondiendo a API versions: $BROKER_COUNT"
  if [[ "$BROKER_COUNT" -ge 3 ]]; then
    pass "Brokers activos: $BROKER_COUNT (≥3 requerido en Principal)"
  else
    fail "Brokers activos insuficientes: $BROKER_COUNT (se esperan 3 en DC Principal)"
  fi

  # 5.4 Ciclo funcional de topic de prueba
  if [[ "$SKIP_TOPIC_TEST" == false ]]; then
    info "5.4 Ejecutando ciclo de prueba de topic: crear → producir → consumir → borrar..."

    CREATE_OUT=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
      --create --topic "$TEST_TOPIC" --partitions 3 --replication-factor "$REPLICATION_FACTOR" || echo "ERROR")
    if echo "$CREATE_OUT" | grep -qi "created topic\|already exists"; then
      pass "Topic de prueba creado: $TEST_TOPIC (particiones=3, replication-factor=$REPLICATION_FACTOR)"

      # Producción
      PRODUCE_OUT=$(echo "bluepoint-validation-$(date '+%Y-%m-%d %H:%M:%S')" | \
        kexec "${KAFKA_HOME}/bin/kafka-console-producer.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TEST_TOPIC" || echo "ERROR")
      if [[ "$PRODUCE_OUT" != "ERROR" ]]; then
        pass "Producción de mensaje de prueba: OK"
      else
        fail "Producción de mensaje de prueba: FALLO"
      fi

      # Consumo
      CONSUME_OUT=$(kexec timeout 15 "${KAFKA_HOME}/bin/kafka-console-consumer.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVER" --topic "$TEST_TOPIC" \
        --from-beginning --max-messages 1 --timeout-ms 10000 || echo "")
      if echo "$CONSUME_OUT" | grep -q "bluepoint-validation"; then
        pass "Consumo de mensaje de prueba: OK"
      else
        fail "Consumo de mensaje de prueba: FALLO o timeout"
      fi

      # Verificar replicación interna (ISR completo)
      DESCRIBE_OUT=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe --topic "$TEST_TOPIC" || echo "ERROR")
      info "$(echo "$DESCRIBE_OUT" | clean_kafka_output)"
      REPLICA_COUNT=$(echo "$DESCRIBE_OUT" | grep -oP 'Replicas: \K[0-9,]+' | head -1 | awk -F',' '{print NF}' || echo 0)
      ISR_COUNT=$(echo "$DESCRIBE_OUT" | grep -oP 'Isr: \K[0-9,]+' | head -1 | awk -F',' '{print NF}' || echo 0)
      if [[ "$ISR_COUNT" -ge "$REPLICATION_FACTOR" ]] 2>/dev/null; then
        pass "Replicación interna: ISR completo ($ISR_COUNT/$REPLICATION_FACTOR réplicas sincronizadas)"
      else
        fail "Replicación interna incompleta: ISR=$ISR_COUNT (esperado $REPLICATION_FACTOR) — revisar réplicas sub-sincronizadas"
      fi

      # Limpieza
      kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
        --delete --topic "$TEST_TOPIC" &>/dev/null && \
        info "Topic de prueba '$TEST_TOPIC' eliminado."
    else
      fail "Creación de topic de prueba: FALLO — $(echo "$CREATE_OUT" | clean_kafka_output)"
    fi
  else
    info "5.4 Ciclo de prueba de topic omitido (--skip-topic-test)"
  fi

else
  info "Módulo funcional omitido — sin contenedor Kafka local disponible"
fi

# =============================================================================
# SECCIÓN 6 — RESILIENCIA DEL CLUSTER
# =============================================================================
header "6. RESILIENCIA Y ESTADO DEL CLUSTER"

if [[ "$SKIP_FUNCTIONAL" == false ]]; then
  info "6.1 Verificando particiones sub-replicadas en el cluster..."
  UNDER_REPLICATED=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe --under-replicated-partitions || echo "")
  # La decisión se toma sobre la salida YA FILTRADA de ruido de logging — la variable
  # cruda casi nunca está vacía (siempre trae logging del cliente), lo que daría falso positivo.
  # || true: si clean_kafka_output filtra el 100% del ruido (nada real que reportar),
  # su grep -v interno sale con status 1 — sin este guard, pipefail+set -e mataría el script aquí.
  UNDER_REPLICATED_CLEAN=$(echo "$UNDER_REPLICATED" | clean_kafka_output || true)
  if [[ -z "$UNDER_REPLICATED_CLEAN" ]]; then
    pass "Sin particiones sub-replicadas detectadas"
  else
    fail "Particiones sub-replicadas detectadas:"
    info "$UNDER_REPLICATED_CLEAN"
  fi

  info "6.2 Verificando quórum de brokers (3/3 requerido en Principal)..."
  if [[ "$BROKER_COUNT" -ge 3 ]]; then
    pass "Quórum de brokers: $BROKER_COUNT/3 — completo"
  else
    fail "Quórum de brokers: $BROKER_COUNT/3 — cluster degradado"
  fi
else
  warn "6. Resiliencia del cluster no evaluada — sin acceso funcional al contenedor"
fi

info "6.3 Estado de Kafka en cada nodo del cluster (TCP broker port)..."
UP_NODES=0
for NODE in "${NODES[@]}"; do
  if timeout 3 bash -c "echo >/dev/tcp/${NODE}/${KAFKA_BROKER_PORT}" 2>/dev/null; then
    pass "Nodo $NODE: puerto ${KAFKA_BROKER_PORT} alcanzable — UP"
    UP_NODES=$((UP_NODES+1))
  else
    fail "Nodo $NODE: puerto ${KAFKA_BROKER_PORT} NO alcanzable — DOWN o inaccesible"
  fi
done
if [[ $UP_NODES -ge 3 ]]; then
  pass "Nodos del cluster Principal: $UP_NODES/3 — completo"
else
  fail "Nodos del cluster Principal: $UP_NODES/3 — cluster degradado"
fi

# =============================================================================
# SECCIÓN 7 — LOGS Y OBSERVABILIDAD
# =============================================================================
header "7. LOGS Y OBSERVABILIDAD"

# 7.1 Logs en /var/log/kafka
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
    pass "Sin errores críticos en los últimos 100 logs del contenedor"
  else
    warn "Se detectaron $ERRORS_LOG líneas con ERROR/Exception/FATAL en logs del contenedor"
  fi
fi

# 7.2b UNKNOWN_TOPIC_ID en particiones internas (ej. __consumer_offsets) — patrón
# transitorio documentado por Kafka durante creación/reasignación de particiones.
# Solo se escala a warning si sigue ocurriendo dentro de la ventana reciente;
# se usa "podman logs --since" (no parseo manual de timestamps) para evitar
# desajustes de zona horaria entre el reloj del host y el del contenedor.
UNKNOWN_TOPIC_ID_STALE_WINDOW="5m"  # ventana sin nuevas ocurrencias para considerarlo resuelto

if [[ "$SKIP_FUNCTIONAL" == false ]]; then
  info "7.2b Verificando warnings UNKNOWN_TOPIC_ID (transitorios esperados en __consumer_offsets)..."
  UTID_TOTAL=$(podman logs --tail 500 "$CONTAINER_KAFKA" 2>/dev/null | grep -c "UNKNOWN_TOPIC_ID" || true)
  if [[ "$UTID_TOTAL" -eq 0 ]]; then
    pass "Sin warnings UNKNOWN_TOPIC_ID en los últimos 500 logs del contenedor"
  else
    UTID_RECENT=$(podman logs --since "$UNKNOWN_TOPIC_ID_STALE_WINDOW" "$CONTAINER_KAFKA" 2>/dev/null | grep -c "UNKNOWN_TOPIC_ID" || true)
    if [[ "$UTID_RECENT" -eq 0 ]]; then
      pass "UNKNOWN_TOPIC_ID: $UTID_TOTAL ocurrencia(s) históricas, ninguna en los últimos $UNKNOWN_TOPIC_ID_STALE_WINDOW — transitorio y resuelto (creación/reasignación de particiones)"
    else
      warn "UNKNOWN_TOPIC_ID: $UTID_RECENT ocurrencia(s) en los últimos $UNKNOWN_TOPIC_ID_STALE_WINDOW — aún activo, verificar si persiste"
    fi
  fi
fi

# 7.3 Grafana Alloy
info "7.3 Verificando Grafana Alloy (agente de observabilidad)..."
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
    warn "Grafana Alloy no encontrado — verificar despliegue del agente de observabilidad"
  fi
fi

# 7.4 node-exporter
info "7.4 Verificando node-exporter..."
if ss -tlnp 2>/dev/null | grep -q ":9100 "; then
  pass "node-exporter: puerto 9100 escuchando"
else
  warn "node-exporter: no detectado en puerto 9100 — verificar despliegue"
fi

# =============================================================================
# SECCIÓN 8 — VERIFICACIÓN DE VERSIÓN
# =============================================================================
header "8. VERSIÓN DE KAFKA Y JAVA"

if [[ "$SKIP_FUNCTIONAL" == false ]]; then
  info "8.1 Verificando versión de Kafka..."
  # Método preferido: los kafka-*.sh soportan --version nativamente (no requiere tocar el broker)
  KAFKA_VER=$(kexec "${KAFKA_HOME}/bin/kafka-topics.sh" --version 2>/dev/null | head -1 | awk '{print $1}')
  # Fallback: parsear el jar de kafka en libs/ (grep -E, no -P: la imagen trae BusyBox grep sin PCRE)
  if [[ -z "$KAFKA_VER" ]]; then
    KAFKA_VER=$(kexec bash -c "ls ${KAFKA_HOME}/libs 2>/dev/null | grep -E '^kafka_[0-9.]+-[0-9.]+\.jar' | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1")
  fi
  KAFKA_VER=${KAFKA_VER:-"no disponible"}
  if [[ "$KAFKA_VER" == "$KAFKA_VERSION_EXPECTED" ]]; then
    pass "Versión Kafka: $KAFKA_VER (esperada: $KAFKA_VERSION_EXPECTED)"
  elif [[ "$KAFKA_VER" != "no disponible" ]]; then
    warn "Versión Kafka: $KAFKA_VER — esperada: $KAFKA_VERSION_EXPECTED"
  else
    warn "No se pudo determinar la versión de Kafka instalada"
  fi

  info "8.2 Verificando versión de Java dentro del contenedor..."
  JVM_IN=$(kexec java -version 2>&1 | head -1 || echo "error")
  info "JVM en contenedor Kafka: $JVM_IN"
  JAVA_MAJOR=$(echo "$JVM_IN" | grep -oP '"\K[0-9]+' | head -1 || true)
  JAVA_MAJOR=${JAVA_MAJOR:-0}
  if [[ "$JAVA_MAJOR" -ge "$JAVA_VERSION_MIN" ]] 2>/dev/null; then
    if [[ "$JAVA_MAJOR" == "$JAVA_VERSION_EXPECTED" ]]; then
      pass "OpenJDK $JAVA_VERSION_EXPECTED confirmado dentro del contenedor (versión homologada)"
    else
      pass "JDK $JAVA_MAJOR compatible con Kafka 4.x (mínimo Java $JAVA_VERSION_MIN) — no es la versión homologada ($JAVA_VERSION_EXPECTED) pero es funcional"
    fi
  else
    fail "JDK dentro del contenedor por debajo del mínimo requerido por Kafka 4.x (Java $JAVA_VERSION_MIN+) — detectado: $JVM_IN"
  fi
else
  warn "8. Verificación de versión omitida — sin acceso funcional al contenedor"
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN"

TOTAL_CHECKS=$((PASS_COUNT + ERRORS + WARNINGS))

echo -e "  Cluster       : ${BOLD}${CLUSTER_NAME}${NC}"
echo -e "  Nodo          : ${BOLD}${HOSTNAME_ACTUAL}${NC}"
echo -e "  Nodos         : ${NODES[*]}"
echo -e "  Fecha         : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "  Total checks  : ${BOLD}${TOTAL_CHECKS}${NC}"
echo -e "  ${GREEN}${BOLD}PASS${NC}          : ${PASS_COUNT}"
echo -e "  ${RED}${BOLD}FAIL${NC}          : ${ERRORS}"
echo -e "  ${YELLOW}${BOLD}WARN${NC}          : ${WARNINGS}"
echo ""
echo -e "  ${BOLD}Nota de alcance:${NC} esta validación cubre únicamente el cluster Kafka."
echo -e "  La integración Kafka↔Flink se valida por separado (fuera de este alcance)."
echo ""

if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN COMPLETADA SIN ERRORES NI ADVERTENCIAS${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN COMPLETADA CON ${WARNINGS} ADVERTENCIA(S) — revisar antes de producción${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S)${NC}"
  echo -e "${RED}      Corregir errores antes de operar el cluster en producción.${NC}"
fi
echo ""

exit $ERRORS
