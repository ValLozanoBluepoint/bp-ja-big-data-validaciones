#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Trino 481 – CLUSTER PRINCIPAL (DC Principal)
#
# Alcance: Trino como tecnología en sí misma (coordinator + workers) — NO
# valida integración con MinIO ni con Gravitino, eso corresponde a los
# scripts separados validacion_trino_minio/ y validacion_trino_gravitino/
# (aún no existen en este repositorio a la fecha de este script).
#
# Topología (nodos compartidos con Flink — ver validate_flink_principal.sh):
#   pbigd-plat-apps01  →  Trino Coordinator  (puerto 8080) · también Flink JM + Redis
#   pbigd-proc01       →  Trino Worker       · también Flink TaskManager
#   pbigd-proc02       →  Trino Worker       · también Flink TaskManager
#   pbigd-proc03       →  Trino Worker       · también Flink TaskManager
#
# Runtime: Podman 5.x · JDK 25 (mínimo 25.0.1, requisito explícito de Trino
# 481 — más estricto que el resto del stack) · Rocky Linux 10.x
# Versión Trino: 481 (ver context/versiones-finales.md; el plan de
# implementación original cita 479 — inconsistencia documental, reportada
# en el informe, no resuelta unilateralmente aquí)
#
# DR: Trino NO figura en la tabla de asignación de recursos del datacenter
# alterno del plan de implementación (a diferencia de Kafka/Flink). Sus
# nodos designados en DR (pbigd-plat-apps01-cont, pbigd-proc01-cont/02-cont)
# existen y ya están validados para Flink/Redis/Alloy, pero nada confirma
# que Trino esté desplegado ahí, y no existe un tercer nodo proc-cont. Por
# eso NO se genera un validate_trino_dr.sh en esta entrega — se documenta
# como "No aplica en DR por diseño" (mismo criterio [OPT] usado para
# Loki/Tempo en observabilidad) en el informe. Antes de cerrar el tema
# definitivamente, confirmar en sitio con:
#   podman ps --format '{{.Names}}\t{{.Image}}' | grep -i trino
# en los 3 nodos -cont.
#
# Uso:
#   chmod +x validate_trino_principal.sh
#   ./validate_trino_principal.sh [--coordinator-host <ip_o_hostname>]
#
# Por defecto asume que se ejecuta DESDE el nodo Coordinator (pbigd-plat-apps01).
# Pasa --coordinator-host si lo ejecutas desde otra máquina.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN – ajusta las IPs/hostnames reales antes de ejecutar
# ---------------------------------------------------------------------------
COORDINATOR_HOST="${COORDINATOR_HOST:-pbigd-plat-apps01}"   # sobreescribible vía env o flag
COORDINATOR_PORT=8080
WORKER_HOSTS=("pbigd-proc01" "pbigd-proc02" "pbigd-proc03")
WORKER_PORT=8080          # cada worker expone su propio /v1/info en el mismo puerto

TRINO_HOME="/opt/trino"
TRINO_DATA="/data/trino"
TRINO_LOGS="/var/log/trino"
TRINO_VERSION_EXPECTED="481"

# JDK 25.0.1 es requisito MÍNIMO explícito de Trino 481 (más estricto que el
# resto del stack: Flink exige 17, Kafka soporta 17+). A diferencia de otros
# componentes, donde una versión de Java distinta a la esperada es WARN
# informativo, aquí una versión menor a JAVA_VERSION_MIN_PATCH es FAIL
# bloqueante — ver check 1.5.
JAVA_VERSION_EXPECTED="25"
JAVA_VERSION_MIN_PATCH="25.0.1"

CONTAINER_COORDINATOR="trino"               # nombre real del contenedor Podman del coordinator (confirmado en sitio: `docker.io/trinodb/trino:479`, nombre "trino", no "trino-coordinator")
CONTAINER_WORKER_PATTERN="trino"            # prefijo del contenedor worker; ajustar si el nodo worker usa un nombre distinto (confirmar con `podman ps` en pbigd-proc0X)
# Unidad systemd/Quadlet real: el Quadlet en sitio es
# /home/admapl/.config/containers/systemd/trino.container, que genera la
# unidad "trino.service" (Quadlet nombra la unidad igual al archivo .container,
# no "trino-coordinator.service" — confirmado en sitio).
SERVICE_COORDINATOR="trino"
SERVICE_WORKER="trino"                      # ajustar si el worker usa otro nombre de Quadlet (confirmar con find en pbigd-proc0X)
PODMAN_USER="admapl"                        # usuario rootless (ajustar si difiere)

NODE_EXPORTER_PORT=9100
NODE_EXPORTER_EMBEDDED_PORT=17935           # exporter unix embebido en Alloy (prometheus.exporter.unix)

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_trino_principal_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
# Verificación de puerto TCP sin depender de netcat (no siempre instalado en
# los nodos); usa el builtin /dev/tcp de bash.
tcp_port_open() {
  local host=$1 port=$2 timeout=${3:-3}
  timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null
}
# Ejecuta una consulta SQL vía la statement API pública de Trino
# (/v1/statement) y sigue la cadena de paginación 'nextUri' hasta obtener
# 'data'. NO usa /v1/node ni /v1/cluster: en Trino con comunicación interna
# endurecida (internal-communication.shared-secret), esos endpoints
# devuelven 404 a propósito sin el secreto interno — confirmado en sitio,
# no son consultables desde fuera aunque el cluster esté sano. system.runtime.*
# sí es SQL público, igual que el resto de queries que ya usa este script.
run_trino_query() {
  local sql="$1" uri="$COORDINATOR_API/v1/statement" method="POST" resp has_data next
  local max_iter=20
  for _ in $(seq 1 "$max_iter"); do
    if [[ "$method" == "POST" ]]; then
      resp=$(curl -sf --max-time 15 -X POST "$uri" -H "X-Trino-User: admapl" --data "$sql" 2>/dev/null || echo "")
    else
      resp=$(curl -sf --max-time 15 "$uri" -H "X-Trino-User: admapl" 2>/dev/null || echo "")
    fi
    [[ -z "$resp" ]] && return 1
    has_data=$(echo "$resp" | python3 -c "import sys,json; print('yes' if 'data' in json.load(sys.stdin) else 'no')" 2>/dev/null || echo "no")
    if [[ "$has_data" == "yes" ]]; then
      echo "$resp"
      return 0
    fi
    next=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('nextUri',''))" 2>/dev/null || echo "")
    [[ -z "$next" ]] && { echo "$resp"; return 1; }
    uri="$next"; method="GET"
  done
  return 1
}
ok()   { log "${GREEN}  [OK]${NC}  $*";  PASS=$((PASS+1)); }
fail() { log "${RED}  [FAIL]${NC} $*"; FAIL=$((FAIL+1)); }
warn() { log "${YELLOW}  [WARN]${NC} $*"; WARN=$((WARN+1)); }
info() { log "${CYAN}  [INFO]${NC} $*"; }
section() { log "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; \
            log "${BOLD}${CYAN}  $*${NC}"; \
            log "${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# Parseo de argumentos
while [[ $# -gt 0 ]]; do
  case "$1" in
    --coordinator-host) COORDINATOR_HOST="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Trino Cluster PRINCIPAL                ║${NC}"
log "${BOLD}║   Fecha : $(date '+%Y-%m-%d %H:%M:%S')                              ║${NC}"
log "${BOLD}║   Trino : $TRINO_VERSION_EXPECTED  ·  Java : JDK $JAVA_VERSION_EXPECTED (mín. $JAVA_VERSION_MIN_PATCH)          ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
COORDINATOR_API="http://${COORDINATOR_HOST}:${COORDINATOR_PORT}"
IS_COORDINATOR=false
[[ "$THIS_HOST" == *"plat-apps"* || "$THIS_HOST" == "$COORDINATOR_HOST" ]] && IS_COORDINATOR=true
IS_WORKER=false
for W in "${WORKER_HOSTS[@]}"; do
  [[ "$THIS_HOST" == *"$W"* || "$THIS_HOST" == "$W" ]] && IS_WORKER=true && break
done

# ===========================================================================
# MÓDULO 1 – Infraestructura base (OS, Java, Podman, NTP)
# ===========================================================================
section "MÓDULO 1 · OS, Java y prerequisitos"

# 1.1 Rocky Linux
OS=$(grep -oP '(?<=Rocky Linux )\S+' /etc/os-release 2>/dev/null || echo "unknown")
info "Sistema operativo detectado: Rocky Linux $OS"
if [[ "$OS" == 10* ]]; then
  ok "Rocky Linux 10.x confirmado"
else
  warn "Versión OS inesperada: $OS"
fi

# 1.2 Podman disponible
if command -v podman &>/dev/null; then
  PDM_VER=$(podman --version | awk '{print $3}')
  ok "Podman disponible: v$PDM_VER"
else
  fail "Podman no encontrado"
fi

# 1.3 Sincronización NTP
if command -v chronyc &>/dev/null; then
  if chronyc tracking &>/dev/null; then
    ok "NTP (chrony) sincronizado"
  else
    warn "chrony no confirmado"
  fi
elif timedatectl status 2>/dev/null | grep -q "synchronized: yes"; then
  ok "NTP sincronizado (timedatectl)"
else
  warn "No se pudo verificar sincronización NTP"
fi

# 1.4 Linger habilitado para admapl (persistencia rootless sin sesión activa)
LINGER=$(loginctl show-user "$PODMAN_USER" -p Linger 2>/dev/null | cut -d= -f2 || echo "unknown")
if [[ "$LINGER" == "yes" ]]; then
  ok "Linger habilitado para $PODMAN_USER"
else
  warn "Linger NO habilitado para $PODMAN_USER (estado: $LINGER) — los contenedores rootless pueden no sobrevivir a un logout"
fi

# 1.6 Versión de Trino desplegada — se confirma de forma definitiva y
# fiable vía REST API en MÓDULO 2 (/v1/info → nodeVersion). No se intenta
# adivinar aquí la ruta interna de la imagen (p.ej. /opt/trino/lib), ya que
# no coincide con el layout real de la imagen oficial (confirmado en sitio:
# el entrypoint corre desde /usr/lib/trino, no /opt/trino) y solo producía
# "unknown" sin aportar información.

# ===========================================================================
# MÓDULO 2 – Topología del clúster
# ===========================================================================
section "MÓDULO 2 · Topología del clúster"

# 2.1 Contenedor y unidad systemd/Quadlet (coordinator, nodo local)
if $IS_COORDINATOR; then
  info "Nodo identificado como Coordinator ($THIS_HOST)"

  CRD_STATUS=$(sudo -u "$PODMAN_USER" podman ps --filter "name=${CONTAINER_COORDINATOR}" \
              --format "{{.Status}}" 2>/dev/null || echo "")
  if ! echo "$CRD_STATUS" | grep -qi "up"; then
    CRD_STATUS=$(podman ps --filter "name=${CONTAINER_COORDINATOR}" --format "{{.Status}}" 2>/dev/null || echo "")
  fi
  if echo "$CRD_STATUS" | grep -qi "up"; then
    ok "Contenedor Coordinator corriendo: $CRD_STATUS"

    # 2.1b JDK ≥ 25.0.1 DENTRO del contenedor — requisito EXPLÍCITO y
    # BLOQUEANTE de Trino 481 (más estricto que el resto del stack: aquí una
    # versión menor es FAIL, no WARN). Se valida el JDK embebido en la
    # imagen del contenedor, no el del host (ver nota en check 1.5).
    CRD_JAVA_FULL=$(podman exec "$CONTAINER_COORDINATOR" java -version 2>&1 \
      | awk -F '"' '/version/ {print $2}' || echo "")
    if [[ -n "$CRD_JAVA_FULL" ]]; then
      CRD_JAVA_MAJOR=$(echo "$CRD_JAVA_FULL" | cut -d'.' -f1)
      info "Java dentro del contenedor Coordinator: $CRD_JAVA_FULL"
      LOWEST_VER=$(printf '%s\n%s\n' "$JAVA_VERSION_MIN_PATCH" "$CRD_JAVA_FULL" | sort -V | head -1)
      if [[ "$CRD_JAVA_MAJOR" -ge "$JAVA_VERSION_EXPECTED" ]] 2>/dev/null && \
         [[ "$LOWEST_VER" == "$JAVA_VERSION_MIN_PATCH" ]]; then
        ok "JDK del contenedor $CRD_JAVA_FULL cumple el mínimo requerido por Trino 481 ($JAVA_VERSION_MIN_PATCH)"
      else
        fail "JDK del contenedor insuficiente para Trino 481: detectado=$CRD_JAVA_FULL, mínimo requerido=$JAVA_VERSION_MIN_PATCH"
      fi
    else
      warn "No se pudo determinar la versión de Java dentro del contenedor Coordinator (podman exec falló)"
    fi
  else
    fail "Contenedor Coordinator no está UP (estado: '${CRD_STATUS:-no encontrado}')"
  fi

  # 2.2 Usuario del contenedor = admapl, no root
  CRD_USER=$(podman inspect "$CONTAINER_COORDINATOR" --format "{{.Config.User}}" 2>/dev/null || echo "")
  if [[ -n "$CRD_USER" && "$CRD_USER" != "0" && "$CRD_USER" != "root" ]]; then
    ok "Contenedor Coordinator corre como usuario no-root: '$CRD_USER'"
  else
    fail "Contenedor Coordinator corre como root o usuario no identificado ('${CRD_USER:-vacío}')"
  fi

  # 2.3 systemd/Quadlet — persistencia y boot
  # Nota: si el script ya corre como $PODMAN_USER, `systemctl --user` se
  # consulta DIRECTO, sin `sudo -u`. `sudo -u <user> systemctl --user` abre
  # una sesión de login nueva vía PAM, que puede fallar por límites de
  # sesiones concurrentes (maxlogins) sin relación alguna con el estado real
  # del servicio — confirmado en sitio ("There were too many logins for
  # 'admapl'"), causaba falsos WARN.
  CURRENT_USER=$(id -un)
  if systemctl is-active --quiet "$SERVICE_COORDINATOR" 2>/dev/null; then
    ok "Servicio activo: $SERVICE_COORDINATOR (systemd sistema)"
    systemctl is-enabled --quiet "$SERVICE_COORDINATOR" 2>/dev/null \
      && ok "  ↳ Habilitado en boot" \
      || warn "  ↳ NO habilitado en boot (faltaría: systemctl enable $SERVICE_COORDINATOR)"
  elif [[ "$CURRENT_USER" == "$PODMAN_USER" ]] && systemctl --user is-active --quiet "$SERVICE_COORDINATOR" 2>/dev/null; then
    ok "Servicio de usuario activo: $SERVICE_COORDINATOR"
  elif [[ "$CURRENT_USER" != "$PODMAN_USER" ]] && sudo -u "$PODMAN_USER" systemctl --user is-active --quiet "$SERVICE_COORDINATOR" 2>/dev/null; then
    ok "Servicio de usuario activo: $SERVICE_COORDINATOR"
  else
    warn "Servicio '$SERVICE_COORDINATOR' no activo como sistema ni usuario (verificar nombre exacto de la unidad/Quadlet)"
  fi
else
  info "Este nodo ($THIS_HOST) no es el Coordinator — omitiendo validaciones locales del coordinator"
  info "Ejecuta este script también en: $COORDINATOR_HOST"
fi

# 2.4 Contenedor y unidad systemd (worker, nodo local)
if $IS_WORKER; then
  info "Nodo identificado como Worker ($THIS_HOST)"

  CONTAINER_WORKER=$(podman ps --filter "name=${CONTAINER_WORKER_PATTERN}" --format "{{.Names}}" 2>/dev/null | head -1)
  if [[ -z "$CONTAINER_WORKER" ]]; then
    fail "No se encontró ningún contenedor con prefijo '${CONTAINER_WORKER_PATTERN}' (podman ps)"
  else
    info "Contenedor Worker detectado: $CONTAINER_WORKER"

    WRK_STATUS=$(podman ps --filter "name=${CONTAINER_WORKER}" --format "{{.Status}}" 2>/dev/null || echo "")
    if echo "$WRK_STATUS" | grep -qi "up"; then
      ok "Contenedor Worker corriendo: $WRK_STATUS"
    else
      fail "Contenedor Worker no está UP (estado: '${WRK_STATUS:-no encontrado}')"
    fi

    WRK_USER=$(podman inspect "$CONTAINER_WORKER" --format "{{.Config.User}}" 2>/dev/null || echo "")
    if [[ -n "$WRK_USER" && "$WRK_USER" != "0" && "$WRK_USER" != "root" ]]; then
      ok "Contenedor Worker corre como usuario no-root: '$WRK_USER'"
    else
      fail "Contenedor Worker corre como root o usuario no identificado ('${WRK_USER:-vacío}')"
    fi

    CURRENT_USER=$(id -un)
    if systemctl is-active --quiet "$SERVICE_WORKER" 2>/dev/null; then
      ok "Servicio activo: $SERVICE_WORKER (systemd sistema)"
      systemctl is-enabled --quiet "$SERVICE_WORKER" 2>/dev/null \
        && ok "  ↳ Habilitado en boot" \
        || warn "  ↳ NO habilitado en boot (faltaría: systemctl enable $SERVICE_WORKER)"
    elif [[ "$CURRENT_USER" == "$PODMAN_USER" ]] && systemctl --user is-active --quiet "$SERVICE_WORKER" 2>/dev/null; then
      ok "Servicio de usuario activo: $SERVICE_WORKER"
    elif [[ "$CURRENT_USER" != "$PODMAN_USER" ]] && sudo -u "$PODMAN_USER" systemctl --user is-active --quiet "$SERVICE_WORKER" 2>/dev/null; then
      ok "Servicio de usuario activo: $SERVICE_WORKER"
    else
      warn "Servicio '$SERVICE_WORKER' no activo como sistema ni usuario (verificar nombre exacto de la unidad/Quadlet)"
    fi
  fi
else
  info "Este nodo ($THIS_HOST) no es un worker conocido (${WORKER_HOSTS[*]})"
fi

# 2.5 Puerto del coordinator
if $IS_COORDINATOR; then
  if ss -tlnp | grep -q ":${COORDINATOR_PORT}"; then
    ok "Puerto $COORDINATOR_PORT escuchando → Trino Coordinator HTTP"
  else
    fail "Puerto $COORDINATOR_PORT NO escucha → Trino Coordinator HTTP"
  fi
fi

# 2.6 REST API del coordinator: /v1/info
info "Consultando REST API: $COORDINATOR_API"
INFO_JSON=$(curl -sf --max-time 10 "$COORDINATOR_API/v1/info" 2>/dev/null || echo "")
if [[ -n "$INFO_JSON" ]]; then
  ok "REST API responde: $COORDINATOR_API/v1/info"
  if command -v python3 &>/dev/null; then
    TRINO_API_VER=$(echo "$INFO_JSON" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('nodeVersion',{}).get('version','?'))" 2>/dev/null || echo "?")
    STARTED=$(echo "$INFO_JSON" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('starting', '?'))" 2>/dev/null || echo "?")
    info "  Trino versión (API): $TRINO_API_VER"
    info "  En arranque (starting): $STARTED"
    if [[ "$TRINO_API_VER" == "$TRINO_VERSION_EXPECTED" ]]; then
      ok "Versión Trino API coincide: $TRINO_API_VER"
    else
      info "Versión Trino desplegada: $TRINO_API_VER"
    fi
  fi
else
  fail "REST API no responde: $COORDINATOR_API/v1/info (¿Coordinator corriendo en $COORDINATOR_HOST?)"
fi

# 2.7 Workers registrados y visibles DESDE el coordinator (no solo "up"
# localmente). Se consulta system.runtime.nodes vía SQL, NO /v1/node ni
# /v1/cluster: esos endpoints internos devuelven 404 a propósito sin el
# secreto de comunicación interna de Trino, sin importar el estado real del
# cluster (ver run_trino_query arriba).
if command -v python3 &>/dev/null; then
  NODES_RESULT=$(run_trino_query "SELECT node_id, state, coordinator FROM system.runtime.nodes")
  if [[ -n "$NODES_RESULT" ]]; then
    ok "Consulta a system.runtime.nodes respondió correctamente"
    NODE_COUNT=$(echo "$NODES_RESULT" | python3 -c \
      "import sys,json; print(len(json.load(sys.stdin).get('data') or []))" 2>/dev/null || echo "0")
    info "Nodos totales reportados por el coordinator (incluye coordinator): $NODE_COUNT"
    # Se espera coordinator + 3 workers = 4 nodos registrados
    if [[ "$NODE_COUNT" -ge 4 ]] 2>/dev/null; then
      ok "Nodos registrados en el cluster: $NODE_COUNT (≥4 esperado: 1 coordinator + 3 workers)"
    else
      fail "Nodos insuficientes registrados en el cluster: $NODE_COUNT (se esperan 4: 1 coordinator + 3 workers) — un worker 'up' localmente pero no reconocido aquí es hallazgo real, no falso positivo"
    fi
  else
    warn "No se pudo consultar system.runtime.nodes vía /v1/statement — no se pudo confirmar el registro de workers desde el coordinator"
  fi
else
  warn "python3 no disponible — no se pudo confirmar el registro de workers desde el coordinator"
fi

# 2.8 Conectividad TCP coordinator ↔ cada worker
ALL_NODES=("$COORDINATOR_HOST" "${WORKER_HOSTS[@]}")
for NODE in "${ALL_NODES[@]}"; do
  [[ "$NODE" == "$THIS_HOST" ]] && continue
  if ping -c 1 -W 2 "$NODE" &>/dev/null; then
    ok "Nodo $NODE alcanzable (ping OK)"
    if tcp_port_open "$NODE" "$COORDINATOR_PORT"; then
      ok "  ↳ Puerto $COORDINATOR_PORT accesible en $NODE"
    else
      warn "  ↳ Puerto $COORDINATOR_PORT no accesible en $NODE"
    fi
  else
    warn "Nodo $NODE NO responde a ping (verificar red o que el host esté levantado)"
  fi
done

# ===========================================================================
# MÓDULO 3 – Validación funcional básica (sin catálogos externos)
# ===========================================================================
section "MÓDULO 3 · Validación funcional básica"

# 3.1 Query trivial de sistema — ejecución distribuida
STATEMENT_RESULT=$(curl -sf --max-time 15 \
  -X POST "$COORDINATOR_API/v1/statement" \
  -H "X-Trino-User: admapl" \
  --data "SELECT 1" 2>/dev/null || echo "")
if [[ -n "$STATEMENT_RESULT" ]] && command -v python3 &>/dev/null; then
  Q_ID=$(echo "$STATEMENT_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id','?'))" 2>/dev/null || echo "?")
  if [[ "$Q_ID" != "?" ]]; then
    ok "Query trivial 'SELECT 1' aceptada por el coordinator (queryId=$Q_ID)"
  else
    warn "Endpoint /v1/statement respondió pero sin queryId identificable"
  fi
else
  warn "No se pudo confirmar ejecución de 'SELECT 1' vía API — validar manualmente con trino-cli: trino --server $COORDINATOR_API --execute 'SELECT 1'"
fi

# 3.2 SHOW CATALOGS — solo confirma respuesta, no valida contenido
CATALOGS_RESULT=$(curl -sf --max-time 15 \
  -X POST "$COORDINATOR_API/v1/statement" \
  -H "X-Trino-User: admapl" \
  --data "SHOW CATALOGS" 2>/dev/null || echo "")
if [[ -n "$CATALOGS_RESULT" ]]; then
  ok "'SHOW CATALOGS' aceptado por el coordinator (contenido de catálogos NO validado aquí — ver validacion_trino_minio/validacion_trino_gravitino)"
else
  warn "No se pudo confirmar respuesta a 'SHOW CATALOGS' vía API"
fi

# ===========================================================================
# MÓDULO 4 – Logs y observabilidad
# ===========================================================================
section "MÓDULO 4 · Logs y observabilidad"

# 4.1 Logs del contenedor sin errores críticos recientes
if $IS_COORDINATOR; then
  CRD_ERRORS=$(podman logs --tail 100 "$CONTAINER_COORDINATOR" 2>/dev/null | grep -ci "ERROR\|Exception\|FATAL" || true)
  if [[ "$CRD_ERRORS" -gt 0 ]]; then
    warn "Se detectaron $CRD_ERRORS líneas con ERROR/Exception/FATAL en logs del Coordinator"
  else
    ok "Sin errores críticos en los últimos 100 logs del Coordinator"
  fi
fi

if $IS_WORKER; then
  if [[ -z "${CONTAINER_WORKER:-}" ]]; then
    warn "Sin contenedor Worker detectado — omitiendo verificación de logs"
  else
    WRK_ERRORS=$(podman logs --tail 100 "$CONTAINER_WORKER" 2>/dev/null | grep -ci "ERROR\|Exception\|FATAL" || true)
    if [[ "$WRK_ERRORS" -gt 0 ]]; then
      warn "Se detectaron $WRK_ERRORS líneas con ERROR/Exception/FATAL en logs del Worker"
    else
      ok "Sin errores críticos en los últimos 100 logs del Worker"
    fi
  fi
fi

# 4.2 node-exporter / Alloy — mismo chequeo "smart" que
# validacion_observabilidad/validate_obs_agents.sh Módulo 2: revisa tanto el
# exporter embebido en Alloy (:17935, prometheus.exporter.unix) como el
# standalone (:9100). Un WARN aquí NO se reclasifica como exporter faltante
# si el componente embebido está healthy — es la misma decisión de
# arquitectura ya documentada para el resto del proyecto.
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
elif ss -tlnp | grep -q ":${NODE_EXPORTER_PORT}"; then
  NE_PORT="$NODE_EXPORTER_PORT"
  NE_MODE="standalone"
  ok "Puerto $NODE_EXPORTER_PORT escuchando (node-exporter standalone activo)"
else
  fail "node-exporter NO está corriendo (ni embebido :$NODE_EXPORTER_EMBEDDED_PORT ni standalone :$NODE_EXPORTER_PORT)"
fi

if [[ "$NE_MODE" == "embebido en Alloy" ]]; then
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
  else
    warn "No se pudo consultar la API de componentes de Alloy en :$NE_PORT"
  fi
fi

# ===========================================================================
# MÓDULO 5 – Recursos (informativo, no bloqueante)
# ===========================================================================
section "MÓDULO 5 · Recursos"

CPU_COUNT=$(nproc 2>/dev/null || echo "?")
RAM_TOTAL=$(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo "?")
info "CPU disponibles: $CPU_COUNT"
info "RAM total: $RAM_TOTAL"
info "Contrastar contra el dimensionamiento del plan de implementación (platform-apps-1 / cmp-1..3)"

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Trino Cluster PRINCIPAL                             ║${NC}"
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
  log "${GREEN}${BOLD}  ✔  Validación completada SIN fallos críticos${NC}"
else
  log "${RED}${BOLD}  ✘  Se detectaron $FAIL fallos críticos — revisar log: $LOG_FILE${NC}"
fi

log ""
log "${CYAN}  ▶ Ejecutar también en los nodos worker restantes:${NC}"
for NODE in "${ALL_NODES[@]}"; do
  [[ "$NODE" != "$THIS_HOST" ]] && log "     $NODE"
done
log ""

exit $FAIL
