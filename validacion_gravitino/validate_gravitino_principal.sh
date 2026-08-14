#!/usr/bin/env bash
# =============================================================================
# BLUEPOINT – Cooperativa Jardín Azuayo · Big Data Platform
# Validación Gravitino (Iceberg REST Catalog) – CLUSTER PRINCIPAL (DC Principal)
#
# Alcance: Gravitino como tecnología en sí misma (servicio REST de catálogo de
# metadata) — NO valida su integración con MinIO/AIStor (creación real de
# tablas Iceberg sobre object storage), eso corresponde al script separado
# validacion_gravitino_minio/ (aún no existe en este repositorio a la fecha
# de este script).
#
# Topología (nodo compartido con Flink/Trino/Redis — ver
# validate_flink_principal.sh y validate_trino_principal.sh):
#   pbigd-plat-apps01  →  Gravitino (Iceberg REST Catalog)  ·  también Flink JM
#                          + Trino Coordinator + Redis Primary/Sentinel + Alloy
#
# El plan de implementación dice textualmente "1 instancia basta en mínimo;
# 2 recomendado" para Gravitino, pero la tabla de asignación de recursos solo
# lista UN nodo (platform-apps-1) con este rol — no hay un segundo nodo
# dedicado documentado. Este script detecta cuántas instancias hay realmente
# desplegadas (check 2.2) en lugar de asumir el mínimo o el recomendado.
#
# Runtime: Podman 5.x · Rocky Linux 10.x (Java corre dentro del contenedor,
# no se valida desde el host).
# Versión Gravitino: el plan de implementación original
# (Bluepoint_PlanImplementacion_v2._06042026.md) cita 1.1.0; la documentación
# de referencia vigente del proyecto cita 1.2.1 — inconsistencia documental,
# mismo patrón ya reportado para Trino (479 vs 481). Se reporta en el
# informe, no se resuelve unilateralmente aquí.
#
# DR: a diferencia de Kafka/Flink/MinIO, Gravitino NO figura en la tabla de
# asignación de recursos del datacenter alterno del plan de implementación.
# La fila platform-apps-dr solo lista "Redis Primary (promovible) + Redis
# Sentinel + API online DR + Alloy" — ni Gravitino, ni Trino, ni siquiera
# Flink JobManager (que va aparte en dr-cmp-1). Esto es una laguna más seria
# que el caso Trino: el Data Lake (MinIO+Iceberg) SÍ tiene presencia
# dimensionada en DR, pero su catálogo de metadata no aparece en ningún lado.
# Por eso NO se genera un validate_gravitino_dr.sh en esta entrega — se deja
# como PREGUNTA ABIERTA a esclarecer con el equipo de infraestructura (no
# como "fuera de alcance" automático). Antes de cerrar el tema, confirmar en
# sitio con:
#   podman ps --format '{{.Names}}\t{{.Image}}' | grep -i gravitino
# en pbigd-plat-apps01-cont (nodo DR candidato, ya usado por Flink/Redis/Alloy DR).
#
# Uso:
#   chmod +x validate_gravitino_principal.sh
#   ./validate_gravitino_principal.sh [--host <ip_o_hostname>]
#
# Por defecto asume que se ejecuta DESDE el nodo pbigd-plat-apps01.
# Pasa --host si lo ejecutas desde otra máquina.
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# CONFIGURACIÓN – ajusta las IPs/hostnames reales antes de ejecutar
# ---------------------------------------------------------------------------
GRAVITINO_HOST="${GRAVITINO_HOST:-pbigd-plat-apps01}"   # sobreescribible vía env o flag
GRAVITINO_PORT=8090          # puerto REST por defecto de Gravitino

GRAVITINO_HOME="/opt/gravitino"
GRAVITINO_LOGS="/var/log/gravitino"
GRAVITINO_VERSION_EXPECTED="1.2.1"   # el plan original cita 1.1.0 — ver inconsistencia reportada en el informe

CONTAINER_NAME_PATTERN="gravitino"   # prefijo/patrón del contenedor Podman
PODMAN_USER="admapl"                 # usuario rootless (ajustar si difiere)

NODE_EXPORTER_PORT=9100
NODE_EXPORTER_EMBEDDED_PORT=17935    # exporter unix embebido en Alloy (prometheus.exporter.unix)

TEST_METALAKE="bluepoint_validacion_test"
TEST_CATALOG="bluepoint_validacion_catalog"
TEST_SCHEMA="bluepoint_validacion_schema"

# ---------------------------------------------------------------------------
# Colores y helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0
LOG_FILE="/tmp/validate_gravitino_principal_$(date +%Y%m%d_%H%M%S).log"

log()  { echo -e "$*" | tee -a "$LOG_FILE"; }
# Verificación de puerto TCP sin depender de netcat (no siempre instalado en
# los nodos); usa el builtin /dev/tcp de bash.
tcp_port_open() {
  local host=$1 port=$2 timeout=${3:-3}
  timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" 2>/dev/null
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
    --host) GRAVITINO_HOST="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# ---------------------------------------------------------------------------
# Encabezado
# ---------------------------------------------------------------------------
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   BLUEPOINT · Validación Gravitino (Iceberg REST Catalog)       ║${NC}"
log "${BOLD}║   Fecha     : $(date '+%Y-%m-%d %H:%M:%S')                          ║${NC}"
log "${BOLD}║   Gravitino : $GRAVITINO_VERSION_EXPECTED                                       ║${NC}"
log "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
log ""

THIS_HOST=$(hostname -s 2>/dev/null || hostname)
GRAVITINO_API="http://${GRAVITINO_HOST}:${GRAVITINO_PORT}"

# ===========================================================================
# MÓDULO 1 – Infraestructura base (OS, Podman, NTP)
# ===========================================================================
section "MÓDULO 1 · OS y prerequisitos"

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

# Detección temprana del/de los contenedor(es) — se reutiliza en Módulos 1, 2 y 4
mapfile -t GRAVITINO_CONTAINERS < <(podman ps --filter "name=${CONTAINER_NAME_PATTERN}" --format "{{.Names}}" 2>/dev/null || true)
GRAVITINO_CONTAINER="${GRAVITINO_CONTAINERS[0]:-}"

# 1.5 Versión de Gravitino desplegada (mejor esfuerzo; confirmación definitiva en MÓDULO 3 vía /api/version)
if [[ -n "$GRAVITINO_CONTAINER" ]]; then
  GRAVITINO_VER_ACTUAL=$(podman exec "$GRAVITINO_CONTAINER" cat "${GRAVITINO_HOME}/version" 2>/dev/null \
    || podman exec "$GRAVITINO_CONTAINER" sh -c 'ls /opt/gravitino/libs 2>/dev/null | grep -oP "(?<=gravitino-common-)[0-9.]+(?=\.jar)"' 2>/dev/null \
    || echo "unknown")
  info "Versión Gravitino detectada en contenedor (mejor esfuerzo): $GRAVITINO_VER_ACTUAL"
  info "La versión definitiva se confirma vía REST API en MÓDULO 3 (/api/version)"
fi

# ===========================================================================
# MÓDULO 2 – Servicio y redundancia
# ===========================================================================
section "MÓDULO 2 · Servicio y redundancia"

# 2.1 Contenedor corriendo, usuario no-root y unidad systemd/Quadlet
if [[ -n "$GRAVITINO_CONTAINER" ]]; then
  GRV_STATUS=$(podman ps --filter "name=${GRAVITINO_CONTAINER}" --format "{{.Status}}" 2>/dev/null || echo "")
  if echo "$GRV_STATUS" | grep -qi "up"; then
    ok "Contenedor Gravitino corriendo: $GRV_STATUS"
  else
    fail "Contenedor Gravitino no está UP (estado: '${GRV_STATUS:-no encontrado}')"
  fi

  # Usuario real del proceso dentro del contenedor. `Config.User` solo
  # refleja lo declarado en la imagen (USER en el Dockerfile) y en Podman
  # rootless puede salir vacío aunque el contenedor NO corra con privilegios
  # reales — el "root" del namespace ya está mapeado a un UID sin
  # privilegios del host. Se verifica el usuario real vía `podman exec id`.
  GRV_UID_INFO=$(podman exec "$GRAVITINO_CONTAINER" id 2>/dev/null || echo "")
  GRV_UID=$(echo "$GRV_UID_INFO" | grep -oP '(?<=uid=)[0-9]+' || echo "")
  if [[ -n "$GRV_UID" && "$GRV_UID" != "0" ]]; then
    ok "Contenedor Gravitino corre como usuario no-root dentro del contenedor: $GRV_UID_INFO"
  elif [[ "$GRV_UID" == "0" ]]; then
    # UID 0 dentro del contenedor no es privilegio real si (a) el host es
    # rootless Y (b) el contenedor no corre --privileged ni con capabilities
    # peligrosas añadidas. Se verifican ambas condiciones en vez de dejarlo
    # como advertencia manual — solo se degrada a FAIL/WARN si de verdad hay
    # riesgo real, no por la sola presencia de UID 0 en el namespace.
    GRV_PRIVILEGED=$(podman inspect "$GRAVITINO_CONTAINER" --format '{{.HostConfig.Privileged}}' 2>/dev/null || echo "unknown")
    GRV_CAPADD=$(podman inspect "$GRAVITINO_CONTAINER" --format '{{.HostConfig.CapAdd}}' 2>/dev/null || echo "")
    if podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null | grep -qi true \
       && [[ "$GRV_PRIVILEGED" == "false" ]] \
       && [[ "$GRV_CAPADD" == "[]" || -z "$GRV_CAPADD" ]]; then
      ok "Contenedor Gravitino corre como UID 0 dentro del namespace, pero host rootless sin --privileged ni CapAdd — sin privilegio real (mapeado a usuario sin privilegios del host)"
    elif [[ "$GRV_PRIVILEGED" == "true" ]]; then
      fail "Contenedor Gravitino corre --privileged con UID 0 — privilegio real sobre el host, requiere corrección"
    elif [[ "$GRV_CAPADD" != "[]" && -n "$GRV_CAPADD" ]]; then
      warn "Contenedor Gravitino corre como UID 0 con capabilities añadidas ($GRV_CAPADD) — revisar si son necesarias, riesgo mayor que rootless simple"
    else
      warn "Contenedor Gravitino corre como UID 0 y no se pudo confirmar si el host es rootless — verificar manualmente con: podman info --format '{{.Host.Security.Rootless}}'"
    fi
  else
    fail "No se pudo determinar el usuario del proceso dentro del contenedor Gravitino"
  fi

  if systemctl is-active --quiet gravitino 2>/dev/null; then
    ok "Servicio activo: gravitino (systemd sistema)"
    systemctl is-enabled --quiet gravitino 2>/dev/null \
      && ok "  ↳ Habilitado en boot" \
      || warn "  ↳ NO habilitado en boot (faltaría: systemctl enable gravitino)"
  else
    # Evitar `sudo -u admapl systemctl --user ...`: cada invocación abre una
    # sesión PAM nueva y, con sesiones huérfanas acumuladas, choca contra el
    # límite de "too many logins" del usuario — dando un WARN falso que no
    # refleja el estado real del servicio. Si el script ya corre como
    # $PODMAN_USER, se consulta directo sin sudo ni sesión nueva.
    CURRENT_USER=$(id -un)
    if [[ "$CURRENT_USER" == "$PODMAN_USER" ]]; then
      if systemctl --user is-active --quiet gravitino 2>/dev/null; then
        ok "Servicio de usuario activo: gravitino (systemctl --user, sesión propia de $PODMAN_USER)"
      else
        warn "Servicio 'gravitino' no activo como sistema ni como usuario $PODMAN_USER (verificar nombre exacto de la unidad/Quadlet)"
      fi
    elif XDG_RUNTIME_DIR="/run/user/$(id -u "$PODMAN_USER" 2>/dev/null)" sudo -u "$PODMAN_USER" --preserve-env=XDG_RUNTIME_DIR systemctl --user is-active --quiet gravitino 2>/dev/null; then
      ok "Servicio de usuario activo: gravitino"
    else
      warn "Servicio 'gravitino' no activo como sistema ni como usuario $PODMAN_USER (verificar nombre exacto de la unidad/Quadlet, o si el chequeo remoto vía sudo -u falló por límite de sesiones PAM — re-ejecutar el script como $PODMAN_USER evita este falso negativo)"
    fi
  fi
else
  fail "No se encontró ningún contenedor Gravitino corriendo — módulo 2 no puede completarse"
fi

# 2.2 Conteo real de instancias desplegadas
N_INSTANCES=${#GRAVITINO_CONTAINERS[@]}
info "Instancias de Gravitino detectadas en este nodo: $N_INSTANCES (${GRAVITINO_CONTAINERS[*]:-ninguna})"
if [[ "$N_INSTANCES" -ge 1 ]]; then
  ok "Al menos 1 instancia desplegada (mínimo exigido por el plan)"
  if [[ "$N_INSTANCES" -ge 2 ]]; then
    info "2+ instancias detectadas — el plan recomienda esta cantidad para redundancia; ver check 2.3 para el mecanismo de HA real"
  else
    warn "Solo 1 instancia desplegada — el plan recomienda 2 para redundancia (no bloqueante, mínimo cumplido)"
  fi
else
  fail "No se detectó ninguna instancia de Gravitino desplegada"
fi

# 2.3 Si hay 2+ instancias: mecanismo real de HA delante (VIP/HAProxy/Keepalived/DNS RR)
# Precedente del proyecto: se encontró una IP virtual "de papel" sin failover
# real detrás en PostgreSQL, y se tuvo que agregar HAProxy+Keepalived después
# de la validación inicial de MinIO. No se asume que 2 instancias == HA real
# solo porque hay 2 procesos corriendo.
if [[ "$N_INSTANCES" -ge 2 ]]; then
  HA_EVIDENCE=""
  if command -v podman &>/dev/null && podman ps --format "{{.Names}}" 2>/dev/null | grep -qiE "haproxy|keepalived"; then
    HA_EVIDENCE="contenedor HAProxy/Keepalived detectado en este nodo"
  elif command -v systemctl &>/dev/null && (systemctl is-active --quiet haproxy 2>/dev/null || systemctl is-active --quiet keepalived 2>/dev/null); then
    HA_EVIDENCE="servicio systemd HAProxy/Keepalived activo"
  fi
  if [[ -n "$HA_EVIDENCE" ]]; then
    ok "Evidencia de mecanismo de HA delante de Gravitino: $HA_EVIDENCE"
  else
    warn "2+ instancias de Gravitino corriendo, pero NO se encontró evidencia de HAProxy/Keepalived/VIP delante — mismo patrón ya reportado para PostgreSQL/MinIO: 2 procesos activos no implican failover real. Confirmar manualmente si existe una VIP, balanceador o DNS round-robin, o si son instancias activas sin mecanismo de conmutación"
  fi
fi

# 2.4 Puerto REST escuchando
if ss -tlnp | grep -q ":${GRAVITINO_PORT}"; then
  ok "Puerto $GRAVITINO_PORT escuchando → Gravitino REST API"
else
  fail "Puerto $GRAVITINO_PORT NO escucha → Gravitino REST API"
fi

# ===========================================================================
# MÓDULO 3 – Validación funcional básica (sin MinIO)
# ===========================================================================
section "MÓDULO 3 · Validación funcional básica"

info "Consultando REST API: $GRAVITINO_API"

# 3.1 Health endpoint + versión vía API
VER_JSON=$(curl -sf --max-time 10 "$GRAVITINO_API/api/version" 2>/dev/null || echo "")
if [[ -n "$VER_JSON" ]]; then
  ok "REST API responde: $GRAVITINO_API/api/version"
  if command -v python3 &>/dev/null; then
    GRV_API_VER=$(echo "$VER_JSON" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(d.get('version',{}).get('version', d.get('version','?')))" 2>/dev/null || echo "?")
    # GRAVITINO_VERSION_EXPECTED es solo una referencia documental, no un
    # requisito de igualdad exacta — el plan no exige una versión mínima
    # bloqueante para Gravitino (a diferencia de Trino). Se detecta y reporta
    # la versión real, sin WARN por desvío frente a la referencia.
    if [[ "$GRV_API_VER" == "$GRAVITINO_VERSION_EXPECTED" ]]; then
      ok "Versión Gravitino API: $GRV_API_VER (coincide con la referencia documental)"
    else
      info "Versión Gravitino API: $GRV_API_VER (referencia documental: $GRAVITINO_VERSION_EXPECTED — solo informativo, no es un mínimo bloqueante)"
    fi
  fi
else
  fail "REST API no responde: $GRAVITINO_API/api/version (¿Gravitino corriendo en $GRAVITINO_HOST:$GRAVITINO_PORT?)"
fi

# 3.2 Listar metalakes existentes
METALAKES_JSON=$(curl -sf --max-time 10 "$GRAVITINO_API/api/metalakes" 2>/dev/null || echo "")
if [[ -n "$METALAKES_JSON" ]]; then
  ok "Endpoint /api/metalakes responde"
  if command -v python3 &>/dev/null; then
    ML_COUNT=$(echo "$METALAKES_JSON" | python3 -c \
      "import sys,json; d=json.load(sys.stdin); print(len(d.get('metalakes', [])))" 2>/dev/null || echo "?")
    info "  Metalakes existentes: $ML_COUNT"
  fi
else
  fail "Endpoint /api/metalakes no responde"
fi

# 3.3 Ciclo crear + eliminar metalake/catálogo/schema de prueba (gestión de
# metadata pura — NO valida que esto se refleje en MinIO, eso es
# validacion_gravitino_minio)
info "Ciclo de prueba: crear y eliminar metalake '$TEST_METALAKE' vía API REST"
# `-sf` sin más ocultaba el código HTTP y el cuerpo del error, dando WARN
# genéricos sin diagnóstico. Se captura el código HTTP explícito con -w.
#
# El DROP de un metalake da 409 MetalakeInUseException si no se pasa
# force=true — Gravitino marca todo metalake como "en uso" por defecto,
# incluso uno recién creado por este mismo script sin usar. No es un
# residuo de una corrida anterior: es el comportamiento normal de la API,
# así que el DELETE siempre debe forzarse. Confirmado en sitio: un DELETE
# sin force da 409 aunque el metalake se haya creado segundos antes.
create_metalake() {
  curl -s -o /tmp/gravitino_create_ml.json --max-time 10 -w "%{http_code}" -X POST "$GRAVITINO_API/api/metalakes" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$TEST_METALAKE\",\"comment\":\"Bluepoint validacion - eliminar si persiste\"}" 2>/dev/null
}
delete_metalake() {
  curl -s -o /tmp/gravitino_delete_ml.json --max-time 10 -w "%{http_code}" -X DELETE "$GRAVITINO_API/api/metalakes/${TEST_METALAKE}?force=true" 2>/dev/null
}

CREATE_HTTP_CODE=$(create_metalake)
if [[ "$CREATE_HTTP_CODE" == 2* ]]; then
  ok "Metalake de prueba creado: $TEST_METALAKE (HTTP $CREATE_HTTP_CODE)"
elif [[ "$CREATE_HTTP_CODE" == "409" ]]; then
  warn "Metalake de prueba '$TEST_METALAKE' ya existía (HTTP 409) — residuo de una corrida anterior sin limpiar; se elimina con force=true y se reintenta"
  delete_metalake >/dev/null
  CREATE_HTTP_CODE=$(create_metalake)
  if [[ "$CREATE_HTTP_CODE" == 2* ]]; then
    ok "Metalake de prueba creado tras limpiar residuo previo: $TEST_METALAKE (HTTP $CREATE_HTTP_CODE)"
  else
    fail "No se pudo crear el metalake de prueba tras limpiar el residuo (HTTP $CREATE_HTTP_CODE) — ver $GRAVITINO_API/api/metalakes/${TEST_METALAKE}"
  fi
else
  fail "No se pudo crear el metalake de prueba vía API (HTTP ${CREATE_HTTP_CODE:-sin respuesta}) — cuerpo en /tmp/gravitino_create_ml.json"
fi

if [[ "$CREATE_HTTP_CODE" == 2* ]]; then
  DELETE_HTTP_CODE=$(delete_metalake)
  if [[ "$DELETE_HTTP_CODE" == 2* ]]; then
    ok "Metalake de prueba eliminado correctamente: $TEST_METALAKE (HTTP $DELETE_HTTP_CODE)"
  else
    warn "No se pudo confirmar la eliminación del metalake de prueba '$TEST_METALAKE' (HTTP ${DELETE_HTTP_CODE:-sin respuesta}) — verificar manualmente y limpiar si persiste"
  fi
fi

# ===========================================================================
# MÓDULO 4 – Logs y observabilidad
# ===========================================================================
section "MÓDULO 4 · Logs y observabilidad"

# 4.1 Logs del contenedor sin errores críticos recientes
if [[ -n "$GRAVITINO_CONTAINER" ]]; then
  GRV_ERRORS=$(podman logs --tail 100 "$GRAVITINO_CONTAINER" 2>/dev/null | grep -ci "ERROR\|Exception\|FATAL" || true)
  if [[ "$GRV_ERRORS" -gt 0 ]]; then
    warn "Se detectaron $GRV_ERRORS líneas con ERROR/Exception/FATAL en logs de Gravitino"
  else
    ok "Sin errores críticos en los últimos 100 logs de Gravitino"
  fi
else
  warn "Sin contenedor Gravitino detectado — omitiendo verificación de logs"
fi

# 4.2 node-exporter / Alloy — mismo chequeo "smart" que
# validacion_observabilidad/validate_obs_agents.sh y validate_trino_principal.sh
# Módulo 4: revisa tanto el exporter embebido en Alloy (:17935,
# prometheus.exporter.unix) como el standalone (:9100). Un WARN aquí NO se
# reclasifica como exporter faltante si el componente embebido está healthy.
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
info "Contrastar contra el dimensionamiento del plan de implementación (platform-apps-1: 4 vCPU / 16 GB, compartido con Flink JM + Trino Coordinator + Redis)"

# ===========================================================================
# RESUMEN FINAL
# ===========================================================================
TOTAL=$((PASS + FAIL + WARN))
log ""
log "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
log "${BOLD}║   RESUMEN – Gravitino PRINCIPAL                                 ║${NC}"
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

exit $FAIL
