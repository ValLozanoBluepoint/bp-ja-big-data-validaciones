#!/usr/bin/env bash
# =============================================================================
#  Bluepoint — Cooperativa Jardín Azuayo | Proyecto Big Data
#  SCRIPT DE VALIDACIÓN: HAProxy + Keepalived delante del clúster MinIO
#  Entorno: DC PRINCIPAL
#  Nodos   : pbigd-stg01 | pbigd-stg02 | pbigd-stg03  (co-ubicados con MinIO)
#
#  ADVERTENCIA — LEER ANTES DE USAR:
#  Al momento de escribir este script NO se encontró, dentro de este
#  repositorio de validaciones, evidencia de que esta capa (HAProxy +
#  Keepalived delante de MinIO) ya esté implementada. En context/Contexto-
#  extra-correos.md (2026-08-01, punto 1) aparece únicamente como una
#  RECOMENDACIÓN de Bluepoint a la Cooperativa, sin script de implementación
#  asociado (a diferencia del caso PgBouncer/Postgres, que sí tiene
#  01_configuracion_keepalived_vip_pgbouncer.sh). Además,
#  validacion_flink_minio/validate_flink_minio_principal.sh apunta hoy
#  directamente a "http://pbigd-stg01:9000" — un solo nodo, sin VIP — lo
#  cual es evidencia adicional de que, si esta capa existe, al menos Flink
#  todavía no la usa.
#
#  Por eso este script NO asume nombres de contenedor, interfaz de red, VIP
#  ni nombre DNS fijos: los autodescubre en tiempo de ejecución (mismo
#  patrón que validacion_minio/validate_minio_principal.sh con
#  CONTAINER_PATTERN="minio"). Si algo no se encuentra, se reporta como
#  FAIL/WARN "no implementado / no encontrado", no se inventa.
#
#  Uso:
#    chmod +x validate_haproxy_minio_principal.sh
#    ./validate_haproxy_minio_principal.sh [opciones]
#
#  Opciones:
#    --vip <IP>          IP virtual esperada (si no se pasa, se intenta autodetectar)
#    --dns <nombre>       Nombre DNS interno esperado para la VIP
#    --iface <iface>      Interfaz de red donde debería aparecer la VIP (autodetecta si se omite)
#    --mc-alias <alias>   Alias de mc apuntando a la VIP (default: minio-vip)
#    --skip-big-object     Omitir la prueba funcional de objeto grande (Módulo 3.2)
# =============================================================================

set -uo pipefail

# ── Colores y helpers (mismo estilo que el resto de scripts del proyecto) ──────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS_TAG="[${GREEN}PASS${NC}]"
FAIL_TAG="[${RED}FAIL${NC}]"
WARN_TAG="[${YELLOW}WARN${NC}]"
INFO_TAG="[${CYAN}INFO${NC}]"
MANUAL_TAG="[${YELLOW}${BOLD}MANUAL${NC}]"

CHECKS=0; ERRORS=0; WARNINGS=0; MANUALS=0

pass()   { CHECKS=$((CHECKS+1)); echo -e "${PASS_TAG} $*"; }
fail()   { CHECKS=$((CHECKS+1)); echo -e "${FAIL_TAG} $*"; ERRORS=$((ERRORS+1)); }
warn()   { CHECKS=$((CHECKS+1)); echo -e "${WARN_TAG} $*"; WARNINGS=$((WARNINGS+1)); }
# pending: check fuera del alcance del script por diseño (sudo/vCenter), no un
# hallazgo — se cuenta aparte de WARNINGS para no mezclar "no pude verificar
# esto por permisos/alcance" con "verifiqué esto y hay un problema real".
pending(){ CHECKS=$((CHECKS+1)); echo -e "${MANUAL_TAG} $*"; MANUALS=$((MANUALS+1)); }
info()   { echo -e "${INFO_TAG} $*"; }
manual() { echo -e "${YELLOW}${BOLD}  [MANUAL]${NC} $*"; }
header(){ echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════════${NC}"; \
          echo -e "${BOLD}${CYAN}  $*${NC}"; \
          echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════════${NC}"; }

# ── Parámetros ─────────────────────────────────────────────────────────────────
ENV_LABEL="Principal"
NODES=("pbigd-stg01" "pbigd-stg02" "pbigd-stg03")
S3_PORT=9000
CONSOLE_PORT=9001
HAPROXY_CONTAINER_PATTERN="haproxy"
HAPROXY_CFG_CANDIDATES=(
  "/usr/local/etc/haproxy/haproxy.cfg"
  "/etc/haproxy/haproxy.cfg"
  "/etc/haproxy/haproxy.cfg.d/minio.cfg"
)
KEEPALIVED_CONF="/etc/keepalived/keepalived.conf"
VIP=""
VIP_DNS=""
VIP_IFACE=""
MC_ALIAS="minio-vip"
SKIP_BIG_OBJECT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --vip)            VIP="$2"; shift 2 ;;
    --dns)            VIP_DNS="$2"; shift 2 ;;
    --iface)          VIP_IFACE="$2"; shift 2 ;;
    --mc-alias)       MC_ALIAS="$2"; shift 2 ;;
    --skip-big-object) SKIP_BIG_OBJECT=true; shift ;;
    *) echo "Opción desconocida: $1"; exit 1 ;;
  esac
done

HOSTNAME_ACTUAL=$(hostname -s 2>/dev/null || hostname)
NODE_ROLE="desconocido"
for n in "${NODES[@]}"; do
  if [[ "$HOSTNAME_ACTUAL" == "$n"* ]]; then NODE_ROLE="$n"; break; fi
done

# ── Banner ─────────────────────────────────────────────────────────────────────
echo -e "${BOLD}"
echo "  HAProxy + Keepalived · Capa de acceso único delante de MinIO"
echo -e "${NC}"
echo -e "${BOLD}  Cooperativa Jardín Azuayo — Proyecto Big Data${NC}"
echo -e "  Entorno: ${BOLD}${CYAN}${ENV_LABEL}${NC}"
echo -e "  Nodo actual: ${BOLD}${HOSTNAME_ACTUAL}${NC}  (rol detectado: ${NODE_ROLE})"
echo -e "  Fecha/Hora: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""

# ── Helpers de autodescubrimiento ───────────────────────────────────────────────
haproxy_container_name() {
  podman ps --format "{{.Names}}" 2>/dev/null | grep -i "$HAPROXY_CONTAINER_PATTERN" | head -1
}

fetch_haproxy_cfg() {
  local c="$1"
  local path
  for path in "${HAPROXY_CFG_CANDIDATES[@]}"; do
    local out
    out=$(podman exec "$c" cat "$path" 2>/dev/null || true)
    if [[ -n "$out" ]]; then
      echo "$out"
      HAPROXY_CFG_PATH_FOUND="$path"
      return 0
    fi
  done
  return 1
}

primary_ipv4() {
  hostname -I 2>/dev/null | awk '{print $1}'
}

autodetect_iface() {
  ip route get 8.8.8.8 2>/dev/null | awk '/dev/{for(i=1;i<=NF;i++) if ($i=="dev") print $(i+1)}' | head -1
}

autodetect_vip() {
  local iface="$1"
  local main_ip
  main_ip=$(primary_ipv4)
  ip -4 -o addr show dev "$iface" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | grep -v -F "$main_ip" | head -1
}

# =============================================================================
# MÓDULO 1 — DESPLIEGUE Y TOPOLOGÍA
# =============================================================================
header "MÓDULO 1 · Despliegue y topología"

# 1.1 Contenedor HAProxy corriendo [admapl]
info "1.1 Buscando contenedor HAProxy (podman ps)..."
HAPROXY_CTR=$(haproxy_container_name)
if [[ -n "$HAPROXY_CTR" ]]; then
  HAPROXY_STATUS=$(podman ps --format "{{.Names}}\t{{.Status}}\t{{.Image}}" 2>/dev/null | grep -i "$HAPROXY_CONTAINER_PATTERN" | head -1)
  if echo "$HAPROXY_STATUS" | grep -qi "up"; then
    pass "Contenedor HAProxy encontrado y corriendo: $HAPROXY_STATUS"
  else
    fail "Contenedor HAProxy encontrado ('$HAPROXY_CTR') pero no está 'Up': $HAPROXY_STATUS"
  fi
else
  fail "No se encontró ningún contenedor con nombre/patrón '${HAPROXY_CONTAINER_PATTERN}' en 'podman ps' — la capa HAProxy no parece estar implementada en este nodo (o corre bajo otro usuario/namespace)"
fi

# 1.2 Usuario dentro del contenedor = no-root [admapl-contenedor]
if [[ -n "$HAPROXY_CTR" ]]; then
  info "1.2 Verificando usuario del proceso dentro del contenedor..."
  CTR_USER=$(podman exec "$HAPROXY_CTR" whoami 2>/dev/null || echo "")
  if [[ -z "$CTR_USER" ]]; then
    CTR_USER=$(podman inspect --format '{{.Config.User}}' "$HAPROXY_CTR" 2>/dev/null || echo "")
  fi
  if [[ -n "$CTR_USER" && "$CTR_USER" != "root" && "$CTR_USER" != "0" ]]; then
    pass "Usuario dentro del contenedor HAProxy: $CTR_USER (no-root, correcto — decisión del correo 2026-08-06)"
  elif [[ "$CTR_USER" == "root" || "$CTR_USER" == "0" ]]; then
    fail "Contenedor HAProxy corre como root — debería correr como usuario sin privilegios (admapl), ya que ninguno de los puertos usados (9000/9001) es privilegiado"
  else
    warn "No se pudo determinar el usuario dentro del contenedor HAProxy — verificar manualmente con 'podman exec $HAPROXY_CTR whoami'"
  fi
else
  warn "1.2 omitido — no hay contenedor HAProxy para inspeccionar"
fi

# 1.3 Unidad systemd/Quadlet de HAProxy activa y habilitada [admapl]
info "1.3 Buscando unidad systemd/Quadlet de HAProxy (usuario y sistema)..."
HAPROXY_UNIT_USER=$(systemctl --user list-units --type=service --all 2>/dev/null | grep -i "haproxy" | awk '{print $1}' | head -1)
HAPROXY_UNIT_SYS=$(systemctl list-units --type=service --all 2>/dev/null | grep -i "haproxy" | awk '{print $1}' | head -1)
if [[ -n "$HAPROXY_UNIT_USER" ]]; then
  STATE=$(systemctl --user is-active "$HAPROXY_UNIT_USER" 2>/dev/null || echo "unknown")
  ENABLED=$(systemctl --user is-enabled "$HAPROXY_UNIT_USER" 2>/dev/null || echo "unknown")
  [[ "$STATE" == "active" ]] && pass "Unidad systemd --user: $HAPROXY_UNIT_USER — activa — habilitada: $ENABLED" \
                              || fail "Unidad systemd --user: $HAPROXY_UNIT_USER — estado: $STATE — habilitada: $ENABLED"
elif [[ -n "$HAPROXY_UNIT_SYS" ]]; then
  STATE=$(systemctl is-active "$HAPROXY_UNIT_SYS" 2>/dev/null || echo "unknown")
  ENABLED=$(systemctl is-enabled "$HAPROXY_UNIT_SYS" 2>/dev/null || echo "unknown")
  [[ "$STATE" == "active" ]] && pass "Unidad systemd (sistema): $HAPROXY_UNIT_SYS — activa — habilitada: $ENABLED" \
                              || fail "Unidad systemd (sistema): $HAPROXY_UNIT_SYS — estado: $STATE — habilitada: $ENABLED"
else
  warn "No se encontró unidad systemd/Quadlet con nombre 'haproxy' (ni --user ni sistema) — si el contenedor está corriendo sin unidad systemd, no sobrevivirá un reinicio del nodo"
fi

# 1.4 Proceso keepalived corriendo y bajo qué usuario [admapl]
info "1.4 Verificando proceso keepalived..."
KEEPALIVED_PS=$(ps -eo user,comm 2>/dev/null | grep -w "keepalived" || true)
if [[ -n "$KEEPALIVED_PS" ]]; then
  KA_USER=$(echo "$KEEPALIVED_PS" | head -1 | awk '{print $1}')
  if [[ "$KA_USER" == "root" ]]; then
    pass "keepalived corriendo — usuario: $KA_USER (correcto, requiere privilegios de kernel para VRRP/VIP/ARP gratuito)"
  else
    warn "keepalived corriendo bajo usuario '$KA_USER' (esperado: root) — revisar, salvo que se use la variante con capabilities documentada oficialmente"
  fi
else
  fail "No se encontró ningún proceso 'keepalived' corriendo en este nodo"
fi

# 1.5 Unidad systemd de Keepalived (host) activa y habilitada [admapl]
info "1.5 Verificando unidad systemd de keepalived (host)..."
# 'systemctl list-unit-files' depende de una conexión a D-Bus que puede fallar
# transitoriamente (timeout, systemd ocupado). Con stderr descartado, un fallo
# transitorio se ve idéntico a "la unidad no existe" — dando PASS/FAIL
# alternando entre corridas sin que nada cambie realmente en el nodo. Por eso
# se agrega un respaldo determinístico: si systemctl no la ve, se busca el
# archivo de unidad directamente en el filesystem antes de declarar ausencia.
KA_UNIT_FOUND=false
if systemctl list-unit-files 2>/dev/null | grep -qi "^keepalived.service"; then
  KA_UNIT_FOUND=true
elif find /usr/lib/systemd/system /etc/systemd/system /run/systemd/system -maxdepth 1 -iname "keepalived.service" 2>/dev/null | grep -q .; then
  KA_UNIT_FOUND=true
  warn "'systemctl list-unit-files' no devolvió 'keepalived.service', pero el archivo de unidad SÍ existe en disco — probable timeout/transiente de D-Bus, no ausencia real. Si este WARN se repite, revisar el estado del propio systemd en el nodo"
fi

if [[ "$KA_UNIT_FOUND" == true ]]; then
  KA_STATE=$(systemctl is-active keepalived 2>/dev/null || echo "unknown")
  KA_ENABLED=$(systemctl is-enabled keepalived 2>/dev/null || echo "unknown")
  [[ "$KA_STATE" == "active" ]] && pass "systemd keepalived.service — activa — habilitada: $KA_ENABLED" \
                                 || fail "systemd keepalived.service — estado: $KA_STATE — habilitada: $KA_ENABLED"
else
  fail "No existe unidad systemd 'keepalived.service' en este nodo (ni vía systemctl ni en el filesystem)"
fi

# 1.6 Cruce: keepalived NO containerizado, HAProxy NO fuera de contenedor
info "1.6 Cruzando 1.1–1.5 para detectar inconsistencias de despliegue..."
KEEPALIVED_CTR=$(podman ps --format "{{.Names}}" 2>/dev/null | grep -i "keepalived" || true)
if [[ -n "$KEEPALIVED_CTR" ]]; then
  fail "keepalived aparece corriendo también como contenedor Podman ('$KEEPALIVED_CTR') — según diseño confirmado (correo 2026-08-06) debe correr nativo a nivel de host, no en contenedor"
else
  pass "keepalived no aparece containerizado en este nodo (correcto)"
fi
# El host ve también los procesos de contenedores rootless (namespaces anidados
# son visibles desde el ancestro), así que un simple 'ps -eo comm' cuenta el
# propio HAProxy del contenedor como si fuera un proceso duplicado en el host.
# Para evitar el falso positivo, se descartan los PIDs que descienden del PID
# maestro del contenedor (podman inspect .State.Pid) antes de decidir.
CTR_INIT_PID=""
if [[ -n "$HAPROXY_CTR" ]]; then
  CTR_INIT_PID=$(podman inspect -f '{{.State.Pid}}' "$HAPROXY_CTR" 2>/dev/null || true)
fi
is_descendant_of() {
  local pid="$1" ancestor="$2" ppid
  while [[ -n "$pid" && "$pid" != "0" && "$pid" != "1" ]]; do
    [[ "$pid" == "$ancestor" ]] && return 0
    ppid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d '[:space:]')
    pid="$ppid"
  done
  return 1
}
HAPROXY_HOST_ONLY_PIDS=""
while read -r pid comm; do
  [[ "$comm" == "haproxy" ]] || continue
  if [[ -n "$CTR_INIT_PID" ]] && is_descendant_of "$pid" "$CTR_INIT_PID"; then
    continue
  fi
  HAPROXY_HOST_ONLY_PIDS+="$pid "
done < <(ps -eo pid=,comm= 2>/dev/null)
if [[ -n "$HAPROXY_HOST_ONLY_PIDS" && -z "$HAPROXY_CTR" ]]; then
  fail "Se encontró un proceso 'haproxy' corriendo fuera de contenedor en el host (PID $HAPROXY_HOST_ONLY_PIDS) — según diseño confirmado debe correr containerizado bajo Podman/admapl"
elif [[ -n "$HAPROXY_HOST_ONLY_PIDS" && -n "$HAPROXY_CTR" ]]; then
  warn "Se encontró un proceso 'haproxy' en el host (PID $HAPROXY_HOST_ONLY_PIDS) que NO desciende del contenedor '$HAPROXY_CTR' — confirmar que no es un HAProxy duplicado fuera de Podman"
else
  pass "No se detectó HAProxy corriendo fuera de contenedor en este nodo (correcto)"
fi
info "Repetir este módulo en los otros 2 nodos (${NODES[*]}) y comparar manualmente — no hay SSH sin contraseña disponible entre nodos en este entorno (mismo patrón que Kafka↔Flink)"

# 1.7 Recursos libres del nodo (INFO)
info "1.7 Recursos del nodo tras la carga adicional de HAProxy+Keepalived (informativo, no afecta PASS/FAIL)..."
info "  $(free -h | awk 'NR==2{print "RAM libre: "$7" / total: "$2}')"
info "  vCPUs: $(nproc)"
info "  Carga (load average): $(uptime | awk -F'load average:' '{print $2}')"

# =============================================================================
# MÓDULO 2 — RUTEO Y SEPARACIÓN DE TRÁFICO
# =============================================================================
header "MÓDULO 2 · Ruteo y separación de tráfico"

HAPROXY_CFG=""
HAPROXY_CFG_PATH_FOUND=""
if [[ -n "$HAPROXY_CTR" ]]; then
  info "Leyendo configuración de HAProxy dentro del contenedor..."
  if HAPROXY_CFG=$(fetch_haproxy_cfg "$HAPROXY_CTR"); then
    pass "Config de HAProxy leída desde: $HAPROXY_CFG_PATH_FOUND"
  else
    fail "No se pudo leer la config de HAProxy en ninguna ruta candidata (${HAPROXY_CFG_CANDIDATES[*]}) — ajustar HAPROXY_CFG_CANDIDATES en el script con la ruta real"
  fi
else
  warn "2.x omitido — no hay contenedor HAProxy del cual leer configuración"
fi

if [[ -n "$HAPROXY_CFG" ]]; then
  # 2.1 Dos frontends separados: 9000 (S3) y 9001 (Consola)
  info "2.1 Verificando frontends separados para API S3 (9000) y Consola (9001)..."
  FRONTEND_S3=$(echo "$HAPROXY_CFG" | grep -A5 -i "frontend" | grep -c "bind.*:${S3_PORT}\b")
  FRONTEND_CONSOLE=$(echo "$HAPROXY_CFG" | grep -A5 -i "frontend" | grep -c "bind.*:${CONSOLE_PORT}\b")
  if [[ "$FRONTEND_S3" -ge 1 && "$FRONTEND_CONSOLE" -ge 1 ]]; then
    pass "Frontends detectados para puerto ${S3_PORT} (S3) y ${CONSOLE_PORT} (Consola)"
  else
    fail "No se detectaron ambos frontends esperados (S3:${S3_PORT}=${FRONTEND_S3}, Consola:${CONSOLE_PORT}=${FRONTEND_CONSOLE}) — revisar bloques 'frontend'/'bind' en la config"
  fi

  # 2.2 Cada frontend enruta a los 3 backends MinIO
  info "2.2 Verificando que los backends listan los 3 nodos MinIO..."
  for NODE in "${NODES[@]}"; do
    if echo "$HAPROXY_CFG" | grep -q "server .*${NODE}"; then
      pass "Backend incluye server hacia $NODE"
    else
      fail "Backend NO incluye server hacia $NODE — revisar sección 'backend'"
    fi
  done

  # 2.3 Health check activo contra endpoint nativo de MinIO (no solo TCP)
  info "2.3 Verificando health check aplicativo (no solo TCP)..."
  if echo "$HAPROXY_CFG" | grep -qi "option httpchk\|http-check"; then
    # HAProxy >= 1.7 separa 'option httpchk' (activa el check) de la línea real
    # con la ruta ('http-check send ... uri ...'); revisar TODAS las líneas
    # que matchean, no solo la primera, para no perder la ruta real.
    HC_LINES=$(echo "$HAPROXY_CFG" | grep -i "option httpchk\|http-check")
    if echo "$HC_LINES" | grep -qi "minio/health"; then
      HC_LINE=$(echo "$HC_LINES" | grep -i "minio/health" | head -1)
      pass "Health check aplicativo configurado contra endpoint nativo de MinIO: $HC_LINE"
    else
      HC_LINE=$(echo "$HC_LINES" | head -1)
      warn "Health check aplicativo configurado, pero no referencia explícitamente /minio/health/*: $HC_LINE — confirmar que no es solo un GET genérico"
    fi
  else
    fail "No se encontró 'option httpchk'/'http-check' — el health check parece ser solo TCP, insuficiente para detectar un MinIO colgado que aún acepta conexiones"
  fi

  # Intento de 'show stat' vía socket admin, si está expuesto
  if [[ -n "$HAPROXY_CTR" ]]; then
    STATS_OUT=$(podman exec "$HAPROXY_CTR" sh -c "echo 'show stat' | socat stdio /var/run/haproxy/admin.sock 2>/dev/null" 2>/dev/null || true)
    if [[ -n "$STATS_OUT" ]]; then
      info "Socket admin de HAProxy accesible — estado de backends:"
      echo "$STATS_OUT" | grep -E "^(minio|s3|console)" | head -10 | while read -r line; do info "    $line"; done
    else
      info "Socket admin de HAProxy no accesible desde aquí (o no expuesto) — omitido, no es bloqueante"
    fi
  fi

  # 2.4 Bind específico a la VIP (no 0.0.0.0)
  # Nota: bajo Podman rootless/pasta, el 'bind' dentro del .cfg NO determina
  # la exposición real en el host — pasta puede restringir el mapeo de
  # puertos a una sola IP del host aunque el .cfg diga 'bind :PUERTO'
  # (evidencia real: pbigd-stg01 tiene 'bind :9000' en el .cfg, pero
  # 'podman port' solo expone 172.17.210.62, nunca 0.0.0.0). Por eso se
  # consulta primero el mapeo real vía 'podman port' — la fuente de verdad —
  # y solo se cae al análisis de texto del .cfg si esa info no está disponible.
  info "2.4 Verificando a qué IP(s) del host queda expuesto HAProxy (podman port)..."
  BIND_CONFIRMED=false
  if [[ -n "$HAPROXY_CTR" ]]; then
    PORT_MAP=$(podman port "$HAPROXY_CTR" 2>/dev/null || true)
    HOST_IPS=$(echo "$PORT_MAP" | grep -E "^(${S3_PORT}|${CONSOLE_PORT})/tcp" | awk '{print $3}' | cut -d: -f1 | sort -u)
    N_IPS=$(echo "$HOST_IPS" | grep -c .)
    if [[ "$N_IPS" -eq 1 && -n "$HOST_IPS" && "$HOST_IPS" != "0.0.0.0" ]]; then
      if [[ -n "$VIP" && "$HOST_IPS" == "$VIP" ]]; then
        pass "podman port confirma exposición real solo en la VIP ($HOST_IPS) para 9000/9001 — el 'bind' interno del .cfg no representa el riesgo real en este entorno rootless (pasta)"
      else
        pass "podman port confirma exposición real solo en una IP específica ($HOST_IPS) para 9000/9001, no en 0.0.0.0 — confirmar manualmente que coincide con la VIP esperada"
      fi
      BIND_CONFIRMED=true
    elif [[ "$N_IPS" -gt 1 ]]; then
      fail "podman port expone 9000/9001 en múltiples IPs del host ($HOST_IPS) — riesgo real de choque con MinIO en la IP propia del nodo"
      BIND_CONFIRMED=true
    fi
  fi
  if [[ "$BIND_CONFIRMED" == false ]]; then
    if echo "$HAPROXY_CFG" | grep -qE "bind[[:space:]]+(0\.0\.0\.0)?:(${S3_PORT}|${CONSOLE_PORT})\b"; then
      warn "El .cfg de HAProxy hace bind a todas las interfaces (0.0.0.0 o ':PUERTO' sin IP) y no se pudo confirmar vía 'podman port' si el host restringe esa exposición — revisar manualmente"
    elif [[ -n "$VIP" ]] && echo "$HAPROXY_CFG" | grep -q "bind[[:space:]]*${VIP}"; then
      pass "HAProxy hace bind explícito a la VIP ($VIP) en los puertos esperados"
    else
      warn "No se pudo confirmar bind explícito a una VIP específica (¿VIP no pasada con --vip? revisar manualmente las líneas 'bind' de la config)"
    fi
  fi

  info "En el host, confirmando qué proceso escucha en 9000/9001..."
  ss -tlnp 2>/dev/null | grep -E ":(${S3_PORT}|${CONSOLE_PORT})\b" | while read -r line; do info "    $line"; done
else
  warn "2.1–2.4 omitidos por falta de config de HAProxy legible"
fi

# =============================================================================
# MÓDULO 3 — OBJETOS GRANDES (buffering / límite de tamaño)
# =============================================================================
header "MÓDULO 3 · Objetos grandes (buffering / límite de tamaño)"

if [[ -n "$HAPROXY_CFG" ]]; then
  info "3.1 Verificando ausencia de límites de tamaño de cuerpo / buffering forzado..."
  if echo "$HAPROXY_CFG" | grep -qi "http-request deny.*len\|reqideny.*Content-Length"; then
    fail "Se encontró una regla que podría limitar el tamaño de objetos subidos — revisar si es intencional"
  else
    pass "No se encontraron reglas explícitas de límite de tamaño de cuerpo en la config"
  fi
  TIMEOUT_LINES=$(echo "$HAPROXY_CFG" | grep -i "timeout" || true)
  if [[ -n "$TIMEOUT_LINES" ]]; then
    info "Timeouts configurados:"
    echo "$TIMEOUT_LINES" | while read -r line; do info "    $line"; done
  else
    warn "No se encontraron directivas 'timeout' explícitas — HAProxy usará defaults, que pueden ser insuficientes para objetos grandes"
  fi
else
  warn "3.1 omitido por falta de config de HAProxy legible"
fi

# 3.2 FUNCIONAL — subir/descargar objeto grande a través del endpoint VIP
if [[ "$SKIP_BIG_OBJECT" == true ]]; then
  info "3.2 Prueba funcional de objeto grande omitida (--skip-big-object)"
elif ! command -v mc &>/dev/null && ! command -v mcli &>/dev/null; then
  warn "3.2 omitido — mc/mcli no encontrado en PATH. Instalar: https://dl.min.io/client/mc/release/linux-amd64/mc"
elif [[ -z "$VIP" && -z "$VIP_DNS" ]]; then
  warn "3.2 omitido — no se pasó --vip ni --dns; no hay endpoint contra el cual probar. Ejecutar con --vip <IP> o --dns <nombre>"
else
  MC_CMD=$(command -v mcli 2>/dev/null || command -v mc)
  TARGET="${VIP_DNS:-$VIP}"
  if ! $MC_CMD alias ls 2>/dev/null | grep -q "^${MC_ALIAS}"; then
    warn "Alias mc '${MC_ALIAS}' no configurado. Crear con:"
    info "    mc alias set ${MC_ALIAS} http://${TARGET}:${S3_PORT} <ACCESS_KEY> <SECRET_KEY>"
    warn "3.2 omitido hasta configurar el alias"
  else
    info "3.2 Subiendo/descargando objeto de prueba de 80 MB a través de ${MC_ALIAS} (${TARGET}:${S3_PORT})..."
    BIG_FILE="/tmp/bluepoint_haproxy_minio_bigobj_$(date +%s).bin"
    dd if=/dev/urandom of="$BIG_FILE" bs=1M count=80 status=none 2>/dev/null
    SHA_ORIG=$(sha256sum "$BIG_FILE" | awk '{print $1}')
    OBJ="bluepoint-validation/haproxy-bigobj-$(date +%s).bin"
    if $MC_CMD cp "$BIG_FILE" "${MC_ALIAS}/raw/${OBJ}" &>/dev/null; then
      pass "Subida de objeto de 80MB vía VIP: OK"
      DOWNLOAD_FILE=$(mktemp)
      if $MC_CMD cp "${MC_ALIAS}/raw/${OBJ}" "$DOWNLOAD_FILE" &>/dev/null; then
        SHA_DOWN=$(sha256sum "$DOWNLOAD_FILE" | awk '{print $1}')
        if [[ "$SHA_ORIG" == "$SHA_DOWN" ]]; then
          pass "Descarga vía VIP: OK — checksum SHA-256 idéntico ($SHA_ORIG)"
        else
          fail "Checksum NO coincide entre subida y descarga (orig: $SHA_ORIG, descargado: $SHA_DOWN) — posible corrupción/buffering incorrecto en HAProxy"
        fi
      else
        fail "Descarga del objeto de prueba vía VIP: FALLÓ"
      fi
      $MC_CMD rm "${MC_ALIAS}/raw/${OBJ}" &>/dev/null || true
      rm -f "$DOWNLOAD_FILE"
    else
      fail "Subida de objeto de 80MB vía VIP: FALLÓ (timeout o error) — revisar timeouts/buffering de HAProxy"
    fi
    rm -f "$BIG_FILE"
  fi
fi

# =============================================================================
# MÓDULO 4 — ALTA DISPONIBILIDAD REAL (VIP / FAILOVER)
# =============================================================================
header "MÓDULO 4 · Alta disponibilidad real (VIP / failover)"

# 4.1 VIP presente en la interfaz de red
info "4.1 Verificando presencia de la VIP en la interfaz de red local..."
IFACE="${VIP_IFACE:-$(autodetect_iface)}"
if [[ -z "$IFACE" ]]; then
  warn "No se pudo autodetectar la interfaz de red — pasar con --iface <interfaz>"
else
  info "Interfaz en uso: $IFACE"
  DETECTED_VIP="${VIP:-$(autodetect_vip "$IFACE")}"
  if [[ -n "$DETECTED_VIP" ]]; then
    if ip -4 -o addr show dev "$IFACE" 2>/dev/null | grep -q "$DETECTED_VIP"; then
      pass "VIP ($DETECTED_VIP) presente en $IFACE de este nodo — este nodo sería el MASTER actual"
      VIP="${VIP:-$DETECTED_VIP}"
    else
      info "VIP ($DETECTED_VIP) NO está en este nodo — normal si este nodo es BACKUP. Confirmar en los otros 2 nodos:"
      info "    ip addr show $IFACE | grep ${DETECTED_VIP}"
    fi
  else
    warn "No se pudo determinar la VIP (ni pasada con --vip, ni autodetectada como IP secundaria en $IFACE) — revisar manualmente 'ip addr show $IFACE' en los 3 nodos"
  fi
fi

# 4.2 Estado VRRP MASTER/BACKUP coherente (journal)
info "4.2 Intentando leer estado VRRP en journal de keepalived..."
JOURNAL_OUT=$(journalctl -u keepalived --since "1 hour ago" --no-pager 2>&1 || true)
if echo "$JOURNAL_OUT" | grep -qi "Permission denied\|-- No entries --\|Failed to"; then
  pending "journal de keepalived no accesible como usuario actual (journald restringido) — verificación manual pendiente"
elif [[ -n "$JOURNAL_OUT" ]]; then
  VRRP_STATE=$(echo "$JOURNAL_OUT" | grep -iE "Entering (MASTER|BACKUP|FAULT) STATE" | tail -1)
  if [[ -n "$VRRP_STATE" ]]; then
    pass "Último estado VRRP en journal: $VRRP_STATE"
  else
    warn "journal de keepalived accesible pero sin transiciones de estado VRRP en la última hora — revisar ventana de tiempo o nivel de log"
  fi
else
  pending "journal de keepalived vacío o no accesible — verificación manual pendiente"
fi

# 4.3 FUNCIONAL — la VIP responde tráfico real (S3 y Consola)
info "4.3 Probando tráfico real contra la VIP (S3 y Consola)..."
if [[ -n "$VIP" || -n "$VIP_DNS" ]]; then
  TARGET="${VIP_DNS:-$VIP}"
  HC_S3=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 --max-time 10 \
    "http://${TARGET}:${S3_PORT}/minio/health/live" 2>/dev/null || echo "000")
  [[ "$HC_S3" == "200" ]] && pass "VIP S3 (${TARGET}:${S3_PORT}) — /minio/health/live HTTP $HC_S3" \
                            || fail "VIP S3 (${TARGET}:${S3_PORT}) — /minio/health/live HTTP $HC_S3 (esperado 200)"

  HC_CONSOLE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 --max-time 10 \
    "http://${TARGET}:${CONSOLE_PORT}" 2>/dev/null || echo "000")
  [[ "$HC_CONSOLE" =~ ^(200|30[12])$ ]] && pass "VIP Consola (${TARGET}:${CONSOLE_PORT}) — HTTP $HC_CONSOLE" \
                                          || fail "VIP Consola (${TARGET}:${CONSOLE_PORT}) — HTTP $HC_CONSOLE"
else
  warn "4.3 omitido — no hay VIP/DNS conocida (pasar con --vip o --dns)"
fi

# 4.4 FUNCIONAL — prueba de failover real [root/sudo — MANUAL, nunca automatizada]
manual "4.4 Prueba de failover real (VIP debe migrar al BACKUP): NO se automatiza con sudo embebido."
pending "4.4 verificación manual pendiente (no ejecutado por este script)"

# 4.5 Nombre DNS interno de la VIP resuelve correctamente
info "4.5 Verificando resolución del nombre DNS interno de la VIP..."
if [[ -n "$VIP_DNS" ]]; then
  RESOLVED=$(getent hosts "$VIP_DNS" 2>/dev/null | awk '{print $1}' || true)
  if [[ -n "$RESOLVED" ]]; then
    if [[ -n "$VIP" && "$RESOLVED" == "$VIP" ]]; then
      pass "DNS '$VIP_DNS' resuelve a $RESOLVED (coincide con la VIP esperada)"
    else
      pass "DNS '$VIP_DNS' resuelve a $RESOLVED"
      [[ -n "$VIP" ]] && warn "  La IP resuelta ($RESOLVED) no coincide con la VIP pasada por --vip ($VIP) — confirmar cuál es la correcta"
    fi
  else
    fail "DNS '$VIP_DNS' no resuelve desde este nodo — revisar registro DNS interno"
  fi
else
  warn "4.5 omitido — no se pasó --dns con el nombre DNS interno esperado"
fi

# =============================================================================
# MÓDULO 5 — DETALLE DE CONFIGURACIÓN DE KEEPALIVED (sensible)
# =============================================================================
header "MÓDULO 5 · Configuración de Keepalived (sensible)"

info "5.1 Intentando leer ${KEEPALIVED_CONF} como usuario actual (sin sudo)..."
if [[ -r "$KEEPALIVED_CONF" ]]; then
  KA_VIP=$(grep -i "virtual_ipaddress" -A2 "$KEEPALIVED_CONF" 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  KA_IFACE=$(grep -i "interface" "$KEEPALIVED_CONF" 2>/dev/null | head -1 | awk '{print $2}')
  KA_PRIO=$(grep -i "priority" "$KEEPALIVED_CONF" 2>/dev/null | head -1 | awk '{print $2}')
  HAS_AUTH=$(grep -ic "authentication" "$KEEPALIVED_CONF" 2>/dev/null || echo 0)
  pass "keepalived.conf legible — VIP: ${KA_VIP:-N/A} — interfaz: ${KA_IFACE:-N/A} — priority: ${KA_PRIO:-N/A}"
  if [[ "$HAS_AUTH" -gt 0 ]]; then
    pass "Bloque de autenticación VRRP presente (no se imprime auth_pass)"
  else
    warn "No se encontró bloque de autenticación VRRP en keepalived.conf — revisar"
  fi
elif [[ -e "$KEEPALIVED_CONF" ]]; then
  # Existe pero este usuario no puede leerlo (permisos 600/root) — sudo sí lo resuelve.
  pending "${KEEPALIVED_CONF} existe pero no es legible como usuario actual (permisos 600/root) — verificación manual pendiente"
  manual "5.1 Ejecutar manualmente: sudo cat ${KEEPALIVED_CONF}"
else
  # No existe en absoluto — no es un problema de permisos, sudo tampoco lo va a encontrar.
  fail "${KEEPALIVED_CONF} no existe en este nodo — no es un problema de permisos (sudo no lo resuelve); revisar si Keepalived está realmente desplegado aquí o si la config vive en otra ruta"
fi

# =============================================================================
# MÓDULO 6 — PRERREQUISITOS DE VIRTUALIZACIÓN (manual/documental)
# =============================================================================
header "MÓDULO 6 · Prerrequisitos de virtualización (vCenter/ESXi)"

pending "6.1 IP reservada para la VIP en el segmento de red de ${NODES[*]} — verificación manual pendiente (sin acceso a vCenter/ESXi desde este script)"
pending "6.2 'Forged Transmits' en modo Accept (no Reject) en el port group del vSwitch de estos 3 nodos — verificación manual pendiente"
pending "6.3 Regla de anti-afinidad DRS entre ${NODES[*]} — verificar que sigue aplicada; ahora también protege a HAProxy/Keepalived co-ubicados, no solo al quórum de MinIO — verificación manual pendiente"

# =============================================================================
# RESUMEN FINAL
# =============================================================================
header "RESUMEN DE VALIDACIÓN"

echo -e "  Componente : HAProxy + Keepalived delante de MinIO"
echo -e "  Entorno    : ${BOLD}${ENV_LABEL}${NC}"
echo -e "  Nodo       : ${BOLD}${HOSTNAME_ACTUAL}${NC}"
echo -e "  Fecha      : $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo ""
PASS_COUNT=$((CHECKS - ERRORS - WARNINGS - MANUALS))
echo -e "  ${BOLD}${CHECKS} checks / ${GREEN}${PASS_COUNT} PASS${NC} / ${RED}${ERRORS} FAIL${NC} / ${YELLOW}${WARNINGS} WARN${NC} / ${YELLOW}${BOLD}${MANUALS} MANUAL${NC}"
echo ""
if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 && $MANUALS -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}  ✅  VALIDACIÓN COMPLETADA SIN ERRORES, ADVERTENCIAS NI PENDIENTES MANUALES${NC}"
elif [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN SIN ERRORES NI ADVERTENCIAS — ${MANUALS} VERIFICACIÓN(ES) MANUAL(ES) PENDIENTE(S)${NC}"
elif [[ $ERRORS -eq 0 ]]; then
  echo -e "${YELLOW}${BOLD}  ⚠️   VALIDACIÓN COMPLETADA CON ${WARNINGS} ADVERTENCIA(S) Y ${MANUALS} PENDIENTE(S) MANUAL(ES)${NC}"
else
  echo -e "${RED}${BOLD}  ❌  VALIDACIÓN FALLÓ: ${ERRORS} ERROR(ES) | ${WARNINGS} ADVERTENCIA(S) | ${MANUALS} PENDIENTE(S) MANUAL(ES)${NC}"
fi
echo ""

exit $ERRORS
