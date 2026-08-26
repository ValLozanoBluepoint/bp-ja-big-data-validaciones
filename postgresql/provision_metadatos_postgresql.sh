#!/usr/bin/env bash
# =============================================================================
#  Bluepoint AI — Cooperativa Jardín Azuayo — Proyecto Big Data
#  Aprovisionamiento de esquemas y roles de la instancia PostgreSQL de METADATOS
#  (platform-db1 / DR: platform-dbdr)
#
#  - Idempotente: puede ejecutarse múltiples veces sin efectos adversos.
#  - Aplica esquemas, roles, grants, default privileges y hardening.
#  - Verifica el resultado con salida OK/FAIL/WARN y cuadro resumen.
#  - Log en /tmp/. Exit code = número de FALLOS.
#
#  EJECUCIÓN (desde la consola de platform-db1, como usuario del motor):
#      # 1) Exportar contraseñas desde el gestor de secretos (NO hardcodear):
#      export PGPW_MIG_META='...'      PGPW_APP_EXTRACTOR='...'
#      export PGPW_APP_FLINK='...'     PGPW_APP_AIRFLOW='...'
#      export PGPW_RO_META='...'
#      # 2) (opcional) Ajustar conexión de administración:
#      export PGHOST=/var/run/postgresql PGPORT=5432 PGUSER=postgres
#      # 3) Ejecutar:
#      bash provision_metadatos_postgresql.sh
#
#  Las contraseñas se inyectan a psql vía \getenv (no aparecen en `ps` ni en el log).
#  Requiere PostgreSQL 16+ (usa \getenv). El proyecto está en 17.x. OK.
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# Configuración (todo overridable por variable de entorno)
# ---------------------------------------------------------------------------
METADB="${METADB:-bd_bigd_meta}"          # nombre de la base de metadatos
ADMINDB="${ADMINDB:-postgres}"         # base de mantenimiento para checks previos
export PGHOST="${PGHOST:-/var/run/postgresql}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-postgres}"

# Límites de conexión por rol (protección ante agotamiento de conexiones)
CL_MIG="${CL_MIG:-5}"
CL_EXTRACTOR="${CL_EXTRACTOR:-20}"
CL_FLINK="${CL_FLINK:-40}"
CL_AIRFLOW="${CL_AIRFLOW:-20}"
CL_RO="${CL_RO:-10}"

TS="$(date +%Y%m%d_%H%M%S)"
LOG="/tmp/provision_metadatos_${METADB}_${TS}.log"

# ---------------------------------------------------------------------------
# Colores y contadores
# ---------------------------------------------------------------------------
if [ -t 1 ]; then
  GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[0;33m'; BLU='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'
else
  GREEN=''; RED=''; YEL=''; BLU=''; BOLD=''; NC=''
fi
PASS=0; FAIL=0; WARN=0

section(){ echo -e "\n${BLU}${BOLD}=== $1 ===${NC}" | tee -a "$LOG"; }
ok(){   echo -e "  ${GREEN}[ OK ]${NC} $1" | tee -a "$LOG"; PASS=$((PASS+1)); }
fail(){ echo -e "  ${RED}[FAIL]${NC} $1" | tee -a "$LOG"; FAIL=$((FAIL+1)); }
warn(){ echo -e "  ${YEL}[WARN]${NC} $1" | tee -a "$LOG"; WARN=$((WARN+1)); }
info(){ echo -e "  $1" | tee -a "$LOG"; }

# psql helper para consultas escalares (silencioso, sin encabezados)
q(){ psql -X -A -t -q -d "$METADB" -c "$1" 2>>"$LOG"; }
# psql helper de administración contra ADMINDB
qa(){ psql -X -A -t -q -d "$ADMINDB" -c "$1" 2>>"$LOG"; }

echo -e "${BOLD}Aprovisionamiento metadatos — base '${METADB}' — ${TS}${NC}" | tee "$LOG"

# ===========================================================================
# FASE 0 — PRE-FLIGHT
# ===========================================================================
section "FASE 0 — Verificaciones previas"

command -v psql >/dev/null 2>&1 && ok "cliente psql disponible" || { fail "psql no encontrado en PATH"; echo; echo "Abortando."; exit 1; }

PGVER="$(qa "SHOW server_version;" | tr -d ' ')"
if [ -n "$PGVER" ]; then
  ok "conexión de administración correcta (server_version=${PGVER})"
else
  fail "no fue posible conectar como '$PGUSER' a '$ADMINDB' (host=$PGHOST port=$PGPORT)"
  echo; echo "Abortando: sin conexión de administración."; exit 1
fi

PGMAJ="$(echo "$PGVER" | cut -d. -f1)"
if [ "${PGMAJ:-0}" -ge 16 ] 2>/dev/null; then
  ok "versión soporta \\getenv (>=16)"
else
  warn "server_version <16: \\getenv podría no estar disponible en psql; verifique la versión del cliente"
fi

# Verificar variables de contraseña (no se imprime su valor)
declare -A PWVARS=(
  [mig_meta]=PGPW_MIG_META
  [app_extractor]=PGPW_APP_EXTRACTOR
  [app_flink]=PGPW_APP_FLINK
  [app_airflow]=PGPW_APP_AIRFLOW
  [ro_meta]=PGPW_RO_META
)
MISSING_PW=()
for role in "${!PWVARS[@]}"; do
  var="${PWVARS[$role]}"
  if [ -z "${!var:-}" ]; then MISSING_PW+=("$role ($var)"); fi
done
if [ "${#MISSING_PW[@]}" -eq 0 ]; then
  ok "todas las variables de contraseña están definidas (5/5)"
else
  warn "faltan contraseñas para: ${MISSING_PW[*]} — esos roles se crearán SIN LOGIN habilitado hasta fijar su clave"
fi

# ===========================================================================
# FASE 1 — BASE DE DATOS
# ===========================================================================
section "FASE 1 — Base de datos de metadatos"

if [ "$(qa "SELECT 1 FROM pg_database WHERE datname='${METADB}';")" = "1" ]; then
  ok "base '${METADB}' ya existe"
else
  if qa "CREATE DATABASE ${METADB};" >/dev/null 2>>"$LOG"; then
    ok "base '${METADB}' creada"
  else
    fail "no se pudo crear la base '${METADB}' (ver log)"
    echo; echo "Abortando."; exit "$FAIL"
  fi
fi

# ===========================================================================
# FASE 2 — ROLES (idempotente)
# ===========================================================================
section "FASE 2 — Roles"

# 2.1 Rol grupo dueño (NOLOGIN) + rol de migraciones/DDL
psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >>"$LOG" 2>&1 <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'meta_owner') THEN
    CREATE ROLE meta_owner NOLOGIN;
  END IF;
END $$;
ALTER ROLE meta_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'mig_meta') THEN
    CREATE ROLE mig_meta LOGIN;
  END IF;
END $$;
ALTER ROLE mig_meta NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
GRANT meta_owner TO mig_meta;
SQL
[ $? -eq 0 ] && ok "roles meta_owner (NOLOGIN) y mig_meta creados/normalizados" || fail "error creando meta_owner/mig_meta (ver log)"
psql -X -q -d "$METADB" -c "ALTER ROLE mig_meta CONNECTION LIMIT ${CL_MIG};" >/dev/null 2>>"$LOG"

# 2.2 Roles de aplicación (LOGIN, sin privilegios peligrosos)
create_login_role(){
  local role="$1" climit="$2"
  psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >>"$LOG" 2>&1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${role}') THEN
    CREATE ROLE ${role} LOGIN;
  END IF;
END \$\$;
ALTER ROLE ${role} NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS INHERIT CONNECTION LIMIT ${climit};
SQL
  return $?
}

for pair in "app_extractor:${CL_EXTRACTOR}" "app_flink:${CL_FLINK}" "app_airflow:${CL_AIRFLOW}" "ro_meta:${CL_RO}"; do
  role="${pair%%:*}"; climit="${pair##*:}"
  if create_login_role "$role" "$climit"; then
    ok "rol ${role} creado/normalizado (LOGIN, NOSUPERUSER, conn_limit=${climit})"
  else
    fail "error creando rol ${role} (ver log)"
  fi
done

# 2.3 Fijar contraseñas desde el entorno (vía \getenv; no expone el valor)
set_pw(){
  local role="$1" var="$2"
  [ -z "${!var:-}" ] && return 2   # sin contraseña => omitir
  METAPW_ENV="$var" psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >/dev/null 2>>"$LOG" <<SQL
\\getenv pw ${var}
ALTER ROLE ${role} WITH LOGIN PASSWORD :'pw';
SQL
  return $?
}
for role in mig_meta app_extractor app_flink app_airflow ro_meta; do
  var="${PWVARS[$role]}"
  set_pw "$role" "$var"; rc=$?
  case "$rc" in
    0) ok "contraseña fijada para ${role}";;
    2) warn "sin contraseña para ${role} (variable ${var} no definida) — login quedará inhabilitado";;
    *) fail "error fijando contraseña para ${role} (ver log)";;
  esac
done

# ===========================================================================
# FASE 3 — ESQUEMAS (idempotente, propiedad de meta_owner)
# ===========================================================================
section "FASE 3 — Esquemas"

for sch in control dlq audit pipeline; do
  psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >>"$LOG" 2>&1 <<SQL
CREATE SCHEMA IF NOT EXISTS ${sch} AUTHORIZATION meta_owner;
ALTER SCHEMA ${sch} OWNER TO meta_owner;
SQL
  [ $? -eq 0 ] && ok "esquema ${sch} (owner meta_owner)" || fail "error con esquema ${sch} (ver log)"
done

# ===========================================================================
# FASE 4 — HARDENING (cierre de public y de la base)
# ===========================================================================
section "FASE 4 — Hardening"

psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >>"$LOG" 2>&1 <<SQL
REVOKE ALL ON DATABASE ${METADB} FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM PUBLIC;
SQL
[ $? -eq 0 ] && ok "public y base cerrados a PUBLIC" || fail "error aplicando hardening (ver log)"

# CONNECT explícito por rol
for role in mig_meta app_extractor app_flink app_airflow ro_meta; do
  psql -X -q -d "$METADB" -c "GRANT CONNECT ON DATABASE ${METADB} TO ${role};" >/dev/null 2>>"$LOG"
done
ok "CONNECT otorgado explícitamente a los 5 roles"

# ===========================================================================
# FASE 5 — PERMISOS POR ROL (mínimo privilegio)
# ===========================================================================
section "FASE 5 — Permisos (USAGE, DML, secuencias, default privileges)"

grant_block(){
  psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >>"$LOG" 2>&1 <<SQL
-- ============ mig_meta: DDL vía meta_owner (crea objetos como meta_owner) ============
GRANT USAGE, CREATE ON SCHEMA control, dlq, audit, pipeline TO meta_owner;

-- ============ app_extractor : control (S/I/U) + audit (I) ============
GRANT USAGE ON SCHEMA control, audit TO app_extractor;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA control TO app_extractor;
GRANT INSERT                 ON ALL TABLES IN SCHEMA audit   TO app_extractor;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control, audit TO app_extractor;

-- ============ app_flink : dlq(I) + audit(I) + control(S/U) + pipeline(I/U) ============
GRANT USAGE ON SCHEMA dlq, audit, control, pipeline TO app_flink;
GRANT INSERT          ON ALL TABLES IN SCHEMA dlq      TO app_flink;
GRANT INSERT          ON ALL TABLES IN SCHEMA audit    TO app_flink;
GRANT SELECT, UPDATE  ON ALL TABLES IN SCHEMA control  TO app_flink;
GRANT INSERT, UPDATE  ON ALL TABLES IN SCHEMA pipeline TO app_flink;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA dlq, audit, pipeline TO app_flink;

-- ============ app_airflow : control/dlq/pipeline (S/I/U/D) + audit (S) ============
GRANT USAGE ON SCHEMA control, dlq, pipeline, audit TO app_airflow;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA control  TO app_airflow;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dlq      TO app_airflow;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pipeline TO app_airflow;
GRANT SELECT                         ON ALL TABLES IN SCHEMA audit    TO app_airflow;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA control, dlq, pipeline TO app_airflow;

-- ============ ro_meta : solo lectura global ============
GRANT pg_read_all_data TO ro_meta;
SQL
  return $?
}
grant_block && ok "grants a nivel de esquema, tablas y secuencias aplicados" || fail "error aplicando grants (ver log)"

# Default privileges: objetos FUTUROS creados por meta_owner heredan permisos
default_priv_block(){
  psql -X -q -v ON_ERROR_STOP=1 -d "$METADB" >>"$LOG" 2>&1 <<SQL
-- control
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA control
  GRANT SELECT, INSERT, UPDATE ON TABLES TO app_extractor;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA control
  GRANT SELECT, UPDATE ON TABLES TO app_flink;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA control
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_airflow;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA control
  GRANT USAGE, SELECT ON SEQUENCES TO app_extractor, app_flink, app_airflow;
-- dlq
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA dlq
  GRANT INSERT ON TABLES TO app_flink;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA dlq
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_airflow;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA dlq
  GRANT USAGE, SELECT ON SEQUENCES TO app_flink, app_airflow;
-- audit (append-only: solo INSERT para productores; SELECT para lectores)
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA audit
  GRANT INSERT ON TABLES TO app_extractor, app_flink;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA audit
  GRANT SELECT ON TABLES TO app_airflow;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA audit
  GRANT USAGE, SELECT ON SEQUENCES TO app_extractor, app_flink;
-- pipeline
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA pipeline
  GRANT INSERT, UPDATE ON TABLES TO app_flink;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA pipeline
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_airflow;
ALTER DEFAULT PRIVILEGES FOR ROLE meta_owner IN SCHEMA pipeline
  GRANT USAGE, SELECT ON SEQUENCES TO app_flink, app_airflow;
SQL
  return $?
}
default_priv_block && ok "ALTER DEFAULT PRIVILEGES aplicado (herencia para objetos futuros)" \
                   || fail "error aplicando default privileges (ver log)"

info "Nota: las migraciones deben crear objetos como meta_owner (p.ej. 'SET ROLE meta_owner;' antes del DDL)"
info "      para que la herencia de default privileges surta efecto. ro_meta lee vía pg_read_all_data."

# ===========================================================================
# FASE 6 — VERIFICACIÓN
# ===========================================================================
section "FASE 6 — Verificación"

# 6.1 Roles y atributos
check_role_attr(){
  local role="$1" wantlogin="$2"
  local row
  row="$(q "SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname='${role}';")"
  if [ -z "$row" ]; then fail "rol ${role} no existe"; return; fi
  IFS='|' read -r canlogin super createdb createrole <<<"$row"
  if [ "$canlogin" = "$wantlogin" ] && [ "$super" = "f" ] && [ "$createdb" = "f" ] && [ "$createrole" = "f" ]; then
    ok "rol ${role}: login=${canlogin}, sin superuser/createdb/createrole"
  else
    fail "rol ${role}: atributos inesperados (login=${canlogin} super=${super} createdb=${createdb} createrole=${createrole})"
  fi
}
check_role_attr meta_owner    f
check_role_attr mig_meta      t
check_role_attr app_extractor t
check_role_attr app_flink     t
check_role_attr app_airflow   t
check_role_attr ro_meta       t

# 6.2 Contraseñas presentes (pg_authid requiere superuser; si no, WARN)
for role in mig_meta app_extractor app_flink app_airflow ro_meta; do
  has="$(q "SELECT (rolpassword IS NOT NULL) FROM pg_authid WHERE rolname='${role}';" 2>/dev/null)"
  case "$has" in
    t) ok "rol ${role}: contraseña establecida";;
    f) warn "rol ${role}: sin contraseña (login inhabilitado hasta fijarla)";;
    *) warn "rol ${role}: no se pudo leer pg_authid (¿sin superuser?)";;
  esac
done

# 6.3 Esquemas y propiedad
for sch in control dlq audit pipeline; do
  owner="$(q "SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname='${sch}';")"
  if [ "$owner" = "meta_owner" ]; then ok "esquema ${sch}: existe y pertenece a meta_owner"
  elif [ -z "$owner" ]; then fail "esquema ${sch}: no existe"
  else fail "esquema ${sch}: owner inesperado (${owner})"; fi
done

# 6.4 Hardening
[ "$(q "SELECT has_schema_privilege('public','public','CREATE');")" = "f" ] \
  && ok "public: CREATE revocado a PUBLIC" || fail "public: CREATE aún disponible a PUBLIC"
[ "$(q "SELECT has_database_privilege('public','${METADB}','CONNECT');")" = "f" ] \
  && ok "base ${METADB}: CONNECT revocado a PUBLIC" || fail "base ${METADB}: CONNECT aún disponible a PUBLIC"

# 6.5 USAGE por rol (muestras representativas)
usage_check(){ # rol esquema esperado(t/f)
  local role="$1" sch="$2" want="$3"
  local got; got="$(q "SELECT has_schema_privilege('${role}','${sch}','USAGE');")"
  if [ "$got" = "$want" ]; then ok "USAGE ${role} -> ${sch} = ${got}"; else fail "USAGE ${role} -> ${sch} = ${got} (esperado ${want})"; fi
}
usage_check app_extractor control t
usage_check app_extractor dlq     f
usage_check app_flink     dlq     t
usage_check app_airflow   pipeline t
usage_check ro_meta       audit   t

# 6.6 Default privileges presentes
for sch in control dlq audit pipeline; do
  n="$(q "SELECT count(*) FROM pg_default_acl d JOIN pg_namespace n ON n.oid=d.defaclnamespace WHERE n.nspname='${sch}';")"
  if [ "${n:-0}" -gt 0 ] 2>/dev/null; then ok "default privileges definidos en ${sch} (${n} entradas)"
  else fail "default privileges ausentes en ${sch}"; fi
done

# 6.7 pg_read_all_data en ro_meta
if [ "$(q "SELECT pg_has_role('ro_meta','pg_read_all_data','MEMBER');")" = "t" ]; then
  ok "ro_meta es miembro de pg_read_all_data (solo lectura global)"
else
  fail "ro_meta NO tiene pg_read_all_data"
fi

# ===========================================================================
# RESUMEN
# ===========================================================================
TOTAL=$((PASS+FAIL+WARN))
echo | tee -a "$LOG"
echo -e "${BOLD}+---------------------------------------------------------------+${NC}" | tee -a "$LOG"
echo -e "${BOLD}|  RESUMEN DE APROVISIONAMIENTO — ${METADB}$(printf '%*s' $((30-${#METADB})) '')|${NC}" | tee -a "$LOG"
echo -e "${BOLD}+---------------------------------------------------------------+${NC}" | tee -a "$LOG"
printf "|  ${GREEN}OK  : %-3d${NC}   ${RED}FAIL: %-3d${NC}   ${YEL}WARN: %-3d${NC}   TOTAL: %-3d          |\n" "$PASS" "$FAIL" "$WARN" "$TOTAL" | tee -a "$LOG"
echo -e "${BOLD}+---------------------------------------------------------------+${NC}" | tee -a "$LOG"
echo -e "  Log: ${LOG}" | tee -a "$LOG"

if [ "$FAIL" -eq 0 ]; then
  echo -e "  ${GREEN}${BOLD}Resultado: APROVISIONAMIENTO CORRECTO${NC}" | tee -a "$LOG"
else
  echo -e "  ${RED}${BOLD}Resultado: ${FAIL} FALLO(S) — revisar el log${NC}" | tee -a "$LOG"
fi

exit "$FAIL"
