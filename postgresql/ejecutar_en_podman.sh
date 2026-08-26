#!/usr/bin/env bash
# =============================================================================
#  Bluepoint AI — Cooperativa Jardín Azuayo — Proyecto Big Data
#  Wrapper: ejecuta provision_metadatos_postgresql.sh DENTRO del contenedor
#  Podman de PostgreSQL en platform-db1 (o platform-dbdr).
#
#  USO (desde el host, como el usuario que administra Podman, p.ej. admapl):
#      # 1) Exportar las contraseñas ANTES de correr este wrapper:
#      export PGPW_MIG_META='...'      PGPW_APP_EXTRACTOR='...'
#      export PGPW_APP_FLINK='...'     PGPW_APP_AIRFLOW='...'
#      export PGPW_RO_META='...'
#      # 2) (opcional) fijar el nombre exacto del contenedor si el
#      #    autodetectado no es el correcto:
#      export CONTAINER_NAME=postgres
#      # 3) Ejecutar:
#      bash ejecutar_en_podman.sh
#
#  Qué hace:
#    - Detecta el contenedor de PostgreSQL corriendo bajo Podman.
#    - Copia provision_metadatos_postgresql.sh dentro del contenedor
#      (ruta temporal), lo ejecuta ahí con `podman exec`, reenviando las
#      variables PGPW_* SOLO como variables de entorno del proceso exec
#      (nunca se escriben a disco ni quedan en el historial del contenedor).
#    - Borra la copia del script del contenedor al finalizar (trap).
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_SCRIPT="${SCRIPT_DIR}/provision_metadatos_postgresql.sh"
REMOTE_PATH="/tmp/provision_metadatos_postgresql.sh"

if [ ! -f "$SRC_SCRIPT" ]; then
  echo "ERROR: no se encontró ${SRC_SCRIPT}" >&2
  exit 1
fi

command -v podman >/dev/null 2>&1 || { echo "ERROR: podman no está en el PATH de este host." >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1) Detectar el contenedor de PostgreSQL
# ---------------------------------------------------------------------------
if [ -n "${CONTAINER_NAME:-}" ]; then
  CID="$(podman ps --filter "name=${CONTAINER_NAME}" --format '{{.ID}}' | head -n1)"
else
  CID="$(podman ps --filter "name=postgres" --format '{{.ID}}' | head -n1)"
  [ -z "$CID" ] && CID="$(podman ps --filter "ancestor=postgres" --format '{{.ID}}' | head -n1)"
fi

if [ -z "$CID" ]; then
  echo "ERROR: no se encontró un contenedor de PostgreSQL corriendo." >&2
  echo "       Revisa 'podman ps' y fija CONTAINER_NAME=<nombre_exacto> si el autodetectado falla." >&2
  podman ps --format 'table {{.ID}} {{.Names}} {{.Image}} {{.Status}}' >&2
  exit 1
fi

CNAME="$(podman ps --filter "id=${CID}" --format '{{.Names}}')"
echo "Contenedor detectado: ${CNAME} (${CID})"

# ---------------------------------------------------------------------------
# 2) Verificar que las contraseñas estén exportadas en el host (WARN, no aborta)
# ---------------------------------------------------------------------------
for var in PGPW_MIG_META PGPW_APP_EXTRACTOR PGPW_APP_FLINK PGPW_APP_AIRFLOW PGPW_RO_META; do
  if [ -z "${!var:-}" ]; then
    echo "WARN: ${var} no está exportada en este shell — el rol correspondiente quedará sin login habilitado." >&2
  fi
done

# ---------------------------------------------------------------------------
# 3) Copiar el script dentro del contenedor y limpiar siempre al salir
# ---------------------------------------------------------------------------
cleanup(){ podman exec "$CID" rm -f "$REMOTE_PATH" >/dev/null 2>&1 || true; }
trap cleanup EXIT

podman cp "$SRC_SCRIPT" "${CID}:${REMOTE_PATH}"
podman exec "$CID" chmod +x "$REMOTE_PATH"

# ---------------------------------------------------------------------------
# 4) Ejecutar dentro del contenedor, reenviando solo las variables necesarias
# ---------------------------------------------------------------------------
podman exec \
  -e PGPW_MIG_META \
  -e PGPW_APP_EXTRACTOR \
  -e PGPW_APP_FLINK \
  -e PGPW_APP_AIRFLOW \
  -e PGPW_RO_META \
  -e "METADB=${METADB:-bigd_meta}" \
  -e "ADMINDB=${ADMINDB:-postgres}" \
  -e "PGHOST=${PGHOST:-/var/run/postgresql}" \
  -e "PGPORT=${PGPORT:-5432}" \
  -e "PGUSER=${PGUSER:-postgres}" \
  "$CID" bash "$REMOTE_PATH"
RC=$?

exit "$RC"
