# Guía de Validación — Cluster MinIO
## Bluepoint · Cooperativa Jardín Azuayo | Proyecto Big Data

Cubre los clusters de los dos datacenters:

| Script | Cluster | Nodos |
|---|---|---|
| `validate_minio_principal.sh` | DC Principal | stg-1, stg-2, stg-3 |
| `validate_minio_dr.sh` | DC Alterno (DR) | dr-stg-1, dr-stg-2, dr-stg-3 |

**Prerequisito obligatorio:** `mc` (MinIO Client) debe estar instalado en cada nodo antes de ejecutar. Sin `mc`, las secciones 5 y 6 (validación funcional, buckets, replicación) se omiten automáticamente.

---

## DC Principal

### Paso 1 — Copiar el script a cada nodo

```bash
for NODE in stg-1 stg-2 stg-3; do
  scp validate_minio_principal.sh admapl@$NODE:/tmp/
done
```

### Paso 2 — Configurar el alias `mc` en cada nodo

```bash
ssh admapl@stg-1 "mc alias set minio-principal http://stg-1:9000 <ACCESS_KEY> <SECRET_KEY>"
ssh admapl@stg-2 "mc alias set minio-principal http://stg-2:9000 <ACCESS_KEY> <SECRET_KEY>"
ssh admapl@stg-3 "mc alias set minio-principal http://stg-3:9000 <ACCESS_KEY> <SECRET_KEY>"
```

### Paso 3 — Ejecutar en los 3 nodos en secuencia

```bash
for NODE in stg-1 stg-2 stg-3; do
  echo "=== Ejecutando en $NODE ==="
  ssh admapl@$NODE "sudo bash /tmp/validate_minio_principal.sh \
    --alias minio-principal" \
    2>&1 | tee /tmp/resultado_minio_principal_${NODE}.log
done
```

---

## DC Alterno (DR)

### Paso 1 — Copiar el script a cada nodo DR

```bash
for NODE in dr-stg-1 dr-stg-2 dr-stg-3; do
  scp validate_minio_dr.sh admapl@$NODE:/tmp/
done
```

### Paso 2 — Configurar los dos alias `mc` en cada nodo DR

El cluster DR necesita ver tanto su propio cluster como el Principal para verificar la replicación.

```bash
ssh admapl@dr-stg-1 "mc alias set minio-dr http://dr-stg-1:9000 <ACCESS_KEY> <SECRET_KEY> && \
                     mc alias set minio-principal http://stg-1:9000 <ACCESS_KEY> <SECRET_KEY>"
ssh admapl@dr-stg-2 "mc alias set minio-dr http://dr-stg-2:9000 <ACCESS_KEY> <SECRET_KEY> && \
                     mc alias set minio-principal http://stg-1:9000 <ACCESS_KEY> <SECRET_KEY>"
ssh admapl@dr-stg-3 "mc alias set minio-dr http://dr-stg-3:9000 <ACCESS_KEY> <SECRET_KEY> && \
                     mc alias set minio-principal http://stg-1:9000 <ACCESS_KEY> <SECRET_KEY>"
```

### Paso 3 — Ejecutar en los 3 nodos DR en secuencia

```bash
for NODE in dr-stg-1 dr-stg-2 dr-stg-3; do
  echo "=== Ejecutando en DR: $NODE ==="
  ssh admapl@$NODE "sudo bash /tmp/validate_minio_dr.sh \
    --alias minio-dr \
    --principal-alias minio-principal" \
    2>&1 | tee /tmp/resultado_minio_dr_${NODE}.log
done
```

---

## Notas

- Si `mc` no está disponible, pasa `--skip-write-test` para ejecutar solo las validaciones estructurales (SO, directorios, puertos, contenedores).
- Si el DC principal está caído, ejecutar el DR con `--skip-replication-check` para evitar falsos errores en la sección de replicación.
- Los logs quedan en `/tmp/resultado_minio_*.log` en la máquina local de quien ejecuta.
