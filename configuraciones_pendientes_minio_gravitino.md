# Configuraciones pendientes — Trino, Gravitino y MinIO/AIStor

Para el equipo de infraestructura de Jardín Azuayo. Esto resume, en un solo
lugar, lo que falta configurar para que Trino y Flink puedan realmente leer y
escribir datos en el lake (MinIO/AIStor) a través de Gravitino. Se detectó al
correr las validaciones del proyecto Big Data (Principal, 2026-08-14).

**Importante**: los valores de ejemplo abajo (endpoints, nombres de bucket,
usuarios, contraseñas) son placeholders para que se entienda la forma del
cambio — no hay que copiarlos carácter a carácter. Reemplazar cada uno por el
valor real del entorno (bucket del warehouse ya definido, endpoint/VIP real
de MinIO, credenciales que genere el propio equipo, etc.).

---

## Qué falta y por qué

Hoy Trino y Flink hablan con Gravitino (el catálogo de metadata) pero
**Gravitino no está conectado a MinIO/AIStor** — su configuración sigue con
los valores de ejemplo de la documentación pública, nunca completados con
los datos reales del entorno. Como consecuencia, ninguna tabla que se cree a
través de Gravitino llega al lake real.

Faltan tres piezas, en tres lugares distintos:

1. Completar la conexión S3 de **Gravitino** hacia MinIO.
2. Agregar la conexión S3 directa de **Trino** hacia MinIO (hoy Trino solo
   sabe hablar con Gravitino, no tiene su propio acceso a los archivos).
3. Crear en **MinIO** los usuarios/policies que esas dos conexiones van a
   usar (ninguno de los dos debe usar credenciales root/admin).

---

## 1. Gravitino — completar el warehouse S3

Archivo dentro del contenedor `gravitino`:
`/root/gravitino/conf/gravitino-iceberg-rest-server.conf`

Hoy tiene:
```
gravitino.iceberg-rest.catalog-backend = memory
gravitino.iceberg-rest.warehouse = /tmp
```

Hay que cambiarlo a algo como esto (los valores son de ejemplo):
```
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.jdbc-driver = org.postgresql.Driver
gravitino.iceberg-rest.uri = jdbc:postgresql://<host-postgres-real>:5432/gravitino
gravitino.iceberg-rest.jdbc-user = <usuario-postgres>
gravitino.iceberg-rest.jdbc-initialize = true

gravitino.iceberg-rest.warehouse = s3://<bucket-warehouse-real>/warehouse
gravitino.iceberg-rest.io-impl = org.apache.iceberg.aws.s3.S3FileIO
gravitino.iceberg-rest.s3-endpoint = http://<endpoint-real-minio>:9000
gravitino.iceberg-rest.s3-access-key-id = <usuario gravitino-svc, ver sección 3>
gravitino.iceberg-rest.s3-secret-access-key = <secreto>
```

- `catalog-backend = memory` se pierde en cada reinicio del contenedor — hay
  que pasar a un backend persistente (`jdbc`, con una base de datos real, no
  el ejemplo de `127.0.0.1`).
- `warehouse` debe ser el bucket S3 real del proyecto, no una ruta local.

## 2. Trino — agregar el bloque S3 que hoy no existe

Archivo dentro del contenedor `trino`: `/etc/trino/catalog/iceberg.properties`

Hoy solo tiene la parte de metadata (esto se queda igual, no tocar):
```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.containers.internal:9001/iceberg/
iceberg.rest-catalog.security=NONE
```

Falta agregar, en el mismo archivo, el bloque de acceso directo a los
archivos (valores de ejemplo):
```
fs.native-s3.enabled=true
s3.endpoint=http://<endpoint-real-minio>:9000
s3.path-style-access=true
s3.aws-access-key=<usuario trino-svc, ver sección 3>
s3.aws-secret-key=<secreto>
```

Trino necesita las dos cosas a la vez: Gravitino solo le dice "la tabla está
en tal ubicación", pero para leer/escribir esa ubicación necesita su propia
conexión a MinIO — no es algo que reemplace la conexión a Gravitino, se
agrega junto a ella en el mismo archivo.

## 3. MinIO — crear los dos usuarios de servicio

Ninguno de los dos componentes debe usar el usuario root/admin de MinIO.

**Trino — solo lectura.** Trino se usa para consultas de PowerBI/reportes,
no para escribir datos (eso lo hace Flink). El usuario de Trino solo necesita
poder leer:

```bash
cat > /tmp/trino-warehouse-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::<bucket-warehouse-real>",
        "arn:aws:s3:::<bucket-warehouse-real>/*"
      ]
    }
  ]
}
EOF

mc admin policy create <alias-minio-principal> trino-warehouse-policy /tmp/trino-warehouse-policy.json
mc admin user add <alias-minio-principal> trino-svc <password-seguro>
mc admin policy attach <alias-minio-principal> trino-warehouse-policy --user trino-svc
```

**Gravitino — lectura, escritura y borrado.** Gravitino necesita poder crear
la ubicación de tablas nuevas y limpiar archivos cuando alguien borra una
tabla (`DROP TABLE ... PURGE`):

```bash
cat > /tmp/gravitino-warehouse-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::<bucket-warehouse-real>",
        "arn:aws:s3:::<bucket-warehouse-real>/*"
      ]
    }
  ]
}
EOF

mc admin policy create <alias-minio-principal> gravitino-warehouse-policy /tmp/gravitino-warehouse-policy.json
mc admin user add <alias-minio-principal> gravitino-svc <password-seguro>
mc admin policy attach <alias-minio-principal> gravitino-warehouse-policy --user gravitino-svc
```

Los dos usuarios (`trino-svc`, `gravitino-svc`) deben apuntar al **mismo
bucket** — el mismo warehouse que ya usa Flink para los checkpoints/datos, no
uno nuevo.

**Nota**: esto es Principal, no DR — Trino y Gravitino no están confirmados
como desplegados en el datacenter alterno todavía, así que no hace falta
repetir esto ahí por ahora.

---

## Cómo confirmar que quedó bien

Después de aplicar los tres cambios, re-correr desde `pbigd-plat-apps01`:

```bash
validacion_trino_gravitino/validate_trino_gravitino_principal.sh
validacion_trino_minio/validate_trino_minio_principal.sh --dns <vip-minio-real> --test-table iceberg.<schema>.<tabla>
validacion_flink_gravitino/validate_flink_gravitino_principal.sh
```

Si el warehouse de Gravitino ya apunta a `s3://...`, el chequeo que antes
marcaba `[FAIL] Warehouse de Gravitino NO apunta a S3/AIStor` debería pasar a
`[OK]`, y las pruebas de lectura/escritura de tablas en los tres scripts
deberían empezar a funcionar contra datos reales.

Para más detalle de cómo se detectó este problema, ver
`hallazgos_transversales.md` (Hallazgo H1) en esta misma carpeta.
