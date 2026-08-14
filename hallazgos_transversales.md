# Hallazgos transversales — no acotados a una sola carpeta de validación

Hallazgos que afectan a más de un script/carpeta de `validaciones/` porque
comparten un componente de infraestructura raíz. Se documentan acá en vez de
duplicarlos en cada `preparacion_*.md` — cada carpeta afectada enlaza a esta
página en vez de repetir el análisis completo.

---

## H1 — Warehouse del Iceberg REST server de Gravitino no apunta a AIStor/MinIO

**Fecha de descubrimiento:** 2026-08-14, durante debugging de FAILs en
`validacion_trino_minio/validate_trino_minio_principal.sh` (Módulos 2/3).

**Severidad:** CRÍTICO — bloquea declarar como válida cualquier integración
de datos (no solo metadata) entre Trino/Flink y AIStor mientras pase por
Gravitino.

**Carpetas afectadas:**
- `validacion_trino_minio/` — Módulo 2/3 no pueden completar pruebas
  funcionales por esta causa raíz (ver Hallazgo local ya documentado en
  `preparacion_integracion_trino_minio.md`, corregido para apuntar acá).
- `validacion_flink_gravitino/` — su Módulo 2 (escritura/lectura Flink↔Gravitino)
  puede dar **falso positivo**: el flujo completo (`CREATE TABLE` /
  `INSERT` / `SELECT`) puede terminar en PASS aun sin este warehouse
  apuntando a S3, porque el backend local (`/tmp`) es un filesystem
  perfectamente funcional dentro del contenedor. Un PASS ahí demuestra que
  Flink habla correctamente el protocolo Iceberg REST — no que los datos
  lleguen al lake real.
- `validacion_trino_gravitino/` — cualquier chequeo que dependa de leer una
  tabla previamente escrita por Flink (o viceversa) puede "funcionar" porque
  ambos comparten el mismo backend en memoria del mismo contenedor Gravitino,
  sin que eso implique persistencia real ni integración con AIStor.

### Evidencia

Archivo `/root/gravitino/conf/gravitino-iceberg-rest-server.conf` dentro del
contenedor `gravitino` (`pbigd-plat-apps01`):

```
# THE CONFIGURATION FOR Iceberg catalog backend
gravitino.iceberg-rest.catalog-backend = memory
gravitino.iceberg-rest.warehouse = /tmp

# THE CONFIGURATION EXAMPLE FOR JDBC CATALOG BACKEND WITH S3 SUPPORT
# gravitino.iceberg-rest.catalog-backend = jdbc
# gravitino.iceberg-rest.jdbc-driver = org.postgresql.Driver
# gravitino.iceberg-rest.uri = jdbc:postgresql://127.0.0.1:5432/postgres
# gravitino.iceberg-rest.jdbc-user = postgres
# gravitino.iceberg-rest.jdbc-initialize = true
# gravitino.iceberg-rest.warehouse = s3://test/my/key/prefix
# gravitino.iceberg-rest.io-impl= org.apache.iceberg.aws.s3.S3FileIO
# gravitino.iceberg-rest.s3-endpoint = http://192.168.215.4:9010
# gravitino.iceberg-rest.s3-region = xxx
```

El bloque comentado es literalmente el ejemplo de referencia de la
documentación pública de Gravitino (nótese `192.168.215.4:9010`, una IP que
no pertenece a este entorno) — nunca fue descomentado ni adaptado con los
valores reales (bucket del warehouse Iceberg, endpoint de MinIO/AIStor,
credenciales).

`podman exec trino sh -c "grep -viE 'secret|access-key' /etc/trino/catalog/iceberg.properties"` confirma además que Trino delega **toda** la
resolución de ubicación de archivos a este servidor REST — no tiene ninguna
config S3 propia (`hive.s3.*`/`fs.native-s3.*`/`s3.*`):

```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.containers.internal:9001/iceberg/
iceberg.rest-catalog.security=NONE
```

Y `SHOW SCHEMAS FROM iceberg` vía Trino solo devuelve `information_schema` y
`system` — sin ningún schema de datos real, consistente con un backend en
memoria que arranca vacío en cada reinicio del contenedor.

### Por qué esto es un hallazgo, no solo un vacío de config

No existe ninguna conexión directa Trino→AIStor independiente de Gravitino
en el entorno tal como está desplegado. Todo el acceso a datos —no solo
metadata— pasa por el Iceberg REST server de Gravitino, y ese servidor no
tiene AIStor configurado en absoluto todavía.

### Clasificación (según `informes/estandar_informes_validacion.md` §1.4)

**Hallazgo de infraestructura real** — no es un defecto de ningún script de
validación ni una decisión de arquitectura deliberada documentada en algún
otro lado. Los tres scripts (`validate_trino_minio_principal.sh`,
`validate_flink_gravitino_principal.sh`, y por extensión
`validate_trino_gravitino_principal.sh`) se comportaron correctamente dado lo
que encontraron; el sistema es el que no está completo.

### Acción requerida (OBLIGATORIA antes de producción)

1. Confirmar con el equipo de infraestructura el bucket/prefijo real del
   warehouse Iceberg en AIStor (p. ej. `s3://<bucket-real>/warehouse`) y el
   endpoint S3 a usar (recomendado: el VIP HAProxy `itaca.jardinazuayo.fin.ec`,
   ya confirmado real en Principal, no un nodo individual).
2. Migrar `gravitino.iceberg-rest.catalog-backend` de `memory` a un backend
   persistente (`jdbc`, con Postgres/H2 real, no el ejemplo con
   `127.0.0.1:5432`) — de lo contrario el catálogo entero (no solo los
   archivos) se pierde en cada reinicio del contenedor `gravitino`.
3. Configurar `gravitino.iceberg-rest.warehouse=s3://...`,
   `gravitino.iceberg-rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO`,
   `gravitino.iceberg-rest.s3-endpoint=<endpoint real>` y las credenciales S3
   correspondientes (coordinar least-privilege con
   `validacion_trino_minio/guia_pasos_manuales_trino_minio.md` Módulo 4).
4. Volver a correr `validate_trino_minio_principal.sh` (Módulo 1.5, ya
   implementado) y `validate_flink_gravitino_principal.sh` (Módulo 1.7, ya
   implementado) — ambos ahora detectan automáticamente este caso y marcan
   FAIL explícito si el warehouse sigue sin ser S3, en vez de dejar pasar un
   PASS engañoso en el Módulo 2 funcional.

### Chequeo automatizado agregado (2026-08-14)

**Nota de diseño (corregida tras revisión):** la primera versión de este
chequeo se implementó dentro de `validate_trino_minio_principal.sh`, pero eso
contradice el alcance que ese mismo script declara en su encabezado ("NO
valida la conexión Trino → Gravitino, eso es `validacion_trino_gravitino/`").
Se corrigió: cada script inspecciona solo lo que le corresponde por su propio
alcance declarado.

- `validacion_trino_minio/validate_trino_minio_principal.sh` — Módulo 1.5:
  se limita a constatar que, si `iceberg.catalog.type=rest`, Trino no tiene
  ninguna config S3 propia (consistente con los checks 1.1-1.4). Reporta
  **FAIL** explicando que la premisa de conexión directa no se cumple, y
  redirige a `validacion_trino_gravitino/` para la confirmación real — **no**
  toca el contenedor `gravitino`.
- `validacion_trino_gravitino/validate_trino_gravitino_principal.sh` — Módulo
  1.5 (nuevo, antes del Módulo 2 funcional): este sí es dueño legítimo de la
  relación con Gravitino (ya la valida en los Módulos 1.1-1.4). Resuelve el
  contenedor Gravitino local y confirma si `gravitino.iceberg-rest.warehouse`
  empieza con `s3://`/`s3a://`. Si no, **FAIL** explícito citando este
  documento, y advertencia repetida en el resumen final.
- `validacion_flink_gravitino/validate_flink_gravitino_principal.sh` —
  Módulo 1.7 (antes del Módulo 2 funcional): mismo chequeo. Se mantiene
  dentro de este script porque está en su alcance declarado (Flink habla
  directamente con el mismo endpoint REST de Gravitino que se audita); no es
  el mismo caso que `trino_minio`, que documenta la exclusión explícita de
  Gravitino en su propio encabezado.

Los tres chequeos degradan a WARN (no FAIL) si no se puede leer la config de
Gravitino (contenedor no local, archivo no encontrado en las rutas
candidatas) — nunca asumen silenciosamente que está bien.

---

## Cómo agregar un nuevo hallazgo transversal

1. Confirmar que el hallazgo afecta genuinamente a 2+ carpetas de
   `validaciones/`, no solo una — si es de una sola carpeta, va en su propio
   `preparacion_*.md`.
2. Sección `## H<N> — <título>` con: fecha, severidad, carpetas afectadas,
   evidencia cruda (comandos + salida real, sin secretos), por qué contradice
   o no documentación previa, clasificación según
   `informes/estandar_informes_validacion.md` §1.4, y acción requerida.
3. Enlazar desde el `preparacion_*.md` de cada carpeta afectada en vez de
   copiar el análisis.
