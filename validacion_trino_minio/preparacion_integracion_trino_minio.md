# Guía de preparación — Validación de integración Trino → MinIO/AIStor (lectura/escritura de datos Iceberg)

## Por qué esta validación es distinta a Trino↔Gravitino

`validacion_trino/validate_trino_principal.sh` valida Trino como tecnología en
sí misma (coordinator + workers) y termina en `SHOW CATALOGS`, dejando
explícito que "el contenido de catálogos NO se valida aquí — ver
`validacion_trino_minio`/`validacion_trino_gravitino`". Ninguno de los dos
existía en el repositorio antes de esta entrega.

Un correo interno del equipo de arquitectura (`context/Contexto-extra-correos.md`,
entrada del 2026-08-06) confirma por qué **no basta con un solo script** para
"la integración de Trino con el catálogo":

> "Gravitino solo maneja metadata (catálogo de tablas), nunca los archivos de
> datos -por eso Flink y Trino tienen una conexión hacia Gravitino (metadata)
> y otra directa hacia AIStor (archivos), sin pasar por Gravitino."

Trino tiene **dos conexiones independientes**, cada una con su propio modo de
fallo:

- **Trino → Gravitino** (metadata/catálogo de tablas) — `validacion_trino_gravitino/`, aún no existe.
- **Trino → AIStor/MinIO** (archivos de datos Iceberg reales) — este script, `validate_trino_minio_principal.sh`.

Es el mismo criterio arquitectónico que ya separó `validacion_flink_minio/`
(checkpoints de Flink sobre MinIO) de la integración Flink↔Gravitino, y ambos
comparten el mismo endpoint S3 físico — de ahí que este script reutilice
prácticamente verbatim la lógica de comparación de endpoint de
`validate_flink_minio_principal.sh`.

---

## Prerrequisito 1 — Clúster Trino sano

Correr primero `validacion_trino/validate_trino_principal.sh`. Este script
asume que el coordinator y los 3 workers ya están validados como sanos
(contenedores UP, REST API respondiendo, `SELECT 1` aceptado). Si ese script
falla, los resultados de este son ruido — no aísla si el problema es Trino en
sí o específicamente su conexión a MinIO.

## Prerrequisito 2 — MinIO sano

Correr primero `validacion_minio/validate_minio_*.sh`. Igual razón que el
Prerrequisito 1: este script asume que MinIO ya está confirmado como sano de
forma independiente.

## Prerrequisito 3 — Catálogo Iceberg de Trino: nombre y ruta NO documentados

**Ni `validacion_gravitino/` ni el plan de implementación citan** el nombre
del catálogo Iceberg de Trino, la ruta exacta de su archivo `.properties`, ni
si el conector S3 en uso es el legacy basado en Hive (`hive.s3.*`) o el
filesystem nativo S3 de Trino 481 (`fs.native-s3.*`, o simplemente `s3.*`
según la implementación específica). Esto es un vacío documental real, no un
descuido de esta guía.

Por eso `validate_trino_minio_principal.sh` **descubre esto en runtime**
(Módulo 1.0), en vez de asumir un valor:

1. Prueba las rutas candidatas conocidas de Trino: `${TRINO_HOME}/etc/catalog/*.properties`
   y `/etc/trino/catalog/*.properties`.
2. Para cada `.properties` encontrado, filtra por `connector.name=iceberg`.
3. Para las claves de endpoint/path-style/credenciales, prueba las 3 familias
   de prefijo conocidas (`fs.native-s3.*`, `hive.s3.*`, `s3.*`) y reporta por
   `info` cuál encontró realmente — no lo asume de antemano.

**Acción si el catálogo no se encuentra o no tiene `connector.name=iceberg`**:
es un hallazgo real (FAIL), no un problema del script — reportarlo al equipo
de infraestructura para confirmar dónde vive la config real.

## Prerrequisito 4 — Endpoint MinIO a validar: nodo individual vs. VIP/DNS de HAProxy

Mismo mecanismo que `validate_flink_minio_principal.sh`: por defecto el
script valida contra un nodo individual (`http://pbigd-stg01:9000`). El VIP
DNS de HAProxy delante de MinIO (`itaca.jardinazuayo.fin.ec`) **ya está
confirmado como real en Principal** (resuelve a `172.17.210.62`, ver
`logs/haproxy_minio/` y `validacion_haproxy_minio/`), pero no se hardcodea
aquí como default silencioso — pasarlo explícitamente:

```bash
./validate_trino_minio_principal.sh --vip <IP_DE_LA_VIP>
# o
./validate_trino_minio_principal.sh --dns itaca.jardinazuayo.fin.ec
```

El correo del 2026-08-01 (`context/Contexto-extra-correos.md`) es explícito
en que los 4 componentes deben compartir exactamente el mismo endpoint:

> "Los 4 componentes (Flink, Iceberg, Trino y Gravitino) deben apuntar
> exactamente al mismo endpoint (IP virtual o nombre DNS interno) y tener
> habilitado explícitamente el acceso 'path-style'..."

Por eso el Módulo 1.2 de este script imprime una nota (`info`) recordando
cruzar manualmente el `endpoint` detectado aquí contra el que reporta
`validate_flink_minio_principal.sh` — ambos deben coincidir exactamente. El
script no lee la config de Flink directamente (eso es responsabilidad de su
propio script), solo señala dónde cruzar la evidencia.

**Si NO se pasa `--vip`/`--dns`**, el script sigue funcionando contra el nodo
individual, pero lo marca con un WARN explícito en el encabezado, igual
criterio que Flink.

## Prerrequisito 5 — Tabla Iceberg real para la prueba de lectura

El script no asume ninguna tabla Iceberg existente — no hay ninguna
confirmada en el repo como "creada por Flink" con nombre conocido. Pasar
`--test-table catalogo.schema.tabla` (idealmente una tabla que Flink ya haya
escrito, para validar el ciclo completo escritura-por-Flink /
lectura-por-Trino). Sin este flag, el Módulo 2 (lectura) queda en WARN, no en
FAIL — es un vacío de dato de entrada, no un fallo de infraestructura.

---

## Procedimiento — DC Principal

```bash
./validate_trino_minio_principal.sh \
  [--coordinator-host host] [--vip IP] [--dns nombre] [--minio-endpoint url] \
  [--test-table catalogo.schema.tabla] [--expected-count N] \
  [--skip-write-test] [--write-test-schema catalogo.schema]
```

1. **Módulo 1** — descubre el catálogo Iceberg (Prerrequisito 3), confirma
   filesystem S3 habilitado, compara el endpoint contra el VIP/DNS esperado
   (Prerrequisito 4), confirma `path-style-access` y presencia de
   credenciales (nunca su valor).
2. **Módulo 2** — `SELECT count(*)` contra `--test-table` vía REST API
   (`/v1/statement`), confirma que Trino lee datos reales desde MinIO.
3. **Módulo 3** — NO asume que Trino es de solo lectura pese a que el plan lo
   describe como "acceso ocasional de Power BI" (ver Hallazgo 1 abajo):
   intenta un `CREATE TABLE ... AS SELECT` de prueba en `--write-test-schema`.
   Si Trino rechaza por permisos, lo documenta como diseño esperado (INFO, no
   FAIL). Si el catálogo sí permite escritura, confirma que el archivo llegó
   al bucket de warehouse vía HTTP (mismo truco `curl` sin `mc` que usa
   Flink), y limpia la tabla de prueba al finalizar.

## DR — no se genera `validate_trino_minio_dr.sh`

`validacion_trino/validate_trino_principal.sh` (cabecera, líneas 23-33)
concluye:

> "Trino NO figura en la tabla de asignación de recursos del datacenter
> alterno del plan de implementación (a diferencia de Kafka/Flink). [...]
> nada confirma que Trino esté desplegado ahí [...]. Por eso NO se genera un
> `validate_trino_dr.sh` en esta entrega — se documenta como 'No aplica en DR
> por diseño' [...]. Antes de cerrar el tema definitivamente, confirmar en
> sitio con: `podman ps --format '{{.Names}}\t{{.Image}}' | grep -i trino` en
> los 3 nodos -cont."

Se hereda esa conclusión sin volver a investigarla: si Trino no está
confirmado en DR, su integración con MinIO tampoco aplica ahí. Si en una
futura visita de sitio se confirma que Trino sí corre en DR, este script
puede adaptarse a `validate_trino_minio_dr.sh` siguiendo el mismo patrón que
`validate_flink_minio_dr.sh` (topología reducida: 2 workers `-cont`, mismo VIP
DNS `itaca.jardinazuayo.fin.ec` pero resolviendo a `172.17.210.182`).

---

## Hallazgos documentales a reportar en el informe (no resueltos aquí)

| # | Hallazgo | Fuente | Nota |
|---|---|---|---|
| 1 | Tensión "Trino opcional/aplazable" vs. "único punto de acceso" de PowerBI/app | Plan de implementación línea 157 ("Trino se puede aplazar") vs. correo 2026-08-06 línea 79 ("consultan exclusivamente... único punto de acceso") | Ambas afirmaciones no pueden ser ciertas simultáneamente si Trino se aplaza. No se decide unilateralmente aquí cuál prevalece. |
| 2 | Versión Trino: 479 (plan) vs. 481 (`versiones-finales.md`) | Ya documentado en `validate_trino_principal.sh` | Se hereda la misma nota, no se re-resuelve. |
| 3 | VIP de MinIO para Trino: recomendado, no necesariamente confirmado como implementado | Correo 2026-08-01 (recomendación) vs. verificación real solo posible en el Módulo 1.2 de este script | El check 1.2 es el que efectivamente lo confirma o refuta en sitio — no asumir que la recomendación se aplicó. |
| 4 | El catálogo Iceberg de Trino en este entorno es `iceberg.catalog.type=rest` (delega a Gravitino), no config S3 propia — la premisa de conexión directa Trino→AIStor no se cumple | Confirmado en sitio 2026-08-14 (`iceberg.properties`) | **CRÍTICO, transversal a otras carpetas.** El Módulo 1.5 de este script solo confirma que Trino no tiene config S3 propia (fuera de su alcance tocar Gravitino, ver su propio encabezado). La confirmación del warehouse real de Gravitino (`memory`/`/tmp`, no AIStor) vive en `validacion_trino_gravitino/validate_trino_gravitino_principal.sh` (Módulo 1.5) y en `hallazgos_transversales.md` (H1) — no reanalizar acá. |

---

## Fuera de alcance de este script

- El contenido/permisos del catálogo REST de Gravitino (metadata) — ver
  `validacion_trino_gravitino/` (aún no existe).
- La salud del clúster Trino en sí, o de MinIO en sí — usar
  `validacion_trino/` y `validacion_minio/` antes de esta validación de
  integración.
- La instalación/configuración del catálogo Iceberg de Trino, o la corrección
  del `.properties` — este script **verifica o reporta el síntoma**, no lo
  configura.
- DR (ver sección arriba).
- El alcance de las credenciales S3 de Trino en MinIO (least-privilege) —
  requiere consola/CLI de administración de MinIO, ver
  `guia_pasos_manuales_trino_minio.md`, Módulo 4.
