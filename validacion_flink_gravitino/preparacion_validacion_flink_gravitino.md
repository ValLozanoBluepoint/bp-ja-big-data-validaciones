# Guía de preparación — Validación de la integración Flink ↔ Gravitino

El script `validate_flink_gravitino_principal.sh` corre en `pbigd-plat-apps01`
(nodo que coloca Flink JobManager y Gravitino) y valida que el catálogo de
metadata compartido del lakehouse (Iceberg REST Catalog, implementado por
Gravitino) esté correctamente conectado desde Flink — de forma **standalone,
sin pasar por Trino**.

## Por qué esta validación es distinta a Trino↔Gravitino

`validacion_trino_gravitino/validate_trino_gravitino_principal.sh` documenta
explícitamente, en su propio encabezado, que "la integración Flink↔Gravitino
tampoco tiene validación propia todavía" — su Módulo 2 solo la ejercita como
una prueba cruzada (confirmar que Trino y Flink comparten el mismo catálogo),
no como una validación dedicada. Además, ese Módulo 2 depende de que Trino ya
haya descubierto y confirmado la URI del catálogo REST en su propia config;
si algún día se corre este script sin `validacion_trino_gravitino/`, o Trino
cambia de configuración, no había ninguna validación que cubriera solo el
lado Flink.

Este script cierra ese hueco: Flink usa el catálogo REST de Gravitino
**directamente** (`CREATE CATALOG ... 'uri' = 'http://gravitino:9001/iceberg/v1'`),
sin que Trino esté involucrado en ningún paso.

## Por qué es un script separado de Flink↔MinIO

El mismo correo interno del equipo de arquitectura
(`context/Contexto-extra-correos.md`, 2026-08-06) que fundamenta
`validacion_trino_gravitino/` aplica aquí: "Gravitino solo maneja metadata
(catálogo de tablas), nunca los archivos de datos -por eso Flink y Trino
tienen una conexión hacia Gravitino (metadata) y otra directa hacia AIStor
(archivos), sin pasar por Gravitino." Flink tiene dos conexiones
independientes:

- **Flink → Gravitino** (metadata/catálogo de tablas) — este script.
- **Flink → AIStor/MinIO** (checkpoints/savepoints) —
  `validacion_flink_minio/`, ya existente. Ese script deja Iceberg/Gravitino
  explícitamente fuera de alcance: su Módulo 3 solo ejercita un job
  `datagen → print` efímero, sin catálogo, para forzar un checkpoint.

Ninguno de los dos reemplaza al otro.

## Requisito confirmado: JAR iceberg-flink-runtime

El mismo correo (línea 69) confirma que "Flink JobManager (platformapps-1) y
TaskManagers (cmp-1/2/3)... sí requieren el JAR
`iceberg-flink-runtime-1.20-1.9.2.jar` en el directorio `lib/` de Flink" —
sin este JAR, un `CREATE CATALOG` de tipo `iceberg` falla en Flink SQL. El
Módulo 1 del script lo verifica explícitamente en el JobManager
(`find /opt/flink/lib -iname 'iceberg-flink-runtime*.jar'`); no se verifica en
los TaskManagers porque el Módulo 2 solo ejecuta el `INSERT`/`SELECT` de
prueba desde el JobManager vía `sql-client.sh` — si el job real de producción
requiere que los TaskManagers también tengan el JAR, confirmarlo por
separado (sin SSH, no automatizable desde este script; mismo criterio que el
Módulo 2 de `validacion_flink_minio/`).

## Antes de correr este script

1. `validacion_flink/validate_flink_principal.sh` y
   `validacion_gravitino/validate_gravitino_principal.sh` ya ejecutados y sin
   FAIL bloqueantes — este script asume que Flink y Gravitino, como
   tecnologías en sí mismas, ya están sanos.
2. Ejecutar desde `pbigd-plat-apps01` (donde colocan ambos contenedores según
   el plan de implementación), o pasar `--gravitino-host` y
   `--flink-jobmanager-host` si alguno vive en otro nodo — sin acceso SSH
   entre nodos, cualquier contenedor no local degrada su chequeo dependiente
   a WARN con instrucción manual, nunca a un FAIL silencioso.
3. `python3` disponible en el nodo de ejecución (parseo de JSON de las REST
   API de Gravitino y Flink).

## Nombre de catálogo/schema — supuesto documentado

A diferencia de `validate_trino_gravitino_principal.sh` (que descubre el
nombre real del catálogo Iceberg leyendo la config `.properties` ya existente
de Trino), este script **no tiene ningún archivo de config estático del que
descubrir un nombre**: el catálogo de Flink se define enteramente vía
`CREATE CATALOG` en el SQL de sesión (ver Módulo 1.6 del script). Por eso usa
un nombre de catálogo/schema arbitrario (`bluepoint_fg` /
`bluepoint_validacion_fg`) — Gravitino no exige que el nombre del catálogo de
Flink coincida con un metalake pre-existente para el Iceberg REST Catalog
genérico. Si en producción Flink usa un nombre de catálogo específico ya
acordado con el equipo, ajustar `TEST_CATALOG`/`TEST_SCHEMA` en el script
antes de correrlo, o pasarlos como variables de entorno si se decide
parametrizarlos en una iteración futura.

## DR — asimetría, no heredado 1:1

Este script **no genera** `validate_flink_gravitino_dr.sh`. A diferencia de
`validate_trino_gravitino_principal.sh` (donde ambos componentes base estaban
sin confirmar en DR), aquí hay una asimetría real:

- `validate_flink_dr.sh` **sí existe** y documenta una topología DR completa:
  JobManager en `pbigd-plat-apps01-cont`, TaskManagers en
  `pbigd-proc01-cont`/`pbigd-proc02-cont`. Flink DR está confirmado
  desplegado.
- `validate_gravitino_principal.sh` sigue con Gravitino como **"PREGUNTA
  ABIERTA"** en DR — no aparece en la tabla de asignación de recursos del
  datacenter alterno del plan de implementación, a diferencia del Data Lake
  (MinIO+Iceberg), que sí tiene presencia dimensionada ahí.

Es decir: **Gravitino, no Flink, es el componente que bloquea** esta
integración en DR. Si en el futuro se confirma que Gravitino está
desplegado en DR, corresponde generar `validate_flink_gravitino_dr.sh`
replicando el patrón Principal/DR ya usado en Kafka/Flink/MinIO —
aprovechando que el lado Flink de esa validación DR ya existe.

## Fuera de alcance de este script

- Conexión directa Flink → AIStor/MinIO para checkpoints/savepoints — ver
  `validacion_flink_minio/`.
- Integración Trino↔Gravitino — ver `validacion_trino_gravitino/` (que
  también ejercita parcialmente Flink↔Gravitino como prueba cruzada, pero no
  reemplaza esta validación dedicada).
- Salud de Flink o Gravitino como tecnologías en sí mismas — ver sus scripts
  respectivos (`validacion_flink/`, `validacion_gravitino/`).
- DR (ver arriba).

## Hallazgos documentales a reportar en el informe (no resueltos aquí)

1. Ningún archivo del repo confirma el nombre real del catálogo/metalake que
   Flink debería usar en producción, ni si Gravitino exige un metalake
   pre-existente para el nombre de catálogo elegido — el script usa un
   nombre arbitrario de prueba (ver sección arriba); si el Módulo 1.3 no
   encuentra el endpoint del Iceberg REST Catalog, es un hallazgo real a
   reportar, no un bug del script.
2. Asimetría de DR entre Flink (confirmado) y Gravitino (pregunta abierta) —
   reportar como hallazgo distinto al ya reportado en
   `validacion_trino_gravitino/` (donde ambos lados estaban sin confirmar),
   no como el mismo hallazgo repetido.
3. Si el Módulo 2 detecta que Flink no puede leer de vuelta la tabla que él
   mismo escribió sobre el catálogo Gravitino, es el **hallazgo prioritario**
   de todo el informe — evidencia directa de que la integración de metadata
   no es funcional, no solo un problema de configuración estática.
4. **CRÍTICO, transversal a otras carpetas.** Un PASS del Módulo 2 (Flink
   escribe/lee de vuelta sobre el catálogo Gravitino) no es por sí solo
   evidencia de integración con AIStor: si el warehouse del iceberg-rest-server
   de Gravitino no apunta a S3 (confirmado en sitio 2026-08-14 con
   `catalog-backend=memory` + `warehouse=/tmp`), Flink puede escribir/leer
   perfectamente bien contra disco local efímero del contenedor sin tocar el
   lake real. Ver `hallazgos_transversales.md` (H1) en la raíz del repo — el
   Módulo 1.7 de este script ahora detecta esto automáticamente antes de
   correr el Módulo 2, y repite la advertencia en el resumen final si aplica.
