# Guía de preparación — Validación de la integración Trino ↔ Gravitino

El script `validate_trino_gravitino_principal.sh` corre en `pbigd-plat-apps01`
(nodo que coloca Trino Coordinator, Flink JobManager y Gravitino) y valida
que el catálogo de metadata compartido del lakehouse (Iceberg REST Catalog,
implementado por Gravitino) esté correctamente conectado desde Trino, y que
sea realmente el **mismo** catálogo que usa Flink — no dos configuraciones
independientes que coincidentemente no fallan por separado.

## Por qué esta validación es distinta a Trino↔MinIO

Un correo interno del equipo de arquitectura
(`context/Contexto-extra-correos.md`, 2026-08-06) confirma que "Gravitino
solo maneja metadata (catálogo de tablas), nunca los archivos de datos -por
eso Flink y Trino tienen una conexión hacia Gravitino (metadata) y otra
directa hacia AIStor (archivos), sin pasar por Gravitino." Trino tiene dos
conexiones independientes:

- **Trino → Gravitino** (metadata/catálogo de tablas) — este script.
- **Trino → AIStor/MinIO** (archivos de datos Iceberg) —
  `validacion_trino_minio/`, ya existente.

Ninguno de los dos reemplaza al otro; ambos deben pasar para dar por
funcional el acceso de Trino al lakehouse completo.

## No existe un script hermano exacto

A diferencia de Trino↔MinIO (que sí tuvo a Flink↔MinIO como referencia
directa), la integración Flink↔Gravitino **no tiene validación propia
todavía** — hueco de cobertura detectado en una revisión anterior del
proyecto. Además, `validacion_flink_minio/` dejó Iceberg/Gravitino
explícitamente fuera de alcance: su Módulo 3 solo ejercita un job
`datagen → print` efímero, sin catálogo, para forzar un checkpoint. No dejó
ninguna tabla Iceberg real ni ningún fixture reutilizable.

Por eso este script:

- No puede apoyarse en una tabla de prueba ya creada por Flink (a diferencia
  de `validacion_trino_minio/`, donde `--test-table` podía apuntar a una
  tabla real escrita previamente).
- Combina patrones de tres scripts ya probados en vez de copiar un caso 1:1:
  - `validate_trino_minio_principal.sh` — descubrimiento del `.properties`
    del catálogo Iceberg de Trino, helper `run_statement()` contra
    `/v1/statement`.
  - `validate_gravitino_principal.sh` — llamadas REST a Gravitino, hallazgo
    de HA "de papel", ciclo crear/eliminar metadata de prueba.
  - `validate_kafka_flink_principal_flink.sh` — envío de jobs SQL a Flink vía
    `podman exec` + `sql-client.sh` en background, sin depender de SSH entre
    nodos.
- Por ser, hasta donde se pudo confirmar en el repo, la primera prueba
  funcional real de escritura/lectura sobre el catálogo REST compartido, el
  Módulo 2 (funcional cruzado) es el de mayor valor de todo el script: si
  Flink y Trino no ven exactamente los mismos objetos en el mismo catálogo,
  el "único punto de acceso a los datos del lake" que describe la
  arquitectura del proyecto no está realmente unificado.

## Antes de correr este script

1. `validacion_trino/validate_trino_principal.sh` y
   `validacion_gravitino/validate_gravitino_principal.sh` ya ejecutados y
   sin FAIL bloqueantes — este script asume que Trino y Gravitino, como
   tecnologías en sí mismas, ya están sanos.
2. Ejecutar desde `pbigd-plat-apps01` (donde colocan los tres contenedores
   según el plan de implementación), o pasar `--coordinator-host`,
   `--gravitino-host` y `--flink-jobmanager-host` si alguno vive en otro
   nodo — sin acceso SSH entre nodos, cualquier contenedor no local degrada
   su chequeo dependiente a WARN con instrucción manual, nunca a un FAIL
   silencioso.
3. `python3` disponible en el nodo de ejecución (parseo de JSON de las tres
   REST API: Trino, Gravitino y Flink).

## Nombre real del catálogo/metalake — vacío documental

Igual que ya se documentó para el catálogo S3 de Trino en
`validacion_trino_minio/preparacion_integracion_trino_minio.md`, **ningún
archivo del repo** (plan de implementación, correos, `validacion_gravitino/`,
`validacion_trino_minio/`) cita el nombre real del catálogo/metalake Iceberg
compartido, ni el nombre exacto de la propiedad usada para la URI del
catálogo REST en Trino (`iceberg.rest-catalog.uri` es el nombre más común en
Trino 4xx, pero no está confirmado en este entorno). El script **descubre**
ambas cosas en runtime (Módulo 1.1), probando las rutas y nombres de
propiedad candidatos conocidos, y reporta como hallazgo si no encuentra
ninguno — no asume nada de antemano.

## DR — heredado, no re-investigado

Este script **no genera** `validate_trino_gravitino_dr.sh`. La conclusión se
hereda directamente de los dos scripts base, sin volver a investigar:

- `validate_trino_principal.sh`: Trino está documentado como "No aplica en
  DR por diseño", **pendiente de confirmación en sitio**
  (`podman ps | grep -i trino` en los 3 nodos `-cont`).
- `validate_gravitino_principal.sh`: Gravitino queda como **"PREGUNTA
  ABIERTA"** sin resolver — ni siquiera llega a clasificarse como "no
  aplica". Es una laguna documental más seria que la de Trino, porque el
  Data Lake (MinIO+Iceberg) sí tiene presencia dimensionada en el
  datacenter alterno, pero su catálogo de metadata no aparece en ningún
  lado.

Ninguno de los dos componentes tiene su despliegue en DR confirmado como
existente. Por lo tanto, la integración entre ambos **tampoco aplica en DR**
hasta que ambas preguntas se resuelvan. Si en el futuro se confirma que
ambos (Trino y Gravitino) están desplegados en DR, corresponde generar
`validate_trino_gravitino_dr.sh` como entrega posterior, replicando el
patrón Principal/DR ya usado en Kafka/Flink/MinIO.

## Fuera de alcance de este script

- Conexión directa Trino → AIStor/MinIO para archivos de datos — ver
  `validacion_trino_minio/`.
- Salud de Trino, Gravitino o Flink como tecnologías en sí mismas — ver sus
  scripts respectivos (`validacion_trino/`, `validacion_gravitino/`,
  `validacion_flink/`).
- Integración Flink↔Gravitino como validación independiente y completa (este
  script solo la ejercita lo mínimo necesario para la prueba cruzada del
  Módulo 2, no reemplaza una validación dedicada de Flink↔Gravitino si se
  decide crear una en el futuro).
- DR (ver arriba).

## Hallazgos documentales a reportar en el informe (no resueltos aquí)

1. Ni el nombre real del catálogo/metalake ni el de la propiedad de URI del
   catálogo REST en Trino están confirmados en el repo — el script los
   descubre en runtime; si el Módulo 1.1 no encuentra ninguno, es un
   hallazgo real a reportar, no un bug del script.
2. Estado de DR de Trino y Gravitino heredado como no confirmado (ver
   sección "DR" arriba) — reportar como una única conclusión heredada, sin
   presentarla como una nueva investigación.
3. Si el Módulo 2 detecta que Flink y Trino no ven el mismo objeto en el
   catálogo compartido (tabla creada por uno no visible desde el otro, o
   visible pero con datos/conteo distinto), es el **hallazgo prioritario**
   de todo el informe — evidencia directa de que el catálogo compartido no
   está realmente unificado, contradiciendo el diseño de "único punto de
   acceso a los datos del lake" que describe la arquitectura del proyecto.
