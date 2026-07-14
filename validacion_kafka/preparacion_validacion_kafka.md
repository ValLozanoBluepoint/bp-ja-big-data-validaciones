# Guía de validación — Kafka (Principal y DR)

Los scripts `validate_kafka_principal.sh` y `validate_kafka_dr.sh` corren en cada nodo del cluster Kafka (KRaft, sin Zookeeper) y revisan, de punta a punta, si ese nodo está listo para operar como backbone de eventos del proyecto Big Data. Cada sección responde a una pregunta concreta sobre el estado del nodo.

## Sección 1 — Sistema Operativo y Runtime

**Para qué:** confirma que la base sobre la que corre Kafka es la correcta antes de mirar nada del propio Kafka.

- **SO Rocky Linux 10.x**: si el sistema operativo no es el esperado, cualquier otro resultado puede ser inconsistente con el diseño homologado.
- **Kernel**: informativo, útil para comparar entre nodos si algo falla de forma distinta en uno solo.
- **Podman ≥5.x**: Kafka corre containerizado; una versión vieja de Podman puede no soportar features que el despliegue necesita.
- **systemd activo**: systemd es quien debe levantar y supervisar el contenedor; si no está "running", nada garantiza que el contenedor sobreviva a un reinicio.
- **NTP/chrony sincronizado**: este es el chequeo más crítico de la sección. El quórum KRaft usa Raft para elegir controller/leader, y Raft es sensible a desfases de reloj entre nodos — un reloj desincronizado puede causar timeouts o elecciones de líder inestables.
- **Naming convention del hostname** (`pbigd-kaf01/02/03` en Principal, `-cont` en DR): valida que el nodo está correctamente identificado según la convención del proyecto, para evitar configurarlo por error con el rol de otro nodo.

## Sección 2 — Directorios y persistencia

**Para qué:** confirma que los datos de Kafka (los logs de los topics, que son el activo más importante) se están guardando donde y cómo deben.

- **Existencia de `/opt/kafka`, `/data/kafka`, `/var/log/kafka`**: si faltan, Kafka no tiene dónde vivir o dónde persistir.
- **`/data/kafka` no debe ser overlay**: el filesystem overlay (el que usa Podman por defecto para la capa del contenedor) no es apto para persistir datos — si el contenedor se recrea, los datos se pierden. Se exige que sea un bind mount sobre una partición dedicada.
- **Partición `/data` independiente**: separa los datos de Kafka del disco del sistema operativo, para que un log que crece sin control no llene el disco raíz y tumbe el nodo entero.

## Sección 3 — Servicio systemd y contenedor Podman

**Para qué:** confirma que el propio proceso de Kafka está corriendo y que va a seguir corriendo solo, sin intervención manual.

- **Unidad systemd activa y habilitada**: "activa" dice que está corriendo ahora; "habilitada" dice que arrancará solo tras un reinicio del servidor — sin esto, un reboot dejaría el nodo caído hasta que alguien lo levante a mano.
- **Contenedor Podman en estado "Up"**: verificación directa de que el proceso Kafka existe como contenedor y no se cayó.
- **Naming convention del contenedor** (`kafka-<índice>`): evita ambigüedad si hay varios contenedores corriendo en el mismo host.
- **Política de reinicio `always`**: si Kafka se cae por cualquier motivo (OOM, panic, etc.), el contenedor debe reiniciarse solo; sin esta política, una caída se vuelve una caída indefinida.

## Sección 4 — Conectividad de red

**Para qué:** confirma que Kafka puede hablar tanto con los clientes (productores/consumidores) como con los otros nodos del propio cluster.

- **Puerto 9092 (broker/client)**: es el puerto por el que las aplicaciones producen y consumen mensajes. Si no escucha, Kafka está efectivamente inutilizable aunque el proceso esté "up".
- **Puerto 9093 (controller KRaft)**: es el canal interno por el que los nodos coordinan el quórum de metadata (reemplaza a Zookeeper). Si no escucha, el nodo no puede participar en las decisiones de cluster (elección de leader, cambios de metadata).
- **Ping + TCP entre los 3 nodos**: valida que no hay un problema de red/firewall/DNS que aislaría a un nodo del resto del cluster — algo que un chequeo local (puerto escuchando) no detecta.

## Sección 5 — Validación funcional (KRaft + topics)

**Para qué:** es la prueba de que Kafka no solo "está corriendo", sino que efectivamente funciona como se espera.

- **Confirmar ausencia de Zookeeper (puerto 2181 y proceso)**: el diseño usa KRaft explícitamente para eliminar la dependencia de Zookeeper; si apareciera Zookeeper, significaría que el despliegue no sigue el diseño acordado.
- **Estado del quórum de metadata** (`kafka-metadata-quorum.sh`): confirma que los controllers realmente eligieron un líder y están de acuerdo sobre el estado del cluster — es el corazón de KRaft.
- **Brokers registrados (≥3)**: confirma que los 3 nodos se ven entre sí como parte del mismo cluster, no como 3 instancias aisladas.
- **Ciclo completo de topic de prueba** (crear → producir → consumir → verificar ISR → borrar): es la prueba end-to-end real — no basta con que el broker responda, hay que probar que un mensaje efectivamente puede escribirse, replicarse y leerse de vuelta. El chequeo de ISR (In-Sync Replicas) confirma que las 3 réplicas del topic de prueba están sincronizadas, no solo que existen.

## Sección 6 — Resiliencia del cluster

**Para qué:** mira el cluster como un todo, no nodo por nodo, para detectar degradación que no es evidente desde un solo servidor.

- **Particiones sub-replicadas**: si una partición tiene menos réplicas sincronizadas de las que debería, el cluster está en riesgo de perder datos ante la caída de un nodo adicional — aunque hoy "funcione".
- **Quórum de brokers (3/3)**: en Principal y en DR por igual, se exige el cluster completo. Es la aplicación práctica de la decisión de diseño: Kafka no reduce capacidad en DR porque es el punto de entrada de todo el flujo de datos (a diferencia de Flink/MinIO DR, que sí toleran operar con menos nodos).

## Sección 7 — Logs y observabilidad

**Para qué:** confirma que, si algo falla en producción, va a quedar rastro para diagnosticarlo.

- **Logs en `/var/log/kafka`**: sin logs, un incidente no se puede investigar después de que pasó.
- **Errores recientes en el contenedor** (ERROR/Exception/FATAL): una señal temprana de que algo anda mal aunque el broker siga "up".
- **Grafana Alloy y node-exporter**: son los agentes que alimentan el monitoreo/alertas del proyecto; sin ellos, un problema puede pasar desapercibido hasta que ya afectó a los usuarios.

## Sección 8 — Verificación de versión

**Para qué:** confirma que el nodo corre exactamente la versión homologada del proyecto (Kafka 4.0.1 sobre JDK 25), evitando incompatibilidades sutiles entre nodos que corrieran versiones distintas (por ejemplo, en un despliegue hecho a mano o parcialmente actualizado).

---

## Diferencias del script DR frente al Principal

El diseño de Bluepoint establece que **Kafka no reduce capacidad en el datacenter alterno** — a diferencia de Flink/MinIO DR, que sí aceptan operar con menos nodos. Esto se traduce en:

- Fallas que en Principal se reportan como advertencia (`warn`) — ping fallido, TCP no alcanzable, unidad systemd no habilitada, política de reinicio distinta a `always` — en DR se reportan como error (`fail`), porque no hay margen: el cluster DR debe estar 100% listo para asumir tráfico real en contingencia.
- Se valida que los recursos del nodo (RAM/vCPU) sean equivalentes a los de Principal, no reducidos.
- Se agrega un chequeo de presión de memoria (OOM/GC) en los logs, algo que en Principal no es tan crítico pero en DR podría anticipar un problema justo cuando más se necesita el cluster funcionando.
- El umbral de quórum exigido es 3/3, igual que en Principal (variable `MIN_BROKERS_REQUIRED=3`).

## Fuera de alcance

Ninguno de los dos scripts valida la integración Kafka↔Flink (por ejemplo, que Flink efectivamente esté consumiendo de los topics de Kafka) — esa validación se hará por separado.
