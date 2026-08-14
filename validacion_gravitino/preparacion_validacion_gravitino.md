# Guía de validación — Gravitino (Iceberg REST Catalog)

El script `validate_gravitino_principal.sh` corre en `pbigd-plat-apps01` y revisa, de punta a punta, si el catálogo de metadata del lakehouse está listo para operar. Valida Gravitino como servicio en sí mismo — **no** valida su integración con MinIO/AIStor (que sí existe conceptualmente en el diseño, pero cuyo script `validacion_gravitino_minio/` aún no existe en este repositorio). Cada sección responde a una pregunta concreta sobre el estado del servicio.

## Sección 1 — Sistema Operativo y prerequisitos

**Para qué:** confirma que la base sobre la que corre Gravitino es la correcta antes de mirar nada del propio servicio.

- **SO Rocky Linux 10.x**: si el sistema operativo no es el esperado, cualquier otro resultado puede ser inconsistente con el diseño homologado.
- **Podman ≥5.x**: Gravitino corre containerizado; una versión vieja de Podman puede no soportar features que el despliegue necesita.
- **NTP/chrony sincronizado**: relevante para que los timestamps de metadata (creación/modificación de tablas Iceberg) sean consistentes entre Gravitino, Flink y Trino.
- **Linger habilitado para `admapl`**: los contenedores rootless (Podman sin root) necesitan linger para sobrevivir a un logout de la sesión que los lanzó.
- **Versión de Gravitino desplegada**: intento de detección local (mejor esfuerzo, vía archivo de versión o nombre de jar); la confirmación definitiva ocurre en la Sección 3 vía `/api/version`.

No se valida la versión de Java: el proceso corre dentro del contenedor, no en el host, y no aporta información nueva sobre el host que ya no cubran los checks de Podman/OS de esta sección.

## Sección 2 — Servicio y redundancia

**Para qué:** confirma que el proceso de Gravitino está corriendo, que va a seguir corriendo solo, y que la redundancia (si existe) es real y no solo aparente.

- **Contenedor "Up" y usuario no-root**: verificación directa de que el proceso existe. El usuario se valida vía `podman exec ... id` (UID real dentro del contenedor), no vía `Config.User` — este último solo refleja lo declarado en la imagen y puede salir vacío en un contenedor rootless aunque el proceso no tenga privilegios reales. Si el UID interno es 0, el script no se queda en una advertencia genérica: verifica directamente `HostConfig.Privileged` y `HostConfig.CapAdd` junto con si el host es Podman rootless. Solo si las tres condiciones son seguras (rootless, sin `--privileged`, sin capabilities añadidas) se reporta **OK** — riesgo real descartado por evidencia, no por advertencia manual. Si hay `--privileged` es FAIL; si hay capabilities añadidas sin ser `--privileged` es WARN (revisar si son necesarias); si no se puede determinar si el host es rootless, WARN con el comando exacto para confirmarlo a mano.
- **Unidad systemd/Quadlet activa y habilitada**: "activa" dice que está corriendo ahora; "habilitada" dice que arrancará solo tras un reinicio del servidor. Si el script se ejecuta ya como el propio usuario `admapl`, consulta `systemctl --user` directamente; solo recurre a `sudo -u admapl` cuando corre como otro usuario. Esto evita abrir una sesión PAM nueva en cada corrida — con sesiones acumuladas, `sudo -u` puede chocar contra el límite de "too many logins" del usuario y dar un WARN falso que no refleja el estado real del servicio.
- **Conteo real de instancias desplegadas**: el plan de implementación (`context/Bluepoint_PlanImplementacion_v2._06042026.md`, sección "Data Lakehouse (warm/cold)", línea 133) dice textualmente: *"Iceberg REST Catalog (Apache Gravitino 1.1.0) (1 instancia basta en mínimo; 2 recomendado)"*, pero la tabla de asignación de recursos del datacenter principal solo documenta **un** nodo (`platform-apps-1`) con este rol. Este script cuenta lo que realmente hay corriendo en el nodo. A diferencia del chequeo de usuario no-root, esta brecha **no es algo que el script pueda resolver con más verificación automática** — es una decisión de arquitectura pendiente (¿se agrega el segundo nodo o se acepta 1 instancia como definitivo?). Por eso el WARN se mantiene intencionalmente en cada corrida, como recordatorio del punto abierto, hasta que el equipo de infraestructura lo resuelva formalmente.
- **Mecanismo de HA real si hay 2+ instancias**: este es el chequeo más delicado de la sección. El proyecto ya tuvo un precedente de encontrar una IP virtual "de papel" (sin failover real detrás) en PostgreSQL, y tuvo que agregar HAProxy+Keepalived *después* de la validación inicial de MinIO. Si Gravitino aparece con 2 instancias, el script busca evidencia real de HAProxy/Keepalived/VIP antes de dar por sentado que existe redundancia funcional — dos procesos corriendo en paralelo sin un mecanismo de conmutación delante no es alta disponibilidad, es dos puntos de falla en vez de uno.

## Sección 3 — Validación funcional básica (sin MinIO)

**Para qué:** confirma que Gravitino no solo "está corriendo", sino que efectivamente gestiona metadata como catálogo REST.

- **`/api/version`**: confirma que la REST API responde y expone la versión real desplegada. Se contrasta contra la versión de referencia vigente (ver Sección "Inconsistencias" más abajo), pero solo como dato informativo (INFO) — el plan no exige a Gravitino una versión mínima bloqueante (a diferencia de Trino, que sí exige JDK 25.0.1 como mínimo), así que un desvío frente a la referencia no se reporta como WARN.
- **`/api/metalakes` (listado)**: confirma que el servicio puede enumerar el estado actual de metalakes existentes, sin modificar nada.
- **Ciclo crear + eliminar metalake de prueba**: la prueba end-to-end real — confirma que Gravitino puede escribir y borrar metadata correctamente. Deliberadamente **no** se prueba que esto se refleje en almacenamiento real de MinIO (creación de tablas Iceberg, escritura de manifiestos, etc.) — esa es la responsabilidad de `validacion_gravitino_minio/`, que valida la integración de punta a punta. El ciclo captura el código HTTP explícito de cada llamada (no solo si el cuerpo de respuesta vino vacío):
  - Si la creación devuelve `409 Conflict` con `MetalakeAlreadyExistsException` (residuo de una corrida anterior que no pudo eliminar el metalake), el script lo elimina y reintenta automáticamente.
  - El `DELETE` siempre se envía con `?force=true`. Confirmado en sitio: Gravitino devuelve `409 MetalakeInUseException` ("please disable it first or use force option") al intentar borrar **cualquier** metalake, incluso uno recién creado por el propio script y sin usar — es el comportamiento estándar de la API, no un síntoma de un problema real, así que forzar el borrado es la operación correcta y esperada aquí (no un workaround cuestionable).

## Sección 4 — Logs y observabilidad

**Para qué:** confirma que, si algo falla en producción, va a quedar rastro para diagnosticarlo.

- **Errores recientes en el contenedor** (ERROR/Exception/FATAL): señal temprana de que algo anda mal aunque el servicio siga respondiendo.
- **Grafana Alloy / node-exporter**: mismo chequeo "smart" ya usado en el resto del proyecto (Trino, observabilidad) — revisa tanto el exporter embebido en Alloy (`:17935`, `prometheus.exporter.unix`) como el standalone (`:9100`); un WARN aquí no se reclasifica como exporter faltante si el componente embebido está `healthy`.

## Sección 5 — Recursos

**Para qué:** informativo, no bloqueante — contrasta CPU/RAM disponibles contra el dimensionamiento documentado para `platform-apps-1` (4 vCPU / 16 GB), compartido entre Flink JobManager, Trino Coordinator, Redis Primary/Sentinel y Gravitino en el mismo nodo.

---

## Módulo DR — no generado en esta entrega

A diferencia de Kafka, Flink y MinIO (que sí tienen scripts DR en este repositorio), **no se generó `validate_gravitino_dr.sh`**. La razón:

- La fila `platform-apps-dr` de la tabla de asignación de recursos del datacenter alterno (plan de implementación) lista únicamente: *Redis Primary (promovible) + Redis Sentinel + API online DR + Alloy*. Ni Gravitino, ni Trino, ni siquiera Flink JobManager (que va documentado aparte, en `dr-cmp-1`) aparecen en esa fila.
- Esto es una laguna más seria que el caso ya documentado para Trino DR: Trino es explícitamente opcional según el plan ("SQL on lake, acceso ocasional"), pero Gravitino es el catálogo de metadata del lakehouse — y el Data Lake (MinIO+Iceberg) sí tiene presencia dimensionada en el datacenter alterno.
- **No se debe asumir que Gravitino simplemente "no está desplegado en DR" ni que "no aplica"**. Se deja como pregunta abierta explícita para el equipo de infraestructura, con el comando de verificación en sitio:
  ```
  podman ps --format '{{.Names}}\t{{.Image}}' | grep -i gravitino
  ```
  ejecutado en `pbigd-plat-apps01-cont` (nodo DR candidato, ya usado por Flink/Redis/Alloy DR según `context/hostnames.txt`).
- Si se confirma la existencia del contenedor, corresponde generar `validate_gravitino_dr.sh` como una entrega posterior, replicando el patrón Principal/DR ya usado en Kafka/Flink/MinIO (umbrales más estrictos en DR, ver por ejemplo `validacion_kafka/preparacion_validacion_kafka.md`, sección "Diferencias del script DR frente al Principal").

## Inconsistencias documentales detectadas

1. **Versión de Gravitino**: el plan de implementación (`Bluepoint_PlanImplementacion_v2._06042026.md`, tabla de componentes y sección 3.1) cita **Gravitino 1.1.0**. La documentación de referencia vigente del proyecto (fuente más reciente, con tabla dedicada a versiones estables finales) cita **Gravitino 1.2.1**. Mismo patrón ya reportado para Trino (479 en el plan vs. 481 en la fuente vigente) — no se resuelve unilateralmente aquí; el script usa 1.2.1 como referencia por ser la fuente más reciente, pero reporta cualquier desvío como WARN, no como asunción silenciosa de cuál documento tiene razón.
2. **Ausencia de Gravitino en la tabla de recursos DR**: ver sección "Módulo DR" arriba — se documenta como pregunta abierta, no como decisión de diseño confirmada.
3. **Cantidad real de instancias vs. recomendación del plan**: el plan recomienda 2 instancias de Gravitino para redundancia, pero la tabla de asignación de recursos del datacenter principal solo documenta un nodo (`platform-apps-1`) con este rol — no hay un segundo nodo dedicado en el diseño escrito. Si la ejecución del script en sitio confirma 2+ instancias, hay que documentar dónde vive físicamente la segunda (¿mismo nodo, dos contenedores? ¿nodo no documentado?) y si existe el HAProxy+Keepalived correspondiente (ver check 2.3) o si se repite el patrón de "VIP de papel" ya visto en PostgreSQL.
