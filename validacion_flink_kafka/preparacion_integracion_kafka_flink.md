# Guía de preparación — Validación de integración Kafka ↔ Flink (Principal y DR)

## Por qué esta validación está dividida en dos scripts

Los scripts originales `validate_kafka_flink_principal.sh` y `validate_kafka_flink_dr.sh` (ahora **deprecados**, ver nota al inicio de cada uno) asumían acceso SSH sin contraseña del usuario `admapl` desde el nodo Flink (JobManager) hacia los nodos Kafka, para poder crear, producir y borrar el topic de prueba de forma remota.

Se confirmó que **ese acceso SSH no existe** en el entorno real de Jardín Azuayo — y tampoco existe SSH sin contraseña entre nodos Flink (JobManager → TaskManagers). No hay SSH sin contraseña de `admapl` entre ningún par de nodos del entorno. Por eso la validación se dividió en dos scripts independientes, uno por lado, que se ejecutan en momentos distintos y se coordinan manualmente por el nombre de un topic:

- **Lado Kafka** (`validate_kafka_flink_<entorno>_kafka.sh`): se ejecuta *manualmente*, en sesión local, directamente en un nodo Kafka. No requiere SSH — el operador ya está físicamente/interactivamente en ese nodo.
- **Lado Flink** (`validate_kafka_flink_<entorno>_flink.sh`): se ejecuta de forma automática en el JobManager. No usa SSH en ningún punto — ni hacia Kafka ni hacia los TaskManagers. Recibe el nombre del topic ya creado como parámetro `--topic`, valida lo que puede automáticamente desde el JobManager (conector, TCP local, sumisión del job SQL, estado RUNNING vía REST API), e **imprime en pantalla los comandos exactos** que el operador debe correr a mano en cada TaskManager para completar dos verificaciones que no se pueden automatizar sin SSH (ver Prerequisito 2 y Procedimiento).

Esto no reduce la validación: se sigue probando el mismo flujo real end-to-end (mensaje escrito en Kafka → leído por un job Flink SQL → visible en el sink), solo que la coordinación entre pasos —y ahora también la verificación en cada TaskManager— la hace el operador de forma manual, con el comando exacto que el script le entrega.

---

## Prerequisito 1 — Conector Kafka instalado en Flink

**Qué es:** el conector Kafka para Flink SQL es un JAR (`flink-sql-connector-kafka-*.jar`) que debe estar presente en `/opt/flink/lib/` dentro del contenedor `flink-jobmanager` (y en los TaskManagers, según cómo esté empaquetada la imagen). Sin ese JAR, Flink no reconoce `'connector' = 'kafka'` en las sentencias SQL — la integración no puede funcionar sin importar qué tan bien esté el resto del stack.

**Quién lo instala:** es responsabilidad de despliegue de la Cooperativa (parte del setup de Flink), no algo que Bluepoint instale como parte de la validación.

**Cómo se verifica:** Módulo 1 de `validate_kafka_flink_<entorno>_flink.sh`. El script **solo verifica** que el JAR exista — si falta, lo reporta como `FAIL` y omite el Módulo 3 (test funcional), pero no lo instala.

**Acción si falta:** reportar como hallazgo a la Cooperativa para que agregue el JAR antes de reintentar la validación.

---

## Prerequisito 2 — Conectividad TCP 9092 cruzada (Flink → Kafka)

**Qué es:** el puerto 9092 (broker/client de Kafka) debe estar alcanzable desde **todos** los nodos Flink que pueden llegar a ejecutar tareas del job — no solo desde el JobManager. Esto es relevante porque, en este diseño, Kafka y Flink viven en segmentos de red distintos (`pbigd-kaf*` vs `pbigd-proc*` / `-cont` en DR), y un chequeo hecho solo desde el JobManager no detecta un firewall o regla de red que bloquee específicamente a un TaskManager.

**Cómo se verifica:** Módulo 2 de `validate_kafka_flink_<entorno>_flink.sh`, en dos pasos:
1. Chequeo TCP **automático**, local, desde el JobManager hacia los 3 brokers Kafka.
2. Chequeo TCP desde **cada TaskManager** — este paso es **manual**: no existe SSH sin contraseña entre ningún par de nodos del entorno (ni siquiera JobManager → TaskManagers), así que el script no puede automatizarlo. Al llegar a este punto, el script imprime en pantalla el comando exacto a correr; el operador debe conectarse a **cada TaskManager** y ejecutarlo localmente ahí:
```bash
timeout 3 bash -c 'cat < /dev/null > /dev/tcp/<broker>/9092' && echo OPEN || echo CLOSED
```
Repetir para los 3 brokers Kafka, en cada TaskManager (Principal: `pbigd-proc01/02/03`; DR: `pbigd-proc01/02-cont`).

**Umbral:**
- Principal: se exige alcance completo (3/3 brokers) desde el JobManager y desde cada TaskManager.
- DR: Kafka DR no se reduce (se exige 3/3 igual que Principal); Flink DR sí tolera degradación — basta con que **al menos 1 TaskManager** tenga acceso completo a los 3 brokers para considerar la integración operativa en contingencia.

---

## Prerequisito 3 — Resolución de nombres dentro de los contenedores (no solo en el host)

**Qué es:** los contenedores `flink-jobmanager` y `flink-tm` tienen su propio namespace de red y su propio `/etc/hosts`/DNS, independiente del host donde corren. Que el **host** (ej. `pbigd-plat-apps01`) resuelva los hostnames de Kafka (`pbigd-kaf01/02/03`) no significa que el **contenedor** también los resuelva — son resoluciones independientes.

**Cómo se detecta:** si el Módulo 3 (integración funcional) falla con un job en estado `FAILED` y el error en `curl .../jobs/<id>/exceptions` incluye `No resolvable bootstrap urls given in bootstrap.servers` o similar, es este problema, no un problema de red/firewall. Confirmar entrando al contenedor:
```bash
podman exec flink-jobmanager getent hosts pbigd-kaf01
```
Si falla, el contenedor no tiene la resolución — hay que repetirlo también en cada contenedor TaskManager.

**Quién lo corrige:** la Cooperativa, agregando los hostnames/IPs de Kafka dentro del contenedor (`podman run --add-host`, montaje de `/etc/hosts`, o DNS del contenedor), tanto en `flink-jobmanager` como en cada TaskManager.

---

## Procedimiento — DC Principal

Ejecutar en este orden:

1. **En un nodo Kafka Principal** (ej. `pbigd-kaf01`), sesión local:
   ```bash
   ./validate_kafka_flink_principal_kafka.sh
   ```
   Al final imprime el nombre del topic de prueba creado (ej. `bluepoint-kafka-flink-it-1739999999`). Copiarlo.

2. **En el JobManager Principal** (`pbigd-plat-apps01`):
   ```bash
   ./validate_kafka_flink_principal_flink.sh --topic bluepoint-kafka-flink-it-1739999999
   ```
   Verifica automáticamente el conector y la conectividad TCP local del JobManager, somete el job Flink SQL y confirma que quedó `RUNNING`. Al final imprime dos comandos que hay que correr manualmente (ver paso 2b y 2c) porque no hay SSH hacia los TaskManagers.

2b. **En CADA TaskManager Principal** (`pbigd-proc01`, `pbigd-proc02`, `pbigd-proc03`), conectividad TCP — comando impreso por el script:
   ```bash
   timeout 3 bash -c 'cat < /dev/null > /dev/tcp/pbigd-kaf0X/9092' && echo OPEN || echo CLOSED
   ```
   (repetir para `kaf01`, `kaf02`, `kaf03`). Los 3 deben responder `OPEN` en los 3 TaskManagers.

2c. **En CADA TaskManager Principal**, confirmación del sink — comando impreso por el script (nombre de contenedor exacto por nodo):
   ```bash
   # pbigd-proc01
   podman logs flink-tm-1 | grep 'bluepoint-it-'
   # pbigd-proc02
   podman logs flink-tm-2 | grep 'bluepoint-it-'
   # pbigd-proc03
   podman logs flink-tm-3 | grep 'bluepoint-it-'
   ```
   Si aparecen líneas con el prefijo, la integración quedó confirmada en ese TaskManager (Flink puede haber asignado la partición a cualquiera de los 3, no hace falta que aparezca en todos).

3. **De vuelta en el nodo Kafka Principal**, limpieza:
   ```bash
   ./validate_kafka_flink_principal_kafka.sh --cleanup bluepoint-kafka-flink-it-1739999999
   ```

## Procedimiento — DC Alterno (DR)

Mismo flujo, en los nodos DR:

1. **En un nodo Kafka DR** (ej. `pbigd-kaf01-cont`):
   ```bash
   ./validate_kafka_flink_dr_kafka.sh
   ```

2. **En el JobManager DR** (`pbigd-plat-apps01-cont`):
   ```bash
   ./validate_kafka_flink_dr_flink.sh --topic <topic-impreso-en-paso-1>
   ```

2b. **En CADA TaskManager DR** (`pbigd-proc01-cont`, `pbigd-proc02-cont`), conectividad TCP — comando impreso por el script:
   ```bash
   timeout 3 bash -c 'cat < /dev/null > /dev/tcp/pbigd-kaf0X-cont/9092' && echo OPEN || echo CLOSED
   ```
   (repetir para `kaf01-cont`, `kaf02-cont`, `kaf03-cont`). Umbral DR: basta con que **al menos 1** TaskManager alcance los 3 brokers.

2c. **En CADA TaskManager DR**, confirmación del sink — comando impreso por el script (nombre de contenedor exacto por nodo):
   ```bash
   # pbigd-proc01-cont
   podman logs flink-tm-1-cont | grep 'bluepoint-it-dr-'
   # pbigd-proc02-cont
   podman logs flink-tm-2-cont | grep 'bluepoint-it-dr-'
   ```

3. **De vuelta en el nodo Kafka DR**, limpieza:
   ```bash
   ./validate_kafka_flink_dr_kafka.sh --cleanup <topic>
   ```

---

## Diferencias Principal vs DR

| Aspecto | Principal | DR |
|---|---|---|
| Brokers Kafka exigidos | 3/3 | 3/3 (Kafka no se reduce en contingencia) |
| TaskManagers Flink | 3 (`pbigd-proc01/02/03`) | 2 (`pbigd-proc01/02-cont`), degradación aceptada |
| Umbral de TM con acceso completo a Kafka | 3/3 (implícito, se espera pleno) | ≥1 (`MIN_FLINK_TM_REACHABLE=1`) |
| Timeout de latencia del sink | 20s | 25s (recursos DR más limitados) |
| Espera de arranque del job SQL | 15s | 20s |
| Severidad esperada de fallas de red | — | Más estricta: en DR no hay margen, el clúster debe estar listo para asumir tráfico real |

---

## Fuera de alcance de estos scripts

- La salud individual de cada clúster (Kafka, Flink) — usar `validacion_kafka/validate_kafka_*.sh` y `validacion_flink/validate_flink_*.sh` antes de esta validación de integración.
- La instalación del conector Kafka, la apertura de puertos/reglas de firewall, o la resolución de nombres dentro de los contenedores (Prerequisito 3) — estos scripts los **verifican o reportan el síntoma**, no los configuran. Si algo falta, es un hallazgo a reportar a la Cooperativa, no algo que el validador deba corregir.
- La habilitación de acceso SSH sin contraseña entre nodos — no existe en este entorno; por eso todos los chequeos multi-nodo (conectividad TM, confirmación de sink) quedaron como pasos manuales documentados arriba, no como algo que el validador deba resolver.
