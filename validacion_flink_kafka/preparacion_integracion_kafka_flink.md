# Guía de preparación — Validación de integración Kafka ↔ Flink (Principal y DR)

## Por qué esta validación está dividida en dos scripts

Los scripts originales `validate_kafka_flink_principal.sh` y `validate_kafka_flink_dr.sh` (ahora **deprecados**, ver nota al inicio de cada uno) asumían acceso SSH sin contraseña del usuario `admapl` desde el nodo Flink (JobManager) hacia los nodos Kafka, para poder crear, producir y borrar el topic de prueba de forma remota.

Se confirmó que **ese acceso SSH no existe** en el entorno real de Jardín Azuayo. Por eso la validación se dividió en dos scripts independientes, uno por lado, que se ejecutan en momentos distintos y se coordinan manualmente por el nombre de un topic:

- **Lado Kafka** (`validate_kafka_flink_<entorno>_kafka.sh`): se ejecuta *manualmente*, en sesión local, directamente en un nodo Kafka. No requiere SSH — el operador ya está físicamente/interactivamente en ese nodo.
- **Lado Flink** (`validate_kafka_flink_<entorno>_flink.sh`): se ejecuta de forma automática en el JobManager. No hace SSH hacia Kafka; recibe el nombre del topic ya creado como parámetro `--topic`. Sí usa SSH, pero **solo entre nodos Flink** (JobManager → TaskManagers), para extender la cobertura de un chequeo (ver Prerequisito 2). Ese SSH interno de Flink ya se usa en otros scripts del proyecto (ver `validacion_flink/preparacion_validacion_flink.md`) y no depende del acceso admapl→Kafka que falta.

Esto no reduce la validación: se sigue probando el mismo flujo real end-to-end (mensaje escrito en Kafka → leído por un job Flink SQL → visible en el sink), solo que la coordinación entre ambos pasos la hace el operador en vez de hacerla el script vía SSH.

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
1. Chequeo TCP local desde el JobManager hacia los 3 brokers Kafka (igual que antes).
2. Chequeo TCP adicional, vía SSH interno de Flink (JobManager → cada TaskManager, usuario `admapl`), repitiendo la misma prueba TCP *desde cada TaskManager*. Este SSH es entre nodos Flink únicamente — no depende del acceso admapl→Kafka que está roto.

Si no se cuenta con SSH ni siquiera entre nodos Flink en algún entorno, usar `--skip-tm-connectivity` y documentar el chequeo como paso manual: copiar y ejecutar en cada TaskManager el mismo comando de prueba TCP:
```bash
timeout 3 bash -c 'cat < /dev/null > /dev/tcp/<broker>/9092' && echo OPEN || echo CLOSED
```

**Umbral:**
- Principal: se exige alcance completo (3/3 brokers) desde el JobManager y desde cada TaskManager.
- DR: Kafka DR no se reduce (se exige 3/3 igual que Principal); Flink DR sí tolera degradación — basta con que **al menos 1 TaskManager** tenga acceso completo a los 3 brokers para considerar la integración operativa en contingencia.

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
   Verifica conector, conectividad (JobManager + TaskManagers) y confirma que los mensajes producidos en el paso 1 llegan al sink.

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
- La instalación del conector Kafka o la apertura de puertos/reglas de firewall — estos scripts los **verifican**, no los configuran. Si algo falta, es un hallazgo a reportar a la Cooperativa, no algo que el validador deba corregir.
