# Validación Funcional Flink — Tarea 2.4
## Bluepoint · Cooperativa Jardín Azuayo | Sprint 2 · Días 5–7
**Responsable:** Bluepoint  
**Versión Flink:** 1.20.1  
**Fecha:** 2026-06-26

---

## Topología involucrada

| VM | Rol | Contenedor(es) |
|---|---|---|
| `platform-apps-1` | Flink JobManager | `flink-jobmanager` |
| `cmp-1` | Flink TaskManager | `flink-taskmanager` |
| `cmp-2` | Flink TaskManager | `flink-taskmanager` |
| `cmp-3` | Flink TaskManager | `flink-taskmanager` |
| `kaf-1` | Kafka broker | `kafka` |
| `kaf-2` | Kafka broker | `kafka` |
| `kaf-3` | Kafka broker | `kafka` |

> **Nota:** Los scripts `validate_flink_principal.sh` y `validate_flink_dr.sh` usan `platformapps-1`
> como nombre de host por defecto, pero el nombre correcto según el plan es `platform-apps-1`.
> Corregir la variable `JM_HOST` antes de ejecutarlos.

---

## ⚠️ Discrepancia de versión detectada

Los scripts de validación tienen `FLINK_VERSION_EXPECTED="2.2.1"`.  
El plan de implementación especifica **Flink 1.20.1**.  
Antes de ejecutar los scripts, confirmar con la Cooperativa qué versión se instaló y ajustar
la variable correspondiente.

---

## Prerequisito: verificar conector Kafka en Flink

Antes de cualquier prueba de integración, confirmar que el conector Kafka está presente
en el contenedor JobManager:

```bash
ssh admapl@platform-apps-1
podman exec flink-jobmanager ls /opt/flink/lib/ | grep -i kafka
```

**Resultado esperado:** un JAR con nombre similar a `flink-sql-connector-kafka-*.jar`.

Si no aparece, la Cooperativa debe agregarlo antes de continuar con las pruebas de integración.
Las pruebas de job básico (Prueba 1) se pueden hacer igual sin el conector.

---

## Prueba 1 — Job de prueba batch (Flink sin Kafka)

Verifica que Flink puede ejecutar jobs de forma independiente.

```bash
ssh admapl@platform-apps-1
podman exec -it flink-jobmanager bash
```

Dentro del contenedor:

```bash
/opt/flink/bin/flink run \
  /opt/flink/examples/batch/WordCount.jar \
  --input /opt/flink/README.txt \
  --output /tmp/wordcount-output.txt
```

**Resultado esperado:**
```
Job has been submitted with JobID <id>
Program execution finished
Job with JobID <id> has finished.
Job Runtime: <tiempo>ms
```

Verificar el archivo de salida:
```bash
head -20 /tmp/wordcount-output.txt
```

**Criterio de aprobación:** job termina en estado `FINISHED`, archivo de salida contiene pares palabra/conteo.

---

## Prueba 2 — Conectividad Flink → Kafka

Confirmar que todos los nodos Flink pueden alcanzar los tres brokers Kafka en el puerto 9092.
Ejecutar en `platform-apps-1`, `cmp-1`, `cmp-2` y `cmp-3`:

```bash
for NODO in platform-apps-1 cmp-1 cmp-2 cmp-3; do
  echo "=== Desde $NODO ==="
  ssh admapl@$NODO "
    for BROKER in kaf-1 kaf-2 kaf-3; do
      nc -z -w 3 \$BROKER 9092 \
        && echo \"  OK: \$BROKER:9092\" \
        || echo \"  FAIL: \$BROKER:9092\"
    done
  "
done
```

**Criterio de aprobación:** los 3 brokers responden `OK` desde los 4 nodos Flink.  
Si alguno falla, hay un problema de red o firewall entre el segmento compute y el segmento Kafka — escalar antes de continuar.

---

## Prueba 3 — Integración Flink + Kafka vía SQL Client

### Paso 3.1 — Crear topic de prueba en Kafka

Ejecutar desde `kaf-1` usando el contenedor `kafka`:

```bash
ssh admapl@kaf-1
podman exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kaf-1:9092 \
  --create \
  --topic flink-test \
  --partitions 3 \
  --replication-factor 3
```

Verificar que el topic se creó:
```bash
podman exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kaf-1:9092 \
  --describe \
  --topic flink-test
```

**Resultado esperado:** topic `flink-test` con 3 particiones replicadas en los 3 brokers.

### Paso 3.2 — Abrir el SQL Client en el JobManager

```bash
ssh admapl@platform-apps-1
podman exec -it flink-jobmanager /opt/flink/bin/sql-client.sh
```

### Paso 3.3 — Crear tabla Kafka y ejecutar query de streaming

En el SQL Client:

```sql
-- Tabla que lee del topic flink-test en Kafka
CREATE TABLE kafka_test (
  mensaje STRING
) WITH (
  'connector'                     = 'kafka',
  'topic'                         = 'flink-test',
  'properties.bootstrap.servers'  = 'kaf-1:9092,kaf-2:9092,kaf-3:9092',
  'properties.group.id'           = 'flink-validation-group',
  'scan.startup.mode'             = 'latest-offset',
  'format'                        = 'raw'
);

-- Leer mensajes en tiempo real (streaming continuo)
SELECT * FROM kafka_test;
```

La query queda en modo streaming esperando mensajes.

### Paso 3.4 — Producir mensajes desde Kafka (en otra terminal)

```bash
ssh admapl@kaf-1
for i in 1 2 3; do
  echo "bluepoint-test-mensaje-$i" | \
  podman exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kaf-1:9092 \
    --topic flink-test
done
```

**Resultado esperado en el SQL Client:** los 3 mensajes aparecen en la tabla de resultados en menos de 2 segundos.

### Paso 3.5 — Verificar job en la UI de Flink

Desde un navegador con acceso a `platform-apps-1`:

```
http://platform-apps-1:8081
```

Debe aparecer un job en estado `RUNNING` con nombre `collect`. Este es el job creado por el SQL Client.

**Criterio de aprobación:** mensajes producidos en Kafka aparecen en el SQL Client de Flink en <2 segundos y el job aparece como `RUNNING` en la UI.

### Paso 3.6 — Limpieza del topic de prueba

Una vez validado, eliminar el topic para no dejar recursos innecesarios:

```bash
ssh admapl@kaf-1
podman exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kaf-1:9092 \
  --delete \
  --topic flink-test
```

---

## Resumen de criterios de aprobación — Tarea 2.4

| # | Prueba | Criterio | Estado |
|---|---|---|---|
| 1 | Job WordCount batch | Termina `FINISHED`, archivo de salida generado | ☐ |
| 2 | Conectividad a Kafka | `nc` OK en 3 brokers desde los 4 nodos Flink | ☐ |
| 2b | Conector Kafka presente | JAR en `/opt/flink/lib/` del contenedor `flink-jobmanager` | ☐ |
| 3 | Integración Kafka→Flink | Mensajes aparecen en SQL Client en <2s, job `RUNNING` en UI | ☐ |

**Aprobado si:** todas las casillas marcadas y sin errores críticos.  
**Bloqueante externo:** si el conector Kafka no está instalado, la Prueba 3 no puede ejecutarse — documentar como faltante de instalación y escalar a la Cooperativa.

---

## Próximo paso: Tarea 2.5 / 2.6 (Días 8–10)

Una vez aprobada esta tarea, la Cooperativa despliega el job de streaming real (`Kafka → Flink → salida`), y Bluepoint valida el flujo end-to-end completo incluyendo latencia y estabilidad.
