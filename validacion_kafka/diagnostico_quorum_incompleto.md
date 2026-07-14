# Diagnóstico: cluster Kafka con 1/3 brokers activos

## Resumen ejecutivo

Los 4 fallos que ves en las secciones 5 y 6 **no son un problema de diseño/arquitectura** — son la misma causa raíz repetida: **solo `pbigd-kaf01` está realmente unido al cluster KRaft; `pbigd-kaf02` y `pbigd-kaf03` no.** El cluster está corriendo como si fuera de 1 solo nodo, no de 3.

Esto es un problema de **despliegue/configuración incompleta**, no de que "Kafka con KRaft de 3 nodos" esté mal planteado. Una vez que los 3 nodos se vean entre sí correctamente, estos 4 fallos deberían desaparecer solos — no hay que rediseñar nada.

## Los 4 fallos, uno por uno

### 1. `[FAIL] Brokers activos insuficientes: 1 (se esperan 3 en DC Principal)` (sección 5.3)

Es el síntoma más directo: al preguntarle al cluster "¿qué brokers están registrados?" (`kafka-broker-api-versions.sh`), solo responde 1. Este es el hecho central — todo lo demás es consecuencia de este.

### 2. `[FAIL] Creación de topic de prueba: FALLO` — `InvalidReplicationFactorException` (sección 5.4)

```
Unable to replicate the partition 3 time(s): The target replication factor of 3
cannot be reached because only 1 broker(s) are registered.
```

**Esto no es un bug ni un fallo real de Kafka — es Kafka protegiéndose correctamente.** El script pide crear un topic con `replication-factor=3`, y Kafka se niega porque físicamente no hay 3 brokers donde poner esas 3 réplicas. Es el comportamiento esperado y correcto ante un cluster incompleto; confirma el mismo diagnóstico del punto 1.

### 3. `[FAIL] Particiones sub-replicadas detectadas:` (sección 6.1) — **con el detalle vacío**

Este SÍ tiene un problema, pero es un **bug del script**, no un hallazgo real: la salida que ves como `[INFO]` vacío después del `FAIL` está vacía porque el filtro de limpieza de logs (`clean_kafka_output`) eliminó todo el ruido — pero el script decide "hay particiones sub-replicadas" mirando la variable **sin limpiar**, que casi siempre tiene *algo* de contenido (aunque sea solo ruido de logging del cliente de Kafka), incluso cuando en realidad no hay ninguna partición sub-replicada. Es decir: **esta sección puede estar marcando FAIL aunque no haya ningún problema real**, simplemente porque el comando siempre imprime algo de logging.

Ya lo corregí en ambos scripts (ver más abajo) para que la decisión se tome sobre la salida ya filtrada, no sobre la cruda.

### 4. `[FAIL] Quórum de brokers: 1/3 — cluster degradado` (sección 6.2)

Repite el hallazgo del punto 1 a nivel de "resiliencia del cluster": con 1/3 no hay quórum completo. Mismo origen.

## Causas más probables (de más a menos común)

1. **Los contenedores de Kafka en `pbigd-kaf02` / `pbigd-kaf03` no están corriendo.** Es la causa #1 más frecuente — revisa con `podman ps` en esos dos hosts si existe y está `Up` un contenedor equivalente a `kafka-broker-01` (ej. `kafka-broker-02`, `kafka-broker-03`).
2. **`controller.quorum.voters` no es idéntico en los 3 nodos.** En KRaft, el archivo `server.properties` de **cada uno** de los 3 nodos debe listar exactamente los mismos 3 votantes, ej.:
   ```
   controller.quorum.voters=1@pbigd-kaf01:9093,2@pbigd-kaf02:9093,3@pbigd-kaf03:9093
   ```
   Si en `pbigd-kaf01` esa lista solo tiene `1@pbigd-kaf01:9093` (como sugiere el `CurrentVoters` que viste antes, con un solo `id: 1`), el nodo ni siquiera está intentando contactar a los otros dos.
3. **`node.id` duplicado o mal asignado.** Cada nodo debe tener un `node.id` único (1, 2, 3). Si por error los 3 tienen `node.id=1`, jamás podrán coexistir como miembros separados del quórum.
4. **`cluster.id` distinto entre nodos.** El almacenamiento KRaft se formatea una vez con `kafka-storage.sh format --cluster-id <UUID> ...`, y ese mismo UUID debe usarse en los 3 nodos. Si `kaf02`/`kaf03` se formatearon con un UUID diferente (o nunca se formatearon), no pueden unirse al cluster de `kaf01` aunque estén corriendo y bien configurados en lo demás.
5. **Conectividad bloqueada en el puerto 9093 (controller) entre nodos.** Esto ya lo cubre la sección 4.3 del propio script — revisa si en esa corrida el TCP a `pbigd-kaf02:9093` / `pbigd-kaf03:9093` dio `PASS` o `WARN/FAIL`. Si el puerto no es alcanzable, ninguna de las otras causas importa hasta resolver eso primero.

## Cómo diagnosticar, paso a paso

```bash
# 1. ¿Los otros 2 nodos tienen el contenedor corriendo?
ssh admapl@pbigd-kaf02 'podman ps'
ssh admapl@pbigd-kaf03 'podman ps'

# 2. Comparar controller.quorum.voters en los 3 nodos (deben ser IDÉNTICOS)
for h in pbigd-kaf01 pbigd-kaf02 pbigd-kaf03; do
  echo "== $h =="
  ssh admapl@$h 'podman exec kafka-broker-0X grep -E "^(node\.id|controller\.quorum\.voters)=" /opt/kafka/config/server.properties'
done

# 3. Comparar cluster.id (meta.properties) en los 3 nodos
for h in pbigd-kaf01 pbigd-kaf02 pbigd-kaf03; do
  echo "== $h =="
  ssh admapl@$h 'podman exec kafka-broker-0X cat /data/kafka/meta.properties 2>/dev/null | grep cluster.id'
done
```

Si el `cluster.id` difiere entre nodos, o si `pbigd-kaf02`/`03` no tienen `meta.properties` (nunca se formatearon), ese es el problema.

## Cómo solucionarlo (según lo que encuentres)

- **Si el contenedor no está corriendo en kaf02/kaf03** → levantarlo (`podman start` / `systemctl start` según cómo esté gestionado).
- **Si `controller.quorum.voters` está incompleto** → corregirlo en los 3 nodos para que los 3 listen a los 3 votantes, y reiniciar los 3 brokers.
- **Si el `cluster.id` no coincide** → hay que re-formatear el storage de `kaf02`/`kaf03` con el **mismo** `cluster.id` que ya tiene `kaf01` (obtenido de su `meta.properties`), usando `kafka-storage.sh format --cluster-id <ID_DE_KAF01> -c server.properties`. Esto normalmente implica limpiar `/data/kafka` en esos 2 nodos primero si ya se formatearon con un ID distinto.
- **Si hay bloqueo de red en 9093** → resolver firewall/reglas de seguridad entre los 3 nodos antes de tocar nada más.

Una vez resuelto, re-correr `validate_kafka_principal.sh` — si el quórum quedó bien, `CurrentVoters` debería mostrar los 3 IDs, `BROKER_COUNT` debería ser 3, y la creación del topic de prueba (RF=3) debería pasar sola.

## Corrección aplicada al script (sección 6.1)

Se corrigió el bug del falso positivo: antes la decisión de "hay particiones sub-replicadas" se tomaba sobre la salida **cruda** del comando (que casi siempre tiene contenido, aunque sea solo logging), y ahora se toma sobre la salida **ya filtrada** de ruido — reflejando si realmente hay o no particiones sub-replicadas.

## Actualización — causa raíz confirmada: `cluster.id` distinto en los 3 nodos

Se verificó `meta.properties` en los 3 nodos:

```
kaf01: cluster.id=ppGWqJhuQNSaezGXt4ECAg
kaf02: cluster.id=VP6nnxobRa-pAdN7NtBFbQ
kaf03: cluster.id=7HmRjdAiQrSj8G_3zYdgfw
```

Los 3 son **distintos** — cada nodo se formateó (`kafka-storage.sh format`) de forma independiente, generando su propio UUID de cluster en vez de compartir uno solo. `node.id` y `controller.quorum.voters` están correctamente configurados en los 3 (se descartó esa hipótesis), pero eso no importa: Kafka nunca deja que nodos con `cluster.id` distinto se unan al mismo quórum. Cada nodo opera como un mini-cluster aislado de 1 solo miembro — consistente con todo lo observado (brokers=1 en los 3, `CurrentVoters` mostrando solo el propio id, snapshots periódicos normales de un cluster de 1 nodo).

**Fix:** re-formatear el storage de los 3 nodos con un único `cluster.id` compartido:
```bash
# 1. Generar UN solo UUID (una sola vez)
kafka-storage.sh random-uuid

# 2. Detener los 3 contenedores, limpiar /data/kafka en los 3, y reformatear los 3 con el MISMO id
kafka-storage.sh format -t <UUID_COMPARTIDO> -c /opt/kafka/config/server.properties

# 3. Reiniciar los 3 contenedores
```
Esto borra el estado actual de cada nodo — confirmar que no hay datos reales antes de ejecutar.

## Actualización — bug de "el script se detiene sin avisar" en 6.1 (ya corregido)

No era un cuelgue de Kafka (el comando responde en <1s). Era un bug de bash: `clean_kafka_output` usa `grep -v | sed`; cuando el 100% de la salida cruda es ruido de logging (nada real que reportar — el caso exacto de kaf02/kaf03, sin particiones sub-replicadas que mostrar), `grep -v` termina con código de salida 1 (semántica normal de grep: "no quedó ninguna línea"). Con `pipefail` + `set -e` activos, ese código de error se propagaba a través de la asignación `UNDER_REPLICATED_CLEAN=$(...)` y bash terminaba el script ahí mismo, sin ningún mensaje. Se corrigió agregando `|| true` a esa asignación puntual en ambos scripts.
