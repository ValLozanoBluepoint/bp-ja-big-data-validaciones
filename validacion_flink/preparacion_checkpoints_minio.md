# Preparación — Checkpoints/Savepoints Flink → MinIO (S3)

## Bluepoint · Cooperativa Jardín Azuayo · Big Data Platform

Este documento describe lo que debe existir en el clúster Flink (DC Principal)
antes de que el Módulo 8 de `validate_flink_principal.sh` pueda pasar de forma
significativa: configuración de checkpoints/savepoints apuntando a MinIO y el
plugin del filesystem S3 instalado en cada nodo.

---

## 1. Prerrequisitos externos (fuera de Flink)

| Ítem | Detalle |
|---|---|
| Bucket MinIO `flink-checkpoints` | **No existe hoy** — no está en `REQUIRED_BUCKETS` de `validacion_minio`. Debe crearse antes de la primera prueba. |
| Endpoint MinIO alcanzable | `http://pbigd-stg01:9000` (nodo individual, valor por defecto) desde los 4 nodos Flink (JM + 3 TM). Si la VIP/DNS de HAProxy delante de MinIO (`validacion_haproxy_minio/`) ya está operativa, `s3.endpoint` debería apuntar ahí en vez de al nodo individual — ver nota abajo |
| Credenciales S3 | Access key / secret key con permisos read/write sobre el bucket `flink-checkpoints` |

Crear el bucket (desde un nodo con `mc` configurado, ver `validacion_minio/`):

```bash
mc mb bluepoint-minio/flink-checkpoints
mc mb bluepoint-minio/flink-checkpoints/checkpoints
mc mb bluepoint-minio/flink-checkpoints/savepoints
```

---

## 2. Topología de nodos

| Rol | Nodo | Contenedor Podman |
|---|---|---|
| JobManager | `pbigd-plat-apps01` | `flink-jobmanager` |
| TaskManager 1 | `pbigd-proc01` | `flink-tm-1` |
| TaskManager 2 | `pbigd-proc02` | `flink-tm-2` |
| TaskManager 3 | `pbigd-proc03` | `flink-tm-3` |

Los 4 nodos comparten la misma estructura base: `/opt/flink` (home), `/data/flink`
(estado local), `/var/log/flink` (logs).

---

## 3. `flink-conf.yaml` de ejemplo

Ruta dentro del contenedor: `/opt/flink/conf/flink-conf.yaml` (debe estar presente
y ser consistente en **los 4 nodos** — JM y los 3 TM, ya que cada TaskManager
también escribe/lee estado local antes de subirlo a S3).

```yaml
# =============================================================================
# flink-conf.yaml — Checkpoints/Savepoints hacia MinIO (S3)
# Bluepoint · Jardín Azuayo · Cluster Flink Principal
# =============================================================================

# --- State backend ---
state.backend: rocksdb
state.backend.incremental: true

# --- Directorios de checkpoints y savepoints (bucket dedicado) ---
state.checkpoints.dir: s3://flink-checkpoints/checkpoints
state.savepoints.dir: s3://flink-checkpoints/savepoints

# --- Checkpointing habilitado a nivel de cluster (o configurarlo por job) ---
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 600000
execution.checkpointing.min-pause: 30000
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# --- Conexión S3 hacia MinIO (no es AWS S3 real) ---
s3.endpoint: http://pbigd-stg01:9000  # nodo individual, ejemplo — usar la VIP/DNS de HAProxy si ya está desplegada
s3.path.style.access: true
s3.access-key: <ACCESS_KEY>
s3.secret-key: <SECRET_KEY>
# Evitar validación de certificado si MinIO usa TLS autofirmado; quitar en prod con CA válida
# s3.ssl.enabled: false

# --- Alta disponibilidad (si aplica en el diseño) ---
# high-availability: zookeeper
# high-availability.storageDir: s3://flink-checkpoints/ha/
```

> **Nota de seguridad:** no dejar `access-key`/`secret-key` en texto plano en el
> archivo si el entorno lo permite — usar variables de entorno
> (`ENABLE_BUILT_IN_PLUGINS` + credenciales inyectadas por systemd/Podman secrets)
> en vez del `.yaml` cuando sea posible.

> **Nota sobre `s3.endpoint` — nodo individual vs. VIP/DNS:** el valor de
> ejemplo arriba apunta a un nodo individual de MinIO, no a una VIP/DNS de
> alta disponibilidad (no hay ninguna confirmada como estática en este
> repositorio). Los scripts de `validacion_flink_minio/` (Módulo 1.5) validan
> justamente que `s3.endpoint` coincida con la VIP/DNS esperada cuando se
> ejecutan con `--vip <IP>` o `--dns <nombre>`; sin esos flags, marcan con un
> WARN explícito que la validación quedó contra el nodo individual. Si la
> capa HAProxy/Keepalived (`validacion_haproxy_minio/`) ya está operativa,
> actualizar `s3.endpoint` aquí a la VIP/DNS real antes de reconfigurar
> Flink.

---

## 4. Plugin S3 — dónde se instala

Flink **no trae soporte S3 por defecto**; se distribuye como plugin opcional. Sin
este JAR, aunque el `flink-conf.yaml` esté bien configurado y MinIO responda al
health-check, cualquier intento de checkpoint falla con
`UnsupportedFileSystemSchemeException` o `ClassNotFoundException` para el scheme `s3`.

### Ubicación requerida

```
/opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-2.2.1.jar
```

(alternativa válida: `s3-fs-presto` con `flink-s3-fs-presto-2.2.1.jar` — usar
**una sola** de las dos implementaciones, no ambas a la vez, para evitar
conflictos de classloading).

Puntos clave:

- Va en **`/opt/flink/plugins/<nombre-plugin>/`**, no en `/opt/flink/lib/`. Los
  plugins de filesystem usan un classloader aislado; si se coloca en `lib/` puede
  generar conflictos de dependencias con el resto del runtime.
- El nombre del subdirectorio (`s3-fs-hadoop`) es el que Flink usa para
  identificar el plugin — debe respetarse tal cual.

### En cuáles de los 4 nodos va

**En los 4** — JobManager y los 3 TaskManager. Motivo:

- El **JobManager** coordina los checkpoints y escribe metadata/HA state en S3.
- Cada **TaskManager** sube directamente su porción del estado (checkpoint de
  cada subtask) a S3 — no pasa por el JobManager. Si falta el plugin en un TM,
  ese nodo específico fallará al hacer checkpoint aunque el resto del clúster
  funcione, generando checkpoints incompletos/fallidos de forma intermitente.

| Nodo | Contenedor | Requiere plugin |
|---|---|---|
| `pbigd-plat-apps01` | `flink-jobmanager` | Sí |
| `pbigd-proc01` | `flink-tm-1` | Sí |
| `pbigd-proc02` | `flink-tm-2` | Sí |
| `pbigd-proc03` | `flink-tm-3` | Sí |

### Verificación rápida (los 4 nodos)

```bash
for c in flink-jobmanager flink-tm-1 flink-tm-2 flink-tm-3; do
  echo "== $c =="
  podman exec "$c" find /opt/flink/plugins -iname "flink-s3-fs-*.jar"
done
```

Si el `find` no devuelve nada en alguno de los 4, ese nodo no podrá persistir
checkpoints/savepoints en MinIO.

---

## 5. Checklist antes de validar

- [ ] Bucket `flink-checkpoints` creado en MinIO (con prefijos `checkpoints/` y `savepoints/`)
- [ ] `flink-conf.yaml` actualizado en los 4 nodos con `state.checkpoints.dir` / `state.savepoints.dir` / credenciales S3
- [ ] `flink-s3-fs-hadoop-*.jar` (o `-presto-*.jar`) presente en `/opt/flink/plugins/s3-fs-hadoop/` en los 4 contenedores
- [ ] Reinicio de JobManager y TaskManagers tras instalar el plugin (Flink lo carga solo al arrancar)
- [ ] Ejecutar `validate_flink_principal.sh` (Módulo 8) y confirmar `OK` en conectividad MinIO + configuración de checkpoints
- [ ] Prueba funcional: lanzar un job con checkpointing habilitado y confirmar en MinIO (`mc ls flink-checkpoints/checkpoints/`) que aparecen objetos
