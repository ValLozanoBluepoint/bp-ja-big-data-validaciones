# Guía de preparación — Validación de integración Flink → MinIO (checkpoints/savepoints)

## Por qué esta validación es distinta a Kafka↔Flink

`validacion_flink/validate_flink_principal.sh` y `validate_flink_dr.sh` ya
incluyen un **Módulo 8** que verifica que el endpoint MinIO responda y que
`flink-conf.yaml` mencione `state.checkpoints`/`s3`/`filesystem.backend`. Eso
es solo una verificación de **configuración estática** — nunca dispara un
checkpoint real ni confirma que un objeto llegó a MinIO. Esta integración
cierra ese hueco con una prueba funcional de punta a punta.

A diferencia de `validacion_flink_kafka/` (que necesita dos scripts —
`_kafka.sh` y `_flink.sh` — porque el lado "productor" vive en otro clúster y
no hay SSH sin contraseña entre nodos), aquí **no hace falta split en dos
scripts**: la confirmación de que el checkpoint llegó a MinIO se obtiene
consultando la **REST API de Flink** (`GET /jobs/:id/checkpoints`), que reporta
`status.id: COMPLETED` y el `external_path` (`s3://flink-checkpoints/checkpoints/<job-id>/chk-N`)
directamente desde el JobManager — sin necesitar `mc` en ningún nodo MinIO ni
coordinar dos operadores.

Sigue existiendo, sin embargo, la misma limitación de fondo que en
Kafka↔Flink: **no hay SSH sin contraseña entre el JobManager y los
TaskManagers**. Como cada TaskManager sube su propio checkpoint directamente a
S3 (no pasa por el JobManager), la presencia del plugin S3 y la configuración
de `flink-conf.yaml` en cada TM no se puede verificar de forma remota. Por eso
esos dos puntos siguen siendo pasos **manuales**, con el comando exacto
impreso por el script, igual que el patrón ya usado en
`preparacion_integracion_kafka_flink.md`.

---

## Prerrequisito 1 — Bucket y prefijos en MinIO

El bucket `flink-checkpoints` **no existe hoy** en `REQUIRED_BUCKETS` de
`validacion_minio/validate_minio_principal.sh` / `_dr.sh` (que solo exige
`raw`, `bronze`, `silver`, `gold`). Debe crearse antes de la primera prueba,
desde un nodo con `mc` configurado (ver `validacion_minio/`):

```bash
mc mb bluepoint-minio/flink-checkpoints
mc mb bluepoint-minio/flink-checkpoints/checkpoints
mc mb bluepoint-minio/flink-checkpoints/savepoints
```

**Acción si falta:** reportar como hallazgo — agregar `flink-checkpoints` a
`REQUIRED_BUCKETS` en ambos scripts de `validacion_minio/` es responsabilidad
de esa validación, no de este script de integración.

---

## Prerrequisito 2 — Plugin S3 en los 4 nodos Flink

**Qué es:** Flink no trae soporte S3 por defecto; se distribuye como plugin
opcional (`flink-s3-fs-hadoop` o `flink-s3-fs-presto`, usar solo una de las
dos implementaciones). Sin este JAR, cualquier intento de checkpoint falla con
`UnsupportedFileSystemSchemeException` aunque `flink-conf.yaml` esté bien
configurado y MinIO responda al health-check.

**Ubicación requerida:** `/opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-2.2.1.jar`
(no en `/opt/flink/lib/` — los plugins de filesystem usan un classloader
aislado).

**En cuáles nodos:** en **los 4** — JobManager y los 3 TaskManagers
(Principal) / 2 TaskManagers (DR). El JobManager coordina los checkpoints y
escribe metadata/HA state en S3; cada TaskManager sube directamente su
porción del estado. Si falta el plugin en un TM, ese nodo específico fallará
al hacer checkpoint de forma intermitente, aunque el resto del clúster
funcione.

**Cómo se verifica:**
- JobManager: automático, Módulo 1 de `validate_flink_minio_<entorno>.sh`
  (`podman exec flink-jobmanager find /opt/flink/plugins -iname "flink-s3-fs-*.jar"`).
- Cada TaskManager: **manual** — no existe SSH sin contraseña entre el
  JobManager y los TaskManagers, así que el script no puede automatizarlo.
  Imprime el comando exacto para que el operador lo corra localmente en cada
  TM:
  ```bash
  podman exec flink-tm-N find /opt/flink/plugins -iname "flink-s3-fs-*.jar"
  ```

**Quién lo instala:** responsabilidad de despliegue de la Cooperativa; el
script solo verifica y reporta el hallazgo, no lo instala.

---

## Prerrequisito 3 — `config.yaml`/`flink-conf.yaml` consistente en los 4 nodos

**Nombre del archivo según versión de Flink:** Flink 2.x renombró
`flink-conf.yaml` (formato plano) a `config.yaml` (formato jerárquico, aunque
sigue aceptando claves en formato plano tipo `state.checkpoints.dir: ...`).
Confirmado en este entorno (2026-08-13): los TaskManagers Principal
(`pbigd-proc01/02/03`) usan `/opt/flink/conf/config.yaml`, no
`flink-conf.yaml` — un `grep`/`cat` apuntado al nombre viejo falla con
`No such file or directory` aunque la configuración exista. Los scripts
`validate_flink_minio_principal.sh`/`_dr.sh` detectan automáticamente cuál de
los dos archivos existe en el JobManager (`config.yaml` primero, luego
`flink-conf.yaml`); los comandos manuales para TaskManagers (Módulo 2, ver
`guia_pasos_manuales_flink_minio.md`) prueban ambos nombres por el mismo
motivo.

Debe estar presente y ser consistente en JM y en todos los TaskManagers (cada
uno también escribe/lee estado local antes de subirlo a S3):

```yaml
state.backend: rocksdb
state.backend.incremental: true
state.checkpoints.dir: s3://flink-checkpoints/checkpoints
state.savepoints.dir: s3://flink-checkpoints/savepoints
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 600000
execution.checkpointing.min-pause: 30000
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
s3.endpoint: http://pbigd-stg01:9000
s3.path.style.access: true
s3.access-key: <ACCESS_KEY>
s3.secret-key: <SECRET_KEY>
```

**Cómo se verifica:** igual patrón que el Prerrequisito 2 — automático en el
JobManager (Módulo 1), manual en cada TaskManager (probando `config.yaml` y
`flink-conf.yaml`, ver nota de nombre de archivo arriba):
```bash
podman exec flink-tm-N sh -c "grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint' /opt/flink/conf/config.yaml 2>/dev/null || grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint' /opt/flink/conf/flink-conf.yaml"
```

**Nota de seguridad:** no dejar `access-key`/`secret-key` en texto plano si el
entorno lo permite — usar credenciales inyectadas por systemd/Podman secrets
en vez del `.yaml`.

---

## Prerrequisito 4 — Endpoint MinIO a validar: nodo individual vs. VIP/DNS de HAProxy

Por defecto, `validate_flink_minio_principal.sh` valida conectividad contra un
**nodo individual de MinIO** (`http://pbigd-stg01:9000`), no contra una
IP virtual/DNS de alta disponibilidad — no hay ningún valor de VIP/DNS
estático confirmado en este repositorio (ver `validacion_haproxy_minio/`).

Si la capa HAProxy + Keepalived delante de MinIO ya está desplegada en el
entorno, pasar la VIP o el nombre DNS reales al script con `--vip <IP>` o
`--dns <nombre>` (ejemplo — **no** es un valor real, sustituir por el que
aplique en el entorno: `--vip 10.20.30.40` o `--dns minio-vip.jazuayo.local`):

```bash
./validate_flink_minio_principal.sh --vip <IP_DE_LA_VIP>
# o
./validate_flink_minio_principal.sh --dns <nombre_dns_de_la_vip>
```

Al pasar `--vip`/`--dns`:
- El propio endpoint que el script usa para probar conectividad (Módulo
  1.1/1.2) pasa a ser esa VIP/DNS, no el nodo individual por defecto.
- El Módulo 1.5 compara el `s3.endpoint` real configurado en
  `config.yaml`/`flink-conf.yaml` contra ese valor esperado.

**Nota — nombre corto vs FQDN:** confirmado en este entorno que
`s3.endpoint` usa el nombre corto (`http://itaca:9000`) mientras que el DNS
de HAProxy documentado en `validacion_haproxy_minio/` es el FQDN
(`itaca.jardinazuayo.fin.ec`). Una comparación de texto simple marcaría esto
como FAIL aunque ambos resuelvan a la misma IP (el contenedor tiene
`search jardinazuayo.fin.ec` en su `/etc/resolv.conf`, así que `itaca` se
completa automáticamente al mismo FQDN). Por eso el Módulo 1.5, si la
comparación de texto no coincide, resuelve ambos nombres por DNS dentro del
propio contenedor del JobManager (`getent hosts`) y compara IPs antes de
marcar FAIL — evita falsos positivos por nombre corto vs FQDN sin dejar de
detectar una deriva real de configuración (IP distinta).

Si se necesita apuntar a un endpoint distinto sin que sea tratado como la VIP
de comparación del Módulo 1.5 (por ejemplo, un endpoint de pruebas), usar
`--minio-endpoint <url>`, que tiene prioridad sobre `--vip`/`--dns` para la
conectividad pero no participa en la comparación del Módulo 1.5.

**Si NO se pasa ninguno de estos flags**, el script sigue funcionando contra
el nodo individual por defecto, pero lo marca con un **WARN explícito** en el
encabezado del reporte (no queda silencioso) indicando que no se está
validando la VIP/DNS de alta disponibilidad.

---

## Procedimiento — DC Principal

Un solo script, corrido en el JobManager (`pbigd-plat-apps01`):

```bash
./validate_flink_minio_principal.sh [--jobmanager-host host] [--skip-functional-test] [--checkpoint-wait-seconds N] [--vip IP] [--dns nombre] [--minio-endpoint url]
```

1. **Módulo 1** — verifica automáticamente en el JobManager: MinIO alcanzable
   (`/minio/health/live`), bucket `flink-checkpoints` accesible, plugin S3
   presente, `flink-conf.yaml` con las claves de checkpoint/S3.
2. **Módulo 2** — imprime en pantalla los comandos exactos para verificar
   plugin S3 y config en **cada TaskManager** (Prerrequisitos 2 y 3). El
   operador debe conectarse a `pbigd-proc01`, `pbigd-proc02`, `pbigd-proc03` y
   correrlos localmente.
3. **Módulo 3** — prueba funcional: somete un job Flink SQL trivial (fuente
   `datagen`, sink `print`) con checkpointing habilitado, espera al menos un
   intervalo de checkpoint, consulta `GET /jobs/:id/checkpoints` vía REST API
   y confirma que existe un checkpoint `COMPLETED` con su `external_path` en
   `s3://flink-checkpoints/...`. Cancela el job al finalizar.

Si el Módulo 3 falla con `status.id != COMPLETED` tras el timeout, revisar:
plugin S3 ausente en el JobManager (Módulo 1), bucket sin permisos de
escritura, o `s3.access-key`/`s3.secret-key` incorrectos.

## Procedimiento — DC Alterno (DR)

Mismo flujo, en la topología DR (JobManager `pbigd-plat-apps01-cont`,
TaskManagers `pbigd-proc01-cont` y `pbigd-proc02-cont` — solo 2 nodos):

```bash
./validate_flink_minio_dr.sh [--jobmanager-host host] [--skip-functional-test] [--checkpoint-wait-seconds N] [--vip IP] [--dns nombre] [--minio-endpoint url]
```

**Corrección (2026-08-13):** la suposición original de este documento — que
DR no tenía HAProxy/VIP delante de MinIO "por diseño" — quedó desactualizada.
Evidencia real (`logs/haproxy_minio/dr/pbigd-stg03-cont.txt`, 2026-08-12)
confirma que DR **sí** tiene esa capa, igual que Principal: mismo nombre DNS
(`itaca.jardinazuayo.fin.ec`), pero resuelve a una IP distinta por segmento de
red (`172.17.210.182` en DR vs `172.17.210.62` en Principal). Por eso ahora se
recomienda pasar `--vip`/`--dns` también en DR (con el valor real de ese
segmento); sin ellos, el WARN del Prerrequisito 4 indica una validación
incompleta, no un diseño esperado.

---

## Diferencias Principal vs DR

| Aspecto | Principal | DR |
|---|---|---|
| TaskManagers Flink | 3 (`pbigd-proc01/02/03`) | 2 (`pbigd-proc01/02-cont`) |
| Umbral de TM con plugin+config OK | 3/3 (pleno) | ≥1 (`MIN_FLINK_TM_PLUGIN_OK=1`, degradación aceptada) |
| Espera de checkpoint | igual al intervalo configurado (`execution.checkpointing.interval`) | igual, con timeout algo mayor por recursos DR limitados |
| Severidad esperada de fallas | — | Más estricta: en DR no hay margen para reconstrucción de estado |
| Endpoint MinIO sin `--vip`/`--dns` | Nodo individual `pbigd-stg01:9000` — VIP real confirmado: `itaca.jardinazuayo.fin.ec` → `172.17.210.62` | Nodo individual `pbigd-stg01-cont:9000` — VIP real confirmado: `itaca.jardinazuayo.fin.ec` → `172.17.210.182` (mismo DNS, distinta IP por segmento) |

---

## Fuera de alcance de este script

- La salud individual de Flink y de MinIO — usar `validacion_flink/validate_flink_*.sh`
  y `validacion_minio/validate_minio_*.sh` antes de esta validación de
  integración.
- La instalación del plugin S3, la creación del bucket `flink-checkpoints`, o
  la corrección de `flink-conf.yaml` — este script los **verifica o reporta el
  síntoma**, no los configura. Si algo falta, es un hallazgo a reportar a la
  Cooperativa.
- La habilitación de acceso SSH sin contraseña entre nodos — no existe en este
  entorno; por eso la verificación de plugin/config en cada TaskManager queda
  como paso manual documentado arriba.
