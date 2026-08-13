# Guía de pasos manuales — Integración Flink → MinIO (checkpoints/savepoints)

Guía complementaria a `validate_flink_minio_principal.sh` y
`validate_flink_minio_dr.sh`. Estos scripts no pueden cerrar toda la
validación: no existe acceso SSH sin contraseña del usuario `admapl` entre
JobManager y TaskManagers (Módulo 2), y la validación de least-privilege de
las credenciales S3 requiere la consola de administración de MinIO, con
credenciales distintas a las que usa Flink (Módulo 6).

**Alcance:** esta guía cubre checkpoints/savepoints de Flink sobre MinIO
(state backend). No cubre la integración Flink↔Iceberg/Gravitino (catálogo
REST, tablas transaccionales) — ese es un ciclo de validación separado,
pendiente de que el equipo confirme URI de Gravitino, bucket de warehouse y
versión del JAR `iceberg-flink-runtime` a usar.

Para cada check: qué se valida, el comando exacto, el resultado esperado, y
un espacio para pegar la evidencia real de la corrida.

**Nota sobre `--vip`/`--dns`:** ambos scripts validan por defecto contra un
nodo individual de MinIO y marcan ese hecho con un WARN explícito en el
reporte (no hay VIP/DNS estático confirmado en el repo). Si la Cooperativa ya
tiene la VIP/DNS de HAProxy operativa, pasarla al ejecutar el script
(`--vip <IP>` o `--dns <nombre>`, valores propios del entorno — no
documentados aquí) para que la validación de conectividad y el Módulo 1.5 se
ejecuten contra la VIP real en lugar del nodo individual. Ver Prerrequisito 4
en `preparacion_integracion_flink_minio.md`.

---

## Módulo 2 — Plugin S3 y configuración en cada TaskManager

**Qué se valida y por qué es manual:** que el plugin `flink-s3-fs-hadoop` (o
`-presto`) esté instalado en `/opt/flink/plugins/` y que `flink-conf.yaml`
tenga las mismas claves de checkpoint/S3 en **cada** TaskManager, no solo en
el JobManager. El script solo puede confirmarlo automáticamente en el nodo
donde corre (normalmente el JobManager); sin SSH entre nodos, cada
TaskManager debe revisarse localmente.

**Nota sobre el nombre del archivo de configuración:** Flink 2.x renombró
`flink-conf.yaml` (formato plano) a `config.yaml` (formato jerárquico, aunque
sigue aceptando claves en formato plano) — confirmado en este entorno: los 3
TaskManagers Principal usan `config.yaml`, no `flink-conf.yaml`. Los scripts
`validate_flink_minio_principal.sh`/`_dr.sh` ya detectan esto automáticamente
en el JobManager (Módulo 1); los comandos manuales de abajo prueban ambos
nombres para no depender de la versión de cada nodo.

**Comando exacto (ejecutar en CADA TaskManager, con el contenedor
correspondiente):**

Principal (`pbigd-proc01/02/03`, contenedores `flink-tm-1/2/3`):
```bash
podman exec flink-tm-<N> find /opt/flink/plugins -iname 'flink-s3-fs-*.jar'
podman exec flink-tm-<N> sh -c "grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint|s3.path.style.access' /opt/flink/conf/config.yaml 2>/dev/null || grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint|s3.path.style.access' /opt/flink/conf/flink-conf.yaml"
```

DR (`pbigd-proc01/02-cont`, contenedores `flink-tm-1-cont/2-cont`):
```bash
podman exec flink-tm-<N>-cont find /opt/flink/plugins -iname 'flink-s3-fs-*.jar'
podman exec flink-tm-<N>-cont sh -c "grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint|s3.path.style.access' /opt/flink/conf/config.yaml 2>/dev/null || grep -E 'state.checkpoints.dir|state.savepoints.dir|s3.endpoint|s3.path.style.access' /opt/flink/conf/flink-conf.yaml"
```

**Resultado esperado:** el `find` devuelve el JAR del plugin y el `grep`
devuelve las 4 claves, con el mismo `s3.endpoint` que el JobManager (validado
automáticamente por el script — Módulo 1.5). Umbral: Principal exige 3/3 TM
OK; DR acepta degradación a 1/2 TM OK (capacidad reducida por diseño).

**Evidencia (pegar salida real):**
```
Principal — pbigd-proc01:


Principal — pbigd-proc02:


Principal — pbigd-proc03:


DR — pbigd-proc01-cont:


DR — pbigd-proc02-cont:

```

---

## Módulo 6 — Least-privilege de las credenciales S3 que usa Flink

**Qué se valida y por qué requiere permisos elevados:** que el
`s3.access-key`/`s3.secret-key` configurado en `flink-conf.yaml` (confirmado
como presente por el script — Módulo 1.7, sin inspeccionar el valor)
corresponda a un usuario de MinIO con permisos acotados solo al bucket
`flink-checkpoints`, no a credenciales root/admin de MinIO. Esto solo se
puede confirmar desde la consola/CLI de administración de MinIO, con
credenciales distintas a las de aplicación — el script nunca las solicita ni
las usa.

**Comando exacto (con un alias `mc` de administrador, NO el de Flink):**
```bash
mc admin user info <alias-admin> <usuario-que-usa-flink>
mc admin policy info <alias-admin> <policy-asociada-al-usuario>
```

**Resultado esperado:** el usuario tiene una policy propia (no
`consoleAdmin`/`readwrite` global) cuyo `Resource` en el JSON de la policy
está acotado a `arn:aws:s3:::flink-checkpoints` y
`arn:aws:s3:::flink-checkpoints/*` — no a `arn:aws:s3:::*`.

**⚠️ Nunca pegar el `secret-key` real en esta guía ni en ningún informe.**
Confirmar solo la existencia y el alcance de la policy.

**Evidencia (pegar salida real, sin secretos):**
```
Principal:


DR:

```

---

## Resumen de checks pendientes por permisos o alcance

| Check | Motivo | Reflejado en el script como |
|---|---|---|
| Módulo 2 (por TaskManager) | sin SSH entre nodos, no automatizable desde el JobManager | INFO — comando impreso, resultado queda manual |
| Módulo 6 (least-privilege MinIO) | requiere consola/CLI de administración de MinIO, credenciales distintas a las de Flink | no reflejado en el script — documentado solo aquí |

Estos puntos deben cerrarse con el equipo de infraestructura de la
Cooperativa antes de declarar la integración apta para producción, siguiendo
el mismo estándar de informe que el resto del proyecto
(`informes/estandar_informes_validacion.md`).
