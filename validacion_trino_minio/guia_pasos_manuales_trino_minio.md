# Guía de pasos manuales — Integración Trino → MinIO/AIStor (datos Iceberg)

Guía complementaria a `validate_trino_minio_principal.sh`. Este script no
puede cerrar toda la validación: el alcance de las credenciales S3 que usa
Trino en MinIO (least-privilege) requiere la consola de administración de
MinIO, con credenciales distintas a las que usa Trino — el script nunca las
solicita ni las usa.

**Alcance:** esta guía cubre la conexión directa Trino → AIStor para archivos
de datos Iceberg (Módulo 4). No cubre la conexión Trino → Gravitino
(metadata/catálogo REST) — ese es un ciclo de validación separado,
`validacion_trino_gravitino/`, aún no existe en este repositorio.

**Nota sobre `--vip`/`--dns`:** el script valida por defecto contra un nodo
individual de MinIO y marca ese hecho con un WARN explícito en el reporte. El
VIP DNS de HAProxy (`itaca.jardinazuayo.fin.ec`) ya está confirmado como real
en Principal (`172.17.210.62`, ver `logs/haproxy_minio/`) — pasarlo con
`--vip`/`--dns` para que la validación de conectividad y el Módulo 1.2 (cruce
de endpoint) se ejecuten contra el valor real. Ver Prerrequisito 4 en
`preparacion_integracion_trino_minio.md`.

---

## Módulo 4 — Least-privilege de las credenciales S3 que usa Trino

**Qué se valida y por qué requiere permisos elevados:** que las credenciales
S3 configuradas en el catálogo Iceberg de Trino (confirmadas como presentes
por el script — Módulo 1.4, sin inspeccionar el valor) correspondan a un
usuario de MinIO con permisos acotados solo al/los bucket(s) que Trino
necesita leer (y escribir, si el Módulo 3 confirmó que el catálogo tiene
escritura habilitada) — no a credenciales root/admin de MinIO. Esto solo se
puede confirmar desde la consola/CLI de administración de MinIO, con
credenciales distintas a las de aplicación.

**Comando exacto (con un alias `mc` de administrador, NO el de Trino):**
```bash
mc admin user info <alias-admin> <usuario-que-usa-trino>
mc admin policy info <alias-admin> <policy-asociada-al-usuario>
```

**Resultado esperado:** el usuario tiene una policy propia (no
`consoleAdmin`/`readwrite` global) cuyo `Resource` en el JSON de la policy
está acotado al/los bucket(s) real(es) del warehouse Iceberg — no a
`arn:aws:s3:::*`. Si el Módulo 3 del script confirmó que el catálogo es de
solo lectura, la policy no debería incluir permisos de escritura
(`s3:PutObject`, `s3:DeleteObject`) sobre ese bucket.

**Nota — comparar contra la credencial de Flink:** si Trino y Flink comparten
el mismo bucket de datos (ambos escriben/leen sobre el mismo warehouse
Iceberg), confirmar si usan el mismo usuario de MinIO o usuarios separados con
policies equivalentes. Un usuario compartido con permisos más amplios de lo
que Trino necesita (por ejemplo, escritura, si Trino solo debería leer) es un
hallazgo de least-privilege a reportar, aunque técnicamente "funcione".

**⚠️ Nunca pegar el `secret-key` real en esta guía ni en ningún informe.**
Confirmar solo la existencia y el alcance de la policy.

**Evidencia (pegar salida real, sin secretos):**
```
Principal:


```

---

## Resumen de checks pendientes por permisos o alcance

| Check | Motivo | Reflejado en el script como |
|---|---|---|
| Módulo 4 (least-privilege MinIO) | requiere consola/CLI de administración de MinIO, credenciales distintas a las de Trino | no reflejado en el script — documentado solo aquí |

Este punto debe cerrarse con el equipo de infraestructura de la Cooperativa
antes de declarar la integración apta para producción, siguiendo el mismo
estándar de informe que el resto del proyecto
(`informes/estandar_informes_validacion.md`).
