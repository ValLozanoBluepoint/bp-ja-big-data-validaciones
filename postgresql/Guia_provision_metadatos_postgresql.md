# Guía de ejecución — Aprovisionamiento de metadatos en PostgreSQL

**Proyecto:** Plataforma Big Data — Cooperativa Jardín Azuayo
**Componente:** Instancia PostgreSQL de **metadatos** (`platform-db1`; DR: `platform-dbdr`)
**Script:** `provision_metadatos_postgresql.sh`
**Preparado por:** Bluepoint AI

> Esta guía cubre **únicamente** la instancia de metadatos del pipeline (watermarks, DLQ, auditoría, estado de procesos). No aplica al clúster de *serving* del Transactional Outbox (`saldo_cuenta` / `movimiento_cuenta`), que tiene su propia configuración HA.

---

## 1. Qué hace el script

El script aprovisiona, de forma **idempotente** (puede ejecutarse varias veces sin efectos adversos), toda la estructura de seguridad de la base de metadatos: esquemas, roles, permisos de mínimo privilegio, *hardening* y verificación. Está construido con el patrón estándar de Bluepoint: salida **OK/FAIL/WARN** con color, cuadro resumen, **log en `/tmp/`** y **código de salida igual al número de fallos**.

Trabaja en seis fases:

| Fase | Nombre | Qué realiza |
|------|--------|-------------|
| 0 | Verificaciones previas | Comprueba `psql`, la conexión de administración, la versión del motor (≥16 para `\getenv`) y la presencia de las variables de contraseña. |
| 1 | Base de datos | Crea la base `bigd_meta` si no existe. |
| 2 | Roles | Crea/normaliza `meta_owner` (dueño, NOLOGIN), `mig_meta` (DDL) y los cuatro roles de aplicación, con atributos seguros y límite de conexiones. Fija contraseñas desde el entorno. |
| 3 | Esquemas | Crea `control`, `dlq`, `audit`, `pipeline`, todos propiedad de `meta_owner`. |
| 4 | Hardening | Cierra el esquema `public` y la base a `PUBLIC`; otorga `CONNECT` explícito por rol. |
| 5 | Permisos | Aplica grants de mínimo privilegio y `ALTER DEFAULT PRIVILEGES` para que los objetos **futuros** hereden permisos automáticamente. |
| 6 | Verificación | Revalida roles, propiedad de esquemas, hardening, `USAGE`, default privileges y `pg_read_all_data`; imprime el cuadro resumen. |

### Objetos que crea

**Esquemas**

| Esquema | Propósito |
|---------|-----------|
| `control` | Marcas de agua (`watermarks`) y control de extracción incremental. |
| `dlq` | Dead Letter Queue: eventos con esquema inválido o error de procesamiento. |
| `audit` | Rastro de auditoría / linaje de ingesta (append-only). |
| `pipeline` | Estado operativo de jobs, reconciliación y ventanas de reproceso. |

**Roles**

| Rol | Login | Uso | Privilegios (resumen) |
|-----|:-----:|-----|-----------------------|
| `meta_owner` | No | Dueño de esquemas y objetos | Propiedad (DDL); no se conecta directamente. |
| `mig_meta` | Sí | Migraciones / DDL | Miembro de `meta_owner`; ejecuta cambios de esquema. |
| `app_extractor` | Sí | Extractores incrementales | `control`: S/I/U · `audit`: I. |
| `app_flink` | Sí | Apache Flink | `dlq`: I · `audit`: I · `control`: S/U · `pipeline`: I/U. |
| `app_airflow` | Sí | Apache Airflow | `control`/`dlq`/`pipeline`: S/I/U/D · `audit`: S. |
| `ro_meta` | Sí | Observabilidad / reportes | Solo lectura global (`pg_read_all_data`). |

*(S=SELECT, I=INSERT, U=UPDATE, D=DELETE)*

---

## 2. Requisitos previos

- Ejecutarse **desde la consola** de `platform-db1` (y `platform-dbdr` en el DR), como el usuario del motor.
- Cliente `psql` disponible y **PostgreSQL ≥ 16** (el proyecto usa 17.x; el script emplea `\getenv`).
- Una cuenta de administración con privilegio para crear base, roles y esquemas (por defecto `postgres`).
- Contraseñas de los roles disponibles en el **gestor de secretos**, exportadas como variables de entorno (ver §3). El script **no contiene contraseñas**.

---

## 3. Variables de entorno

### Contraseñas (obligatorias para habilitar login)

Se leen con `\getenv`, por lo que **no aparecen en `ps` ni en el log**. Si falta alguna, el rol se crea pero su login queda inhabilitado y se marca como `WARN`.

```bash
export PGPW_MIG_META='...'
export PGPW_APP_EXTRACTOR='...'
export PGPW_APP_FLINK='...'
export PGPW_APP_AIRFLOW='...'
export PGPW_RO_META='...'
```

### Conexión y parámetros (opcionales; con valores por defecto)

| Variable | Defecto | Descripción |
|----------|---------|-------------|
| `METADB` | `bigd_meta` | Nombre de la base de metadatos. |
| `ADMINDB` | `postgres` | Base de mantenimiento para los chequeos previos. |
| `PGHOST` | `/var/run/postgresql` | Host o socket de administración. |
| `PGPORT` | `5432` | Puerto. |
| `PGUSER` | `postgres` | Usuario de administración. |
| `CL_MIG` / `CL_EXTRACTOR` / `CL_FLINK` / `CL_AIRFLOW` / `CL_RO` | `5` / `20` / `40` / `20` / `10` | Límite de conexiones por rol. |

---

## 4. Cómo se ejecuta

```bash
# 1) Exportar contraseñas desde el gestor de secretos (ver §3)
export PGPW_MIG_META='...' PGPW_APP_EXTRACTOR='...' PGPW_APP_FLINK='...' \
       PGPW_APP_AIRFLOW='...' PGPW_RO_META='...'

# 2) (opcional) Ajustar conexión de administración
export PGHOST=/var/run/postgresql PGPORT=5432 PGUSER=postgres

# 3) Ejecutar
bash provision_metadatos_postgresql.sh
```

Para el **datacenter alterno**, se ejecuta igual en `platform-dbdr` (ajustando `PGHOST` si corresponde).

---

## 5. Interpretación de la salida

- Cada comprobación imprime `[ OK ]`, `[FAIL]` o `[WARN]`.
- Al final se muestra un cuadro resumen con los conteos y la ruta del log:
  ```
  +---------------------------------------------------------------+
  |  RESUMEN DE APROVISIONAMIENTO — bigd_meta                     |
  +---------------------------------------------------------------+
  |  OK  : 50    FAIL: 0     WARN: 0     TOTAL: 50           |
  +---------------------------------------------------------------+
  ```
- **Código de salida = número de FALLOS.** `0` significa aprovisionamiento correcto.
- El **log completo** queda en `/tmp/provision_metadatos_<METADB>_<TIMESTAMP>.log`.

| Marcador | Significado | Acción |
|----------|-------------|--------|
| `OK` | La comprobación pasó. | Ninguna. |
| `WARN` | Situación no bloqueante (p. ej. falta una contraseña). | Revisar; el rol afectado no podrá iniciar sesión hasta corregirlo. |
| `FAIL` | Error que impide un estado correcto. | Revisar el log; corregir y re-ejecutar. |

---

## 6. Idempotencia y re-ejecución

El script puede ejecutarse cuantas veces sea necesario: los roles y esquemas se crean solo si no existen, y los `GRANT` / `ALTER DEFAULT PRIVILEGES` se reaplican sin error. Re-ejecutarlo es la vía recomendada para **normalizar** el estado tras un cambio manual o para **rotar contraseñas** (basta volver a exportar las variables y correrlo).

---

## 7. Pasos posteriores al aprovisionamiento

### 7.1 Las migraciones deben crear objetos como `meta_owner`

Para que la herencia de permisos (`ALTER DEFAULT PRIVILEGES`) surta efecto, el DDL debe ejecutarse bajo `meta_owner`:

```sql
SET ROLE meta_owner;
CREATE TABLE control.watermarks (...);
RESET ROLE;
```

En Flyway/Liquibase, configurar la sentencia inicial de cada conexión (`initSql`) con `SET ROLE meta_owner;`.

### 7.2 (Recomendado) Fijar `search_path` por rol

Como los objetos **no** viven en `public`, conviene fijar el `search_path` de cada rol para no calificar el esquema en cada consulta:

```sql
ALTER ROLE app_extractor IN DATABASE bigd_meta SET search_path = control, audit;
ALTER ROLE app_flink     IN DATABASE bigd_meta SET search_path = control, dlq, audit, pipeline;
ALTER ROLE app_airflow   IN DATABASE bigd_meta SET search_path = control, dlq, pipeline, audit;
ALTER ROLE ro_meta       IN DATABASE bigd_meta SET search_path = control, dlq, audit, pipeline;
```

---

## 8. Métodos de conexión recomendados por aplicación

Todas las conexiones deben usar **TLS** (`sslmode=verify-full` con la CA del proyecto) y autenticación **`scram-sha-256`**. El *pooling* es **del lado de la aplicación**; su tamaño **no debe exceder** el `CONNECTION LIMIT` del rol (definido en el script). La instancia de metadatos es *standalone*, por lo que no se interpone PgBouncer salvo que el total agregado de conexiones lo justifique (ver §8.1).

| Aplicación | Rol | Driver / método recomendado | Pooling recomendado | Esquemas | Notas clave |
|------------|-----|-----------------------------|---------------------|----------|-------------|
| Migraciones (Flyway/Liquibase) | `mig_meta` | JDBC (`org.postgresql.Driver`) | Sin pool (proceso de despliegue) | todos | `initSql = SET ROLE meta_owner;` para que los objetos queden bajo `meta_owner`. |
| Extractores incrementales | `app_extractor` | Java → JDBC + **HikariCP**; Python → **psycopg3** + `psycopg_pool` | Pool pequeño (≤ 15) | `control`, `audit` | Solo upsert de watermark e inserción de auditoría; sin acceso a `dlq`/`pipeline`. |
| Apache Flink | `app_flink` | **Flink JDBC connector** (`jdbc` / JDBC sink, `org.postgresql.Driver`) | Pool interno del sink; `sink.buffer-flush` ajustado (≤ 40) | `control`, `dlq`, `audit`, `pipeline` | Upserts idempotentes (`ON CONFLICT`); at-least-once + idempotencia. No requiere XA. |
| Apache Airflow | `app_airflow` | **PostgresHook** (psycopg2/psycopg3) vía Connection; SQLAlchemy `QueuePool` | Pool moderado (≤ 15) | `control`, `dlq`, `pipeline`, `audit` | Único rol con `DELETE` (purga de DLQ y ventanas de reproceso); solo lectura en `audit`. |
| Observabilidad / BI | `ro_meta` | **Grafana** datasource PostgreSQL (solo lectura); `psql`/DBeaver para operación | Pool del datasource (≤ 10) | todos (lectura) | Solo lectura global (`pg_read_all_data`); nunca escribe. |

### Ejemplos de cadena de conexión

**JDBC (Flink, extractores Java, migraciones)**
```
jdbc:postgresql://platform-db1:5432/bigd_meta?ssl=true&sslmode=verify-full&currentSchema=control
```

**Python — psycopg3 (extractores)**
```
postgresql://app_extractor@platform-db1:5432/bigd_meta?sslmode=verify-full&options=-c%20search_path%3Dcontrol,audit
```

**Airflow — URI SQLAlchemy (Connection)**
```
postgresql+psycopg2://app_airflow@platform-db1:5432/bigd_meta?sslmode=verify-full
```
> En Airflow, definir el esquema y el `search_path` en los *extras* de la Connection, y **no** almacenar la contraseña en texto plano (usar el backend de secretos de Airflow).

**Grafana — datasource PostgreSQL (solo lectura)**
```
Host: platform-db1:5432   Database: bigd_meta   User: ro_meta
TLS/SSL Mode: verify-full   (Root CA del proyecto)
```

> En todos los casos, la **contraseña** proviene del gestor de secretos de cada aplicación, nunca embebida en la cadena ni en repositorios de configuración.

### 8.1 ¿Cuándo introducir PgBouncer en esta instancia?

Por defecto no es necesario: el *pooling* de HikariCP / SQLAlchemy / el sink de Flink es suficiente para los volúmenes de metadatos. Considérese PgBouncer (modo **transaction**) solo si la suma de conexiones de todas las aplicaciones se acerca a los límites del servidor; en ese caso aplican las restricciones habituales del modo transacción (`SET LOCAL` en lugar de `SET SESSION`, tablas temporales dentro de la misma transacción).

---

## 9. Seguridad — puntos de control

- Ningún rol de aplicación tiene `SUPERUSER`, `CREATEDB`, `CREATEROLE` ni `BYPASSRLS`.
- La propiedad de los objetos (`meta_owner`) está separada de su uso (roles de aplicación): comprometer una cuenta de app no permite alterar el esquema.
- `public` y la base están cerrados a `PUBLIC`; el `CONNECT` es explícito por rol.
- `audit` es *append-only* para los productores (solo `INSERT`); `app_airflow` solo puede leerlo.
- Autenticación `scram-sha-256`, TLS `verify-full`, contraseñas en gestor de secretos y rotación periódica (re-ejecutando el script).
- `CONNECTION LIMIT` por rol como defensa ante agotamiento de conexiones.

---

## 10. Resolución de problemas

| Síntoma | Causa probable | Solución |
|---------|----------------|----------|
| `FAIL` en Fase 0 (sin conexión) | `PGHOST`/`PGPORT`/`PGUSER` incorrectos o motor detenido | Ajustar variables; verificar que el servicio esté activo. |
| `WARN` "sin contraseña para \<rol\>" | Variable `PGPW_*` no exportada | Exportar la variable y re-ejecutar. |
| App autentica pero "permission denied for table" | Objeto creado por un rol distinto de `meta_owner` | Recrear el objeto bajo `SET ROLE meta_owner;` o reasignar permisos y re-ejecutar el script. |
| App no encuentra la tabla sin calificar esquema | `search_path` no fijado | Aplicar §7.2 o calificar el esquema (`control.watermarks`). |
| `\getenv` no reconocido | Cliente `psql` < 16 | Usar el `psql` del motor 17.x del proyecto. |
