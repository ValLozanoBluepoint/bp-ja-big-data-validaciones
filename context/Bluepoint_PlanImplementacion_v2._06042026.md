Cooperativa Jardín Azuayo 

Plan de Implementación de software base Proyecto Big Data 

Bluepoint AI 

6-4-2026 v2 

Página 1 

Contenido Componentes, versiones, rol y compatibilidad con Rocky Linux 10 ........................................................................................... 4 Observaciones del stack ............................................................................................................................................................ 4 Separación clara de responsabilidades ................................................................................................................................ 4 Alta disponibilidad garantizada ............................................................................................................................................ 4 Arquitectura sin dependencia de software propietario ...................................................................................................... 4 Compatible con virtualización .............................................................................................................................................. 4 Lista completa de componentes y versiones ........................................................................................................................... 4 Organización de Clústers ........................................................................................................................................................... 6 Organización de VMs standalone ............................................................................................................................................. 7 Componentes mandatorios (mínimos indispensables) ........................................................................................................... 8 Base de plataforma (sin esto no hay operación) ................................................................................................................. 8 Streaming / eventos (para outbox y replica) ....................................................................................................................... 8 Procesamiento (para transformar y servir) .......................................................................................................................... 8 Data Lakehouse (warm/cold) ................................................................................................................................................ 8 Serving online (saldo/movimientos)..................................................................................................................................... 8 Data Warehouse en HA (mandatorio por tu requisito) ....................................................................................................... 8 Componentes opcionales (recomendados, pero no bloquean el arranque) ......................................................................... 8 SQL on lake (acceso ocasional Power BI al datalake) .......................................................................................................... 8 Orquestación / scheduling (para réplica diferida y mantenimiento) ................................................................................. 8 Data quality (evidencia) ........................................................................................................................................................ 8 Gobierno/catálogo/linaje ...................................................................................................................................................... 8 Observabilidad completa (recomendable, pero no bloqueante) ....................................................................................... 8 Pool de conexiones al DW .................................................................................................................................................... 9 Asignación de recursos .................................................................................................................................................................. 9 Datacenter principal .................................................................................................................................................................. 9 Datacenter alterno .................................................................................................................................................................... 9 Estrategia de asignación de recursos para el Datacenter Alterno...................................................................................... 9 Enfoque general de la estrategia........................................................................................................................................ 10 Principales decisiones de diseño ........................................................................................................................................ 11 Sprint Planning – Implementación Software Base ..................................................................................................................... 13 Características del plan ........................................................................................................................................................... 13 Resumen de Sprints – Implementación Plataforma Big Data ............................................................................................... 13 SPRINT 1 – Base general de la plataforma ............................................................................................................................. 14 Día 1: Preparación de equipos y entorno .......................................................................................................................... 14 Día 1–2: Documentación y preparación de nodos ............................................................................................................ 14 Día 3–4: Contenedores y runtime ...................................................................................................................................... 15 Día 5–7: Observabilidad base ............................................................................................................................................. 16 

Página 2 

Día 8–10: Storage base ....................................................................................................................................................... 17 SPRINT 2 – Capa de eventos y procesamiento ...................................................................................................................... 18 Día 1–4: Kafka ...................................................................................................................................................................... 18 Día 5–7: Flink ....................................................................................................................................................................... 18 Día 8–10: Integración Kafka–Flink ...................................................................................................................................... 19 SPRINT 3 – Data Lake y Catálogo ............................................................................................................................................ 20 Día 1–4: Iceberg + catálogo ................................................................................................................................................ 20 Día 5–7: Integración Flink → Iceberg ................................................................................................................................. 20 Día 8–10: Trino .................................................................................................................................................................... 20 SPRINT 4 – Data Warehouse y Serving ................................................................................................................................... 22 Día 1–5: PostgreSQL HA ...................................................................................................................................................... 22 Día 6–8: Redis HA ................................................................................................................................................................ 22 Día 9–10: APIs ...................................................................................................................................................................... 23 

Página 3 

## Componentes, versiones, rol y compatibilidad con Rocky Linux 10 

## Observaciones del stack 

Separación clara de responsabilidades 

- Kafka → transporte 

- Flink → procesamiento 

- Redis → serving de baja latencia 

- PostgreSQL → analítica estructurada 

- Iceberg/MinIO → almacenamiento histórico escalable 

- Trino → SQL sobre lake 

## Alta disponibilidad garantizada 

- PostgreSQL → Patroni + etcd + VIP (RPO≈0) 

- Redis → Sentinel 

- Kafka → 3 brokers 

- MinIO → distribuido 

## Arquitectura sin dependencia de software propietario 

- 100% open source 

- Independiente de vendor lock-in 

## Compatible con virtualización 

- No requiere bare-metal 

- Funciona bajo ESXi/VMware con anti-affinity 

## Lista completa de componentes y versiones 

|No.|Componente|Versión|Función y objetivo del componente|
|---|---|---|---|
|1|Rocky Linux|10.x|Sistema operativo base de todos los nodos.<br>Proporciona estabilidad, compatibilidad binaria<br>Enterprise Linux y soporte SELinux/systemd para<br>operación segura enproducción.|
|2|Podman|5.x|Runtime de contenedores OCI sin daemon. Permite<br>despliegue reproducible y gestión vía systemd.<br>Asegura aislamiento y portabilidad de servicios.|
|3|Apache Kafka (KRaft)|4.0.1|Backbone de eventos. Transporte confiable de<br>mensajes para Transactional Outbox (near-realtime)<br>y replicación diferida. Garantiza durabilidad y<br>ordenamiento.|
|4|Apache Flink|1.20.1|Motor de procesamiento streaming/batch. Ejecuta<br>jobs de ingestión, transformación, enriquecimiento<br>y carga hacia Redis (serving) o Iceberg (lakehouse).|
|5|MinIO (S3 compatible)|RELEASE.2026-<br>02-07T07-43-<br>34Z|Almacenamiento distribuido tipo S3. Base del Data<br>Lake (raw/warm/cold), checkpoints de Flink y<br>almacenamiento de tablas Iceberg.|
|6|Apache Iceberg|1.9.2|Formato de tablas transaccionales para Data Lake.<br>Permite ACID, versionado, time-travel y evolución<br>de esquema sobre MinIO.|
|7|Apache Gravitino (Iceberg<br>REST Catalog)|1.1.0|Implementación REST del catálogo Iceberg.<br>Centraliza metadatos de tablas y permite acceso<br>consistente desde FlinkyTrino.|



Página 4 

||||Bluepoint|
|---|---|---|---|
|8|Trino|479|Motor SQL distribuido. Permite consultas analíticas<br>sobre Iceberg (MinIO). Usado para acceso ocasional<br>de Power BI al Data Lake.|
|9|PostgreSQL (DW)|17.9|Motor del Data Warehouse institucional. Ejecuta<br>consultas analíticas frecuentes y alimenta Power BI.<br>Configurado en clúster HA con replicación síncrona<br>(RPO≈0).|
|10|Patroni|4.0.3|Orquestador HA para PostgreSQL. Gestiona failover<br>automático y mantiene consistencia en replicación<br>síncrona.|
|11|etcd|3.5.16|Distributed Configuration Store (DCS) usado por<br>Patroni para coordinación y quórum del clúster<br>PostgreSQL.|
|12|HAProxy|2.8.6|Balanceador de carga TCP para PostgreSQL. Enruta<br>conexiones al nodo primario activo del clúster.|
|13|Keepalived|2.2.8|Proporciona VIP flotante para endpoint único del<br>DW. Garantiza continuidad ante caída de nodo<br>primario.|
|14|PgBouncer|1.24.0|Pool de conexiones PostgreSQL. Estabiliza alta<br>concurrencia (Power BI, APIs) y evita sobrecarga del<br>primario.|
|15|Redis (Serving Layer)|7.4.x|Cache de alta disponibilidad para consultas de<br>saldos y movimientos recientes con latencia<br>mínima.|
|16|Redis Sentinel|7.4.x|Gestión de failover automático del clúster Redis.<br>Garantiza continuidad en capa de serving.|
|17|Apache Airflow|3.1.7|Orquestador de workflows. Gestiona DAGs de<br>replicación diferida, backfills, validaciones DQ y<br>reconciliaciones.|
|18|Great Expectations|1.11.1|Framework de calidad de datos. Ejecuta<br>validaciones automáticas en procesos batch y<br>lakehouse.|
|19|OpenMetadata|1.12.1|Plataforma de gobierno y catálogo de datos.<br>Proporciona linaje, ownership y trazabilidad<br>institucional.|
|20|OpenSearch|2.18.0|Motor de indexación requerido por OpenMetadata<br>para búsqueda y almacenamiento de metadatos.|
|21|Prometheus|3.5.1|Recolección de métricas de infraestructura y<br>servicios.|
|22|Grafana|12.3.4|Visualización de métricas, logs y trazas. Consolida<br>observabilidad.|
|23|Loki|3.6.5|Centralización de logs de todos los servicios.|
|24|Tempo|2.10.1|Almacenamiento de trazas distribuidas<br>(OpenTelemetry).|
|25|OpenTelemetry Collector|0.146.1|Recepción y exportación de métricas/trazas desde<br>aplicaciones hacia Tempo y Prometheus.|
|26|Grafana Alloy|1.6.0|Agente unificado para envío de logs y métricas<br>desde nodos hacia la plataforma de observabilidad.|



Página 5 

## Organización de Clústers 

Página 6 

## Organización de VMs standalone 

Página 7 

## Componentes mandatorios (mínimos indispensables) 

## Base de plataforma (sin esto no hay operación) 

- Rocky Linux 10.x 

- Podman 5.x + systemd 

- Grafana Alloy 1.6.0 + node-exporter _(para visibilidad mínima; si se elimina, habrá cero visibilidad)_ 

## Streaming / eventos (para outbox y replica) 

- Apache Kafka 4.0.1 (KRaft, 3 nodos) 

## Procesamiento (para transformar y servir) 

- Apache Flink 1.20.1 (JobManager + TaskManagers) 

## Data Lakehouse (warm/cold) 

- MinIO RELEASE.2026-02-07T07-43-34Z (3 nodos) 

- Apache Iceberg 1.9.2 

- Iceberg REST Catalog (Apache Gravitino 1.1.0) (1 instancia basta en mínimo; 2 recomendado) 

## Serving online (saldo/movimientos) 

- Redis 7.4.x 

- Redis Sentinel 7.4.x (mandatorio si se exige HA en serving online) 

## Data Warehouse en HA (mandatorio por tu requisito) 

- PostgreSQL 17.9 (3 nodos en HA) 

- Patroni 4.0.3 

- etcd 3.5.16 

- HAProxy 2.8.6 

- Keepalived 2.2.8 (para VIP y endpoint único) 

## Componentes opcionales (recomendados, pero no bloquean el arranque) 

## SQL on lake (acceso ocasional Power BI al datalake) 

- Trino 479 → Opcional (solo si Power BI consultará directamente el lake). 

   - Si Power BI puede vivir 100% en DW: Trino se puede aplazar. 

## Orquestación / scheduling (para réplica diferida y mantenimiento) 

- Airflow 3.1.7 → Opcional en MVP si inicialmente ejecutan procesos con systemd timers/cron, pero muy recomendable para operar sin “mano humana”. 

## Data quality (evidencia) 

- Great Expectations 1.11.1 → Opcional en MVP; recomendable para control formal y auditoría. 

## Gobierno/catálogo/linaje 

- OpenMetadata 1.12.1 + OpenSearch 2.18.0 → Opcional para “mínimos indispensables” (la plataforma corre sin esto). 

   - En MVP puedes sustituir por: naming conventions + repositorio de schemas + runbooks + tablas de auditoría (outbox/watermarks/DLQ). 

## Observabilidad completa (recomendable, pero no bloqueante) 

- Prometheus 3.5.1 

- Grafana 12.3.4 

- Loki 3.6.5 

- Tempo 2.10.1 

- OpenTelemetry Collector 0.146.1 

Página 8 

En “mínimos indispensables” se puede operar solo con logs locales + Alloy enviando a un syslog / stack reducido, pero en producción real es conveniente levantar al menos Prometheus + Grafana. 

## Pool de conexiones al DW 

- PgBouncer 1.24.0 → Opcional, pero muy recomendable con Power BI y alta concurrencia. 

## Asignación de recursos 

## Datacenter principal 

|VM|vCPU|RAM|Disco|Rol|Componentes principales|
|---|---|---|---|---|---|
|kaf-1|4|8 GB|200 GB|Kafka|Kafka (KRaft) + Alloy + node-exporter|
|kaf-2|4|8 GB|200 GB|Kafka|Kafka (KRaft) + Alloy + node-exporter|
|kaf-3|4|8 GB|200 GB|Kafka|Kafka (KRaft) + Alloy + node-exporter|
|stg-1|4|8 GB|500 GB|Storage|MinIO distributed + Alloy + node-exporter|
|stg-2|4|8 GB|500 GB|Storage|MinIO distributed + Alloy + node-exporter|
|stg-3|4|8 GB|500 GB|Storage|MinIO distributed + Alloy + node-exporter|
|dw-1|4|14 GB|200 GB|DW HA|etcd + Postgres + Patroni + HAProxy + Keepalived (+ opc<br>PgBouncer) + Alloy|
|dw-2|4|14 GB|200 GB|DW HA|etcd + Postgres + Patroni + HAProxy + Keepalived (+ opc<br>PgBouncer) + Alloy|
|dw-3|2|4 GB|200 GB|DW HA<br>(witness)|etcd (quórum) +_(opcional liviano)_HAProxy standby +<br>Alloy|
|cmp-1|4|12 GB|200 GB|Compute|Flink TaskManager + Trino worker + Alloy|
|cmp-2|4|12 GB|200 GB|Compute|Flink TaskManager + Trino worker + Alloy|
|cmp-3|4|12 GB|200 GB|Compute|Flink TaskManager + Trino worker + Alloy|
|platform-<br>apps-1|4|16 GB|200 GB|Standalone|Flink JobManager + Trino Coordinator + Gravitino (Iceberg<br>REST) + Redis Primary + Redis Sentinel +_(Airflow en MVP-_<br>_2)_+ Alloy|
|platform-db-<br>1|3|8 GB|200 GB|Standalone|Postgres meta (audit/watermarks) + Redis Replica + Redis<br>Sentinel + Alloy|
|obs-1|4|8 GB|500 GB|Standalone|Prometheus+Grafana (MVP-2) + Redis Sentinel + Alloy<br>_(Loki/Tempo opcional)_|



## Datacenter alterno 

## Estrategia de asignación de recursos para el Datacenter Alterno 

La asignación de recursos del datacenter alterno responde a una estrategia deliberada de continuidad operativa inteligente , cuyo objetivo es equilibrar tres factores clave: 

1. Protección de los servicios críticos del negocio 

2. Preservación íntegra de la información (Data Lake y Data Warehouse) 

3. Optimización del uso de infraestructura y costos operativos 

Página 9 

## Enfoque general de la estrategia 

A diferencia de un esquema tradicional de alta disponibilidad donde se replica toda la infraestructura con la misma capacidad, en este caso se adopta un enfoque más eficiente: 

No todos los componentes de la plataforma requieren el mismo nivel de capacidad en un escenario de contingencia. 

Por ello, la arquitectura del datacenter alterno se ha diseñado diferenciando claramente dos tipos de funciones: 

## a) Funciones críticas (alta prioridad operativa) 

Son aquellas que deben mantenerse activas incluso ante la caída total del datacenter principal: 

- Captura y transporte de eventos (Kafka) 

- Procesamiento near real-time (Flink) 

- Consulta online de saldos y movimientos (Redis + API) 

Estas funciones están directamente relacionadas con la operación diaria del negocio y la experiencia del cliente , por lo que se mantienen con una capacidad suficiente para operar de forma efectiva en contingencia. 

b) Funciones de preservación de datos (alta prioridad de integridad, baja prioridad operativa) Corresponden a: 

- Data Lake (MinIO + Iceberg) 

- Data Warehouse (PostgreSQL) 

En el datacenter alterno, estas capas no están dimensionadas para soportar carga analítica intensiva, sino para: 

- garantizar que no se pierda información 

- permitir la recuperación completa del sistema 

- habilitar la reconstrucción del entorno analítico una vez restablecido el datacenter principal 

## c) Asignación de recursos 

|VM|vCPU|RAM|Disco|Rol|Componentes principales|
|---|---|---|---|---|---|
|dr-kaf-1|4|8 GB|200 GB|Kafka DR|Kafka (KRaft) + Alloy + node-exporter|
|dr-kaf-2|4|8 GB|200 GB|Kafka DR|Kafka (KRaft) + Alloy + node-exporter|
|dr-kaf-3|4|8 GB|200 GB|Kafka DR|Kafka (KRaft) + Alloy + node-exporter|
|dr-stg-1|2|6 GB|500 GB|Storage DR|MinIO distributed + Alloy + node-exporter|
|dr-stg-2|2|6 GB|500 GB|Storage DR|MinIO distributed + Alloy + node-exporter|
|dr-stg-3|2|6 GB|500 GB|Storage DR|MinIO distributed + Alloy + node-exporter|
|dr-dw-1|2|8 GB|200 GB|DW HA DR|etcd + Postgres + Patroni + HAProxy + Keepalived + Alloy|
|dr-dw-2|2|8 GB|200 GB|DW HA DR|etcd + Postgres + Patroni + HAProxy + Keepalived + Alloy|
|dr-dw-3|1|3 GB|200 GB|DW HA DR<br>(witness)|etcd (quórum) + Alloy|
|dr-cmp-1|4|10 GB|200 GB|Compute<br>DR|Flink JobManager + Flink TaskManager + Alloy|
|dr-cmp-2|3|8 GB|200 GB|Compute<br>DR|Flink TaskManager + Alloy|
|platform-<br>apps-dr|3|10 GB|200 GB|Standalone<br>DR|Redis Primary (promovible) + Redis Sentinel + API online<br>DR + Alloy|



Página 10 

|platform-db-<br>dr|2|6 GB|200 GB|Standalone<br>DR|Postgres meta mínimo + Redis Replica + Redis Sentinel +<br>Alloy|
|---|---|---|---|---|---|
|obs-dr-1|1|2 GB|200 GB|Standalone<br>DR|Prometheus básico + Redis Sentinel + Alloy|



## Principales decisiones de diseño 

## Reducción de capacidad en Data Lake (MinIO) 

Aunque se mantiene la arquitectura distribuida de 3 nodos (por requerimientos de resiliencia), se reducen CPU y memoria porque: 

- el objetivo no es ejecutar procesamiento intensivo 

- el workload en contingencia es principalmente: 

   - replicación 

   - escritura secuencial 

   - almacenamiento 

## Reducción de capacidad en Data Warehouse 

El clúster PostgreSQL en el datacenter alterno: 

- mantiene alta disponibilidad (Patroni + etcd) 

- pero reduce significativamente su capacidad de cómputo 

Esto se debe a que en contingencia: 

- no se ejecutarán dashboards masivos (Power BI) 

- no se soportarán múltiples usuarios concurrentes 

- el uso principal será: 

   - validación 

   - recuperación 

   - consultas puntuales 

## Mantenimiento completo de Kafka 

Kafka no se reduce en capacidad porque: 

- es el punto de entrada del flujo de datos 

- cualquier pérdida o retraso aquí impacta todo el sistema 

- es crítico para: 

   - continuidad del negocio 

   - consistencia de datos 

   - recuperación posterior 

## Dimensionamiento mínimo viable de Flink 

El clúster de Flink en el site alterno: 

- se reduce a un tamaño mínimo funcional 

- ejecuta únicamente los jobs críticos de serving 

Esto permite: 

- reconstruir estados (ej. saldos) 

- mantener procesamiento de eventos 

- evitar consumo innecesario de recursos 

## Priorización del Serving Online (Redis + API) 

El componente de serving (Redis + API): 

Página 11 

- se mantiene con capacidad suficiente 

- es promovible a primario en contingencia 

Esto garantiza: 

- continuidad de consultas de clientes 

- disponibilidad de saldos y movimientos 

- soporte a canales digitales 

## Observabilidad básica 

En el datacenter alterno se implementa únicamente: 

- monitoreo básico 

- métricas esenciales 

Se evita replicar toda la plataforma de observabilidad porque: 

- no es crítica para la operación inmediata 

- reduce significativamente el consumo de recursos 

Página 12 

## Sprint Planning – Implementación Software Base 

Duración por sprint 

- 2 semanas (10 días hábiles) 

- Validación continua, sin esperar al final del sprint 

## Características del plan 

## 1. Validación temprana 

Cada bloque técnico tiene validación inmediata → evita retrabajo. 

## 2. Construcción incremental 

Orden: 

1. Infraestructura 

2. Eventos 

3. Procesamiento 

4. Storage 

5. Serving 

6. Gobierno 

## 3. Separación de responsabilidades 

- Cooperativa → instalación 

- Bluepoint → validación y control de calidad 

## Resumen de Sprints – Implementación Plataforma Big Data 

|Sprint|Semanas|Objetivo principal|Resultado esperado|
|---|---|---|---|
|Sprint 1|Semana<br>1–2|Documentación, validación y estabilización<br>de infraestructura base|Infraestructura auditada, documentada y<br>validada; runtime y observabilidad base<br>operativos|
|Sprint 2|Semana<br>3–4|Implementación de capa de eventos y<br>procesamiento streaming (Kafka + Flink)|Pipeline streaming operativo (Kafka → Flink) con<br>validación end-to-end|
|Sprint 3|Semana<br>5–6|Implementación de Data Lake y catálogo<br>(MinIO + Iceberg + Gravitino + Trino)|Data Lake funcional con capacidades de consulta<br>SQL sobre datos almacenados|
|Sprint 4|Semana<br>7–8|Implementación de capa de serving<br>(PostgreSQL HA + Redis HA + APIs)|Plataforma lista para consumo con alta<br>disponibilidad en almacenamiento y serving|



Página 13 

## SPRINT 1 – Base general de la plataforma 

Objetivo 

Dejar operativos los componentes base de infraestructura: 

- Sistema operativo preparado 

- Podman + systemd 

- Networking interno 

- Observabilidad básica 

- Almacenamiento (MinIO base) 

## Día 1: Preparación de equipos y entorno 

Tarea 0.1 – Kickoff técnico operativo 

Responsable: Bluepoint + Cooperativa 

Objetivo: 

- Alinear equipos técnicos (infraestructura + data + plataforma) 

- • Definir flujo de trabajo y responsabilidades 

Entregables: 

- matriz RACI 

- canales de comunicación 

- acuerdos de validación 

Tarea 0.2 – Setup de herramienta de seguimiento (Microsoft Teams) Responsable: Cooperativa 

Actividades: 

- creación de equipo: BigData-Platform 

- canales: 

   - #general 

   - #infraestructura 

   - #data-platform 

   - #validaciones 

   - #incidentes 

- integración con planner o listas 

OK: 

- todos los equipos con acceso 

- estructura clara de comunicación 

Día 1–2: Documentación y preparación de nodos Tarea 1.1.1 – Documentación de inventario de VMs Responsable: Cooperativa 

Contenido mínimo: 

- nombre VM 

- cluster / rol 

- vCPU, RAM, disco 

Página 14 

- IP y hostname 

- datacenter (principal / secundario) 

Formato: 

- Excel 

## Tarea 1.1.2 – Documentación de red 

Responsable: Cooperativa 

Contenido: 

- topología de red 

- rangos IP 

- puertos abiertos por componente 

- reglas firewall 

Tarea 1.1.3 – Documentación de particionado de disco Responsable: Cooperativa 

Contenido: 

- estructura real implementada 

- tamaños por partición 

- puntos de montaje 

## Tarea 1.1.4 – Validación de documentación 

Responsable: Bluepoint 

OK: 

- consistencia vs diseño 

- cobertura completa 

- sin ambigüedades 

## Tarea 1.2 – Validación base OS 

Responsable: Bluepoint 

Validación: 

- conectividad entre nodos 

- latencia interna 

- mounts correctos 

- uso de disco y permisos 

## Día 3–4: Contenedores y runtime 

Tarea 1.3 – Instalación de Podman + systemd 

Responsable: Cooperativa 

Descarga / Docs: 

- Podman: https://podman.io/getting-started/installation 

Página 15 

- Docs: https://docs.podman.io 

Actividades: 

- instalación de podman 

- configuración rootless (opcional) 

- integración con systemd 

## Tarea 1.4 – Validación contenedores 

Responsable: Bluepoint 

Validación: 

- ejecución de contenedor de prueba 

- persistencia de volúmenes 

- reinicio automático (systemd) 

## Día 5–7: Observabilidad base 

## Tarea 1.5 – Instalación Alloy + node-exporter 

Responsable: Cooperativa 

## Descarga: 

- Alloy: https://grafana.com/docs/alloy/latest/ 

- Node exporter: https://prometheus.io/download/ 

## Actividades: 

- despliegue en todos los nodos 

- configuración de métricas básicas 

## Tarea 1.6 – Instalación Prometheus (obs-1) 

Responsable: Cooperativa 

Descarga: 

- https://prometheus.io/download/ 

Docs: 

- https://prometheus.io/docs/introduction/overview/ 

## Tarea 1.7 – Validación observabilidad 

Responsable: Bluepoint 

Validación: 

- scrape de métricas 

- visibilidad de todos los nodos 

- • alertas básicas 

Página 16 

## Día 8–10: Storage base 

Tarea 1.8 – Instalación MinIO cluster (principal + DR) Responsable: Cooperativa 

Descarga: 

- https://min.io/download 

Docs: 

- https://min.io/docs/minio/linux/index.html 

Actividades: 

- despliegue distribuido (3 nodos) 

- configuración de buckets base 

## Tarea 1.9 – Validación MinIO 

Responsable: Bluepoint 

Validación: 

- acceso distribuido 

- escritura/lectura 

- resiliencia (simulación de nodo caído) 

Página 17 

SPRINT 2 – Capa de eventos y procesamiento Objetivo 

- Kafka operativo 

- Flink operativo 

- Base de procesamiento streaming 

## Día 1–4: Kafka 

Tarea 2.1 – Instalación Kafka (KRaft mode) Responsable: Cooperativa 

Descarga: 

- https://kafka.apache.org/downloads 

Docs: 

• https://kafka.apache.org/documentation/ Actividades: 

- configuración KRaft (sin Zookeeper) 

- cluster de 3 nodos 

- configuración de listeners internos 

Tarea 2.2 – Validación Kafka 

Responsable: Bluepoint 

Validación: 

- creación de topics 

- producción/consumo 

- replicación interna 

## Día 5–7: Flink 

Tarea 2.3 – Instalación Flink cluster 

Responsable: Cooperativa 

Descarga: 

- https://flink.apache.org/downloads/ 

Docs: 

• https://nightlies.apache.org/flink/flink-docs-stable/ Actividades: 

- JobManager 

- TaskManagers 

- configuración cluster 

Tarea 2.4 – Validación Flink Responsable: Bluepoint 

Página 18 

Validación: 

- job de prueba 

- integración con Kafka 

- procesamiento básico 

## Día 8–10: Integración Kafka–Flink 

Tarea 2.5 – Deploy job streaming básico Responsable: Cooperativa 

Actividades: 

- job de lectura Kafka → salida simple 

## Tarea 2.6 – Validación end-to-end 

Responsable: Bluepoint 

Validación: 

- flujo completo Kafka → Flink 

- • latencia 

- estabilidad 

Página 19 

## SPRINT 3 – Data Lake y Catálogo 

Objetivo 

- Iceberg operativo 

- Catálogo REST 

- integración con almacenamiento 

## Día 1–4: Iceberg + catálogo 

Tarea 3.1 – Instalación Iceberg REST Catalog (Gravitino) Responsable: Cooperativa 

Descarga: 

- https://gravitino.apache.org/ 

Docs: 

- https://gravitino.apache.org/docs/latest/ 

## Tarea 3.2 – Validación catálogo 

Responsable: Bluepoint 

Validación: 

- creación de schemas 

- registro de tablas 

## Día 5–7: Integración Flink → Iceberg 

Tarea 3.3 – Configuración writer Iceberg Responsable: Cooperativa 

## Tarea 3.4 – Validación ingestion 

Responsable: Bluepoint 

Validación: 

- escritura en Iceberg 

- lectura desde MinIO 

## Día 8–10: Trino 

Tarea 3.5 – Instalación Trino Responsable: Cooperativa 

Descarga: 

• https://trino.io/download.html Docs: 

- https://trino.io/docs/current/ 

Página 20 

Tarea 3.6 – Validación SQL Responsable: Bluepoint 

Validación: 

- queries sobre Iceberg 

- • performance básica 

Página 21 

SPRINT 4 – Data Warehouse y Serving Objetivo 

- PostgreSQL HA 

- Redis HA 

- APIs base 

## Día 1–5: PostgreSQL HA 

Tarea 4.1 – Instalación PostgreSQL 

Responsable: Cooperativa 

Descarga: 

• https://www.postgresql.org/download/ Docs: 

- https://www.postgresql.org/docs/ 

## Tarea 4.2 – Patroni + etcd 

Responsable: Cooperativa 

Descarga: 

- https://patroni.readthedocs.io 

- https://etcd.io/docs/ 

Tarea 4.3 – Validación HA Responsable: Bluepoint 

Validación: 

- failover 

- replicación 

## Día 6–8: Redis HA 

Tarea 4.4 – Instalación Redis + Sentinel Responsable: Cooperativa 

Descarga: 

- https://redis.io/download/ 

Docs: 

- https://redis.io/docs/ 

Tarea 4.5 – Validación Redis Responsable: Bluepoint 

Página 22 

Día 9–10: APIs 

Tarea 4.6 – Deploy API base Responsable: Cooperativa 

Tarea 4.7 – Validación end-to-end Responsable: Bluepoint 

Página 23 

