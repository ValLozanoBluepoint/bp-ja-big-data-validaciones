# Graph Report - validaciones  (2026-08-20)

## Corpus Check
- 66 files · ~186,520 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 556 nodes · 932 edges · 53 communities (47 shown, 6 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 25 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `8c7201a6`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Informe de validación Flink DR (HTML)
- Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)
- validate_haproxy_minio_dr.sh
- validate_haproxy_minio_principal.sh
- PostgreSQL 17.10 + Patroni 4.1 (pbigd-dlh01, ROL: LEADER/PRIMARIO)
- Tabla de Estado de Implementación y Validación
- Guía de preparación — Validación de integración Trino → MinIO/AIStor (lectura/escritura de datos Iceberg)
- graphify skill (SKILL.md)
- validate_trino_minio_principal.sh
- Guía de ejecución — Aprovisionamiento de metadatos en PostgreSQL
- validate_flink_dr.sh
- validate_flink_principal.sh
- validate_gravitino_principal.sh
- validate_kafka_dr.sh
- validate_kafka_principal.sh
- validate_obs_stack_dr.sh
- validate_kafka_flink_dr_flink.sh
- provision_metadatos_postgresql.sh
- validate_obs_stack_principal.sh
- validate_trino_gravitino_principal.sh
- validate_trino_principal.sh
- validate_redis_dr.sh
- Arquitectura Flink + MinIO + Gravitino (Diagrama)
- validate_redis_principal.sh
- validate_flink_gravitino_principal.sh
- validate_kafka_flink_dr_kafka.sh
- validate_kafka_flink_principal_flink.sh
- validate_flink_minio_dr.sh
- flink-conf.yaml (checkpoints/savepoints hacia MinIO)
- Guía de validación — Gravitino (Iceberg REST Catalog)
- validate_trino_minio_principal.sh
- Flink-MinIO Integration Validation Report (Principal)
- validate_kafka_flink_principal_kafka.sh
- validate_flink_minio_principal.sh
- validate_minio_dr.sh
- validate_minio_principal.sh
- validate_obs_agents.sh
- Gravitino Warehouse Points to /tmp Instead of S3/AIStor
- Trino Cluster Validation Report (Coordinator, plat-apps01)
- Informe de validación Kafka Principal (HTML)
- validate_postgresql_dr.sh
- Flink Side Validation (Principal)
- VIP 172.17.210.182 MASTER on stg01-cont (DR)
- HAProxy/Keepalived Not Deployed on stg03-cont (DR)
- VIP 172.17.210.62 MASTER on stg01 (Principal)
- Guía de pasos manuales — Integración Trino → MinIO/AIStor (datos Iceberg)
- Guía de preparación — Validación de integración Kafka ↔ Flink
- validacion-node-exporter.sh
- Ejecución de validación de observabilidad (comandos por DC)
- validate_gravitino_principal.sh
- Guía de Validación — Cluster MinIO
- validate_postgresql_principal.sh
- ejecutar_en_podman.sh

## God Nodes (most connected - your core abstractions)
1. `Informe de validación Flink DR (HTML)` - 15 edges
2. `provision_metadatos_postgresql.sh script` - 13 edges
3. `Guía de ejecución — Aprovisionamiento de metadatos en PostgreSQL` - 11 edges
4. `Guía de preparación — Validación de integración Trino → MinIO/AIStor (lectura/escritura de datos Iceberg)` - 11 edges
5. `Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)` - 11 edges
6. `Tabla de Estado de Implementación y Validación` - 11 edges
7. `validate_flink_dr.sh script` - 10 edges
8. `validate_flink_principal.sh script` - 10 edges
9. `graphify skill (SKILL.md)` - 10 edges
10. `Resumen Ejecutivo — Validación Flink (Principal + DR)` - 10 edges

## Surprising Connections (you probably didn't know these)
- `context/Contexto-extra-correos.md (2026-08-06)` --semantically_similar_to--> `context/Contexto-extra-correos.md (2026-08-06)`  [INFERRED] [semantically similar]
  validacion_flink_gravitino/preparacion_validacion_flink_gravitino.md → validacion_trino_gravitino/preparacion_validacion_trino_gravitino.md
- `validate_flink_minio_principal.sh` --semantically_similar_to--> `validate_trino_minio_principal.sh`  [INFERRED] [semantically similar]
  validacion_flink_minio/preparacion_integracion_flink_minio.md → validacion_trino_minio/preparacion_integracion_trino_minio.md
- `context/Contexto-extra-correos.md (2026-08-06)` --semantically_similar_to--> `context/Contexto-extra-correos.md (2026-08-01)`  [INFERRED] [semantically similar]
  validacion_trino_gravitino/preparacion_validacion_trino_gravitino.md → validacion_haproxy_minio/guia_pasos_manuales_haproxy_minio.md
- `Project CLAUDE.md — graphify usage rules` --conceptually_related_to--> `graphify skill (SKILL.md)`  [INFERRED]
  CLAUDE.md → .claude/skills/graphify/SKILL.md
- `Informe de validación Flink DR (HTML)` --references--> `MinIO DR (pbigd-stg01-cont:9000)`  [EXTRACTED]
  validacion_flink/informe_flink_dr.html → logs/flink/dr/pbigd-plat-apps01-cont.txt

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **VIP Failover Cluster Validation (Principal, stg01-03)** — logs_haproxy_minio_principal_pbigd_stg01_report, logs_haproxy_minio_principal_pbigd_stg02_report, logs_haproxy_minio_principal_pbigd_stg03_report [EXTRACTED 1.00]
- **Kafka to Flink End-to-End Integration Flow (Principal)** — logs_kafka_flink_principal_test_topic, logs_kafka_flink_principal_kafka_brokers, logs_kafka_flink_principal_flink_job [EXTRACTED 1.00]
- **Trino Cluster Topology (Coordinator + 3 Workers)** — logs_trino_pbigd_plat_apps01_report, logs_trino_pbigd_proc01_report, logs_trino_pbigd_proc02_report, logs_trino_pbigd_proc03_report [EXTRACTED 1.00]
- **Hallazgo H1 afecta tres scripts de validación con mismo chequeo de warehouse S3** — hallazgos_transversales_h1_warehouse_gravitino_no_s3, hallazgos_transversales_validate_trino_minio_principal_sh, hallazgos_transversales_validate_flink_gravitino_principal_sh, hallazgos_transversales_validate_trino_gravitino_principal_sh [EXTRACTED 1.00]
- **Tres piezas de configuración pendiente para que Trino/Flink lleguen al lake real vía Gravitino** — configuraciones_pendientes_minio_gravitino_gravitino_iceberg_rest_server_conf, configuraciones_pendientes_minio_gravitino_iceberg_properties, configuraciones_pendientes_minio_gravitino_trino_svc_usuario, configuraciones_pendientes_minio_gravitino_gravitino_svc_usuario [EXTRACTED 1.00]
- **Estado desigual en DR entre componentes del lakehouse (Flink confirmado, Trino/Gravitino no)** — validacion_flink_gravitino_preparacion_validacion_flink_gravitino_validate_flink_dr_sh, validacion_gravitino_preparacion_validacion_gravitino_dr_pregunta_abierta, validacion_trino_minio_preparacion_integracion_trino_minio_tension_trino_opcional [INFERRED 0.75]
- **Flujo de validación y corrección de PostgreSQL HA (diagrama → 3 hallazgos → scripts de corrección)** — context_contexto_extra_correos, context_bluepoint_planimplementacion_v2__06042026_postgresql_ha, context_contexto_extra_correos_pgbouncer_vip_keepalived, context_contexto_extra_correos_pentaho_bi_segregation, context_contexto_extra_correos_pgbouncer_auth_hardening [EXTRACTED 0.90]
- **Stack de observabilidad Bluepoint: Alloy (mandatorio) + Prometheus (mandatorio) + Grafana + Loki/Tempo/OTel (opcionales)** — concept_grafana_alloy, concept_prometheus_central, concept_grafana_stack_principal, concept_loki_optional, concept_tempo_optional, concept_otel_collector_optional [EXTRACTED 1.00]
- **Cadena de dependencias del modo contingencia DR: Flink DR + Redis DR + Kafka DR + persistencia local** — concept_error_flink_dr_directorios_faltantes, concept_error_redis_dr_inaccesible, concept_error_kafka_dr_inaccesible, concept_kafka_dr_cluster, concept_redis_dr [INFERRED 0.90]
- **Patrón de coordinación manual por ausencia de SSH sin contraseña entre nodos de integración** — validacion_flink_kafka_preparacion_integracion_kafka_flink [INFERRED 0.85]
- **Diagnóstico y resolución del cluster.id divergente en Kafka Principal** — validacion_kafka_diagnostico_quorum_incompleto, validacion_kafka_informe_kafka_principal, validacion_kafka_diagnostico_quorum_incompleto_cluster_id_mismatch [INFERRED 0.85]
- **Contraste de política de capacidad en DR: Kafka pleno vs Flink/MinIO degradado** — validacion_kafka_preparacion_validacion_kafka, validacion_kafka_informe_kafka_dr [INFERRED 0.75]

## Communities (53 total, 6 thin omitted)

### Community 0 - "Informe de validación Flink DR (HTML)"
Cohesion: 0.07
Nodes (46): FAIL crítico — /data/flink y /var/log/flink no existen en nodos DR, Bug en validate_flink_dr.sh — grep -c bajo set -euo pipefail rompe comparación [[ "$ERRORS" -eq 0 ]], WARN bloqueante — Kafka DR (dr-kaf-1) no alcanzable, FAIL contradictorio — ping a pbigd-proc01-cont falla desde JM DR pese a API REST y logs locales mostrando el nodo activo, FAIL contradictorio — puerto REST 8081 / RPC 6123 no alcanzable TM→JM pese a que la API REST del JM sí responde, WARN bloqueante — Redis DR inaccesible en :6379, Segmentation fault en 'systemctl --user is-active' bajo sudo -u (Podman + systemd --user), Discrepancia de versión Flink — plan especifica 1.20.1, scripts esperan 2.2.1 (+38 more)

### Community 1 - "Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)"
Cohesion: 0.18
Nodes (20): Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026), Estrategia de asignación de recursos del Datacenter Alterno (funciones críticas vs. preservación de datos), Apache Flink — motor de procesamiento streaming/batch, Apache Gravitino — Iceberg REST Catalog, Apache Iceberg — formato de tablas transaccionales, Apache Kafka (KRaft) — backbone de eventos, MinIO — Data Lake distribuido S3-compatible, Stack de observabilidad (Prometheus, Grafana, Loki, Tempo, OTel, Alloy) (+12 more)

### Community 2 - "validate_haproxy_minio_dr.sh"
Cohesion: 0.21
Nodes (9): fail(), header(), info(), is_descendant_of(), manual(), pass(), pending(), validate_haproxy_minio_dr.sh script (+1 more)

### Community 3 - "validate_haproxy_minio_principal.sh"
Cohesion: 0.21
Nodes (9): fail(), header(), info(), is_descendant_of(), manual(), pass(), pending(), validate_haproxy_minio_principal.sh script (+1 more)

### Community 4 - "PostgreSQL 17.10 + Patroni 4.1 (pbigd-dlh01, ROL: LEADER/PRIMARIO)"
Cohesion: 0.30
Nodes (12): Clientes / Aplicaciones (Java, Python/API, BI/Reporting, Pentaho, Apps Móviles, Otros Clientes), Arquitectura PostgreSQL HA Enterprise (Diagrama), ETCD (pbigd-dlh01, 172.17.210.96), ETCD (pbigd-dlh02, 172.17.210.97), ETCD (pbigd-dlh03, 172.17.210.98), HAProxy 2.8 (pbigd-dlh01, 172.17.210.96), HAProxy 2.8 (pbigd-dlh02, 172.17.210.97), PgBouncer 1.24 (pbigd-dlh01, 172.17.210.96) (+4 more)

### Community 5 - "Tabla de Estado de Implementación y Validación"
Cohesion: 0.32
Nodes (12): Flink, Iceberg, Integración Flink - Iceberg, Integración Kafka - Flink, Integración Trino - Iceberg, Kafka, MinIO, Observabilidad (+4 more)

### Community 6 - "Guía de preparación — Validación de integración Trino → MinIO/AIStor (lectura/escritura de datos Iceberg)"
Cohesion: 0.17
Nodes (11): DR — no se genera `validate_trino_minio_dr.sh`, Fuera de alcance de este script, Guía de preparación — Validación de integración Trino → MinIO/AIStor (lectura/escritura de datos Iceberg), Hallazgos documentales a reportar en el informe (no resueltos aquí), Por qué esta validación es distinta a Trino↔Gravitino, Prerrequisito 1 — Clúster Trino sano, Prerrequisito 2 — MinIO sano, Prerrequisito 3 — Catálogo Iceberg de Trino: nombre y ruta NO documentados (+3 more)

### Community 7 - "graphify skill (SKILL.md)"
Cohesion: 0.20
Nodes (11): .claude/CLAUDE.md — graphify skill pointer, graphify reference: add-watch, graphify reference: exports, graphify reference: extraction-spec (subagent prompt), graphify reference: GitHub clone and cross-repo merge, graphify reference: commit hook and CLAUDE.md integration, graphify reference: query, path, explain, graphify reference: transcribe video/audio (+3 more)

### Community 8 - "validate_trino_minio_principal.sh"
Cohesion: 0.05
Nodes (47): bucket warehouse S3 real, Flink, Gravitino (catálogo de metadata), gravitino-iceberg-rest-server.conf, usuario gravitino-svc (lectura/escritura/borrado), /etc/trino/catalog/iceberg.properties, MinIO/AIStor, Trino (+39 more)

### Community 9 - "Guía de ejecución — Aprovisionamiento de metadatos en PostgreSQL"
Cohesion: 0.11
Nodes (18): 10. Resolución de problemas, 1. Qué hace el script, 2. Requisitos previos, 3. Variables de entorno, 4. Cómo se ejecuta, 5. Interpretación de la salida, 6. Idempotencia y re-ejecución, 7.1 Las migraciones deben crear objetos como `meta_owner` (+10 more)

### Community 10 - "validate_flink_dr.sh"
Cohesion: 0.51
Nodes (10): check_systemd(), dr(), fail(), info(), log(), ok(), section(), validate_flink_dr.sh script (+2 more)

### Community 11 - "validate_flink_principal.sh"
Cohesion: 0.51
Nodes (10): check_port_local(), check_systemd_unit(), fail(), info(), log(), ok(), section(), validate_flink_principal.sh script (+2 more)

### Community 12 - "validate_gravitino_principal.sh"
Cohesion: 0.40
Nodes (8): delete_metalake(), fail(), info(), log(), ok(), section(), validate_gravitino_principal.sh script, warn()

### Community 13 - "validate_kafka_dr.sh"
Cohesion: 0.40
Nodes (9): check_port_local(), check_systemd_unit(), fail(), header(), info(), kexec(), pass(), validate_kafka_dr.sh script (+1 more)

### Community 14 - "validate_kafka_principal.sh"
Cohesion: 0.42
Nodes (9): check_port_local(), check_systemd_unit(), fail(), header(), info(), kexec(), pass(), validate_kafka_principal.sh script (+1 more)

### Community 15 - "validate_obs_stack_dr.sh"
Cohesion: 0.49
Nodes (9): check_svc(), dr(), fail(), info(), log(), ok(), section(), validate_obs_stack_dr.sh script (+1 more)

### Community 16 - "validate_kafka_flink_dr_flink.sh"
Cohesion: 0.51
Nodes (9): dr(), fail(), info(), log(), ok(), section(), validate_kafka_flink_dr_flink.sh script, tcp_port_open() (+1 more)

### Community 17 - "provision_metadatos_postgresql.sh"
Cohesion: 0.20
Nodes (17): check_role_attr(), create_login_role(), default_priv_block(), fail(), grant_block(), info(), ok(), PGHOST (+9 more)

### Community 18 - "validate_obs_stack_principal.sh"
Cohesion: 0.60
Nodes (9): check_svc(), fail(), info(), log(), ok(), opt(), section(), validate_obs_stack_principal.sh script (+1 more)

### Community 19 - "validate_trino_gravitino_principal.sh"
Cohesion: 0.47
Nodes (8): fail(), info(), log(), ok(), run_statement(), section(), validate_trino_gravitino_principal.sh script, warn()

### Community 20 - "validate_trino_principal.sh"
Cohesion: 0.47
Nodes (8): fail(), info(), log(), ok(), section(), validate_trino_principal.sh script, tcp_port_open(), warn()

### Community 21 - "validate_redis_dr.sh"
Cohesion: 0.38
Nodes (8): check_port_local(), check_systemd_unit(), fail(), header(), info(), pass(), validate_redis_dr.sh script, warn()

### Community 22 - "Arquitectura Flink + MinIO + Gravitino (Diagrama)"
Cohesion: 0.47
Nodes (9): AIStor (MinIO) — Almacenamiento de objetos, 3 nodos, Aplicación web (Consultas SQL), Flink (JobManager + TaskManagers), Flink web UI (Diagnóstico), Gravitino (Iceberg REST Catalog / metadata), Arquitectura Flink + MinIO + Gravitino (Diagrama), PowerBI (Consultas SQL), StreamPark (GUI principal) (+1 more)

### Community 23 - "validate_redis_principal.sh"
Cohesion: 0.38
Nodes (8): check_port_local(), check_systemd_unit(), fail(), header(), info(), pass(), validate_redis_principal.sh script, warn()

### Community 24 - "validate_flink_gravitino_principal.sh"
Cohesion: 0.67
Nodes (8): fail(), info(), log(), ok(), run_flink_sql(), section(), validate_flink_gravitino_principal.sh script, warn()

### Community 25 - "validate_kafka_flink_dr_kafka.sh"
Cohesion: 0.56
Nodes (8): dr(), fail(), info(), log(), ok(), section(), validate_kafka_flink_dr_kafka.sh script, warn()

### Community 26 - "validate_kafka_flink_principal_flink.sh"
Cohesion: 0.56
Nodes (8): fail(), info(), log(), ok(), section(), validate_kafka_flink_principal_flink.sh script, tcp_port_open(), warn()

### Community 27 - "validate_flink_minio_dr.sh"
Cohesion: 0.58
Nodes (8): dr(), fail(), info(), log(), ok(), section(), validate_flink_minio_dr.sh script, warn()

### Community 28 - "flink-conf.yaml (checkpoints/savepoints hacia MinIO)"
Cohesion: 0.11
Nodes (18): context/Contexto-extra-correos.md (2026-08-06), JAR iceberg-flink-runtime-1.20-1.9.2.jar, Renombre flink-conf.yaml → config.yaml (Flink 2.x), Módulo 2 — Plugin S3 y config por TaskManager (manual), flink-conf.yaml (checkpoints/savepoints hacia MinIO), JobManager (pbigd-plat-apps01 / flink-jobmanager), Plugin flink-s3-fs-hadoop, TaskManagers (flink-tm-1/2/3) (+10 more)

### Community 29 - "Guía de validación — Gravitino (Iceberg REST Catalog)"
Cohesion: 0.22
Nodes (8): Guía de validación — Gravitino (Iceberg REST Catalog), Inconsistencias documentales detectadas, Módulo DR — no generado en esta entrega, Sección 1 — Sistema Operativo, Java y prerequisitos, Sección 2 — Servicio y redundancia, Sección 3 — Validación funcional básica (sin MinIO), Sección 4 — Logs y observabilidad, Sección 5 — Recursos

### Community 30 - "validate_trino_minio_principal.sh"
Cohesion: 0.53
Nodes (7): fail(), info(), log(), ok(), section(), validate_trino_minio_principal.sh script, warn()

### Community 31 - "Flink-MinIO Integration Validation Report (Principal)"
Cohesion: 0.25
Nodes (8): Flink Principal Checkpoint Persisted to MinIO Bucket flink-checkpoints, Flink DR Checkpoint Persisted to MinIO DR Bucket flink-checkpoints, config.yaml DR World-Readable (777) with Plaintext S3 Credentials, Flink-MinIO Integration Validation Report (DR), config.yaml Principal World-Readable (755) with Plaintext S3 Credentials, Flink-MinIO Integration Validation Report (Principal), MINIO_ENDPOINT Hardcoded to Single MinIO Node Instead of VIP/DNS in validate_flink_minio_principal.sh, HAProxy+Keepalived Validation Report (Principal stg01, Early Run)

### Community 32 - "validate_kafka_flink_principal_kafka.sh"
Cohesion: 0.61
Nodes (7): fail(), info(), log(), ok(), section(), validate_kafka_flink_principal_kafka.sh script, warn()

### Community 33 - "validate_flink_minio_principal.sh"
Cohesion: 0.64
Nodes (7): fail(), info(), log(), ok(), section(), validate_flink_minio_principal.sh script, warn()

### Community 34 - "validate_minio_dr.sh"
Cohesion: 0.54
Nodes (7): check_port_local(), fail(), header(), info(), pass(), validate_minio_dr.sh script, warn()

### Community 35 - "validate_minio_principal.sh"
Cohesion: 0.54
Nodes (7): check_port_local(), fail(), header(), info(), pass(), validate_minio_principal.sh script, warn()

### Community 36 - "validate_obs_agents.sh"
Cohesion: 0.64
Nodes (7): fail(), info(), log(), ok(), section(), validate_obs_agents.sh script, warn()

### Community 37 - "Gravitino Warehouse Points to /tmp Instead of S3/AIStor"
Cohesion: 0.29
Nodes (7): Flink DR Confirmed but Gravitino DR Status Open Question, hallazgos_transversales.md, Flink-Gravitino Integration Validation Report (Principal), Gravitino Warehouse Points to /tmp Instead of S3/AIStor, Trino CREATE TABLE Fails: No Factory for Location /tmp/... — Gravitino Metadata Write Issue, Trino Sees Flink-Created Iceberg Table Metadata but 0 Rows — Catalog/Data Inconsistency, Trino-Gravitino Integration Validation Report (Principal)

### Community 38 - "Trino Cluster Validation Report (Coordinator, plat-apps01)"
Cohesion: 0.29
Nodes (7): Gravitino Validation Report (Principal), Gravitino Deployed with Only 1 Instance, No HA Redundancy, Trino Cluster Validation Report (Principal, run from plat-apps01), Trino Cluster Validation Report (Coordinator, plat-apps01), Trino Cluster Validation Report (Worker, proc01), Trino Cluster Validation Report (Worker, proc02), Trino Cluster Validation Report (Worker, proc03)

### Community 39 - "Informe de validación Kafka Principal (HTML)"
Cohesion: 0.38
Nodes (7): Diagnóstico: cluster Kafka con 1/3 brokers activos, Causa raíz: cluster.id distinto por nodo impide unión al quórum KRaft, Informe de validación Kafka DR (HTML), Informe de validación Kafka Principal (HTML), Reclasificación: node-exporter ausente es informativo porque Alloy embebe prometheus.exporter.unix, Guía de validación — Kafka (Principal y DR), Decisión de diseño: Kafka no reduce capacidad en DR (a diferencia de Flink/MinIO)

### Community 40 - "validate_postgresql_dr.sh"
Cohesion: 0.53
Nodes (8): check_port_local(), check_systemd_unit(), fail(), header(), info(), pass(), validate_postgresql_dr.sh script, warn()

### Community 41 - "Flink Side Validation (Principal)"
Cohesion: 0.50
Nodes (5): Flink SQL Job 36a4e1cc66b1a964dbec6f919dae0520 (Kafka Source), Flink Side Validation (Principal), Kafka Brokers pbigd-kaf01/02/03, Kafka Side Validation (Principal), Test Topic bluepoint-kafka-flink-it-1784670820

### Community 42 - "VIP 172.17.210.182 MASTER on stg01-cont (DR)"
Cohesion: 0.50
Nodes (4): HAProxy+Keepalived Validation Report (DR, stg01-cont), VIP 172.17.210.182 MASTER on stg01-cont (DR), HAProxy+Keepalived Validation Report (DR, stg02-cont), VIP BACKUP Role on stg02-cont (DR)

### Community 43 - "HAProxy/Keepalived Not Deployed on stg03-cont (DR)"
Cohesion: 0.50
Nodes (4): HAProxy/Keepalived Not Deployed on stg03-cont (DR), HAProxy+Keepalived Validation Report (DR, stg03-cont), HAProxy/Keepalived Not Deployed on stg03 (Principal), HAProxy+Keepalived Validation Report (Principal, stg03)

### Community 44 - "VIP 172.17.210.62 MASTER on stg01 (Principal)"
Cohesion: 0.50
Nodes (4): HAProxy+Keepalived Validation Report (Principal, stg01), VIP 172.17.210.62 MASTER on stg01 (Principal), HAProxy+Keepalived Validation Report (Principal, stg02), VIP BACKUP Role on stg02 (Principal)

### Community 45 - "Guía de pasos manuales — Integración Trino → MinIO/AIStor (datos Iceberg)"
Cohesion: 0.50
Nodes (3): Guía de pasos manuales — Integración Trino → MinIO/AIStor (datos Iceberg), Módulo 4 — Least-privilege de las credenciales S3 que usa Trino, Resumen de checks pendientes por permisos o alcance

### Community 51 - "validate_postgresql_principal.sh"
Cohesion: 0.53
Nodes (8): check_port_local(), check_systemd_unit(), fail(), header(), info(), pass(), validate_postgresql_principal.sh script, warn()

## Ambiguous Edges - Review These
- `graphify reference: extraction-spec (subagent prompt)` → `graphify reference: query, path, explain`  [AMBIGUOUS]
  .claude/skills/graphify/references/query.md · relation: references
- `Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)` → `Versiones finales de componentes (Kafka, Flink, Gravitino, Trino, Iceberg)`  [AMBIGUOUS]
  context/versiones-finales.md · relation: conceptually_related_to
- `HA "de papel" (VIP sin failover real)` → `Ciclo crear+eliminar metalake de prueba (force=true)`  [AMBIGUOUS]
  validacion_gravitino/preparacion_validacion_gravitino.md · relation: conceptually_related_to

## Knowledge Gaps
- **100 isolated node(s):** `ejecutar_en_podman.sh script`, `PGHOST`, `PGPORT`, `PGUSER`, `PWVARS` (+95 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **6 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `graphify reference: extraction-spec (subagent prompt)` and `graphify reference: query, path, explain`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._
- **What is the exact relationship between `Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)` and `Versiones finales de componentes (Kafka, Flink, Gravitino, Trino, Iceberg)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `HA "de papel" (VIP sin failover real)` and `Ciclo crear+eliminar metalake de prueba (force=true)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `validate_flink_gravitino_principal.sh` connect `validate_trino_minio_principal.sh` to `flink-conf.yaml (checkpoints/savepoints hacia MinIO)`?**
  _High betweenness centrality (0.006) - this node is a cross-community bridge._
- **What connects `ejecutar_en_podman.sh script`, `PGHOST`, `PGPORT` to the rest of the system?**
  _100 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Informe de validación Flink DR (HTML)` be split into smaller, more focused modules?**
  _Cohesion score 0.07439613526570048 - nodes in this community are weakly interconnected._
- **Should `validate_trino_minio_principal.sh` be split into smaller, more focused modules?**
  _Cohesion score 0.0545790934320074 - nodes in this community are weakly interconnected._