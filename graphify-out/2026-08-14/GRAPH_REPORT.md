# Graph Report - validaciones  (2026-08-14)

## Corpus Check
- 50 files · ~146,863 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 361 nodes · 657 edges · 33 communities (30 shown, 3 thin omitted)
- Extraction: 96% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 22 edges (avg confidence: 0.84)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `d96c409a`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Informe de validación Flink DR (HTML)
- Módulo 2 — Plugin S3 y configuración en cada TaskManager
- Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)
- Stack Observabilidad DC Principal run — pbigd-plat-obs01
- validate_haproxy_minio_dr.sh
- validate_haproxy_minio_principal.sh
- PostgreSQL 17.10 + Patroni 4.1 (pbigd-dlh01, ROL: LEADER/PRIMARIO)
- Tabla de Estado de Implementación y Validación
- graphify skill (SKILL.md)
- validate_flink_dr.sh
- validate_flink_principal.sh
- validate_kafka_dr.sh
- validate_kafka_principal.sh
- validate_obs_stack_dr.sh
- validate_kafka_flink_dr_flink.sh
- validate_obs_stack_principal.sh
- Arquitectura Flink + MinIO + Gravitino (Diagrama)
- validate_kafka_flink_dr_kafka.sh
- validate_kafka_flink_principal_flink.sh
- validate_flink_minio_dr.sh
- validate_kafka_flink_principal_kafka.sh
- validate_flink_minio_principal.sh
- validate_minio_dr.sh
- validate_minio_principal.sh
- validate_obs_agents.sh
- Informe de validación Kafka Principal (HTML)
- Guía de preparación — Validación de integración Flink → MinIO
- validacion-node-exporter.sh
- Ejecución de validación de observabilidad (comandos por DC)
- Guía de Validación — Cluster MinIO
- Guía de validación — Gravitino (Iceberg REST Catalog)
- validate_gravitino_principal.sh
- validate_trino_principal.sh

## God Nodes (most connected - your core abstractions)
1. `Informe de validación Flink DR (HTML)` - 15 edges
2. `Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)` - 11 edges
3. `Tabla de Estado de Implementación y Validación` - 11 edges
4. `validate_flink_dr.sh script` - 10 edges
5. `validate_flink_principal.sh script` - 10 edges
6. `graphify skill (SKILL.md)` - 10 edges
7. `Resumen Ejecutivo — Validación Flink (Principal + DR)` - 10 edges
8. `validate_kafka_flink_dr_flink.sh script` - 9 edges
9. `validate_haproxy_minio_dr.sh script` - 9 edges
10. `validate_haproxy_minio_principal.sh script` - 9 edges

## Surprising Connections (you probably didn't know these)
- `Módulo 6 — Least-privilege de credenciales S3 de Flink` --semantically_similar_to--> `Check 5.1 — Lectura de /etc/keepalived/keepalived.conf`  [INFERRED] [semantically similar]
  validacion_flink_minio/guia_pasos_manuales_flink_minio.md → validacion_haproxy_minio/guia_pasos_manuales_haproxy_minio.md
- `Módulo 2 — Plugin S3 y configuración en cada TaskManager` --semantically_similar_to--> `Check 4.2 — Estado VRRP en journal de Keepalived`  [INFERRED] [semantically similar]
  validacion_flink_minio/guia_pasos_manuales_flink_minio.md → validacion_haproxy_minio/guia_pasos_manuales_haproxy_minio.md
- `Módulo 6 — Least-privilege de credenciales S3 de Flink` --semantically_similar_to--> `Módulo 6 — Prerrequisitos de virtualización (vCenter/ESXi)`  [INFERRED] [semantically similar]
  validacion_flink_minio/guia_pasos_manuales_flink_minio.md → validacion_haproxy_minio/guia_pasos_manuales_haproxy_minio.md
- `Project CLAUDE.md — graphify usage rules` --conceptually_related_to--> `graphify skill (SKILL.md)`  [INFERRED]
  CLAUDE.md → .claude/skills/graphify/SKILL.md
- `Flink Checkpoints/Savepoints hacia MinIO (S3)` --conceptually_related_to--> `MinIO DR (pbigd-stg01-cont:9000)`  [INFERRED]
  validacion_flink/preparacion_checkpoints_minio.md → logs/flink/dr/pbigd-plat-apps01-cont.txt

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Validación de configuración de checkpoints Flink sobre MinIO** — validacion_flink_minio_guia_pasos_manuales_flink_minio_modulo2_plugin_s3, validacion_flink_minio_guia_pasos_manuales_flink_minio_flink_s3_fs_hadoop_plugin, validacion_flink_minio_guia_pasos_manuales_flink_minio_flink_conf_yaml, validacion_flink_minio_guia_pasos_manuales_flink_minio_flink_checkpoints_bucket [EXTRACTED 0.95]
- **Verificación de failover real de HAProxy+Keepalived delante de MinIO** — validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_check_4_4_failover_test, validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_keepalived, validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_vip, validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_minio_cluster [EXTRACTED 0.90]
- **Checklist de prerrequisitos vCenter/ESXi para VIP/failover** — validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_modulo6_vcenter_esxi_prereqs, validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_6_1_ip_reservada_vip, validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_6_2_forged_transmits, validacion_haproxy_minio_guia_pasos_manuales_haproxy_minio_6_3_anti_afinidad_drs [EXTRACTED 0.95]
- **Flujo de validación y corrección de PostgreSQL HA (diagrama → 3 hallazgos → scripts de corrección)** — context_contexto_extra_correos, context_bluepoint_planimplementacion_v2__06042026_postgresql_ha, context_contexto_extra_correos_pgbouncer_vip_keepalived, context_contexto_extra_correos_pentaho_bi_segregation, context_contexto_extra_correos_pgbouncer_auth_hardening [EXTRACTED 0.90]
- **Flujo de integración funcional Kafka→Flink validado end-to-end (topic de prueba, conector SQL, job print)** — logs_integracion_kafka_flink_principal_run, concept_kafka_flink_it_topic, concept_flink_sql_connector_kafka_jar, concept_kafka_principal_cluster, logs_flink_principal_pbigd_apps01_validate_flink_principal_run [EXTRACTED 1.00]
- **Stack de observabilidad Bluepoint: Alloy (mandatorio) + Prometheus (mandatorio) + Grafana + Loki/Tempo/OTel (opcionales)** — concept_grafana_alloy, concept_prometheus_central, concept_grafana_stack_principal, concept_loki_optional, concept_tempo_optional, concept_otel_collector_optional [EXTRACTED 1.00]
- **Cadena de dependencias del modo contingencia DR: Flink DR + Redis DR + Kafka DR + persistencia local** — concept_error_flink_dr_directorios_faltantes, concept_error_redis_dr_inaccesible, concept_error_kafka_dr_inaccesible, concept_kafka_dr_cluster, concept_redis_dr [INFERRED 0.90]
- **Patrón de coordinación manual por ausencia de SSH sin contraseña entre nodos de integración** — validacion_flink_kafka_preparacion_integracion_kafka_flink, validacion_flink_minio_preparacion_integracion_flink_minio [INFERRED 0.85]
- **Diagnóstico y resolución del cluster.id divergente en Kafka Principal** — validacion_kafka_diagnostico_quorum_incompleto, validacion_kafka_informe_kafka_principal, validacion_kafka_diagnostico_quorum_incompleto_cluster_id_mismatch [INFERRED 0.85]
- **Contraste de política de capacidad en DR: Kafka pleno vs Flink/MinIO degradado** — validacion_kafka_preparacion_validacion_kafka, validacion_kafka_informe_kafka_dr [INFERRED 0.75]

## Communities (33 total, 3 thin omitted)

### Community 0 - "Informe de validación Flink DR (HTML)"
Cohesion: 0.11
Nodes (34): FAIL crítico — /data/flink y /var/log/flink no existen en nodos DR, Bug en validate_flink_dr.sh — grep -c bajo set -euo pipefail rompe comparación [[ "$ERRORS" -eq 0 ]], WARN bloqueante — Kafka DR (dr-kaf-1) no alcanzable, FAIL contradictorio — ping a pbigd-proc01-cont falla desde JM DR pese a API REST y logs locales mostrando el nodo activo, FAIL contradictorio — puerto REST 8081 / RPC 6123 no alcanzable TM→JM pese a que la API REST del JM sí responde, WARN bloqueante — Redis DR inaccesible en :6379, Segmentation fault en 'systemctl --user is-active' bajo sudo -u (Podman + systemd --user), Flink Checkpoints/Savepoints hacia MinIO (S3) (+26 more)

### Community 1 - "Módulo 2 — Plugin S3 y configuración en cada TaskManager"
Cohesion: 0.09
Nodes (29): informes/estandar_informes_validacion.md, Bucket MinIO flink-checkpoints, flink-conf.yaml (claves checkpoint/S3), flink-s3-fs-hadoop / flink-s3-fs-presto plugin, iceberg-flink-runtime JAR (integración Flink↔Iceberg/Gravitino, fuera de alcance), Módulo 2 — Plugin S3 y configuración en cada TaskManager, Módulo 6 — Least-privilege de credenciales S3 de Flink, validate_flink_minio_dr.sh (+21 more)

### Community 2 - "Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)"
Cohesion: 0.18
Nodes (20): Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026), Estrategia de asignación de recursos del Datacenter Alterno (funciones críticas vs. preservación de datos), Apache Flink — motor de procesamiento streaming/batch, Apache Gravitino — Iceberg REST Catalog, Apache Iceberg — formato de tablas transaccionales, Apache Kafka (KRaft) — backbone de eventos, MinIO — Data Lake distribuido S3-compatible, Stack de observabilidad (Prometheus, Grafana, Loki, Tempo, OTel, Alloy) (+12 more)

### Community 3 - "Stack Observabilidad DC Principal run — pbigd-plat-obs01"
Cohesion: 0.15
Nodes (19): Grafana Alloy (agente de observabilidad, incl. exporter.unix embebido), Grafana 12.3.4 (stack observabilidad DC Principal), Kafka DR cluster (dr-kaf-1 / pbigd-kaf0X-cont), Kafka Principal cluster (pbigd-kaf01/02/03), Quórum de metadata KRaft (sin Zookeeper), Loki 3.6.5 (logs centralizados, componente opcional), OpenTelemetry Collector 0.146.1 (opcional), Prometheus central (escila.jardinazuayo.fin.ec:9090 / pbigd-plat-obs01) (+11 more)

### Community 4 - "validate_haproxy_minio_dr.sh"
Cohesion: 0.21
Nodes (9): fail(), header(), info(), is_descendant_of(), manual(), pass(), pending(), validate_haproxy_minio_dr.sh script (+1 more)

### Community 5 - "validate_haproxy_minio_principal.sh"
Cohesion: 0.21
Nodes (9): fail(), header(), info(), is_descendant_of(), manual(), pass(), pending(), validate_haproxy_minio_principal.sh script (+1 more)

### Community 6 - "PostgreSQL 17.10 + Patroni 4.1 (pbigd-dlh01, ROL: LEADER/PRIMARIO)"
Cohesion: 0.30
Nodes (12): Clientes / Aplicaciones (Java, Python/API, BI/Reporting, Pentaho, Apps Móviles, Otros Clientes), Arquitectura PostgreSQL HA Enterprise (Diagrama), ETCD (pbigd-dlh01, 172.17.210.96), ETCD (pbigd-dlh02, 172.17.210.97), ETCD (pbigd-dlh03, 172.17.210.98), HAProxy 2.8 (pbigd-dlh01, 172.17.210.96), HAProxy 2.8 (pbigd-dlh02, 172.17.210.97), PgBouncer 1.24 (pbigd-dlh01, 172.17.210.96) (+4 more)

### Community 7 - "Tabla de Estado de Implementación y Validación"
Cohesion: 0.32
Nodes (12): Flink, Iceberg, Integración Flink - Iceberg, Integración Kafka - Flink, Integración Trino - Iceberg, Kafka, MinIO, Observabilidad (+4 more)

### Community 8 - "graphify skill (SKILL.md)"
Cohesion: 0.20
Nodes (11): .claude/CLAUDE.md — graphify skill pointer, graphify reference: add-watch, graphify reference: exports, graphify reference: extraction-spec (subagent prompt), graphify reference: GitHub clone and cross-repo merge, graphify reference: commit hook and CLAUDE.md integration, graphify reference: query, path, explain, graphify reference: transcribe video/audio (+3 more)

### Community 9 - "validate_flink_dr.sh"
Cohesion: 0.51
Nodes (10): check_systemd(), dr(), fail(), info(), log(), ok(), section(), validate_flink_dr.sh script (+2 more)

### Community 10 - "validate_flink_principal.sh"
Cohesion: 0.51
Nodes (10): check_port_local(), check_systemd_unit(), fail(), info(), log(), ok(), section(), validate_flink_principal.sh script (+2 more)

### Community 11 - "validate_kafka_dr.sh"
Cohesion: 0.40
Nodes (9): check_port_local(), check_systemd_unit(), fail(), header(), info(), kexec(), pass(), validate_kafka_dr.sh script (+1 more)

### Community 12 - "validate_kafka_principal.sh"
Cohesion: 0.42
Nodes (9): check_port_local(), check_systemd_unit(), fail(), header(), info(), kexec(), pass(), validate_kafka_principal.sh script (+1 more)

### Community 13 - "validate_obs_stack_dr.sh"
Cohesion: 0.49
Nodes (9): check_svc(), dr(), fail(), info(), log(), ok(), section(), validate_obs_stack_dr.sh script (+1 more)

### Community 14 - "validate_kafka_flink_dr_flink.sh"
Cohesion: 0.51
Nodes (9): dr(), fail(), info(), log(), ok(), section(), validate_kafka_flink_dr_flink.sh script, tcp_port_open() (+1 more)

### Community 15 - "validate_obs_stack_principal.sh"
Cohesion: 0.60
Nodes (9): check_svc(), fail(), info(), log(), ok(), opt(), section(), validate_obs_stack_principal.sh script (+1 more)

### Community 16 - "Arquitectura Flink + MinIO + Gravitino (Diagrama)"
Cohesion: 0.47
Nodes (9): AIStor (MinIO) — Almacenamiento de objetos, 3 nodos, Aplicación web (Consultas SQL), Flink (JobManager + TaskManagers), Flink web UI (Diagnóstico), Gravitino (Iceberg REST Catalog / metadata), Arquitectura Flink + MinIO + Gravitino (Diagrama), PowerBI (Consultas SQL), StreamPark (GUI principal) (+1 more)

### Community 17 - "validate_kafka_flink_dr_kafka.sh"
Cohesion: 0.56
Nodes (8): dr(), fail(), info(), log(), ok(), section(), validate_kafka_flink_dr_kafka.sh script, warn()

### Community 18 - "validate_kafka_flink_principal_flink.sh"
Cohesion: 0.56
Nodes (8): fail(), info(), log(), ok(), section(), validate_kafka_flink_principal_flink.sh script, tcp_port_open(), warn()

### Community 19 - "validate_flink_minio_dr.sh"
Cohesion: 0.58
Nodes (8): dr(), fail(), info(), log(), ok(), section(), validate_flink_minio_dr.sh script, warn()

### Community 20 - "validate_kafka_flink_principal_kafka.sh"
Cohesion: 0.61
Nodes (7): fail(), info(), log(), ok(), section(), validate_kafka_flink_principal_kafka.sh script, warn()

### Community 21 - "validate_flink_minio_principal.sh"
Cohesion: 0.64
Nodes (7): fail(), info(), log(), ok(), section(), validate_flink_minio_principal.sh script, warn()

### Community 22 - "validate_minio_dr.sh"
Cohesion: 0.54
Nodes (7): check_port_local(), fail(), header(), info(), pass(), validate_minio_dr.sh script, warn()

### Community 23 - "validate_minio_principal.sh"
Cohesion: 0.54
Nodes (7): check_port_local(), fail(), header(), info(), pass(), validate_minio_principal.sh script, warn()

### Community 24 - "validate_obs_agents.sh"
Cohesion: 0.64
Nodes (7): fail(), info(), log(), ok(), section(), validate_obs_agents.sh script, warn()

### Community 25 - "Informe de validación Kafka Principal (HTML)"
Cohesion: 0.38
Nodes (7): Diagnóstico: cluster Kafka con 1/3 brokers activos, Causa raíz: cluster.id distinto por nodo impide unión al quórum KRaft, Informe de validación Kafka DR (HTML), Informe de validación Kafka Principal (HTML), Reclasificación: node-exporter ausente es informativo porque Alloy embebe prometheus.exporter.unix, Guía de validación — Kafka (Principal y DR), Decisión de diseño: Kafka no reduce capacidad en DR (a diferencia de Flink/MinIO)

### Community 26 - "Guía de preparación — Validación de integración Flink → MinIO"
Cohesion: 0.67
Nodes (4): Guía de preparación — Validación de integración Kafka ↔ Flink, Patrón: split de scripts en dos lados por ausencia de SSH sin contraseña entre nodos, Guía de preparación — Validación de integración Flink → MinIO, Requisito: plugin S3 de Flink debe estar en JobManager y los 3/2 TaskManagers, no solo en JM

### Community 30 - "Guía de validación — Gravitino (Iceberg REST Catalog)"
Cohesion: 0.22
Nodes (8): Guía de validación — Gravitino (Iceberg REST Catalog), Inconsistencias documentales detectadas, Módulo DR — no generado en esta entrega, Sección 1 — Sistema Operativo, Java y prerequisitos, Sección 2 — Servicio y redundancia, Sección 3 — Validación funcional básica (sin MinIO), Sección 4 — Logs y observabilidad, Sección 5 — Recursos

### Community 31 - "validate_gravitino_principal.sh"
Cohesion: 0.53
Nodes (7): fail(), info(), log(), ok(), section(), validate_gravitino_principal.sh script, warn()

### Community 32 - "validate_trino_principal.sh"
Cohesion: 0.56
Nodes (8): fail(), info(), log(), ok(), section(), validate_trino_principal.sh script, tcp_port_open(), warn()

## Ambiguous Edges - Review These
- `graphify reference: extraction-spec (subagent prompt)` → `graphify reference: query, path, explain`  [AMBIGUOUS]
  .claude/skills/graphify/references/query.md · relation: references
- `Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)` → `Versiones finales de componentes (Kafka, Flink, Gravitino, Trino, Iceberg)`  [AMBIGUOUS]
  context/versiones-finales.md · relation: conceptually_related_to
- `flink-s3-fs-hadoop / flink-s3-fs-presto plugin` → `iceberg-flink-runtime JAR (integración Flink↔Iceberg/Gravitino, fuera de alcance)`  [AMBIGUOUS]
  validacion_flink_minio/guia_pasos_manuales_flink_minio.md · relation: conceptually_related_to

## Knowledge Gaps
- **41 isolated node(s):** `validacion-node-exporter.sh script`, `Sección 1 — Sistema Operativo, Java y prerequisitos`, `Sección 2 — Servicio y redundancia`, `Sección 3 — Validación funcional básica (sin MinIO)`, `Sección 4 — Logs y observabilidad` (+36 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `graphify reference: extraction-spec (subagent prompt)` and `graphify reference: query, path, explain`?**
  _Edge tagged AMBIGUOUS (relation: references) - confidence is low._
- **What is the exact relationship between `Plan de Implementación de software base — Proyecto Big Data (v2, 06-04-2026)` and `Versiones finales de componentes (Kafka, Flink, Gravitino, Trino, Iceberg)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **What is the exact relationship between `flink-s3-fs-hadoop / flink-s3-fs-presto plugin` and `iceberg-flink-runtime JAR (integración Flink↔Iceberg/Gravitino, fuera de alcance)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `Informe de validación Flink DR (HTML)` connect `Informe de validación Flink DR (HTML)` to `Stack Observabilidad DC Principal run — pbigd-plat-obs01`?**
  _High betweenness centrality (0.007) - this node is a cross-community bridge._
- **Why does `Kafka DR cluster (dr-kaf-1 / pbigd-kaf0X-cont)` connect `Stack Observabilidad DC Principal run — pbigd-plat-obs01` to `Informe de validación Flink DR (HTML)`?**
  _High betweenness centrality (0.005) - this node is a cross-community bridge._
- **Why does `validate_flink_principal.sh run — pbigd-plat-apps01 (JM Principal)` connect `Informe de validación Flink DR (HTML)` to `Stack Observabilidad DC Principal run — pbigd-plat-obs01`?**
  _High betweenness centrality (0.005) - this node is a cross-community bridge._
- **What connects `validacion-node-exporter.sh script`, `Sección 1 — Sistema Operativo, Java y prerequisitos`, `Sección 2 — Servicio y redundancia` to the rest of the system?**
  _41 weakly-connected nodes found - possible documentation gaps or missing edges._