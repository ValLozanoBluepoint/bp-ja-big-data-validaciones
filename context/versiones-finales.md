| Tecnología | Ver. estable | JDK requerido | Ver. mín JDK | Docs |
|---|---|---|---|---|
| Apache Kafka | 4.3.0 | 25 | - | [Link](https://kafka.apache.org/documentation/#java) |
| Apache Flink | 2.2.1 | 17 | - | [Link](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/) |
| Apache Gravitino | 1.2.1 | 17 | - | [Link](https://gravitino.apache.org/docs/1.2.1/how-to-install) |
| Trino | 481 | 25 | 25.0.1 | [Link](https://trino.io/docs/current/installation/deployment.html) |

---

### Nota: Apache Iceberg

**Versión de referencia: 1.11.0**

| Motor | Iceberg format version soportado | Notas |
|---|---|---|
| Flink 2.1 | v1, v2 | Usa `iceberg-flink-runtime-2.1` |
| Trino 481 | v1, v2 | Conector built-in, default escribe v2 |
