validate_flink_principal.sh — Cluster Flink DC Principal

Topología cubierta:

platformapps-1 → Flink JobManager (8081 REST / 6123 RPC)
cmp-1, cmp-2, cmp-3 → Flink TaskManagers

12 módulos de validación:

| Módulo | Qué valida |
|-|-|
| 1 | OS Rocky Linux 10, OpenJDK 17, Podman 5.x, NTP |
| 2 | Directorios /opt/flink, /data/flink, /var/log/flink + permisos |
| 3 | Contenedor JobManager (estado, versión Flink 2.2.1, JVM 17) |
| 4 | Contenedor TaskManager (estado, slots, JVM) |
| 5 | Puertos 8081 y 6123 escuchando |
| 6 | REST API: overview, TaskManagers (≥3 exigidos), slots, jobs |
| 7 | systemd: servicio activo y habilitado en boot |
| 8 | Conectividad hacia MinIO para checkpoints S3 |
| 9 | Job de prueba (endpoint /jars, Flink CLI) |
| 10 | Conectividad inter-nodos (ping + puerto RPC) |
| 11 | Logs del contenedor, detección de errores, Alloy, node-exporter |

validate_flink_dr.sh — Cluster Flink DC Alterno
Topología cubierta:

dr-cmp-1 → JobManager + TaskManager colocados (4 vCPU / 10 GB)
dr-cmp-2 → TaskManager adicional (3 vCPU / 8 GB)

Diferencias clave respecto al principal:

Umbrales reducidos intencionalmente (mínimo 1 TM activo es aceptable)
Validación de RAM/vCPU del nodo frente a lo especificado en el diseño
Verificación del uptime del contenedor (relevante en failover)
Módulo de jobs críticos de contingencia (serving Kafka→Flink→Redis)
Detección de presión de memoria (OOM/GC) — crítica con recursos limitados
Verificación de autoarranque (enabled) como requisito obligatorio en DR
Resumen diferenciado: "LISTO para contingencia" vs "funcional con advertencias"


Para ejecutar en todos los nodos de forma secuencial:

```bash
# DC Principal
for NODE in platformapps-1 cmp-1 cmp-2 cmp-3; do
  ssh admapl@$NODE 'bash validate_flink_principal.sh'
done
```

```bash
# DC Alterno
for NODE in dr-cmp-1 dr-cmp-2; do
  ssh admapl@$NODE 'bash validate_flink_dr.sh'
done
```

Nota: Si los nombres de los contenedores Podman difieren de flink-jobmanager / flink-taskmanager, ajusta las variables CONTAINER_JM y CONTAINER_TM al inicio de cada script antes de ejecutar.
