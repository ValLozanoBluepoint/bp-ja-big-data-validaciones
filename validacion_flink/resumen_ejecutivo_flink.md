# Resumen Ejecutivo — Validación Flink (Principal + DR)
## Bluepoint · Cooperativa Jardín Azuayo
**Responsable:** Bluepoint
**Flink:** 2.2.1 · **Java:** OpenJDK 17 · **Runtime:** Podman 5.8.2 / Rocky Linux 10.x
**Fecha de ejecución:** 2026-07-07
**Alcance:** 4 nodos clúster PRINCIPAL + 3 nodos clúster DR (contingencia)

---

## 1. Estado por nodo

| Clúster | Nodo | Rol | Checks | PASS | FAIL | WARN | Veredicto |
|---|---|---|---|---|---|---|---|
| Principal | pbigd-plat-apps01 | JobManager | 40 | 32 | 0 | 8 | OK |
| Principal | pbigd-proc01 | TaskManager | 36 | 27 | 1 | 8 | Con fallo |
| Principal | pbigd-proc02 | TaskManager | 36 | 27 | 1 | 8 | Con fallo |
| Principal | pbigd-proc03 | TaskManager | 36 | 27 | 1 | 8 | Con fallo |
| DR | pbigd-plat-apps01-cont | JobManager | 41 | 26 | 3 | 12 | **NO APTO** |
| DR | pbigd-proc01-cont | TaskManager | 37 | 24 | 2 | 11 | **NO APTO** |
| DR | pbigd-proc02-cont | TaskManager | 37 | 23 | 2 | 12 | **NO APTO** |

**Lectura rápida:** el clúster PRINCIPAL está operativo con una falla de
conectividad puntual a diagnosticar. El clúster DR **no está en condiciones
de asumir contingencia** — los 3 nodos reportan fallos críticos de
persistencia y el servicio de saldos no podría escribir en un failover real.

---

## 2. Hallazgos críticos — Clúster Principal

**Puerto REST 8081 / RPC 6123 no alcanzable entre TaskManager y JobManager**
(FAIL en proc01, proc02 y proc03). Contradice al propio Módulo 6, donde la
consulta a la REST API (`http://pbigd-plat-apps01:8081/overview`) sí responde
y confirma los 3 TaskManagers registrados con slots libres. Esto apunta a que
el chequeo de puerto (Módulo 10) usa una ruta o método distinto al que usa la
consulta de API — probablemente una regla de firewall/ACL asimétrica entre el
segmento de los TaskManagers y el del JobManager, o un mecanismo de prueba de
puerto no representativo del tráfico real.

**Acción:** validar con el equipo de red las reglas de firewall en 8081/6123
entre proc01-03 → pbigd-plat-apps01, y confirmar en el script qué método usa
el chequeo de Módulo 10 (`nc`/`curl`) para descartar falso positivo.

---

## 3. Hallazgos críticos — Clúster DR

1. **`/data/flink` y `/var/log/flink` no existen en ningún nodo DR** (FAIL en
   los 3 nodos). Sin estos directorios no hay dónde persistir checkpoints,
   savepoints ni logs — en un failover real, Flink no podría recuperar
   estado. Es el hallazgo más grave del reporte.
   **Acción:** crear la estructura de directorios con los permisos correctos
   en los 3 nodos DR antes de la próxima prueba de contingencia.

2. **Redis DR no accesible en `pbigd-plat-apps01-cont:6379`** (WARN en los 3
   nodos). Los dos jobs críticos de contingencia (reconstrucción de saldos y
   serving de últimos movimientos) dependen de esta escritura — si Redis no
   responde, el DR no cumple su propósito funcional aunque el resto de checks
   pase.
   **Acción:** validar despliegue y conectividad de red hacia Redis DR;
   bloqueante para declarar el DR apto.

3. **Nodo `pbigd-proc01-cont` no respondía ping desde el JobManager** (FAIL),
   pero ese mismo nodo, evaluado directamente, sí ve red hacia
   `plat-apps01-cont` y `proc02-cont`. Indica un problema de conectividad
   intermitente o asimétrico en el segmento DR, no un nodo caído.
   **Acción:** monitorear estabilidad de red del segmento DR entre estos
   nodos.

4. **vCPU insuficientes en el JobManager DR:** 3 disponibles vs. 4 esperados
   (RAM sí es suficiente).
   **Acción:** escalar capacidad o confirmar formalmente que el
   dimensionamiento reducido es aceptado para modo contingencia.

---

## 4. Hallazgos transversales (Principal y DR)

- **`systemctl --user is-active` termina en Segmentation fault** en los 3 TM
  del Principal y en los 3 nodos DR. Es un fallo reproducible del entorno
  (podman + systemd + `sudo -u`), no solo ausencia de configuración: no hay
  garantía de que los servicios de Flink se reactiven automáticamente tras un
  reinicio de host. Requiere revisión de infraestructura, no solo del script.
- **Bug confirmado en `validate_flink_dr.sh`** (líneas ~595-604): el conteo de
  errores de log usa `grep -c ... || echo 0` bajo `set -euo pipefail`; cuando
  `grep` no encuentra coincidencias sale con status 1 aunque ya imprimió
  `"0"`, y el `|| echo 0` agrega una segunda línea. El resultado
  (`"0\n0"`) rompe la comparación `[[ "$ERRORS" -eq 0 ]]` con *syntax error in
  expression*. Es un defecto del script de validación, no de la
  infraestructura — se recomienda corregirlo para no generar ruido en
  próximas corridas.
- En el log de `pbigd-proc01-cont` se registra al operador editando el script
  en vivo (`vi validate_flink_dr.sh`) tras un error de "command not found" /
  "syntax error near unexpected token" en el primer intento — indica que el
  archivo llegó con problema de formato/codificación y se corrigió de forma
  ad-hoc en el nodo. Se recomienda redistribuir el script ya corregido en vez
  de parchear copias locales.
- **`node-exporter` ausente** en los 7 nodos (Principal y DR) — gap de
  observabilidad de métricas de host en Grafana/Prometheus.
- Se detectaron líneas ERROR/Exception/FATAL en los logs recientes de ambos
  JobManager (6 en Principal, 3 en DR) que aún no se han revisado en detalle.

---

## 5. Acciones recomendadas para arquitectos

| Prioridad | Acción | Responsable sugerido |
|---|---|---|
| Crítica | Crear `/data/flink` y `/var/log/flink` en los 3 nodos DR con permisos correctos | Cooperativa |
| Crítica | Validar/reparar conectividad de red a Redis DR (`6379`) — bloqueante para failover | Cooperativa |
| Alta | Investigar y resolver el bloqueo de red 8081/6123 entre TaskManagers y JobManager en Principal | Cooperativa (red) |
| Alta | Investigar conectividad intermitente hacia `pbigd-proc01-cont` desde el JM DR | Cooperativa (red) |
| Alta | Resolver el segfault de `systemctl --user is-active` en Podman/systemd y habilitar autoarranque de los servicios Flink | Cooperativa (infra) |
| Media | Confirmar dimensionamiento de vCPU aceptado para el JM DR (3 vs. 4) | Arquitectura |
| Media | Corregir el bug de conteo de errores en `validate_flink_dr.sh` y redistribuir versión limpia | Bluepoint |
| Media | Revisar en detalle las líneas ERROR/Exception/FATAL de ambos JobManager | Bluepoint |
| Baja | Desplegar `node-exporter` en los 7 nodos | Cooperativa |

---

## 6. Veredicto

- **Principal:** operativo, sin fallos bloqueantes para operación normal,
  pero con una inconsistencia de red TM↔JM que debe aclararse antes de
  confiar en el failover automático entre nodos.
- **DR:** **no apto para contingencia** en su estado actual. Los 3 fallos
  críticos del JobManager DR (directorios de persistencia, Redis inaccesible,
  conectividad intermitente) deben resolverse y revalidarse antes de aceptar
  este clúster como plan de contingencia funcional.
