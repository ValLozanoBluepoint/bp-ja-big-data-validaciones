# Guía de pasos manuales — HAProxy + Keepalived delante de MinIO

Guía complementaria a `validate_haproxy_minio_principal.sh` y
`validate_haproxy_minio_dr.sh`. Estos scripts no pueden cerrar toda la
validación: algunos checks requieren privilegios de `root`/`sudo` en el host
(que el script nunca solicita automáticamente, por diseño), y otros requieren
acceso a vCenter/ESXi, fuera del alcance de un script que corre dentro de una VM.

**Nota de contexto importante:** al momento de escribir esta guía no se
encontró, en este repositorio, evidencia de que la capa HAProxy+Keepalived
delante de MinIO ya esté implementada — en `context/Contexto-extra-correos.md`
aparece únicamente como una recomendación de Bluepoint a la Cooperativa
(2026-08-01), y tanto `validacion_flink_minio/validate_flink_minio_principal.sh`
como los logs de `logs/flink/dr/` muestran a Flink apuntando hoy directamente a
un nodo MinIO individual, no a una VIP. Si al ejecutar los checks de esta guía
resulta que la capa no existe todavía, ese es el resultado esperado a
documentar — no un error de la guía.

Para cada check: qué se valida, el comando exacto, el resultado esperado, y un
espacio para pegar la evidencia real de la corrida (mismo criterio que ya usa
este proyecto para los pasos manuales de Kafka↔Flink, ver
`validacion_flink_kafka/preparacion_integracion_kafka_flink.md`).

---

## Check 4.2 — Estado VRRP en journal de Keepalived (si journald está restringido)

**Qué se valida y por qué requiere permisos elevados:** confirmar que los 3
nodos (Principal: `pbigd-stg01/02/03`; DR: `pbigd-stg01/02/03-cont`) están de
acuerdo entre sí sobre cuál es el nodo MASTER actual y cuáles son BACKUP. El
script intenta leerlo sin `sudo`; si journald tiene el journal restringido a
root en ese nodo, la lectura falla y este check queda pendiente.

**Comando exacto (ejecutar en CADA uno de los 3 nodos):**
```bash
sudo journalctl -u keepalived --since "1 hour ago" --no-pager | grep -iE "Entering (MASTER|BACKUP|FAULT) STATE"
```

**Resultado esperado:** exactamente **un** nodo en `Entering MASTER STATE` y
los otros dos en `Entering BACKUP STATE`, sin transiciones repetidas o
recientes hacia `FAULT STATE` (indicaría flapping). Si dos nodos se declaran
MASTER simultáneamente, es un problema real de configuración VRRP (`vrid`
duplicado o problema de red multicast/unicast) — no dejarlo pasar.

**Evidencia (pegar salida real de los 3 nodos):**
```
pbigd-stg01:


pbigd-stg02:


pbigd-stg03:

```

---

## Check 4.4 — Prueba de failover real

**Qué se valida y por qué requiere permisos elevados:** que al detener
Keepalived en el nodo MASTER, la VIP realmente migre al nodo BACKUP y el
tráfico siga respondiendo durante la transición — la única forma de comprobar
que el failover funciona en la práctica, no solo que la configuración "se ve
bien". Detener un servicio de sistema requiere `sudo`; por diseño, ningún
script de este proyecto ejecuta esto de forma automática.

**Procedimiento (en orden):**

1. Identificar el nodo MASTER actual (ver salida de Módulo 4.1 del script, o `ip addr show <interfaz>` en los 3 nodos buscando la VIP).
2. Desde una máquina externa a los 3 nodos, lanzar un loop de tráfico continuo contra la VIP, en una terminal aparte, y dejarlo corriendo durante todo el procedimiento:
   ```bash
   while true; do curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" --connect-timeout 2 --max-time 5 http://<VIP>:9000/minio/health/live; sleep 1; done
   ```
3. En el nodo MASTER identificado en el paso 1:
   ```bash
   sudo systemctl stop keepalived
   ```
4. Observar el loop del paso 2: anotar cuántos códigos distintos de `200` (o timeouts) aparecen y durante cuántos segundos, antes de volver a `200` de forma sostenida.
5. Confirmar en el nuevo nodo MASTER que la VIP migró:
   ```bash
   ip addr show <interfaz> | grep <VIP>
   ```
6. **Restaurar el estado original — no omitir este paso:**
   ```bash
   sudo systemctl start keepalived
   ```
   Confirmar que el nodo original vuelve a ser MASTER (o queda como BACKUP correctamente, según la prioridad configurada) y que no queda con Keepalived detenido.

**Resultado esperado:** la VIP aparece en el nuevo nodo MASTER en segundos
(no minutos), la ventana de códigos distintos de `200`/timeouts en el loop es
corta (segundos, no un corte prolongado), y tras el paso 6 el clúster vuelve a
un estado estable de 3 nodos con Keepalived activo en los 3.

**Evidencia (pegar salida real):**
```
Nodo MASTER antes del test:


Salida del loop de curl durante el failover (recorte relevante):


Nodo MASTER después del failover (ip addr show):


Confirmación de restauración (systemctl start keepalived + estado final):

```

---

## Check 5.1 — Lectura de `/etc/keepalived/keepalived.conf`

**Qué se valida y por qué requiere permisos elevados:** confirmar los
parámetros no sensibles de la configuración real de Keepalived (VIP,
interfaz, priority) cuando el archivo tiene permisos restrictivos (600,
propiedad de root) y el script no puede leerlo como usuario `admapl`.

**Comando exacto:**
```bash
sudo cat /etc/keepalived/keepalived.conf
```

**Resultado esperado:** el bloque `vrrp_instance` muestra la VIP esperada en
`virtual_ipaddress`, la interfaz de red correcta en `interface`, y una
`priority` coherente con el rol (MASTER = priority más alta). Debe existir un
bloque `authentication` con `auth_type` y `auth_pass`.

**⚠️ ADVERTENCIA — nunca pegar el valor real de `auth_pass` en esta guía ni en
ningún informe.** Al pegar la evidencia, reemplazar la línea de `auth_pass`
por `auth_pass ***REDACTADO***`, o simplemente confirmar en la evidencia que
el bloque `authentication` existe, sin transcribir el bloque completo.

**Evidencia (pegar salida real, con `auth_pass` redactado):**
```


```

---

## Módulo 6 — Prerrequisitos de virtualización (vCenter/ESXi)

Mismo criterio ya usado en la validación de PostgreSQL/PgBouncer para estos
tres puntos (ver `context/Contexto-extra-correos.md`, correo 2026-08-01,
punto 3.a) — aquí aplicados a `pbigd-stg01/02/03` (Principal) y
`pbigd-stg01/02/03-cont` (DR), que ahora alojan también HAProxy+Keepalived
además de MinIO.

### 6.1 — IP reservada para la VIP

**Qué se valida:** que la IP que se usará como VIP esté formalmente
reservada (no asignable por DHCP a otra VM) en el mismo segmento de red de
los 3 nodos de storage.

**Dónde revisar:** vCenter/ESXi → gestión de IPs del segmento de red de
`pbigd-stg01/02/03` (o el sistema de IPAM que use la Cooperativa, si no es
vCenter directamente).

**Resultado esperado:** la IP de la VIP aparece marcada como reservada/fuera
del pool DHCP, y no responde a ping/ARP desde ninguna otra VM antes del
despliegue de Keepalived.

**Evidencia:**
```


```

### 6.2 — "Forged Transmits" en modo Accept

**Qué se valida:** que el port group del vSwitch donde están las VMs
`pbigd-stg01/02/03` (o `-cont` en DR) tenga la política de seguridad "Forged
Transmits" en **Accept**, no en Reject. Si queda en Reject, el vSwitch
descarta el ARP gratuito que el nodo de respaldo envía al tomar la VIP
durante un failover, y la conmutación automática no funciona en el momento
real en que se necesita — aunque en una prueba controlada (Check 4.4, hecha
justo después de configurar) pueda parecer que todo funciona.

**Dónde revisar:** vCenter → Networking → seleccionar el vSwitch/port group
de esas VMs → Edit Settings → Security → "Forged Transmits".

**Resultado esperado:** "Forged Transmits" = **Accept**.

**Evidencia:**
```


```

### 6.3 — Regla de anti-afinidad DRS

**Qué se valida:** que exista una regla de anti-afinidad DRS que impida que
`pbigd-stg01`, `pbigd-stg02` y `pbigd-stg03` corran en el mismo host físico
ESXi al mismo tiempo. Si ya existe por requerimiento del propio clúster
distribuido de MinIO (quórum), confirmar que **sigue aplicada** — ahora
protege también a HAProxy/Keepalived co-ubicados: la caída de un solo host
físico ya no solo pondría en riesgo el quórum de MinIO, sino también la
disponibilidad de todo el endpoint de acceso.

**Dónde revisar:** vCenter → Cluster → Configure → VM/Host Rules.

**Resultado esperado:** regla de tipo "Separate Virtual Machines" activa,
incluyendo las 3 VMs del entorno correspondiente (Principal o DR).

**Evidencia:**
```


```

---

## Resumen de checks pendientes por permisos o alcance

| Check | Motivo | Reflejado en el script como |
|---|---|---|
| 4.2 (si journald restringido) | requiere `sudo journalctl` | MANUAL — verificación manual pendiente |
| 4.4 | requiere detener un servicio (`sudo systemctl stop keepalived`) | MANUAL — marcado explícitamente como manual, nunca ejecutado |
| 5.1 (si el archivo es 600/root) | requiere `sudo cat` sobre archivo restringido | MANUAL — verificación manual pendiente |
| 6.1, 6.2, 6.3 | requieren acceso a vCenter/ESXi, fuera de la VM | MANUAL — verificación manual pendiente (documental) |

Nota: el tag `MANUAL` es distinto de `WARN` — `WARN` queda reservado para
hallazgos reales que el script sí pudo verificar (p. ej. journal accesible
pero sin transiciones VRRP recientes). `MANUAL` indica que el check está
fuera del alcance del script por diseño (permisos o acceso a vCenter/ESXi),
no que se haya detectado un problema.

Estos 4 puntos (más el sub-caso de 4.2/5.1 si los permisos ya son laxos en el
entorno real) son los que deben cerrarse con el equipo de infraestructura de
la Cooperativa antes de declarar esta capa apta para producción, siguiendo el
mismo estándar de informe que el resto del proyecto
(`informes/estandar_informes_validacion.md`): cada WARN de esta lista debe
tener su fila correspondiente en la sección de hallazgos del informe final,
con la evidencia pegada arriba.
