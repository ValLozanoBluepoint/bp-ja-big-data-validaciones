# Contexto extra

## 2026-08-01

1. IP única de MinIO para Flink, Iceberg, Trino y Gravitino

La recomendación es exponer un único endpoint mediante HAProxy + Keepalived (el mismo patrón ya usado para PostgreSQL), delante de los 3 nodos de storage, con enrutamiento separado para la API S3 (puerto 9000) y la Consola (puerto 9001). Los 4 componentes (Flink, Iceberg, Trino y Gravitino) deben apuntar exactamente al mismo endpoint (IP virtual o nombre DNS interno) y tener habilitado explícitamente el acceso "path-style", ya que MinIO detrás de una IP propia no soporta el direccionamiento por subdominio que sí soporta un proveedor cloud. Mantener este valor centralizado evita que cada componente quede configurado de forma distinta.

2. IP única de Flink para uso desde Apache StreamPark

Actualmente el JobManager de Flink es una instancia única (sin redundancia activa), por lo que la recomendación inmediata es simplemente apuntar StreamPark al nombre DNS interno de ese servidor, sin necesidad de una IP virtual adicional (una VIP sin un segundo JobManager al cual conmutar no aporta disponibilidad real). Si en el futuro se requiere alta disponibilidad real del JobManager, la vía técnica es habilitar los servicios de alta disponibilidad nativos de Flink (que requieren un componente de coordinación tipo ZooKeeper autoalojado), y recién ahí desplegar una IP virtual delante del puerto REST de Flink. Lo dejamos como una decisión a evaluar en conjunto, no como algo bloqueante para el estado actual del proyecto.

3. Validación de la implementación PostgreSQL para BigData

Revisamos el diagrama de arquitectura compartido. La base (Patroni + etcd + HAProxy + PgBouncer, PostgreSQL 17.10) está bien encaminada y refleja varias de las recomendaciones ya conversadas. Encontramos, sin embargo, tres puntos que consideramos necesario corregir antes de pasar a producción:

a) La IP virtual mencionada para el acceso de clientes (PgBouncer) no tiene, en el diseño actual, un mecanismo real de failover detrás. Se requiere agregar Keepalived en los dos nodos de la capa de acceso para que esa IP realmente conmute automáticamente ante una falla. La implementación en sí no requiere hardware adicional (corre sobre los mismos dos servidores que ya tienen PgBouncer/HAProxy), pero antes de aplicarla es necesario que el equipo de infraestructura/virtualización confirme tres puntos, ya que ambos nodos son máquinas virtuales sobre VMware/ESXi:
Reservar la dirección IP que se usará como IP virtual, en el mismo segmento de red de pbigd-dlh01/pbigd-dlh02.
Revisar la política de seguridad "Forged Transmits" en el port group del vSwitch donde están esas dos VMs, y dejarla en modo Accept. Si queda en Reject, el vSwitch descarta el ARP gratuito que el nodo de respaldo envía al tomar la IP virtual durante un failover, y la conmutación automática no funciona en el momento en que realmente se necesita, aunque en pruebas controladas pueda parecer que todo está bien.
Confirmar que exista una regla de anti-afinidad DRS para que pbigd-dlh01 y pbigd-dlh02 nunca corran en el mismo host físico ESXi; de lo contrario, la caída de un solo host físico podría tumbar a ambos nodos de la capa de acceso simultáneamente y la IP virtual no tendría a dónde conmutar.

b) El diagrama contempla que aplicaciones de BI/Reportería (Pentaho) consulten directamente este clúster PostgreSQL, que fue diseñado y aprobado específicamente para servir el Transactional Outbox con baja latencia. Mezclar consultas analíticas con ese clúster, que además usa replicación síncrona, puede introducir demoras en las transacciones en tiempo real. Recomendamos redirigir la reportería hacia el lakehouse (Trino + Iceberg), que es la capa pensada para ese propósito, o bien —si se necesita una solución más inmediata— aislar esas consultas en una réplica adicional que no forme parte del esquema de replicación síncrona.

c) El mecanismo de autenticación de PgBouncer definido usa al superusuario "postgres" como la identidad con la que las aplicaciones se conectan a través del pool, y consulta directamente la tabla interna de contraseñas del motor. Dado que se trata de datos financieros, recomendamos crear un usuario de aplicación con permisos acotados únicamente a las tablas del Outbox, y ajustar el mecanismo de autenticación para que nunca exponga las credenciales del superusuario.

Para facilitar la implementación de estas tres correcciones, adjuntamos los scripts correspondientes, listos para ejecutar (con las variables de cada entorno claramente señaladas para ajustar antes de correrlos):

01_configuracion_keepalived_vip_pgbouncer.sh — despliega y valida la IP virtual con failover real (punto a). Recomendamos ejecutarlo recién después de confirmar los tres puntos de virtualización mencionados arriba.
02_hardening_autenticacion_pgbouncer.sql — crea el usuario de aplicación de bajo privilegio y ajusta el mecanismo de autenticación a nivel de base de datos (punto c).
03_hardening_configuracion_pgbouncer.sh — aplica el cambio correspondiente en la configuración de PgBouncer y valida que el superusuario ya no pueda usarse a través del pool (punto c).
04_verificacion_disponibilidad_lakehouse_outbox.sh — script de apoyo para decidir, con evidencia concreta, si ya es viable redirigir Pentaho al lakehouse o si conviene la solución intermedia de una réplica separada (punto b).

Cada script incluye su propia validación con resultado PASS/FAIL/WARN al finalizar, en la misma línea de los scripts de validación que ya hemos compartido anteriormente.

## 2026-08-06

Les compartimos la aclaración sobre el usuario del sistema operativo con el que deben ejecutarse HAProxy y Keepalived. La respuesta es distinta para cada uno.

Keepalived como usuario root: correcto

Keepalived necesita operaciones a nivel de kernel para cumplir su función -manejar el protocolo VRRP, asignar y retirar la IP virtual de la interfaz de red, y enviar el ARP gratuito durante un failover-. Esto requiere privilegios de administrador (o, alternativamente, un conjunto específico de capacidades de Linux). El propio proyecto de Keepalived ofrece una plantilla oficial para ejecutarlo sin root usando esas capacidades, pero existen reportes documentados de fallos (segfaults) al usar esa alternativa en ciertas configuraciones. Por eso, root sigue siendo la práctica más sólida y ampliamente documentada para Keepalived, y así está correctamente implementado.

HAProxy como usuario root: no es correcto, y conviene corregirlo

La documentación oficial de HAProxy es clara: la práctica recomendada es que, aunque el proceso puede arrancar como root, debe liberar esos privilegios inmediatamente después de iniciar y continuar su ejecución como un usuario sin privilegios. Ejecutarlo como root de forma permanente expone innecesariamente el resto del sistema si llegara a presentarse una vulnerabilidad en el proceso.

En este proyecto, además, ni siquiera aplica la justificación habitual para arrancar como root: esa justificación existe únicamente cuando el proceso necesita enlazarse a un puerto privilegiado (menor a 1024, como el 80 o 443). Todos los puertos que usa HAProxy en esta arquitectura son puertos altos -6432, 15432, 15433, 18404 para PgBouncer/Postgres, y 9000/9001 para MinIO- por lo que HAProxy podría ejecutarse como usuario sin privilegios desde el primer momento, sin ningún paso intermedio.

Esto también es una desviación del patrón que el propio proyecto ya adoptó: los servicios de la capa de acceso corren en contenedores Podman rootless bajo el usuario admapl (así debería estar implementado PgBouncer). HAProxy debería seguir el mismo esquema.

Recomendación

Keepalived: mantener como está, ejecutándose como root a nivel de host (no dentro de un contenedor).
HAProxy: migrar a ejecución bajo el usuario admapl, en línea con el resto de los servicios de la capa de acceso.

# 2026-08-06

Retomamos la aclaración sobre el despliegue de Iceberg, ahora con la verificación de la documentación del proyecto y un diagrama de referencia adjunto.

Verificación en los documentos

Revisamos el Plan de Implementación y la Guía de Instalación de Componentes Base, y ninguno de los dos indica instalar Iceberg en los nodos de MinIO (stg-1, stg-2, stg-3). No existe ninguna tarea de ese tipo en el plan: la sección de MinIO (punto 6 de la guía de instalación) solo contempla el despliegue del clúster distribuido, directorios, persistencia y validaciones propias de MinIO -nada relacionado con Iceberg.

Dicho eso, sí identificamos el origen probable de la confusión: la tabla de componentes describe a Iceberg como "formato de tablas transaccionales... sobre MinIO", y la sección de Gravitino lo describe como un servicio "integrado con MinIO + Iceberg". Ninguna de las dos frases es incorrecta, pero al no aclarar explícitamente que Iceberg no es una instalación separada sino librerías embebidas en los motores de cómputo, es comprensible que se haya interpretado como un componente a instalar junto con MinIO en los mismos nodos. Vamos a proponer un ajuste de redacción en la próxima actualización del plan para cerrar esa ambigüedad.

Precisión de dónde va cada elemento

Nodos de MinIO (stg-1/2/3): sin cambios, no requieren ninguna librería de Iceberg.
Flink JobManager (platformapps-1) y TaskManagers (cmp-1/2/3): sí requieren el JAR iceberg-flink-runtime-1.20-1.9.2.jar en el directorio lib/ de Flink.
Trino: el conector Iceberg viene incluido en la distribución, solo se configura el catálogo.
Gravitino (platformapps-1): incluye las librerías core de Iceberg como parte de su implementación del catálogo REST.

Diagrama de referencia

Adjuntamos un diagrama que resume cómo interactúan estos componentes, incluyendo las interfaces de gestión de Flink y los dos consumidores mencionados:

Gravitino solo maneja metadata (catálogo de tablas), nunca los archivos de datos -por eso Flink y Trino tienen una conexión hacia Gravitino (metadata) y otra directa hacia AIStor (archivos), sin pasar por Gravitino.
StreamPark y Flink web UI (opcional) son la capa de gestión/monitoreo de Flink (línea discontinua en el diagrama), no consumidores de datos.
Aplicación web y PowerBI consultan exclusivamente a través de Trino vía SQL, manteniendo un único punto de acceso a los datos del lake.
