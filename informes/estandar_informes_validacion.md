# Estándar — Informes de Validación de Implementación (Bluepoint · Jardín Azuayo)

Guía compacta para generar informes de validación de arquitectura ejecutada por
scripts (MinIO, Flink, Kafka, observabilidad, etc.), a partir de logs de
ejecución. Modelos de referencia: `informe_minio_principal.pdf`,
`informe_minio_dr.pdf`. Aplica a cualquier componente validado por un script
tipo `validate_<componente>_<entorno>.sh` que corre en 1+ nodos y produce
salida `[OK]/[WARN]/[FAIL]/[INFO]` con un resumen final de conteos.

## 1. Insumo y proceso

1. Reunir **todos** los logs crudos del componente/entorno (un archivo por
   nodo). No mezclar corridas de entornos distintos (Principal y DR van en
   informes separados, aunque compartan hallazgos).
2. Leer cada log completo — no resumir a partir del bloque final de conteos.
   Los hallazgos importantes suelen estar en el detalle ([WARN]/[FAIL] con su
   texto), no solo en el número.
3. Detectar **contradicciones dentro del propio log** (p. ej. un módulo dice
   que un puerto no es alcanzable y otro módulo, en el mismo run, sí logra
   conectarse a ese puerto). Nunca resolver la contradicción por conveniencia:
   documentarla como hallazgo a aclarar con evidencia de ambos lados.
4. Separar tres categorías de hallazgo, porque cada una tiene una acción
   distinta:
   - **Hallazgo de infraestructura real** (el sistema no cumple lo esperado).
   - **Defecto del script de validación** (segfault, bug de conteo, typo de
     hostname, lógica de umbral invertida). No es un problema del sistema
     validado — se reporta para que se corrija el script, no la infra.
   - **Decisión de arquitectura deliberada que el script no contempla** (p.
     ej. un exporter de métricas embebido en otro agente en vez de standalone).
     Se reclasifica con nota aclaratoria y evidencia, pero **sin borrar** el
     WARN/FAIL crudo de la matriz — se anota, no se oculta.
5. Los conteos PASS/WARN/FAIL del resumen ejecutivo son la suma literal de la
   matriz. Nunca ajustar números para que "luzcan mejor".
6. Si los nodos tienen **roles distintos** (p. ej. JobManager vs
   TaskManager) con checks distintos, no forzar una única matriz homogénea:
   dividir en "checks comunes a todos los nodos" + una subsección de matriz
   por rol. No usar filas en blanco ni omitir checks — si un check no aplica
   a un rol, no aparece en su tabla de rol.

## 2. Estructura del documento (orden fijo)

1. **Encabezado de marca**: logo/texto "Bluepoint" arriba a la derecha,
   línea pequeña "Cooperativa Jardín Azuayo · Proyecto Big Data" y título
   "INFORME DE VALIDACIÓN DE IMPLEMENTACIÓN" en negrita.
2. **Tabla de metadatos** (filas etiqueta/valor, etiqueta con fondo azul
   oscuro y texto blanco): Componente, Clúster/Entorno, Nodos validados
   (con rol si aplica), IPs (si son relevantes, como en DR), Versión del
   software, SO/Runtime, Fecha de validación, **Resultado general** (badge
   de color, ver §4).
3. **1. Resumen ejecutivo**: 1 párrafo de contexto (qué se validó, cuándo,
   con qué script, cuántos dominios/puntos de control) + 1 párrafo de
   conclusión de alto nivel (operativo / con hallazgos a resolver / no apto)
   + 3 tarjetas grandes con los totales PASS / WARN / FAIL.
4. **2. Matriz de estado por nodo**: tabla(s) Verificación × Nodo con badges
   PASS/WARN/FAIL. Si hay más de un rol, usar 2.1/2.2/2.3 por rol tal como
   indica §1.6. Mencionar explícitamente si los resultados son homogéneos
   entre nodos o dónde divergen.
5. **3. Hallazgos y acciones correctivas**: tabla `Tipo | Hallazgo | Detalle
   técnico | Acción requerida`, ordenada por severidad real (no por orden de
   aparición en el log): FAIL primero, luego WARN bloqueante, WARN no
   bloqueante, INFO al final. Cada fila de detalle técnico debe incluir el
   dato concreto (puerto, ruta, comando, valor esperado vs. observado), no
   una paráfrasis vaga.
   - Cuando aplique, agregar un bloque **"Nota aclaratoria — <hallazgo>"**
     inmediatamente después de la tabla, con el mismo tratamiento usado para
     node-exporter en los modelos MinIO: explica la causa raíz, la evidencia
     que la sostiene, y reclasifica la severidad real sin alterar la matriz.
6. **4. Análisis técnico destacado**: subsecciones numeradas (4.1, 4.2, …)
   en prosa técnica corta, una por dominio validado (infraestructura base,
   almacenamiento, red/quórum, funcionalidad, ciclo de vida del contenedor,
   capacidad/RTO si es DR). Cada subsección conecta el dato crudo con su
   implicancia ("por qué importa"), no repite la tabla.
7. **5. Acciones requeridas antes de producción**: agrupar en
   **OBLIGATORIA** (bloquea el paso a producción o a considerar el DR
   funcional) / **RECOMENDADA** (no bloquea pero se debe planificar) /
   **OPCIONAL** (mejora, sin urgencia). Cada acción obligatoria lleva pasos
   numerados o el comando exacto cuando exista (p. ej. `mc admin replicate
   add …`).
8. **6. Conclusión**: 2-4 párrafos que cierran con una frase en **negrita**
   que resume el veredicto y la condición de desbloqueo. Cerrar con tabla de
   firmas `Elaborado por | Revisado por` (Bluepoint AI — Consultoría Big
   Data / Equipo de Infraestructura — Cooperativa Jardín Azuayo) y fecha.
9. **Pie de página** en cada hoja: `Informe de validación <Componente> <Entorno> - Página N`.

## 3. Estilo visual

- Paleta: azul marca `#1e3fd8` (logo/títulos), azul oscuro `#1e2a8a` (fondo
  de barras de sección y etiquetas de metadatos, texto blanco), verde
  `#1f9d55` (PASS), naranja `#e08a1e` (WARN), rojo `#d43f3f` (FAIL), gris
  claro `#f4f4f4` para filas alternas de tabla.
- Tipografía sans-serif (Arial/Helvetica), cuerpo 9–10pt, títulos de sección
  ~12pt en blanco sobre barra de color, título principal ~20-22pt.
- Badges de estado: texto en mayúsculas, centrado, negrita, color de fondo
  sólido, texto blanco.
- Tablas a ancho completo de página, bordes finos grises, encabezado en
  negrita.
- Sin adornos decorativos, sin iconografía más allá de ✓/⚠/✖ para el badge
  de resultado general.

## 4. Vocabulario y clasificación

- **Resultado general** (badge en metadatos): `✓ APROBADO` (0 FAIL, WARNs
  menores) · `⚠ APROBADO CON OBSERVACIONES` (0 FAIL sin resolver, o FAIL
  presentes pero con evidencia fuerte de falso positivo pendiente de
  confirmar — nunca ocultar el FAIL crudo de la matriz) · `✖ NO APTO /
  RECHAZADO` (FAIL confirmado que impide la función core validada, p. ej.
  persistencia inexistente en un DR). Usar el mismo veredicto que el propio
  script haya declarado en su resumen cuando exista, salvo que el análisis
  encuentre evidencia sólida para matizarlo — y en ese caso explicarlo.
- Nunca usar "aprobado" sin calificador si existe al menos un FAIL sin
  resolver en la matriz.
- "OBLIGATORIO" implica bloqueante para pasar a producción o declarar el
  componente apto; "RECOMENDADO" implica buena práctica con ventana de
  mantenimiento; "OPCIONAL" implica mejora sin impacto en el veredicto.
- Tono: formal, técnico, en español neutro/ecuatoriano de negocio. Frases
  cortas y factuales. Evitar adjetivos sin respaldo de dato ("excelente",
  "perfecto") — preferir el dato mismo.

## 5. Pipeline de generación a PDF (sin dependencias externas)

Este entorno no tiene `pandoc`/`weasyprint`/`wkhtmltopdf`/Chromium, pero sí
`libreoffice`/`soffice`. Ruta usada y validada para `informe_flink_principal.pdf`
e `informe_flink_dr.pdf`: escribir un HTML autocontenido (CSS inline en
`<style>`) y convertir con `soffice --headless --convert-to pdf archivo.html`.
El filtro HTML de LibreOffice tiene soporte de CSS limitado — respetar estas
reglas o el render sale sin colores/anchos:

- **Nunca combinar dos clases en un mismo elemento** (`class="badge pass"`)
  ni usar selectores compuestos (`.badge.pass`, `td.cardcell`, `.cards td`
  descendente sobre clase+etiqueta genérica) para aplicar color/fondo — se
  ignoran en silencio. Definir **una clase única por combinación visual**
  (`.pass`, `.warn`, `.fail`, `.cardnum-g`, etc.) con todas sus propiedades
  juntas, y aplicar una sola clase por elemento.
  Excepción que sí funciona: selector descendente simple `.claseA claseB`
  donde `claseA` está en un contenedor (`table`/`div`) y `claseB` es una
  etiqueta HTML (p. ej. `.sechead td{...}`) — usado para las barras de
  sección y sí se renderiza bien.
- **Ancho de tablas**: no uses `width:XXmm` ni `width:100%` en CSS — no se
  respeta. Usa el atributo HTML `width="670"` (en px) directamente en
  `<table>` y en las `<th>`/`<td>` que deban fijar ancho de columna.
- Fija el tamaño de página con `@page{size:A4;margin:18mm 16mm;}` en el
  `<style>`; sí se respeta.
- Los saltos de página con `<div style="page-break-before:always">` sí
  funcionan; úsalos entre secciones grandes (matriz, hallazgos, análisis,
  acciones) para paginar de forma similar a los modelos de referencia.
- **No hay pie de página real por hoja**: un `<div>` al final del `<body>`
  solo aparece una vez, en la última página, no en todas. No lo uses para
  simular "Página N" (queda engañoso). Si se necesita paginación real,
  requiere otra herramienta (Chromium headless, weasyprint) fuera de este
  entorno.
- Verifica siempre el resultado leyendo el PDF generado antes de darlo por
  bueno — el fallo de estilo es silencioso, no lanza error de conversión.

## 6. Checklist antes de entregar el informe

- [ ] Los conteos de la tarjeta ejecutiva coinciden con la suma de la matriz.
- [ ] Cada FAIL de la matriz tiene una fila correspondiente en §3 y una
      acción en §5.
- [ ] Toda contradicción encontrada en los logs está documentada, no
      resuelta en silencio.
- [ ] Todo defecto de script (crash, bug de lógica, typo de host) está
      señalado como tal y no mezclado con hallazgos de infraestructura.
- [ ] El resultado general (badge) es consistente con §4 de este estándar.
- [ ] La conclusión tiene una frase en negrita con el veredicto accionable.
