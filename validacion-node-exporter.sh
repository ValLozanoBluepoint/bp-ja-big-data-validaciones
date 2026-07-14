echo "=== Validación node-exporter embebido en Alloy ===" && \
echo "--- Servicio Alloy ---" && \
systemctl --user is-active alloy.service 2>/dev/null || \
  systemctl is-active alloy.service 2>/dev/null || \
  echo "no encontrado en contexto sistema ni usuario" && \
echo "--- Puerto 17935 escuchando ---" && \
ss -tlnp | grep ":17935" || echo "puerto 17935 no escuchando" && \
echo "--- Métricas de nodo disponibles ---" && \
curl -s --connect-timeout 5 http://localhost:17935/metrics 2>/dev/null | \
  grep -E "^node_cpu_seconds_total|^node_memory_MemTotal|^node_filesystem_size" | \
  head -5 || echo "métricas de nodo no encontradas en :17935" && \
echo "--- Versión Alloy ---" && \
curl -s --connect-timeout 5 http://localhost:17935/-/ready 2>/dev/null || \
  curl -s --connect-timeout 5 http://localhost:12345/-/ready 2>/dev/null || \
  echo "endpoint ready no accesible"
