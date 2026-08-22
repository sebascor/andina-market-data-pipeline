-- ============================================================================
-- GOLD LAYER — KPI 3: Valor promedio de pedido (AOV) y frecuencia de compra
-- por segmento de cliente
-- ============================================================================
-- Grano: 1 fila por segmento de cliente (nuevo, recurrente, vip, inactivo).
-- Fuente: main.tpt_silver.customers + main.tpt_silver.orders.
--
-- Decisión de diseño — LEFT JOIN, no INNER JOIN:
-- Se usa LEFT JOIN desde Customers hacia Orders (no al revés) para que
-- clientes sin ningún pedido válido (típicamente el segmento 'inactivo')
-- sigan contando en customer_count con orders/AOV en 0 o NULL, en vez de
-- desaparecer de la tabla — perder esos clientes ocultaría justo la señal
-- que este KPI busca mostrar (¿el segmento 'inactivo' realmente compra
-- menos, o no compra nada?).
--
-- Decisión de diseño — se excluyen pedidos 'cancelled' del AOV/revenue,
-- consistente con KPI 1: un pedido cancelado no representa valor de compra
-- real, aunque sí se filtra dentro del JOIN (no en un WHERE externo) para
-- no perder al cliente completo si todos sus pedidos fueron cancelados.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_gold.customer_value_by_segment AS
SELECT
  c.segment,

  COUNT(DISTINCT c.user_id)                                         AS customer_count,
  COUNT(o.order_id)                                                 AS order_count,
  ROUND(COUNT(o.order_id) * 1.0 / COUNT(DISTINCT c.user_id), 2)     AS avg_orders_per_customer,
  ROUND(SUM(o.total), 2)                                            AS total_revenue,
  ROUND(AVG(o.total), 2)                                            AS avg_order_value,

  current_timestamp() AS _gold_processed_at

FROM main.tpt_silver.customers AS c
LEFT JOIN main.tpt_silver.orders AS o
  ON c.user_id = o.user_id
  AND o.status != 'cancelled'

GROUP BY
  c.segment;
