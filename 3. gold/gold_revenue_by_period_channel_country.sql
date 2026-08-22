-- ============================================================================
-- GOLD LAYER — KPI 1: Revenue/GMV por período, canal y país
-- ============================================================================
-- Grano: 1 fila por semana × canal × país.
-- Fuente: main.tpt_silver.orders + main.tpt_silver.customers.
--
-- Decisión de diseño — qué cuenta como "revenue":
-- Se excluyen los pedidos con status = 'cancelled' del cálculo de ingresos
-- (representan demanda que nunca se concretó). Se incluyen 'pending',
-- 'confirmed', 'preparing', 'shipped' y 'delivered' como GMV bruto —
-- no se filtra por estado de pago aprobado en Payments, porque este KPI
-- mide volumen de pedidos, no caja cobrada (para eso existe el KPI 4,
-- tasa de aprobación de pagos, que sí vive en Payments).
--
-- Decisión de diseño — grano temporal:
-- Semana (DATE_TRUNC('week', order_date)), no día ni mes: suficientemente
-- granular para ver tendencia, sin generar demasiadas filas para un
-- dashboard que filtra por rango.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_gold.revenue_by_period_channel_country AS
SELECT
  DATE_TRUNC('week', o.order_date) AS period_week,
  o.channel,
  c.country,

  COUNT(*)                    AS order_count,
  SUM(o.total)                AS total_revenue,
  ROUND(AVG(o.total), 2)      AS avg_order_value,

  current_timestamp() AS _gold_processed_at

FROM main.tpt_silver.orders AS o
INNER JOIN main.tpt_silver.customers AS c
  ON o.user_id = c.user_id

WHERE o.status != 'cancelled'

GROUP BY
  DATE_TRUNC('week', o.order_date),
  o.channel,
  c.country;
