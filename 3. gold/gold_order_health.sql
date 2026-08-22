-- ============================================================================
-- GOLD LAYER — KPI 2: Tasa de cancelación y tiempo de ciclo de pedido
-- ============================================================================
-- Grano: 1 fila por mes.
-- Fuente: main.tpt_silver.orders (única tabla necesaria).
--
-- Decisión de diseño — tiempo de ciclo:
-- Se mide solo sobre pedidos 'delivered' (order_date -> updated_at), ya que
-- es el único estado que representa un ciclo completo y cerrado. Pedidos
-- en estados intermedios (shipped, preparing) no tienen un "fin" válido
-- todavía y se excluyen del promedio para no subestimarlo.
--
-- Limitación conocida (documentada en README 2.3): sin SCD Type 2 activo,
-- 'updated_at' solo refleja la ÚLTIMA transición de estado, no cada paso
-- intermedio — este KPI mide el ciclo total (creación -> entrega), no el
-- tiempo detenido en cada estado individual (ej. cuánto tardó en pasar de
-- 'confirmed' a 'shipped').
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_gold.order_health AS
SELECT
  DATE_TRUNC('month', order_date) AS period_month,

  COUNT(*)                                              AS total_orders,
  COUNT(*) FILTER (WHERE status = 'cancelled')          AS cancelled_orders,
  ROUND(
    100.0 * COUNT(*) FILTER (WHERE status = 'cancelled') / COUNT(*), 2
  )                                                      AS cancellation_rate_pct,

  COUNT(*) FILTER (WHERE status = 'delivered')          AS delivered_orders,
  ROUND(
    AVG(
      CASE WHEN status = 'delivered'
        THEN (UNIX_TIMESTAMP(updated_at) - UNIX_TIMESTAMP(order_date)) / 3600.0
      END
    ), 2
  )                                                      AS avg_cycle_time_hours,

  current_timestamp() AS _gold_processed_at

FROM main.tpt_silver.orders

GROUP BY
  DATE_TRUNC('month', order_date);
