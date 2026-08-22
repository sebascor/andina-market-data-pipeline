-- ============================================================================
-- SILVER LAYER — PAYMENTS
-- Nota: Payments ya es un event-log desde el origen (previous_status/
-- new_status/event_timestamp por fila) — no requiere SCD Type 2 ni CDC
-- en esta capa. Silver solo preserva el historial, no lo reconstruye.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ETAPA A: silver_clean
-- Responsabilidad: limpieza INTRA-tabla (dedupe, tipos)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.payments_clean
(
  CONSTRAINT event_id_not_null EXPECT (event_id IS NOT NULL) 
)
AS
SELECT
  event_id,
  payment_id,
  order_id,
  method,

  -- Check 2: cast de tipos
  CAST(amount AS DECIMAL(10, 2)) AS amount,

  -- previous_status es legítimamente NULL en el primer evento de cada pago
  -- (creación → "pending"), así que no se le aplica constraint de not-null
  previous_status,
  new_status,
  CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,

  -- Check 5: metadata de auditoría
  current_timestamp() AS _silver_processed_at

FROM main.tpt_bronze.payments

-- Check 1: deduplicación — resuelve los 8 eventos duplicados exactos
-- inyectados en payments.csv (issue de calidad #7)
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) = 1;

-- ----------------------------------------------------------------------------
-- CUARENTENA — FK inválida contra Orders
-- Igual que con OrderItems: los eventos de pago de los 5 pedidos con
-- order_id NULL en Orders (issue de calidad #3) caen aquí como huérfanos.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.payments_invalid_fk AS
SELECT
  p.*,
  'order_id no existe en orders (posible order_id nulo/eliminado en origen)' AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_silver.payments_clean AS p
LEFT ANTI JOIN main.tpt_silver.orders AS o
  ON p.order_id = o.order_id;


-- ----------------------------------------------------------------------------
-- ETAPA B: silver_conformed
-- Responsabilidad: integridad referencial ENTRE tablas.
-- Valida order_id contra Orders.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.payments AS
SELECT p.*
FROM main.tpt_silver.payments_clean AS p
INNER JOIN main.tpt_silver.orders AS o
  ON p.order_id = o.order_id;
