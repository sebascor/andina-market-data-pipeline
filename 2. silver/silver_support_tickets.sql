-- ============================================================================
-- SILVER LAYER — SUPPORT TICKETS
-- SupportTickets no tiene issues de calidad inyectados intencionalmente —
-- los checks quedan como safeguard general, consistente con el resto
-- de tablas del pipeline.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ETAPA A: silver_clean
-- Responsabilidad: limpieza INTRA-tabla (dedupe, tipos, categóricas)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.support_tickets_clean
(
  CONSTRAINT ticket_id_not_null EXPECT (ticket_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  ticket_id,
  order_id,
  customer_id,
  subject,
  body_text,

  -- Check 4: estandarización de categórica (safeguard; status ya viene
  -- limpio desde origen — open/in_progress/resolved/closed)
  LOWER(TRIM(status)) AS status,

  -- Check 2: cast de tipos
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(updated_at AS TIMESTAMP) AS updated_at,

  -- Check 5: metadata de auditoría
  current_timestamp() AS _silver_processed_at

FROM main.tpt_bronze.support_tickets

-- Check 1: deduplicación (safeguard general). ticket_id es la clave nativa
-- de esta tabla y nunca fue nula/duplicada en origen, así que es segura
-- para particionar directamente (a diferencia de order_id en Orders).
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY updated_at DESC) = 1;


-- ----------------------------------------------------------------------------
-- ETAPA B: silver_conformed
-- Responsabilidad: integridad referencial ENTRE tablas.
-- Un ticket válido necesita AMBAS referencias correctas: el pedido al que
-- aplica (order_id → Orders) y el cliente que lo generó (customer_id →
-- Customers).
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.support_tickets AS
SELECT t.*
FROM main.tpt_silver.support_tickets_clean AS t
INNER JOIN main.tpt_silver.orders AS o
  ON t.order_id = o.order_id
INNER JOIN main.tpt_silver.customers AS c
  ON t.customer_id = c.user_id;


-- ----------------------------------------------------------------------------
-- CUARENTENA — FK inválida contra Orders
-- Si algún ticket de muestra cayó sobre uno de los 5 pedidos con order_id
-- NULL (issue de calidad #3), queda huérfano y aparece aquí.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.support_tickets_invalid_order_fk AS
SELECT
  t.*,
  'order_id no existe en orders (posible order_id nulo/eliminado en origen)' AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_silver.support_tickets_clean AS t
LEFT ANTI JOIN main.tpt_silver.orders AS o
  ON t.order_id = o.order_id;


-- ----------------------------------------------------------------------------
-- CUARENTENA — FK inválida contra Customers
-- Safeguard: no se esperan filas aquí (sin issues inyectados en esta FK).
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.support_tickets_invalid_customer_fk AS
SELECT
  t.*,
  'customer_id no existe en customers' AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_silver.support_tickets_clean AS t
LEFT ANTI JOIN main.tpt_silver.customers AS c
  ON t.customer_id = c.user_id;
