-- ============================================================================
-- SILVER LAYER — ORDERS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ETAPA A: silver_clean
-- Responsabilidad: limpieza INTRA-tabla (dedupe, tipos, nulls, categóricas)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.orders_clean
(
  -- Sin order_id no hay forma de unir con OrderItems/Payments/SupportTickets
  CONSTRAINT order_id_not_null EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  order_id,
  user_id,

  -- Check 2: cast de tipos
  CAST(order_date AS TIMESTAMP) AS order_date,

  -- Check 4: estandarización de categóricas (issue de calidad #5 inyectado)
  CASE
    WHEN UPPER(TRIM(channel)) IN ('WEB', 'WEBB')     THEN 'web'
    WHEN UPPER(TRIM(channel)) IN ('APP', 'AAP')       THEN 'app'
    WHEN UPPER(TRIM(channel)) IN ('STORE', 'STOREE')  THEN 'store'
    ELSE LOWER(TRIM(channel))
  END AS channel,

  LOWER(TRIM(status)) AS status,
  CAST(total AS DECIMAL(10, 2)) AS total,
  CAST(updated_at AS TIMESTAMP) AS updated_at,

  -- Check 5: metadata de auditoría
  current_timestamp() AS _silver_processed_at

FROM main.tpt_bronze.orders

-- Check 1: deduplicación (safeguard general, aunque Orders no tiene
-- duplicados exactos inyectados como Customers/Payments)
QUALIFY ROW_NUMBER() OVER (PARTITION BY source_row_id ORDER BY updated_at DESC) = 1;


-- ----------------------------------------------------------------------------
-- CUARENTENA — nulls detectados en silver_clean
-- Captura los 5 order_id NULL inyectados (issue de calidad #3)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.orders_null_id AS
SELECT
  *,
  'order_id nulo' AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_bronze.orders
WHERE order_id IS NULL;


-- ----------------------------------------------------------------------------
-- ETAPA B: silver_conformed
-- Responsabilidad: integridad referencial ENTRE tablas.
-- Aquí sí hay trabajo real: valida que user_id exista en customers,
-- captura las 5 FK inválidas inyectadas (issue de calidad #4).
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.orders AS
SELECT o.*
FROM main.tpt_silver.orders_clean AS o
INNER JOIN main.tpt_silver.customers AS c
  ON o.user_id = c.user_id;


-- ----------------------------------------------------------------------------
-- CUARENTENA — FK inválida detectada en silver_conformed
-- Captura CUST-99999, CUST-00099, CUST-88888, CUST-00000, UNKNOWN
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.orders_invalid_fk AS
SELECT
  o.*,
  'user_id no existe en customers' AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_silver.orders_clean AS o
LEFT ANTI JOIN main.tpt_silver.customers AS c
  ON o.user_id = c.user_id;
