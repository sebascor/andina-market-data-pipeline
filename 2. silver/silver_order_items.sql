-- ============================================================================
-- SILVER LAYER — ORDER ITEMS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ETAPA A: silver_clean
-- Responsabilidad: limpieza INTRA-tabla (dedupe, tipos, valores de negocio)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.order_items_clean
(
  CONSTRAINT order_item_id_not_null EXPECT (order_item_id IS NOT NULL) ON VIOLATION DROP ROW,
  -- Check 7: valores de negocio inválidos (issue de calidad #6 inyectado)
  CONSTRAINT valid_quantity   EXPECT (quantity > 0)   ON VIOLATION DROP ROW,
  CONSTRAINT valid_unit_price EXPECT (unit_price > 0) ON VIOLATION DROP ROW
)
AS
SELECT
  order_item_id,
  order_id,
  sku,

  -- Check 2: cast de tipos
  CAST(quantity AS INT)              AS quantity,
  CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
  CAST(created_at AS TIMESTAMP)      AS created_at,

  -- Check 5: metadata de auditoría
  current_timestamp() AS _silver_processed_at

FROM main.tpt_bronze.order_items

-- Check 1: deduplicación (safeguard general)
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY created_at DESC) = 1;


-- ----------------------------------------------------------------------------
-- CUARENTENA — valores de negocio inválidos (cantidad/precio negativos)
-- Captura los 5 + 5 registros inyectados (issue de calidad #6)
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.order_items_invalid_values AS
SELECT
  *,
  CASE
    WHEN quantity <= 0 AND unit_price <= 0 THEN 'cantidad y precio unitario inválidos'
    WHEN quantity <= 0                     THEN 'cantidad negativa o cero'
    WHEN unit_price <= 0                   THEN 'precio unitario negativo o cero'
  END AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_bronze.order_items
WHERE quantity <= 0 OR unit_price <= 0;


-- ----------------------------------------------------------------------------
-- ETAPA B: silver_conformed
-- Responsabilidad: integridad referencial ENTRE tablas.
-- Valida order_id contra Orders y sku contra Products.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.order_items AS
SELECT oi.*
FROM main.tpt_silver.order_items_clean AS oi
INNER JOIN main.tpt_silver.orders AS o
  ON oi.order_id = o.order_id
INNER JOIN main.tpt_silver.products AS p
  ON oi.sku = p.sku;


-- ----------------------------------------------------------------------------
-- CUARENTENA — FK inválida contra Orders
-- Aquí caen, entre otros, los OrderItems de los 5 pedidos cuyo order_id
-- quedó NULL en Orders (issue de calidad #3) — huérfanos por diseño.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.order_items_invalid_fk AS
SELECT
  oi.*,
  'order_id no existe en orders (posible order_id nulo/eliminado en origen)' AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_silver.order_items_clean AS oi
LEFT ANTI JOIN main.tpt_silver.orders AS o
  ON oi.order_id = o.order_id;
