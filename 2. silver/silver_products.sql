-- ============================================================================
-- SILVER LAYER — PRODUCTS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ETAPA A: silver_clean
-- Responsabilidad: limpieza INTRA-tabla (dedupe, tipos, valores de negocio)
-- Products no tiene issues de calidad inyectados (sin duplicados, sin nulls,
-- sin categóricas sucias) — los checks quedan como safeguard general,
-- consistente con el resto de tablas, aunque no se espere que rechacen nada.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.products
(
  CONSTRAINT sku_not_null    EXPECT (sku IS NOT NULL)  ON VIOLATION DROP ROW,
  CONSTRAINT valid_price     EXPECT (price > 0)         ON VIOLATION DROP ROW
)
AS
SELECT
  sku,
  name,
  category,

  -- Check 2: cast de tipos
  CAST(price AS DECIMAL(10, 2)) AS price,

  description,
  LOWER(TRIM(status))           AS status,
  CAST(created_at AS TIMESTAMP) AS created_at,

  -- Check 5: metadata de auditoría
  current_timestamp() AS _silver_processed_at

FROM main.tpt_bronze.products

-- Check 1: deduplicación (safeguard general)
QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY created_at DESC) = 1;


-- ----------------------------------------------------------------------------
-- CUARENTENA — safeguard general (sku nulo o precio inválido)
-- Se espera 0 filas: Products no tiene issues de calidad inyectados.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.products AS
SELECT
  *,
  CASE
    WHEN sku IS NULL THEN 'sku nulo'
    WHEN price <= 0  THEN 'precio inválido'
  END AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_bronze.products
WHERE sku IS NULL OR price <= 0;
