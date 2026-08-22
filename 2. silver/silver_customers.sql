-- ============================================================================
-- SILVER LAYER — CUSTOMERS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ETAPA A: silver_clean
-- Responsabilidad: limpieza INTRA-tabla (dedupe, tipos, nulls, categóricas)
-- No mira otras tablas todavía.
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_silver.customers
(
  -- Check 3: manejo de nulls — cualquier fila sin user_id no es utilizable
  CONSTRAINT user_id_not_null EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW,
  -- Check 2 (parcial): formato de email válido
  CONSTRAINT valid_email_format EXPECT (email RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')

)
AS
SELECT
  user_id,
  name,
  email,
  phone,

  city,

  -- Check 4: estandarización de categóricas (issue de calidad #2 inyectado)
  CASE
    WHEN UPPER(TRIM(country)) IN ('COLOMBIA', 'COL')            THEN 'Colombia'
    WHEN UPPER(TRIM(country)) IN ('MEXICO', 'MX', 'MÉXICO')     THEN 'México'
    WHEN UPPER(TRIM(country)) IN ('PERU', 'PE', 'PERÚ')          THEN 'Perú'
    WHEN UPPER(TRIM(country)) IN ('CHILE', 'CL')                 THEN 'Chile'
    WHEN UPPER(TRIM(country)) IN ('ARGENTINA', 'ARG')            THEN 'Argentina'
    WHEN UPPER(TRIM(country)) IN ('ECUADOR', 'ECU')              THEN 'Ecuador'
    ELSE country
  END AS country,

  -- Check 2: cast/validación de tipos
  CAST(signup_date AS TIMESTAMP) AS signup_date,
  LOWER(TRIM(segment))            AS segment,
  CAST(updated_at AS TIMESTAMP)   AS updated_at,

  -- Check 5: metadata de auditoría
  current_timestamp() AS _silver_processed_at

FROM main.tpt_bronze.customers

-- Check 1: deduplicación por clave de negocio.
-- Nos quedamos con la versión más reciente por source_row_id
-- (resuelve los 5 duplicados exactos inyectados en customers.csv)
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY source_row_id DESC) = 1;


-- ----------------------------------------------------------------------------
-- CUARENTENA: filas que no pasaron los checks de silver_clean
-- Nunca se descartan silenciosamente — quedan trazadas con su razón de rechazo
-- ----------------------------------------------------------------------------

CREATE OR REFRESH MATERIALIZED VIEW main.tpt_quarantine.customers AS
SELECT
  *,
  CASE
    WHEN user_id IS NULL THEN 'user_id nulo'
    WHEN NOT email RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$' THEN 'formato de email inválido'
    ELSE 'razón desconocida'
  END AS rejection_reason,
  current_timestamp() AS _quarantined_at
FROM main.tpt_bronze.customers
WHERE user_id IS NULL
   OR NOT email RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$';
