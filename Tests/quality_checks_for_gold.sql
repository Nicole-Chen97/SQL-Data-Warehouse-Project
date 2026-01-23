/*
===============================================================================
Gold Layer - Data Quality Checks (PostgreSQL)
===============================================================================
Script Purpose:
    This script validates the quality of data in the 'gold' schema after the
    Silver → Gold ETL is completed. The checks focus on ensuring Gold tables are
    analytics-ready by verifying:

    - Surrogate key integrity: NULL or duplicate keys in dimension tables
    - Referential integrity: fact foreign keys correctly match dimension keys
    - Data model consistency: no orphan records between fact and dimensions
    - (Optional) Fact sanity: NULL / invalid measures where not allowed


Usage Notes:
    - Run these checks after building the Gold Layer (after Silver → Gold load).
    - Any returned rows indicate records to investigate.
    - Fix issues by adjusting Gold transformation logic (preferred), or by
      tracing back to Silver layer anomalies when necessary.

===============================================================================
*/


-- ============================================================================
-- 1) gold.dim_customers
-- ============================================================================

-- 1.1 Dimension key integrity : customer_key should be unique and not null
-- Expectation: 0 rows
SELECT customer_key, COUNT(*) AS cnt
FROM gold.dim_customers
GROUP BY customer_key
HAVING customer_key IS NULL OR COUNT(*) > 1;



-- ============================================================================
-- 2) gold.dim_products
-- ============================================================================
-- 2.1 gold.dim_products: product_key should be unique and not null
-- Expectation: 0 rows
SELECT product_key, COUNT(*) AS cnt
FROM gold.dim_products
GROUP BY product_key
HAVING product_key IS NULL OR COUNT(*) > 1;


-- ============================================================================
-- 3) gold.fact_sales 
-- ============================================================================
-- 3.1 gold.fact_sales connectivity: every FK should match an existing dimension row
-- Expectation: 0 rows
SELECT
  f.customer_key,
  f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
  ON p.product_key = f.product_key
WHERE c.customer_key IS NULL
   OR p.product_key IS NULL;



