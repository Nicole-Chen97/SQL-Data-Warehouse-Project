/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

-- ============================================================================
-- 1) silver.crm_cust_info
-- ============================================================================
-- 1.1 Key integrity: NULL or duplicate cst_id
-- Expectation: 0 rows
SELECT cst_id, COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL  ;

-- 1.2 Unwanted spaces (TRIM)
-- Expectation: 0 rows
SELECT cst_id,cst_lastname,cst_firstname,cst_marital_status
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) 
  OR cst_firstname!=TRIM(cst_firstname) 
  OR cst_marital_status != TRIM(cst_marital_status);

-- 1.3 Domain check (review distinct values)
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ============================================================================
-- 2) silver.crm_prd_info
-- ============================================================================
-- 2.1 Key integrity: NULL or duplicate prd_id
-- Expectation: 0 rows

SELECT prd_id ,COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- 2.2 Unwanted spaces
-- Expectation: 0 rows
SELECT 
prd_key, prd_nm
FROM silver.crm_prd_info
WHERE  prd_key != TRIM(prd_key) 
  OR prd_nm!=TRIM(prd_nm) ;

-- 2.3 Cost validity : for NULLs or Negative Values
-- Expectation: 0 rows
SELECT prd_id, prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL;


-- 2.4 Domain check
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- 2.5 Date order: end date should not be earlier than start date
-- Expectation: 0 rows
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- ============================================================================
-- 3) silver.crm_sales_details
-- ============================================================================
-- 3.1 Grain-aware duplicate check (Needs confirmation!)
-- If sls_ord_num is truly unique in your model, keep this.
-- Otherwise consider checking a composite key (e.g., sls_ord_num + prd_id).
---row count : duplicate -----
SELECT sls_ord_num ,COUNT(*) AS cnt
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING sls_ord_num IS NULL OR COUNT(*) > 1;

-- 3.2 Raw date validity (from bronze) - ensure yyyymmdd is 8 digits and within range
-- Expectation: 0 rows
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
	OR LENGTH(sls_due_dt::text) != 8   ---- need to change to text
	OR sls_due_dt > 20500101 
	OR sls_due_dt < 19000101;

-- 3.3 Date order checks (order date should not be after ship/due date)
-- Expectation: 0 rows 
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
  OR sls_order_dt > sls_due_dt

-- 3.4 Numeric validity (no NULL or non-positive for key measures)：Sales, Quantity, Price
-- Expectation: 0 rows (unless returns/refunds exist and are represented differently)
SELECT sls_sales , sls_quantity ,sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
 OR sls_quantity IS NULL OR sls_quantity <= 0
 OR sls_price IS NULL OR sls_price <=0
 OR sls_sales IS NULL OR sls_sales <= 0 ;
 
-- 3.5 Cross-field consistency: sales = quantity * price
-- Expectation: 0 rows (consider rounding rules if decimals exist)
SELECT sls_ord_num, sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales <> sls_quantity * sls_price;
 
 
-- Check for NULLs or Negative Values in Cost
SELECT  prd_cost
FROM silver.crm_sales_details
WHERE prd_cost <0 OR prd_cost IS NULL



-- ============================================================================
-- 4) silver.erp_cust_az12
-- ============================================================================
-- 4.1 Duplicate customer id 
SELECT cid , COUNT(*) AS cnt
FROM silver.erp_cust_az12
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- 4.2 Birthdate range
SELECT cid, bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
  OR bdate > CURRENT_DATE;

-- 4.3 Domain check
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- 4.4 Unwanted spaces in gen (if stored as text)
SELECT cid, gen 
FROM silver.erp_cust_az12
WHERE gen != TRIM(gen);

-- ============================================================================
-- 5) silver.erp_loc_a101
-- ============================================================================
-- 5.1 Duplicate id
SELECT cid, COUNT(*)
FROM silver.erp_loc_a101
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) >1;

-- 5.2 Domain check : cntry
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

-- 5.3 Unwanted spaces
SELECT cntry 
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry);


-- ============================================================================
-- 6) silver.erp_px_cat_g1v2
-- ============================================================================
-- 6.1 Duplicate id 
SELECT id , COUNT(*) AS cnt
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING id IS NULL OR COUNT(*) >1;

-- 6.2 Domain check
SELECT DISTINCT cat ,subcat
FROM silver.erp_px_cat_g1v2
ORDER BY cat, subcat;

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1vs;

-- 6.3 Unwanted spaces across key text columns
-- Expectation: 0 rows
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) 
  OR subcat<> TRIM(subcat) 
  OR maintenance <> TRIM(maintenance)

