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


CALL silver.load_silver();


SELECT * FROM silver.crm_cust_info

-- ==================
------------------crm_cust_info
-- ==================
---row count : duplicate -----
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 

---id null----
SELECT cst_id 
FROM silver.crm_cust_info
WHERE cst_id is NULL


---- trim -----
SELECT cst_id,
cst_lastname,
cst_firstname,
cst_marital_status
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname) OR cst_firstname!=TRIM(cst_firstname) OR cst_marital_status != TRIM(cst_marital_status)


---DISTINCT---
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info





-- ==================
------------------silver.crm_prd_info
-- ==================
---row count : duplicate -----
SELECT prd_id ,COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1


---id null----
SELECT prd_id 
FROM silver.crm_prd_info
WHERE prd_id is NULL


---- trim -----
SELECT 
prd_key, prd_nm
FROM silver.crm_prd_info
WHERE  prd_key != TRIM(prd_key) OR prd_nm!=TRIM(prd_nm) 




---DISTINCT---
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for NULLs or Negative Values in Cost
SELECT  prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL


---Date 
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


-- ====================================================================
-- Checking 'silver.crm_sales_details'

---row count : duplicate -----
SELECT sls_ord_num ,COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*)>1


---id null----
SELECT sls_ord_num 
FROM silver.crm_sales_details
WHERE sls_ord_num is NULL



-- Check for NULLs or Negative Values in Cost
SELECT  prd_cost
FROM silver.crm_sales_details
WHERE prd_cost <0 OR prd_cost IS NULL

---Date 
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
	OR LENGTH(sls_due_dt::text) != 8   ---- need to change to text
	OR sls_due_dt > 20500101 
	OR sls_due_dt < 19000101;

---Date 
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt



----Sales Quantity Price
SELECT sls_sales , sls_quantity ,sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
 OR sls_quantity IS NULL 
 OR sls_price IS NULL 
 OR sls_sales IS NULL 
 OR sls_quantity <= 0
 OR sls_price <=0
 OR sls_sales <= 0 




-- ====================================================================
-- Checking 'silver.erp_cust_az12'
SELECT cid , COUNT(*)
FROM silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) >1



SELECT cid, bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > CURRENT_DATE;-------check data propecessing 



SELECT DISTINCT gen
FROM silver.erp_cust_az12

SELECT cid, gen 
FROM silver.erp_cust_az12
WHERE gen != TRIM(gen)



-- ====================================================================
-- Checking 'silver.erp_loc_a101'

SELECT cid, COUNT(*)
FROM silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) >1


SELECT DISTINCT cntry
FROM silver.erp_loc_a101


SELECT cntry 
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry)


-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
SELECT id , COUNT(*)
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) >1


SELECT DISTINCT cat ,subcat
FROM silver.erp_px_cat_g1v2
ORDER BY cat

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1vs


SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat<> TRIM(subcat) OR maintenance <> TRIM(maintenance)





