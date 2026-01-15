/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Purpose:
    This script defines the DDL for tables in the `bronze` schema.
    Existing tables will be dropped and recreated to ensure a consistent
    and up-to-date schema definition.

Usage:
    Run this script to (re)initialize the Bronze layer table structures.
===============================================================================
*/



-- -----CRM------------------
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INTEGER,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id        INTEGER,
    prd_key       TEXT,
    prd_nm        TEXT,
    prd_cost      INTEGER,
    prd_line      TEXT,
    prd_start_dt  TIMESTAMP,
    prd_end_dt    TIMESTAMP
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num    TEXT,
    sls_prd_key    TEXT,
    sls_cust_id    INTEGER,
    sls_order_dt   INTEGER,
    sls_ship_dt    INTEGER,
    sls_due_dt     INTEGER,
    sls_sales      INTEGER,
    sls_quantity   INTEGER,
    sls_price      INTEGER
);

-- --------------ERP----------------
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid   TEXT,
    cntry TEXT
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid   TEXT,
    bdate DATE,
    gen   TEXT
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          TEXT,
    cat         TEXT,
    subcat      TEXT,
    maintenance TEXT
);

