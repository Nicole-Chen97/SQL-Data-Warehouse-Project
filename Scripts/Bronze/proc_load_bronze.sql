
/*
===============================================================================
Stored Procedure: bronze.load_bronze (CSV Source -> Bronze Schema)
===============================================================================
Script Purpose:
    This stored procedure loads raw source data from external CSV files into the
    'bronze' schema (landing layer). It performs a full refresh load and records
    basic runtime logs using RAISE NOTICE.

    Key actions:
    - Truncates each bronze table before loading (full reload).
    - Uses PostgreSQL COPY to bulk load CSV files from the local filesystem path
      (e.g., /data/source_crm, /data/source_erp).
    - Prints per-table load duration and total batch duration.
    - On error, prints the error message and re-raises the exception so the whole
      transaction is rolled back (no partial loads).

Parameters:
    None.
    This procedure does not accept parameters and does not return a value.

Prerequisites / Notes:
    - The PostgreSQL server must be able to read the CSV paths used in COPY.
      In Docker setups, this typically means mounting the host folder into the
      Postgres container (e.g., mapped to /data/...).
    - CSV files are expected to have headers (HEADER true) and use comma delimiter.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/






CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE 
	start_time 			TIMESTAMP;
	end_time   			TIMESTAMP;
	batch_start_time	TIMESTAMP;
	batch_end_time		TIMESTAMP;
--begin : 區塊化，讓跑的時候是大量跑，非一筆一筆---
BEGIN
	---類似 	try ----
	BEGIN 
		batch_start_time := clock_timestamp();
		RAISE NOTICE '================================================';
		RAISE NOTICE 'Load Bronze Layer';
		RAISE NOTICE '================================================';
		RAISE NOTICE '------------------------------------------------';
	    RAISE NOTICE 'Loading CRM Tables';
	    RAISE NOTICE '------------------------------------------------';
	
	
		-- ===== crm_cust_info =====
	
	
		-----:= 賦值語法--------
	-----TRUNCATE TABLE  RESTART IDENTITY 清空原先有的資料，包含重置「自動編號用的 sequence」----
		----% 是「佔位符」，是「佔位符」，後面用逗號把變數丟進來------
		-----EXTRACT : 「從這個時間（或時間差）裡，取出我想要的那個部分」---
		---EPOCH = 「從 1970-01-01 起算的秒數」----
		-----end_time - start_time   -- 得到 interval 00:00:03.527891
		
		-----crm_cust_info------
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info ;
		RAISE NOTICE '	 Truncating Table: bronze.crm_cust_info >>';
		COPY bronze.crm_cust_info
		FROM '/data/source_crm/cust_info.csv'
		WITH(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>>> LOAD Duration : % seconds', 
			EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
		
	
		-----crm_prd_info------
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info ;
		RAISE NOTICE '	 Truncating Table: bronze.crm_prd_info >>';
		COPY bronze.crm_prd_info
		FROM '/data/source_crm/prd_info.csv'
		WITH(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>>> LOAD Duration : % seconds', 
			EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
	
	
	
		-----crm_sales_details------
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details ;
		RAISE NOTICE '	 Truncating Table: bronze.crm_sales_details>>';
		COPY bronze.crm_sales_details
		FROM '/data/source_crm/sales_details.csv'
		WITH(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>>> LOAD Duration : % seconds', 
			EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
	
	
		-- ===== erp_cust_info =====
		
		-----erp_loc_a101------
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101 ;
		RAISE NOTICE '	 Truncating Table: bronze.erp_loc_a101>>';
		COPY bronze.erp_loc_a101
		FROM '/data/source_erp/loc_a101.csv'
		WITH(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>>> LOAD Duration : % seconds', 
			EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
	
	
		-----erp_cust_az12------
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 ;
		RAISE NOTICE '	 Truncating Table: bronze.erp_cust_az12>>';
		COPY bronze.erp_cust_az12
		FROM '/data/source_erp/cust_az12.csv'
		WITH(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>>> LOAD Duration : % seconds', 
			EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
	
	
	
		-----erp_px_cat_g1v2------
		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 ;
		RAISE NOTICE '	 Truncating Table: bronze.erp_px_cat_g1v2>>';
		COPY bronze.erp_px_cat_g1v2
		FROM '/data/source_erp/px_cat_g1v2.csv'
		WITH(
			FORMAT csv,
			HEADER true,
			DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>>> LOAD Duration : % seconds', 
			EXTRACT(EPOCH FROM (end_time - start_time));
		RAISE NOTICE '>> -------------';
	
	
	-------last---------------	
		batch_end_time := clock_timestamp();
	    RAISE NOTICE '================================================';
	    RAISE NOTICE 'Bronze Load Finished. Total Duration: % seconds',
	    EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
	    RAISE NOTICE '================================================';

    EXCEPTION
        WHEN OTHERS THEN
            -- 類似 SQL Server CATCH：印錯誤 + 拋出
            RAISE NOTICE '!! ERROR in bronze.load_bronze: %', SQLERRM;
            RAISE;
    END;
END;
$$;
