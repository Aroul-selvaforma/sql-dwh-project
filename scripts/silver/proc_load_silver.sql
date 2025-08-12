/*
This stored procedure to load the silver layer completely
We truncate the tables
we do INSERT to load data from bronze tables

no parameters  are used
Usage Exemple:
EXEC Silver.proc_load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME , @end_time DATETIME, @silver_start_time DATETIME, @silver_end_time DATETIME
	BEGIN TRY
		SET @silver_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';
		/*CRM*/
		PRINT '------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info -- because we want to do a full load
		PRINT '>> Inserting Data Into Table silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date)
		SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,--REMOVING UNWANTED SPACES
		TRIM(cst_lastname) AS cst_lastname,--REMOVING UNWANTED SPACES
		--DATA NORMALISATION or STANDARDIZATION
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEn 'Married'
		ELSE 'n/a' -- HANDLING MISSING VALUES
		END cst_marital_status,
		--DATA NORMALISATION or STANDARDIZATION
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEn 'Male'
		ELSE 'n/a' -- HANDLING MISSING VALUES
		END cst_gndr,
		cst_create_date
		FROM ( 
		SELECT *,
		--REMOVE THE DUPLICATES
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)  as flag_last
		FROM bronze.crm_cust_info) t
		WHERE flag_last = 1 -- DATA FILTERING

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info -- because we want to do a full load
		PRINT '>> Inserting Data Into Table silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt

		)

		SELECT 
		prd_id,
		-- Transformations : Derived new columns (based on calculations of existing one)
		REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
		REPLACE(SUBSTRING(prd_key, 7, LEN(prd_key)), '-','_') AS prd_key,
		prd_nm,
		--handling missing info
		ISNULL(prd_cost,0) AS prd_cost,
		--data normalization and handling missing data
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a'
			 END AS prd_line,
			 --data type casting
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		/*prd_start_dt is sometimes > prd_end_dt
		So we use lead function to take the next start date and to put in the current end date*/
		--data type casting end data enrichment (adding new, relevant data to enhance the dataset for analysis)
		CAST((LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details -- because we want to do a full load
		PRINT '>> Inserting Data Into Table silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales ,
		sls_quantity ,
		sls_price 
		)

		SELECT sls_ord_num,
			   sls_prd_key,
			   sls_cust_id,
			   --handling invalid data data type casting
			   CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			   END AS sls_order_dt,
			   CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			   END AS sls_ship_dt,
			   CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			   END AS sls_due_dt,
			   -- handling missing valuez and invalid data 
			   CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity*ABS(sls_price)
					ELSE sls_sales
			   END AS sls_sales,
			   sls_quantity,
			   CASE WHEN sls_price IS NULL OR sls_price <=0
					THEN sls_sales/NULLIF(sls_quantity,0)
					ELSE sls_price
			   END AS sls_price
		  FROM DataWarehouse.bronze.crm_sales_details

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12 -- because we want to do a full load
		PRINT '>> Inserting Data Into Table silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEn NULL
			 ELSE bdate
		END bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEn 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101 -- because we want to do a full load
		PRINT '>> Inserting Data Into Table silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		SELECT
		REPLACE(cid,'-','') AS cid,-- handled invalid values
		CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry -- data normalization, missing valuez
		FROM bronze.erp_loc_a101

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2 -- because we want to do a full load
		PRINT '>> Inserting Data Into Table silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
		)
		-- no need to transform because data quality is good
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);

		SET @silver_end_time = GETDATE();
		PRINT '-----------------------------------------------------------------------'
		PRINT '>>Loading Silver layer completed. Total Duration ' + CAST(DATEDIFF(second, @silver_start_time,@silver_end_time) AS NVARCHAR);
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'EROR Message :'+ERROR_MESSAGE();
		PRINT 'EROR Message :'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'EROR Message :'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
