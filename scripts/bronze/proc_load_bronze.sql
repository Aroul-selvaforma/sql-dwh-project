/*
This stored procedure to load the bronze layer completely
We truncate the tables
we do Bulk INSERT to load data from csv files to bronze tables

no parameters  are used
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME, @bronze_start_time DATETIME, @bronze_end_time DATETIME
	BEGIN TRY
		SET @bronze_start_time = GETDATE();
		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';
		/*CRM*/
		PRINT '------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info -- because we want to do a full load
		PRINT '>> Inserting Data Into Table bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Protfolios\SQL Datawarehouse\sql-dwh-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info -- because we want to do a full load
		PRINT '>> Inserting Data Into Table bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Protfolios\SQL Datawarehouse\sql-dwh-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details -- because we want to do a full load
		PRINT '>> Inserting Data Into Table bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Protfolios\SQL Datawarehouse\sql-dwh-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);

		/*ERP*/
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12 -- because we want to do a full load
		PRINT '>> Inserting Data Into Table bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Protfolios\SQL Datawarehouse\sql-dwh-project\datasets\source_erp\cust_az12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table bronze..erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101 -- because we want to do a full load
		PRINT '>> Inserting Data Into Table bronze..erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Protfolios\SQL Datawarehouse\sql-dwh-project\datasets\source_erp\loc_a101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);
		PRINT '-----------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2 -- because we want to do a full load
		PRINT '>> Inserting Data Into Table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Protfolios\SQL Datawarehouse\sql-dwh-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		ROWTERMINATOR = '\n',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR);

		SET @bronze_end_time = GETDATE();
		PRINT '-----------------------------------------------------------------------'
		PRINT '>>Loading Bronze layer completed. Total Duration ' + CAST(DATEDIFF(second, @bronze_start_time,@bronze_end_time) AS NVARCHAR);
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'EROR Message :'+ERROR_MESSAGE();
		PRINT 'EROR Message :'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'EROR Message :'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
	
END;
