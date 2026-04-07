/*
Procedure to load data from csv, procedure doesnt accept any parameter
________________________
EXEC bronze.load_bronze
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		
		SET @batch_start_time = GETDATE()

		PRINT '======================================================================================';
		PRINT 'Loading Bronze layer';
		PRINT '======================================================================================';

		PRINT '--------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info; --Incase you rerun the BULK INSERT, data will be inserted twice, manage it by using truncate

		PRINT '>> Tnserting Data Into: bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK  --  No tablock-> other users can access table and row by row lock multiple lock, using tabloc -> one lock, no one can use the table
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prod_info';

		TRUNCATE TABLE bronze.crm_prod_info; 

		PRINT '>> Tnserting Data Into: bronze.crm_prod_info';

		BULK INSERT bronze.crm_prod_info
		FROM 'D:\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK  
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sls_info';

		TRUNCATE TABLE bronze.crm_sls_info; 

		PRINT '>> Tnserting Data Into: bronze.crm_sls_info';

		BULK INSERT bronze.crm_sls_info
		FROM 'D:\Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK  
		)

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'

		
		PRINT '--------------------------------------------------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '--------------------------------------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_AZ12';

		TRUNCATE TABLE bronze.erp_cust_AZ12; 

		PRINT '>> Tnserting Data Into: bronze.erp_cust_AZ12';

		BULK INSERT bronze.erp_cust_AZ12
		FROM 'D:\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK  
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_LOC_A101';

		TRUNCATE TABLE bronze.erp_LOC_A101; 

		PRINT '>> Tnserting Data Into: bronze.erp_LOC_A101'; 

		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK  
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_PX_CAT_G1V2';

		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2; 

		PRINT '>> Tnserting Data Into: bronze.erp_PX_CAT_G1V2'

		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK 
		)

		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET  @batch_end_time = GETDATE()
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time , @batch_end_time ) AS NVARCHAR) + ' seconds'
		
	END TRY
	BEGIN CATCH
		PRINT '======================================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================================================';
	END CATCH
END
