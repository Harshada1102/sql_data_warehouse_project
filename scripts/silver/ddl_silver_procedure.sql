/*
Procedure performs ETL (Extract, Transform and Load)
Loads data from bronze schema tables to silver schema tables.

Procedure doesnt accept any parameters

Action performed:

Truncate silver table
Transform and insert data into silver table

Usage example:

EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		
		SET @batch_start_time = GETDATE()

		PRINT '======================================================================================';
		PRINT 'Loading Silver layer';
		PRINT '======================================================================================';

		PRINT '--------------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables'
		PRINT '--------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();

        PRINT '>>Truncating Table : silver.crm_cust_info'
        TRUNCATE TABLE silver.crm_cust_info
        PRINT '>>Inserting data into table: silver.crm_cust_info'

        INSERT INTO silver.crm_cust_info (
            cust_id,
            cust_key,
            cust_firstname,
            cust_lastname,
            cust_marital_status,
            cust_gndr,
            cust_create_date
        )
        SELECT 
        cust_id,
        cust_key,
        TRIM(cust_firstname) cust_firstname,
        TRIM(cust_lastname) cust_lastname,
        CASE WHEN UPPER(TRIM(cust_marital_status)) = 'S' then 'Single'
             WHEN UPPER(TRIM(cust_marital_status)) = 'M' then 'Married'
             ELSE 'n/a'
        END cust_marital_status,
        CASE WHEN UPPER(TRIM(cust_gndr)) = 'F' then 'Female'
             WHEN UPPER(TRIM(cust_gndr)) = 'M' then 'Male'
             ELSE 'n/a'
        END cust_gndr,
        cust_create_date
        FROM(
            SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cust_id IS NOT NULL
            ) t 
        WHERE flag_last =1

        SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();

        PRINT 'Truncating Table : silver.crm_prod_info'
        TRUNCATE TABLE silver.crm_prod_info
        PRINT 'Inserting data into table: silver.crm_prod_info'

        INSERT INTO silver.crm_prod_info(
        prod_id,
        cat_id,
        prod_key,
        prod_nm,
        Prod_cost,
        prod_line,
        prod_start_dt,
        prod_end_dt
        )
        SELECT
        prod_id,
        REPLACE(SUBSTRING(prod_key, 1, 5),'-','_') AS cat_id,
        SUBSTRING(prod_key, 7, LEN(prod_key)) AS prod_key,
        prod_nm,
        ISNULL(prod_cost,0) AS prod_cost,
        CASE UPPER(TRIM(prod_line)) 
             WHEN 'R' THEN 'Road'
             WHEN 'M' THEN 'Mountain'
             WHEN 'S' THEN 'Other sales'
             WHEN 'T' THEN 'Touring'
             ELSE 'n/a'
        END AS prod_line,
        prod_start_dt,
        DATEADD(DAY, -1, LEAD(prod_start_dt) OVER(PARTITION BY prod_key ORDER BY prod_start_dt) )AS prod_end_dt 
        FROM bronze.crm_prod_info

        SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();

        PRINT 'Truncating Table : silver.crm_sls_info'
        TRUNCATE TABLE silver.crm_sls_info
        PRINT 'Inserting data into table: silver.crm_sls_info'

        INSERT INTO silver.crm_sls_info (
        sls_ord_num,
        sls_prod_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
        SELECT
        sls_ord_num,
        sls_prod_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
        END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
        END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
        END AS sls_due_dt,
        CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <=0 
                THEN sls_sales/NULLIF(sls_quantity,0)
            ELSE sls_price
        END AS sls_price
        FROM bronze.crm_sls_info

        SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();

        PRINT 'Truncating Table : silver.erp_cust_AZ12'
        TRUNCATE TABLE silver.erp_cust_AZ12
        PRINT 'Inserting data into table: silver.erp_cust_AZ12'

        INSERT INTO silver.erp_cust_AZ12(
        CID,
        BDATE,
        GEN
        )
        SELECT 
        CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) 
	        ELSE CID
        END AS CID,
        CASE WHEN BDATE > GETDATE() THEN NULL
	        ELSE BDATE
        END AS BDATE,
        CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
	        WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
	        ELSE 'n/a'
        END AS GEN
        FROM bronze.erp_cust_AZ12

        SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();

        PRINT 'Truncating Table : silver.erp_LOC_A101'
        TRUNCATE TABLE silver.erp_LOC_A101
        PRINT 'Inserting data into table: silver.erp_LOC_A101'

        INSERT INTO silver.erp_LOC_A101(
        CID,
        CNTRY)
        SELECT 
        REPLACE(CID,'-','') CID,
        CASE WHEN TRIM(CNTRY) ='DE' THEN 'Germany'
            WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
            WHEN TRIM(CNTRY) = '' OR TRIM(CNTRY)  IS NULL THEN 'n/a'
            ELSE TRIM(CNTRY)
        END AS CNTRY
        FROM bronze.erp_LOC_A101

        SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET @start_time = GETDATE();

        PRINT 'Truncating Table : silver.erp_PX_CAT_G1V2'
        TRUNCATE TABLE silver.erp_PX_CAT_G1V2
        PRINT 'Inserting data into table: silver.erp_PX_CAT_G1V2'

        INSERT INTO silver.erp_PX_CAT_G1V2(
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
        )
        SELECT 
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE
        FROM bronze.erp_PX_CAT_G1V2

        SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>----------------'

		SET  @batch_end_time = GETDATE()
		PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time , @batch_end_time ) AS NVARCHAR) + ' seconds'
		
	END TRY
	BEGIN CATCH
		PRINT '======================================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================================================';
	END CATCH
END
