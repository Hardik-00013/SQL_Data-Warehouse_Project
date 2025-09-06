-- Remember the naming convention for the stored procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_full DATETIME, @end_time_full DATETIME

	BEGIN TRY
		SET @start_time_full = GETDATE();
		PRINT '=========================';
		PRINT 'Loading the Bronze Layer';
		PRINT '=========================';

		PRINT '======================';
		PRINT 'Loading the CRM tables';
		PRINT '======================';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT 'Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info  
		FROM 'C:\Users\hardi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info  
		FROM 'C:\Users\hardi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\hardi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


		PRINT '======================';
		PRINT 'Loading the ERP tables';
		PRINT '======================';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT 'Inserting data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\Users\hardi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Inserting data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\hardi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);


		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Inserting data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\hardi\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Time: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR);

		PRINT '--------------'
		SET @end_time_full = GETDATE();
		PRINT 'Total time taken for the load is :' + CAST(DATEDIFF(SECOND, @start_time_full, @end_time_full) AS NVARCHAR);
	END TRY
		
	BEGIN CATCH
		PRINT 'Error Occured During Bronze Layer'
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
END
