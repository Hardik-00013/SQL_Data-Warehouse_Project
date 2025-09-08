CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME2, @end_time DATETIME2, @start_time_whole DATETIME2, @end_time_whole DATETIME2
		BEGIN TRY 
		SET @start_time_whole = GETDATE();
		-- ============
		-- CRM Table: 1
		-- ============
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info ;
		PRINT '>> Inserting data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info 
			(cst_id, 
			cst_key, 
			cst_firstname,
			cst_lastname, 
			cst_material_status, 
			cst_gndr, 
			cst_create_date)

			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				CASE
					WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' -- Data Standardization
					WHEN UPPER(TRIM(cst_material_status)) = 'F' THEN 'Married'
				ELSE 'N/A' END AS cst_marital_status,
				CASE
					WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' -- To handle the upper case values and unwanted spaces
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'N/A' END AS cst_gndr,
				cst_create_date
			FROM 
				(SELECT 
					*,
					ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag_Last
				FROM bronze.crm_cust_info) AS t
			WHERE Flag_Last = 1 AND cst_id IS NOT NULL;

			SET @end_time = GETDATE();
			PRINT '>> The time taken to load this table into silver layer is: ' 
			+ CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds';

		-- ============
		-- CRM Table: 2
		-- ============
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info 
			(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
			-- Understand the transformations properly
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,-- Derived Column
				SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,-- Derived Column
				prd_nm,
				ISNULL(prd_cost, 0) AS prd_cost, -- Handled missing values
				CASE UPPER(TRIM(prd_line)) -- Assigned meaningful names instead of abbreviations
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'N/A' END AS prd_line,-- Handled missing values
				CAST(prd_start_dt AS DATE),
				-- For dates, start < end and end can be NULL, no price period should overlap with another
				CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
			FROM bronze.crm_prd_info;

			SET @end_time = GETDATE();
			PRINT '>> The time taken to load this table into silver layer is: ' 
			+ CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds';

		-- ============
		-- CRM Table: 3
		-- ============
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
			(sls_ord_num, 
			 sls_prd_key, 
			 sls_cust_id, 
			 sls_order_dt, 
			 sls_ship_dt, 
			 sls_due_dt, 
			 sls_sales, 
			 sls_quantity, 
			 sls_price)
			 -- Understand all the transformations
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END 
			AS sls_order_dt, -- 
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END 
			AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END 
			AS sls_due_dt,
			CASE
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity, 0)
				ELSE sls_price 
			END AS sls_price
		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> The time taken to load this table into silver layer is: ' 
		+ CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds';

		-- ============
		-- ERP Table: 1
		-- ============
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12
			(cid,
			 bdate,
			 gen)

			SELECT
				CASE
					WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid-- We removes the extra substring that would have prevented us from JOINing this table with crm_cust_info
				END AS cid,
				CASE
					WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END as bdate,
				CASE
					WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					ELSE 'N/A'
				END AS gen
			FROM bronze.erp_cust_az12;

			SET @end_time = GETDATE();
			PRINT '>> The time taken to load this table into silver layer is: ' 
			+ CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds';

		-- ============
		-- ERP Table: 2
		-- ============
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
			(cid, cntry)

			SELECT
				REPLACE(cid, '-', '') AS cid,
				CASE
					WHEN TRIM(cntry) IN ('DE') THEN 'Germany'
					WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					WHEN TRIM(cntry) IS NULL or TRIM(cntry) = '' THEN 'N/A'
					ELSE TRIM(cntry)
				END AS cntry
			FROM bronze.erp_loc_a101;

			SET @end_time = GETDATE();
			PRINT '>> The time taken to load this table into silver layer is: ' 
			+ CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds';

		-- ============
		-- ERP Table: 3
		-- ============
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
			(id, cat, subcat, maintenance)

		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> The time taken to load this table into silver layer is: ' 
		+ CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds';

		SET @end_time_whole = GETDATE();
		PRINT 'The total time taken for data load in silver layer is: '
		+ CAST(DATEDIFF(SECOND, @start_time_whole, @end_time_whole) AS NVARCHAR) + ' seconds';
	END TRY
	 
	BEGIN CATCH
		PRINT '===================================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================='
	END CATCH
END

EXEC silver.load_silver;
	

