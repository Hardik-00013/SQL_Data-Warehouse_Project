-- Making the table empty and the loading the data
TRUNCATE TABLE bronze.crm_cust_info;
-- Inserting data into table 'bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info  
FROM '' -- Enter your file path here
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
-- Checking the quality of data inserted, once the insert operation is complete
SELECT * FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_cust_info;-- comparing the number against the number of rows in CSV file


-- Making the table empty and the loading the data
TRUNCATE TABLE bronze.crm_prd_info;
-- Inserting data into table 'bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info  
FROM ''
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
-- Checking the quality of data inserted, once the insert operation is complete
SELECT * FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;-- comparing the number against the number of rows in CSV file


-- Making the table empty and the loading the data
TRUNCATE TABLE bronze.crm_sales_details;
-- Inserting data into table 'bronze.crm_sales_details'
BULK INSERT bronze.crm_sales_details 
FROM ''
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
-- Checking the quality of data inserted, once the insert operation is complete
SELECT * FROM bronze.crm_sales_details;
SELECT COUNT(*) FROM bronze.crm_sales_details;


-- Making the table empty and the loading the data
TRUNCATE TABLE bronze.erp_cust_az12;
-- Inserting data into table 'bronze.erp_cust_az12'
BULK INSERT bronze.erp_cust_az12 
FROM ''
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
-- Checking the quality of data inserted, once the insert operation is complete
SELECT * FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_cust_az12;


-- Making the table empty and the loading the data
TRUNCATE TABLE bronze.erp_loc_a101;
-- Inserting data into table 'bronze.erp_loc_a101'
BULK INSERT bronze.erp_loc_a101
FROM ''
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
-- Checking the quality of data inserted, once the insert operation is complete
SELECT * FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_loc_a101;


-- Making the table empty and the loading the data
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
-- Inserting data into table 'bronze.erp_px_cat_g1v2'
BULK INSERT bronze.erp_px_cat_g1v2
FROM ''
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);
-- Checking the quality of data inserted, once the insert operation is complete
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

