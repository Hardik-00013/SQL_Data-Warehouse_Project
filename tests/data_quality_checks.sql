-- Check for NULLs and duplicates in the primary key
-- Expectation: No result
SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
	GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: no results
-- check seperately for each column in order to see where to apply the TRIM() function in the transformation query
SELECT 
	*
FROM bronze.crm_prd_info
	WHERE prd_nm != TRIM(prd_nm);

-- Data Standardization & Consistency
SELECT DISTINCT
	prd_line
FROM silver.crm_prd_info;

SELECT DISTINCT
	gen
FROM bronze.erp_cust_az12;
-- For a numbers column, check for any NULLs or Negative numbers
-- Expectation: No results
SELECT 
	*
FROM silver.crm_prd_info
	WHERE prd_cost IS NULL OR prd_cost < 0;

-- End date must not be smallar then the start date
SELECT
	*
FROM silver.crm_prd_info
	WHERE prd_end_dt < prd_start_dt;
	

-- Filter out unmatched data after applying the transformation
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
	WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
(SELECT distinct id FROM bronze.erp_px_cat_g1v2);

-- Filtering out unmatched data after applying the transformation
-- The answer for this query are the products that donot have any orders
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
	WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) 
	NOT IN (SELECT DISTINCT sls_prd_key FROM bronze.crm_sales_details);

SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
	WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- Query to check the data quality of the first column
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
	WHERE sls_ord_num != TRIM(sls_ord_num);

/* Query to check the data quality of the second column, 
   this column is used to JOIN it with silver.crm_prd_info table */
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
	WHERE sls_prd_key NOT IN (SELECT DISTINCT prd_key FROM silver.crm_prd_info);

/* Query to check the data quality of the second column, 
   this column is used to JOIN it with silver.crm_prd_info table, cst_id column */
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
	WHERE sls_cust_id NOT IN (SELECT DISTINCT cst_id FROM silver.crm_cust_info);

/* Making sure that date column (which originally conatins integers, 
   but have to be converted to date) donot have any zeros or -ve numbers */

SELECT
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
	WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;
-- Combine these 2 queries using the OR statements

-- Dates should be between the current date and when your business started
SELECT
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
	WHERE sls_order_dt NOT BETWEEN 19900612 AND 20261015;

SELECT
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
	WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8;

SELECT
	NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
	WHERE sls_ship_dt NOT BETWEEN 19900612 AND 20261015;

SELECT
	NULLIF(sls_due_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
	WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

SELECT
	NULLIF(sls_due_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
	WHERE sls_due_dt NOT BETWEEN 19900612 AND 20261015;

-- Order date must be always less than the ship date or the due date (write a check query for that rule)

-- Business rule: Sales = Quantity * Price
-- Check Data Consistency: Between Sales, Quantity and Price
-- >> Values must not be NULL, Zero or Negative

SELECT DISTINCT -- DISTINCT is used to explore the variety without redundancy
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
	WHERE sls_sales != sls_quantity * sls_price
	   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;


-- To find any missing information and to make sure our transformation has worked proeprly
SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid-- We removes the extra substring that would have prevented us from JOINing this table with crm_cust_info
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cust_az12
	WHERE (CASE
		   WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	       ELSE cid
	       END) NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- We have to make sure that the dates are in a certain range
SELECT 
	bdate
FROM bronze.erp_cust_az12
	WHERE bdate < '1926-01-01' OR bdate > GETDATE()
ORDER BY bdate;

