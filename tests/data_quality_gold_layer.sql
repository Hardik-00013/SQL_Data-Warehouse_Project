-- Check if any duplicates were introduced by the JOIN logic
SELECT 
	cst_id, 
	COUNT(*) AS Duplicate_Count
FROM 
	(SELECT
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_material_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM silver.crm_cust_info AS ci 
		LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
		LEFT JOIN silver .erp_loc_a101 AS la
	ON ci.cst_key = la.cid) t
GROUP BY cst_id 
	HAVING COUNT(*) > 1;

-- Query to check if there are any discrepencies caused by the JOIN logic
SELECT 
	prd_key, 
	COUNT(*)
FROM 
	(SELECT 
	  pn.prd_id,
	  pn.cat_id,
	  pn.prd_key,
	  pn.prd_nm,
	  pn.prd_cost,
	  pn.prd_line,
	  pn.prd_start_dt,
	  pn.prd_end_dt,
	  pc.cat,
	  pc.subcat,
	  pc.maintenance
	FROM
		(SELECT 
		  *,
		  ROW_NUMBER() OVER(PARTITION BY cat_id, prd_key ORDER BY prd_start_dt DESC) AS Start_Date_Rank
		FROM silver.crm_prd_info) AS pn LEFT JOIN silver.erp_px_cat_g1v2 AS pc
	ON pn.cat_id = pc.id
		WHERE pn.Start_Date_Rank = 1) t
GROUP BY prd_key
	HAVING COUNT(*) > 1;

-- Data Integration 
SELECT DISTINCT -- It will give us various combinations of possible values, so that we can handle them
	ci.cst_gndr,
	ca.gen,
	CASE -- This process is what is known as data integration
		WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- CRM is the master for gender info
		ELSE COALESCE(ca.gen, 'N/A')
	END AS new_gen
FROM silver.crm_cust_info AS ci 
	LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
	LEFT JOIN silver .erp_loc_a101 AS la
ON ci.cst_key = la.cid;

-- Finding out how to partition the products
SELECT DISTINCT
	cat_id,
	COUNT(*)
FROM silver.crm_prd_info pn
	GROUP BY cat_id, prd_key;

-- Foreign Key Integrity (Dimensions)
-- You can run these queries seperately aswell
SELECT
  *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
  ON f.product_key = p.product_key
WHERE
  c.customer_key IS NULL OR p.product_key IS NULL;
