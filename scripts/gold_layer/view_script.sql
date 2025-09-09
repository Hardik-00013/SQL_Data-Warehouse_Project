-- ================================
-- Creating the dim_customers view
-- ================================

-- Renaming columns to nice friendly names
-- Using snake_case here
CREATE VIEW gold.dim_customers AS
	SELECT
		-- We are going to use this key to connect to the data model
		ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS fisrt_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,
		ci.cst_material_status AS marital_status,
		CASE -- This process is what is known as data integration
			WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- CRM is the master for gender info
			ELSE COALESCE(ca.gen, 'N/A')
		END AS gender,
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date
	FROM silver.crm_cust_info AS ci 
		LEFT JOIN silver.erp_cust_az12 AS ca	
	ON ci.cst_key = ca.cid
		LEFT JOIN silver .erp_loc_a101 AS la
	ON ci.cst_key = la.cid;

-- ===============================
-- Creating the dim_products view
-- ===============================
-- Or Just 'prd_end_dt IS NULL' would have made the job much easier
CREATE VIEW gold.dim_products AS 
	SELECT 
		ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
		pn.prd_id AS product_id,
		pn.prd_key AS product_number,
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date
	FROM
		(SELECT
			*,
			ROW_NUMBER() OVER(PARTITION BY cat_id, prd_key ORDER BY prd_start_dt DESC) AS Start_Date_Rank
		 FROM silver.crm_prd_info) AS pn LEFT JOIN silver.erp_px_cat_g1v2 AS pc
	ON pn.cat_id = pc.id
		WHERE pn.Start_Date_Rank = 1;-- Filter out all the historical data
  
 -- ============================
-- Creating the fact_sales view
-- =============================
/* We want to join the fact and the dim tables in the
   gold tables in the gold layer using surrogate keys */
CREATE VIEW gold.fact_sales AS 
	SELECT
	  sd.sls_ord_num AS order_number,
	  pr.product_key, -- We replaced this column of the fact table with the surrogate key in dim table 
	  cu.customer_key,
	  sd.sls_order_dt AS order_date,
	  sd.sls_ship_dt AS shipping_date,
	  sd.sls_due_dt AS due_date,
	  sd.sls_sales AS sales_amount,
	  sd.sls_quantity AS quantity,
	  sd.sls_price
	FROM silver.crm_sales_details AS sd LEFT JOIN gold.dim_products AS pr
		ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers AS cu
		ON sd.sls_cust_id = cu.customer_id;

