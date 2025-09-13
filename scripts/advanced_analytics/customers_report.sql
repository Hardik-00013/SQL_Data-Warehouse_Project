/*
=====================================================
Customer Report
=====================================================
Purpose:
- This report consolidates key customer metrics and behaviors

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value
		- average monthly spend
=====================================================
*/

-- This view can now be used for further reporting and analysis

CREATE VIEW gold.report_customers AS
	-- =================
    -- CTE 1 Starts Here
    -- =================
	WITH major_customer_information AS
	(
		SELECT
			*,
			CASE
				WHEN customer_lifespan >= 12 AND total_lifetime_sales > 5000 THEN 'VIP'
				WHEN customer_lifespan >= 12 AND total_lifetime_sales <= 5000 THEN 'Regular'
				ELSE 'Regular'
			END AS customer_range
		FROM
			(SELECT
				gdc.customer_key,
				DATEDIFF(YEAR, gdc.birthdate, '2012-01-28') AS Age_In_Years,
				MIN(gfs.order_date) AS date_of_first_order,
				MAX(gfs.order_date) AS date_of_last_order,
				DATEDIFF(MONTH, MIN(gfs.order_date), MAX(gfs.order_date)) AS customer_lifespan,
				DATEDIFF(MONTH, MAX(gfs.order_date), '2014-01-28') AS recency, 
				COUNT(DISTINCT order_number) AS total_orders,
				SUM(sales_amount) AS total_lifetime_sales,
				SUM(quantity) AS total_quantity,
				COUNT(DISTINCT product_key) AS total_products
			FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_customers AS gdc
				ON gfs.customer_key = gdc.customer_key
			GROUP BY gdc.customer_key, DATEDIFF(YEAR, gdc.birthdate, '2012-01-28')) AS t
	),
	-- =================
	-- CTE 2 Starts Here
	-- =================
	 avg_order_value AS 
	(
		SELECT
			customer_key,
			SUM(order_value)/(COUNT(customer_key)) AS average_order_value
		FROM
			(SELECT
				gdc.customer_key,
				SUM(gfs.sales_amount) AS order_value
			FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_customers AS gdc
				ON gfs.customer_key = gdc.customer_key
			GROUP BY gdc.customer_key, gfs.order_number) AS t
		GROUP BY customer_key
	),
	-- =================
	-- CTE 3 Starts Here
	-- =================
	/* We are calculating this by considering the time between
	   and first purchase month and last month till which the business ran
	   and the total sales amount of the customer till then */
	 avg_monthly_spend AS
	 (
		SELECT
			gdc.customer_key,
			ROUND(CAST(SUM(gfs.sales_amount) AS FLOAT)/NULLIF(DATEDIFF(MONTH, MIN(gfs.order_date), '2014-01-28'), 0), 2) 
			AS average_monthly_spend
		FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_customers AS gdc
			ON gfs.customer_key = gdc.customer_key
		GROUP BY gdc.customer_key
	)

	SELECT 
		c.customer_key,
		c.customer_id,
		CONCAT(c.fisrt_name, ' ', c.last_name) AS customer_full_name,
		Age_In_Years,
		CASE
			WHEN Age_In_Years < 20 THEN 'Below 20'
			WHEN Age_In_Years BETWEEN 20 AND 29 THEN '20-29'
			WHEN Age_In_Years BETWEEN 30 AND 39 THEN '30-39'
			WHEN Age_In_Years BETWEEN 40 AND 49 THEN '40-49'
			ELSE '50 and Above'
		END AS age_segment,
		mci.total_orders,
		mci.date_of_last_order,
		mci.customer_lifespan,
		mci.recency,
		mci.total_lifetime_sales,
		mci.total_quantity,
		mci.total_products,
		mci.customer_range,
		aov.average_order_value,
		ams.average_monthly_spend
	FROM gold.dim_customers AS c LEFT JOIN major_customer_information AS mci
		ON c.customer_key = mci.customer_key
	LEFT JOIN avg_order_value AS aov
		ON c.customer_key = aov.customer_key
	LEFT JOIN avg_monthly_spend AS ams
		ON c.customer_key = ams.customer_key;



