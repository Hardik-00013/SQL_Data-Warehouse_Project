-- === Report 1 ===
/* Find the favourite product of the high value customers(also the percentage it takes in their total spend) 
   Days since their last order 
   Sort by percentage exceeded from their average_order_duration 
   High Value Customers: who have more than $4000 in combined purchases till date 
   We can further calculate the %age of customers in each customer_health_status category*/ 

-- recognizing the high value customers and days since their last order
WITH high_value_customers AS
	(SELECT 
		customer_key,
		SUM(sales_amount) AS total_customer_sales,
		DATEDIFF(DAY, MAX(order_date), '2014-02-01') AS days_since_last_order
	FROM gold.fact_sales
		GROUP BY customer_key
	HAVING SUM(sales_amount) >= 4000),

-- recognizing the favourite product of the customer
customer_favourite_product AS
	(SELECT 
		customer_key,
		product_key,
		product_sale_percentage
	FROM
		(SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY product_sale_percentage DESC) AS favourite_product_rank
		FROM 
			(SELECT
				gfs.customer_key,
				gfs.product_key,
				ROUND(CAST(SUM(gfs.sales_amount) AS FLOAT)/ hvc.total_customer_sales * 100, 2) AS product_sale_percentage
			FROM gold.fact_sales AS gfs INNER JOIN high_value_customers AS hvc
				ON gfs.customer_key = hvc.customer_key
			GROUP BY gfs.customer_key, gfs.product_key, hvc.total_customer_sales) AS t) AS t1
	WHERE favourite_product_rank = 1),

-- finding the average duration between consecutive orders by the customer
average_order_duration AS 
	(SELECT
		customer_key,
		ROUND(CAST(SUM(COALESCE(day_difference, 0)) AS FLOAT)/NULLIF(COUNT(customer_key) - 1, 0), 2) AS average_order_duration
	FROM 
		(SELECT 
			gfs.customer_key,
			gfs.order_number,
			DATEDIFF(DAY, gfs.order_date, LEAD(gfs.order_date) OVER (PARTITION BY gfs.customer_key ORDER BY gfs.order_date))  AS day_difference
		FROM gold.fact_sales AS gfs INNER JOIN high_value_customers AS hvc
			ON gfs.customer_key = hvc.customer_key
		GROUP BY gfs.order_number, gfs.customer_key, gfs.order_date) AS t
	GROUP BY customer_key)

-- Constructing the main query
-- adding the customer_health_status field and calculating the risk associated with the customer
SELECT 
	*,
	CASE 
		WHEN gone_past_aod BETWEEN -10000 AND 10 THEN 'Active/Loyal'
		WHEN gone_past_aod BETWEEN 11 AND 25 THEN 'Healthy'
		WHEN gone_past_aod BETWEEN 26 AND 60 THEN 'At Risk'
		WHEN gone_past_aod BETWEEN 61 AND 150 THEN 'Lapsed'
		ELSE 'Churned'
	END AS customer_health_status
FROM 
	(SELECT
		hvc.customer_key,
		CONCAT(gdc.fisrt_name, ' ', gdc.last_name) AS customer_name,
		gdp.product_key,
		gdp.product_name AS customer_fav_product,
		hvc.days_since_last_order,
		aod.average_order_duration,
		FLOOR(CAST(hvc.days_since_last_order - aod.average_order_duration AS FLOAT) / aod.average_order_duration * 100)
		AS gone_past_aod
	FROM high_value_customers AS hvc 
		INNER JOIN gold.dim_customers AS gdc ON hvc.customer_key = gdc.customer_key 
		INNER JOIN customer_favourite_product AS cfp ON  cfp.customer_key = hvc.customer_key
		INNER JOIN gold.dim_products AS gdp ON gdp.product_key = cfp.product_key
		INNER JOIN average_order_duration AS aod ON aod.customer_key = hvc.customer_key) AS t
ORDER BY gone_past_aod DESC;
