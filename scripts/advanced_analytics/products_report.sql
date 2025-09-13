/*
=====================================================
Product Report
=====================================================
Purpose:
- This report consolidates key product metrics and behaviors.

Highlights:
1. Gathers essential fields such as product name, category, subcategory, and cost.
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
3. Aggregates product-level metrics:
- total orders
- total sales
- total quantity sold
- total customers (unique)
- lifespan (in months)
4. Calculates valuable KPIs:
- recency (months since last sale)
- average order revenue (AOR)
- average monthly revenue
=====================================================
*/
CREATE VIEW gold.report_products AS

	SELECT
		gdp.product_name,
		gdp.category,
		gdp.subcategory,
		AVG(gdp.cost) AS average_cost,
		SUM(gfs.sales_amount) AS total_sales,
		CASE
			WHEN SUM(gfs.sales_amount) > 50000 THEN 'High Performer'
			WHEN SUM(gfs.sales_amount) > 10000 THEN 'Mid Performer'
			ELSE 'Low Performer'
		END AS product_segment,
		SUM(gfs.quantity) AS total_quantity,
		COUNT(DISTINCT gfs.customer_key) AS total_customers,
		MAX(gfs.order_date) AS last_sales_date,
		DATEDIFF(MONTH, MIN(gfs.order_date), '2014-01-28') AS lifespan,
		DATEDIFF(MONTH, MAX(gfs.order_date), '2014-01-28') AS recency,
		ROUND(CAST(SUM(sales_amount) AS FLOAT)/SUM(gfs.quantity), 2) AS average_selling_price,
		ROUND(CAST(SUM(gfs.sales_amount) AS FLOAT)/COUNT(DISTINCT order_number), 2) AS average_order_revenue,
		ROUND(CAST(SUM(gfs.sales_amount) AS FLOAT)/NULLIF(DATEDIFF(MONTH, MIN(order_date), '2014-01-28'), 0), 2) AS average_monthly_revenue
	FROM gold.fact_sales AS gfs LEFT JOIN  gold.dim_products AS gdp
		ON gfs.product_key = gdp.product_key
	WHERE gfs.order_date IS NOT NULL
		GROUP BY gdp.product_name, gdp.category, gdp.subcategory;

