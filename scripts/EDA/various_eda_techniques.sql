-- STEP 1: DATABASE EXPLORATION
-- Explore all the objects in the database
SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- Explore all the columns in the database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = 'dim_customers'; -- See columns for a speciifc table


-- STEP 2: DIMENSION EXPLORATION
-- Explore all the countries our customers come from
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore all the product categories 'The Major Divisions'
SELECT DISTINCT category FROM gold.dim_products;
SELECT DISTINCT 
	category, 
	subcategory, 
	product_name
FROM gold.dim_products
	ORDER BY 1,2,3;-- Detailing even further (adding extra layers of details)

-- STEP 3: DATE EXPLORATION
-- Find the date of the first and the last order, how many years of sales are available
SELECT
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS years_of_sale,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS months_of_sale
FROM gold.fact_sales;

-- Find the oldest and the youngest customer
SELECT
	MIN(birthdate) AS oldest_birthdate,
	MIN(DATEDIFF(YEAR, birthdate, GETDATE())) AS youngest_customer_age,
	MAX(birthdate) AS youngest_birthdate,
	MAX(DATEDIFF(YEAR, birthdate, GETDATE())) AS oldest_customer_age
FROM gold.dim_customers;

-- STEP 4: MEASURE EXPLORATION
-- Find the Total Sales
SELECT 
	SUM(sales_amount) AS Total_Sales
FROM gold.fact_sales;

-- Find how many items are sold
SELECT
	SUM(quantity) AS Total_Items_Sold
FROM gold.fact_sales;

-- Find the average selling price
SELECT
	AVG(sls_price) AS Average_Selling_Price
FROM gold.fact_sales;

-- Find the Total number of Orders
SELECT
	COUNT(order_number) AS Total_Orders_With_Duplicates,
	COUNT(DISTINCT order_number) AS Total_Orders -- We don't want to count the same order twice
FROM gold.fact_sales;

-- Find the total number of products
SELECT
	COUNT(product_key) AS Total_Products,
	COUNT(DISTINCT product_key) AS Total_Products_Dup_Check -- To make sure there are no duplicates
FROM gold.dim_products;

-- Find the total number of customers
SELECT
	COUNT(customer_key) AS Total_Customers,
	COUNT(DISTINCT customer_key) AS Total_Customers_Dup_Check -- To make sure there are no duplicates
FROM gold.dim_customers;

-- Find the total number of customers that have placed an order
-- Finding: All our registered customers have placed an order
SELECT
	COUNT(DISTINCT gdc.customer_key) AS Total_Customers_Who_Placed_Order
FROM gold.dim_customers AS gdc INNER JOIN gold.fact_sales AS gfs
	ON gdc.customer_key = gfs.customer_key;
-- OR
SELECT
	COUNT(DISTINCT customer_key) AS total_customers 
FROM gold.fact_sales;


-- TASK: Generate a report that shows all key metrics of the business
-- Generating an EDA report, for 'Measure Exploration' (for numbers whose aggregations make sense)
SELECT 
	'Total Sales' AS [Measure Name], 
	SUM(sales_amount) AS [Measure Value] 
FROM gold.fact_sales 
	UNION ALL
SELECT 
	'Total Number Of Items Sold', 
	SUM(quantity) 
FROM gold.fact_sales
	UNION ALL
SELECT 
	'Average Selling Price', 
	AVG(sls_price) 
FROM gold.fact_sales
	UNION ALL
SELECT 
	'Total Number Of Orders',
	COUNT(order_number)
FROM gold.fact_sales
	UNION ALL
SELECT
	'Total Orders Without Duplicates',
	COUNT(DISTINCT order_number)
FROM gold.fact_sales
	UNION ALL
SELECT
	'Total Products',
	COUNT(product_key)
FROM gold.dim_products
	UNION ALL
SELECT -- This is just to make sure there are no duplicates
	'Total Products Distinct',
	COUNT(DISTINCT product_key)
FROM gold.dim_products
	UNION ALL
SELECT
	'Total Number Of Customers',
	COUNT(customer_key)
FROM gold.dim_customers
	UNION ALL
SELECT -- This is just to make sure there are no duplicates
	'Total Number Of Distinct Customers',
	COUNT(DISTINCT customer_key)
FROM gold.dim_customers
	UNION ALL
SELECT
	'Total Customers Who Placed Order',
	COUNT(DISTINCT customer_key)
FROM gold.fact_sales;


-- STEP 5: Magnitude Analysis
-- Report 1: Find total customers by countries
SELECT
	country,
	COUNT(customer_key) AS Total_Customers
FROM gold.dim_customers
	GROUP BY country
ORDER BY Total_Customers DESC;

-- Report 2: Find total customers by gender
SELECT
	gender,
	COUNT(*) AS Total_Customers
FROM gold.dim_customers
	GROUP BY gender
ORDER BY Total_Customers DESC;

-- Report 3: Find total products by category
SELECT
	category,
	COUNT(product_key) AS Total_Products
FROM gold.dim_products
	GROUP BY category
ORDER BY Total_Products DESC;

-- Report 4: What is the average costs in each category?
SELECT
	category,
	AVG(cost) AS Average_Cost
FROM gold.dim_products
	GROUP BY category
ORDER BY Average_Cost DESC;

-- Report 5: What is the total revenue generated for each category?
-- Insight: Our business is making alot of money selling Bikes
SELECT
	gdp.category,
	SUM(gfs.sales_amount) AS Total_Revenue_Generated
FROM gold.dim_products AS gdp LEFT JOIN gold.fact_sales AS gfs
	ON gdp.product_key = gfs.product_key
GROUP BY gdp.category
	ORDER BY Total_Revenue_Generated DESC;
-- The below query becomes an important verification for the above query
SELECT 
	* 
FROM gold.fact_sales gfs INNER JOIN gold.dim_products gdp 
	ON gfs.product_key = gdp.product_key
WHERE category = 'Components';

-- Report 6: Find total revenue generated by each customer
SELECT
	gdc.customer_key,
	CONCAT(gdc.fisrt_name, gdc.last_name) AS Customer_Name,
	SUM(gfs.sales_amount) AS Revenue_By_Customer
FROM gold.dim_customers AS gdc LEFT JOIN gold.fact_sales AS gfs
	ON gdc.customer_key = gfs.customer_key
GROUP BY gdc.customer_key, CONCAT(gdc.fisrt_name, gdc.last_name)
	ORDER BY Revenue_By_Customer;


-- Report 7: What is the distribution of sold items across countries?
-- Basically finding the total quantity by country
SELECT
	gdc.country,
	SUM(gfs.quantity) AS Number_Of_Items_Sold
FROM gold.dim_customers AS gdc LEFT JOIN gold.fact_sales AS gfs
	ON gdc.customer_key = gfs.customer_key
GROUP BY gdc.country
	ORDER BY Number_Of_Items_Sold
OPTION (HASH JOIN);

-- STEP 6: Ranking Analysis
-- Report 1: Which 5 products generate the highest revenue?
-- Can also do it for the best performing subcategories
SELECT TOP 5
	gdp.product_name,
	SUM(gfs.sales_amount) AS Revenue_Generated
FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_products AS gdp
	ON gfs.product_key = gdp.product_key
GROUP BY gdp.product_name
	ORDER BY Revenue_Generated DESC;
-- OR
SELECT
	* 
FROM 
	(SELECT TOP 5
		gdp.product_name,
		SUM(gfs.sales_amount) AS Revenue_Generated,
		ROW_NUMBER() OVER(ORDER BY SUM(gfs.sales_amount) DESC) AS Product_Ranking
	FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_products AS gdp
		ON gfs.product_key = gdp.product_key
	GROUP BY gdp.product_name) t
WHERE Product_Ranking <= 5;

-- Report 2: What are the 5 worst-performing products in terms of sales?
SELECT TOP 5
	gdp.product_name,
	SUM(gfs.sales_amount) AS Revenue_Generated
FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_products AS gdp
	ON gfs.product_key = gdp.product_key
GROUP BY gdp.product_name
	ORDER BY Revenue_Generated;

-- Report 3: Find the top 10 customers who have generated the highest revenue
SELECT TOP 10
	gdc.customer_key,
	SUM(sales_amount) AS Revenue_Generated
FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_customers AS gdc
	ON gfs.customer_key = gdc.customer_key
GROUP BY gdc.customer_key
	ORDER BY Revenue_Generated DESC;

-- Report 4: Find 3 customers with the fewest orders placed
SELECT 
	*
FROM 
	(SELECT
		gdc.customer_key,
		COUNT(DISTINCT gfs.order_number) AS Number_Of_Orders,
		ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT gfs.order_number)) AS Order_Based_Ranking
	FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_customers AS gdc
		ON gfs.customer_key = gdc.customer_key
	GROUP BY gdc.customer_key) AS t
WHERE Order_Based_Ranking <= 3;


	










