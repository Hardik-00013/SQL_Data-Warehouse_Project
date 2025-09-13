-- CUMULATIVE ANALYSIS
/* Calculate the total sales per month
   and the running total of sales over time */

SELECT
	*,
	SUM(Total_Sales) OVER(ORDER BY Month_Year_Combo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Running_Total,
	-- Can also create running total for each year and running total across years
	SUM(Total_Sales) 
		OVER(PARTITION BY Month_Year_Combo ORDER BY Month_Year_Combo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
	AS Yearly_Running_Total,
	CASE 
		WHEN MONTH(Month_Year_Combo) = 1 THEN 'YRT_Satrts_Here'
		ELSE '' END AS Check_Point
FROM
	(SELECT
		DATETRUNC(MONTH,order_date) AS Month_Year_Combo,
		SUM(sales_amount) AS Total_Sales
	FROM gold.fact_sales
		WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH, order_date)) AS t;

-- PERFORMANCE ANALYSIS
/* Analyze the yearly performance of products by comparing
   each product's sales  to both it average sales 
   performance and the previous year's sales */
-- Carrification: We are averaging sales_amount for a specific product across all years
WITH yearly_product_sales AS (
	SELECT 
		-- We can similarly perform a month-over-month analysis here
		YEAR(gfs.order_date) AS Order_Year,
		gdp.product_name AS Product_Name,
		SUM(gfs.sales_amount) AS Current_Sales
	FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_products AS gdp
		ON gfs.product_key = gdp.product_key
	WHERE order_date IS NOT NULL
		GROUP BY YEAR(gfs.order_date), gdp.product_name
)
SELECT 
	Order_Year,
	Product_Name,
	Current_Sales, 
	AVG(Current_Sales) OVER (PARTITION BY Product_Name) AS Avg_Sales,
	Current_Sales - AVG(Current_Sales) OVER (PARTITION BY Product_Name) AS Diff_Avg,
	CASE 
		WHEN Current_Sales - AVG(Current_Sales) OVER (PARTITION BY Product_Name) > 0 THEN 'Above Average'
		WHEN Current_Sales - AVG(Current_Sales) OVER (PARTITION BY Product_Name) < 0 THEN 'Below Average'
	    ELSE 'Average'
	END AS Change_Description,
	-- Year Over Year Analysis
	LAG(Current_Sales) OVER (PARTITION BY Product_Name ORDER BY Order_Year) AS Previous_Year_Sales,
	Current_Sales - LAG(Current_Sales) OVER (PARTITION BY Product_Name ORDER BY Order_Year) AS diff_py,
	CASE 
		WHEN Current_Sales - LAG(Current_Sales) OVER (PARTITION BY Product_Name ORDER BY Order_Year) > 0 THEN 'Sales Increase'
		WHEN Current_Sales - LAG(Current_Sales) OVER (PARTITION BY Product_Name ORDER BY Order_Year) < 0 THEN 'Sales Decrease'
	    ELSE 'No Growth'
	END AS Sales_Growth_Trend
FROM yearly_product_sales
	ORDER BY Product_Name, Order_Year;

-- PART-TO-WHOLE ANALYSIS (Proportional Analysis)
-- Which category contributes the most to overall sales?
-- We can also do the ranking to identify the highest contributing category if the categories are large in number
SELECT
	*,
	SUM(Total_Sales) OVER (ORDER BY  Total_Sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
	AS Total_Sales_Across_Categories, -- Total Sales Across All Categories
	ROUND((CAST(Total_Sales AS FLOAT)/SUM(Total_Sales) OVER (ORDER BY  Total_Sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) 
	* 100, 2) AS Category_Share_Of_Total
FROM 
	(SELECT
		gdp.category,
		SUM(sales_amount) AS Total_Sales
	FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_products AS gdp
		ON gfs.product_key = gdp.product_key
	GROUP BY gdp.category) AS t
ORDER BY Category_Share_Of_Total DESC;

-- OR we can also do it with the help of a CTE
/* Insight: The business is relying heavily on a single category,
   which is not healthy as a loss of market share in that specific
   category will lead to a proportional decline in the health of the business.
   Hence it makes sense to strategize on how to increase in revenue from other
   sources, and diversify if necessary */

WITH category_sales AS
(
	SELECT
		gdp.category,
		SUM(sales_amount) AS Total_Sales
	FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_products AS gdp
		ON gfs.product_key = gdp.product_key
	GROUP BY gdp.category
)
SELECT
	*,
	SUM(Total_Sales) OVER (ORDER BY Total_Sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
	AS Total_Sales_Across_Categories,
	ROUND((CAST(Total_Sales AS FLOAT)/SUM(Total_Sales) OVER (ORDER BY Total_Sales ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
	* 100, 2) AS Category_Share_Of_Total
FROM category_sales
	ORDER BY Category_Share_Of_Total DESC;


-- DATA SEGMENTATION
/* Segment products into cost ranges and count
   how many products fall into each segment */

-- First try to understand what are the range of prices
SELECT
	MAX(cost) AS Maximun_Product_Cost,
	MIN(cost) AS Minimun_Product_Cost
FROM gold.dim_products;

-- Creating categories (or defining ranges)
-- We can also use a CTE here
SELECT
	Cost_Range,
	COUNT(product_key) AS Total_Products_In_Range
FROM 
	(SELECT
		*,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END AS Cost_Range
	FROM gold.dim_products) AS t
GROUP BY Cost_Range
	ORDER BY Total_Products_In_Range DESC;
   
/*  Group customers into three segments based on their spending behavior:
		- VIP: Customers with at least 12 months of history and spending more than €5,000.
		- Regular: Customers with at least 12 months of history but spending €5,000 or less.
		- New: Customers with a lifespan less than 12 months.
	And find the total number of customers by each group */
WITH customer_information AS 
(   -- First Intermediate Result
	SELECT
		gdc.customer_key,
		CONCAT(gdc.fisrt_name, ' ', gdc.last_name) AS Customer_Full_Name,
		SUM(gfs.sales_amount) AS Purchase_Till_Date,
		MIN(gfs.order_date) AS First_Order_Date,
		MAX(gfs.order_date) AS Last_Order_Date
	FROM gold.fact_sales AS gfs LEFT JOIN gold.dim_customers AS gdc
		ON gfs.customer_key = gdc.customer_key
	GROUP BY gdc.customer_key, CONCAT(gdc.fisrt_name, ' ', gdc.last_name)
)
SELECT
	*,
	COUNT(customer_key) OVER (PARTITION BY Customer_Spending_Behaviour) AS Num_Cust_Each_Category
FROM 
	(SELECT -- Second Intermendiate Result
		*,
		CASE 
			WHEN Purchase_Till_Date > 5000 AND DATEDIFF(MONTH, First_Order_Date, Last_Order_Date) >= 12 THEN 'VIP'
			WHEN Purchase_Till_Date <= 5000 AND DATEDIFF(MONTH, First_Order_Date, Last_Order_Date) >= 12 THEN 'Regular'
			ELSE 'New'
		END AS Customer_Spending_Behaviour
	FROM customer_information) t 
ORDER BY Customer_Spending_Behaviour DESC, Purchase_Till_Date DESC, customer_key;




