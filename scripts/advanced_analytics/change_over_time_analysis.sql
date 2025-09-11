-- Analyze sales performance over time
SELECT
	YEAR(order_date) Year,
	SUM(sales_amount) AS Total_Sales,
	COUNT(DISTINCT customer_key) AS Total_Customers,-- Are we gaining customers over time or losing them
	SUM(quantity) AS Total_Quantity
FROM gold.fact_sales
	WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
	ORDER BY YEAR(order_date);

/* Since 2013 was a really good year for the business,
   and sales drastically dropped in 2014 => check for total time in 2014 till now */
SELECT
	MIN(order_date),
	MAX(order_date),
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)),
	SUM(sales_amount)
FROM gold.fact_sales
	WHERE YEAR(order_date) = 2014;
-- We find out 2014 has sales for the month of January only
-- Lets compare it with sales of the month of January in 2013
SELECT
	MIN(order_date),
	MAX(order_date),
	SUM(sales_amount)
FROM gold.fact_sales
	WHERE YEAR(order_date) = 2013 AND MONTH(order_date) = 1;
-- Insight: We find out that the sales in Jan 2013 are significantly higher than those in Jan 2104

-- Lets generate a report showing the Sales Performance of all years for the month of January
SELECT
	YEAR(order_date) AS YEAR,
	SUM(sales_amount) AS Total_January_Sales
FROM gold.fact_sales
	WHERE MONTH(order_date) = 1
GROUP BY YEAR(order_date)
	ORDER BY YEAR(order_date);
-- The sales increased by some percentage in 2012 from 2011 and then almost doubled in January 2013
-- But then plummeted drastically in January 2014, being the lowest of all time

-- Analysis using the DATETRUNC() FUNCTION
SELECT
  DATETRUNC(month, order_date) AS order_date,
  SUM(sales_amount) AS total_sales,
  COUNT(DISTINCT customer_key) AS total_customers,
  SUM(quantity) AS total_quantity
FROM gold.fact_sales
	WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
	ORDER BY DATETRUNC(month, order_date);

-- Lets see how each month of each year is performing is performing
/* Lets also see how the sales have changed for the same month across the year
   and customer growth/decline for the same month across years */
SELECT 
	Year,
	Month_Of_Year,
	Total_Sales,
	Number_Of_Customers,
	-- Analyzing the Sales Growth Trend
	CASE 
		WHEN Last_Year_Same_Month_Sales IS NULL OR Last_Year_Same_Month_Sales = 0
		THEN 'N/A - No Previous Year Data'
		WHEN Total_Sales >= Last_Year_Same_Month_Sales
		THEN 'Increase By: ' + 
		CAST(ROUND((CAST(Total_Sales - Last_Year_Same_Month_Sales AS FLOAT) / CAST(Last_Year_Same_Month_Sales AS FLOAT)) * 100, 2) AS NVARCHAR(50))
		+ ' %'
		ELSE 'Decrease By: ' + 
		CAST(ROUND((CAST(Last_Year_Same_Month_Sales - Total_Sales AS FLOAT) / CAST(Last_Year_Same_Month_Sales AS FLOAT)) * 100, 2) AS NVARCHAR(50))
		+ ' %' 
	END AS Sales_Growth_Trend,
	
	-- Analyzing The Customer Growth Trend
	CASE 
		WHEN Last_Year_Same_Month_Customers IS NULL OR Last_Year_Same_Month_Customers = 0
		THEN 'N/A - No Previous Year Data'
		WHEN Number_Of_Customers >= Last_Year_Same_Month_Customers
		THEN 'Increase By: ' + 
		CAST(ROUND((CAST(Number_Of_Customers - Last_Year_Same_Month_Customers AS FLOAT) / CAST(Last_Year_Same_Month_Customers AS FLOAT)) * 100, 2) AS NVARCHAR(50))
		+ ' %'
		ELSE 'Decrease By: ' + 
		CAST(ROUND((CAST(Last_Year_Same_Month_Customers - Number_Of_Customers AS FLOAT) / CAST(Last_Year_Same_Month_Customers AS FLOAT)) * 100, 2) AS NVARCHAR(50))
		+ ' %' 
	END AS Customer_Growth_Trend
FROM
	(SELECT
		*,
		LAG(Total_Sales) OVER (PARTITION BY Month_Of_Year ORDER BY Year) AS Last_Year_Same_Month_Sales,
		LAG(Number_Of_Customers) OVER (PARTITION BY Month_Of_Year ORDER BY Year) AS Last_Year_Same_Month_Customers
	FROM
		(SELECT
			YEAR(order_date) AS Year,
			MONTH(order_date) AS Month_Of_Year,
			SUM(sales_amount) AS Total_Sales,
			COUNT(DISTINCT customer_key) AS Number_Of_Customers
		FROM gold.fact_sales
		WHERE order_date IS NOT NULL
		GROUP BY MONTH(order_date), YEAR(order_date)) AS t_one) AS t_two;

