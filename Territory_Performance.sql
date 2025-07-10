-- Project: Territory Efficiency and Potential Analysis
-- Tool: MySQL
-- Author: Rohith
-- Description: This SQL script prepares, cleans, and analyzes territory-level customer data to support MCDA and BI reporting.

-- Step 1: Create staging table for cleaning
CREATE TABLE sales_staging LIKE sales;
INSERT INTO sales_staging SELECT * FROM sales;

-- Step 2: Identify and remove exact duplicates
WITH duplicate_cte AS (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY customers, town, postcode_sector, `Business Type`, territory, `Active Customer`, last_booking_time, last_contact_time, `Revenue (Last 5 years)`, `Revenue (Last year)`) AS row_num
  FROM sales_staging
)
DELETE FROM sales_staging
WHERE CONCAT(customers, town, postcode_sector, `Business Type`, territory, `Active Customer`, last_booking_time, last_contact_time, `Revenue (Last 5 years)`, `Revenue (Last year)`) IN (
  SELECT CONCAT(customers, town, postcode_sector, `Business Type`, territory, `Active Customer`, last_booking_time, last_contact_time, `Revenue (Last 5 years)`, `Revenue (Last year)`)
  FROM duplicate_cte WHERE row_num > 1
);

-- Step 3: Standardize town names (sample examples)
UPDATE sales_staging SET town = 'Colchester' WHERE town LIKE 'Colc%';
UPDATE sales_staging SET town = 'Chelmsford' WHERE town LIKE 'Chelm%';
UPDATE sales_staging SET town = 'Great Dunmow' WHERE town IN ('Gt Dunmow', 'Gt DUNMOW');
-- (Repeat as needed for remaining towns)

-- Step 4: Filter incorrect records
DELETE FROM sales_staging
WHERE `Active Customer` = 'No' AND `Revenue (Last 5 years)` > 0;

-- Step 5: Create staging2 with row numbers for deduplication and exploration
CREATE TABLE sales_staging2 AS
SELECT *,
  ROW_NUMBER() OVER(
    PARTITION BY customers, town, postcode_sector, `Business Type`, territory, `Active Customer`, last_booking_time, last_contact_time, `Revenue (Last 5 years)`, `Revenue (Last year)`
  ) AS row_num
FROM sales_staging;

-- Step 6: Repeated customers by territory
WITH repeated_customers AS (
  SELECT territory, customers
  FROM sales_staging2
  GROUP BY territory, customers
  HAVING COUNT(*) > 1
),
total_customers AS (
  SELECT territory, COUNT(DISTINCT customers) AS total_customers
  FROM sales_staging2
  GROUP BY territory
)
SELECT
  tc.territory,
  COUNT(DISTINCT rc.customers) AS repeated_customers,
  tc.total_customers,
  ROUND(COUNT(DISTINCT rc.customers) * 100.0 / tc.total_customers, 2) AS repeated_customer_percent
FROM total_customers tc
LEFT JOIN repeated_customers rc ON tc.territory = rc.territory
GROUP BY tc.territory, tc.total_customers
ORDER BY repeated_customer_percent DESC;

-- Step 7: Total revenue (last 5 years and last year) per territory
SELECT
  territory,
  SUM(`Revenue (Last 5 years)`) AS total_revenue_5y,
  SUM(`Revenue (Last year)`) AS total_revenue_1y
FROM sales_staging2
GROUP BY territory
ORDER BY total_revenue_5y DESC;

-- Step 8: Total bookings from repeated customers
WITH repeated_customers AS (
  SELECT territory, customers
  FROM sales_staging2
  GROUP BY territory, customers
  HAVING COUNT(*) > 1
)
SELECT
  s.territory,
  COUNT(*) AS repeated_customer_bookings
FROM sales_staging2 s
JOIN repeated_customers r ON s.territory = r.territory AND s.customers = r.customers
WHERE s.last_booking_time IS NOT NULL
GROUP BY s.territory
ORDER BY repeated_customer_bookings DESC;

-- Step 9: Average days since last contact by territory
SELECT
  territory,
  AVG(DATEDIFF(CURDATE(), STR_TO_DATE(last_contact_time, '%d-%m-%Y'))) AS avg_days_since_last_contact
FROM sales_staging2
WHERE last_contact_time IS NOT NULL
GROUP BY territory
ORDER BY avg_days_since_last_contact ASC;