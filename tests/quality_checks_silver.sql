/*
This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schema
It includes checks for:
- Null or duplicate primary keys.
- Unwanted spcaes in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields

*/

SELECT 
COUNT(*)
FROM silver.crm_prod_info 
GROUP BY prod_id
HAVING COUNT(*) >1 or prod_id IS NULL

--Check for unwanted spaces, Expectation : No Results
SELECT
prod_nm
FROM silver.crm_prod_info
WHERE prod_nm != TRIM(prod_nm)

--Check for NULLS or Negative numbers, Expectation : No Results
SELECT
prod_cost
FROM silver.crm_prod_info
WHERE prod_cost < 0 or prod_cost IS NULL

--Data Standardization & consistency
SELECT DISTINCT prod_line 
FROM silver.crm_prod_info

--Check for invalid date orders

SELECT *
FROM silver.crm_prod_info 
WHERE prod_end_dt < prod_start_dt

USE DatawareHouse
--Check for invalid dates
SELECT
NULLIF(sls_due_dt,0) sls_due_dt
FROM silver.crm_sls_info
WHERE sls_due_dt <=0
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

--Check Invalid Date Orders
SELECT sls_order_dt, sls_due_dt
FROM silver.crm_sls_info
WHERE sls_order_dt >sls_ship_dt 
OR sls_order_dt > sls_due_dt 

--Check data consistency
--Sales is null,zero or negative derive it using quantity and price
--Price is zero or null calculate it using Sales and Quantity 
--Price is negative , convert it into positive value

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sls_info
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0

SELECT DISTINCT GEN 
FROM silver.erp_cust_AZ12

SELECT BDATE
FROM silver.erp_cust_AZ12 
WHERE BDATE < '1924-01-01' OR BDATE > getdate()
