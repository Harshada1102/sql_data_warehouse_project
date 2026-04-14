/*

DDL Script: Cretes Gold Views

Script creates view for the gold layer in the data warehouse.
Gold layer represents the final dimension and fact tables (Star schema)

Data is combined from silver layer

Usage: Views can be queried directly for analytics and reporting

*/


CREATE VIEW gold.dim_customers AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY cust_id) AS customer_key,
	ci.cust_id AS customer_id,
	ci.cust_key AS customer_number,
	ci.cust_firstname AS first_name,
	ci.cust_lastname AS last_name,
	loc.CNTRY AS country,
	ci.cust_marital_status AS marital_status,
	CASE WHEN ci.cust_gndr != 'n/a' THEN ci.cust_gndr 
	ELSE COALESCE(bd.GEN, 'n/a')
	END AS gender,
    bd.BDATE AS birth_date,
	ci.cust_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_AZ12 bd
ON ci.cust_key = bd.CID
LEFT JOIN silver.erp_LOC_A101 loc
ON ci.cust_key =  loc.CID


CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prod_start_dt, pn.prod_key) AS product_key,
	pn.prod_id AS product_id,
	pn.prod_key AS product_number,
	pn.prod_nm AS category_name,
	pn.cat_id AS category_id,
	pc.CAT AS category,
	pc.SUBCAT AS subcategory,
	pc.MAINTENANCE as maintenance,
	pn.prod_cost AS cost,
	pn.prod_line AS product_line,
	pn.prod_start_dt AS start_date
FROM 
silver.crm_prod_info pn
LEFT JOIN silver.erp_PX_CAT_G1V2 pc
ON pn.cat_id = pc.ID
WHERE prod_end_dt IS NULL


CREATE VIEW gold.fact_sales AS 
SELECT 
	sc.sls_ord_num AS order_number,
	gp.product_key,
	cu.customer_key,
	sc.sls_order_dt AS order_date,
	sc.sls_ship_dt AS shipping_date,
	sc.sls_due_dt AS due_date,
	sc.sls_sales AS sales_amount,
	sc.sls_quantity AS quantity,
	sc.sls_price AS price
FROM silver.crm_sls_info sc
LEFT JOIN gold.dim_products gp
ON sc.sls_prod_key = gp.product_number
LEFT JOIN gold.dim_customers cu
ON sc.sls_cust_id = cu.customer_id
