/*

Creates tables for bronze schema 

*/


USE DatawareHouse

IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
cust_id INT,
cust_key NVARCHAR(50),
cust_firstname NVARCHAR(25),
cust_lastname NVARCHAR(25),
cust_maritial_status NVARCHAR(25),
cust_gndr NVARCHAR(25),
cust_create_date DATE
);

--cst_id	cst_key	cst_firstname	cst_lastname	cst_marital_status	cst_gndr	cst_create_date
IF OBJECT_ID('bronze.crm_prod_info','U') IS NOT NULL
DROP TABLE bronze.crm_prod_info;

CREATE TABLE bronze.crm_prod_info (
prod_id INT,
prod_key NVARCHAR(50),
prod_nm NVARCHAR(50), --varchar
prod_cost NVARCHAR(50),
prod_line NVARCHAR(50),
prod_start_dt DATE,
prod_end_dt DATE
);

--prd_id	prd_key	prd_nm	prd_cost	prd_line	prd_start_dt	prd_end_dt

IF OBJECT_ID('bronze.crm_sls_info','U') IS NOT NULL
DROP TABLE  bronze.crm_sls_info;

CREATE TABLE bronze.crm_sls_info(
sls_ord_num NVARCHAR(25), --varchar
sls_prod_key NVARCHAR(25),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

--sls_ord_num	sls_prd_key	sls_cust_id	sls_order_dt	sls_ship_dt	sls_due_dt	sls_sales	sls_quantity	sls_price

IF OBJECT_ID('bronze.erp_cust_AZ12','U') IS NOT NULL
DROP TABLE bronze.erp_cust_AZ12;

CREATE TABLE bronze.erp_cust_AZ12(
CID NVARCHAR(25),
BDATE DATE,
GEN NVARCHAR(25)
);

--CID	BDATE	GEN

IF OBJECT_ID('bronze.erp_LOC_A101','U') IS NOT NULL
DROP TABLE bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101(
CID VARCHAR(25),
CNTRY VARCHAR(25)
);

--CID	CNTRY

IF OBJECT_ID('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
DROP TABLE bronze.erp_PX_CAT_G1V2;

CREATE TABLE bronze.erp_PX_CAT_G1V2(
ID NVARCHAR(25),
CAT NVARCHAR(25),
SUBCAT NVARCHAR(25),
MAINTENANCE NVARCHAR(25)
);

--ID	CAT	SUBCAT	MAINTENANCE

SELECT name
FROM sys.tables
WHERE schema_id = SCHEMA_ID('bronze');  --SCHEMA_ID CONVERTS THE SCHEMA NAME INTO ID
