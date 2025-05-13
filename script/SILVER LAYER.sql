-- EXPLORATION 
SELECT [prd_id],  [prd_key], [prd_nm], [prd_cost], [prd_line], [prd_start_dt], [prd_end_dt]
FROM [SILVER].[crm_prd_info];
SELECT [sls_ord_num], [sls_prd_key], [sls_cust_id], [sls_order_dt], [sls_ship_dt], [sls_due_dt], [sls_sales], [sls_quantity], [sls_price]
FROM [SILVER].[crm_sales_details];

SELECT [CID], [BDATE], [GEN]
FROM [SILVER].[erp_cust_AZ12];
SELECT [CID], [CNTRY]
FROM [SILVER].[erp_loc_A101];
SELECT [ID], [CAT], [SUBCAT],[MAINTENANCE]
FROM [SILVER].[erp_px_G1V2];

-- CREATING SCHEMA
CREATE SCHEMA SILVER;
GO

-- SILVER LAYER TABLE CREATION 
USE DataWarehouse;
IF OBJECT_ID('crm_cust_info' , 'U') is not null
	DROP TABLE SILVER.crm_cust_info;
CREATE TABLE SILVER.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50), 
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	-- ADDED TIME STAMP
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);
IF OBJECT_ID('crm_prd_info', 'U') is not null
	DROP TABLE SILVER.crm_prd_info;
CREATE TABLE SILVER.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost NVARCHAR(50), 
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('crm_sales_details', 'U') is not null
	DROP TABLE SILVER.crm_prd_info;
CREATE TABLE SILVER.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('erp_cust_AZ12', 'U') is not null
	DROP TABLE SILVER.erp_cust_AZ12;
CREATE TABLE SILVER.erp_cust_AZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50),
dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('erp_loc_A101', 'U') is not null
	DROP TABLE SILVER.erp_loc_A101;
CREATE TABLE SILVER.erp_loc_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_time DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('erp_px_G1V2', 'U') is not null
	DROP TABLE SILVER.erp_px_G1V2;
CREATE TABLE SILVER.erp_px_G1V2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_time DATETIME2 DEFAULT GETDATE()
);

/* 
DATA CLEANING HAPPENS HERE,
ONLY CLEAN DATA CAN BE MOVED INTO THE SILVER LAYER
*/
SELECT *
FROM [bronze].[crm_cust_info];

--CHECK FOR NULLS AND DUPLLICATES IN PK
SELECT cst_id
FROM [bronze].[crm_cust_info]
WHERE cst_id IS NULL
--THREE NULL PRESENT
-- CHECK FOR DUPLICATE
WITH NUM AS (
SELECT cst_id, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_id) AS RN
FROM [bronze].[crm_cust_info]
)
SELECT *
 FROM NUM
WHERE RN > 1;
-- REMOVE THE DUPLICATE ROWS
-- THIS IS ALL THE VALUES WITHOUT DUPLICATES
SELECT *
FROM (SELECT cst_id, 
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_id) AS RN 
		FROM  [bronze].[crm_cust_info]) AS SUB
WHERE RN = 1;



---TRIMMING OPERATION 
-- CHECK TRIM
SELECT cst_firstname FROM [bronze].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname FROM [bronze].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_lastname);
SELECT cst_marital_status FROM [bronze].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_marital_status);
SELECT cst_gndr FROM [bronze].[crm_cust_info]
WHERE cst_firstname != TRIM(cst_gndr)


-- CLEANING THE DATA
SELECT [cst_id],[cst_key],
	TRIM([cst_firstname]) AS cst_firstname, 
	TRIM([cst_lastname]) AS cst_lastname, 
	CASE 
		WHEN TRIM([cst_marital_status]) = 'M' THEN 'Married'
		WHEN TRIM([cst_marital_status]) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE 
		WHEN TRIM([cst_gndr]) = 'M' THEN 'Male'
		WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr, [cst_create_date]
FROM (SELECT *, 
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS RN 
		FROM  [bronze].[crm_cust_info]
		) AS SUB
		WHERE RN = 1
ORDER BY cst_id;

TRUNCATE TABLE [SILVER].[crm_cust_info]
INSERT INTO [SILVER].[crm_cust_info] (
	cst_id,
	cst_key ,
	cst_firstname ,
	cst_lastname,
	cst_marital_status, 
	cst_gndr,
	cst_create_date) 
SELECT [cst_id],[cst_key],
	TRIM([cst_firstname]) AS cst_firstname, 
	TRIM([cst_lastname]) AS cst_lastname, 
	CASE 
		WHEN TRIM([cst_marital_status]) = 'M' THEN 'Married'
		WHEN TRIM([cst_marital_status]) = 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	CASE 
		WHEN TRIM([cst_gndr]) = 'M' THEN 'Male'
		WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr, [cst_create_date]
FROM (SELECT *, 
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS RN 
		FROM  [bronze].[crm_cust_info]
		) AS SQ
		WHERE RN = 1

SELECT * FROM [SILVER].[crm_cust_info]

-- CCLEANING prd_info TABLE
SELECT [prd_id],  [prd_key], [prd_nm], [prd_cost], [prd_line], [prd_start_dt], [prd_end_dt]
FROM [BRONZE].[crm_prd_info];
-- CHECKING DUPLICATE
SELECT * 
	FROM (SELECT prd_id, ROW_NUMBER() OVER(PARTITION BY prd_id ORDER BY prd_id) AS RN FROM [bronze].[crm_prd_info]) AS SUB_2
	WHERE RN > 1;
--
SELECT 
	[prd_id],  
	[prd_key],
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id, 
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	[prd_nm], ISNULL([prd_cost], 0) AS prd_cost, 
	CASE [prd_line]
		WHEN 'R' THEN 'ROAD'
		WHEN 'M' THEN 'MOUNTAIN'
		WHEN 'S' THEN 'STREET'
		WHEN 'T' THEN 'TOURING'
		ELSE 'n/a'
		END AS prd_line,
	[prd_start_dt], 
	CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS adjusted_end_date
FROM [BRONZE].[crm_prd_info];

SELECT * FROM [bronze].[crm_prd_info]
WHERE prd_end_dt < prd_start_dt AND NOT prd_end_dt IS NULL