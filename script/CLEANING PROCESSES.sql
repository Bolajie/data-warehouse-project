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
		WHERE RN = 1 AND cst
ORDER BY cst_id;

-- CCLEANING prd_info TABLE
SELECT [prd_id],  [prd_key], [prd_nm], [prd_cost], [prd_line], [prd_start_dt], [prd_end_dt]
FROM [BRONZE].[crm_prd_info];
-- CHECKING DUPLICATE
SELECT * 
	FROM (SELECT prd_id, ROW_NUMBER() OVER(PARTITION BY prd_id ORDER BY prd_id) AS RN FROM [bronze].[crm_prd_info]) AS SUB_2
	WHERE RN > 1;
--
use DataWarehouse;
SELECT 
	[prd_id],  
	[prd_key] AS prd_code,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id, --extract category id
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- extract product id
	[prd_nm], ISNULL([prd_cost], 0) AS prd_cost, 
	CASE [prd_line]
		WHEN 'R' THEN 'ROAD'
		WHEN 'M' THEN 'MOUNTAIN'
		WHEN 'S' THEN 'STREET'
		WHEN 'T' THEN 'TOURING'
		ELSE 'n/a'
		END AS prd_line, -- Data normalization
	[prd_start_dt], 
	CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS adjusted_end_date
FROM [BRONZE].[crm_prd_info];

SELECT * FROM [bronze].[crm_prd_info]

SELECT [sls_ord_num], [sls_prd_key], [sls_cust_id],  
CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) < 8 THEN NULL
	ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE)
	END AS [sls_order_dt],
CASE WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) < 8 THEN NULL
	ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE)
	END AS [sls_ship_dt], 
CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) < 8 THEN NULL
	ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE)
	END AS [sls_due_dt], 
CASE 
	WHEN  [sls_sales] IS NULL 
	OR [sls_sales] <= 0 
	OR [sls_sales] != [sls_quantity] *ABS([sls_price]) 
	THEN  [sls_quantity] * [sls_price]
	ELSE [sls_sales]
END AS [sls_sales], 
[sls_quantity],
CASE
	WHEN [sls_price] IS NULL
	OR [sls_price] <= 0
	OR [sls_price] != [sls_sales]/[sls_quantity]
	THEN [sls_sales] / NULLIF([sls_quantity], 0)
	ELSE [sls_price]
END AS [sls_price]

 FROM [bronze].[crm_sales_details];



SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  where [sls_ord_num] != TRIM([sls_ord_num])

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details]
  WHERE [sls_order_dt] <= 0
  OR [sls_order_dt] >  20300101
  OR [sls_order_dt] < 19990101;

  SELECT [sls_ship_dt] 
  FROM [bronze].[crm_sales_details]
  WHERE [sls_ship_dt] <= 0
  OR LEN([sls_ship_dt]) < 8
  OR [sls_ship_dt] < 19990101
  OR [sls_ship_dt] >20300101

  SELECT [sls_order_dt], [sls_ship_dt]
  FROM [bronze].[crm_sales_details]
  WHERE [sls_order_dt] > [sls_ship_dt]
  OR [sls_order_dt] > [sls_due_dt]

    SELECT [sls_due_dt]
  FROM [bronze].[crm_sales_details]
  WHERE [sls_due_dt] <= 0
  OR LEN([sls_due_dt]) < 8
  OR [sls_due_dt] < 19990101
  OR [sls_due_dt] >20300101
  


SELECT [sls_quantity]*[sls_price] AS COMP, [sls_sales] 
FROM [bronze].[crm_sales_details]
WHERE [sls_sales] IS NULL
OR [sls_sales] <= 0
OR [sls_quantity]*[sls_price]  != [sls_sales]

SELECT [sls_quantity] 
	FROM [bronze].[crm_sales_details]
	WHERE [sls_quantity] <= 0
	OR [sls_quantity] IS NULL;

SELECT [sls_price]
	FROM [bronze].[crm_sales_details]
	WHERE [sls_price] <= 0
	OR [sls_price] IS NULL;

SELECT * FROM [bronze].[erp_cust_AZ12]
WHERE [CID] IS NULL
OR [BDATE] IS NULL 
OR [BDATE] != CAST([BDATE] AS DATE) 
OR [BDATE] < '1940-01-01' OR [BDATE] > '2015-01-01'
ORDER BY [BDATE] DESC

SELECT [GEN] 
FROM [bronze].[erp_cust_AZ12]
WHERE [GEN] != 'MALE'
AND [GEN] != 'FEMALE'
OR [GEN] != TRIM([GEN])

 SELECT * FROM [bronze].[erp_cust_AZ12]


 SELECT DISTINCT [CNTRY],
 CASE 
	WHEN CNTRY IN ('USA', 'United States', 'US') 
	THEN 'United States'
	WHEN CNTRY = 'DE' THEN 'Germany'
	WHEN  CNTRY = '' OR CNTRY IS NULL THEN 'n/a'
	ELSE TRIM(CNTRY)
	END
 FROM [bronze].[erp_loc_A101]
 ORDER BY CNTRY ASC

 SELECT DISTINCT [MAINTENANCE] FROM [bronze].[erp_px_G1V2]

 SELECT * FROM [bronze].[erp_cust_AZ12]
SELECT 
CASE WHEN [CID] LIKE 'NAS%' THEN
SUBSTRING([CID], 4, LEN([CID])) 
ELSE CID
END AS CID,
CASE 
	WHEN [BDATE] < '1940-01-01'
		OR [BDATE] > GETDATE()
	THEN NULL
	ELSE [BDATE]
	END AS BDATE,
 CASE 
 WHEN [GEN] IN ('M', 'Male') THEN 'MALE'
 WHEN [GEN] IN ('F', 'Female') THEN 'FEMALE'
 ELSE 'n/a'
 END AS GENDER
 FROM [bronze].[erp_cust_AZ12]

  -- CONTINUE ERP CLEANING FOR ERP_LOC
 SELECT REPLACE(CID, '-', '') AS CID, 
  CASE 
	WHEN CNTRY IN ('USA', 'United States', 'US') 
	THEN 'United States'
	WHEN CNTRY = 'DE' THEN 'Germany'
	WHEN  CNTRY = '' OR CNTRY IS NULL THEN 'n/a'
	ELSE TRIM(CNTRY)
	END AS CNTRY
 FROM [bronze].[erp_loc_A101]

 	 SELECT * FROM [bronze].[erp_px_G1V2]-- LAST TABLE HAS GOOD QUALITY