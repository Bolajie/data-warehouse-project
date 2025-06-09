CREATE SCHEMA gold
GO

SELECT
		C1.[cst_id] AS customer_id
      ,C1.[cst_key] customer_number
      ,C1.[cst_firstname] AS first_name
      ,C1.[cst_lastname] AS last_name
      ,C1.[cst_marital_status] AS marital_status
	  , CASE 
		WHEN C1.cst_gndr != 'n/a' THEN UPPER(C1.cst_gndr) -- CRM is master
			ELSE COALESCE(C2.GEN, 'n/a')
		END AS gender,
		C3.CNTRY AS location,
	C2.BDATE AS birth_date,
      C1.[cst_create_date] AS create_date
  FROM [DataWarehouse].[SILVER].[crm_cust_info] AS C1
	LEFT JOIN [SILVER].[erp_cust_AZ12] AS C2
	ON C1.cst_key = C2.CID
	LEFT JOIN [SILVER].[erp_loc_A101] AS C3
	ON C1.cst_key = C3.CID
	-- MAKING SURE NO Data
SELECT 
  cst_id,
  COUNT(*) AS record_count
FROM (
  SELECT
    C1.[cst_id],
    C1.[cst_key],
    C1.[cst_firstname],
    C1.[cst_lastname],
    C1.[cst_marital_status],
    C1.[cst_gndr],
    C1.[cst_create_date],
    C2.BDATE,
    C3.CNTRY
  FROM [DataWarehouse].[SILVER].[crm_cust_info] AS C1
  LEFT JOIN [SILVER].[erp_cust_AZ12] AS C2 ON C1.cst_key = C2.CID
  LEFT JOIN [SILVER].[erp_loc_A101] AS C3 ON C1.cst_key = C3.CID
) AS sub
GROUP BY cst_id
having count(*) > 1;
-- create view
CREATE VIEW gold.dimcustomer as 
SELECT
		C1.[cst_id] AS customer_id
      ,C1.[cst_key] customer_number
      ,C1.[cst_firstname] AS first_name
      ,C1.[cst_lastname] AS last_name
      ,C1.[cst_marital_status] AS marital_status
	  , CASE 
		WHEN C1.cst_gndr != 'n/a' THEN UPPER(C1.cst_gndr) -- CRM is master
			ELSE COALESCE(C2.GEN, 'n/a')
		END AS gender,
		C3.CNTRY AS location,
	C2.BDATE AS birth_date,
      C1.[cst_create_date] AS create_date
  FROM [DataWarehouse].[SILVER].[crm_cust_info] AS C1
	LEFT JOIN [SILVER].[erp_cust_AZ12] AS C2
	ON C1.cst_key = C2.CID
	LEFT JOIN [SILVER].[erp_loc_A101] AS C3
	ON C1.cst_key = C3.CID
	-- PRODUCT JOINS
USE DataWarehouse;
SELECT [prd_id] AS product_id
      ,[prd_code] AS product_code
      ,[prd_cat_id] AS product_category_id
      ,[prd_key] AS product_key
      ,[prd_nm] AS product_name
	  ,p1.CAT AS category
	  ,p1.SUBCAT AS sub_category
	  ,p1.MAINTENANCE
	  ,[prd_cost] AS cost
      ,[prd_line] product_line
      ,[prd_start_dt] start_date
  FROM [SILVER].[crm_prd_info]AS P
  LEFT JOIN [SILVER].[erp_px_G1V2] p1
  ON p.prd_cat_id = p1.ID