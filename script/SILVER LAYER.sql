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
	prd_code NVARCHAR(50),
	prd_cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT, 
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_time DATETIME2 DEFAULT GETDATE() -- ADDING DATA ENTRY TIME STAMPS
);

IF OBJECT_ID('crm_sales_details', 'U') is not null
	DROP TABLE SILVER.crm_prd_info;
CREATE TABLE SILVER.crm_sales_detailS(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
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


CREATE OR ALTER PROCEDURE SILVER.LOAD_SILVER AS
BEGIN
    DECLARE @start_load DATETIME, @end_load DATETIME, @start_batch_load DATETIME, @end_batch_load DATETIME;

    BEGIN TRY
        PRINT '==========================================='
        PRINT 'LOADING SILVER LAYER BEGINS'
        SET @start_batch_load = GETDATE()
        PRINT '==========================================='

        ------------------------------------------
        -- Load crm_cust_info
        ------------------------------------------
        PRINT 'START LOADING crm_cust_info'
        SET @start_load = GETDATE()

        TRUNCATE TABLE [SILVER].[crm_cust_info];
        INSERT INTO [SILVER].[crm_cust_info] (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date)
        SELECT 
            [cst_id],
            [cst_key],
            TRIM([cst_firstname]) AS cst_firstname,
            TRIM([cst_lastname]) AS cst_lastname,
            CASE 
                WHEN TRIM([cst_marital_status]) = 'M' THEN 'Married'
                WHEN TRIM([cst_marital_status]) = 'S' THEN 'Single'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN TRIM([cst_gndr]) = 'M' THEN 'Male'
                WHEN TRIM([cst_gndr]) = 'F' THEN 'Female'
                ELSE 'n/a'
            END AS cst_gndr,
            [cst_create_date]
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS RN
            FROM [bronze].[crm_cust_info]
        ) AS SUB
        WHERE RN = 1 AND cst_id IS NOT NULL
        ORDER BY cst_id;

        SET @end_load = GETDATE()
        PRINT 'LOADED crm_cust_info'
        PRINT '>>>>> DURATION: ' + CAST(DATEDIFF(SECOND, @start_load, @end_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='

        ------------------------------------------
        -- Load crm_prd_info
        ------------------------------------------
        PRINT 'START LOADING crm_prd_info'
        SET @start_load = GETDATE()

        TRUNCATE TABLE [SILVER].[crm_prd_info];
        INSERT INTO [SILVER].[crm_prd_info] (
            [prd_id], [prd_code], [prd_cat_id], [prd_key],
            [prd_nm], [prd_cost], [prd_line], [prd_start_dt], [prd_end_dt])
        SELECT 
            [prd_id],
            [prd_key] AS prd_code,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            [prd_nm],
            ISNULL([prd_cost], 0) AS prd_cost,
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

        SET @end_load = GETDATE()
        PRINT 'LOADED crm_prd_info'
        PRINT '>>>>> DURATION: ' + CAST(DATEDIFF(SECOND, @start_load, @end_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='

        ------------------------------------------
        -- Load crm_sales_details
        ------------------------------------------
        PRINT 'START LOADING crm_sales_details'
        SET @start_load = GETDATE()

        TRUNCATE TABLE [SILVER].[crm_sales_details];
        INSERT INTO [SILVER].[crm_sales_details] (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
            sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        SELECT 
            [sls_ord_num], [sls_prd_key], [sls_cust_id],
            CASE WHEN [sls_order_dt] = 0 OR LEN([sls_order_dt]) < 8 THEN NULL
                ELSE CAST(CAST([sls_order_dt] AS VARCHAR) AS DATE) END,
            CASE WHEN [sls_ship_dt] = 0 OR LEN([sls_ship_dt]) < 8 THEN NULL
                ELSE CAST(CAST([sls_ship_dt] AS VARCHAR) AS DATE) END,
            CASE WHEN [sls_due_dt] = 0 OR LEN([sls_due_dt]) < 8 THEN NULL
                ELSE CAST(CAST([sls_due_dt] AS VARCHAR) AS DATE) END,
            CASE 
                WHEN [sls_sales] IS NULL OR [sls_sales] <= 0 
                     OR [sls_sales] != [sls_quantity] * ABS([sls_price])
                THEN [sls_quantity] * [sls_price]
                ELSE [sls_sales]
            END,
            [sls_quantity],
            CASE 
                WHEN [sls_price] IS NULL OR [sls_price] <= 0 
                     OR [sls_price] != [sls_sales]/[sls_quantity]
                THEN [sls_sales] / NULLIF([sls_quantity], 0)
                ELSE [sls_price]
            END
        FROM [bronze].[crm_sales_details];

        SET @end_load = GETDATE()
        PRINT 'LOADED crm_sales_details'
        PRINT '>>>>> DURATION: ' + CAST(DATEDIFF(SECOND, @start_load, @end_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='

        ------------------------------------------
        -- Load erp_cust_AZ12
        ------------------------------------------
        PRINT 'START LOADING erp_cust_AZ12'
        SET @start_load = GETDATE()

        TRUNCATE TABLE [SILVER].[erp_cust_AZ12];
        INSERT INTO [SILVER].[erp_cust_AZ12] (CID, BDATE, GEN)
        SELECT 
            CASE WHEN [CID] LIKE 'NAS%' THEN SUBSTRING([CID], 4, LEN([CID])) ELSE [CID] END,
            CASE 
                WHEN [BDATE] < '1940-01-01' OR [BDATE] > GETDATE() THEN NULL
                ELSE [BDATE]
            END,
            CASE 
                WHEN [GEN] IN ('M', 'Male') THEN 'MALE'
                WHEN [GEN] IN ('F', 'Female') THEN 'FEMALE'
                ELSE 'n/a'
            END
        FROM [bronze].[erp_cust_AZ12];

        SET @end_load = GETDATE()
        PRINT 'LOADED erp_cust_AZ12'
        PRINT '>>>>> DURATION: ' + CAST(DATEDIFF(SECOND, @start_load, @end_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='

        ------------------------------------------
        -- Load erp_loc_A101
        ------------------------------------------
        PRINT 'START LOADING erp_loc_A101'
        SET @start_load = GETDATE()

        TRUNCATE TABLE [SILVER].[erp_loc_A101];
        INSERT INTO [SILVER].[erp_loc_A101] (CID, CNTRY)
        SELECT 
            REPLACE(CID, '-', ''),
            CASE 
                WHEN CNTRY IN ('USA', 'United States', 'US') THEN 'United States'
                WHEN CNTRY = 'DE' THEN 'Germany'
                WHEN CNTRY = '' OR CNTRY IS NULL THEN 'n/a'
                ELSE TRIM(CNTRY)
            END
        FROM [bronze].[erp_loc_A101];

        SET @end_load = GETDATE()
        PRINT 'LOADED erp_loc_A101'
        PRINT '>>>>> DURATION: ' + CAST(DATEDIFF(SECOND, @start_load, @end_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='

        ------------------------------------------
        -- Load erp_px_G1V2 (no transformation)
        ------------------------------------------
        PRINT 'START LOADING erp_px_G1V2'
        SET @start_load = GETDATE()

        TRUNCATE TABLE [SILVER].[erp_px_G1V2];
        INSERT INTO [SILVER].[erp_px_G1V2] ([ID], [CAT], [SUBCAT], [MAINTENANCE])
        SELECT * FROM [bronze].[erp_px_G1V2];

        SET @end_load = GETDATE()
        PRINT 'LOADED erp_px_G1V2'
        PRINT '>>>>> DURATION: ' + CAST(DATEDIFF(SECOND, @start_load, @end_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='

        SET @end_batch_load = GETDATE()
        PRINT 'TOTAL BATCH DURATION: ' + CAST(DATEDIFF(SECOND, @start_batch_load, @end_batch_load) AS NVARCHAR) + ' SECONDS'
        PRINT '==========================================='
    
    END TRY

    BEGIN CATCH
        PRINT '*****************************************************'
        PRINT 'ERROR OCCURRED WHILE LOADING SILVER LAYER'
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE()
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR)
        PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR)
        PRINT '*****************************************************'
    END CATCH
END

EXEC [SILVER].[LOAD_SILVER]