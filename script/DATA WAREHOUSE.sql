CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
USE DataWarehouse;
IF OBJECT_ID('crm_cust_info' , 'U') is not null
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50), 
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
IF OBJECT_ID('crm_prd_info', 'U') is not null
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost NVARCHAR(50), 
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);

IF OBJECT_ID('crm_sales_details', 'U') is not null
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID('erp_cust_AZ12', 'U') is not null
	DROP TABLE bronze.erp_cust_AZ12;
CREATE TABLE bronze.erp_cust_AZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)
);

IF OBJECT_ID('erp_loc_A101', 'U') is not null
	DROP TABLE bronze.erp_loc_A101;
CREATE TABLE bronze.erp_loc_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);

IF OBJECT_ID('erp_px_G1V2', 'U') is not null
	DROP TABLE bronze.erp_px_G1V2;
CREATE TABLE bronze.erp_px_G1V2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
);
-- AUTOMATE BULK INSERT PROCDURES USING PROCEDURE
CREATE OR ALTER PROCEDURE bronze.LOAD_BRONZE AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
	SET @BATCH_START_TIME = GETDATE()
	BEGIN TRY
		PRINT '*****************************************'
		PRINT 'LOADING BRONZE LAYER......'
		PRINT '*****************************************'
		-- CALCULATING HOW LONG IT TAKES A TABLE TO LOAD
		SET @START_TIME = GETDATE()
		--TRUNCATE PREVENTS BULK LOAD FROM CREATING DUPLICATES
		TRUNCATE TABLE [bronze].[crm_cust_info]
		-- BULK LOAD HELPS TO INSERT CSV FILES
		BULK INSERT [bronze].[crm_cust_info]
		FROM "C:\Users\Bolaji Ilori\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"
		WITH (
		FIRSTROW = 2, -- INDICATE FIRST ROW
		FIELDTERMINATOR= ',', -- INDICATE SEPARATOR
		TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT '>> DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR)

		PRINT 'LOAD cust_info TABLE COMPLETED'

		SET @START_TIME = GETDATE()
		
		TRUNCATE TABLE [bronze].[crm_prd_info]
		BULK INSERT [bronze].[crm_prd_info]
		FROM "C:\Users\Bolaji Ilori\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR= ',',
		TABLOCK
		);
		PRINT 'LOAD prd_info TABLE COMPLETED'
		SET @END_TIME = GETDATE()
		PRINT '>> DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR)

		PRINT '>>>> START LOAD'
		SET @START_TIME = GETDATE()

		TRUNCATE TABLE [bronze].[crm_sales_details]
		BULK INSERT [bronze].[crm_sales_details]
		FROM "C:\Users\Bolaji Ilori\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR= ',',
		TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT '>> DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR)
		PRINT 'LOAD sales_details TABLE COMPLETED'

		SET @START_TIME = GETDATE()
		TRUNCATE TABLE [bronze].[erp_cust_AZ12]
		BULK INSERT [bronze].[erp_cust_AZ12]
		FROM "C:\Users\Bolaji Ilori\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR= ',',
		TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT '>> DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR)


		SET @START_TIME = GETDATE()
		
		PRINT 'LOAD CUST_AZ12 TABLE COMPLETED'
		TRUNCATE TABLE [bronze].[erp_loc_A101]
		BULK INSERT [bronze].[erp_loc_A101]
		FROM "C:\Users\Bolaji Ilori\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR= ',',
		TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT '>> DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR)
		PRINT 'LOAD loc_a101 TABLE COMPLETED'


		SET @START_TIME = GETDATE()
		TRUNCATE TABLE [bronze].[erp_px_G1V2]
		BULK INSERT [bronze].[erp_px_G1V2]
		FROM "C:\Users\Bolaji Ilori\Documents\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR= ',',
		TABLOCK
		);
		SET @END_TIME = GETDATE()
		PRINT '>> DURATION : ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR)
			SET @BATCH_END_TIME = GETDATE()

		PRINT 'TOTAL BATCH LOADDURATION: ' + CAST(DATEDIFF(second, @BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + ' SECONDS'
		
		PRINT 'LOAD erp_px_G1V2 TABLE COMPLETED'
		PRINT '*************************************************'
		PRINT 'LOADING COMPLETED'
		PRINT '**************************************************'
	
	END TRY
	BEGIN CATCH
		PRINT '*****************************************************'
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE:' +CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '******************************************************'
	END CATCH
	
END

-- EXCECUTE PROCEDURE

EXEC BRONZE.LOAD_BRONZE;


