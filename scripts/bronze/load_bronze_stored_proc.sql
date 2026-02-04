/*
This stored procedure is used to load data into the bronze layer 
the load approach is : FULL LOAD /Batch Processing / Truncate Insert 
the stored proc executes the following :
Truncate CRM and ERP tables 
load the data from csv files into these tables using bulk insert 
table lock is applied while the loading process 
*/
USE [MyDataWarehouse]
GO
/****** Object:  StoredProcedure [bronze].[load_bronze_layer]    Script Date: 2/4/2026 2:45:30 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER   PROCEDURE [bronze].[load_bronze_layer] AS 
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME ;
	BEGIN TRY
			--Inserting crm_cust_info data 
			Print('===================================================================================')
			PRINT('LOADING BRONZE LAYER')
			Print('===================================================================================')

			PRINT('-----------------------------------------------------------------------------------')
			PRINT('LOADING CRM SYSTEM FILES')
			PRINT('-----------------------------------------------------------------------------------')

			SET @start_time=GETDATE();
			PRINT('>>Truncating table bronze.crm_cust_info')
			TRUNCATE TABLE bronze.crm_cust_info;

			print('>>Inserting data into table bronze.crm_cust_info ')
			BULK INSERT bronze.crm_cust_info
			FROM 'D:\Education\Projects\DWH_project\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
			with(
			FIRSTROW=2 ,
			FIELDTERMINATOR =',',
			TABLOCK);
			SET @end_time=GETDATE()
			PRINT '>>Load Duration is : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'seconds'
			PRINT('==============================================================================')

			--Inserting crm prod_info
			SET @start_time=GETDATE()
			PRINT('>>Truncating table bronze.crm_prd_info')
			TRUNCATE TABLE bronze.crm_prd_info ;
			print('>>Inserting data into table bronze.crm_prd_info ')
			BULK INSERT bronze.crm_prd_info
			FROM 'D:\Education\Projects\DWH_project\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
			WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK);
			SET @end_time=GETDATE()
			PRINT '>>Load duration is : ' +CAST( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			PRINT('==============================================================================')

			--Inserting data into bronze.crm_sales_details
			SET @start_time=GETDATE()
			PRINT('>>Truncating table bronze.crm_sales_details')
			Truncate Table bronze.crm_sales_details
			print('>>Inserting data into table bronze.crm_sales_details ')
			BULK INSERT bronze.crm_sales_details
			FROM 'D:\Education\Projects\DWH_project\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK)
			SET @end_time=GETDATE()
			PRINT 'Load duration is : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			PRINT('==============================================================================')
			
			PRINT('-----------------------------------------------------------------------------------')
			PRINT('LOADING ERP SYSTEM FILES')
			PRINT('-----------------------------------------------------------------------------------')


			--Inserting into into erp tables 
			--inserting into erp_CUST_AZ12
			SET @start_time=GETDATE()
			PRINT('>>Truncating table bronze.erp_CUST_AZ12')
			TRUNCATE TABLE bronze.erp_CUST_AZ12;
			print('>>Inserting data into table bronze.erp_CUST_AZ12 ')
			BULK INSERT bronze.erp_CUST_AZ12 
			FROM 'D:\Education\Projects\DWH_project\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK)
			SET @end_time=GETDATE()
			PRINT 'Load duration is : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds'
			PRINT('==============================================================================')
			

			--inserting into erp_LOC_A101
			SET @start_time=GETDATE()
			PRINT('>>Truncating table bronze.erp_LOC_A101')
			TRUNCATE TABLE bronze.erp_LOC_A101;
			print('>>Inserting data into table bronze.erp_LOC_A101 ')
			BULK INSERT bronze.erp_LOC_A101
			FROM 'D:\Education\Projects\DWH_project\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK)
			SET @end_time=GETDATE()
			PRINT('Load duration is : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR ) + 'seconds')
			PRINT('==============================================================================')

			--INSERTING INTO bronze.erp_PX_CAT_G12V2
			SET @start_time=GETDATE()
			PRINT('>>Truncating table bronze.erp_PX_CAT_G1V2')
			TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
			print('>>Inserting data into table bronze.erp_PX_CAT_G1V2 ')
			BULK INSERT bronze.erp_PX_CAT_G1V2
			FROM 'D:\Education\Projects\DWH_project\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			)
			SET @end_time=GETDATE()
			PRINT ('Load duration is : ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds')
	END TRY
	BEGIN CATCH
	PRINT('-----------------------------------------------------------------------------------')
		PRINT 'AN ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST(ERROR_NUMBER() AS NVARCHAR)
		PRINT('--------------------------------------------------------------------------------------')
	END CATCH

END
