

/* 
==============================================================================================
This stored procedure performs the ETL process to populate the silver schema data from the bronze schema
Action Performed :
Truncate silver tables 
Insert transformed and cleansed data from bronze layer to silver layer 

Parameters :
NONE 

USAGE EXAMPLE : 
EXEC Silver.load_silver_layer

*/

USE [MyDataWarehouse]
GO
CREATE PROCEDURE [silver].[load_silver_layer] AS
BEGIN
	declare @start_time datetime ,
	@end_time datetime,
	@batch_start_time datetime,
	@batch_end_time datetime
	BEGIN TRY
			SET @batch_start_time=getdate()

			PRINT('INSERTING INTO SILVER_CUST_INFO')
			PRINT('TRUNCATING TABLE :silver.crm_cust_info ')

			SET @start_time=getdate();
	
			TRUNCATE TABLE silver.crm_cust_info;
	
			INSERT INTO silver.crm_cust_info
			(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)

			SELECT cst_id ,
			cst_key,
			TRIM(cst_firstname) AS first_name,
			TRIM (cst_lastname) AS last_name,

			CASE WHEN UPPER (TRIM(cst_marital_status ))='S' THEN 'Single'
				 WHEN UPPER (TRIM(cst_marital_status))='M' THEN 'Married'
				 ELSE 'n/a' 
			END cst_marital_status,


			CASE WHEN UPPER (TRIM(cst_gndr ))='M' THEN 'Male'
				 WHEN UPPER (TRIM(cst_gndr))='F' THEN 'Female'
				 ELSE 'n/a' 
			END cst_gndr,
			cst_create_date

			from (
			select *,
			ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
			) t where flag_last=1 
			set @end_time =getdate()

			print ('LOAD DURATION IS : ' + CAST(DATEDIFF(second,@start_time,@end_time) as varchar)+'seconds')
			-------------------------------------------------------------------------------------

			PRINT('============================================')
			PRINT('INSERT INTO SILVER.CRM_PRD_INFO')
			PRINT('TRUNCATING TABLE : silver.crm_prd_info')
			set @start_time=getdate();
			TRUNCATE TABLE silver.crm_prd_info ;
			INSERT INTO silver.crm_prd_info(prd_id,prd_cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

			SELECT prd_id,
			REPLACE (SUBSTRING(prd_key,1,5),'-','_' )as prd_cat_id,
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			TRIM(prd_nm) as prd_nm,
			ISNULL (prd_cost,0) AS prd_cost,
			CASE  WHEN UPPER(TRIM (prd_line)) ='M' Then 'Mountain'
				  WHEN UPPER(TRIM(prd_line)) ='R' then 'Road'
				  WHEN UPPER(TRIM(prd_line))='S' then 'Other Sales'
				  WHEN UPPER(TRIM(prd_line))='T' Then 'Touring'
				  ELSE 'n/a'
			END prd_line,

			CAST(prd_start_dt  as DATE)AS prd_start_dt,
			CAST (DATEADD (day,-1,
			LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt asc)
			)
			AS DATE) as prd_end_dt
			FROM bronze.crm_prd_info

			set @end_time=getdate()
			print ('LOAD DURATION IS : ' + CAST(DATEDIFF(second,@start_time,@end_time) as varchar)+'seconds')


			PRINT('============================================')
			PRINT('INSERT INTO SILVER.CRM_SALES_DETAILS')
			PRINT('TRUNCATING TABLE :silver.crm_sales_details ')
			TRUNCATE TABLE silver.crm_sales_details;
			set @start_time=getdate();
	
			INSERT INTO silver.crm_sales_details(sls_ord_num
			  ,sls_prd_key
			  ,sls_cust_id
			  ,sls_order_dt
			  ,sls_ship_dt
			  ,sls_due_dt
			  ,sls_sales
			  ,sls_quantity
			  ,sls_price)

			SELECT sls_ord_num,
				  sls_prd_key,
				  sls_cust_id,
				 CASE WHEN sls_order_dt <=0 OR LEN(sls_order_dt)!=8 THEN NULL
					  ELSE CAST(CAST(sls_order_dt as nvarchar) as date)
				 END AS sls_order_dt,

				 CASE WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt)!=8 THEN NULL
					  ELSE CAST(CAST(sls_ship_dt as nvarchar) as date)
				 END AS sls_ship_dt,

				 CASE WHEN sls_due_dt <=0 OR LEN(sls_due_dt)!=8 THEN NULL
					  ELSE CAST(CAST(sls_due_dt as nvarchar) as date)
				 END AS sls_due_dt,

				 CASE WHEN sls_sales <=0 or sls_sales is null or sls_sales !=abs(sls_quantity)*abs(sls_price)
					  THEN  abs(sls_price)* abs(sls_quantity)
					  ELSE sls_sales
				 END AS sls_sales,

				 sls_quantity,

				 CASE WHEN sls_price <= 0 or sls_price is null 
					  THEN sls_sales/nullif(sls_quantity,0)
					  else sls_price
				 END AS sls_price
			  FROM bronze.crm_sales_details
			  set @end_time=getdate()
			  print ('LOAD DURATION IS : ' + CAST(DATEDIFF(second,@start_time,@end_time) as varchar)+'seconds')
			  -----------------------------------------------------------------------------------
			PRINT('============================================')
			PRINT('INSERT INTO silver.erp_CUST_AZ12')
			PRINT('TRUNCATING TABLE :silver.erp_CUST_AZ12 ')
			set @start_time=getdate()
			TRUNCATE TABLE silver.erp_CUST_AZ12
			INSERT INTO silver.erp_CUST_AZ12(CID,BDATE,GEN)
			SELECT  CASE WHEN cid like 'NAS%'
						THEN SUBSTRING(cid,4,len(cid))
						ELSE cid
				END AS cid,
						CASE WHEN bdate >getdate()
						THEN null 
						ELSE bdate
					  END AS bdate,
      
					  CASE WHEN upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
						   WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
						   ELSE 'n/a' 
						   END AS gen
				  FROM [MyDataWarehouse].[bronze].[erp_CUST_AZ12]
				  set @end_time=GETDATE()
				  print ('LOAD DURATION IS : ' + CAST(DATEDIFF(second,@start_time,@end_time) as varchar)+'seconds')



			PRINT('============================================')
			PRINT('INSERT INTO silver.erp_LOC_A101')
			PRINT('TRUNCATING TABLE :silver.erp_LOC_A101 ')
			set @start_time=getdate();
			TRUNCATE TABLE silver.erp_LOC_A101;
			INSERT INTO silver.erp_LOC_A101(CID,CNTRY)
			select 
			REPLACE(CID,'-','') AS CID,
			CASE WHEN TRIM(CNTRY) ='DE' THEN 'Germany'
				 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
				 WHEN CNTRY IS NULL OR CNTRY ='' THEN 'n/a'
				 ELSE CNTRY
			END AS CNTRY
			FROM bronze.erp_LOC_A101
			set @end_time=getdate();
			print ('LOAD DURATION IS : ' + CAST(DATEDIFF(second,@start_time,@end_time) as varchar)+'seconds')

			PRINT('============================================')
			PRINT('INSERT INTO silver.erp_PX_CAT_G1V2')
			PRINT('TRUNCATING TABLE :silver.erp_PX_CAT_G1V2 ')
			set @start_time=getdate()
			TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
			INSERT INTO silver.erp_PX_CAT_G1V2(ID,CAT,SUBCAT,MAINTENANCE)
				  select ID,
					CAT,
					SUBCAT,
					MAINTENANCE
					from bronze.erp_PX_CAT_G1V2
			set @end_time=GETDATE();
			set @batch_end_time=getdate();
			print('LOAD DURATION IS : '+CAST(DATEDIFF(second,@start_time,@end_time)AS VARCHAR))
			print('BATCH LOAD DURATION IS : '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS VARCHAR))


		
	END TRY
	BEGIN CATCH 
	PRINT('********************************************')
	print('AN ERROR OCCURED WHILE LOADING SILVER LAYER ')
	PRINT 'ERROR_MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR NUMBER'  +ERROR_NUMBER();
	PRINT('********************************************')
	END CATCH 
END

