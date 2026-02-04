--Transformation of products table 
--product key needs to be splitted so we can extract the category id
--a new product key column needs to be created so it can be joined with the product key at the crm sales table 
--null values at the prd_cost column is replaced with 0
--appreviations in the prd_line needs to be replaces 
--because of the end date is less than the start date , lead window function is used to 
--use the start date of the second record for the same product key -1 as the end date of the previous record
--inserting the transformed data into the silver layer 
INSERT INTO silver.crm_prd_info (
prd_id ,
prd_key,
prd_cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)

select 
prd_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') as prd_cat_id,
prd_nm,
ISNULL(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line))
	  WHEN 'M' THEN 'Mountain'
	  WHEN 'R' THEN 'Road' 
	  WHEN 'S' THEN 'Other Sales' 
	  WHEN 'T' THEN 'Touring' 
	  ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) as prd_start_dt,
CAST (DATEADD(day,-1,LEAD(prd_start_dt ) OVER (PARTITION BY prd_key order by prd_start_dt ) ) AS DATE ) as prd_end_dt
from bronze.crm_prd_info




