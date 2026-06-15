--data quality checks for bronze.crm_prd_info
--checking for duplicates at pk 
select prd_id, count(*)
from
bronze.crm_prd_info
group by prd_id
having count(*) > 1 and prd_id is null ;
--result : no duplicates 


----------------------------------------------------------------------------------------------------
--products table
--prd key needs to be split to apply to the business rules (JOIN with the cat table )
--first 5 char to join with the cat table 
--from the 7th char to the end to link with the sales table 
--checking if all the products categories are listed already n the categories table 
--result : cat CO_PE is not within the cat table 
SELECT prd_id,
prd_key,
REPLACE (SUBSTRING(prd_key,1,5),'-','_' )as cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE (SUBSTRING(prd_key,1,5),'-','_' ) not in (
select ID from bronze.erp_PX_CAT_G1V2)

--checking if there is any unwanted spaces for the prd_nm
-- no results 
select prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

--checking for null or negative numbers for the cost column 
--result : 2 null values 
select 
prd_cost
from bronze.crm_prd_info
where prd_cost <0 or  prd_cost is null
--checking for prd_line 
--result: no obvious desc 
select distinct  prd_line
from bronze.crm_prd_info

--checking end and start date knowing that startdate must not proceed end date
--for each product key make the end date of one the start date of the next one 
--subtract 1 day 
-- cast the date to only include date and not date time as time is not needed 
SELECT prd_id,
	prd_key,
	TRIM(prd_nm),	
	prd_line,
	CAST(prd_start_dt  as DATE)AS prd_start_dt,
	prd_end_dt,
	CAST (DATEADD (day,-1,LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt asc) ) AS DATE) as test_end_dt
	FROM bronze.crm_prd_info
	where prd_start_dt >prd_end_dt
	----------------------------------------------------------------------------------------------------

	--checking sales details table 
	--checking if the sales order number doesn't have any spaces 
	select 
	sls_ord_num
	from bronze.crm_sales_details 
	where sls_ord_num! = trim(sls_ord_num)
	--result : all are fine 
	--checking if the product key matches with the products table and customers id matches 
	--with the customers table 
	--result all the products in sales are listed at the prducts table same for the customer id 

	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	from bronze.crm_sales_details 
	where --sls_prd_key not in (select prd_key from silver.crm_prd_info)
	sls_cust_id not in (select cst_id from silver.crm_cust_info)

