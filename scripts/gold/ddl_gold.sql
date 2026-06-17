/*
This script is for creating views for the gold layer 
this layer makes the data ready for analytics and reporting 
the views are dim customers ,dim products and fact_sales 

*/

/*
script for the customers dimension 
*/
CREATE VIEW [gold].[dim_customers] AS 
select
ROW_NUMBER () OVER(ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
ci.cst_marital_status AS martial_status,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
ELSE COALESCE(cxi.GEN,'n/a') 
END AS gender,
cxi.BDATE as birthday,
cl.CNTRY as country,
ci.cst_create_date AS create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_CUST_AZ12 cxi
on ci.cst_key=cxi.CID
LEFT JOIN silver.erp_LOC_A101 as cl 
on ci.cst_key=cl.CID

/*
script for the products dimension 
*/
CREATE VIEW [gold].[dim_products] AS 
select 
ROW_NUMBER ()  OVER(ORDER BY pcrm.prd_key,pcrm.prd_start_dt) as product_key,
pcrm.prd_id as product_id,
pcrm.prd_key as product_number,
pcrm.prd_nm as product_name,
pcrm.prd_cat_id as category_id,
perp.CAT as category,
perp.SUBCAT as subcategory,
perp.MAINTENANCE,
pcrm.prd_cost as cost,
pcrm.prd_line as product_line,
pcrm.prd_start_dt as start_date
from silver.crm_prd_info pcrm
LEFT JOIN silver.erp_PX_CAT_G1V2 perp
on pcrm.prd_cat_id=perp.ID
where pcrm.prd_end_dt IS NULL --to focus only on the current products

/*
script for the sales fact table
*/
CREATE VIEW [gold].[fact_sales] as 
SELECT  
     sls_ord_num as order_number,
     gp.product_key ,
     cu.customer_key,
      sls_order_dt as order_date,
      sls_ship_dt as shipping_date,
      sls_due_dt as due_date,
 
      sls_quantity as quantity,
      sls_price  as price ,
	  sls_sales as sales_amount
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id
LEFT JOIN gold.dim_products gp
on sd.sls_prd_key=gp.product_number

