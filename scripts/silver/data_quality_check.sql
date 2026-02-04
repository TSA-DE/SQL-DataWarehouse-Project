--Data Quality check
--check for nulls and duplicates at the primary key
select cst_id,count(*) as number_of_repitions
from bronze.crm_cust_info
group by cst_id
having count(*) != 1 or cst_id IS NULL

--note: primary key has duplicate and null as well ,we also need to review the nulls
--even it appeared only one time 

--for the string values check for the unwanted spaces 
--expectation :no results
select   cst_firstname 
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)
--result : 15 rows have spaces 
--checking last name column
select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)
--17 rows has white space 

--checking for gender 

--result : no rows which means good quality of gender column

--checking for maritial status column 
select cst_marital_status
from bronze.crm_cust_info
where cst_marital_status != TRIM(cst_marital_status)
--no result > good quality


--checking for data standarization and consistency
select  distinct cst_gndr
from bronze.crm_cust_info

select  distinct cst_marital_status
from bronze.crm_cust_info



