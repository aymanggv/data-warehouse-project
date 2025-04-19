-- Check for nulls or duplicates in PK
-- Check for unwanted spaces
-- Data standardization and consistency 

-- EXEC sp_rename 'bronze.crm_cust_info.ct_create_date', 'cst_create_date', 'COLUMN';

truncate table silver.crm_cust_info;
with cte as(
select *,
ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as rn
from [DataWarehouse].[bronze].[crm_cust_info]
)
insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_gndr,
	cst_marital_status,
	cst_create_date
)
select 
cst_id, 
cst_key, 
Trim(cst_firstname) as cst_firstname, 
Trim(cst_lastname) as cst_lastname, 
case when Upper(Trim(cst_gndr)) = 'F' Then 'Female'
	when Upper(Trim(cst_gndr)) = 'M' Then 'Male'
	else 'N/A'
End cst_gndr,
case when Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
	when Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
	else 'N/A'
End cst_marital_status,
cst_create_date
from cte
where rn = 1 and cst_id is not null
;

--select * from silver.crm_cust_info;
