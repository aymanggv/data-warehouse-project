---- Check if duplicates have been created after join
--select cst_id, count(*) from (
--select
--ci.cst_id,
--ci.cst_key,
--ci.cst_firstname,
--ci.cst_lastname,
--ci.cst_marital_status,
--ci.cst_gndr,
--ci.cst_create_date,
--ca.bdate,
--ca.gen,
--la.cntry
--from silver.crm_cust_info as ci
--left join silver.erp_cust_az12 as ca
--on ci.cst_key = ca.cid
--left join silver.erp_loc_a101 as la
--on ci.cst_key = la.cid
--) t group by cst_id
--having count(*) > 1
--;

---- Data integration
--select distinct
--ci.cst_gndr,
--ca.gen,
--case when ci.cst_gndr != 'N/A' then ci.cst_gndr -- CRM is master for the gender info
--	 else coalesce (ca.gen, 'N/A') -- The COALESCE() function returns the first non-null value in a list. Could also do this WHEN ca.gen IS NOT NULL THEN ca.gen ELSE 'N/A'  
--end as new_gen
--from silver.crm_cust_info as ci
--left join silver.erp_cust_az12 as ca
--on ci.cst_key = ca.cid
--left join silver.erp_loc_a101 as la
--on ci.cst_key = la.cid
--order by 1,2
--;



create view gold.dim_customers as 
select
ROW_NUMBER() over (order by cst_id) as customer_key, -- Creating a surrogate key
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr != 'N/A' then ci.cst_gndr
	 else coalesce (ca.gen, 'N/A')
end as gender,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
on ci.cst_key = la.cid
;


select * from gold.dim_customers;