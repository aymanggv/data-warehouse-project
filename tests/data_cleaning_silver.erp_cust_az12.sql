---- Identify out of range dates
--select distinct 
--bdate
--from bronze.erp_cust_az12
--where bdate < '1924-01-01' or bdate > GETDATE();

---- Data standardization and consistency
--select distinct
--gen,
--case when upper(trim(gen)) in ('F','Female') then 'Female' --IN allows you to specify multiple values in a WHERE clause.
--	 when upper(trim(gen)) in ('M','Male') then 'Male'
--	 else 'N/A'
--end as new_gen
--from bronze.erp_cust_az12
--;


truncate table silver.erp_cust_az12;
insert into silver.erp_cust_az12(
cid,
bdate,
gen
)
select 
case when cid like 'NAS%' then SUBSTRING(cid, 4, LEN(cid))
	 else cid
end cid,
case when bdate > GETDATE() then null
	 else bdate
end as bdate,
case when upper(trim(gen)) in ('F','Female') then 'Female' --IN allows you to specify multiple values in a WHERE clause.
	 when upper(trim(gen)) in ('M','Male') then 'Male'
	 else 'N/A'
end as gen
from bronze.erp_cust_az12;

---- Identify out of range dates
--select distinct 
--bdate
--from silver.erp_cust_az12
--where bdate < '1924-01-01' or bdate > GETDATE();


---- Data standardization and consistency
--select distinct
--gen
--from silver.erp_cust_az12
--;

--select * from silver.erp_cust_az12