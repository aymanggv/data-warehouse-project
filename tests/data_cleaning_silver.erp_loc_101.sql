-- Data standardization and consistency
--select distinct
--cntry as old_cntry,
--case when trim(cntry) = 'DE' then 'Germany'
--	 when trim(cntry) in ('US', 'USA') then 'United States'
--	 when trim(cntry) = '' or trim(cntry) is null then 'N/A'
--	 else trim(cntry)
--end as cntry
--from bronze.erp_loc_a101
--order by cntry
--;


truncate table silver.erp_loc_a101;
insert into silver.erp_loc_a101 (
cid,
cntry
)
select 
Replace(cid, '-', ''),
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US', 'USA') then 'United States'
	 when trim(cntry) = '' or trim(cntry) is null then 'N/A'
	 else trim(cntry)
end as cntry
from bronze.erp_loc_a101
;

---- Data standardization and consistency
--select distinct cntry
--from silver.erp_loc_a101

--select *
--from silver.erp_loc_a101
