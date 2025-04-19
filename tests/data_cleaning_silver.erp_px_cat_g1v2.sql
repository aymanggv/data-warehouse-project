---- Check unwanted spaces for all columns
--SELECT id
--      ,cat
--      ,subcat
--      ,maintenance
-- FROM bronze.erp_px_cat_g1v2
-- where maintenance	!= trim(maintenance);

 ---- Data standardization and consistency
 --select distinct
 --maintenance
 --from bronze.erp_px_cat_g1v2


truncate table silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2(
	   id
      ,cat
      ,subcat
      ,maintenance
)
SELECT id
      ,cat
      ,subcat
      ,maintenance
 FROM bronze.erp_px_cat_g1v2;


 --select * from silver.erp_px_cat_g1v2;
