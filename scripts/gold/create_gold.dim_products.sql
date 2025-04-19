---- Check if duplicates have been created after join
--select prd_key, count(*) from(
--SELECT prd_id
--      ,cat_id
--      ,prd_key
--      ,prd_nm
--      ,prd_cost
--      ,prd_line
--      ,prd_start_dt
--	  ,pc.cat
--	  ,pc.subcat
--	  ,pc.maintenance
--  FROM silver.crm_prd_info pn
--  left join silver.erp_px_cat_g1v2 pc
--  on pn.cat_id = pc.id
--  where prd_end_dt is null -- To get up to date items. Could alsp use row number but more complex.
--  ) t group by prd_key
--  having count(*) > 1
--  ;



create view gold.dim_products as
  SELECT 
	   ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	   prd_id as product_id
	  ,prd_key as product_number
	  ,prd_nm as product_name
      ,cat_id as category_id
	  ,pc.cat as category
	  ,pc.subcat as subcateegory
	  ,pc.maintenance
      ,prd_cost as cost
      ,prd_line as product_line
      ,prd_start_dt as start_date
  FROM silver.crm_prd_info pn
  left join silver.erp_px_cat_g1v2 pc
  on pn.cat_id = pc.id
  where prd_end_dt is null -- To get up to date items. Could alsp use row number but more complex.

  -- select * from gold.dim_products