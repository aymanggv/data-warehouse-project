-- check for nulls or duplicates in PK
--select prd_id, count(*)
--from bronze.crm_prd_info
--group by prd_id
--having count(*) > 1 or prd_id is null;

--check for unwanted spaces
-- expectation: no results
--select prd_nm 
--from bronze.[crm_prd_info]
--where prd_nm != Trim(prd_nm)

-- check for nulls or negaive numbers
--select *
--from bronze.crm_prd_info
--where prd_cost < 0 or prd_cost is null;

-- data standardization and consistency
--select distinct prd_line
--from [bronze].[crm_prd_info];

-- check invalid date orders
--select * 
--from bronze.crm_prd_info
--where prd_end_dt < prd_start_dt;


truncate table silver.crm_prd_info;
insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT [prd_id]
	  ,REPLACE(SUBSTRING(prd_key, 1,5), '-', '_') as cat_id
	  ,SUBSTRING(prd_key, 7,LEN(prd_key)) as prd_key
      ,[prd_nm]
      ,isnull(prd_cost, 0) as prd_cost
	  ,case when upper(Trim(prd_line)) = 'M' then 'Mountain'
			when upper(Trim(prd_line)) = 'R' then 'Road'
			when upper(Trim(prd_line)) = 'S' then 'Other Sales'
			when upper(Trim(prd_line)) = 'T' then 'Touring'
			else 'N/A'
		end prd_line
      ,cast([prd_start_dt] as date) as prd_start_dt
      ,cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt -- -1 so theres no overlapping
  FROM [DataWarehouse].[bronze].[crm_prd_info]
  ;

  
  --select * 
  --from silver.crm_prd_info