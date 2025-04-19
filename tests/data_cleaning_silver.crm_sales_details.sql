---- Check unwanted spaces
--SELECT *
--FROM bronze.crm_sales_details
--where sls_ord_num != trim(sls_ord_num)

---- Check invalid dates
--select nullif (sls_order_dt, 0) sls_order_dt
--from bronze.crm_sales_details
--where sls_order_dt <= 0 
--or len(sls_order_dt) != 8
--or sls_order_dt > 20500101
--or sls_order_dt < 19000101
--;

---- Check invalid dates
--select nullif (sls_ship_dt, 0) sls_ship_dt
--from bronze.crm_sales_details
--where sls_ship_dt <= 0 
--or len(sls_ship_dt) != 8
--or sls_ship_dt > 20500101
--or sls_ship_dt < 19000101
--;

---- Check invalid dates
--select nullif (sls_due_dt, 0) sls_due_dt
--from bronze.crm_sales_details
--where sls_due_dt <= 0 
--or len(sls_due_dt) != 8
--or sls_due_dt > 20500101
--or sls_due_dt < 19000101
--;


---- Check invalid date orders
--select * 
--from bronze.crm_sales_details
--where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt or sls_ship_dt > sls_due_dt
--;


-- Check data consistency: b/w sales, quantity, and price
-- >> Sales = Quantity * Price
-- >> Values must not be null, zero or negative
-- >> Rules: If Sales is negative, zero, or null, derive it using Quantity and Price.
-- >> Rules: If Price is zero or null, calculate it using Sales and Quantity
-- >> Rules: If Price is negative, convert it to a positive value

--select distinct
--sls_sales as old_sales,
--sls_quantity,
--sls_price as old_sls_price,
--case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
--	 then sls_quantity * ABS(sls_price)
--	 else sls_sales
--end sls_sales,
--case when sls_price is null or sls_price <=0
--	 then sls_sales/ nullif(sls_quantity, 0)
--	 else sls_price
--end as sls_price
--FROM bronze.crm_sales_details
--where sls_sales != sls_quantity * sls_price
--or sls_sales is null or sls_quantity is null or sls_price is null
--or sls_sales <= 0  or sls_quantity <= 0 or sls_price <= 0
--order by sls_sales, sls_quantity, sls_price
--;


--select * from bronze.crm_sales_details;


truncate table silver.crm_sales_details;
insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
SELECT  
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt <=0 or len(sls_order_dt) != 8 then null
	 else Cast(Cast(sls_order_dt as varchar) as date)
end as sls_order_dt,
case when sls_ship_dt <=0 or len(sls_ship_dt) != 8 then null
	 else Cast(Cast(sls_ship_dt as varchar) as date)
end as sls_ship_dt,
case when sls_due_dt <=0 or len(sls_due_dt) != 8 then null
	 else Cast(Cast(sls_due_dt as varchar) as date)
end as sls_due_dt,
case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
	 then sls_quantity * ABS(sls_price)
	 else sls_sales
end sls_sales,
sls_quantity,
case when sls_price is null or sls_price <=0
	 then sls_sales/ nullif(sls_quantity, 0)
	 else sls_price
end as sls_price
FROM bronze.crm_sales_details

--SELECT DISTINCT
--sls_sales,
--sls_quantity,
--sls_price
--FROM silver.crm_sales_details
--WHERE sls_sales != sls_quantity * sls_price
--OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
--OR sls_sales <= 0 or sls_quantity <= 0 OR sls_price <= 0
--ORDER BY sls_sales, sls_quantity, sls_price

--select * from silver.crm_sales_details