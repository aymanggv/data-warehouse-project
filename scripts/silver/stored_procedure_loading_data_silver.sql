create or alter procedure silver.load_silver as
begin

declare @start_time datetime, @end_time datetime, @silver_layer_start_time datetime, @silver_layer_end_time datetime;
	begin try
		set @silver_layer_start_time = GETDATE();

		print '=========================================================================';
		print 'Loading Silver Layer';
		print '=========================================================================';

		print '-------------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------------------------------';

		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info'
		truncate table silver.crm_cust_info;
		print '>> Inserting Data Into Table: silver.crm_cust_info';
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
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'

		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info'
		truncate table silver.crm_prd_info;
		print '>> Inserting Data Into Table: silver.crm_prd_info'
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
		  set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_sales_details'
		truncate table silver.crm_sales_details;
		print '>> Inserting Data Into Table: silver.crm_sales_details'
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
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		print '-------------------------------------------------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------------------------------------------------';



		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_cust_az12'
		truncate table silver.erp_cust_az12;
		print '>> Inserting Data Into Table: silver.erp_cust_az12'
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
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'



		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_loc_a101'
		truncate table silver.erp_loc_a101;
		print '>> Inserting Data Into Table: silver.erp_loc_a101'
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
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'



		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into Table: silver.erp_px_cat_g1v2'
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
		 set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'



	end try
		begin catch
			print '=========================================================================';
			print 'Error Occured During Loading Of The Silver Layer';
			print 'Error Message: ' + error_message();
			print 'Error Message: ' + cast (error_number() as nvarchar);
			print 'Error Message: ' + cast (error_state() as nvarchar);
			print '=========================================================================';
		end catch


		set @silver_layer_end_time = GETDATE();
			print '>> Load Duration of Entire Batch of Silver Layer: ' + cast (datediff(second, @silver_layer_start_time, @silver_layer_end_time) as nvarchar) + ' seconds';
			print '>> ------------------------------------------------------------'
end