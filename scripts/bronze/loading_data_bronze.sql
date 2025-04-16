create or alter procedure bronze.load_bronze_layer AS
Begin
	declare @start_time datetime, @end_time datetime, @bronze_layer_start_time datetime, @bronze_layer_end_time datetime;
	begin try
		set @bronze_layer_start_time = GETDATE();

		print '=========================================================================';
		print 'Loading Bronze Layer';
		print '=========================================================================';

		print '-------------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-------------------------------------------------------------------------';

		set @start_time = GETDATE();
		-- Truncate and then insert
		print '>> Truncating Table: crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting Data Into Table: crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\ayman\OneDrive\Documents\Code\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock -- option to improve performance where I lock the table while loading data into it
		);
		-- select * from bronze.crm_cust_info;
		-- select count(*)from bronze.crm_cust_info;
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		set @start_time = GETDATE();
		print '>> Truncating Table: crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting Data Into Table: crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\ayman\OneDrive\Documents\Code\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock -- option to improve performance where I lock the table while loading data into it
		);
		-- select * from bronze.crm_prd_info;
		-- select count(*)from bronze.crm_prd_info;
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		set @start_time = GETDATE();
		print '>> Truncating Table: crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>> Inserting Data Into Table: crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\ayman\OneDrive\Documents\Code\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock -- option to improve performance where I lock the table while loading data into it
		);
		-- select * from bronze.crm_sales_details;
		-- select count(*)from bronze.crm_sales_details;
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		print '-------------------------------------------------------------------------';
		print 'Loading ERP Tables';
		print '-------------------------------------------------------------------------';


		set @start_time = GETDATE();
		print '>> Truncating Table: erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print '>> Inserting Data Into Table: erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\ayman\OneDrive\Documents\Code\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock -- option to improve performance where I lock the table while loading data into it
		);
		-- select * from bronze.erp_cust_az12;
		-- select count(*)from bronze.erp_cust_az12;
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		set @start_time = GETDATE();
		print '>> Truncating Table: erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting Data Into Table: erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\ayman\OneDrive\Documents\Code\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock -- option to improve performance where I lock the table while loading data into it
		);
		-- select * from bronze.erp_loc_a101;
		-- select count(*)from bronze.erp_loc_a101;
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


		set @start_time = GETDATE();
		print '>> Truncating Table: erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		print '>> Inserting Data Into Table: erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\ayman\OneDrive\Documents\Code\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock -- option to improve performance where I lock the table while loading data into it
		);
		-- select * from bronze.erp_px_cat_g1v2;
		-- select count(*)from bronze.erp_px_cat_g1v2;
		set @end_time = GETDATE();
		print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'


	end try
	begin catch
		print '=========================================================================';
		print 'Error occured during loading of the bronze layer';
		print 'Error Message: ' + error_message();
		print 'Error Message: ' + cast (error_number() as nvarchar);
		print 'Error Message: ' + cast (error_state() as nvarchar);
		print '=========================================================================';
	end catch


	set @bronze_layer_end_time = GETDATE();
		print '>> Load Duration of Entire Batch of Bronze Layer: ' + cast (datediff(second, @bronze_layer_start_time, @bronze_layer_end_time) as nvarchar) + ' seconds';
		print '>> ------------------------------------------------------------'
End
