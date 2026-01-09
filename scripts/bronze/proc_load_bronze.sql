/*
=======================================
Stored Procdure : Load bronze Layer (Source -> bronze)
=======================================
Script Purpose :
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:   
        - Truncates the bronze tables before loading data.
        - Uses the BULK INSERT command to load data from csv files to bronze tables.
Parameters: None. This stored procedure does not accept any parameters or return any values.

Usage Example: EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT('=======================================');
		PRINT('Loading Bronze Layer');
		PRINT('=======================================');

		PRINT('=======================================');
		PRINT('Loading CRM Table');
		PRINT('=======================================');

		SET @start_time = GETDATE();
		PRINT('>> Truncating Table : bronze.crm_cust_info')
		TRUNCATE TABLE bronze.crm_cust_info; -- used to quickly remove all rows from a table while keeping the table's structure intact so that whenver we execute this query again then the same rows won't be loaded i.e duplicate rows won't be loaded in the data warehouse
	
		PRINT ('>> Inserting Data Into : bronze.crm_cust_info')
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sahil\Desktop\New DA-DS\SQL Projects\Data Warehouse Material\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- we are skipping the header row i.e row 1 of the csv file coz it contains only the names of columns not actual data
			FIELDTERMINATOR = ',', -- specifying which separator we have used between the columns
			TABLOCK  -- locking the entire table to improve performance for large scale operations
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time)as NVARCHAR ) + ' seconds')
	--------------x--------------------------------x--------------------------x---------------------------x-------------------------x------------------------------x---------------------------x-------------------------x------------------------x-----------------------x-------------------------x--------------------------------x
		
		PRINT('>> Truncating Table : bronze.crm_prd_info')
		TRUNCATE TABLE bronze.crm_prd_info; -- used to quickly remove all rows from a table while keeping the table's structure intact so that whenver we execute this query again then the same rows won't be loaded i.e duplicate rows won't be loaded in the data warehouse
	
		PRINT ('>> Inserting Data Into : bronze.crm_prd_info')
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sahil\Desktop\New DA-DS\SQL Projects\Data Warehouse Material\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2, -- we are skipping the header row i.e row 1 of the csv file coz it contains only the names of columns not actual data
			FIELDTERMINATOR = ',', -- specifying which separator we have used between the columns
			TABLOCK  -- locking the entire table to improve performance for large scale operations
		);

	--------------x--------------------------------x--------------------------x---------------------------x-------------------------x------------------------------x---------------------------x-------------------------x------------------------x-----------------------x-------------------------x--------------------------------x

		PRINT('>> Truncating Table : bronze.crm_sales_details')
		TRUNCATE TABLE bronze.crm_sales_details; -- used to quickly remove all rows from a table while keeping the table's structure intact so that whenver we execute this query again then the same rows won't be loaded i.e duplicate rows won't be loaded in the data warehouse
	
		PRINT ('>> Inserting Data Into : bronze.crm_sales_details')
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sahil\Desktop\New DA-DS\SQL Projects\Data Warehouse Material\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2, -- we are skipping the header row i.e row 1 of the csv file coz it contains only the names of columns not actual data
			FIELDTERMINATOR = ',', -- specifying which separator we have used between the columns
			TABLOCK  -- locking the entire table to improve performance for large scale operations
		);

		--------------x--------------------------------x--------------------------x---------------------------x-------------------------x------------------------------x---------------------------x-------------------------x------------------------x-----------------------x-------------------------x--------------------------------x
		PRINT('=======================================');
		PRINT('Loading ERP Table');
		PRINT('=======================================');

		PRINT('>> Truncating Table : bronze.erp_cust_az12')
		TRUNCATE TABLE bronze.erp_cust_az12; -- used to quickly remove all rows from a table while keeping the table's structure intact so that whenver we execute this query again then the same rows won't be loaded i.e duplicate rows won't be loaded in the data warehouse
	
		PRINT ('>> Inserting Data Into : bronze.erp_cust_az12')
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\sahil\Desktop\New DA-DS\SQL Projects\Data Warehouse Material\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2, -- we are skipping the header row i.e row 1 of the csv file coz it contains only the names of columns not actual data
			FIELDTERMINATOR = ',', -- specifying which separator we have used between the columns
			TABLOCK  -- locking the entire table to improve performance for large scale operations
		);

	--------------x--------------------------------x--------------------------x---------------------------x-------------------------x------------------------------x---------------------------x-------------------------x------------------------x-----------------------x-------------------------x--------------------------------x

		PRINT('>> Truncating Table : bronze.erp_loc_a101')
		TRUNCATE TABLE bronze.erp_loc_a101; -- used to quickly remove all rows from a table while keeping the table's structure intact so that whenver we execute this query again then the same rows won't be loaded i.e duplicate rows won't be loaded in the data warehouse
	
		PRINT ('>> Inserting Data Into : bronze.erp_loc_a101')
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\sahil\Desktop\New DA-DS\SQL Projects\Data Warehouse Material\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2, -- we are skipping the header row i.e row 1 of the csv file coz it contains only the names of columns not actual data
			FIELDTERMINATOR = ',', -- specifying which separator we have used between the columns
			TABLOCK  -- locking the entire table to improve performance for large scale operations
		);

	--------------x--------------------------------x--------------------------x---------------------------x-------------------------x------------------------------x---------------------------x-------------------------x------------------------x-----------------------x-------------------------x--------------------------------x

		PRINT('>> Truncating Table : bronze.erp_px_cat_g1v2')
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; -- used to quickly remove all rows from a table while keeping the table's structure intact so that whenver we execute this query again then the same rows won't be loaded i.e duplicate rows won't be loaded in the data warehouse
	
		PRINT ('>> Inserting Data Into : bronze.erp_px_cat_g1v2')
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sahil\Desktop\New DA-DS\SQL Projects\Data Warehouse Material\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2, -- we are skipping the header row i.e row 1 of the csv file coz it contains only the names of columns not actual data
			FIELDTERMINATOR = ',', -- specifying which separator we have used between the columns
			TABLOCK  -- locking the entire table to improve performance for large scale operations
		);
		SET @batch_start_time = GETDATE()
		PRINT('==============================================')
		PRINT('Loading Bronze Layer is completed')
		PRINT('Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds')
		PRINT('==============================================')

	END TRY	
	BEGIN CATCH
		PRINT('=============================================')
		PRINT('Error Occured during Loading Bronze layer')
		PRINT('Error message : ' + ERROR_MESSAGE())
		PRINT('Error Number : ' + CAST(ERROR_NUMBER() as NVARCHAR))
		PRINT('=============================================')
	END CATCH
END;
EXEC bronze.load_bronze
