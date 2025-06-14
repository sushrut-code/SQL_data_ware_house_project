/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
-- CRM Tables

TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA LOCAL INFILE 'C:/Users/LENOVO/Downloads/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' 
IGNORE 1 ROWS;

TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA LOCAL INFILE 'C:/Users/LENOVO/Downloads/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' 
IGNORE 1 ROWS;

TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA LOCAL INFILE 'C:/Users/LENOVO/Downloads/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' 
IGNORE 1 ROWS;


-- ERP Tables

TRUNCATE TABLE bronze.erp_loc_a101;

LOAD DATA LOCAL INFILE 'C:/Users/LENOVO/Downloads/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' 
IGNORE 1 ROWS;

TRUNCATE TABLE bronze.erp_cust_az12;

LOAD DATA LOCAL INFILE 'C:/Users/LENOVO/Downloads/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' 
IGNORE 1 ROWS;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE 'C:/Users/LENOVO/Downloads/sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
IGNORE 1 ROWS;
