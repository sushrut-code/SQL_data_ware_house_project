DELIMITER //

CREATE PROCEDURE silver_load_silver()
BEGIN
  DECLARE exit handler FOR SQLEXCEPTION
  BEGIN
    SELECT '==========================================';
    SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
    SELECT CONCAT('Error Code: ', SQLSTATE, ' Message: ', MESSAGE_TEXT) FROM performance_schema.events_errors_summary_by_thread_by_error LIMIT 1;
    SELECT '==========================================';
  END;

  DECLARE start_time DATETIME;
  DECLARE end_time DATETIME;
  DECLARE batch_start_time DATETIME;
  DECLARE batch_end_time DATETIME;

  SET batch_start_time = NOW();
  SELECT '================================================';
  SELECT 'Loading Silver Layer';
  SELECT '================================================';

  -- Load crm_cust_info
  SET start_time = NOW();
  SELECT '>> Truncating Table: silver.crm_cust_info';
  TRUNCATE TABLE silver.crm_cust_info;
  SELECT '>> Inserting Data Into: silver.crm_cust_info';

  INSERT INTO silver.crm_cust_info (
    cst_id, 
    cst_key, 
    cst_firstname, 
    cst_lastname, 
    cst_marital_status, 
    cst_gndr,
    cst_create_date
  )
  SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
      WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
      WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
      ELSE 'n/a'
    END,
    CASE 
      WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
      WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
      ELSE 'n/a'
    END,
    cst_create_date
  FROM (
    SELECT * 
    FROM bronze.crm_cust_info a
    WHERE NOT EXISTS (
      SELECT 1 FROM bronze.crm_cust_info b 
      WHERE a.cst_id = b.cst_id AND b.cst_create_date > a.cst_create_date
    )
    AND cst_id IS NOT NULL
  ) t;

  SET end_time = NOW();
  SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  -- Load crm_prd_info
  SET start_time = NOW();
  SELECT '>> Truncating Table: silver.crm_prd_info';
  TRUNCATE TABLE silver.crm_prd_info;
  SELECT '>> Inserting Data Into: silver.crm_prd_info';

  INSERT INTO silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
  )
  SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7) AS prd_key,
    prd_nm,
    IFNULL(prd_cost, 0),
    CASE 
      WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
      WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
      WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
      WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
      ELSE 'n/a'
    END,
    prd_start_dt,
    NULL -- MySQL doesn't support LEAD directly here without window functions, use post-processing if needed
  FROM bronze.crm_prd_info;

  SET end_time = NOW();
  SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  -- Load crm_sales_details
  SET start_time = NOW();
  SELECT '>> Truncating Table: silver.crm_sales_details';
  TRUNCATE TABLE silver.crm_sales_details;
  SELECT '>> Inserting Data Into: silver.crm_sales_details';

  INSERT INTO silver.crm_sales_details (
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
    CASE 
      WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
      ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
    END,
    CASE 
      WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
      ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
    END,
    CASE 
      WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
      ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
    END,
    CASE 
      WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
      ELSE sls_sales
    END,
    sls_quantity,
    CASE 
      WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity, 0)
      ELSE sls_price
    END
  FROM bronze.crm_sales_details;

  SET end_time = NOW();
  SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  -- Load erp_cust_az12
  SET start_time = NOW();
  SELECT '>> Truncating Table: silver.erp_cust_az12';
  TRUNCATE TABLE silver.erp_cust_az12;
  SELECT '>> Inserting Data Into: silver.erp_cust_az12';

  INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
  )
  SELECT
    IF(LEFT(cid, 3) = 'NAS', SUBSTRING(cid, 4), cid),
    IF(bdate > NOW(), NULL, bdate),
    CASE
      WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
      ELSE 'n/a'
    END
  FROM bronze.erp_cust_az12;

  SET end_time = NOW();
  SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  -- Load erp_loc_a101
  SET start_time = NOW();
  SELECT '>> Truncating Table: silver.erp_loc_a101';
  TRUNCATE TABLE silver.erp_loc_a101;
  SELECT '>> Inserting Data Into: silver.erp_loc_a101';

  INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
  )
  SELECT
    REPLACE(cid, '-', ''),
    CASE
      WHEN TRIM(cntry) = 'DE' THEN 'Germany'
      WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
      WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
      ELSE TRIM(cntry)
    END
  FROM bronze.erp_loc_a101;

  SET end_time = NOW();
  SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  -- Load erp_px_cat_g1v2
  SET start_time = NOW();
  SELECT '>> Truncating Table: silver.erp_px_cat_g1v2';
  TRUNCATE TABLE silver.erp_px_cat_g1v2;
  SELECT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

  INSERT INTO silver.erp_px_cat_g1v2 (
    id, cat, subcat, maintenance
  )
  SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;

  SET end_time = NOW();
  SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds');

  SET batch_end_time = NOW();
  SELECT '==========================================';
  SELECT 'Loading Silver Layer is Completed';
  SELECT CONCAT('Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds');
  SELECT '==========================================';
END;
//

DELIMITER ;
