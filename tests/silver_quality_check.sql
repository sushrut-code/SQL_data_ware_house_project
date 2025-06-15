-- ============================================================================
-- Silver Layer Quality Checks (MySQL)
-- ============================================================================

-- ====================================================================
-- silver.crm_cust_info
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
SELECT 
    cst_id,
    COUNT(*) AS count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces in customer key
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Marital Status Standardization Check
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

-- ====================================================================
-- silver.crm_prd_info
-- ====================================================================

-- Check for NULLs or Duplicates in Primary Key
SELECT 
    prd_id,
    COUNT(*) AS count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces in product name
SELECT 
    prd_nm 
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Values in Cost
SELECT 
    prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Product Line Standardization
SELECT DISTINCT 
    prd_line 
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders (start > end)
SELECT 
    * 
FROM silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL AND prd_start_dt IS NOT NULL
  AND prd_end_dt < prd_start_dt;

-- ====================================================================
-- silver.crm_sales_details
-- ====================================================================

-- Check for invalid date format in Bronze raw layer
SELECT 
    sls_due_dt 
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
   OR LENGTH(sls_due_dt) != 8 
   OR sls_due_dt > 20500101 
   OR sls_due_dt < 19000101;

-- Check for Invalid Date Orders in Silver
SELECT 
    * 
FROM silver.crm_sales_details
WHERE (sls_order_dt IS NOT NULL AND sls_ship_dt IS NOT NULL AND sls_order_dt > sls_ship_dt)
   OR (sls_order_dt IS NOT NULL AND sls_due_dt IS NOT NULL AND sls_order_dt > sls_due_dt);

-- Check Sales = Quantity * Price and for NULL/Invalids
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
   OR sls_sales != sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- silver.erp_cust_az12
-- ====================================================================

-- Check Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > NOW();

-- Gender Standardization
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- silver.erp_loc_a101
-- ====================================================================

-- Country Code Standardization
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- silver.erp_px_cat_g1v2
-- ====================================================================

-- Check for Unwanted Spaces
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Maintenance Type Standardization
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
