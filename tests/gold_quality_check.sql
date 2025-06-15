-- ============================================================================
-- Quality Checks: Gold Layer
-- ============================================================================

-- ============================================================================
-- Check Uniqueness in gold.dim_customers
-- Expectation: No results (no duplicates)
-- ============================================================================
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold_dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ============================================================================
-- Check Uniqueness in gold.dim_products
-- Expectation: No results (no duplicates)
-- ============================================================================
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold_dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ============================================================================
-- Check Referential Integrity in gold.fact_sales
-- Expectation: No unmatched foreign keys (i.e., NULLs)
-- ============================================================================
SELECT 
    f.*
FROM gold_fact_sales f
LEFT JOIN gold_dim_customers c ON f.customer_key = c.customer_key
LEFT JOIN gold_dim_products p ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;

