-- This script performs various quality checks for data consistency, accuracy,
-- and standardization across the 'silver' layer. It includes checks for:
-- - Null or duplicate primary keys.
-- - Unwanted spaces in string fields.
-- - Data standardization and consistency.
-- - Invalid date ranges and orders.
-- - Data consistency between related fields.

-- CRM CUSTOMER INFO (silver.crm_cust_info)--------------------------------------------------------------------------------------------------------------------------------

-- Null or duplicate primary keys (cst_id)
SELECT cst_id, COUNT(*) AS duplicate_count
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING cst_id IS NULL OR COUNT(*) > 1;

-- Unwanted spaces in string fields
SELECT *
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname  != TRIM(cst_lastname)
   OR cst_key       != TRIM(cst_key);

-- Data standardization and allowed values
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Female', 'Male', 'n/a');

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single', 'Married', 'n/a');

-- Invalid/implausible create dates
SELECT *
FROM silver.crm_cust_info
WHERE cst_create_date IS NULL
   OR cst_create_date < DATE '1900-01-01'
   OR cst_create_date > CURRENT_DATE;


-- CRM PRODUCT INFO (silver.crm_prd_info) ----------------------------------------------------------------------------------------------------------------------------------

-- Null or duplicate primary keys (prd_id)
SELECT prd_id, COUNT(*) AS duplicate_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING prd_id IS NULL OR COUNT(*) > 1;

-- Unwanted spaces in string fields
SELECT *
FROM silver.crm_prd_info
WHERE prd_nm  != TRIM(prd_nm)
   OR prd_key != TRIM(prd_key);

-- Data standardization and allowed values for prd_line
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain', 'Road', 'Touring', 'Other Sales', 'n/a');

-- Invalid/implausible product date ranges
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt IS NULL
   OR prd_end_dt   IS NULL
   OR prd_start_dt > prd_end_dt
   OR prd_start_dt < DATE '1900-01-01'
   OR prd_start_dt > CURRENT_DATE;

-- Overlapping date ranges per prd_key
SELECT *
FROM (
    SELECT
        prd_key,
        prd_start_dt,
        prd_end_dt,
        LAG(prd_end_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS prev_end_dt
    FROM silver.crm_prd_info
) t
WHERE prev_end_dt IS NOT NULL
  AND prd_start_dt <= prev_end_dt;

-- Data consistency: product category id exists in ERP category table
SELECT DISTINCT cat_id
FROM silver.crm_prd_info
WHERE cat_id IS NULL
   OR cat_id NOT IN (SELECT id FROM silver.erp_px_cat_g1v2);


-- CRM SALES DETAILS (silver.crm_sales_details) ----------------------------------------------------------------------------------------------------------------------------------
-- Null or duplicate keys (composite: sls_ord_num, sls_prd_key)
SELECT sls_ord_num, sls_prd_key, COUNT(*) AS duplicate_count
FROM silver.crm_sales_details
GROUP BY sls_ord_num, sls_prd_key
HAVING sls_ord_num IS NULL
    OR sls_prd_key IS NULL
    OR COUNT(*) > 1;

-- Unwanted spaces in string fields
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Invalid order/ship/due dates and ordering
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL
   OR sls_ship_dt  IS NULL
   OR sls_due_dt   IS NULL
   OR sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt
   OR sls_order_dt < DATE '1900-01-01'
   OR sls_order_dt > CURRENT_DATE
   OR sls_ship_dt  > CURRENT_DATE
   OR sls_due_dt   > CURRENT_DATE;

-- Data consistency between related fields: quantity, price, sales
SELECT *
FROM silver.crm_sales_details
WHERE sls_quantity IS NULL OR sls_quantity <= 0
   OR sls_price    IS NULL OR sls_price    <= 0
   OR sls_sales    IS NULL OR sls_sales    <= 0
   OR sls_sales != sls_quantity * sls_price;

-- Referential integrity: product key exists
SELECT s.*
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
       ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;

-- Referential integrity: customer id exists
SELECT s.*
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c
       ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;


-- ERP CUSTOMER (silver.erp_cust_az12) ----------------------------------------------------------------------------------------------------------------------------------
-- Null or duplicate primary keys (cid)
SELECT cid, COUNT(*) AS duplicate_count
FROM silver.erp_cust_az12
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- Unwanted spaces in string fields
SELECT *
FROM silver.erp_cust_az12
WHERE cid != TRIM(cid);

-- Data standardization and allowed values for gen
SELECT DISTINCT gen
FROM silver.erp_cust_az12
WHERE gen NOT IN ('Female', 'Male', 'n/a');

-- Invalid/implausible birth dates
SELECT *
FROM silver.erp_cust_az12
WHERE bdate IS NULL
   OR bdate < DATE '1900-01-01'
   OR bdate > CURRENT_DATE;

-- Consistency with CRM customers: cid should exist in crm_cust_info.cst_key
SELECT e.*
FROM silver.erp_cust_az12 e
LEFT JOIN silver.crm_cust_info c
       ON e.cid = c.cst_key
WHERE c.cst_key IS NULL;


-- ERP LOCATION (silver.erp_loc_a101)----------------------------------------------------------------------------------------------------------------------------------
-- Null or duplicate keys (cid)
SELECT cid, COUNT(*) AS duplicate_count
FROM silver.erp_loc_a101
GROUP BY cid
HAVING cid IS NULL OR COUNT(*) > 1;

-- Unwanted spaces in string fields
SELECT *
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry);

-- Data standardization and allowed values for cntry
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
WHERE cntry NOT IN ('Germany', 'United States', 'n/a');

-- Consistency with CRM customers: cid should exist in crm_cust_info.cst_key
SELECT l.*
FROM silver.erp_loc_a101 l
LEFT JOIN silver.crm_cust_info c
       ON l.cid = c.cst_key
WHERE c.cst_key IS NULL;


-- ERP PRODUCT CATEGORY --------------------------------------------------------------------------------------------------------------------------------------
-- Null or duplicate primary keys (id)
SELECT id, COUNT(*) AS duplicate_count
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING id IS NULL OR COUNT(*) > 1;

-- Unwanted spaces in string fields
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Basic presence/allowed-values sanity (free text domain; ensure non-empty after trim)
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE TRIM(cat) = '' OR TRIM(subcat) = '' OR TRIM(maintenance) = '';

-- Consistency: categories referenced by crm_prd_info must exist
SELECT DISTINCT p.cat_id
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 e
       ON p.cat_id = e.id
WHERE e.id IS NULL;


----------------------------------------------------------------------------------------------------------------------------------
-- CROSS-DOMAIN CONSISTENCY SNAPSHOTS
----------------------------------------------------------------------------------------------------------------------------------

-- Customers in ERP without CRM match (by key)
SELECT e.cid
FROM silver.erp_cust_az12 e
LEFT JOIN silver.crm_cust_info c
       ON e.cid = c.cst_key
WHERE c.cst_key IS NULL;

-- Locations with unknown customers
SELECT l.cid
FROM silver.erp_loc_a101 l
LEFT JOIN silver.crm_cust_info c
       ON l.cid = c.cst_key
WHERE c.cst_key IS NULL;

-- Sales referencing missing products or customers
SELECT COUNT(*) AS sales_missing_products
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p
       ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;

SELECT COUNT(*) AS sales_missing_customers
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c
       ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;


