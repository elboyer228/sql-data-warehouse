-- This script is used to create the silver tables from the bronze tables with quality checks and transformations for the CRM and ERP tables
-- WARNING: This script will remove all rows from the silver tables if they exist


-- CRM CUSTOMER INFO TABLE --------------------------------------------------------------------------------------------------------------------------------------

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SELECT 
cst_id, 
cst_key, 
TRIM(cst_firstname) AS cst_firstname, 
TRIM(cst_lastname) AS cst_lastname,
CASE 
	WHEN UPPER(TRIM(cst_marital_status))='S' then 'Single'
	WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
	ELSE 'n/a' 
END cst_marital_status,
CASE 
	WHEN UPPER(TRIM(cst_gndr))='F' then 'Female'
	WHEN UPPER(TRIM(cst_gndr))='M' then 'Male'
	ELSE 'n/a' 
END cst_gndr,
cst_create_date
FROM (
	SELECT * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num 
	FROM bronze.crm_cust_info
) WHERE cst_id IS NOT NULL AND row_num = 1;


-- CRM PRODUCT INFO TABLE --------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
    prd_id, 
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- can be used to join with erp_px_cat_g1v2 table
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key, -- can be used to join with crm_sales_details table
    prd_nm, 
    coalesce(prd_cost, 0) AS prd_cost, 
    CASE WHEN UPPER(TRIM(prd_line))='M' then 'Mountain'
        WHEN UPPER(TRIM(prd_line))='R' then 'Road'
        WHEN UPPER(TRIM(prd_line))='T' then 'Touring'
        WHEN UPPER(TRIM(prd_line))='S' then 'Other Sales'
        ELSE 'n/a' 
    END AS prd_line_type,
    CAST(prd_start_dt AS DATE) as prd_start_dt, 
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) as prd_end_dt 
FROM bronze.crm_prd_info;


-- CRM SALES DETAILS TABLE --------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_quantity, sls_sales, sls_price)
SELECT 
    sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
        CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) <> 8 THEN NULL 
        ELSE to_date(sls_order_dt::text, 'YYYYMMDD')
    END AS sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) <> 8 THEN NULL 
        ELSE to_date(sls_ship_dt::text, 'YYYYMMDD')
    END AS sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) <> 8 THEN NULL 
        ELSE to_date(sls_due_dt::text, 'YYYYMMDD')
    END AS sls_due_dt, 
    sls_quantity, 
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
        END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price 
        END AS sls_price
FROM bronze.crm_sales_details;


-- ERP CUSTOMER TABLE --------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
SELECT 
    CASE WHEN cid like 'NAS%' 
        THEN SUBSTRING(cid, 4, LENGTH(cid)) 
        ELSE cid 
    END AS cid,
    CASE WHEN bdate > CURRENT_TIMESTAMP THEN NULL
        ELSE bdate
    END AS bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
         ELSE 'n/a' 
         END AS gen
FROM bronze.erp_cust_az12;


-- ERP LOCATION TABLE --------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' WHEN  TRIM(cntry) IN ('US', 'USA') THEN 'United States' WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' ELSE cntry END AS standardized_cntry
FROM bronze.erp_loc_a101;


-- ERP PRODUCT CATEGORY TABLE --------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2;