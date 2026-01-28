-- SILVER DATASET i.e. creating tables from bronze tables
-- WARNING: This script will drop the tables if they exist
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE, 
    dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
); 


DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE, 
    dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
   sls_ord_num VARCHAR(50),
   sls_prd_key VARCHAR(50),
   sls_cust_id INT,
   sls_order_dt DATE,
   sls_ship_dt DATE,
   sls_due_dt DATE,
   sls_sales INT,
   sls_quantity INT,
   sls_price INT,
   dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ERP Tables 
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50),
    dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50),
    dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50),
    dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- QUALITY CHECKS OF BRONZE TABLES AND TRANSFORMATIONS INTO SILVER TABLES --------------------------------------------------------------------------------------------------------------------------------------

-- CRM CUSTOMER INFO TABLE --------------------------------------------------------------------------------------------------------------------------------------
-- Check for duplicate primary keys
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Get only the latest record for each primary key
SELECT * FROM (SELECT * , ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_num FROM bronze.crm_cust_info) WHERE cst_id IS NOT NULL AND row_num = 1;

-- Clean unwanted spaces in String Variables 

SELECT cst_firstname 
FROM bronze.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info 
WHERE cst_lastname != TRIM(cst_lastname);

-- Gender and Marital Status are not standardized, so we need to standardize them
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;



-- FINAL SILVER TABLES
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
-- Check duplicate or null of primary key
SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info GROUP BY prd_id HAVING COUNT(*) >1 or prd_id = NULL;

-- Insert into silver.crm_prd_info table
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


-- WHERE SUBSTRING(prd_key, 7, LENGTH(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details);
-- WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (SELECT distinct id from bronze.erp_px_cat_g1v2);

-- Check for null or negative values in prd_cost
SELECT prd_cost from bronze.crm_prd_info WHERE prd_cost IS NULL or prd_cost < 0;

-- Check for invalid dates in prd_start_dt and prd_end_dt
SELECT * from bronze.crm_prd_info WHERE prd_start_dt IS NULL or prd_end_dt IS NULL or prd_start_dt > prd_end_dt;
-- We do have issues with some dates (end before start and overlapping dates)

SELECT 
prd_id, 
prd_key, 
prd_nm, 
prd_cost, 
prd_line, 
prd_start_dt, 
prd_end_dt, 
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as next_prd_start_dt
FROM bronze.crm_prd_info 
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');






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



SELECT * FROM bronze.crm_sales_details WHERE sls_ord_num IS NULL OR sls_ord_num != TRIM(sls_ord_num);

SELECT * FROM bronze.crm_sales_details WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info); -- 0 rows 
SELECT * FROM bronze.crm_sales_details WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info); -- 0 rows

-- Change INT TO DATE for sls_order_dt, sls_ship_dt, sls_due_dt

SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt FROM bronze.crm_sales_details WHERE sls_order_dt <= 0; -- Change 0 rows to NULL values 
SELECT sls_order_dt FROM bronze.crm_sales_details WHERE LENGTH(sls_order_dt::text) <> 8; -- Check correct format of date
SELECT sls_order_dt FROM bronze.crm_sales_details WHERE sls_order_dt > 20250101 OR sls_order_dt < 19000101; -- Check maximum date

-- Check order date smaller than ship date
SELECT * FROM bronze.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt; -- 0 rows

-- Check sls_sales, sls_quantity, sls_price
SELECT DISTINCT sls_sales, sls_quantity, sls_price 
FROM bronze.crm_sales_details 
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price; 

-- Here both sls_sales and sls_price are bad quality data, so we need to either talk to the buisness/source system owner to fix the data or we need to fix it ouselves based on rules we fix. 
-- Let's fix it ourselves based on rules we find.
-- If Sales is negative, zero or null, derive it using quantity and price
-- If Price is zero or null, derive it using quantity and sales
-- If Price is negative, convert it to positive value 


SELECT sls_quantity, sls_price, sls_sales,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
        END AS corrected_sls_sales
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price);


SELECT 
 sls_sales as old_sls_sales,
    sls_quantity, 
    sls_price as old_sls_price,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
        END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price 
        END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
ORDER BY sls_sales;



---- ERP CUSTOMER TABLE --------------------------------------------------------------------------------------------------------------------------------------
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

SELECT * FROM silver.crm_cust_info; -- Cid can be join on cst_key, need to remove NAS 


SELECT
    cid,
    bdate,
    gen
FROM (
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
            ELSE cid
        END AS cid,
        bdate,
        gen
    FROM bronze.erp_cust_az12
)
WHERE cid NOT IN (
    SELECT DISTINCT cst_key
    FROM silver.crm_cust_info
);

-- Check for range of dates in bdate
SELECT MIN(bdate), MAX(bdate) FROM bronze.erp_cust_az12; -- 1916-02-10 | 9999-11-20
SELECT bdate FROM bronze.erp_cust_az12 WHERE bdate > CURRENT_TIMESTAMP;

SELECT DISTINCT gen FROM bronze.erp_cust_az12; -- F, M, n/a, Male and Female -> bad quality data 
SELECT DISTINCT 
       gen , 
       CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
            ELSE 'n/a' 
            END AS standardized_gen
FROM bronze.erp_cust_az12;



-- ERP LOCATION TABLE --------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM bronze.erp_loc_a101; -- Cid can be join on cst_key, need to remove '-'
SELECT REPLACE(cid, '-', '') AS cid FROM bronze.erp_loc_a101 WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info); -- No unmatching data between both table

SELECT DISTINCT cntry FROM bronze.erp_loc_a101 ORDER BY cntry; -- Mix of countries names (US, USA, United States), need to standardize
SELECT DISTINCT cntry, CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' WHEN  TRIM(cntry) IN ('US', 'USA') THEN 'United States' WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' ELSE cntry END AS standardized_cntry
FROM bronze.erp_loc_a101;

TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101(cid, cntry)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany' WHEN  TRIM(cntry) IN ('US', 'USA') THEN 'United States' WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a' ELSE cntry END AS standardized_cntry
FROM bronze.erp_loc_a101;

-- ERP PRODUCT CATEGORY TABLE --------------------------------------------------------------------------------------------------------------------------------------

SELECT * FROM bronze.erp_px_cat_g1v2; -- Id can be join on cat_id added from prd_key in crm_prd_info table
SELECT cat FROM bronze.erp_px_cat_g1v2 WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance); -- No unwanted spaces in String Variables 

SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2 ORDER BY cat; 
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2 ORDER BY subcat; 
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2 ORDER BY maintenance; 


TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2;