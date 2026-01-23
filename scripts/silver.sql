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
   sls_order_dt INT,
   sls_ship_dt INT,
   sls_due_dt INT,
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

-- QUALITY CHECKS OF BRONZE TABLES AND TRANSFORMATIONS INTO SILVER TABLES

-- CRM CUSTOMER INFO TABLE
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



-- CRM PRODUCT INFO TABLE

SELECT 