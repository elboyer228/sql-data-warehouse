-- Bronze dataset i.e. creating tables from source systems
-- WARNING: This script will drop the tables if they exist



-- CRM Tables 
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATE
); 


DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
   sls_ord_num VARCHAR(50),
   sls_prd_key VARCHAR(50),
   sls_cust_id INT,
   sls_order_dt INT,
   sls_ship_dt INT,
   sls_due_dt INT,
   sls_sales INT,
   sls_quantity INT,
   sls_price INT
);


-- ERP Tables 

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    CID VARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50)
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(50)
);



-- BULK LOAD DATA INTO BRONZE TABLES

-- CRM Tables
TRUNCATE TABLE bronze.crm_cust_info;
COPY bronze.crm_cust_info
FROM '/datasets/source_crm/cust_info.csv'
DELIMITER ','
CSV
HEADER;


TRUNCATE TABLE bronze.crm_prd_info;
COPY bronze.crm_prd_info
FROM '/datasets/source_crm/prd_info.csv'
DELIMITER ','
CSV
HEADER;

TRUNCATE TABLE bronze.crm_sales_details;
COPY bronze.crm_sales_details
FROM '/datasets/source_crm/sales_details.csv'
DELIMITER ','
CSV
HEADER;

-- ERP Tables
TRUNCATE TABLE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12
FROM '/datasets/source_erp/cust_az12.csv'
DELIMITER ','
CSV
HEADER;

TRUNCATE TABLE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101
FROM '/datasets/source_erp/loc_a101.csv'
DELIMITER ','
CSV
HEADER;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2
FROM '/datasets/source_erp/px_cat_g1v2.csv'
DELIMITER ','
CSV
HEADER;
