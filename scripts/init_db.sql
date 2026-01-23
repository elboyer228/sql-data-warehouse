-- Create main database and schemas bronze, silver, and gold
-- WARNING: This script will drop the database "datawarehouse" if it exists 
-- Must be run while connected to postgres 


DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

\connect datawarehouse

CREATE SCHEMA staging;
CREATE SCHEMA silver;
CREATE SCHEMA gold;