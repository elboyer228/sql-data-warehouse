-- Must be run while connected to postgres 
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

\connect datawarehouse

CREATE SCHEMA staging;
CREATE SCHEMA silver;
CREATE SCHEMA gold;