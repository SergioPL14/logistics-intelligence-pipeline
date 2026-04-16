-- Initialize Logistics Data Warehouse schemas (medallion architecture)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Separate database/schema for Airflow metadata isolation is recommended;
-- here we keep Airflow in its own DB created by the airflow service init.
