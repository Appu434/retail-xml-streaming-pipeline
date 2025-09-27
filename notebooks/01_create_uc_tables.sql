USE CATALOG retail;
USE SCHEMA core;

-- Bronze (semi-structured)
CREATE TABLE IF NOT EXISTS bronze_transactions
USING DELTA
LOCATION 'dbfs:/mnt/bronze/transactions';

-- Silver (normalized)
CREATE TABLE IF NOT EXISTS silver_orders
USING DELTA
LOCATION 'dbfs:/mnt/silver/orders';

CREATE TABLE IF NOT EXISTS silver_line_items
USING DELTA
LOCATION 'dbfs:/mnt/silver/line_items';

CREATE TABLE IF NOT EXISTS silver_payments
USING DELTA
LOCATION 'dbfs:/mnt/silver/payments';

-- Gold (aggregates)
CREATE TABLE IF NOT EXISTS gold_daily_store_sales
USING DELTA
LOCATION 'dbfs:/mnt/gold/daily_store_sales';
