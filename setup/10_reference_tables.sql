-- 10_reference_tables.sql
-- REFERENCE DDL of every table the pipeline produces (bronze -> silver -> gold).
--
-- !! READ-ONLY reference file — DO NOT execute against retail_dev !!
-- The pipeline (Lakeflow Spark Declarative Pipeline) owns and manages all tables below.
-- Running this file would create conflicting managed tables that cause pipeline failures.
-- This file documents the exact schema contract so you can:
--   (a) read every table definition in one place, and
--   (b) diff it against the real tables with 20_check_actual_schema.sql.
-- Decimal precisions (29,2 / 38,2 / 28,2) follow Spark's arithmetic rules; if the diff shows a
-- different precision on your runtime, take the real one.

CREATE CATALOG IF NOT EXISTS retail_dev;
CREATE SCHEMA  IF NOT EXISTS retail_dev.bronze;
CREATE SCHEMA  IF NOT EXISTS retail_dev.silver;
CREATE SCHEMA  IF NOT EXISTS retail_dev.gold;

-- =============================================================================================
-- BRONZE  : raw, all STRING, append-only, + _rescued_data / _source_file / _ingested_at
-- =============================================================================================
CREATE TABLE IF NOT EXISTS retail_dev.bronze.raw_orders (
  order_id STRING, customer_id STRING, product_id STRING, order_date STRING,
  quantity STRING, unit_price STRING,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
) USING DELTA COMMENT 'Raw orders (Auto Loader, csv)';

CREATE TABLE IF NOT EXISTS retail_dev.bronze.raw_customers_cdc (
  customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING,
  country STRING, loyalty_tier STRING, op STRING, event_ts STRING,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
) USING DELTA COMMENT 'Raw customer CDC events (op I/U/D)';

CREATE TABLE IF NOT EXISTS retail_dev.bronze.raw_products (
  product_id STRING, product_name STRING, category STRING, brand STRING,
  unit_cost STRING, list_price STRING, is_active STRING, updated_at STRING,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
) USING DELTA COMMENT 'Raw product delta feed';

CREATE TABLE IF NOT EXISTS retail_dev.bronze.raw_payments (
  payment_id STRING, order_id STRING, payment_method STRING, amount STRING,
  currency STRING, payment_status STRING, payment_ts STRING,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
) USING DELTA COMMENT 'Raw payments';

-- =============================================================================================
-- SILVER  : typed + validated (<feed>_clean) and rejected rows (<feed>_quarantine, + failed_rules)
-- =============================================================================================
CREATE TABLE IF NOT EXISTS retail_dev.silver.orders_clean (
  order_id BIGINT, customer_id STRING, product_id STRING, order_date DATE,
  quantity INT, unit_price DECIMAL(18,2),
  _source_file STRING, _ingested_at TIMESTAMP,
  total_amount DECIMAL(29,2), _rescued_data STRING
) USING DELTA CLUSTER BY (order_date) COMMENT 'Validated orders';

CREATE TABLE IF NOT EXISTS retail_dev.silver.orders_quarantine (
  order_id BIGINT, customer_id STRING, product_id STRING, order_date DATE,
  quantity INT, unit_price DECIMAL(18,2),
  _source_file STRING, _ingested_at TIMESTAMP,
  total_amount DECIMAL(29,2), _rescued_data STRING,
  failed_rules ARRAY<STRING>
) USING DELTA COMMENT 'Orders that failed validation';

CREATE TABLE IF NOT EXISTS retail_dev.silver.customers_cdc_clean (
  customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING,
  country STRING, loyalty_tier STRING, op STRING, event_ts TIMESTAMP,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
) USING DELTA COMMENT 'Validated customer CDC events';

CREATE TABLE IF NOT EXISTS retail_dev.silver.customers_cdc_quarantine (
  customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING,
  country STRING, loyalty_tier STRING, op STRING, event_ts TIMESTAMP,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP,
  failed_rules ARRAY<STRING>
) USING DELTA COMMENT 'Customer CDC events that failed validation';

CREATE TABLE IF NOT EXISTS retail_dev.silver.products_clean (
  product_id STRING, product_name STRING, category STRING, brand STRING,
  unit_cost DECIMAL(18,2), list_price DECIMAL(18,2), is_active BOOLEAN, updated_at TIMESTAMP,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
) USING DELTA COMMENT 'Validated product change events';

CREATE TABLE IF NOT EXISTS retail_dev.silver.products_quarantine (
  product_id STRING, product_name STRING, category STRING, brand STRING,
  unit_cost DECIMAL(18,2), list_price DECIMAL(18,2), is_active BOOLEAN, updated_at TIMESTAMP,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP,
  failed_rules ARRAY<STRING>
) USING DELTA COMMENT 'Product rows that failed validation';

CREATE TABLE IF NOT EXISTS retail_dev.silver.payments_clean (
  payment_id STRING, order_id BIGINT, payment_method STRING, amount DECIMAL(18,2),
  currency STRING, payment_status STRING, payment_ts TIMESTAMP,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP,
  payment_date DATE
) USING DELTA CLUSTER BY (payment_date) COMMENT 'Validated payments';

CREATE TABLE IF NOT EXISTS retail_dev.silver.payments_quarantine (
  payment_id STRING, order_id BIGINT, payment_method STRING, amount DECIMAL(18,2),
  currency STRING, payment_status STRING, payment_ts TIMESTAMP,
  _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP,
  payment_date DATE,
  failed_rules ARRAY<STRING>
) USING DELTA COMMENT 'Payments that failed validation';

-- =============================================================================================
-- GOLD  : dimensions (CDC targets) and business aggregates (materialized views)
-- =============================================================================================
-- SCD Type 2 (streaming table fed by AUTO CDC). __START_AT/__END_AT have the type of sequence_by.
CREATE TABLE IF NOT EXISTS retail_dev.gold.dim_customer (
  customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING,
  country STRING, loyalty_tier STRING,
  __START_AT TIMESTAMP, __END_AT TIMESTAMP
) USING DELTA COMMENT 'Customer dimension, SCD2';

CREATE TABLE IF NOT EXISTS retail_dev.gold.dim_customer_current (
  customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING,
  country STRING, loyalty_tier STRING,
  __START_AT TIMESTAMP, __END_AT TIMESTAMP
) USING DELTA COMMENT 'Current rows of dim_customer (materialized view)';

-- SCD Type 1 (current state only)
CREATE TABLE IF NOT EXISTS retail_dev.gold.dim_product (
  product_id STRING, product_name STRING, category STRING, brand STRING,
  unit_cost DECIMAL(18,2), list_price DECIMAL(18,2), is_active BOOLEAN, updated_at TIMESTAMP
) USING DELTA COMMENT 'Product dimension, SCD1';

CREATE TABLE IF NOT EXISTS retail_dev.gold.daily_sales_mv (
  order_date DATE, order_count BIGINT, customer_count BIGINT, units_sold BIGINT, revenue DECIMAL(38,2)
) USING DELTA COMMENT 'Daily sales KPIs (materialized view)';

CREATE TABLE IF NOT EXISTS retail_dev.gold.daily_payments_mv (
  payment_date DATE, payment_method STRING, payment_count BIGINT,
  collected_amount DECIMAL(28,2), refunded_amount DECIMAL(28,2), failed_count BIGINT
) USING DELTA COMMENT 'Payments per day and method (materialized view)';

CREATE TABLE IF NOT EXISTS retail_dev.gold.order_payment_status_mv (
  order_id BIGINT, customer_id STRING, order_date DATE, total_amount DECIMAL(29,2),
  paid_amount DECIMAL(28,2), payment_status STRING
) USING DELTA COMMENT 'Order total vs paid amount: PAID / PARTIAL / UNPAID (materialized view)';
