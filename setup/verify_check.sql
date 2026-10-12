-- ============================================================
-- verify_check.sql — post-pipeline verification for all 4 datamarts
-- Run after each pipeline update (replace retail_dev if needed)
-- ============================================================

-- 1) Quarantine counts for ALL 12 feeds across 4 datamarts
--    (compare with EXPECTED line printed by generate_landing_data.py)
SELECT 'orders'              AS feed, count(*) AS quarantined FROM retail_dev.silver.orders_quarantine
UNION ALL SELECT 'order_items',        count(*) FROM retail_dev.silver.order_items_quarantine
UNION ALL SELECT 'order_returns',      count(*) FROM retail_dev.silver.order_returns_quarantine
UNION ALL SELECT 'customers_cdc',      count(*) FROM retail_dev.silver.customers_cdc_quarantine
UNION ALL SELECT 'customer_addresses', count(*) FROM retail_dev.silver.customer_addresses_quarantine
UNION ALL SELECT 'customer_preferences', count(*) FROM retail_dev.silver.customer_preferences_quarantine
UNION ALL SELECT 'payments',           count(*) FROM retail_dev.silver.payments_quarantine
UNION ALL SELECT 'payment_refunds',   count(*) FROM retail_dev.silver.payment_refunds_quarantine
UNION ALL SELECT 'payment_methods',   count(*) FROM retail_dev.silver.payment_methods_quarantine
UNION ALL SELECT 'products',          count(*) FROM retail_dev.silver.products_quarantine
UNION ALL SELECT 'product_categories', count(*) FROM retail_dev.silver.product_categories_quarantine
UNION ALL SELECT 'product_suppliers', count(*) FROM retail_dev.silver.product_suppliers_quarantine;

-- Which DQ rules fired (sample: orders datamart)
SELECT explode(failed_rules) AS rule, count(*) AS n
FROM retail_dev.silver.orders_quarantine GROUP BY 1 ORDER BY 2 DESC;

-- 2) Rescued data: extra columns that landed in _rescued_data (all feeds with explicit schemas)
SELECT 'customers_cdc' AS feed, _source_file, count(*) AS rows_with_rescued, any_value(_rescued_data) AS sample
FROM retail_dev.bronze.raw_customers_cdc WHERE _rescued_data IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT 'customer_addresses', _source_file, count(*), any_value(_rescued_data)
FROM retail_dev.bronze.raw_customer_addresses WHERE _rescued_data IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT 'customer_preferences', _source_file, count(*), any_value(_rescued_data)
FROM retail_dev.bronze.raw_customer_preferences WHERE _rescued_data IS NOT NULL GROUP BY 1, 2;

SELECT 'payments' AS feed, _source_file, count(*) AS rows_with_rescued, any_value(_rescued_data) AS sample
FROM retail_dev.bronze.raw_payments WHERE _rescued_data IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT 'payment_refunds', _source_file, count(*), any_value(_rescued_data)
FROM retail_dev.bronze.raw_payment_refunds WHERE _rescued_data IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT 'payment_methods', _source_file, count(*), any_value(_rescued_data)
FROM retail_dev.bronze.raw_payment_methods WHERE _rescued_data IS NOT NULL GROUP BY 1, 2;

SELECT 'products' AS feed, _source_file, count(*) AS rows_with_rescued, any_value(_rescued_data) AS sample
FROM retail_dev.bronze.raw_products WHERE _rescued_data IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT 'product_categories', _source_file, count(*), any_value(_rescued_data)
FROM retail_dev.bronze.raw_product_categories WHERE _rescued_data IS NOT NULL GROUP BY 1, 2
UNION ALL
SELECT 'product_suppliers', _source_file, count(*), any_value(_rescued_data)
FROM retail_dev.bronze.raw_product_suppliers WHERE _rescued_data IS NOT NULL GROUP BY 1, 2;

-- 3) Silver clean row counts per datamart (should match generator's EXPECTED line)
SELECT 'orders'              AS feed, count(*) AS clean_rows FROM retail_dev.silver.orders_clean
UNION ALL SELECT 'order_items',        count(*) FROM retail_dev.silver.order_items_clean
UNION ALL SELECT 'order_returns',      count(*) FROM retail_dev.silver.order_returns_clean
UNION ALL SELECT 'customers_cdc',      count(*) FROM retail_dev.silver.customers_cdc_clean
UNION ALL SELECT 'customer_addresses', count(*) FROM retail_dev.silver.customer_addresses_clean
UNION ALL SELECT 'customer_preferences', count(*) FROM retail_dev.silver.customer_preferences_clean
UNION ALL SELECT 'payments',           count(*) FROM retail_dev.silver.payments_clean
UNION ALL SELECT 'payment_refunds',   count(*) FROM retail_dev.silver.payment_refunds_clean
UNION ALL SELECT 'payment_methods',   count(*) FROM retail_dev.silver.payment_methods_clean
UNION ALL SELECT 'products',          count(*) FROM retail_dev.silver.products_clean
UNION ALL SELECT 'product_categories', count(*) FROM retail_dev.silver.product_categories_clean
UNION ALL SELECT 'product_suppliers', count(*) FROM retail_dev.silver.product_suppliers_clean;

-- ============================================================
-- GOLD LAYER — Customers datamart
-- ============================================================

-- 4) SCD2 customer dimension: total versions, closed versions, current versions
SELECT 'total' AS category,   count(*) AS n FROM retail_dev.gold.dim_customer
UNION ALL SELECT 'closed',    count(*) FROM retail_dev.gold.dim_customer WHERE __END_AT IS NOT NULL
UNION ALL SELECT 'current',   count(*) FROM retail_dev.gold.dim_customer_current;

-- SCD2 history for a specific customer (set the id printed by the generator as "Late out-of-order event")
-- SELECT customer_id, city, loyalty_tier, __START_AT, __END_AT
-- FROM retail_dev.gold.dim_customer WHERE customer_id = '<late_customer_id>' ORDER BY __START_AT;

-- 5) Customer 360: current customers with current address
SELECT count(*) AS customer_360_rows FROM retail_dev.gold.customer_360_mv;
SELECT loyalty_tier, count(*) FROM retail_dev.gold.customer_360_mv GROUP BY 1 ORDER BY 2 DESC;

-- ============================================================
-- GOLD LAYER — Orders datamart
-- ============================================================

-- 6) Daily sales KPIs
SELECT count(*) AS daily_sales_rows FROM retail_dev.gold.daily_sales_mv;
SELECT order_date, order_count, customer_count, units_sold, revenue
FROM retail_dev.gold.daily_sales_mv ORDER BY order_date DESC LIMIT 10;

-- 7) Order summary: orders with line item counts and return stats
SELECT count(*) AS order_summary_rows FROM retail_dev.gold.order_summary_mv;
SELECT order_id, total_amount, item_count, item_total, return_count, refund_total, net_revenue
FROM retail_dev.gold.order_summary_mv ORDER BY net_revenue DESC LIMIT 10;

-- ============================================================
-- GOLD LAYER — Payments datamart
-- ============================================================

-- 8) Payment summary: payments with refund info
SELECT count(*) AS payment_summary_rows FROM retail_dev.gold.payment_summary_mv;
SELECT payment_id, amount, refunded_amount, refund_count, net_amount
FROM retail_dev.gold.payment_summary_mv ORDER BY net_amount DESC LIMIT 10;

-- 9) Daily payments by method
SELECT count(*) AS daily_payments_rows FROM retail_dev.gold.daily_payments_mv;
SELECT payment_date, payment_method, payment_count, collected_amount, refunded_amount, failed_count
FROM retail_dev.gold.daily_payments_mv ORDER BY payment_date DESC LIMIT 10;

-- ============================================================
-- GOLD LAYER — Products datamart
-- ============================================================

-- 10) SCD1 product dimension: one row per product, soft-deleted flagged
SELECT is_active, count(*) FROM retail_dev.gold.dim_product GROUP BY 1;

-- 11) Product catalog: products with category hierarchy
SELECT count(*) AS product_catalog_rows FROM retail_dev.gold.product_catalog_mv;
SELECT product_id, product_name, category, brand, unit_cost, list_price, is_active, parent_category
FROM retail_dev.gold.product_catalog_mv LIMIT 10;

-- ============================================================
-- CROSS-DATAMART RECONCILIATION
-- ============================================================

-- 12) Order payment reconciliation
SELECT payment_status, count(*) AS order_count
FROM retail_dev.gold.order_payment_status_mv GROUP BY 1 ORDER BY 2 DESC;

-- 13) Revenue vs collected: total order revenue vs total collected payments
SELECT
  (SELECT sum(revenue)      FROM retail_dev.gold.daily_sales_mv)        AS total_revenue,
  (SELECT sum(collected_amount) FROM retail_dev.gold.daily_payments_mv) AS total_collected;

-- ============================================================
-- REFERENCE TABLES
-- ============================================================

-- 14) Reference table row counts
SELECT 'dq_rules' AS ref_table, count(*) AS rows FROM retail_dev.ops.dq_rules
UNION ALL SELECT 'column_transforms', count(*) FROM retail_dev.ops.column_transforms
UNION ALL SELECT 'feed_config',       count(*) FROM retail_dev.ops.feed_config;

-- 15) DQ rules per feed
SELECT feed_name, count(*) AS rule_count FROM retail_dev.ops.dq_rules GROUP BY 1 ORDER BY 1;

-- 16) Column transforms per feed
SELECT feed_name, stage, count(*) AS transform_count
FROM retail_dev.ops.column_transforms GROUP BY 1, 2 ORDER BY 1, 2;

-- 17) Feed config overview
SELECT feed_name, load_pattern, bronze_table, silver_table, gold_table
FROM retail_dev.ops.feed_config ORDER BY 1;
