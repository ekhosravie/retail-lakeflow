-- 00_setup_real.sql  -  objects you create BY HAND in the real catalog (safe to re-run)
-- Pipeline tables (bronze/silver/gold) are NOT created here: Lakeflow creates them.

CREATE CATALOG IF NOT EXISTS retail_dev;

CREATE SCHEMA IF NOT EXISTS retail_dev.landing;
CREATE SCHEMA IF NOT EXISTS retail_dev.bronze;
CREATE SCHEMA IF NOT EXISTS retail_dev.silver;
CREATE SCHEMA IF NOT EXISTS retail_dev.gold;
CREATE SCHEMA IF NOT EXISTS retail_dev.ops;

CREATE VOLUME IF NOT EXISTS retail_dev.landing.orders;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.customers_cdc;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.products;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.payments;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.order_items;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.order_returns;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.customer_addresses;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.customer_preferences;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.product_categories;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.product_suppliers;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.payment_refunds;
CREATE VOLUME IF NOT EXISTS retail_dev.landing.payment_methods;

-- ---------------------------------------------------------------------------------------------
-- ops.feed_config : one row per feed. NOT read by the pipeline yet (it mirrors what is hard-coded
-- in src/*). Level 12 (metadata-driven) will generate bronze/silver/gold flows from this table.
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dev.ops.feed_config (
  feed_name         STRING NOT NULL COMMENT 'logical feed name',
  source_format     STRING          COMMENT 'csv | json | parquet',
  landing_volume    STRING          COMMENT 'schema.volume under the catalog',
  bronze_table      STRING,
  silver_table      STRING,
  quarantine_table  STRING,
  gold_table        STRING,
  load_pattern      STRING          COMMENT 'append | cdc_scd1 | cdc_scd2',
  key_columns       ARRAY<STRING>   COMMENT 'business key(s)',
  sequence_column   STRING          COMMENT 'ordering column for CDC',
  delete_condition  STRING          COMMENT 'SQL expression marking delete events',
  is_enabled        BOOLEAN,
  CONSTRAINT pk_feed_config PRIMARY KEY (feed_name)
) USING DELTA
COMMENT 'Control table: one row per feed (reference for the metadata-driven level)';

-- ---------------------------------------------------------------------------------------------
-- ops.dq_rules : the same rules that are hard-coded in src/silver/*.py (raw, NOT null-safe form;
-- the code wraps each in COALESCE(rule, FALSE)).
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dev.ops.dq_rules (
  feed_name      STRING NOT NULL,
  rule_name      STRING NOT NULL,
  rule_expr      STRING NOT NULL COMMENT 'SQL boolean expression, true = row is valid',
  action         STRING          COMMENT 'drop | fail | warn',
  is_enabled     BOOLEAN,
  CONSTRAINT pk_dq_rules PRIMARY KEY (feed_name, rule_name)
) USING DELTA
COMMENT 'Data-quality rules per feed (reference for the metadata-driven level)';

-- ---------------------------------------------------------------------------------------------
-- Seed data (idempotent: INSERT OVERWRITE)
-- ---------------------------------------------------------------------------------------------
INSERT OVERWRITE retail_dev.ops.feed_config VALUES
 ('orders',        'csv', 'landing.orders',        'bronze.raw_orders',        'silver.orders_clean',        'silver.orders_quarantine',        'gold.daily_sales_mv',  'append',   array('order_id'),    NULL,         NULL,        TRUE),
 ('customers_cdc', 'csv', 'landing.customers_cdc', 'bronze.raw_customers_cdc', 'silver.customers_cdc_clean', 'silver.customers_cdc_quarantine', 'gold.dim_customer',    'cdc_scd2', array('customer_id'), 'event_ts',   "op = 'D'",  TRUE),
 ('products',      'csv', 'landing.products',      'bronze.raw_products',      'silver.products_clean',      'silver.products_quarantine',      'gold.dim_product',     'cdc_scd1', array('product_id'),  'updated_at', NULL,        TRUE),
 ('payments',      'csv', 'landing.payments',      'bronze.raw_payments',      'silver.payments_clean',      'silver.payments_quarantine',      'gold.daily_payments_mv','append',  array('payment_id'),  NULL,         NULL,        TRUE),
 ('order_items',         'csv', 'landing.order_items',         'bronze.raw_order_items',         'silver.order_items_clean',         'silver.order_items_quarantine',         'gold.order_summary_mv',  'append', array('order_item_id'), NULL,      NULL, TRUE),
 ('order_returns',       'csv', 'landing.order_returns',       'bronze.raw_order_returns',       'silver.order_returns_clean',       'silver.order_returns_quarantine',       'gold.order_summary_mv',  'append', array('return_id'),    NULL,      NULL, TRUE),
 ('customer_addresses',  'csv', 'landing.customer_addresses',  'bronze.raw_customer_addresses',  'silver.customer_addresses_clean',  'silver.customer_addresses_quarantine',  'gold.customer_360_mv',   'append', array('address_id'),  NULL,      NULL, TRUE),
 ('customer_preferences','csv', 'landing.customer_preferences','bronze.raw_customer_preferences','silver.customer_preferences_clean','silver.customer_preferences_quarantine','gold.customer_360_mv',   'append', array('preference_id'), NULL,     NULL, TRUE),
 ('product_categories',  'csv', 'landing.product_categories',  'bronze.raw_product_categories',  'silver.product_categories_clean',  'silver.product_categories_quarantine',  'gold.product_catalog_mv','append', array('category_id'),  NULL,      NULL, TRUE),
 ('product_suppliers',   'csv', 'landing.product_suppliers',   'bronze.raw_product_suppliers',   'silver.product_suppliers_clean',   'silver.product_suppliers_quarantine',   'gold.product_catalog_mv','append', array('supplier_id'),  NULL,      NULL, TRUE),
 ('payment_refunds',     'csv', 'landing.payment_refunds',     'bronze.raw_payment_refunds',     'silver.payment_refunds_clean',     'silver.payment_refunds_quarantine',     'gold.payment_summary_mv','append', array('refund_id'),    NULL,      NULL, TRUE),
 ('payment_methods',     'csv', 'landing.payment_methods',     'bronze.raw_payment_methods',     'silver.payment_methods_clean',     'silver.payment_methods_quarantine',     'gold.payment_summary_mv','append', array('method_id'),     NULL,      NULL, TRUE);

INSERT OVERWRITE retail_dev.ops.dq_rules VALUES
 ('orders',        'order_id_present',   'order_id IS NOT NULL',  'drop', TRUE),
 ('orders',        'valid_date',         'order_date IS NOT NULL','drop', TRUE),
 ('orders',        'qty_positive',       'quantity > 0',          'drop', TRUE),
 ('orders',        'price_non_neg',      'unit_price >= 0',       'drop', TRUE),
 ('customers_cdc', 'customer_id_present','customer_id IS NOT NULL','drop', TRUE),
 ('customers_cdc', 'valid_op',           "op IN ('I','U','D')",   'drop', TRUE),
 ('customers_cdc', 'valid_event_ts',     'event_ts IS NOT NULL',  'drop', TRUE),
 ('customers_cdc', 'valid_email',        "op = 'D' OR email LIKE '%@%'", 'drop', TRUE),
 ('products',      'product_id_present', 'product_id IS NOT NULL','drop', TRUE),
 ('products',      'name_present',       'product_name IS NOT NULL','drop', TRUE),
 ('products',      'unit_cost_valid',    'unit_cost >= 0',        'drop', TRUE),
 ('products',      'list_price_valid',   'list_price >= 0',       'drop', TRUE),
 ('products',      'is_active_valid',    'is_active IS NOT NULL', 'drop', TRUE),
 ('products',      'updated_at_valid',   'updated_at IS NOT NULL','drop', TRUE),
 ('payments',      'payment_id_present', 'payment_id IS NOT NULL','drop', TRUE),
 ('payments',      'order_id_valid',     'order_id IS NOT NULL',  'drop', TRUE),
 ('payments',      'amount_positive',    'amount > 0',            'drop', TRUE),
 ('payments',      'valid_status',       "payment_status IN ('SUCCESS','FAILED','PENDING','REFUNDED')", 'drop', TRUE),
 ('payments',      'valid_method',       "payment_method IN ('CARD','CASH','WALLET','BANK_TRANSFER')",  'drop', TRUE),
 ('payments',      'payment_ts_valid',   'payment_ts IS NOT NULL','drop', TRUE),
 ('order_items',        'item_id_present',    'order_item_id IS NOT NULL','drop', TRUE),
 ('order_items',        'order_id_valid',     'order_id IS NOT NULL',  'drop', TRUE),
 ('order_items',        'qty_positive',       'quantity > 0',          'drop', TRUE),
 ('order_items',        'price_non_neg',      'unit_price >= 0',       'drop', TRUE),
 ('order_returns',      'return_id_present',  'return_id IS NOT NULL', 'drop', TRUE),
 ('order_returns',      'order_id_valid',     'order_id IS NOT NULL',  'drop', TRUE),
 ('order_returns',      'refund_non_neg',     'refund_amount >= 0',    'drop', TRUE),
 ('order_returns',      'return_date_valid',  'return_date IS NOT NULL','drop', TRUE),
 ('customer_addresses', 'address_id_present', 'address_id IS NOT NULL','drop', TRUE),
 ('customer_addresses', 'customer_id_valid',  'customer_id IS NOT NULL','drop', TRUE),
 ('customer_addresses', 'city_present',       'city IS NOT NULL',      'drop', TRUE),
 ('customer_preferences','pref_id_present',   'preference_id IS NOT NULL','drop', TRUE),
 ('customer_preferences','cust_id_valid',     'customer_id IS NOT NULL','drop', TRUE),
 ('customer_preferences','pref_key_present',  'preference_key IS NOT NULL','drop', TRUE),
 ('product_categories', 'category_id_present','category_id IS NOT NULL','drop', TRUE),
 ('product_categories', 'category_name_present','category_name IS NOT NULL','drop', TRUE),
 ('product_suppliers',  'supplier_id_present','supplier_id IS NOT NULL','drop', TRUE),
 ('product_suppliers',  'supplier_name_present','supplier_name IS NOT NULL','drop', TRUE),
 ('payment_refunds',    'refund_id_present', 'refund_id IS NOT NULL', 'drop', TRUE),
 ('payment_refunds',    'payment_id_valid',   'payment_id IS NOT NULL', 'drop', TRUE),
 ('payment_refunds',    'refund_amount_positive','refund_amount > 0','drop', TRUE),
 ('payment_refunds',    'refund_date_valid', 'refund_date IS NOT NULL','drop', TRUE),
 ('payment_methods',    'method_id_present',  'method_id IS NOT NULL', 'drop', TRUE),
 ('payment_methods',    'method_name_present','method_name IS NOT NULL','drop', TRUE);

-- ---------------------------------------------------------------------------------------------
-- ops.column_transforms : per-column transform expressions for each feed's silver-layer typed view.
-- The silver Python files read this table at module load time and build their .select() and
-- .withColumn() calls dynamically — no hardcoded transforms in the code.
-- stage = 'select'  → applied in the main .select() list (must reference raw bronze columns)
-- stage = 'derived' → applied with .withColumn() AFTER the select (can reference select aliases)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS retail_dev.ops.column_transforms (
  feed_name       STRING NOT NULL,
  column_name     STRING NOT NULL COMMENT 'output column alias',
  transform_expr  STRING NOT NULL COMMENT 'SQL expression producing the column value',
  position        INT    NOT NULL COMMENT 'ordering within the stage',
  stage           STRING NOT NULL COMMENT 'select | derived',
  CONSTRAINT pk_column_transforms PRIMARY KEY (feed_name, column_name)
) USING DELTA
COMMENT 'Per-column transform expressions for silver-layer typed views (metadata-driven)';

INSERT OVERWRITE retail_dev.ops.column_transforms VALUES
 -- orders
 ('orders', 'order_id',      'try_cast(order_id AS BIGINT)',          1, 'select'),
 ('orders', 'customer_id',   'trim(customer_id)',                     2, 'select'),
 ('orders', 'product_id',    'trim(product_id)',                      3, 'select'),
 ('orders', 'order_date',    'try_cast(order_date AS DATE)',          4, 'select'),
 ('orders', 'quantity',      'try_cast(quantity AS INT)',             5, 'select'),
 ('orders', 'unit_price',    'try_cast(unit_price AS DECIMAL(18,2))', 6, 'select'),
 ('orders', '_rescued_data', '_rescued_data',                        7, 'select'),
 ('orders', '_source_file',  '_source_file',                          8, 'select'),
 ('orders', '_ingested_at',  '_ingested_at',                          9, 'select'),
 ('orders', 'total_amount',  'quantity * unit_price',                 1, 'derived'),
 -- customers_cdc
 ('customers_cdc', 'customer_id',   'trim(customer_id)',              1, 'select'),
 ('customers_cdc', 'first_name',    'initcap(trim(first_name))',     2, 'select'),
 ('customers_cdc', 'last_name',     'initcap(trim(last_name))',      3, 'select'),
 ('customers_cdc', 'email',        'lower(trim(email))',            4, 'select'),
 ('customers_cdc', 'city',         'trim(city)',                    5, 'select'),
 ('customers_cdc', 'country',      'upper(trim(country))',         6, 'select'),
 ('customers_cdc', 'loyalty_tier', 'upper(trim(loyalty_tier))',      7, 'select'),
 ('customers_cdc', 'op',           'upper(trim(op))',               8, 'select'),
 ('customers_cdc', 'event_ts',     'try_cast(event_ts AS TIMESTAMP)', 9, 'select'),
 ('customers_cdc', '_rescued_data','_rescued_data',                 10, 'select'),
 ('customers_cdc', '_source_file', '_source_file',                 11, 'select'),
 ('customers_cdc', '_ingested_at', '_ingested_at',                 12, 'select'),
 -- products
 ('products', 'product_id',    'trim(product_id)',                      1, 'select'),
 ('products', 'product_name',  'trim(product_name)',                   2, 'select'),
 ('products', 'category',      'trim(category)',                      3, 'select'),
 ('products', 'brand',         'trim(brand)',                         4, 'select'),
 ('products', 'unit_cost',     'try_cast(unit_cost AS DECIMAL(18,2))',  5, 'select'),
 ('products', 'list_price',    'try_cast(list_price AS DECIMAL(18,2))', 6, 'select'),
 ('products', 'is_active',     'try_cast(lower(trim(is_active)) AS BOOLEAN)', 7, 'select'),
 ('products', 'updated_at',    'try_cast(updated_at AS TIMESTAMP)',    8, 'select'),
 ('products', '_rescued_data', '_rescued_data',                        9, 'select'),
 ('products', '_source_file',  '_source_file',                        10, 'select'),
 ('products', '_ingested_at',  '_ingested_at',                        11, 'select'),
 -- payments
 ('payments', 'payment_id',     'trim(payment_id)',                     1, 'select'),
 ('payments', 'order_id',       'try_cast(order_id AS BIGINT)',          2, 'select'),
 ('payments', 'payment_method', 'upper(trim(payment_method))',          3, 'select'),
 ('payments', 'amount',         'try_cast(amount AS DECIMAL(18,2))',     4, 'select'),
 ('payments', 'currency',       'upper(trim(currency))',               5, 'select'),
 ('payments', 'payment_status', 'upper(trim(payment_status))',          6, 'select'),
 ('payments', 'payment_ts',     'try_cast(payment_ts AS TIMESTAMP)',     7, 'select'),
 ('payments', '_rescued_data',  '_rescued_data',                       8, 'select'),
 ('payments', '_source_file',   '_source_file',                        9, 'select'),
 ('payments', '_ingested_at',   '_ingested_at',                       10, 'select'),
 ('payments', 'payment_date',   'to_date(payment_ts)',                  1, 'derived'),
 -- order_items
 ('order_items', 'order_item_id', 'try_cast(order_item_id AS BIGINT)',    1, 'select'),
 ('order_items', 'order_id',      'try_cast(order_id AS BIGINT)',          2, 'select'),
 ('order_items', 'product_id',   'trim(product_id)',                     3, 'select'),
 ('order_items', 'quantity',     'try_cast(quantity AS INT)',            4, 'select'),
 ('order_items', 'unit_price',  'try_cast(unit_price AS DECIMAL(18,2))',5, 'select'),
 ('order_items', '_rescued_data','_rescued_data',                       6, 'select'),
 ('order_items', '_source_file', '_source_file',                        7, 'select'),
 ('order_items', '_ingested_at', '_ingested_at',                        8, 'select'),
 ('order_items', 'line_total',  'quantity * unit_price',                1, 'derived'),
 -- order_returns
 ('order_returns', 'return_id',     'trim(return_id)',                     1, 'select'),
 ('order_returns', 'order_id',      'try_cast(order_id AS BIGINT)',          2, 'select'),
 ('order_returns', 'product_id',   'trim(product_id)',                    3, 'select'),
 ('order_returns', 'return_date',  'try_cast(return_date AS DATE)',        4, 'select'),
 ('order_returns', 'return_reason','trim(return_reason)',                 5, 'select'),
 ('order_returns', 'refund_amount','try_cast(refund_amount AS DECIMAL(18,2))',6, 'select'),
 ('order_returns', '_rescued_data','_rescued_data',                       7, 'select'),
 ('order_returns', '_source_file', '_source_file',                        8, 'select'),
 ('order_returns', '_ingested_at', '_ingested_at',                        9, 'select'),
 -- customer_addresses
 ('customer_addresses', 'address_id',   'trim(address_id)',                1, 'select'),
 ('customer_addresses', 'customer_id',  'trim(customer_id)',                2, 'select'),
 ('customer_addresses', 'address_type','upper(trim(address_type))',        3, 'select'),
 ('customer_addresses', 'street',      'trim(street)',                    4, 'select'),
 ('customer_addresses', 'city',        'trim(city)',                      5, 'select'),
 ('customer_addresses', 'country',     'upper(trim(country))',             6, 'select'),
 ('customer_addresses', 'is_current', 'try_cast(lower(trim(is_current)) AS BOOLEAN)', 7, 'select'),
 ('customer_addresses', 'updated_at', 'try_cast(updated_at AS TIMESTAMP)',8, 'select'),
 ('customer_addresses', '_rescued_data','_rescued_data',                  9, 'select'),
 ('customer_addresses', '_source_file', '_source_file',                  10, 'select'),
 ('customer_addresses', '_ingested_at', '_ingested_at',                  11, 'select'),
 -- customer_preferences
 ('customer_preferences', 'preference_id',  'trim(preference_id)',           1, 'select'),
 ('customer_preferences', 'customer_id',   'trim(customer_id)',             2, 'select'),
 ('customer_preferences', 'preference_key','upper(trim(preference_key))',   3, 'select'),
 ('customer_preferences', 'preference_value','trim(preference_value)',      4, 'select'),
 ('customer_preferences', 'updated_at',   'try_cast(updated_at AS TIMESTAMP)',5, 'select'),
 ('customer_preferences', '_rescued_data','_rescued_data',                 6, 'select'),
 ('customer_preferences', '_source_file', '_source_file',                  7, 'select'),
 ('customer_preferences', '_ingested_at', '_ingested_at',                  8, 'select'),
 -- product_categories
 ('product_categories', 'category_id',     'trim(category_id)',              1, 'select'),
 ('product_categories', 'category_name',   'trim(category_name)',             2, 'select'),
 ('product_categories', 'parent_category','trim(parent_category)',          3, 'select'),
 ('product_categories', 'is_active',     'try_cast(lower(trim(is_active)) AS BOOLEAN)', 4, 'select'),
 ('product_categories', '_rescued_data','_rescued_data',                    5, 'select'),
 ('product_categories', '_source_file', '_source_file',                     6, 'select'),
 ('product_categories', '_ingested_at', '_ingested_at',                     7, 'select'),
 -- product_suppliers
 ('product_suppliers', 'supplier_id',   'trim(supplier_id)',               1, 'select'),
 ('product_suppliers', 'supplier_name', 'trim(supplier_name)',             2, 'select'),
 ('product_suppliers', 'country',      'upper(trim(country))',             3, 'select'),
 ('product_suppliers', 'rating',       'try_cast(rating AS DECIMAL(3,2))',4, 'select'),
 ('product_suppliers', 'is_active',    'try_cast(lower(trim(is_active)) AS BOOLEAN)', 5, 'select'),
 ('product_suppliers', '_rescued_data','_rescued_data',                    6, 'select'),
 ('product_suppliers', '_source_file', '_source_file',                     7, 'select'),
 ('product_suppliers', '_ingested_at', '_ingested_at',                     8, 'select'),
 -- payment_refunds
 ('payment_refunds', 'refund_id',     'trim(refund_id)',                    1, 'select'),
 ('payment_refunds', 'payment_id',   'trim(payment_id)',                   2, 'select'),
 ('payment_refunds', 'refund_amount','try_cast(refund_amount AS DECIMAL(18,2))',3, 'select'),
 ('payment_refunds', 'refund_date',  'try_cast(refund_date AS DATE)',      4, 'select'),
 ('payment_refunds', 'refund_reason','upper(trim(refund_reason))',         5, 'select'),
 ('payment_refunds', '_rescued_data','_rescued_data',                     6, 'select'),
 ('payment_refunds', '_source_file', '_source_file',                      7, 'select'),
 ('payment_refunds', '_ingested_at', '_ingested_at',                      8, 'select'),
 -- payment_methods
 ('payment_methods', 'method_id',        'trim(method_id)',                 1, 'select'),
 ('payment_methods', 'method_name',      'upper(trim(method_name))',        2, 'select'),
 ('payment_methods', 'processing_fee_pct','try_cast(processing_fee_pct AS DECIMAL(5,2))', 3, 'select'),
 ('payment_methods', 'is_active',        'try_cast(lower(trim(is_active)) AS BOOLEAN)', 4, 'select'),
 ('payment_methods', '_rescued_data','_rescued_data',                      5, 'select'),
 ('payment_methods', '_source_file', '_source_file',                       6, 'select'),
 ('payment_methods', '_ingested_at', '_ingested_at',                       7, 'select');
