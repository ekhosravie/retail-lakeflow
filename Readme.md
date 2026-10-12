# retail-lakeflow

A metadata-driven retail data lakehouse built on **Lakeflow Spark Declarative Pipelines (SDP)** in Databricks. The project implements a medallion architecture (Bronze → Silver → Gold) organized into **four business datamarts**: Customers, Orders, Payments, and Products.

All data-quality rules and column transformations are **fully metadata-driven** — they are loaded at runtime from Unity Catalog reference tables (`ops.dq_rules`, `ops.column_transforms`). No DQ logic or column casting is hardcoded in pipeline source files.

---

## Pipeline Configuration

| Setting | Value |
| --- | --- |
| Pipeline ID | `375cfcb2-0e25-47f2-a1c0-c1cb0c6d5558` |
| Catalog | `retail_dev` |
| Default schema | `bronze` |
| Compute | Serverless + Photon |
| Channel | CURRENT |
| Mode | Triggered (not continuous) |
| Libraries | `src/**` (glob include) |
| Landing root | `/Volumes/retail_dev/landing` |

Pipeline parameters (set in configuration):

* `catalog` → `retail_dev` (used by all Python files via `spark.conf.get("catalog")`)
* `landing_root` → `/Volumes/retail_dev/landing` (Auto Loader source root)

---

## Database Structure

### Schemas

| Schema | Purpose |
| --- | --- |
| `retail_dev.landing` | UC volumes holding raw CSV files from `generate_landing_data.py` |
| `retail_dev.bronze` | Raw streaming tables (Auto Loader, all STRING columns) |
| `retail_dev.silver` | Typed + validated clean tables and quarantine tables |
| `retail_dev.gold` | Dimensions (SCD1/SCD2) and materialized view aggregations |
| `retail_dev.ops` | Reference tables: `feed_config`, `dq_rules`, `column_transforms` |

### Datamart → Feed → Table Mapping

#### Customers Datamart

| Feed | Bronze | Silver Clean | Silver Quarantine | Gold |
| --- | --- | --- | --- | --- |
| customers_cdc | `raw_customers_cdc` | `customers_cdc_clean` | `customers_cdc_quarantine` | `dim_customer` (SCD2), `dim_customer_current` (MV), `customer_360_mv` (MV) |
| customer_addresses | `raw_customer_addresses` | `customer_addresses_clean` | `customer_addresses_quarantine` | `customer_360_mv` |
| customer_preferences | `raw_customer_preferences` | `customer_preferences_clean` | `customer_preferences_quarantine` | `customer_360_mv` |

#### Orders Datamart

| Feed | Bronze | Silver Clean | Silver Quarantine | Gold |
| --- | --- | --- | --- | --- |
| orders | `raw_orders` | `orders_clean` | `orders_quarantine` | `daily_sales_mv` (MV), `order_summary_mv` (MV) |
| order_items | `raw_order_items` | `order_items_clean` | `order_items_quarantine` | `order_summary_mv` |
| order_returns | `raw_order_returns` | `order_returns_clean` | `order_returns_quarantine` | `order_summary_mv` |

#### Payments Datamart

| Feed | Bronze | Silver Clean | Silver Quarantine | Gold |
| --- | --- | --- | --- | --- |
| payments | `raw_payments` | `payments_clean` | `payments_quarantine` | `daily_payments_mv` (MV), `payment_summary_mv` (MV), `order_payment_status_mv` (MV) |
| payment_refunds | `raw_payment_refunds` | `payment_refunds_clean` | `payment_refunds_quarantine` | `payment_summary_mv` |
| payment_methods | `raw_payment_methods` | `payment_methods_clean` | `payment_methods_quarantine` | `payment_summary_mv` |

#### Products Datamart

| Feed | Bronze | Silver Clean | Silver Quarantine | Gold |
| --- | --- | --- | --- | --- |
| products | `raw_products` | `products_clean` | `products_quarantine` | `dim_product` (SCD1), `product_catalog_mv` (MV) |
| product_categories | `raw_product_categories` | `product_categories_clean` | `product_categories_quarantine` | `product_catalog_mv` |
| product_suppliers | `raw_product_suppliers` | `product_suppliers_clean` | `product_suppliers_quarantine` | `product_catalog_mv` |

**Totals:** 12 bronze tables, 12 silver clean + 12 silver quarantine, 10 gold tables (4 dimensions + 6 MVs).

---

## Reference Tables (Metadata-Driven Mechanism)

All three reference tables live in `retail_dev.ops` and are seeded by `setup/00_setup_real.sql`.

### `ops.feed_config`

One row per feed (12 rows). Maps each feed to its bronze, silver, quarantine, and gold table names, load pattern (`append`, `cdc_scd1`, `cdc_scd2`), key columns, sequence column, and delete condition.

### `ops.dq_rules`

Data-quality rules per feed (44 rows). Each row defines:

* `feed_name` — which feed this rule applies to
* `rule_name` — unique rule identifier (e.g., `order_id_present`, `qty_positive`)
* `rule_expr` — SQL boolean expression, true = row is valid (e.g., `quantity > 0`)
* `action` — `drop` (row goes to quarantine)
* `is_enabled` — boolean toggle

Silver layers load these at pipeline initialization via `spark.read.table("retail_dev.ops.dq_rules")` filtered by `feed_name`. Each rule is wrapped in `COALESCE(rule_expr, FALSE)` so NULL results count as failures. The combined predicate splits rows into `*_clean` (all rules pass) and `*_quarantine` (any rule fails, with `failed_rules` array listing which rules fired).

### `ops.column_transforms`

Column transformation directives per feed (111 rows). Each row defines:

* `feed_name` — which feed this transform applies to
* `column_name` — output column alias
* `transform_expr` — SQL expression producing the column value
* `stage` — `select` (applied in the `.select()` clause) or `derived` (applied via `.withColumn()` after select)
* `position` — ordering within the stage

Silver layers load these at initialization, ordered by `stage, position`. `select`-stage transforms build the typed DataFrame from the raw bronze STRING columns (e.g., `try_cast(order_id AS BIGINT)`, `trim(customer_id)`, `upper(trim(payment_method))`). `derived`-stage transforms add computed columns afterward (e.g., `quantity * unit_price` as `total_amount`, `to_date(payment_ts)` as `payment_date`).

**Adding a new feed or changing a DQ rule requires only a new row in the reference table — no Python code changes.**

---

## Source Code Organization

```
retail-lakeflow/
├── databricks.yml                  # Declarative Automation Bundle (DAB) config
├── Readme.md
├── resources/
│   └── retail_pipeline.yml         # DAB pipeline resource definition
├── setup/
│   ├── 00_setup_real.sql           # DDL: catalogs, schemas, volumes, reference tables + seed data
│   ├── 10_reference_tables.sql     # Read-only schema reference (NOT executed)
│   ├── verify_check.sql            # Post-pipeline verification queries (all 4 datamarts)
│   └── generate_landing_data.py    # CSV data generator (12 feeds, 3 daily files each)
├── src/
│   ├── bronze/
│   │   ├── customers/              # customers_cdc_bronze.py, customer_addresses_bronze.py, customer_preferences_bronze.py
│   │   ├── orders/                 # orders_bronze.py, order_items_bronze.py, order_returns_bronze.py
│   │   ├── payments/               # payments_bronze.py, payment_refunds_bronze.py, payment_methods_bronze.py
│   │   └── products/               # products_bronze.py, product_categories_bronze.py, product_suppliers_bronze.py
│   ├── silver/
│   │   ├── customers/              # customers_silver.py, customer_addresses_silver.py, customer_preferences_silver.py
│   │   ├── orders/                 # orders_silver.py, order_items_silver.py, order_returns_silver.py
│   │   ├── payments/               # payments_silver.py, payment_refunds_silver.py, payment_methods_silver.py
│   │   └── products/               # products_silver.py, product_categories_silver.py, product_suppliers_silver.py
│   └── gold/
│       ├── customers/              # dim_customer_gold.py, customer_360_gold.py
│       ├── orders/                 # daily_sales_gold.py, order_summary_gold.py
│       ├── payments/               # payments_gold.py, payment_summary_gold.py
│       └── products/               # dim_product_gold.py, product_catalog_gold.py
└── tests/
    ├── test_orders.py             # 5 tests: silver DQ split + gold aggregations
    ├── test_customers.py          # 4 tests: silver DQ split + customer_360 join
    ├── test_payments.py            # 5 tests: silver DQ split + payment_summary + reconciliation
    └── test_products.py            # 4 tests: silver DQ split + product_catalog join
```

---

## Medallion Architecture — How It Works

### Bronze Layer (Ingestion)

Each bronze file defines a **streaming table** using Auto Loader (`cloudFiles` format) that reads CSV files from the corresponding landing volume. Key design decisions:

* All columns are read as **STRING** (explicit schema or `inferColumnTypes=false`) — bronze never fails on dirty data; typing happens in silver.
* Each table adds `_source_file` (from `_metadata.file_path`) and `_ingested_at` (current timestamp).
* Most feeds use `rescuedDataColumn="_rescued_data"` to capture extra/unknown columns from later files without failing.
* No business logic, no filtering — bronze is a faithful raw copy.

### Silver Layer (Validation & Typing)

Each silver file follows an identical dynamic pattern:

1. **Load DQ rules** from `ops.dq_rules` filtered by `feed_name`, wrapping each in `COALESCE(rule_expr, FALSE)`.
2. **Load column transforms** from `ops.column_transforms` filtered by `feed_name`, ordered by `stage, position`.
3. **Build a typed view** (`@dp.view`) that reads the bronze streaming table, applies `select`-stage transforms (casting, trimming, uppercasing), then applies `derived`-stage transforms (computed columns).
4. **Publish clean table** (`@dp.table`) with `@dp.expect_all_or_drop(RULES)` — only rows passing all DQ rules are retained.
5. **Publish quarantine table** (`@dp.table`) — rows failing any DQ rule, with a `failed_rules` array listing which rules fired.

Clean and quarantine are **exact complements** — every bronze row lands in exactly one of the two tables.

### Gold Layer (Business Aggregations & Dimensions)

Gold tables are either **materialized views** (batch reads from silver) or **Auto CDC targets** (streaming):

* **`dim_customer`** — SCD Type 2 via Auto CDC. Every change creates a new row with `__START_AT`/`__END_AT`. Deletes close the current version (set `__END_AT`). `dim_customer_current` is a MV filtering to open-ended rows (`__END_AT IS NULL`).
* **`dim_product`** — SCD Type 1 via Auto CDC. Latest version overwrites the previous. Soft deletes set `is_active = false`.
* **`customer_360_mv`** — Joins `dim_customer_current` with current `customer_addresses_clean` (where `is_current = true`).
* **`daily_sales_mv`** — Aggregates `orders_clean` by `order_date`: order count, distinct customers, units sold, revenue.
* **`order_summary_mv`** — Joins `orders_clean` with `order_items_clean` (item counts/totals) and `order_returns_clean` (return counts/refund totals), computes `net_revenue`.
* **`payment_summary_mv`** — Joins `payments_clean` with `payment_refunds_clean` (refund stats), computes `net_amount`.
* **`daily_payments_mv`** — Aggregates `payments_clean` by `payment_date` and `payment_method`: payment count, collected/refunded/failed amounts.
* **`order_payment_status_mv`** — Reconciles orders with successful payments: `PAID` (fully covered), `PARTIAL` (partially paid), `UNPAID` (no payments).
* **`product_catalog_mv`** — Joins `dim_product` with active `product_categories_clean` to add `parent_category`.

---

## Setup Instructions

### 1. Create Infrastructure

Run `setup/00_setup_real.sql` in a Databricks SQL editor. This creates:

* Catalog: `retail_dev`
* Schemas: `landing`, `bronze`, `silver`, `gold`, `ops`
* Volumes: 12 landing volumes (one per feed)
* Reference tables: `feed_config`, `dq_rules`, `column_transforms` with all seed data

### 2. Generate Landing Data

Run `setup/generate_landing_data.py` as a Databricks notebook or script:

```python
# Generates 3 daily CSV files per feed (12 feeds × 3 days = 36 files)
# Each file has realistic rows + ~5% bad rows (for quarantine testing)
# Day 3 files include an extra column (lands in _rescued_data)
# Set DAYS = [1] → run pipeline → DAYS = [2] → run → DAYS = [3] → run
```

Environment variables:
* `RETAIL_CATALOG` (default: `retail_dev`)
* `RETAIL_ROOT` (default: `/Volumes/retail_dev/landing`)
* `FORCE = True` to overwrite existing files (requires pipeline full refresh afterward)

### 3. Run the Pipeline

Trigger a pipeline update from the Lakeflow Pipelines Editor. The pipeline will:

1. Auto Loader picks up new CSV files from landing volumes → bronze tables
2. Silver layers apply DQ rules and column transforms → clean + quarantine tables
3. Gold layers aggregate and join → dimensions and materialized views

### 4. Verify Results

Run `setup/verify_check.sql` to check:

* Quarantine counts for all 12 feeds
* Rescued data for all feeds with explicit schemas
* Silver clean row counts
* SCD2 customer dimension (total/closed/current versions)
* All 10 gold MVs and dimensions
* Cross-datamart reconciliation (revenue vs collected payments)
* Reference table row counts and per-feed breakdowns

---

## Testing

Tests use the **TestPipeline API** (`pyspark.pipelines.testing`) with pytest. Each test:

1. Seeds bronze tables (or silver/gold upstream tables) as regular tables with test data
2. Runs selective pipeline refreshes via `test_pipeline.run(test_spark, set([...]))`
3. Asserts on output tables (clean vs quarantine counts, derived columns, column transforms, gold aggregations)

18 tests across 4 files:

| File | Tests | Coverage |
| --- | --- | --- |
| `test_orders.py` | 5 | orders/order_items/order_returns silver DQ + `order_summary_mv` + `daily_sales_mv` |
| `test_customers.py` | 4 | customers_cdc/addresses/preferences silver DQ + `customer_360_mv` |
| `test_payments.py` | 5 | payments/refunds/methods silver DQ + `payment_summary_mv` + `order_payment_status_mv` |
| `test_products.py` | 4 | products/categories/suppliers silver DQ + `product_catalog_mv` |

Run tests from the Lakeflow Pipelines Editor **Run tests** button.

---

## Declarative Automation Bundle (DAB)

The project includes a DAB configuration (`databricks.yml`) for source-controlled deployment:

* **Bundle name:** `retail-lakeflow`
* **Dev target:** `retail_dev` catalog, development mode
* **Prod target:** `retail_prod` catalog, production mode
* **Resource:** `resources/retail_pipeline.yml` defines the pipeline

Deploy with `databricks bundle deploy -t dev` (or `-t prod`).

---

## Key Design Principles

1. **Metadata-driven DQ and transforms** — all rules and column mappings live in `ops.dq_rules` and `ops.column_transforms`. Adding or modifying rules requires only SQL `INSERT`/`UPDATE` on reference tables, not Python code changes.
2. **Bronze never fails** — all columns as STRING, `rescuedDataColumn` captures schema drift, no filtering.
3. **Clean and quarantine are exact complements** — `COALESCE(rule, FALSE)` ensures NULL results count as failures, so every row lands in exactly one table.
4. **Datamart organization** — each datamart has its own subfolder in bronze/silver/gold, making the codebase navigable and modular.
5. **Deterministic data generation** — `generate_landing_data.py` uses a fixed seed (42), producing reproducible CSV files with ~5% bad rows and day-3 schema evolution.
6. **Cross-datamart reconciliation** — `order_payment_status_mv` joins orders and payments across datamarts to verify business consistency.