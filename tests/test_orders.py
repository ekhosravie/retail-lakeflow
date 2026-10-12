"""Tests for the Orders datamart: orders, order_items, order_returns.

Covers silver DQ splitting (clean vs quarantine) and gold aggregations
(order_summary_mv, daily_sales_mv).
"""
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def test_orders_silver_dq_split(test_spark):
    """Orders silver layer splits rows into clean and quarantine by DQ rules."""
    # Seed the bronze source table with 3 valid + 2 invalid rows
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_orders (
            order_id    STRING,
            customer_id STRING,
            product_id  STRING,
            order_date  STRING,
            quantity    STRING,
            unit_price  STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_orders VALUES
        ('1', 'C001', 'P001', '2024-01-01', '5', '10.00', NULL, 'test.csv', now()),
        ('2', 'C002', 'P002', '2024-01-02', '3', '20.00', NULL, 'test.csv', now()),
        ('3', 'C003', 'P003', '2024-01-03', '2', '15.00', NULL, 'test.csv', now()),
        (NULL, 'C004', 'P004', '2024-01-04', '1', '5.00',  NULL, 'test.csv', now()),
        ('5', 'C005', 'P005', NULL,         '0', '10.00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.orders_clean",
        "retail_dev.silver.orders_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.orders_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.orders_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify total_amount derived column
    totals = {r.order_id: r.total_amount for r in clean.collect()}
    assert totals[1] == 50.00, f"Order 1 total_amount should be 50.00, got {totals[1]}"
    assert totals[2] == 60.00, f"Order 2 total_amount should be 60.00, got {totals[2]}"


def test_order_items_silver_dq_split(test_spark):
    """Order items silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_order_items (
            order_item_id STRING,
            order_id      STRING,
            product_id    STRING,
            quantity      STRING,
            unit_price    STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_order_items VALUES
        ('1', '1', 'P001', '2', '10.00', NULL, 'test.csv', now()),
        ('2', '1', 'P002', '1', '20.00', NULL, 'test.csv', now()),
        ('3', '2', 'P003', '3', '5.00',  NULL, 'test.csv', now()),
        (NULL, '3', 'P004', '1', '5.00',  NULL, 'test.csv', now()),
        ('5', '3', 'P005', '0', '10.00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.order_items_clean",
        "retail_dev.silver.order_items_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.order_items_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.order_items_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify line_total derived column
    items = {r.order_item_id: r.line_total for r in clean.collect()}
    assert items[1] == 20.00, f"Item 1 line_total should be 20.00, got {items[1]}"
    assert items[2] == 20.00, f"Item 2 line_total should be 20.00, got {items[2]}"


def test_order_returns_silver_dq_split(test_spark):
    """Order returns silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_order_returns (
            return_id     STRING,
            order_id      STRING,
            product_id    STRING,
            return_date   STRING,
            return_reason STRING,
            refund_amount STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_order_returns VALUES
        ('R1', '1', 'P001', '2024-01-10', 'DAMAGED',   '10.00', NULL, 'test.csv', now()),
        ('R2', '2', 'P002', '2024-01-11', 'WRONG_ITEM','20.00', NULL, 'test.csv', now()),
        (NULL, '3', 'P003', '2024-01-12', 'OTHER',     '5.00',  NULL, 'test.csv', now()),
        ('R4', '4', 'P004', NULL,          'DAMAGED',  '15.00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.order_returns_clean",
        "retail_dev.silver.order_returns_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.order_returns_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.order_returns_quarantine")
    assert clean.count() == 2, f"Expected 2 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"


def test_order_summary_gold(test_spark):
    """Order summary MV joins orders with item and return stats."""
    # Seed silver tables directly for gold MV test
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.orders_clean (
            order_id BIGINT, customer_id STRING, product_id STRING,
            order_date DATE, quantity INT, unit_price DECIMAL(18,2),
            total_amount DECIMAL(18,2),
            _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.orders_clean VALUES
        (1, 'C001', 'P001', '2024-01-01', 5, 10.00, 50.00, NULL, 'test.csv', now()),
        (2, 'C002', 'P002', '2024-01-02', 3, 20.00, 60.00, NULL, 'test.csv', now())
    """)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.order_items_clean (
            order_item_id BIGINT, order_id BIGINT, product_id STRING,
            quantity INT, unit_price DECIMAL(18,2), line_total DECIMAL(18,2),
            _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.order_items_clean VALUES
        (1, 1, 'P001', 2, 10.00, 20.00, NULL, 'test.csv', now()),
        (2, 1, 'P002', 1, 20.00, 20.00, NULL, 'test.csv', now()),
        (3, 2, 'P003', 3, 5.00,  15.00, NULL, 'test.csv', now())
    """)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.order_returns_clean (
            return_id STRING, order_id BIGINT, product_id STRING,
            return_date DATE, return_reason STRING, refund_amount DECIMAL(18,2),
            _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.order_returns_clean VALUES
        ('R1', 1, 'P001', '2024-01-10', 'DAMAGED', 10.00, NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set(["retail_dev.gold.order_summary_mv"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("SELECT * FROM retail_dev.gold.order_summary_mv ORDER BY order_id")
    rows = {r.order_id: r for r in result.collect()}

    # Order 1: 2 items (total 40.00), 1 return (refund 10.00), net 50-10=40
    assert rows[1].item_count == 2, f"Order 1 item_count should be 2, got {rows[1].item_count}"
    assert rows[1].item_total == 40.00, f"Order 1 item_total should be 40.00, got {rows[1].item_total}"
    assert rows[1].return_count == 1, f"Order 1 return_count should be 1, got {rows[1].return_count}"
    assert rows[1].refund_total == 10.00, f"Order 1 refund_total should be 10.00, got {rows[1].refund_total}"
    assert rows[1].net_revenue == 40.00, f"Order 1 net_revenue should be 40.00, got {rows[1].net_revenue}"

    # Order 2: 1 item (total 15.00), 0 returns, net 60-0=60
    assert rows[2].item_count == 1, f"Order 2 item_count should be 1, got {rows[2].item_count}"
    assert rows[2].return_count == 0, f"Order 2 return_count should be 0, got {rows[2].return_count}"
    assert rows[2].net_revenue == 60.00, f"Order 2 net_revenue should be 60.00, got {rows[2].net_revenue}"


def test_daily_sales_gold(test_spark):
    """Daily sales MV aggregates order count, customer count, units, and revenue."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.orders_clean (
            order_id BIGINT, customer_id STRING, product_id STRING,
            order_date DATE, quantity INT, unit_price DECIMAL(18,2),
            total_amount DECIMAL(18,2),
            _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.orders_clean VALUES
        (1, 'C001', 'P001', '2024-01-01', 5, 10.00, 50.00, NULL, 'test.csv', now()),
        (2, 'C002', 'P002', '2024-01-01', 3, 20.00, 60.00, NULL, 'test.csv', now()),
        (3, 'C001', 'P003', '2024-01-02', 2, 15.00, 30.00, NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set(["retail_dev.gold.daily_sales_mv"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("SELECT * FROM retail_dev.gold.daily_sales_mv ORDER BY order_date")
    rows = {str(r.order_date): r for r in result.collect()}

    # 2024-01-01: 2 orders, 2 distinct customers, 8 units, 110.00 revenue
    assert rows["2024-01-01"].order_count == 2, f"Jan 1 order_count should be 2, got {rows['2024-01-01'].order_count}"
    assert rows["2024-01-01"].customer_count == 2, f"Jan 1 customer_count should be 2, got {rows['2024-01-01'].customer_count}"
    assert rows["2024-01-01"].units_sold == 8, f"Jan 1 units_sold should be 8, got {rows['2024-01-01'].units_sold}"
    assert rows["2024-01-01"].revenue == 110.00, f"Jan 1 revenue should be 110.00, got {rows['2024-01-01'].revenue}"

    # 2024-01-02: 1 order, 1 customer, 2 units, 30.00 revenue
    assert rows["2024-01-02"].order_count == 1, f"Jan 2 order_count should be 1, got {rows['2024-01-02'].order_count}"
    assert rows["2024-01-02"].revenue == 30.00, f"Jan 2 revenue should be 30.00, got {rows['2024-01-02'].revenue}"