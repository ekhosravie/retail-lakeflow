"""Tests for the Payments datamart: payments, payment_refunds, payment_methods.

Covers silver DQ splitting (clean vs quarantine) and gold payment_summary_mv,
daily_payments_mv, order_payment_status_mv.
"""
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def test_payments_silver_dq_split(test_spark):
    """Payments silver layer splits rows into clean and quarantine by DQ rules."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_payments (
            payment_id     STRING,
            order_id       STRING,
            payment_method STRING,
            amount         STRING,
            currency       STRING,
            payment_status STRING,
            payment_ts     STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_payments VALUES
        ('P001', '1', 'CARD',    '50.00', 'USD', 'SUCCESS',  '2024-01-01T10:00:00', NULL, 'test.csv', now()),
        ('P002', '2', 'CASH',    '60.00', 'USD', 'SUCCESS',  '2024-01-02T11:00:00', NULL, 'test.csv', now()),
        ('P003', '3', 'WALLET',  '30.00', 'USD', 'PENDING',  '2024-01-03T12:00:00', NULL, 'test.csv', now()),
        (NULL,   '4', 'CARD',    '10.00', 'USD', 'SUCCESS',  '2024-01-04T10:00:00', NULL, 'test.csv', now()),
        ('P005', '5', 'BITCOIN', '20.00', 'USD', 'SUCCESS',  '2024-01-05T11:00:00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.payments_clean",
        "retail_dev.silver.payments_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.payments_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.payments_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify derived column payment_date
    rows = {r.payment_id: r for r in clean.collect()}
    assert str(rows['P001'].payment_date) == '2024-01-01', f"P001 payment_date should be 2024-01-01"
    assert rows['P001'].payment_method == 'CARD', f"payment_method should be uppercased"
    assert rows['P001'].currency == 'USD', f"currency should be uppercased"


def test_payment_refunds_silver_dq_split(test_spark):
    """Payment refunds silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_payment_refunds (
            refund_id     STRING,
            payment_id    STRING,
            refund_amount STRING,
            refund_date   STRING,
            refund_reason STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_payment_refunds VALUES
        ('R001', 'P001', '10.00', '2024-01-10', 'DAMAGED',   NULL, 'test.csv', now()),
        ('R002', 'P002', '20.00', '2024-01-11', 'WRONG_ITEM', NULL, 'test.csv', now()),
        (NULL,   'P003', '5.00',  '2024-01-12', 'OTHER',     NULL, 'test.csv', now()),
        ('R004', 'P004', '0.00',  '2024-01-13', 'DAMAGED',   NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.payment_refunds_clean",
        "retail_dev.silver.payment_refunds_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.payment_refunds_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.payment_refunds_quarantine")
    assert clean.count() == 2, f"Expected 2 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify refund_reason uppercased
    rows = {r.refund_id: r for r in clean.collect()}
    assert rows['R001'].refund_reason == 'DAMAGED', f"refund_reason should be uppercased"
    assert rows['R002'].refund_reason == 'WRONG_ITEM', f"refund_reason should be uppercased"


def test_payment_methods_silver_dq_split(test_spark):
    """Payment methods silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_payment_methods (
            method_id          STRING,
            method_name        STRING,
            processing_fee_pct STRING,
            is_active          STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_payment_methods VALUES
        ('M1', 'CARD',    '2.50', 'true',  NULL, 'test.csv', now()),
        ('M2', 'CASH',    '0.00', 'true',  NULL, 'test.csv', now()),
        ('M3', 'WALLET',  '1.50', 'false', NULL, 'test.csv', now()),
        (NULL, 'BANK_TRANSFER', '1.00', 'true',  NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.payment_methods_clean",
        "retail_dev.silver.payment_methods_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.payment_methods_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.payment_methods_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 1, f"Expected 1 quarantine row, got {quarantine.count()}"

    # Verify method_name uppercased and is_active cast to boolean
    rows = {r.method_id: r for r in clean.collect()}
    assert rows['M1'].method_name == 'CARD', f"method_name should be uppercased"
    assert rows['M1'].is_active is True, f"M1 is_active should be True"
    assert rows['M3'].is_active is False, f"M3 is_active should be False"


def test_payment_summary_gold(test_spark):
    """Payment summary MV joins payments with refund stats."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.payments_clean (
            payment_id      STRING,
            order_id        BIGINT,
            payment_method  STRING,
            amount          DECIMAL(18,2),
            currency        STRING,
            payment_status  STRING,
            payment_ts      TIMESTAMP,
            payment_date    DATE,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.payments_clean VALUES
        ('P001', 1, 'CARD',   50.00, 'USD', 'SUCCESS', '2024-01-01T10:00:00', '2024-01-01', NULL, 'test.csv', now()),
        ('P002', 2, 'CASH',   60.00, 'USD', 'SUCCESS', '2024-01-02T11:00:00', '2024-01-02', NULL, 'test.csv', now())
    """)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.payment_refunds_clean (
            refund_id     STRING,
            payment_id    STRING,
            refund_amount DECIMAL(18,2),
            refund_date   DATE,
            refund_reason STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.payment_refunds_clean VALUES
        ('R001', 'P001', 10.00, '2024-01-10', 'DAMAGED', NULL, 'test.csv', now()),
        ('R002', 'P001', 5.00,  '2024-01-11', 'OTHER',   NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set(["retail_dev.gold.payment_summary_mv"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("SELECT * FROM retail_dev.gold.payment_summary_mv ORDER BY payment_id")
    rows = {r.payment_id: r for r in result.collect()}

    # P001: 50.00 amount, 15.00 refunded (2 refunds), net 35.00
    assert rows['P001'].refunded_amount == 15.00, f"P001 refunded_amount should be 15.00, got {rows['P001'].refunded_amount}"
    assert rows['P001'].refund_count == 2, f"P001 refund_count should be 2, got {rows['P001'].refund_count}"
    assert rows['P001'].net_amount == 35.00, f"P001 net_amount should be 35.00, got {rows['P001'].net_amount}"

    # P002: 60.00 amount, 0 refunds, net 60.00
    assert rows['P002'].refunded_amount == 0, f"P002 refunded_amount should be 0, got {rows['P002'].refunded_amount}"
    assert rows['P002'].refund_count == 0, f"P002 refund_count should be 0, got {rows['P002'].refund_count}"
    assert rows['P002'].net_amount == 60.00, f"P002 net_amount should be 60.00, got {rows['P002'].net_amount}"


def test_order_payment_status_gold(test_spark):
    """Order payment reconciliation MV: PAID / PARTIAL / UNPAID."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.orders_clean (
            order_id BIGINT, customer_id STRING, product_id STRING,
            order_date DATE, quantity INT, unit_price DECIMAL(18,2),
            total_amount DECIMAL(18,2),
            _rescued_data STRING, _source_file STRING, _ingested_at TIMESTAMP
        )
    """)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.payments_clean (
            payment_id      STRING,
            order_id        BIGINT,
            payment_method  STRING,
            amount          DECIMAL(18,2),
            currency        STRING,
            payment_status  STRING,
            payment_ts      TIMESTAMP,
            payment_date    DATE,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.orders_clean VALUES
        (1, 'C001', 'P001', '2024-01-01', 5, 10.00, 50.00, NULL, 'test.csv', now()),
        (2, 'C002', 'P002', '2024-01-02', 3, 20.00, 60.00, NULL, 'test.csv', now()),
        (3, 'C003', 'P003', '2024-01-03', 2, 15.00, 30.00, NULL, 'test.csv', now())
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.payments_clean VALUES
        ('P001', 1, 'CARD', 50.00, 'USD', 'SUCCESS', '2024-01-01T10:00:00', '2024-01-01', NULL, 'test.csv', now()),
        ('P002', 2, 'CASH', 30.00, 'USD', 'SUCCESS', '2024-01-02T11:00:00', '2024-01-02', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set(["retail_dev.gold.order_payment_status_mv"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("SELECT * FROM retail_dev.gold.order_payment_status_mv ORDER BY order_id")
    rows = {r.order_id: r for r in result.collect()}

    # Order 1: total 50, paid 50 → PAID
    assert rows[1].payment_status == 'PAID', f"Order 1 should be PAID, got {rows[1].payment_status}"
    assert rows[1].paid_amount == 50.00, f"Order 1 paid_amount should be 50.00, got {rows[1].paid_amount}"

    # Order 2: total 60, paid 30 → PARTIAL
    assert rows[2].payment_status == 'PARTIAL', f"Order 2 should be PARTIAL, got {rows[2].payment_status}"
    assert rows[2].paid_amount == 30.00, f"Order 2 paid_amount should be 30.00, got {rows[2].paid_amount}"

    # Order 3: total 30, paid 0 → UNPAID
    assert rows[3].payment_status == 'UNPAID', f"Order 3 should be UNPAID, got {rows[3].payment_status}"
    assert rows[3].paid_amount == 0, f"Order 3 paid_amount should be 0, got {rows[3].paid_amount}"