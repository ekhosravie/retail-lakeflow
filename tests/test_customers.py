"""Tests for the Customers datamart: customers_cdc, customer_addresses, customer_preferences.

Covers silver DQ splitting (clean vs quarantine) and gold customer_360_mv join.
"""
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def test_customers_cdc_silver_dq_split(test_spark):
    """Customers CDC silver layer splits rows into clean and quarantine by DQ rules."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_customers_cdc (
            customer_id   STRING,
            first_name    STRING,
            last_name     STRING,
            email         STRING,
            city          STRING,
            country       STRING,
            loyalty_tier  STRING,
            op            STRING,
            event_ts      STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_customers_cdc VALUES
        ('C001', 'Alice', 'Smith',  'alice@test.com',  'NYC',  'US', 'GOLD',   'I', '2024-01-01T10:00:00', NULL, 'test.csv', now()),
        ('C002', 'Bob',   'Jones',  'bob@test.com',    'LA',   'US', 'SILVER', 'I', '2024-01-01T11:00:00', NULL, 'test.csv', now()),
        ('C003', 'Carol', 'Taylor', 'carol@test.com',  'Paris','FR', 'BRONZE', 'U', '2024-01-02T12:00:00', NULL, 'test.csv', now()),
        (NULL,  'Dave',  'Brown',   'dave@test.com',   'NYC',  'US', 'GOLD',   'I', '2024-01-03T10:00:00', NULL, 'test.csv', now()),
        ('C005', 'Eve',   'White',  'eve@test.com',    'LA',   'US', 'GOLD',   'X', '2024-01-03T11:00:00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.customers_cdc_clean",
        "retail_dev.silver.customers_cdc_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.customers_cdc_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.customers_cdc_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify column transforms: op uppercased, email lowercased, names initcapped
    rows = {r.customer_id: r for r in clean.collect()}
    assert rows['C001'].op == 'I', f"op should be uppercased to 'I'"
    assert rows['C001'].email == 'alice@test.com', f"email should be lowercased"
    assert rows['C001'].first_name == 'Alice', f"first_name should be initcapped"
    assert rows['C003'].op == 'U', f"op should be uppercased to 'U'"


def test_customer_addresses_silver_dq_split(test_spark):
    """Customer addresses silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_customer_addresses (
            address_id   STRING,
            customer_id  STRING,
            address_type STRING,
            street       STRING,
            city         STRING,
            country      STRING,
            is_current   STRING,
            updated_at   STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_customer_addresses VALUES
        ('A1', 'C001', 'HOME',   '123 Main St', 'NYC',  'US', 'true',  '2024-01-01T10:00:00', NULL, 'test.csv', now()),
        ('A2', 'C002', 'WORK',   '456 Oak Ave', 'LA',   'US', 'false', '2024-01-02T11:00:00', NULL, 'test.csv', now()),
        ('A3', 'C003', 'HOME',   '789 Pine Rd', 'Paris','FR', 'true',  '2024-01-03T12:00:00', NULL, 'test.csv', now()),
        (NULL, 'C004', 'HOME',  '999 Elm St',  'NYC',  'US', 'true',  '2024-01-04T10:00:00', NULL, 'test.csv', now()),
        ('A5', 'C005', 'WORK',   NULL,           'LA',   'US', 'true',  '2024-01-05T11:00:00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.customer_addresses_clean",
        "retail_dev.silver.customer_addresses_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.customer_addresses_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.customer_addresses_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify is_current cast to boolean
    rows = {r.address_id: r for r in clean.collect()}
    assert rows['A1'].is_current is True, f"A1 is_current should be True"
    assert rows['A2'].is_current is False, f"A2 is_current should be False"


def test_customer_preferences_silver_dq_split(test_spark):
    """Customer preferences silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_customer_preferences (
            preference_id    STRING,
            customer_id      STRING,
            preference_key   STRING,
            preference_value STRING,
            updated_at       STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_customer_preferences VALUES
        ('P1', 'C001', 'language',    'en',    '2024-01-01T10:00:00', NULL, 'test.csv', now()),
        ('P2', 'C002', 'currency',    'USD',   '2024-01-02T11:00:00', NULL, 'test.csv', now()),
        ('P3', 'C003', 'newsletter',  'true',  '2024-01-03T12:00:00', NULL, 'test.csv', now()),
        (NULL, 'C004', 'language',    'fr',    '2024-01-04T10:00:00', NULL, 'test.csv', now()),
        ('P5', NULL,   'currency',    'EUR',   '2024-01-05T11:00:00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.customer_preferences_clean",
        "retail_dev.silver.customer_preferences_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.customer_preferences_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.customer_preferences_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify preference_key uppercased
    rows = {r.preference_id: r for r in clean.collect()}
    assert rows['P1'].preference_key == 'LANGUAGE', f"preference_key should be uppercased"
    assert rows['P2'].preference_key == 'CURRENCY', f"preference_key should be uppercased"


def test_customer_360_gold(test_spark):
    """Customer 360 MV joins current customers with current addresses."""
    # Seed dim_customer_current (gold MV upstream)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.gold.dim_customer_current (
            customer_id   STRING,
            first_name    STRING,
            last_name     STRING,
            email         STRING,
            city          STRING,
            country       STRING,
            loyalty_tier  STRING,
            __START_AT    TIMESTAMP,
            __END_AT      TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.gold.dim_customer_current VALUES
        ('C001', 'Alice', 'Smith', 'alice@test.com', 'NYC',  'US', 'GOLD',   '2024-01-01T10:00:00', NULL),
        ('C002', 'Bob',   'Jones', 'bob@test.com',   'LA',   'US', 'SILVER', '2024-01-01T11:00:00', NULL)
    """)

    # Seed customer_addresses_clean (silver upstream)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.customer_addresses_clean (
            address_id   STRING,
            customer_id  STRING,
            address_type STRING,
            street       STRING,
            city         STRING,
            country      STRING,
            is_current   BOOLEAN,
            updated_at   TIMESTAMP,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.customer_addresses_clean VALUES
        ('A1', 'C001', 'HOME', '123 Main St', 'NYC',  'US', true,  '2024-01-01T10:00:00', NULL, 'test.csv', now()),
        ('A2', 'C001', 'WORK', '456 Oak Ave', 'NYC',  'US', false, '2024-01-02T11:00:00', NULL, 'test.csv', now()),
        ('A3', 'C002', 'HOME', '789 Pine Rd', 'LA',   'US', true,  '2024-01-03T12:00:00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set(["retail_dev.gold.customer_360_mv"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("SELECT * FROM retail_dev.gold.customer_360_mv ORDER BY customer_id, address_type")
    rows = result.collect()

    # C001 should have only the current (HOME) address, not the WORK address
    c001_rows = [r for r in rows if r.customer_id == 'C001']
    assert len(c001_rows) == 1, f"C001 should have 1 row (current address only), got {len(c001_rows)}"
    assert c001_rows[0].address_type == 'HOME', f"C001 address_type should be HOME"
    assert c001_rows[0].street == '123 Main St', f"C001 street should be '123 Main St'"
    assert c001_rows[0].loyalty_tier == 'GOLD', f"C001 loyalty_tier should be GOLD"

    # C002 should have the HOME address
    c002_rows = [r for r in rows if r.customer_id == 'C002']
    assert len(c002_rows) == 1, f"C002 should have 1 row, got {len(c002_rows)}"
    assert c002_rows[0].address_type == 'HOME', f"C002 address_type should be HOME"