"""Tests for the Products datamart: products, product_categories, product_suppliers.

Covers silver DQ splitting (clean vs quarantine) and gold product_catalog_mv.
"""
from pyspark.pipelines.testing import TestPipeline, test_spark

test_pipeline = TestPipeline.active()


def test_products_silver_dq_split(test_spark):
    """Products silver layer splits rows into clean and quarantine by DQ rules."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_products (
            product_id   STRING,
            product_name STRING,
            category     STRING,
            brand        STRING,
            unit_cost    STRING,
            list_price   STRING,
            is_active    STRING,
            updated_at   STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_products VALUES
        ('P001', 'Widget',    'ELECTRONICS', 'Acme',  '5.00',  '10.00', 'true',  '2024-01-01T10:00:00', NULL, 'test.csv', now()),
        ('P002', 'Gadget',    'ELECTRONICS', 'Beta',  '3.00',  '8.00',  'true',  '2024-01-02T11:00:00', NULL, 'test.csv', now()),
        ('P003', 'Doohickey', 'TOOLS',       'Gamma', '2.00',  '5.00',  'false', '2024-01-03T12:00:00', NULL, 'test.csv', now()),
        (NULL,  'Thingamajig','TOOLS',       'Delta', '1.00',  '3.00',  'true',  '2024-01-04T10:00:00', NULL, 'test.csv', now()),
        ('P005', NULL,        'MISC',        'Epsilon','1.00', '2.00',  'true',  '2024-01-05T11:00:00', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.products_clean",
        "retail_dev.silver.products_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.products_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.products_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 2, f"Expected 2 quarantine rows, got {quarantine.count()}"

    # Verify is_active cast to boolean and numeric casts
    rows = {r.product_id: r for r in clean.collect()}
    assert rows['P001'].is_active is True, f"P001 is_active should be True"
    assert rows['P001'].unit_cost == 5.00, f"P001 unit_cost should be 5.00, got {rows['P001'].unit_cost}"
    assert rows['P001'].list_price == 10.00, f"P001 list_price should be 10.00, got {rows['P001'].list_price}"
    assert rows['P003'].is_active is False, f"P003 is_active should be False"


def test_product_categories_silver_dq_split(test_spark):
    """Product categories silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_product_categories (
            category_id      STRING,
            category_name    STRING,
            parent_category  STRING,
            is_active        STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_product_categories VALUES
        ('CAT1', 'ELECTRONICS', NULL,      'true',  NULL, 'test.csv', now()),
        ('CAT2', 'TOOLS',       NULL,      'true',  NULL, 'test.csv', now()),
        ('CAT3', 'GADGETS',     'ELECTRONICS', 'true',  NULL, 'test.csv', now()),
        (NULL,   'MISC',        NULL,      'false', NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.product_categories_clean",
        "retail_dev.silver.product_categories_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.product_categories_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.product_categories_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 1, f"Expected 1 quarantine row, got {quarantine.count()}"

    # Verify is_active cast to boolean
    rows = {r.category_id: r for r in clean.collect()}
    assert rows['CAT1'].is_active is True, f"CAT1 is_active should be True"
    assert rows['CAT1'].category_name == 'ELECTRONICS', f"category_name should be trimmed"
    assert rows['CAT3'].parent_category == 'ELECTRONICS', f"parent_category should be trimmed"


def test_product_suppliers_silver_dq_split(test_spark):
    """Product suppliers silver layer splits rows into clean and quarantine."""
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.bronze.raw_product_suppliers (
            supplier_id   STRING,
            supplier_name STRING,
            country       STRING,
            rating        STRING,
            is_active     STRING,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.bronze.raw_product_suppliers VALUES
        ('S1', 'Acme Corp',    'US', '4.50', 'true',  NULL, 'test.csv', now()),
        ('S2', 'Beta Inc',     'UK', '3.80', 'true',  NULL, 'test.csv', now()),
        ('S3', 'Gamma Ltd',    'DE', '4.20', 'false', NULL, 'test.csv', now()),
        (NULL, 'Delta LLC',     'US', '3.50', 'true',  NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set([
        "retail_dev.silver.product_suppliers_clean",
        "retail_dev.silver.product_suppliers_quarantine",
    ]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    clean = test_spark.sql("SELECT * FROM retail_dev.silver.product_suppliers_clean")
    quarantine = test_spark.sql("SELECT * FROM retail_dev.silver.product_suppliers_quarantine")
    assert clean.count() == 3, f"Expected 3 clean rows, got {clean.count()}"
    assert quarantine.count() == 1, f"Expected 1 quarantine row, got {quarantine.count()}"

    # Verify country uppercased, rating cast to decimal, is_active boolean
    rows = {r.supplier_id: r for r in clean.collect()}
    assert rows['S1'].country == 'US', f"S1 country should be uppercased to 'US'"
    assert rows['S1'].rating == 4.50, f"S1 rating should be 4.50, got {rows['S1'].rating}"
    assert rows['S1'].is_active is True, f"S1 is_active should be True"
    assert rows['S3'].is_active is False, f"S3 is_active should be False"


def test_product_catalog_gold(test_spark):
    """Product catalog MV joins dim_product with product categories."""
    # Seed dim_product (SCD1 gold table)
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.gold.dim_product (
            product_id   STRING,
            product_name STRING,
            category      STRING,
            brand         STRING,
            unit_cost     DECIMAL(18,2),
            list_price    DECIMAL(18,2),
            is_active     BOOLEAN,
            updated_at    TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.gold.dim_product VALUES
        ('P001', 'Widget',    'ELECTRONICS', 'Acme',  5.00,  10.00, true,  '2024-01-01T10:00:00'),
        ('P002', 'Gadget',    'ELECTRONICS', 'Beta',  3.00,  8.00,  true,  '2024-01-02T11:00:00'),
        ('P003', 'Doohickey', 'TOOLS',       'Gamma', 2.00,  5.00,  false, '2024-01-03T12:00:00')
    """)

    # Seed product_categories_clean
    test_spark.sql("""
        CREATE OR REPLACE TABLE retail_dev.silver.product_categories_clean (
            category_id      STRING,
            category_name    STRING,
            parent_category  STRING,
            is_active        BOOLEAN,
            _rescued_data STRING,
            _source_file   STRING,
            _ingested_at   TIMESTAMP
        )
    """)
    test_spark.sql("""
        INSERT INTO retail_dev.silver.product_categories_clean VALUES
        ('CAT1', 'ELECTRONICS', NULL,      true,  NULL, 'test.csv', now()),
        ('CAT2', 'TOOLS',       NULL,      true,  NULL, 'test.csv', now()),
        ('CAT3', 'GADGETS',     'ELECTRONICS', true,  NULL, 'test.csv', now())
    """)

    status = test_pipeline.run(test_spark, set(["retail_dev.gold.product_catalog_mv"]))
    assert status.is_success, f"Pipeline run failed: {status.error_message}"

    result = test_spark.sql("SELECT * FROM retail_dev.gold.product_catalog_mv ORDER BY product_id")
    rows = {r.product_id: r for r in result.collect()}

    # P001: ELECTRONICS category, parent_category should be NULL (top-level)
    assert rows['P001'].category == 'ELECTRONICS', f"P001 category should be ELECTRONICS"
    assert rows['P001'].parent_category is None, f"P001 parent_category should be NULL"
    assert rows['P001'].is_active is True, f"P001 is_active should be True"

    # P003: TOOLS category, parent_category should be NULL
    assert rows['P003'].category == 'TOOLS', f"P003 category should be TOOLS"
    assert rows['P003'].parent_category is None, f"P003 parent_category should be NULL"
    assert rows['P003'].is_active is False, f"P003 is_active should be False"

    # Verify all 3 products are in the catalog
    assert len(rows) == 3, f"Expected 3 products in catalog, got {len(rows)}"