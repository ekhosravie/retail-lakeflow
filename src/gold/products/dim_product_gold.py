from pyspark import pipelines as dp

CATALOG = spark.conf.get("catalog")


@dp.view(name="products_feed")
def products_feed():
    return spark.readStream.table(f"{CATALOG}.silver.products_clean")


# SCD Type 1: latest version overwrites the old one. Deactivation is a soft delete (is_active = false).
dp.create_streaming_table(
    name=f"{CATALOG}.gold.dim_product",
    comment="Product dimension, SCD Type 1 (current state only)",
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.dim_product",
    source="products_feed",
    keys=["product_id"],
    sequence_by="updated_at",
    except_column_list=["_rescued_data", "_source_file", "_ingested_at"],
    stored_as_scd_type=1,
)
