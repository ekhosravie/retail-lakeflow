from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.view(name="customers_cdc_feed")
def customers_cdc_feed():
    return spark.readStream.table(f"{CATALOG}.silver.customers_cdc_clean")


# SCD Type 2: every change creates a new row with __START_AT / __END_AT.
dp.create_streaming_table(
    name=f"{CATALOG}.gold.dim_customer",
    comment="Customer dimension, SCD Type 2 (history kept)",
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG}.gold.dim_customer",
    source="customers_cdc_feed",
    keys=["customer_id"],
    sequence_by="event_ts",                       # handles out-of-order events
    apply_as_deletes=F.expr("op = 'D'"),          # delete = close the current version
    except_column_list=["op", "event_ts", "_rescued_data", "_source_file", "_ingested_at"],
    stored_as_scd_type=2,
)


@dp.materialized_view(
    name=f"{CATALOG}.gold.dim_customer_current",
    comment="Only the current version of each customer (open-ended rows of dim_customer)",
)
def dim_customer_current():
    return spark.read.table(f"{CATALOG}.gold.dim_customer").where("__END_AT IS NULL")
