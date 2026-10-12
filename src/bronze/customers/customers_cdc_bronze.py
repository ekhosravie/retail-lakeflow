from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

# Explicit all-STRING schema: bronze never fails on dirty data and never depends on
# which file Auto Loader happens to sample first. Typing happens in silver.
# Extra/unknown columns (e.g. a new column in a later file) land in _rescued_data.
SCHEMA = """customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING, country STRING, loyalty_tier STRING, op STRING, event_ts STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_customers_cdc",
    comment="Raw customer CDC events (op I/U/D), append-only",
)
def raw_customers_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/customers_cdc")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
