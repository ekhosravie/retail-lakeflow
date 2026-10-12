from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

# Explicit all-STRING schema: bronze never fails on dirty data and never depends on
# which file Auto Loader happens to sample first. Typing happens in silver.
# Extra/unknown columns (e.g. a new column in a later file) land in _rescued_data.
SCHEMA = """product_id STRING, product_name STRING, category STRING, brand STRING, unit_cost STRING, list_price STRING, is_active STRING, updated_at STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_products",
    comment="Raw product delta feed, append-only",
)
def raw_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/products")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
