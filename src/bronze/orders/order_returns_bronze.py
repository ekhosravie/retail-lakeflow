from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

SCHEMA = """return_id STRING, order_id STRING, product_id STRING, return_date STRING, return_reason STRING, refund_amount STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_order_returns",
    comment="Raw order returns from CSV, append-only",
)
def raw_order_returns():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/order_returns")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
