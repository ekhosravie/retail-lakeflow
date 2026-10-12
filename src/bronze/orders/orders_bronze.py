from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

@dp.table(
    name=f"{CATALOG}.bronze.raw_orders",
    comment="Raw orders from CSV, append-only, no business logic",
)
def raw_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")  # keep strings in bronze
        .load(f"{LANDING}/orders")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
