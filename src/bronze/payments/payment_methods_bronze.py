from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

SCHEMA = """method_id STRING, method_name STRING, processing_fee_pct STRING, is_active STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_payment_methods",
    comment="Raw payment methods from CSV, append-only",
)
def raw_payment_methods():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/payment_methods")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
