from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

SCHEMA = """refund_id STRING, payment_id STRING, refund_amount STRING, refund_date STRING, refund_reason STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_payment_refunds",
    comment="Raw payment refunds from CSV, append-only",
)
def raw_payment_refunds():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/payment_refunds")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
