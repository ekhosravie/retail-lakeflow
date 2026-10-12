from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

SCHEMA = """preference_id STRING, customer_id STRING, preference_key STRING, preference_value STRING, updated_at STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_customer_preferences",
    comment="Raw customer preferences from CSV, append-only",
)
def raw_customer_preferences():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/customer_preferences")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
