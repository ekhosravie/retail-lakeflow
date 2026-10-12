from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
LANDING = spark.conf.get("landing_root")

SCHEMA = """supplier_id STRING, supplier_name STRING, country STRING, rating STRING, is_active STRING"""


@dp.table(
    name=f"{CATALOG}.bronze.raw_product_suppliers",
    comment="Raw product suppliers from CSV, append-only",
)
def raw_product_suppliers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(SCHEMA)
        .load(f"{LANDING}/product_suppliers")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
