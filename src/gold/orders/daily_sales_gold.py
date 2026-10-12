from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(name=f"{CATALOG}.gold.daily_sales_mv", comment="Daily sales KPIs")
def daily_sales():
    return (
        spark.read.table(f"{CATALOG}.silver.orders_clean")
        .groupBy("order_date")
        .agg(
            F.count("*").alias("order_count"),
            F.countDistinct("customer_id").alias("customer_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
        )
    )
