from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.order_summary_mv",
    comment="Order summary: orders with line item counts and return stats",
)
def order_summary():
    orders = spark.read.table(f"{CATALOG}.silver.orders_clean")
    items = spark.read.table(f"{CATALOG}.silver.order_items_clean")
    returns = spark.read.table(f"{CATALOG}.silver.order_returns_clean")

    item_stats = items.groupBy("order_id").agg(
        F.count("*").alias("item_count"),
        F.sum("line_total").alias("item_total"),
    )
    return_stats = returns.groupBy("order_id").agg(
        F.count("*").alias("return_count"),
        F.sum("refund_amount").alias("refund_total"),
    )

    return (
        orders
        .join(item_stats, "order_id", "left")
        .join(return_stats, "order_id", "left")
        .withColumn("item_count", F.coalesce(F.col("item_count"), F.lit(0)))
        .withColumn("item_total", F.coalesce(F.col("item_total"), F.lit(0)))
        .withColumn("return_count", F.coalesce(F.col("return_count"), F.lit(0)))
        .withColumn("refund_total", F.coalesce(F.col("refund_total"), F.lit(0)))
        .withColumn("net_revenue", F.col("total_amount") - F.col("refund_total"))
    )
