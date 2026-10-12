from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.daily_payments_mv",
    comment="Payments per day and method",
)
def daily_payments():
    p = spark.read.table(f"{CATALOG}.silver.payments_clean")
    return p.groupBy("payment_date", "payment_method").agg(
        F.count("*").alias("payment_count"),
        F.sum(F.when(F.col("payment_status") == "SUCCESS", F.col("amount"))).alias("collected_amount"),
        F.sum(F.when(F.col("payment_status") == "REFUNDED", F.col("amount"))).alias("refunded_amount"),
        F.sum(F.when(F.col("payment_status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
    )


@dp.materialized_view(
    name=f"{CATALOG}.gold.order_payment_status_mv",
    comment="Reconciliation: order total vs successfully paid amount",
)
def order_payment_status():
    orders = spark.read.table(f"{CATALOG}.silver.orders_clean").select(
        "order_id", "customer_id", "order_date", "total_amount"
    )
    paid = (
        spark.read.table(f"{CATALOG}.silver.payments_clean")
        .where("payment_status = 'SUCCESS'")
        .groupBy("order_id")
        .agg(F.sum("amount").alias("paid_amount"))
    )
    return (
        orders.join(paid, "order_id", "left")
        .withColumn("paid_amount", F.coalesce(F.col("paid_amount"), F.lit(0)))
        .withColumn(
            "payment_status",
            F.when(F.col("paid_amount") >= F.col("total_amount"), "PAID")
             .when(F.col("paid_amount") > 0, "PARTIAL")
             .otherwise("UNPAID"),
        )
    )
