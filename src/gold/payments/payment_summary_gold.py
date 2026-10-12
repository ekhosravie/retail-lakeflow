from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.payment_summary_mv",
    comment="Payment summary: payments with refund info",
)
def payment_summary():
    payments = spark.read.table(f"{CATALOG}.silver.payments_clean")
    refunds = spark.read.table(f"{CATALOG}.silver.payment_refunds_clean")

    refund_stats = refunds.groupBy("payment_id").agg(
        F.sum("refund_amount").alias("refunded_amount"),
        F.count("*").alias("refund_count"),
    )

    return (
        payments
        .join(refund_stats, "payment_id", "left")
        .withColumn("refunded_amount", F.coalesce(F.col("refunded_amount"), F.lit(0)))
        .withColumn("refund_count", F.coalesce(F.col("refund_count"), F.lit(0)))
        .withColumn("net_amount", F.col("amount") - F.col("refunded_amount"))
    )
