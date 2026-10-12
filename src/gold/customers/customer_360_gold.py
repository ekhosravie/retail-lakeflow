from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.customer_360_mv",
    comment="Customer 360: current customers with current address",
)
def customer_360():
    customers = spark.read.table(f"{CATALOG}.gold.dim_customer_current")
    addresses = (
        spark.read.table(f"{CATALOG}.silver.customer_addresses_clean")
        .where("is_current = true")
    )

    return (
        customers.alias("c")
        .join(addresses.alias("a"), "customer_id", "left")
        .select(
            "c.customer_id", "c.first_name", "c.last_name", "c.email", "c.loyalty_tier",
            "a.address_type", "a.street", "a.city", "a.country",
        )
    )
