from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")


@dp.materialized_view(
    name=f"{CATALOG}.gold.product_catalog_mv",
    comment="Product catalog: products with category hierarchy",
)
def product_catalog():
    products = spark.read.table(f"{CATALOG}.gold.dim_product")
    categories = (
        spark.read.table(f"{CATALOG}.silver.product_categories_clean")
        .where("is_active = true")
    )

    return (
        products.alias("p")
        .join(categories.alias("c"), F.col("p.category") == F.col("c.category_name"), "left")
        .select(
            "p.product_id", "p.product_name", "p.category", "p.brand",
            "p.unit_cost", "p.list_price", "p.is_active",
            F.col("c.parent_category").alias("parent_category"),
        )
    )
