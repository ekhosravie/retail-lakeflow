from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("catalog")
FEED   = "order_items"

# DQ rules loaded dynamically from ops.dq_rules — no hardcoded expressions.
_dq_rows = (spark.read.table(f"{CATALOG}.ops.dq_rules")
            .where(f"feed_name = '{FEED}' AND is_enabled = true")
            .select("rule_name", "rule_expr")
            .collect())
RAW_RULES = {r.rule_name: r.rule_expr for r in _dq_rows}
RULES   = {k: f"COALESCE({v}, FALSE)" for k, v in RAW_RULES.items()}
ALL_OK  = " AND ".join(f"({r})" for r in RULES.values())
FAILED  = "filter(array(" + ", ".join(f"IF({r}, NULL, '{k}')" for k, r in RULES.items()) + "), x -> x IS NOT NULL)"

# Column transforms loaded dynamically from ops.column_transforms — no hardcoded select expressions.
_ct_rows = (spark.read.table(f"{CATALOG}.ops.column_transforms")
            .where(f"feed_name = '{FEED}'")
            .orderBy("stage", "position")
            .collect())
_select_exprs = [F.expr(r.transform_expr).alias(r.column_name) for r in _ct_rows if r.stage == 'select']
_derived_cols = [(r.column_name, r.transform_expr) for r in _ct_rows if r.stage == 'derived']


@dp.view(name="order_items_typed")
def order_items_typed():
    df = spark.readStream.table(f"{CATALOG}.bronze.raw_order_items").select(*_select_exprs)
    for col_name, expr in _derived_cols:
        df = df.withColumn(col_name, F.expr(expr))
    return df


@dp.table(name=f"{CATALOG}.silver.order_items_clean", comment="Validated order line items")
@dp.expect_all_or_drop(RULES)
def order_items_clean():
    return spark.readStream.table("order_items_typed")


@dp.table(name=f"{CATALOG}.silver.order_items_quarantine", comment="Order items that failed validation")
def order_items_quarantine():
    return (spark.readStream.table("order_items_typed")
            .where(f"NOT ({ALL_OK})")
            .withColumn("failed_rules", F.expr(FAILED)))
