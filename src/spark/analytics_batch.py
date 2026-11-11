#!/usr/bin/env python3
"""
PART 2 - Spark batch job 2: product affinity and cohort analysis.

    python src/spark/analytics_batch.py

The brief asks for ONE of product recommendation indicators or cohort analysis.
Both are implemented here because they answer different business questions and
together they exercise the two Spark patterns that matter: a self-join with
aggregation, and a time-based pivot.

A  PRODUCT AFFINITY - "users who bought X also bought Y"
B  COHORT ANALYSIS  - do customers who joined in month M keep spending?

Reads the Parquet written by batch_clean.py, never the raw JSON.
"""
import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from spark_session import build

PQ = config.local_uri(os.path.join(config.DATA_DIR, "parquet"))
os.makedirs(config.RESULTS_DIR, exist_ok=True)

COMPLETED = ["completed", "shipped", "delivered"]

# Minimum times two products must appear together before the pair is reported.
#
# This had to be lowered from 5 to 2, and the reason is a finding in itself:
# the catalogue holds 5,000 products and the average basket holds 1.9 items, so
# there are ~12.5 million possible pairs chasing ~440,000 baskets. Genuine
# co-occurrence is therefore RARE - at a threshold of 5 exactly one pair
# qualified. Real retail data is far more concentrated, because a few hundred
# products carry most of the volume. See the limitations section of the report.
MIN_PAIR_COUNT = int(os.environ.get("MIN_PAIR_COUNT", "2"))


def save(df, name, n=50):
    rows = [r.asDict() for r in df.limit(n).collect()]
    import json
    with open(os.path.join(config.RESULTS_DIR, f"{name}.json"), "w") as fh:
        json.dump(rows, fh, indent=2, default=str)
    print(f"   -> results/{name}.json ({len(rows)} rows)")
    return rows


def banner(t):
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}", flush=True)


def product_affinity(spark, items, products):
    """
    A - "users who bought X also bought Y".

    Market basket analysis by self-joining the line items on transaction_id.
    The join condition keeps product_a < product_b so each pair appears once
    rather than twice, which halves the shuffle.

    Three measures, because raw co-occurrence is misleading on its own:
      support     how often the pair appears at all
      confidence  P(Y | X) - of the baskets with X, how many had Y
      lift        confidence / P(Y) - how much MORE likely Y is given X.
                  Lift > 1 means a real association; lift near 1 means the
                  pair just happens to contain two popular products.
    """
    banner("A. PRODUCT AFFINITY - users who bought X also bought Y")

    baskets = (items.filter(F.col("status").isin(COMPLETED))
                    .select("transaction_id", "product_id").distinct())

    total_baskets = baskets.select("transaction_id").distinct().count()
    print(f"   {total_baskets:,} completed baskets")

    product_counts = (baskets.groupBy("product_id")
                             .agg(F.countDistinct("transaction_id").alias("baskets")))

    a = baskets.withColumnRenamed("product_id", "product_a")
    b = baskets.withColumnRenamed("product_id", "product_b")

    pairs = (a.join(b, "transaction_id")
              .filter(F.col("product_a") < F.col("product_b"))   # each pair once
              .groupBy("product_a", "product_b")
              .agg(F.count("*").alias("pair_count"))
              .filter(F.col("pair_count") >= MIN_PAIR_COUNT))

    affinity = (pairs
        .join(product_counts.withColumnRenamed("product_id", "product_a")
                            .withColumnRenamed("baskets", "a_baskets"), "product_a")
        .join(product_counts.withColumnRenamed("product_id", "product_b")
                            .withColumnRenamed("baskets", "b_baskets"), "product_b")
        .withColumn("support", F.round(F.col("pair_count") / F.lit(total_baskets), 6))
        .withColumn("confidence_a_to_b",
                    F.round(F.col("pair_count") / F.col("a_baskets"), 4))
        .withColumn("lift", F.round(
            (F.col("pair_count") / F.col("a_baskets")) /
            (F.col("b_baskets") / F.lit(total_baskets)), 3))
        .join(products.select(F.col("product_id").alias("product_a"),
                              F.col("name").alias("name_a")), "product_a")
        .join(products.select(F.col("product_id").alias("product_b"),
                              F.col("name").alias("name_b")), "product_b")
        .select("product_a", "name_a", "product_b", "name_b",
                "pair_count", "support", "confidence_a_to_b", "lift")
        .orderBy(F.desc("lift")))

    # TWO rankings, because lift alone is misleading on sparse data.
    #
    # Sorting by lift puts pairs with pair_count = 2 at the top with lift over
    # 1,000. That is arithmetic, not insight: when both products are nearly
    # never bought, ANY co-occurrence looks enormously more likely than chance.
    # The volume ranking below is the defensible one - it shows the pairs that
    # actually happen often enough to act on.
    print("\n   Ranked by LIFT (statistically fragile at this sparsity):")
    affinity.show(10, truncate=40)
    save(affinity, "spark_product_affinity", 50)

    by_volume = affinity.orderBy(F.desc("pair_count"), F.desc("lift"))
    print("\n   Ranked by CO-OCCURRENCE COUNT (the actionable view):")
    by_volume.show(10, truncate=40)
    save(by_volume, "spark_product_affinity_by_volume", 50)

    stats = affinity.agg(
        F.count("*").alias("pairs_above_threshold"),
        F.max("pair_count").alias("max_pair_count"),
        F.round(F.avg("pair_count"), 2).alias("avg_pair_count")).collect()[0].asDict()
    print(f"\n   {stats}")
    import json as _json
    with open(os.path.join(config.RESULTS_DIR, "spark_affinity_stats.json"), "w") as fh:
        _json.dump(stats, fh, indent=2)
    return affinity


def cohort_analysis(spark, users, transactions):
    """
    B - cohort analysis.

    Group customers by the month they registered, then track what that same
    group spent in each month afterwards. The question is retention: does a
    cohort keep buying, or does it spend once and go quiet?

    months_since is the key column: 0 is the registration month, 1 the month
    after, and so on. That alignment is what makes cohorts comparable - every
    cohort's month 0 sits in the same column regardless of calendar date.
    """
    banner("B. COHORT ANALYSIS - spending by registration cohort")

    tx = (transactions.filter(F.col("status").isin(COMPLETED))
                      .select("user_id", "timestamp", "total", "transaction_id"))

    joined = (tx.join(users.select("user_id", "registration_date", "cohort_month"),
                      "user_id")
                .withColumn("months_since",
                            F.months_between(F.col("timestamp"),
                                             F.col("registration_date")).cast("int"))
                .filter(F.col("months_since") >= 0))

    cohort_size = (users.groupBy("cohort_month")
                        .agg(F.countDistinct("user_id").alias("cohort_users")))

    cohort = (joined.groupBy("cohort_month", "months_since")
                    .agg(F.countDistinct("user_id").alias("active_users"),
                         F.countDistinct("transaction_id").alias("orders"),
                         F.round(F.sum("total"), 2).alias("revenue"))
                    .join(cohort_size, "cohort_month")
                    .withColumn("retention_pct",
                                F.round(F.col("active_users") /
                                        F.col("cohort_users") * 100, 2))
                    .withColumn("revenue_per_user",
                                F.round(F.col("revenue") / F.col("active_users"), 2))
                    .orderBy("cohort_month", "months_since"))

    cohort.show(20, truncate=False)
    save(cohort, "spark_cohort_analysis", 200)
    return cohort


def main():
    spark = build("ECommerce-Analytics")
    spark.sparkContext.setLogLevel("ERROR")

    items = spark.read.parquet(PQ + "/transaction_items")
    products = spark.read.parquet(PQ + "/products")
    users = spark.read.parquet(PQ + "/users")
    transactions = spark.read.parquet(PQ + "/transactions")

    # Both jobs read the line items and the transactions repeatedly.
    items.cache()
    transactions.cache()

    product_affinity(spark, items, products)
    cohort_analysis(spark, users, transactions)

    banner("EXECUTION PLAN - narrow vs wide")
    narrow = items.filter(F.col("quantity") > 1).select("product_id", "subtotal")
    wide = items.groupBy("product_id").agg(F.sum("subtotal"))
    print("NARROW (filter + select) - no shuffle expected:")
    narrow.explain()
    print("\nWIDE (groupBy + agg) - 'Exchange' is the shuffle:")
    wide.explain()

    spark.stop()


if __name__ == "__main__":
    main()
