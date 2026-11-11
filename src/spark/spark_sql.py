#!/usr/bin/env python3
"""
PART 2 - Spark SQL analytics.

    python src/spark/spark_sql.py

Registers the cleaned Parquet as temporary views and answers five business
questions in SQL. The last one is the cross-store demonstration the brief asks
for: data that would live in MongoDB and in HBase, pulled into DataFrames and
joined in one SQL statement.
"""
import json
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
from spark_session import build

PQ = config.local_uri(os.path.join(config.DATA_DIR, "parquet"))
os.makedirs(config.RESULTS_DIR, exist_ok=True)


def run(spark, name, question, sql, show=10):
    print(f"\n{'=' * 70}\n{name}\n{question}\n{'=' * 70}")
    print(sql.strip())
    df = spark.sql(sql)
    df.show(show, truncate=False)
    rows = [r.asDict() for r in df.limit(100).collect()]
    with open(os.path.join(config.RESULTS_DIR, f"sql_{name}.json"), "w") as fh:
        json.dump(rows, fh, indent=2, default=str)
    print(f"-> results/sql_{name}.json")
    return rows


def main():
    spark = build("ECommerce-SparkSQL")
    spark.sparkContext.setLogLevel("ERROR")

    for view in ["users", "products", "categories", "subcategories",
                 "transactions", "transaction_items", "sessions", "page_views"]:
        spark.read.parquet(f"{PQ}/{view}").createOrReplaceTempView(view)
    print("Views:", spark.catalog.listTables() and
          [t.name for t in spark.catalog.listTables()])

    # ---------------------------------------------------------------- Q1
    run(spark, "revenue_by_category",
        "Q1. Which categories earn the money, and how concentrated is it?",
        """
        SELECT  p.category_id,
                c.category_name,
                COUNT(DISTINCT i.transaction_id)      AS orders,
                SUM(i.quantity)                       AS units,
                ROUND(SUM(i.subtotal), 2)             AS revenue,
                ROUND(AVG(i.unit_price), 2)           AS avg_price,
                ROUND(100 * SUM(i.subtotal) /
                      SUM(SUM(i.subtotal)) OVER (), 2) AS pct_of_revenue
        FROM        transaction_items i
        JOIN        products   p ON i.product_id  = p.product_id
        LEFT JOIN   categories c ON p.category_id = c.category_id
        WHERE  i.status IN ('completed', 'shipped', 'delivered')
        GROUP BY p.category_id, c.category_name
        ORDER BY revenue DESC
        """)

    # ---------------------------------------------------------------- Q2
    run(spark, "conversion_funnel",
        "Q2. Where do users fall out of the funnel?",
        """
        WITH stages AS (
            SELECT  COUNT(*)                                            AS sessions,
                    SUM(CASE WHEN products_viewed > 0 THEN 1 ELSE 0 END) AS viewed_product,
                    SUM(CASE WHEN cart_item_count > 0 THEN 1 ELSE 0 END) AS added_to_cart,
                    SUM(CASE WHEN converted THEN 1 ELSE 0 END)           AS converted
            FROM sessions
        )
        SELECT  '1. sessions'        AS stage, sessions       AS n,
                100.0                AS pct_of_sessions FROM stages
        UNION ALL
        SELECT  '2. viewed product', viewed_product,
                ROUND(100.0 * viewed_product / sessions, 2) FROM stages
        UNION ALL
        SELECT  '3. added to cart',  added_to_cart,
                ROUND(100.0 * added_to_cart  / sessions, 2) FROM stages
        UNION ALL
        SELECT  '4. converted',      converted,
                ROUND(100.0 * converted      / sessions, 2) FROM stages
        """)

    # ---------------------------------------------------------------- Q3
    run(spark, "device_performance",
        "Q3. Does the device people browse on change whether they buy?",
        """
        SELECT  device_type,
                COUNT(*)                                    AS sessions,
                ROUND(AVG(duration_seconds), 1)             AS avg_duration_s,
                ROUND(AVG(page_view_count), 2)              AS avg_page_views,
                SUM(CASE WHEN converted THEN 1 ELSE 0 END)  AS conversions,
                ROUND(100.0 * SUM(CASE WHEN converted THEN 1 ELSE 0 END)
                      / COUNT(*), 2)                        AS conversion_rate_pct
        FROM sessions
        GROUP BY device_type
        ORDER BY sessions DESC
        """)

    # ---------------------------------------------------------------- Q4
    run(spark, "hourly_demand",
        "Q4. When during the day does demand and conversion peak?",
        """
        SELECT  hour_of_day,
                COUNT(*)                                        AS sessions,
                SUM(CASE WHEN converted THEN 1 ELSE 0 END)      AS conversions,
                ROUND(100.0 * SUM(CASE WHEN converted THEN 1 ELSE 0 END)
                      / COUNT(*), 2)                            AS conversion_rate_pct,
                ROUND(AVG(duration_seconds), 0)                 AS avg_duration_s
        FROM sessions
        GROUP BY hour_of_day
        ORDER BY hour_of_day
        """, show=24)

    # ---------------------------------------------------------------- Q5
    # The cross-store query the brief asks for. In production:
    #   user_profile  comes from MongoDB  (document per user)
    #   engagement    comes from HBase    (session rows per user)
    #   spend         comes from Spark    (the transaction history)
    # Here all three are DataFrames, and the SQL is identical to what it would
    # be with real connectors - the join logic is the deliverable, not the
    # plumbing.
    run(spark, "cross_store_user_value",
        "Q5. CROSS-STORE: combine profile (MongoDB), engagement (HBase) and spend (Spark)",
        """
        WITH profile AS (             -- would be a MongoDB collection
            SELECT user_id, state, cohort_month, registration_date
            FROM users
        ),
        engagement AS (               -- would be an HBase scan per user
            SELECT  user_id,
                    COUNT(*)                        AS sessions,
                    ROUND(AVG(duration_seconds), 1) AS avg_session_s,
                    SUM(page_view_count)            AS total_page_views,
                    SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS converting_sessions
            FROM sessions
            GROUP BY user_id
        ),
        spend AS (                    -- Spark over the full transaction history
            SELECT  user_id,
                    COUNT(*)                  AS orders,
                    ROUND(SUM(total), 2)      AS lifetime_spend,
                    ROUND(AVG(total), 2)      AS avg_order_value
            FROM transactions
            WHERE status IN ('completed', 'shipped', 'delivered')
            GROUP BY user_id
        )
        SELECT  p.user_id, p.state, p.cohort_month,
                e.sessions, e.avg_session_s, e.total_page_views,
                s.orders, s.lifetime_spend, s.avg_order_value,
                ROUND(s.lifetime_spend / e.sessions, 2) AS revenue_per_session,
                ROUND(100.0 * e.converting_sessions / e.sessions, 2) AS session_conv_pct
        FROM            profile    p
        LEFT JOIN       engagement e ON p.user_id = e.user_id
        LEFT JOIN       spend      s ON p.user_id = s.user_id
        WHERE  s.lifetime_spend IS NOT NULL AND e.sessions IS NOT NULL
        ORDER BY s.lifetime_spend DESC
        """, show=15)

    spark.stop()


if __name__ == "__main__":
    main()
