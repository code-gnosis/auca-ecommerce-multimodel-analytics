#!/usr/bin/env python3
"""
Two analytical aggregation pipelines on MongoDB.

    python src/mongodb/aggregations.py

PIPELINE 1  Revenue and product popularity by category
PIPELINE 2  Customer segmentation by purchasing behaviour (RFM-style)

Both run entirely inside MongoDB. Nothing is pulled into Python and grouped
there - the whole point of an aggregation pipeline is that the work happens
next to the data.

Results are printed and written to results/ as JSON for the report.
"""
import json
import os
import sys
from datetime import datetime

from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

os.makedirs(config.RESULTS_DIR, exist_ok=True)


def save(name, rows):
    path = os.path.join(config.RESULTS_DIR, f"{name}.json")
    with open(path, "w") as fh:
        json.dump(rows, fh, indent=2, default=str)
    print(f"   -> {len(rows)} rows to results/{name}.json")


# ---------------------------------------------------------------- pipeline 1

REVENUE_BY_CATEGORY = [
    # Only completed sales count as revenue. Cancelled orders are not revenue,
    # and counting them is the easiest way to overstate a business.
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},

    # One document per line item, so each product in a basket is counted.
    {"$unwind": "$items"},

    # Join to the product catalogue to reach the category.
    {"$lookup": {
        "from": "products",
        "localField": "items.product_id",
        "foreignField": "_id",
        "as": "product",
    }},
    {"$unwind": "$product"},

    {"$group": {
        "_id": {"category_id": "$product.category_id",
                "category_name": "$product.category_name"},
        "revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
        "line_items": {"$sum": 1},
        "distinct_products": {"$addToSet": "$items.product_id"},
        "distinct_buyers": {"$addToSet": "$user_id"},
    }},

    {"$project": {
        "_id": 0,
        "category_id": "$_id.category_id",
        "category_name": "$_id.category_name",
        "revenue": {"$round": ["$revenue", 2]},
        "units_sold": 1,
        "line_items": 1,
        "products_sold": {"$size": "$distinct_products"},
        "buyers": {"$size": "$distinct_buyers"},
        "revenue_per_buyer": {
            "$round": [{"$divide": ["$revenue", {"$size": "$distinct_buyers"}]}, 2]},
    }},
    {"$sort": {"revenue": -1}},
]

TOP_PRODUCTS = [
    {"$match": {"status": {"$in": ["completed", "shipped", "delivered"]}}},
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
        "orders": {"$sum": 1},
    }},
    {"$sort": {"revenue": -1}},
    {"$limit": 15},
    {"$lookup": {"from": "products", "localField": "_id",
                 "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$project": {
        "_id": 0,
        "product_id": "$_id",
        "name": "$p.name",
        "category_name": "$p.category_name",
        "revenue": {"$round": ["$revenue", 2]},
        "units_sold": 1,
        "orders": 1,
        "current_stock": "$p.current_stock",
    }},
]

# ---------------------------------------------------------------- pipeline 2

# The dataset has no age field, so segmentation is behavioural: how often a
# customer buys and how much they spend.
#
# A FINDING, not just a method. Fixed retail bands (1, 2-4, 5-9, 10-24, 25+)
# put ALL 10,000 customers in the top band, because the generator gives every
# user between 27 and 80 orders. There is no casual-browser segment in this
# data at all. $bucketAuto is the honest response: it chooses quantile
# boundaries from the data instead of imposing bands the data does not have.
USER_SEGMENTS = [
    {"$match": {"purchase_summary.order_count": {"$gt": 0}}},
    {"$bucketAuto": {
        "groupBy": "$purchase_summary.order_count",
        "buckets": 6,
        "output": {
            "customers": {"$sum": 1},
            "total_revenue": {"$sum": "$purchase_summary.lifetime_value"},
            "avg_lifetime_value": {"$avg": "$purchase_summary.lifetime_value"},
            "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
        },
    }},
    {"$project": {
        "_id": 0,
        "orders_from": "$_id.min",
        "orders_to": "$_id.max",
        "customers": 1,
        "total_revenue": {"$round": ["$total_revenue", 2]},
        "avg_lifetime_value": {"$round": ["$avg_lifetime_value", 2]},
        "avg_order_value": {"$round": ["$avg_order_value", 2]},
    }},
]

# Value tiers, using boundaries taken from the observed spend range
# (19,746 to 80,882). These are the bands a marketing team would act on.
VALUE_TIERS = [
    {"$match": {"purchase_summary.lifetime_value": {"$gt": 0}}},
    {"$bucket": {
        "groupBy": "$purchase_summary.lifetime_value",
        "boundaries": [0, 30000, 40000, 50000, 60000, 1000000],
        "default": "other",
        "output": {
            "customers": {"$sum": 1},
            "revenue": {"$sum": "$purchase_summary.lifetime_value"},
            "avg_orders": {"$avg": "$purchase_summary.order_count"},
            "avg_order_value": {"$avg": "$purchase_summary.avg_order_value"},
        },
    }},
    {"$project": {
        "_id": 0,
        "tier_floor": "$_id",
        "customers": 1,
        "revenue": {"$round": ["$revenue", 2]},
        "avg_orders": {"$round": ["$avg_orders", 1]},
        "avg_order_value": {"$round": ["$avg_order_value", 2]},
        "pct_of_customers": {"$round": [
            {"$multiply": [{"$divide": ["$customers", 10000]}, 100]}, 2]},
    }},
]

# $facet runs several independent aggregations over the same input in ONE pass
# over the collection, which is cheaper than three separate queries.
SEGMENT_OVERVIEW = [
    {"$facet": {
        "by_state": [
            {"$match": {"purchase_summary.order_count": {"$gt": 0}}},
            {"$group": {
                "_id": "$geo_data.state",
                "customers": {"$sum": 1},
                "revenue": {"$sum": "$purchase_summary.lifetime_value"},
            }},
            {"$project": {"_id": 0, "state": "$_id", "customers": 1,
                          "revenue": {"$round": ["$revenue", 2]}}},
            {"$sort": {"revenue": -1}},
            {"$limit": 10},
        ],
        "overall": [
            {"$group": {
                "_id": None,
                "all_users": {"$sum": 1},
                "buyers": {"$sum": {"$cond": [
                    {"$gt": ["$purchase_summary.order_count", 0]}, 1, 0]}},
                "revenue": {"$sum": "$purchase_summary.lifetime_value"},
            }},
            {"$project": {
                "_id": 0, "all_users": 1, "buyers": 1,
                "revenue": {"$round": ["$revenue", 2]},
                "buyer_rate_pct": {"$round": [
                    {"$multiply": [{"$divide": ["$buyers", "$all_users"]}, 100]}, 2]},
            }},
        ],
    }},
]


def show(title, rows, limit=10):
    print(f"\n=== {title} ===")
    if not rows:
        print("   (no rows)")
        return
    cols = list(rows[0].keys())
    print("   " + "  ".join(f"{c:>18}" for c in cols))
    for r in rows[:limit]:
        print("   " + "  ".join(f"{str(r.get(c, '')):>18}" for c in cols))


def main():
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]

    print("PIPELINE 1 - revenue and popularity by category")
    rows = list(db.transactions.aggregate(REVENUE_BY_CATEGORY, allowDiskUse=True))
    show("Revenue by category", rows)
    save("mongo_revenue_by_category", rows)

    top = list(db.transactions.aggregate(TOP_PRODUCTS, allowDiskUse=True))
    show("Top 15 products by revenue", top)
    save("mongo_top_products", top)

    print("\n\nPIPELINE 2 - customer segmentation")
    seg = list(db.users.aggregate(USER_SEGMENTS, allowDiskUse=True))
    show("Customers by purchase frequency (quantile buckets)", seg)
    save("mongo_user_segments", seg)

    tiers = list(db.users.aggregate(VALUE_TIERS, allowDiskUse=True))
    show("Customers by lifetime value tier", tiers)
    save("mongo_value_tiers", tiers)

    overview = list(db.users.aggregate(SEGMENT_OVERVIEW, allowDiskUse=True))[0]
    show("Top states by revenue", overview["by_state"])
    show("Overall", overview["overall"])
    save("mongo_segment_overview", overview["by_state"])
    save("mongo_overall", overview["overall"])


if __name__ == "__main__":
    main()
