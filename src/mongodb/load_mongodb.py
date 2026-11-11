#!/usr/bin/env python3
"""
Load the e-commerce dataset into MongoDB.

    python src/mongodb/load_mongodb.py

DOCUMENT MODEL - four collections
---------------------------------
products      Product plus the category and subcategory NAMES copied in.
              Denormalised on purpose: every product query wants the category
              label, and a $lookup on every read is far more expensive than
              storing one extra string per product. Categories change rarely.

users         Profile plus an embedded purchase summary (orders, lifetime
              spend, first and last order). The summary is computed once at
              load time so the dashboard-style query "show me this customer"
              is a single document read with no joins.

transactions  Kept as generated, with the line items embedded in an array.
              This is the natural fit for the document model: a transaction is
              always read as a whole, and the items have no life of their own.

sessions      A representative subset, with the page_views array embedded.
              The whole point of the document model here is that one session
              document holds the entire user journey - entry, every page view,
              cart contents and the outcome - with no joins at all.
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

from pymongo import ASCENDING, DESCENDING, MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def read_json(name):
    path = os.path.join(config.DATA_DIR, name)
    with open(path) as fh:
        return json.load(fh)


def session_files():
    """Every sessions_N.json, in numeric order."""
    files = [f for f in os.listdir(config.DATA_DIR)
             if f.startswith("sessions_") and f.endswith(".json")]
    return sorted(files, key=lambda f: int(f.split("_")[1].split(".")[0]))


def build_category_lookup(categories):
    """category_id -> name, and subcategory_id -> (name, profit_margin)."""
    cat_name, sub_info = {}, {}
    for cat in categories:
        cat_name[cat["category_id"]] = cat["name"]
        for sub in cat.get("subcategories", []):
            sub_info[sub["subcategory_id"]] = (sub["name"], sub.get("profit_margin"))
    return cat_name, sub_info


def main():
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_DB]

    print(f"Loading into {config.MONGO_URI}/{config.MONGO_DB}\n")

    # ---------------------------------------------------------- categories
    categories = read_json("categories.json")
    db.categories.drop()
    db.categories.insert_many(categories)
    cat_name, sub_info = build_category_lookup(categories)
    print(f"categories   {len(categories):>9,}")

    # ---------------------------------------------------------- products
    # Copy the category name onto each product. One redundant string removes a
    # $lookup from every product query we will ever write.
    products = read_json("products.json")
    for p in products:
        p["_id"] = p["product_id"]
        p["category_name"] = cat_name.get(p.get("category_id"))
        if p.get("subcategory_id") in sub_info:
            name, margin = sub_info[p["subcategory_id"]]
            p["subcategory_name"] = name
            p["profit_margin"] = margin
        # current_price is the last entry in price_history, which saves every
        # reader from sorting the array themselves.
        history = p.get("price_history") or []
        p["current_price"] = history[-1]["price"] if history else p.get("base_price")
    db.products.drop()
    db.products.insert_many(products)
    db.products.create_index([("category_id", ASCENDING)])
    db.products.create_index([("is_active", ASCENDING), ("current_stock", ASCENDING)])
    print(f"products     {len(products):>9,}")

    # ---------------------------------------------------------- transactions
    transactions = read_json("transactions.json")
    for t in transactions:
        t["_id"] = t["transaction_id"]
        t["timestamp"] = datetime.fromisoformat(t["timestamp"])
    db.transactions.drop()
    db.transactions.insert_many(transactions, ordered=False)
    db.transactions.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    db.transactions.create_index([("status", ASCENDING)])
    print(f"transactions {len(transactions):>9,}")

    # ---------------------------------------------------------- users
    # Embed a purchase summary so "show me this customer" is ONE read.
    summary = defaultdict(lambda: {"orders": 0, "spend": 0.0,
                                   "first": None, "last": None})
    for t in transactions:
        if t.get("status") == "cancelled":
            continue
        s = summary[t["user_id"]]
        s["orders"] += 1
        s["spend"] += t.get("total", 0.0)
        ts = t["timestamp"]
        s["first"] = ts if s["first"] is None or ts < s["first"] else s["first"]
        s["last"] = ts if s["last"] is None or ts > s["last"] else s["last"]

    users = read_json("users.json")
    for u in users:
        u["_id"] = u["user_id"]
        u["registration_date"] = datetime.fromisoformat(u["registration_date"])
        u["last_active"] = datetime.fromisoformat(u["last_active"])
        s = summary.get(u["user_id"])
        u["purchase_summary"] = {
            "order_count": s["orders"] if s else 0,
            "lifetime_value": round(s["spend"], 2) if s else 0.0,
            "avg_order_value": round(s["spend"] / s["orders"], 2) if s and s["orders"] else 0.0,
            "first_order": s["first"] if s else None,
            "last_order": s["last"] if s else None,
        }
    db.users.drop()
    db.users.insert_many(users)
    db.users.create_index([("purchase_summary.lifetime_value", DESCENDING)])
    db.users.create_index([("geo_data.state", ASCENDING)])
    print(f"users        {len(users):>9,}")

    # ---------------------------------------------------------- sessions
    # A subset. Spark reads every file; MongoDB holds enough to demonstrate
    # the document model and answer journey questions.
    db.sessions.drop()
    loaded, batch = 0, []
    for fname in session_files():
        if loaded >= config.MONGO_SESSION_LIMIT:
            break
        for s in read_json(fname):
            if loaded >= config.MONGO_SESSION_LIMIT:
                break
            s["_id"] = s["session_id"]
            s["start_time"] = datetime.fromisoformat(s["start_time"])
            s["end_time"] = datetime.fromisoformat(s["end_time"])
            batch.append(s)
            loaded += 1
            if len(batch) >= 10000:
                db.sessions.insert_many(batch, ordered=False)
                batch = []
    if batch:
        db.sessions.insert_many(batch, ordered=False)
    db.sessions.create_index([("user_id", ASCENDING), ("start_time", DESCENDING)])
    db.sessions.create_index([("conversion_status", ASCENDING)])
    db.sessions.create_index([("viewed_products", ASCENDING)])
    print(f"sessions     {loaded:>9,}  (subset of the full dataset)")

    print("\nCollections now in the database:")
    for name in sorted(db.list_collection_names()):
        print(f"   {name:<14} {db[name].estimated_document_count():>9,} documents")
    client.close()


if __name__ == "__main__":
    main()
