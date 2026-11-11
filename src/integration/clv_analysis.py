#!/usr/bin/env python3
"""
PART 3 - Integrated analytics: Customer Lifetime Value.

    python src/integration/clv_analysis.py

BUSINESS QUESTION
-----------------
Which customers are worth keeping, and which are only browsing? Marketing
needs a ranked list with a reason attached, not just a spend total.

WHY THIS NEEDS ALL THREE SYSTEMS
--------------------------------
  MongoDB   user profile + embedded purchase summary.
            One document read per customer, already aggregated at load time.
            Document model, because a customer is naturally one document.

  HBase     engagement. Sessions per customer, how long, how recently.
            A short range scan per user on user_sessions, which is exactly
            what the user_id#reverse_timestamp row key was designed for.

  Spark /   the join and the scoring. Neither MongoDB nor HBase can join
  Python    across stores, and HBase cannot aggregate at all.

This script really does connect to both databases. Nothing is simulated.

THE MODEL
---------
    CLV = avg_order_value x purchase_frequency x expected_lifetime_months

  - avg_order_value and order_count come from MongoDB
  - purchase_frequency is orders per active month
  - expected_lifetime is a simple retention proxy built from engagement:
    a customer who is still browsing is assumed to stay longer than one who
    has gone quiet. The weights are a stated judgement, not a measurement,
    and the raw inputs are reported next to every score so the effect of the
    weighting is visible.
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

import happybase
from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

os.makedirs(config.RESULTS_DIR, exist_ok=True)

BASE_LIFETIME_MONTHS = 6.0      # assumed for a customer with no engagement signal
MAX_LIFETIME_MONTHS = 24.0


def from_mongodb():
    """
    STEP 1 - profile and spend, from MongoDB.

    One aggregation pipeline. The purchase summary was embedded at load time,
    so this reads documents rather than recomputing from transactions.
    """
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    pipeline = [
        {"$match": {"purchase_summary.order_count": {"$gt": 0}}},
        {"$project": {
            "_id": 0,
            "user_id": 1,
            "state": "$geo_data.state",
            "registration_date": 1,
            "last_active": 1,
            "orders": "$purchase_summary.order_count",
            "lifetime_spend": "$purchase_summary.lifetime_value",
            "avg_order_value": "$purchase_summary.avg_order_value",
            "first_order": "$purchase_summary.first_order",
            "last_order": "$purchase_summary.last_order",
        }},
    ]
    rows = list(db.users.aggregate(pipeline, allowDiskUse=True))
    print(f"   MongoDB  -> {len(rows):,} purchasing customers")
    return {r["user_id"]: r for r in rows}


def from_hbase():
    """
    STEP 2 - engagement, from HBase.

    A single scan over user_sessions reading only the 's' column family.
    Row keys are user_id#reverse_timestamp, so every user's sessions arrive
    together and the FIRST row seen for a user is their most recent session.
    """
    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT)
    table = conn.table("user_sessions")

    engagement = defaultdict(lambda: {"sessions": 0, "duration": 0.0,
                                      "converting": 0, "last_seen": None})
    for key, data in table.scan(columns=[b"s:duration", b"s:conversion",
                                         b"s:start_time"]):
        user_id = key.decode().split("#")[0]
        e = engagement[user_id]
        e["sessions"] += 1
        try:
            e["duration"] += float(data.get(b"s:duration", b"0").decode())
        except ValueError:
            pass
        if data.get(b"s:conversion", b"").decode() == "converted":
            e["converting"] += 1
        # Rows arrive newest-first per user, so the first one wins.
        if e["last_seen"] is None:
            e["last_seen"] = data.get(b"s:start_time", b"").decode()
    conn.close()
    print(f"   HBase    -> engagement for {len(engagement):,} users")
    return engagement


def score(profile, engagement):
    """STEP 3 - join and score. This is the part neither database can do."""
    results = []
    for user_id, p in profile.items():
        e = engagement.get(user_id)
        sessions = e["sessions"] if e else 0
        avg_duration = (e["duration"] / sessions) if sessions else 0.0
        converting = e["converting"] if e else 0

        # Active months between first and last order, floored at 1 so a
        # single-order customer does not divide by zero.
        first, last = p.get("first_order"), p.get("last_order")
        months = 1.0
        if isinstance(first, datetime) and isinstance(last, datetime):
            months = max((last - first).days / 30.0, 1.0)
        frequency = p["orders"] / months

        # Engagement raises the assumed lifetime. A customer with many
        # sessions and some that convert is assumed to stay longer.
        engagement_factor = 1.0 + min(sessions / 10.0, 2.0) \
                                + min(converting / 3.0, 1.0)
        lifetime = min(BASE_LIFETIME_MONTHS * engagement_factor,
                       MAX_LIFETIME_MONTHS)

        clv = p["avg_order_value"] * frequency * lifetime

        results.append({
            "user_id": user_id,
            "state": p.get("state"),
            # from MongoDB
            "orders": p["orders"],
            "lifetime_spend": round(p["lifetime_spend"], 2),
            "avg_order_value": round(p["avg_order_value"], 2),
            # from HBase
            "sessions": sessions,
            "avg_session_seconds": round(avg_duration, 1),
            "converting_sessions": converting,
            # computed here
            "active_months": round(months, 2),
            "orders_per_month": round(frequency, 3),
            "assumed_lifetime_months": round(lifetime, 2),
            "predicted_clv": round(clv, 2),
        })
    results.sort(key=lambda r: -r["predicted_clv"])
    return results


def main():
    print("CUSTOMER LIFETIME VALUE - MongoDB + HBase + Python\n")
    print("STEP 1/3  profile and spend")
    profile = from_mongodb()
    print("STEP 2/3  engagement")
    engagement = from_hbase()
    print("STEP 3/3  join and score")
    results = score(profile, engagement)

    with_engagement = [r for r in results if r["sessions"] > 0]
    print(f"\n   {len(results):,} customers scored, "
          f"{len(with_engagement):,} with engagement data from HBase")

    print("\n=== TOP 15 CUSTOMERS BY PREDICTED CLV ===")
    hdr = ("user_id", "state", "orders", "spend", "AOV", "sess",
           "ord/mo", "life_mo", "CLV")
    print("   " + "".join(f"{h:>13}" for h in hdr))
    for r in results[:15]:
        print("   " + "".join(f"{v:>13}" for v in (
            r["user_id"], r["state"], r["orders"], r["lifetime_spend"],
            r["avg_order_value"], r["sessions"], r["orders_per_month"],
            r["assumed_lifetime_months"], r["predicted_clv"])))

    # The actionable split: high value but going quiet.
    engaged = [r for r in with_engagement if r["sessions"] >= 5]
    quiet = [r for r in with_engagement
             if r["sessions"] < 5 and r["lifetime_spend"] > 0]
    summary = {
        "customers_scored": len(results),
        "with_hbase_engagement": len(with_engagement),
        "total_predicted_clv": round(sum(r["predicted_clv"] for r in results), 2),
        "total_actual_spend": round(sum(r["lifetime_spend"] for r in results), 2),
        "avg_clv": round(sum(r["predicted_clv"] for r in results) / len(results), 2),
        "engaged_customers": len(engaged),
        "at_risk_high_spend": len(quiet),
    }
    print("\n=== SUMMARY ===")
    for k, v in summary.items():
        print(f"   {k:<28} {v:>16,}")

    with open(os.path.join(config.RESULTS_DIR, "clv_top200.json"), "w") as fh:
        json.dump(results[:200], fh, indent=2, default=str)
    with open(os.path.join(config.RESULTS_DIR, "clv_summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print("\n-> results/clv_top200.json, results/clv_summary.json")


if __name__ == "__main__":
    main()
