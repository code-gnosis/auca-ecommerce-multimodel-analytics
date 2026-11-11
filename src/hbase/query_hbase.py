#!/usr/bin/env python3
"""
Queries against the HBase tables.

    python src/hbase/query_hbase.py

Each query below is here to show what the row-key design BUYS. Every one is a
short range scan on a contiguous block of rows - never a full table scan.
"""
import json
import os
import sys

import happybase

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

os.makedirs(config.RESULTS_DIR, exist_ok=True)


def decode(row):
    return {k.decode(): v.decode() for k, v in row.items()}


def q1_recent_sessions(conn, user_id, n=10):
    """
    QUERY 1 - the N most recent sessions for one user.

    row_prefix puts us at the start of that user's block, and because the
    timestamp is REVERSED the first rows returned are already the newest.
    We stop after n rows. No sort, no full scan.
    """
    table = conn.table("user_sessions")
    out = []
    for key, data in table.scan(row_prefix=f"{user_id}#".encode(), limit=n):
        d = decode(data)
        out.append({
            "row_key": key.decode(),
            "session_id": d.get("s:session_id"),
            "start_time": d.get("s:start_time"),
            "duration_s": d.get("s:duration"),
            "device": d.get("s:device"),
            "state": d.get("s:state"),
            "referrer": d.get("s:referrer"),
            "conversion": d.get("s:conversion"),
            "page_views": d.get("p:page_views"),
        })
    return out


def q2_summary_only(conn, user_id, n=5):
    """
    QUERY 2 - the same scan, but asking for the 's' family only.

    This is the payoff of splitting the families: the large 'p' cells are
    never read off disk. On a real cluster that is the difference between
    touching one store file and two.
    """
    table = conn.table("user_sessions")
    out = []
    for key, data in table.scan(row_prefix=f"{user_id}#".encode(),
                                columns=[b"s"], limit=n):
        out.append({"row_key": key.decode(), **decode(data)})
    return out


def q3_product_over_time(conn, product_id, start_date, end_date):
    """
    QUERY 3 - one product's daily metrics across a date range.

    The row key is product_id#date with the date FORWARD, so a date range is a
    contiguous scan between two row keys.
    """
    table = conn.table("product_daily")
    out = []
    for key, data in table.scan(row_start=f"{product_id}#{start_date}".encode(),
                                row_stop=f"{product_id}#{end_date}".encode()):
        d = decode(data)
        out.append({
            "date": key.decode().split("#")[1],
            "views": float(d.get("m:views", 0)),
            "cart_adds": float(d.get("m:cart_adds", 0)),
            "purchases": float(d.get("m:purchases", 0)),
            "units": float(d.get("m:units", 0)),
            "revenue": float(d.get("m:revenue", 0)),
        })
    return out


def q4_conversion_counts(conn, limit=20000):
    """
    QUERY 4 - conversion mix across a sample of sessions.

    An honest demonstration of what HBase is NOT for: there is no aggregation
    engine, so counting means scanning rows and counting in the client. This
    is precisely the work that belongs in Spark, and saying so is part of the
    technology-selection argument.
    """
    table = conn.table("user_sessions")
    counts, devices, seen = {}, {}, 0
    for _key, data in table.scan(columns=[b"s:conversion", b"s:device"], limit=limit):
        d = decode(data)
        c = d.get("s:conversion", "unknown")
        counts[c] = counts.get(c, 0) + 1
        dev = d.get("s:device", "unknown")
        devices[dev] = devices.get(dev, 0) + 1
        seen += 1
    return {"scanned": seen, "by_conversion": counts, "by_device": devices}


def main():
    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT)
    table = conn.table("user_sessions")

    # Pick a user that actually has rows, rather than hardcoding one.
    sample_user = None
    for key, _ in table.scan(limit=1):
        sample_user = key.decode().split("#")[0]
    if sample_user is None:
        print("user_sessions is empty - run load_hbase.py first")
        return

    results = {}

    print(f"=== QUERY 1: 10 most recent sessions for {sample_user} ===")
    rows = q1_recent_sessions(conn, sample_user, 10)
    for r in rows:
        print(f"   {r['start_time']}  {r['device']:<8} {r['conversion']:<12} "
              f"{r['page_views']:>3} views  {r['duration_s']:>5}s")
    print("   (returned newest-first with no sort - the reverse timestamp does it)")
    results["q1_recent_sessions"] = rows

    print(f"\n=== QUERY 2: summary family only, same user ===")
    rows2 = q2_summary_only(conn, sample_user, 5)
    print(f"   {len(rows2)} rows, "
          f"{len(rows2[0]) - 1 if rows2 else 0} cells each - no 'p' cells read")
    results["q2_summary_only"] = rows2

    ptable = conn.table("product_daily")
    sample_product = None
    for key, _ in ptable.scan(limit=1):
        sample_product = key.decode().split("#")[0]

    if sample_product:
        print(f"\n=== QUERY 3: {sample_product} day by day ===")
        series = q3_product_over_time(conn, sample_product, "2000-01-01", "2100-01-01")
        print(f"   {len(series)} days with activity")
        for r in series[:8]:
            print(f"   {r['date']}  views {r['views']:>5.0f}  "
                  f"cart {r['cart_adds']:>4.0f}  bought {r['purchases']:>4.0f}  "
                  f"revenue {r['revenue']:>10.2f}")
        results["q3_product_timeseries"] = {"product_id": sample_product,
                                            "series": series}

    print(f"\n=== QUERY 4: conversion mix over a scan ===")
    mix = q4_conversion_counts(conn)
    print(f"   scanned {mix['scanned']:,} rows")
    for k, v in sorted(mix["by_conversion"].items(), key=lambda x: -x[1]):
        print(f"      {k:<14} {v:>8,}")
    print("   (counting in the client - this is the job that belongs in Spark)")
    results["q4_conversion_mix"] = mix

    with open(os.path.join(config.RESULTS_DIR, "hbase_queries.json"), "w") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\n-> results/hbase_queries.json")
    conn.close()


if __name__ == "__main__":
    main()
