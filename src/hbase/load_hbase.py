#!/usr/bin/env python3
"""
Load a representative subset of the data into HBase.

    python src/hbase/load_hbase.py

Two tables, two different row-key designs, because they answer two different
questions. See src/hbase/hbase_schema.txt for the create commands.

  user_sessions    user_id # reverse_timestamp
                   "the most recent sessions for this user" -> short scan

  product_daily    product_id # yyyy-mm-dd
                   "how did this product do over March"     -> range scan
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

import happybase

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# A fixed, far-future millisecond value. Subtracting the real timestamp from it
# inverts the ordering while keeping the result a fixed-width positive number,
# so the zero-padded string still sorts correctly.
MAX_TS = 9999999999999


def reverse_ts(dt):
    """Zero-padded reverse timestamp, so newest sorts first."""
    return f"{MAX_TS - int(dt.timestamp() * 1000):013d}"


def session_row_key(user_id, start_time):
    return f"{user_id}#{reverse_ts(start_time)}".encode()


def session_files():
    files = [f for f in os.listdir(config.DATA_DIR)
             if f.startswith("sessions_") and f.endswith(".json")]
    return sorted(files, key=lambda f: int(f.split("_")[1].split(".")[0]))


def load_sessions(conn, limit):
    """user_sessions: one row per session, two column families."""
    table = conn.table("user_sessions")
    loaded = 0

    for fname in session_files():
        if loaded >= limit:
            break
        print(f"   reading {fname} ...", flush=True)
        with open(os.path.join(config.DATA_DIR, fname)) as fh:
            sessions = json.load(fh)

        # batch() buffers writes and sends them together. Writing row by row
        # would mean one RPC per session, which is the usual reason a HBase
        # load is slow.
        with table.batch(batch_size=2000) as batch:
            for s in sessions:
                if loaded >= limit:
                    break
                start = datetime.fromisoformat(s["start_time"])
                row = session_row_key(s["user_id"], start)

                geo = s.get("geo_data") or {}
                dev = s.get("device_profile") or {}
                cart = s.get("cart_contents") or {}

                batch.put(row, {
                    # 's' - the summary, always read
                    b"s:session_id":   str(s["session_id"]).encode(),
                    b"s:start_time":   s["start_time"].encode(),
                    b"s:end_time":     str(s.get("end_time", "")).encode(),
                    b"s:duration":     str(s.get("duration_seconds", 0)).encode(),
                    b"s:device":       str(dev.get("type", "")).encode(),
                    b"s:os":           str(dev.get("os", "")).encode(),
                    b"s:browser":      str(dev.get("browser", "")).encode(),
                    b"s:state":        str(geo.get("state", "")).encode(),
                    b"s:city":         str(geo.get("city", "")).encode(),
                    b"s:referrer":     str(s.get("referrer", "")).encode(),
                    b"s:conversion":   str(s.get("conversion_status", "")).encode(),
                    # 'p' - the detail, read only when drilling in
                    b"p:page_views":   str(len(s.get("page_views") or [])).encode(),
                    b"p:viewed":       ",".join(s.get("viewed_products") or []).encode(),
                    b"p:cart_items":   str(len(cart)).encode(),
                    b"p:cart_value":   str(round(sum(
                        c.get("price", 0) * c.get("quantity", 0)
                        for c in cart.values()), 2)).encode(),
                })
                loaded += 1
        print(f"      {loaded:,} sessions so far", flush=True)
    return loaded


def load_product_daily(conn, session_limit):
    """
    product_daily: views and purchases per product per day.

    Built by walking the same session subset plus the transactions, counting
    into a dict, then writing one row per (product, day). The cells are sparse
    on purpose: a product with no sales that day has no purchase cells at all,
    and HBase stores nothing for them.
    """
    table = conn.table("product_daily")
    metrics = defaultdict(lambda: defaultdict(float))

    scanned = 0
    for fname in session_files():
        if scanned >= session_limit:
            break
        with open(os.path.join(config.DATA_DIR, fname)) as fh:
            sessions = json.load(fh)
        for s in sessions:
            if scanned >= session_limit:
                break
            day = s["start_time"][:10]
            for pid in (s.get("viewed_products") or []):
                metrics[(pid, day)]["views"] += 1
            for pid in (s.get("cart_contents") or {}):
                metrics[(pid, day)]["cart_adds"] += 1
            scanned += 1

    with open(os.path.join(config.DATA_DIR, "transactions.json")) as fh:
        transactions = json.load(fh)
    for t in transactions:
        if t.get("status") == "cancelled":
            continue
        day = t["timestamp"][:10]
        for item in t.get("items", []):
            key = (item["product_id"], day)
            metrics[key]["purchases"] += 1
            metrics[key]["units"] += item.get("quantity", 0)
            metrics[key]["revenue"] += item.get("subtotal", 0.0)

    with table.batch(batch_size=2000) as batch:
        for (pid, day), m in metrics.items():
            cells = {f"m:{k}".encode(): str(round(v, 2)).encode()
                     for k, v in m.items() if v}
            batch.put(f"{pid}#{day}".encode(), cells)
    return len(metrics)


def main():
    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT)
    print(f"Connected to HBase at {config.HBASE_HOST}:{config.HBASE_PORT}")
    print("Tables:", [t.decode() for t in conn.tables()], "\n")

    print(f"Loading up to {config.HBASE_SESSION_LIMIT:,} sessions into user_sessions")
    n = load_sessions(conn, config.HBASE_SESSION_LIMIT)
    print(f"   {n:,} session rows written\n")

    print("Building product_daily from sessions + transactions")
    rows = load_product_daily(conn, config.HBASE_SESSION_LIMIT)
    print(f"   {rows:,} product-day rows written")

    conn.close()


if __name__ == "__main__":
    main()
