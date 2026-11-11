#!/usr/bin/env python3
"""
Convert the generated JSON arrays to JSON Lines, once, before Spark reads them.

    python src/spark/to_jsonl.py

WHY THIS STEP EXISTS
--------------------
The generator writes each file as ONE JSON array:  [ {...}, {...}, ... ]

Spark can read that, but only with multiLine=true, and a multiLine file cannot
be SPLIT. One 188 MB file becomes one task that must hold the entire parsed
array in memory. With 10 such files and 4 cores the job either crawls or runs
out of memory - both of which happened here before this step was added.

JSON Lines - one object per line - is splittable. Spark can give a 188 MB file
to four tasks reading different byte ranges, because a line boundary is a safe
place to split. The parse is also cheaper: no backtracking over a huge array.

The conversion is a single streaming pass in plain Python. It costs about two
minutes once; it saves that on every Spark job afterwards.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

OUT = os.path.join(config.DATA_DIR, "jsonl")


def normalise(record):
    """
    Turn cart_contents from an OBJECT KEYED BY PRODUCT ID into an ARRAY.

    The generator writes:   {"prod_00123": {"quantity": 2, "price": 129.99}}

    Spark infers a schema from the data, so a different product id in every
    document becomes a different FIELD. Across 2 million sessions that is a
    struct with 5,000 named fields - unusable, and it broke every query that
    touched the cart.

    Dynamic keys are data, not schema. Rewriting them as an array of
    {product_id, quantity, price} gives Spark one stable type to infer and
    makes the cart queryable with ordinary array functions.
    """
    cart = record.get("cart_contents")
    if isinstance(cart, dict):
        record["cart_items"] = [
            {"product_id": pid,
             "quantity": v.get("quantity"),
             "price": v.get("price")}
            for pid, v in cart.items()
        ]
        record.pop("cart_contents", None)
    return record


def convert(name):
    src = os.path.join(config.DATA_DIR, name)
    dst = os.path.join(OUT, name + "l")          # sessions_0.json -> .jsonl
    if os.path.exists(dst) and os.path.getmtime(dst) > os.path.getmtime(src):
        print(f"   {name:<22} already converted")
        return 0
    t0 = time.time()
    with open(src) as fh:
        records = json.load(fh)
    with open(dst, "w") as fh:
        for r in records:
            fh.write(json.dumps(normalise(r)))
            fh.write("\n")
    size = os.path.getsize(dst) / 1e6
    print(f"   {name:<22} {len(records):>9,} records  {size:>7.1f} MB  "
          f"{time.time() - t0:>5.1f}s")
    return len(records)


def main():
    os.makedirs(OUT, exist_ok=True)
    names = ["users.json", "products.json", "categories.json", "transactions.json"]
    names += sorted([f for f in os.listdir(config.DATA_DIR)
                     if f.startswith("sessions_") and f.endswith(".json")],
                    key=lambda f: int(f.split("_")[1].split(".")[0]))
    total = 0
    print(f"Converting JSON arrays to JSON Lines in {OUT}/\n")
    for n in names:
        total += convert(n)
    print(f"\n{total:,} records converted")


if __name__ == "__main__":
    main()
