#!/usr/bin/env python3
"""Re-derive one day's advertised price changes straight from the snapshots.

No database, no pipeline, no trust in any number printed in a README. Reads two
raw daily files out of history/<brand>/ and diffs them on (store_id, sku).

    python verify_day.py publicstorage 2026-10-03 2026-10-04
"""
import json, sys
from collections import Counter


def load(brand, date):
    with open(f"history/{brand}/{date}.json", encoding="utf-8") as fh:
        stores = json.load(fh)
    prices = {}
    for st in stores:
        for u in st.get("units") or []:
            sku = u.get("sku")
            if sku:
                prices[(st["store_id"], sku)] = u.get("price")
    return stores, prices


def main(brand, d0, d1):
    s0, p0 = load(brand, d0)
    s1, p1 = load(brand, d1)
    shared = p0.keys() & p1.keys()

    moves = Counter()
    for k in shared:
        a, b = p0[k], p1[k]
        if a is None or b is None or a == b:
            moves["unchanged" if a == b else "unpriced"] += 1
        else:
            moves["up" if b > a else "down"] += 1

    changed = moves["up"] + moves["down"]
    print(f"{brand}  {d0} -> {d1}")
    print(f"  stores            {len(s0):>7,} -> {len(s1):,}")
    print(f"  SKUs in both days {len(shared):>7,}")
    print(f"  price changes     {changed:>7,}  ({changed / len(shared):.2%} of panel)")
    print(f"    up              {moves['up']:>7,}")
    print(f"    down            {moves['down']:>7,}")
    print(f"  unchanged         {moves['unchanged']:>7,}")
    print(f"  SKUs added        {len(p1.keys() - p0.keys()):>7,}")
    print(f"  SKUs dropped      {len(p0.keys() - p1.keys()):>7,}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:])
