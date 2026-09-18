#!/usr/bin/env python3
"""Composition-controlled test of the 2026-09-13/14 Public Storage sequence.

Compares count-expanded and flat-count SKUs inside increasingly narrow cells:
ZIP3 market x size, city/state x size, and store x size.  The primary outcome
is log change in modeled four-month customer cost; secondary outcomes are a
09-14 price cut and promotion change.  ZIP3 is the market definition already
used by this project and is a tighter geographic cell than a conventional MSA.
"""
from __future__ import annotations

import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "history" / "publicstorage"


def load(day: str):
    stores = json.loads((HISTORY / f"{day}.json").read_text(encoding="utf-8"))
    units, meta = {}, {}
    for store in stores:
        sid = str(store.get("store_id") or "")
        meta[sid] = {
            "zip3": str(store.get("zip") or "")[:3],
            "city_state": (str(store.get("city") or "").strip().lower(),
                           str(store.get("state") or "").strip().upper()),
        }
        for unit in store.get("units") or []:
            sku = str(unit.get("sku") or "")
            if sid and sku:
                units[(sid, sku)] = unit
    return units, meta


def count(unit):
    return int(unit.get("count") or 0)


def price(unit):
    value = unit.get("price")
    return float(value) if isinstance(value, (int, float)) and value > 0 else None


def promo(unit):
    return str(unit.get("promo") or unit.get("promo2") or "").strip()


def cost(unit):
    p = price(unit)
    if p is None:
        return None
    text = promo(unit).lower()
    if "50% off first 4" in text:
        return p * 2
    if "40% off for 4" in text:
        return p * 2.4
    if "30% off for 4" in text:
        return p * 2.8
    if "$1 first month" in text or "$1 special" in text:
        return 1 + p * 3
    if "first month 50% off" in text:
        return p * 3.5
    if "2nd month free" in text:
        return p * 3
    if not text:
        return p * 4
    return None


def weighted_effect(cells):
    """Weighted mean within-cell expanded-minus-flat difference."""
    numerator = denominator = 0.0
    for expanded, flat in cells:
        # Harmonic sample-size weight: large only when both arms are populated.
        weight = len(expanded) * len(flat) / (len(expanded) + len(flat))
        numerator += weight * (statistics.fmean(expanded) - statistics.fmean(flat))
        denominator += weight
    return numerator / denominator


def interval(cells, seed=20260915, draws=2000):
    rng = random.Random(seed)
    values = []
    for _ in range(draws):
        sample = [cells[rng.randrange(len(cells))] for _ in cells]
        values.append(weighted_effect(sample))
    values.sort()
    return values[int(draws * .025)], values[int(draws * .975)]


def report(name, rows, stratum, only_10x10=False):
    grouped = defaultdict(lambda: [[], [], [], [], [], []])
    # Each cell holds expanded/flat for: log cost, cut flag, promo-change flag.
    for row in rows:
        if only_10x10 and row["size"] != "10x10":
            continue
        key = stratum(row)
        if key is None:
            continue
        arm = 0 if row["expanded"] else 1
        grouped[key][arm].append(row["log_cost"])
        grouped[key][2 + arm].append(row["cut"])
        grouped[key][4 + arm].append(row["promo_changed"])

    eligible = [values for values in grouped.values() if values[0] and values[1]]
    cost_cells = [(v[0], v[1]) for v in eligible]
    cut_cells = [(v[2], v[3]) for v in eligible]
    promo_cells = [(v[4], v[5]) for v in eligible]
    effect = weighted_effect(cost_cells)
    low, high = interval(cost_cells)
    cut_effect = weighted_effect(cut_cells)
    promo_effect = weighted_effect(promo_cells)
    expanded_n = sum(len(v[0]) for v in eligible)
    flat_n = sum(len(v[1]) for v in eligible)
    cell_diffs = [statistics.fmean(a) - statistics.fmean(b) for a, b in cost_cells]

    print(f"\n{name}")
    print(f"  eligible matched cells: {len(eligible):,}")
    print(f"  observations in those cells: expanded {expanded_n:,}; flat {flat_n:,}")
    print(f"  within-cell four-month cost gap: {math.expm1(effect):+.2%} "
          f"(cell-bootstrap 95% {math.expm1(low):+.2%} to {math.expm1(high):+.2%})")
    print(f"  within-cell price-cut gap: {cut_effect:+.1%} points")
    print(f"  within-cell promo-change gap: {promo_effect:+.1%} points")
    print(f"  cells where expanded arm had larger cost growth: "
          f"{sum(d > 0 for d in cell_diffs):,}/{len(cell_diffs):,} "
          f"({sum(d > 0 for d in cell_diffs)/len(cell_diffs):.1%})")


def main():
    d12, meta = load("2026-09-12")
    d13, _ = load("2026-09-13")
    d14, _ = load("2026-09-14")
    common = set(d12) & set(d13) & set(d14)
    rows = []
    for key in common:
        delta = count(d13[key]) - count(d12[key])
        if delta < 0:  # controls are explicitly flat, not decreasing
            continue
        c0, c1 = cost(d13[key]), cost(d14[key])
        p0, p1 = price(d13[key]), price(d14[key])
        if not c0 or not c1 or not p0 or not p1:
            continue
        sid, sku = key
        rows.append({
            "store": sid,
            "zip3": meta[sid]["zip3"] if meta[sid]["zip3"].isdigit() else None,
            "city_state": meta[sid]["city_state"],
            "size": str(d13[key].get("size") or ""),
            "expanded": delta > 0,
            "log_cost": math.log(c1 / c0),
            "cut": float(p1 < p0),
            "promo_changed": float(promo(d13[key]) != promo(d14[key])),
        })

    expanded = [r for r in rows if r["expanded"]]
    flat = [r for r in rows if not r["expanded"]]
    print("=== RAW COMPARISON BEFORE COMPOSITION CONTROL ===")
    print(f"modeled observations: expanded {len(expanded):,}; flat {len(flat):,}")
    print(f"mean per-offer four-month change: "
          f"expanded {statistics.fmean(math.expm1(r['log_cost']) for r in expanded):+.2%}; "
          f"flat {statistics.fmean(math.expm1(r['log_cost']) for r in flat):+.2%}")

    report("ZIP3 MARKET x SIZE", rows, lambda r: (r["zip3"], r["size"]) if r["zip3"] else None)
    report("ZIP3 MARKET — 10x10 ONLY", rows, lambda r: r["zip3"], only_10x10=True)
    report("CITY/STATE x SIZE", rows, lambda r: (r["city_state"], r["size"]))
    report("CITY/STATE — 10x10 ONLY", rows, lambda r: r["city_state"], only_10x10=True)
    report("SAME STORE x SIZE", rows, lambda r: (r["store"], r["size"]))
    report("SAME STORE — 10x10 ONLY", rows, lambda r: r["store"], only_10x10=True)


if __name__ == "__main__":
    main()
