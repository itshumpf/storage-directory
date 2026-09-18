#!/usr/bin/env python3
"""Test whether the 2026-09-13 availability expansion and 09-14 offer reset align.

This stays at exact (store_id, sku) identity.  It compares SKUs whose advertised
count rose on 09-13 with flat/decreasing-count controls and measures their 09-14
price and promotion outcomes.  It does not treat the advertised count as audited
physical vacancy or infer whether a human or an automated system initiated it.
"""
from __future__ import annotations

import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "history" / "publicstorage"


def load(day: str) -> tuple[dict, dict]:
    stores = json.loads((HISTORY / f"{day}.json").read_text(encoding="utf-8"))
    units = {}
    store_rows = {}
    for store in stores:
        sid = str(store.get("store_id") or "")
        store_rows[sid] = store
        for unit in store.get("units") or []:
            sku = str(unit.get("sku") or "")
            if sid and sku:
                units[(sid, sku)] = unit
    return units, store_rows


def count(unit: dict) -> int:
    return int(unit.get("count") or 0)


def price(unit: dict) -> float | None:
    value = unit.get("price")
    return float(value) if isinstance(value, (int, float)) and value > 0 else None


def promo(unit: dict) -> str:
    return str(unit.get("promo") or unit.get("promo2") or "").strip()


def four_month_cost(unit: dict) -> float | None:
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


def pct(n: int, d: int) -> str:
    return f"{n / d:.1%}" if d else "n/a"


def summarize(label: str, keys: set, d13: dict, d14: dict) -> None:
    matched = keys & set(d14)
    down = same = up = 0
    changed_promo = 0
    both = 0
    price_ratios = []
    cost_ratios = []
    for key in matched:
        p0, p1 = price(d13[key]), price(d14[key])
        price_changed = False
        if p0 is not None and p1 is not None:
            price_ratios.append(p1 / p0)
            if p1 < p0:
                down += 1
                price_changed = True
            elif p1 > p0:
                up += 1
                price_changed = True
            else:
                same += 1
        promo_changed = promo(d13[key]) != promo(d14[key])
        changed_promo += promo_changed
        both += price_changed and promo_changed
        c0, c1 = four_month_cost(d13[key]), four_month_cost(d14[key])
        if c0 and c1:
            cost_ratios.append(c1 / c0)

    print(f"\n{label}: {len(keys):,} on 9/13; {len(matched):,} still present 9/14")
    print(f"  price cut:       {down:,} ({pct(down, len(matched))})")
    print(f"  price unchanged: {same:,} ({pct(same, len(matched))})")
    print(f"  price raised:    {up:,} ({pct(up, len(matched))})")
    print(f"  promo changed:   {changed_promo:,} ({pct(changed_promo, len(matched))})")
    print(f"  price+promo:     {both:,} ({pct(both, len(matched))})")
    if price_ratios:
        print(f"  median 9/14 price / 9/13 price: {statistics.median(price_ratios):.3f}")
    if cost_ratios:
        print(f"  median modeled four-month ratio: {statistics.median(cost_ratios):.3f}")
        old_cost = sum(four_month_cost(d13[key]) for key in matched
                       if four_month_cost(d13[key]) and four_month_cost(d14[key]))
        new_cost = sum(four_month_cost(d14[key]) for key in matched
                       if four_month_cost(d13[key]) and four_month_cost(d14[key]))
        print(f"  aggregate modeled four-month cost: {old_cost:,.0f} -> {new_cost:,.0f} "
              f"({new_cost / old_cost - 1:+.1%})")
        print(f"  offers modeled costlier / cheaper: "
              f"{sum(r > 1 for r in cost_ratios):,} / {sum(r < 1 for r in cost_ratios):,}")


def main() -> None:
    d12, stores12 = load("2026-09-12")
    d13, stores13 = load("2026-09-13")
    d14, stores14 = load("2026-09-14")

    retained_12_13 = set(d12) & set(d13)
    increased = {k for k in retained_12_13 if count(d13[k]) > count(d12[k])}
    flat = {k for k in retained_12_13 if count(d13[k]) == count(d12[k])}
    decreased = {k for k in retained_12_13 if count(d13[k]) < count(d12[k])}
    appeared = set(d13) - set(d12)

    print("=== 9/13 ADVERTISED-COUNT EVENT ===")
    print(f"retained SKUs with count increase: {len(increased):,}; "
          f"advertised units +{sum(count(d13[k])-count(d12[k]) for k in increased):,}")
    print(f"retained SKUs with flat count:     {len(flat):,}")
    print(f"retained SKUs with count decrease: {len(decreased):,}; "
          f"advertised units {sum(count(d13[k])-count(d12[k]) for k in decreased):,}")
    print(f"SKUs appearing on 9/13:            {len(appeared):,}; "
          f"advertised units +{sum(count(d13[k]) for k in appeared):,}")

    summarize("COUNT INCREASED ON 9/13", increased, d13, d14)
    summarize("COUNT FLAT ON 9/13 (CONTROL)", flat, d13, d14)
    summarize("COUNT DECREASED ON 9/13 (CONTROL)", decreased, d13, d14)
    summarize("SKU APPEARED ON 9/13", appeared, d13, d14)

    # Test whether the count-increase cohort is targeted beyond the overall
    # reach of the reset.  The comparison rate is among all exact SKUs that
    # were present on both 09-13 and 09-14.
    common_13_14 = set(d13) & set(d14)
    all_cuts = {k for k in common_13_14 if price(d13[k]) and price(d14[k])
                and price(d14[k]) < price(d13[k])}
    all_promos = {k for k in common_13_14 if promo(d13[k]) != promo(d14[k])}
    inc_matched = increased & common_13_14
    expected_cuts = len(inc_matched) * len(all_cuts) / len(common_13_14)
    expected_promos = len(inc_matched) * len(all_promos) / len(common_13_14)
    observed_cuts = len(inc_matched & all_cuts)
    observed_promos = len(inc_matched & all_promos)
    print("\n=== OVERLAP VERSUS PORTFOLIO-WIDE REACH ===")
    print(f"all matched SKUs price-cut on 9/14: {len(all_cuts):,}/{len(common_13_14):,} "
          f"({pct(len(all_cuts), len(common_13_14))})")
    print(f"count-increase cohort cuts: observed {observed_cuts:,}, expected {expected_cuts:,.0f}, "
          f"ratio {observed_cuts/expected_cuts:.2f}x")
    print(f"all matched SKUs promo-changed on 9/14: {len(all_promos):,}/{len(common_13_14):,} "
          f"({pct(len(all_promos), len(common_13_14))})")
    print(f"count-increase cohort promo changes: observed {observed_promos:,}, "
          f"expected {expected_promos:,.0f}, ratio {observed_promos/expected_promos:.2f}x")

    # Persistence: if the 09-13 increase is a transient incomplete scrape, it
    # should vanish on 09-14.  Measure the exact cohort rather than portfolio totals.
    persisted = increased & set(d14)
    c12 = sum(count(d12[k]) for k in persisted)
    c13 = sum(count(d13[k]) for k in persisted)
    c14 = sum(count(d14[k]) for k in persisted)
    print("\n=== PERSISTENCE OF THE COUNT-INCREASE COHORT ===")
    print(f"advertised units on exact persisted SKUs: {c12:,} -> {c13:,} -> {c14:,}")
    print(f"9/14 retained {c14-c12:,} of the 9/13 increase of {c13-c12:,} "
          f"({pct(c14-c12, c13-c12)})")

    # Store-level breadth.
    store_count_delta = Counter()
    for key in retained_12_13:
        store_count_delta[key[0]] += count(d13[key]) - count(d12[key])
    exposure_stores = {sid for sid, delta in store_count_delta.items() if delta > 0}
    cut_stores = {sid for sid, sku in all_cuts}
    promo_stores = {sid for sid, sku in all_promos}
    all_stores = set(stores13) & set(stores14)
    print("\n=== STORE-LEVEL BREADTH ===")
    print(f"stores with net retained-SKU count expansion 9/13: {len(exposure_stores):,}/{len(all_stores):,}")
    print(f"stores with at least one price cut 9/14:       {len(cut_stores):,}/{len(all_stores):,}")
    print(f"stores with at least one promo change 9/14:    {len(promo_stores):,}/{len(all_stores):,}")
    print(f"expanded stores also price-cut:                "
          f"{len(exposure_stores & cut_stores):,}/{len(exposure_stores):,} "
          f"({pct(len(exposure_stores & cut_stores), len(exposure_stores))})")
    print(f"expanded stores also promo-changed:            "
          f"{len(exposure_stores & promo_stores):,}/{len(exposure_stores):,} "
          f"({pct(len(exposure_stores & promo_stores), len(exposure_stores))})")


if __name__ == "__main__":
    main()
