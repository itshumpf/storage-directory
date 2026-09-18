#!/usr/bin/env python3
"""Decompose two Public Storage snapshots into rate and promotion value."""
from __future__ import annotations

import argparse
import json
import statistics
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def load(day: str) -> dict[tuple[str, str], dict]:
    stores = json.loads(
        (ROOT / "history" / "publicstorage" / f"{day}.json").read_text(encoding="utf-8"))
    return {(str(store["store_id"]), str(unit["sku"])): unit
            for store in stores for unit in store.get("units", [])
            if unit.get("sku") and unit.get("price")}


def promotion(unit: dict) -> str:
    return str(unit.get("promo") or unit.get("promo2") or "").strip()


def four_month_cost(price: float, promo: str) -> float | None:
    """Offer-text model; excludes fees, insurance, tax, and later rent changes."""
    text = promo.lower()
    if "50% off first 4" in text:
        return price * 2
    if "40% off for 4" in text:
        return price * 2.4
    if "30% off for 4" in text:
        return price * 2.8
    if "$1 first month" in text or "$1 special" in text:
        return 1 + price * 3
    if "first month 50% off" in text:
        return price * 3.5
    if "2nd month free" in text:
        return price * 3
    if not text:
        return price * 4
    return None


def money(value: float) -> str:
    return f"${value:,.0f}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default="2026-09-13")
    parser.add_argument("--after", default="2026-09-14")
    args = parser.parse_args()

    old, new = load(args.before), load(args.after)
    rows = []
    transitions = Counter()
    for key in set(old) & set(new):
        before, after = old[key], new[key]
        p0, p1 = float(before["price"]), float(after["price"])
        m0, m1 = promotion(before), promotion(after)
        c00, c11 = four_month_cost(p0, m0), four_month_cost(p1, m1)
        c10, c01 = four_month_cost(p1, m0), four_month_cost(p0, m1)
        if None in (c00, c11, c10, c01):
            continue
        # Shapley decomposition averages both possible orders (change price
        # first vs change promotion first), avoiding an arbitrary attribution.
        price_effect = ((c10 - c00) + (c11 - c01)) / 2
        promo_effect = ((c01 - c00) + (c11 - c10)) / 2
        rows.append({"p0": p0, "p1": p1, "c0": c00, "c1": c11,
                     "price_effect": price_effect, "promo_effect": promo_effect,
                     "changed": p0 != p1 or m0 != m1})
        transitions[(m0 or "(none)", m1 or "(none)")] += 1

    base0 = sum(4 * row["p0"] for row in rows)
    base1 = sum(4 * row["p1"] for row in rows)
    cost0 = sum(row["c0"] for row in rows)
    cost1 = sum(row["c1"] for row in rows)
    savings0, savings1 = base0 - cost0, base1 - cost1
    price_effect = sum(row["price_effect"] for row in rows)
    promo_effect = sum(row["promo_effect"] for row in rows)

    print(f"comparison: {args.before} -> {args.after}")
    print(f"matched offers modeled: {len(rows):,}")
    print(f"median monthly rate: {money(statistics.median(r['p0'] for r in rows))} -> "
          f"{money(statistics.median(r['p1'] for r in rows))}")
    print("\nFOUR-MONTH ACCOUNTING")
    print(f"headline value before promotions: {money(base0)} -> {money(base1)} "
          f"({(base1 / base0 - 1):+.1%})")
    print(f"promotional discount supplied:   {money(savings0)} -> {money(savings1)} "
          f"({(savings1 / savings0 - 1):+.1%})")
    print(f"effective customer cost:          {money(cost0)} -> {money(cost1)} "
          f"({(cost1 / cost0 - 1):+.1%})")
    print(f"promo share of headline value:    {savings0 / base0:.1%} -> {savings1 / base1:.1%}")
    print(f"median per-offer cost change:      "
          f"{statistics.median((r['c1'] / r['c0'] - 1) for r in rows):+.1%}")
    print("\nSHAPLEY EFFECTS (sum exactly to effective-cost change)")
    print(f"monthly-rate effect: {money(price_effect)} ({price_effect / cost0:+.1%})")
    print(f"promotion effect:    {money(promo_effect)} ({promo_effect / cost0:+.1%})")
    print(f"net effect:          {money(price_effect + promo_effect)} "
          f"({(price_effect + promo_effect) / cost0:+.1%})")

    changed = [row for row in rows if row["changed"]]
    ch_base0 = sum(4 * row["p0"] for row in changed)
    ch_base1 = sum(4 * row["p1"] for row in changed)
    ch_cost0 = sum(row["c0"] for row in changed)
    ch_cost1 = sum(row["c1"] for row in changed)
    ch_save0, ch_save1 = ch_base0 - ch_cost0, ch_base1 - ch_cost1
    print(f"\nCHANGED-OFFER COHORT ({len(changed):,})")
    print(f"headline value before promotions: {money(ch_base0)} -> {money(ch_base1)} "
          f"({ch_base1 / ch_base0 - 1:+.1%})")
    print(f"promotional discount supplied:   {money(ch_save0)} -> {money(ch_save1)} "
          f"({ch_save1 / ch_save0 - 1:+.1%})")
    print(f"effective customer cost:          {money(ch_cost0)} -> {money(ch_cost1)} "
          f"({ch_cost1 / ch_cost0 - 1:+.1%})")
    print("\nLARGEST PROMOTION TRANSITIONS")
    for (before, after), count in transitions.most_common(15):
        print(f"{count:>7,}  {before} -> {after}")


if __name__ == "__main__":
    main()
