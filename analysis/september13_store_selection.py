#!/usr/bin/env python3
"""Study which stores were selected for the 2026-09-13 availability expansion.

"Selected" means a store's advertised count rose in net across exact SKUs that
were present on both 09-12 and 09-13.  That reproduces the 3,018-store cohort
used by september13_14_sequence.py.  The script then asks whether selection is
associated with information observable before the event:

* advertised availability and listing trends from 09-01 through 09-12;
* 09-12 price position within ZIP3 x unit-size peer cells;
* 09-12 promotion richness; and
* participation in the 08-01 price-rise / 40%-off-for-four-month event.

Counts are advertised website availability, not audited vacancy or occupancy.
Associations identify possible targeting inputs; they do not establish the
operator's rule or prove that a human rather than an algorithm chose stores.
"""
from __future__ import annotations

import csv
import json
import math
import random
import statistics
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SNAPSHOTS = ROOT / "history" / "publicstorage"
RATE_LOG = ROOT / "history" / "rate_changes.csv"
PRE_DAYS = [date(2026, 9, d).isoformat() for d in range(1, 13)]


def load(day: str) -> dict[str, dict]:
    rows = json.loads((SNAPSHOTS / f"{day}.json").read_text(encoding="utf-8"))
    return {str(row.get("store_id") or ""): row for row in rows
            if row.get("store_id") is not None}


def unit_count(unit: dict) -> int:
    try:
        return int(unit.get("count") or 0)
    except (TypeError, ValueError):
        return 0


def price(unit: dict) -> float | None:
    value = unit.get("price")
    return float(value) if isinstance(value, (int, float)) and value > 0 else None


def promo(unit: dict) -> str:
    return str(unit.get("promo") or unit.get("promo2") or "").strip()


def promo_discount_share(unit: dict) -> float | None:
    """Advertised four-month savings divided by four-month headline value."""
    p = price(unit)
    if p is None:
        return None
    text = promo(unit).lower()
    if "50% off first 4" in text:
        return 0.50
    if "40% off for 4" in text:
        return 0.40
    if "30% off for 4" in text:
        return 0.30
    if "$1 first month" in text or "$1 special" in text:
        return (p - 1.0) / (4.0 * p)
    if "first month 50% off" in text or "50% off 1st month" in text:
        return 0.125
    if "2nd month free" in text:
        return 0.25
    if not text:
        return 0.0
    return None


def sku_map(stores: dict[str, dict]) -> dict[tuple[str, str], dict]:
    result = {}
    for sid, store in stores.items():
        for unit in store.get("units") or []:
            sku = str(unit.get("sku") or "")
            if sku:
                result[(sid, sku)] = unit
    return result


def slope(values: list[float]) -> float:
    """OLS units per day for equally spaced observations."""
    n = len(values)
    xbar = (n - 1) / 2
    ybar = statistics.fmean(values)
    denom = sum((x - xbar) ** 2 for x in range(n))
    return sum((x - xbar) * (y - ybar) for x, y in enumerate(values)) / denom


def fmt(value: float, pct: bool = False) -> str:
    return f"{value:+.2%}" if pct else f"{value:+.2f}"


def arm_summary(label: str, selected: set[str], metrics: dict[str, float],
                pct: bool = False) -> None:
    a = [v for sid, v in metrics.items() if sid in selected and math.isfinite(v)]
    b = [v for sid, v in metrics.items() if sid not in selected and math.isfinite(v)]
    print(f"  {label:29} selected n={len(a):,} median {fmt(statistics.median(a), pct)}; "
          f"other n={len(b):,} median {fmt(statistics.median(b), pct)}; "
          f"mean gap {fmt(statistics.fmean(a) - statistics.fmean(b), pct)}")


def diff_ci(selected_values: list[float], other_values: list[float],
            seed: int, draws: int = 2000) -> tuple[float, float, float]:
    """Difference in means and an independent store bootstrap interval."""
    observed = statistics.fmean(selected_values) - statistics.fmean(other_values)
    rng = random.Random(seed)
    diffs = []
    na, nb = len(selected_values), len(other_values)
    for _ in range(draws):
        am = sum(selected_values[rng.randrange(na)] for _ in range(na)) / na
        bm = sum(other_values[rng.randrange(nb)] for _ in range(nb)) / nb
        diffs.append(am - bm)
    diffs.sort()
    return observed, diffs[int(draws * .025)], diffs[int(draws * .975)]


def weighted_cell_gap(cells: dict, field: str) -> tuple[float, int, int, int]:
    """Harmonic-weighted selected-minus-other difference inside cells."""
    numerator = denominator = 0.0
    used = an = bn = 0
    for arms in cells.values():
        a, b = arms[0], arms[1]
        if not a or not b:
            continue
        w = len(a) * len(b) / (len(a) + len(b))
        numerator += w * (statistics.fmean(a) - statistics.fmean(b))
        denominator += w
        used += 1
        an += len(a)
        bn += len(b)
    return numerator / denominator, used, an, bn


def store_cell_gap(metric: dict[str, float], selected: set[str], universe: set[str],
                   stores: dict[str, dict]) -> tuple[float, int, int, int]:
    cells = defaultdict(lambda: [[], []])
    for sid in universe:
        if sid not in metric:
            continue
        zip3 = str(stores[sid].get("zip") or "")[:3]
        if zip3.isdigit():
            cells[zip3][0 if sid in selected else 1].append(metric[sid])
    return weighted_cell_gap(cells, "store metric")


def main() -> None:
    d12 = load("2026-09-12")
    d13 = load("2026-09-13")
    u12, u13 = sku_map(d12), sku_map(d13)
    retained = set(u12) & set(u13)
    delta = Counter()
    for sid, sku in retained:
        delta[sid] += unit_count(u13[(sid, sku)]) - unit_count(u12[(sid, sku)])
    universe = set(d12) & set(d13)
    selected = {sid for sid in universe if delta[sid] > 0}
    other = universe - selected

    print("=== COHORT ===")
    print(f"stores present 09-12 and 09-13: {len(universe):,}")
    print(f"selected (net retained-SKU count expansion): {len(selected):,}")
    print(f"other stores: {len(other):,}")

    # Load the leak-free pre-period sequentially and retain only store totals.
    series = defaultdict(lambda: {"units": [], "listings": []})
    for day in PRE_DAYS:
        stores = load(day)
        present = universe & set(stores)
        for sid in present:
            units = stores[sid].get("units") or []
            series[sid]["units"].append(sum(unit_count(u) for u in units))
            series[sid]["listings"].append(len(units))
    complete = {sid for sid in universe
                if len(series[sid]["units"]) == len(PRE_DAYS)}

    unit_slope = {sid: slope(series[sid]["units"]) for sid in complete}
    listing_slope = {sid: slope(series[sid]["listings"]) for sid in complete}
    unit_log_change = {
        sid: math.log((series[sid]["units"][-1] + 1) /
                      (series[sid]["units"][0] + 1))
        for sid in complete
    }
    listing_change = {sid: series[sid]["listings"][-1] -
                      series[sid]["listings"][0] for sid in complete}
    unit_start = {sid: float(series[sid]["units"][0]) for sid in complete}
    unit_end = {sid: float(series[sid]["units"][-1]) for sid in complete}
    listing_start = {sid: float(series[sid]["listings"][0]) for sid in complete}

    print("\n=== 1. PRE-EVENT ADVERTISED AVAILABILITY, 09-01 THROUGH 09-12 ===")
    print(f"complete 12-day store panels: {len(complete):,}; no 09-13 data enter these trends")
    arm_summary("09-01 advertised-count level", selected, unit_start)
    arm_summary("09-12 advertised-count level", selected, unit_end)
    arm_summary("09-01 listing-record level", selected, listing_start)
    arm_summary("advertised-count slope/day", selected, unit_slope)
    arm_summary("09-01 to 09-12 log change", selected, unit_log_change, pct=True)
    arm_summary("listing-record slope/day", selected, listing_slope)
    arm_summary("listing-record endpoint Δ", selected, listing_change)
    for name, values, seed in (
            ("advertised-count log change", unit_log_change, 1301),
            ("listing-record endpoint change", listing_change, 1302)):
        a = [values[sid] for sid in complete & selected]
        b = [values[sid] for sid in complete - selected]
        gap, lo, hi = diff_ci(a, b, seed)
        suffix = " log points" if "log" in name else " listings"
        print(f"  bootstrap mean gap, {name}: {gap:+.4f}{suffix} "
              f"(95% {lo:+.4f} to {hi:+.4f})")
    for name, values in (("count log-change", unit_log_change),
                         ("listing endpoint change", listing_change)):
        gap, used, an, bn = store_cell_gap(values, selected, complete, d12)
        print(f"  within-ZIP3 {name} gap: {gap:+.4f} across {used:,} markets "
              f"({an:,} selected / {bn:,} other stores)")
    for arm_name, ids in (("selected", complete & selected),
                          ("other", complete - selected)):
        up_units = sum(unit_slope[sid] > 0 for sid in ids) / len(ids)
        up_list = sum(listing_slope[sid] > 0 for sid in ids) / len(ids)
        print(f"  share already trending upward, {arm_name:8}: "
              f"counts {up_units:.1%}; listings {up_list:.1%}")

    # Market-relative price position immediately before selection.  Each offer
    # is benchmarked against the median for its ZIP3 x normalized-size cell.
    peer_prices = defaultdict(list)
    offers = []
    for sid in universe:
        store = d12[sid]
        zip3 = str(store.get("zip") or "")[:3]
        for unit in store.get("units") or []:
            p = price(unit)
            size = str(unit.get("size") or "")
            if p and zip3.isdigit() and size:
                peer_prices[(zip3, size)].append(p)
                offers.append((sid, zip3, size, unit, p))
    peer_median = {cell: statistics.median(values)
                   for cell, values in peer_prices.items() if len(values) >= 5}
    price_position = defaultdict(list)
    promo_by_store = defaultdict(list)
    promo_cells = defaultdict(lambda: [[], []])
    deep_by_store = defaultdict(list)
    for sid, zip3, size, unit, p in offers:
        cell = (zip3, size)
        if cell in peer_median:
            price_position[sid].append(math.log(p / peer_median[cell]))
        discount = promo_discount_share(unit)
        if discount is not None:
            promo_by_store[sid].append(discount)
            deep_by_store[sid].append(float(discount >= 0.30))
            promo_cells[cell][0 if sid in selected else 1].append(discount)
    store_price = {sid: statistics.median(values)
                   for sid, values in price_position.items() if values}
    store_promo = {sid: statistics.fmean(values)
                   for sid, values in promo_by_store.items() if values}
    store_deep = {sid: statistics.fmean(values)
                  for sid, values in deep_by_store.items() if values}

    print("\n=== 2. PRICE POSITION GOING INTO THE EVENT (09-12) ===")
    print("Each offer is relative to the median in its ZIP3 x size peer cell.")
    arm_summary("store median log price ratio", selected, store_price, pct=True)
    a = [store_price[sid] for sid in selected if sid in store_price]
    b = [store_price[sid] for sid in other if sid in store_price]
    gap, lo, hi = diff_ci(a, b, 1303)
    print(f"  bootstrap mean selected-minus-other gap: {math.expm1(gap):+.2%} "
          f"(95% {math.expm1(lo):+.2%} to {math.expm1(hi):+.2%})")

    print("\n=== 3. PROMOTION RICHNESS GOING INTO THE EVENT (09-12) ===")
    print("Richness = modeled savings / four-month headline value; offers unweighted by count.")
    arm_summary("store mean discount share", selected, store_promo, pct=True)
    arm_summary("share on >=30% four-month", selected, store_deep, pct=True)
    gap, used, an, bn = weighted_cell_gap(promo_cells, "promo")
    print(f"  within ZIP3 x size discount-share gap: {gap:+.2%} points "
          f"across {used:,} cells ({an:,} selected / {bn:,} other offers)")

    # Exact August cohort used by promo_cost_model.py: moved onto the named
    # promo and also received a price increase on 08-01.
    august_price = {}
    august_promo = {}
    with RATE_LOG.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("date") != "2026-08-01":
                continue
            key = (str(row.get("store_id") or ""), str(row.get("sku") or ""))
            if row.get("field") == "price":
                try:
                    old, new = float(row["old"]), float(row["new"])
                except (TypeError, ValueError):
                    continue
                if new > old:
                    august_price[key] = (old, new)
            elif row.get("field") == "promo" and row.get("new") == "40% off For 4 Month":
                august_promo[key] = (row.get("old") or "", row.get("new") or "")
    august_units = set(august_price) & set(august_promo)
    august_counts = Counter(sid for sid, sku in august_units)
    august_stores = set(august_counts) & universe

    print("\n=== 4. PARTICIPATION IN THE 08-01 EVENT ===")
    for label, ids in (("selected", selected), ("other", other)):
        hit = ids & august_stores
        print(f"  {label:8}: {len(hit):,}/{len(ids):,} stores participated "
              f"({len(hit)/len(ids):.1%}); {sum(august_counts[s] for s in hit):,} event SKUs")
    raw_gap = len(selected & august_stores) / len(selected) - len(other & august_stores) / len(other)
    print(f"  raw participation-rate gap: {raw_gap:+.1%} points")
    # ZIP3-stratified store-level comparison.
    aug_cells = defaultdict(lambda: [[], []])
    for sid in universe:
        zip3 = str(d12[sid].get("zip") or "")[:3]
        if zip3.isdigit():
            aug_cells[zip3][0 if sid in selected else 1].append(float(sid in august_stores))
    gap, used, an, bn = weighted_cell_gap(aug_cells, "august")
    print(f"  within-ZIP3 participation gap: {gap:+.1%} points across {used:,} markets "
          f"({an:,} selected / {bn:,} other stores)")

    print("\n=== 5. DOES THE SEPTEMBER ASSOCIATION SURVIVE AUGUST STATUS? ===")
    for august_label, pool in (("August participants", universe & august_stores),
                               ("August non-participants", universe - august_stores)):
        a_ids, b_ids = pool & selected, pool - selected
        print(f"  {august_label}: selected {len(a_ids):,}; other {len(b_ids):,}; "
              f"September selection rate {len(a_ids)/len(pool):.1%}")
        for metric_label, metric, as_pct in (
                ("pre-period count log change", unit_log_change, True),
                ("market-relative price", store_price, True),
                ("four-month discount share", store_promo, True)):
            a = [metric[sid] for sid in a_ids if sid in metric]
            b = [metric[sid] for sid in b_ids if sid in metric]
            if not a or not b:
                continue
            gap = statistics.fmean(a) - statistics.fmean(b)
            if metric_label == "market-relative price":
                gap = math.expm1(gap)
            print(f"    selected-minus-other {metric_label}: {gap:+.2%}")

    print("\n=== INTERPRETATION GUARDRAIL ===")
    print("These are pre-event associations. They can narrow the plausible store-selection")
    print("rule, but advertised counts are not physical vacancy and association is not causation.")


if __name__ == "__main__":
    main()
