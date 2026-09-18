#!/usr/bin/env python3
"""Trace the September 13 selected-store cohort backward through July/August.

The cohort definition is fixed using only 09-12 -> 09-13 retained-SKU count
changes.  We then project that fixed label backward; historical data never
participate in defining selection.

Windows:
  * July 23-31: catalog coverage is stable after the July 23 expansion.
  * August 1-25: last uninterrupted daily run before the August 26-30 gap.
  * September 1-12: leak-free period immediately before selection.

Offer cross-sections use exact Git snapshots on July 31 and August 25, plus the
immutable September 12 snapshot.  "Wave" is deliberately mechanical: at least
10,000 price changes or 5,000 promotion changes on one day.
"""
from __future__ import annotations

import csv
import json
import math
import re
import statistics
import subprocess
from collections import Counter, defaultdict
from pathlib import Path

from september13_store_selection import (
    ROOT, RATE_LOG, load, price, promo_discount_share, sku_map, unit_count,
    weighted_cell_gap,
)


DATE_RE = re.compile(r"20\d\d-\d\d-\d\d")
WINDOWS = {
    "July 23-31": (ROOT / "history" / "2026-07.csv", "2026-07-23", "2026-07-31"),
    "August 1-25": (ROOT / "history" / "2026-08.csv", "2026-08-01", "2026-08-25"),
    "September 1-12": (ROOT / "history" / "2026-09.csv", "2026-09-01", "2026-09-12"),
}


def git_snapshot(day: str) -> dict[str, dict]:
    raw = subprocess.check_output(
        ["git", "log", "--all", "--format=%H%x09%s", "--", "enriched_locations.json"],
        cwd=ROOT, text=True, encoding="utf-8", errors="replace")
    commit = None
    for line in raw.splitlines():
        sha, sep, subject = line.partition("\t")
        match = DATE_RE.search(subject)
        if sep and match and match.group() == day:
            commit = sha
            break
    if not commit:
        raise RuntimeError(f"No enriched_locations.json Git snapshot for {day}")
    blob = subprocess.check_output(
        ["git", "show", f"{commit}:enriched_locations.json"], cwd=ROOT)
    rows = json.loads(blob)
    return {str(row.get("store_id") or ""): row for row in rows
            if row.get("store_id") is not None}


def cohort() -> tuple[set[str], set[str], dict[str, dict]]:
    d12, d13 = load("2026-09-12"), load("2026-09-13")
    u12, u13 = sku_map(d12), sku_map(d13)
    retained = set(u12) & set(u13)
    delta = Counter()
    for sid, sku in retained:
        delta[sid] += unit_count(u13[(sid, sku)]) - unit_count(u12[(sid, sku)])
    universe = set(d12) & set(d13)
    selected = {sid for sid in universe if delta[sid] > 0}
    return selected, universe - selected, d12


def median_text(values: list[float], percent: bool = False) -> str:
    value = statistics.median(values)
    return f"{value:+.2%}" if percent else f"{value:,.1f}"


def print_compare(label: str, metric: dict[str, float], selected: set[str],
                  other: set[str], percent: bool = False, transform=None) -> None:
    a = [metric[sid] for sid in selected if sid in metric and math.isfinite(metric[sid])]
    b = [metric[sid] for sid in other if sid in metric and math.isfinite(metric[sid])]
    gap = statistics.fmean(a) - statistics.fmean(b)
    if transform:
        gap = transform(gap)
    gap_text = f"{gap:+.2%}" if percent else f"{gap:+.2f}"
    print(f"  {label:31} selected n={len(a):,} med {median_text(a, percent)}; "
          f"other n={len(b):,} med {median_text(b, percent)}; mean gap {gap_text}")


def cross_section(stores: dict[str, dict], selected: set[str], other: set[str]) -> None:
    universe = (selected | other) & set(stores)
    peers = defaultdict(list)
    offers = []
    avail, listings = {}, {}
    for sid in universe:
        store = stores[sid]
        units = store.get("units") or []
        avail[sid] = float(sum(unit_count(u) for u in units))
        listings[sid] = float(len(units))
        zip3 = str(store.get("zip") or "")[:3]
        for unit in units:
            p = price(unit)
            size = str(unit.get("size") or "")
            if p and zip3.isdigit() and size:
                peers[(zip3, size)].append(p)
                offers.append((sid, zip3, size, unit, p))
    medians = {cell: statistics.median(vals)
               for cell, vals in peers.items() if len(vals) >= 5}
    price_pos = defaultdict(list)
    discounts = defaultdict(list)
    discount_cells = defaultdict(lambda: [[], []])
    for sid, zip3, size, unit, p in offers:
        if (zip3, size) in medians:
            price_pos[sid].append(math.log(p / medians[(zip3, size)]))
        discount = promo_discount_share(unit)
        if discount is not None:
            discounts[sid].append(discount)
            discount_cells[(zip3, size)][0 if sid in selected else 1].append(discount)
    store_price = {sid: statistics.median(vals) for sid, vals in price_pos.items() if vals}
    store_discount = {sid: statistics.fmean(vals) for sid, vals in discounts.items() if vals}
    print_compare("advertised-count level", avail, selected, other)
    print_compare("listing-record level", listings, selected, other)
    print_compare("market-relative headline price", store_price, selected, other,
                  percent=True, transform=math.expm1)
    print_compare("four-month discount share", store_discount, selected, other,
                  percent=True)
    gap, used, an, bn = weighted_cell_gap(discount_cells, "promotion")
    print(f"  {'within ZIP3 x size promo gap':31} {gap:+.2%} points across "
          f"{used:,} cells ({an:,} selected / {bn:,} other offers)")


def load_window(path: Path, first: str, last: str, universe: set[str]):
    by_store = defaultdict(dict)
    dates = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            day, sid = row["date"], str(row["store_id"])
            if first <= day <= last and sid in universe:
                dates.add(day)
                try:
                    units = float(row["units_avail"])
                    listings = float(row["listings"])
                except (TypeError, ValueError):
                    continue
                by_store[sid][day] = (units, listings)
    # The monthly aggregate was not rebuilt for 09-12, but the immutable
    # full snapshot exists.  Add the endpoint rather than silently shortening
    # the requested pre-event window.
    if last == "2026-09-12" and last not in dates:
        stores = load(last)
        for sid in universe & set(stores):
            units = stores[sid].get("units") or []
            by_store[sid][last] = (float(sum(unit_count(u) for u in units)),
                                   float(len(units)))
        dates.add(last)
    dates = sorted(dates)
    return by_store, dates


def trend_window(path: Path, first: str, last: str, selected: set[str], other: set[str]) -> None:
    universe = selected | other
    rows, dates = load_window(path, first, last, universe)
    complete = {sid for sid, values in rows.items() if all(day in values for day in dates)}
    unit_change, listing_change = {}, {}
    for sid in complete:
        u0, l0 = rows[sid][dates[0]]
        u1, l1 = rows[sid][dates[-1]]
        unit_change[sid] = math.log((u1 + 1) / (u0 + 1))
        listing_change[sid] = l1 - l0
    print(f"  observed {dates[0]} .. {dates[-1]} ({len(dates)} dates); "
          f"complete cohort panels {len(complete):,}")
    print_compare("advertised-count log change", unit_change, selected, other, percent=True)
    print_compare("listing-record endpoint change", listing_change, selected, other)


def waves(selected: set[str], other: set[str]) -> None:
    price_rows = Counter()
    promo_rows = Counter()
    touched = defaultdict(set)
    directions = defaultdict(list)
    eligible = defaultdict(set)
    for path in (ROOT / "history" / "2026-07.csv", ROOT / "history" / "2026-08.csv"):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                eligible[row["date"]].add(str(row["store_id"]))
    with RATE_LOG.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            day = row.get("date", "")
            if not ("2026-07-01" <= day <= "2026-08-31"):
                continue
            sid = str(row.get("store_id") or "")
            if row.get("field") == "price":
                try:
                    old, new = float(row["old"]), float(row["new"])
                except (TypeError, ValueError):
                    continue
                if old <= 0:
                    continue
                price_rows[day] += 1
                directions[day].append(new < old)
            elif row.get("field") == "promo":
                promo_rows[day] += 1
            touched[day].add(sid)
    wave_days = sorted(day for day in set(price_rows) | set(promo_rows)
                       if price_rows[day] >= 10_000 or promo_rows[day] >= 5_000)
    exposures = defaultdict(list)
    print("  mechanical threshold: >=10,000 price rows or >=5,000 promo rows")
    print(f"  {'date':12}{'price':>9}{'% down':>9}{'promo':>9}  "
          f"{'selected touched':>18}{'other touched':>16}{'gap':>8}")
    for day in wave_days:
        a_eligible = selected & eligible[day]
        b_eligible = other & eligible[day]
        for sid in a_eligible | b_eligible:
            exposures[sid].append(float(sid in touched[day]))
        ar = len(a_eligible & touched[day]) / len(a_eligible)
        br = len(b_eligible & touched[day]) / len(b_eligible)
        down = (sum(directions[day]) / len(directions[day])
                if directions[day] else float("nan"))
        down_text = f"{down:8.1%}" if math.isfinite(down) else f"{'--':>8}"
        print(f"  {day:12}{price_rows[day]:9,}{down_text}{promo_rows[day]:9,}  "
              f"{ar:17.1%}{br:16.1%}{ar-br:+8.1%}")
    exposure_rate = {sid: statistics.fmean(values)
                     for sid, values in exposures.items() if values}
    print_compare("share of eligible waves touched", exposure_rate, selected, other,
                  percent=True)


def main() -> None:
    selected, other, september = cohort()
    print("=== FIXED SEPTEMBER COHORT PROJECTED BACKWARD ===")
    print(f"selected {len(selected):,}; other {len(other):,}\n")

    print("=== AVAILABILITY TRENDS BEFORE EACH MONTH-END CROSS-SECTION ===")
    for label, (path, first, last) in WINDOWS.items():
        print(f"\n{label}")
        trend_window(path, first, last, selected, other)

    print("\n=== OFFER CROSS-SECTIONS ===")
    for label, stores in (("July 31", git_snapshot("2026-07-31")),
                          ("August 25", git_snapshot("2026-08-25")),
                          ("September 12", september)):
        print(f"\n{label}: {len(stores):,} stores in source snapshot")
        cross_section(stores, selected, other)

    print("\n=== REPEATED EXPOSURE TO BROAD JULY/AUGUST WAVES ===")
    waves(selected, other)

    print("\n=== GUARDRAIL ===")
    print("September selection is projected backward, not predicted here. Repeated")
    print("association is evidence of a persistent treatment segment, not proof of")
    print("the private variable or business rule used to define that segment.")


if __name__ == "__main__":
    main()
