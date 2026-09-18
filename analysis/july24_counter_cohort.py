#!/usr/bin/env python3
"""Negative-control cohort for the September 13 store-selection analysis.

The September cohort is outcome-defined, so its historical profile can be
misleading if every cohort defined by a broad treatment wave has the same
profile.  July 24 is the natural negative control because that wave touched
the eventual September *other* stores more often.

Definition:
  * eligible: Public Storage stores present in the July 24 aggregate snapshot;
  * treated: at least one price or promotion change logged on July 24;
  * complement: eligible stores with neither kind of logged change that day.

The primary pre-treatment profile is the exact July 23 Git snapshot.  The
July 11-23 trend is secondary and explicitly restricted to continuously
observed stores because catalog coverage expanded sharply on July 23.
"""
from __future__ import annotations

import csv
from collections import Counter

from september13_historical_cohort import (
    ROOT, RATE_LOG, cohort, cross_section, git_snapshot, trend_window,
)


def active_stores(day: str) -> set[str]:
    path = ROOT / "history" / f"{day[:7]}.csv"
    stores = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("date") == day:
                stores.add(str(row.get("store_id") or ""))
    return stores


def touched_stores(day: str) -> tuple[set[str], set[str], set[str]]:
    price, promo = set(), set()
    with RATE_LOG.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("date") != day:
                continue
            sid = str(row.get("store_id") or "")
            if row.get("field") == "price":
                try:
                    if float(row.get("old") or 0) <= 0:
                        continue
                except (TypeError, ValueError):
                    continue
                price.add(sid)
            elif row.get("field") == "promo":
                promo.add(sid)
    return price | promo, price, promo


def overlap_table(july: set[str], july_other: set[str], september: set[str],
                  september_other: set[str]) -> None:
    print("\n=== RELATIONSHIP TO THE LATER SEPTEMBER COHORT ===")
    print(f"  July-treated -> September selected: "
          f"{len(july & september):,}/{len(july):,} ({len(july & september)/len(july):.1%})")
    print(f"  July-control -> September selected: "
          f"{len(july_other & september):,}/{len(july_other):,} "
          f"({len(july_other & september)/len(july_other):.1%})")
    print("  membership table:")
    print(f"    July treated / September selected: {len(july & september):,}")
    print(f"    July treated / September other:    {len(july & september_other):,}")
    print(f"    July control / September selected: {len(july_other & september):,}")
    print(f"    July control / September other:    {len(july_other & september_other):,}")


def prior_wave_exposure(treated: set[str], control: set[str]) -> None:
    print("\n=== EXPOSURE TO EARLIER BROAD JULY WAVES ===")
    for day in ("2026-07-15", "2026-07-18", "2026-07-23"):
        active = active_stores(day)
        touched, price, promo = touched_stores(day)
        a, b = treated & active, control & active
        ar = len(a & touched) / len(a)
        br = len(b & touched) / len(b)
        print(f"  {day}: July-24 treated {ar:.1%}; control {br:.1%}; "
              f"gap {ar-br:+.1%} points")


def main() -> None:
    eligible = active_stores("2026-07-24")
    touched, price_touched, promo_touched = touched_stores("2026-07-24")
    treated = eligible & touched
    control = eligible - treated
    september, september_other, september12 = cohort()

    print("=== JULY 24 NEGATIVE-CONTROL COHORT ===")
    print(f"eligible stores: {len(eligible):,}")
    print(f"treated by any price/promo change: {len(treated):,}")
    print(f"  price-touched: {len(eligible & price_touched):,}")
    print(f"  promo-touched: {len(eligible & promo_touched):,}")
    print(f"  touched by both: {len(eligible & price_touched & promo_touched):,}")
    print(f"untouched complement: {len(control):,}")

    print("\n=== PRE-TREATMENT AVAILABILITY TREND (CONTINUOUSLY OBSERVED SUBSET) ===")
    print("Catalog coverage expands on July 23, so newly added stores cannot enter this trend.")
    trend_window(ROOT / "history" / "2026-07.csv", "2026-07-11", "2026-07-23",
                 treated, control)

    print("\n=== PRIMARY PRE-TREATMENT PROFILE: JULY 23 ===")
    cross_section(git_snapshot("2026-07-23"), treated, control)

    prior_wave_exposure(treated, control)
    overlap_table(treated, control, september, september_other)

    print("\n=== FORWARD TRAJECTORY OF THE FIXED JULY 24 COHORT ===")
    for label, stores in (("July 31", git_snapshot("2026-07-31")),
                          ("August 25", git_snapshot("2026-08-25")),
                          ("September 12", september12)):
        print(f"\n{label}")
        cross_section(stores, treated, control)

    print("\n=== DECISION RULE ===")
    print("If this July-defined cohort is also above-market and promotion-rich before")
    print("July 24, the September profile may be generic selection-on-treatment. If")
    print("its pre-treatment profile is different or inverted, the two-lane reading")
    print("survives this negative control.")


if __name__ == "__main__":
    main()
