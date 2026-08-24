#!/usr/bin/env python3
"""Detect and characterise repricing waves in the advertised-rate log.

WHAT THIS ANSWERS
-----------------
Public Storage does not appear to reprice continuously. Advertised rates sit
completely static for days at a time, then tens of thousands of units change at
once. This script finds those events and describes each one, so the claim rests
on a command you can re-run rather than on a chart someone eyeballed.

Run:
    python analysis/repricing_waves.py
    python analysis/repricing_waves.py --min-changes 10000 --csv out.csv

DEFINITIONS, stated so they can be argued with
----------------------------------------------
A "wave" is a date on which the number of logged *price* changes exceeds
--min-changes (default 10,000). That threshold is a choice, not a discovery:
between waves, ordinary days log zero to a few hundred price changes, so any
cut point between ~1,000 and ~12,000 selects the same set of dates. The output
prints the full daily series so you can see that for yourself.

"Skew" is the fraction of changes that moved down. 50% is a balanced wave —
repricing with no net national direction. Skew far from 50% is a pricing
decision with a direction, and those are the ones that move a national median.

WHAT THIS CANNOT SEE
--------------------
  * Transacted rents. These are advertised online rates only.
  * More than one repricing per day. The scrape is daily, so two changes to the
    same unit inside 24 hours are logged as one net change.
  * Why. Direction and date are observable; motive is not.
  * Any operator other than the one scraped.
"""
import argparse
import collections
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "..", "history", "rate_changes.csv")


def load(path):
    """Per-date: total rows, price rows, and every new/old ratio."""
    total = collections.Counter()
    price = collections.Counter()
    ratios = collections.defaultdict(list)
    deltas = collections.defaultdict(list)
    skipped = 0

    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            date = row["date"]
            total[date] += 1
            if row["field"] != "price":
                continue
            price[date] += 1
            try:
                old = float(row["old"])
                new = float(row["new"])
            except (TypeError, ValueError):
                skipped += 1
                continue
            if old <= 0:
                skipped += 1
                continue
            ratios[date].append(new / old)
            deltas[date].append(new - old)

    return total, price, ratios, deltas, skipped


def describe(ratios, deltas):
    n = len(ratios)
    if not n:
        return None
    srt = sorted(ratios)
    return {
        "n": n,
        "median_ratio": srt[n // 2],
        "frac_down": sum(1 for r in ratios if r < 1) / n,
        "median_delta": sorted(deltas)[n // 2],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--min-changes", type=int, default=10000,
                    help="price changes in a day for it to count as a wave "
                         "(default 10000)")
    ap.add_argument("--skew", type=float, default=0.65,
                    help="frac_down above this, or below 1-this, is a "
                         "directional wave (default 0.65)")
    ap.add_argument("--csv", help="also write the wave table here")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print(f"no rate log at {args.log}", file=sys.stderr)
        return 1

    total, price, ratios, deltas, skipped = load(args.log)
    dates = sorted(total)

    print(f"rate log      : {os.path.normpath(args.log)}")
    print(f"dates covered : {dates[0]} .. {dates[-1]}  ({len(dates)} days)")
    print(f"rows          : {sum(total.values()):,} "
          f"({sum(price.values()):,} price)")
    if skipped:
        print(f"unparseable   : {skipped:,} price rows skipped "
              f"(non-numeric or old <= 0)")
    print()

    print("FULL DAILY SERIES — the threshold is defensible only if the gap is")
    print("obvious, so here is every day rather than just the ones selected.")
    print()
    print(f"{'date':12} {'price chgs':>11} {'% down':>8} {'med ratio':>10} "
          f"{'med $':>7}  wave")
    waves = []
    for d in dates:
        s = describe(ratios[d], deltas[d])
        if s is None:
            print(f"{d:12} {price[d]:11,} {'—':>8} {'—':>10} {'—':>7}")
            continue
        is_wave = price[d] >= args.min_changes
        directional = (s["frac_down"] >= args.skew
                       or s["frac_down"] <= 1 - args.skew)
        mark = ""
        if is_wave:
            mark = "WAVE" + ("  <-- DIRECTIONAL" if directional else "")
            waves.append((d, price[d], s, directional))
        print(f"{d:12} {price[d]:11,} {s['frac_down']:7.1%} "
              f"{s['median_ratio']:10.3f} {s['median_delta']:+7.2f}  {mark}")

    print()
    print(f"=== {len(waves)} waves at >= {args.min_changes:,} price changes ===")
    quiet = [d for d in dates if price[d] == 0]
    print(f"days with ZERO price changes: {len(quiet)} of {len(dates)}")
    if waves:
        gaps = [(waves[i][0], waves[i - 1][0]) for i in range(1, len(waves))]
        print(f"wave dates: {', '.join(w[0] for w in waves)}")
        print(f"directional (skew beyond {args.skew:.0%}): "
              f"{sum(1 for w in waves if w[3])}")

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "price_changes", "frac_down", "median_ratio",
                        "median_delta", "directional"])
            for d, n, s, direc in waves:
                w.writerow([d, n, f"{s['frac_down']:.4f}",
                            f"{s['median_ratio']:.4f}",
                            f"{s['median_delta']:.2f}", int(direc)])
        print(f"\nwrote {args.csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
