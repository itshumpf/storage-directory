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
import datetime
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "..", "history", "rate_changes.csv")
DEFAULT_SNAPS = os.path.join(HERE, "..", "store-history.json")


def snapshot_window(path):
    """The contiguous run of daily snapshots ending at the most recent one.

    Why this exists: a day on which no price moved writes NO rows to
    rate_changes.csv. Deriving the observation window from the change log
    therefore deletes exactly the days the headline claim is about — the
    quiet ones — and reports a window shorter than the one actually observed.
    Until 2026-08-23 this file counted 41 days and 13 zero-change days; the
    real figures are 44 and 16.

    The snapshot list also carries isolated early probes (2026-04-29,
    2026-07-09) that are not part of the daily series, so we take only the
    unbroken run at the end. Returns [] if the file is missing, and the
    caller falls back to log-derived dates with a warning.
    """
    try:
        with open(path) as fh:
            raw = json.load(fh)["d"]
    except (OSError, KeyError, ValueError):
        return []
    days = sorted(datetime.date.fromisoformat(d) for d in raw)
    if not days:
        return []
    run = [days[-1]]
    for d in reversed(days[:-1]):
        if (run[0] - d).days != 1:
            break
        run.insert(0, d)
    return run


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
            try:
                old = float(row["old"])
                new = float(row["new"])
            except (TypeError, ValueError):
                skipped += 1
                continue
            if old <= 0:
                # A unit arriving, not a repricing — see update_rate_log.py's
                # sku_map docstring. Skipped BEFORE the count, so the daily
                # total and the median/direction stats describe the same rows.
                # (Counting it here and excluding it below is what made
                # 2026-08-21 read "2 changes" with dashes for every statistic.)
                skipped += 1
                continue
            price[date] += 1
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
    ap.add_argument("--snapshots", default=DEFAULT_SNAPS,
                    help="JSON with a 'd' array of snapshot dates; supplies "
                         "the true observation window including days on which "
                         "nothing changed (default ../store-history.json)")
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

    window = snapshot_window(args.snapshots)
    if window:
        first_change = min(sorted(total)) if total else None
        if first_change:
            window = [d for d in window
                      if d >= datetime.date.fromisoformat(first_change)]
        dates = [d.isoformat() for d in window]
        gaps = (window[-1] - window[0]).days + 1 - len(window)
        source = (f"{os.path.basename(args.snapshots)} "
                  f"({len(window)} snapshots, {gaps} calendar gaps)")
    else:
        dates = sorted(total)
        source = ("CHANGE LOG ONLY — no snapshot file; days with zero changes "
                  "are INVISIBLE and the window below is understated")

    print(f"rate log      : {os.path.normpath(args.log)}")
    print(f"window source : {source}")
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

    def med(vals):
        s = sorted(vals)
        n = len(s)
        if not n:
            return 0
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    wave_n = [price[d] for d in dates if price[d] >= args.min_changes]
    quiet_n = [price[d] for d in dates if price[d] < args.min_changes]
    zero_n = [d for d in dates if price[d] == 0]

    print(f"quiet days : {len(quiet_n):>3}   median {med(quiet_n):>9,.0f}   "
          f"range {min(quiet_n):,} - {max(quiet_n):,}")
    print(f"wave days  : {len(wave_n):>3}   median {med(wave_n):>9,.0f}   "
          f"range {min(wave_n):,} - {max(wave_n):,}")
    print(f"days with ZERO price changes: {len(zero_n)} of {len(dates)}")
    if quiet_n and wave_n:
        print(f"gap: busiest quiet day {max(quiet_n):,}, smallest wave "
              f"{min(wave_n):,} — any threshold between selects the same set")
    if waves:
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
