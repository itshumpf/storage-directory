#!/usr/bin/env python3
"""Emit repricing-stats.json so repricing.html stops going stale.

WHY THIS EXISTS
---------------
`repricing.html` was hand-written with its figures typed in. The daily Action
regenerates trends, markets and insights but never touched it, so every number
on the page froze on the day it was edited while the dataset kept growing. On
2026-08-24 the page still claimed a 44-day window on a dataset that had moved
to 45, and separately carried a coverage figure ("10,173 tracked listings")
that had been wrong by 5.6x for an unknown length of time.

Both are the same failure: a number with no command behind it decays silently,
and always in whichever direction flatters the page.

This script recomputes every figure on that page that moves, and writes them to
`repricing-stats.json` for the page to read at load time. Figures tied to a
dated finding -- the 1 August increase, the 20 August fall, the single-unit
trace -- are deliberately NOT emitted here: those describe specific days and
must not silently change.

Run:
    python analysis/build_repricing_stats.py
    python analysis/build_repricing_stats.py --out repricing-stats.json

Reads only committed files. No network.
"""
import argparse
import collections
import csv
import datetime
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
LOG = os.path.join(ROOT, "history", "rate_changes.csv")
SNAPS = os.path.join(ROOT, "store-history.json")
HIST = [os.path.join(ROOT, "history", "2026-07.csv"),
        os.path.join(ROOT, "history", "2026-08.csv")]
OUT = os.path.join(ROOT, "repricing-stats.json")

WORDS = {12: "twelve", 13: "thirteen", 14: "fourteen", 15: "fifteen",
         16: "sixteen", 17: "seventeen", 18: "eighteen", 19: "nineteen",
         20: "twenty", 40: "forty", 41: "forty-one", 42: "forty-two",
         43: "forty-three", 44: "forty-four", 45: "forty-five",
         46: "forty-six", 47: "forty-seven", 48: "forty-eight",
         49: "forty-nine", 50: "fifty"}


def word(n):
    """Spelled-out form for prose, falling back to digits past the table."""
    return WORDS.get(n, f"{n:,}")


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def snapshot_window(path):
    try:
        days = sorted(datetime.date.fromisoformat(d)
                      for d in json.load(open(path, encoding="utf-8"))["d"])
    except (OSError, KeyError, ValueError):
        return []
    if not days:
        return []
    run = [days[-1]]
    for d in reversed(days[:-1]):
        if (run[0] - d).days != 1:
            break
        run.insert(0, d)
    return run


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--min-changes", type=int, default=10000)
    ap.add_argument("--skew", type=float, default=0.65)
    args = ap.parse_args()

    n_chg, downs, deltas = collections.Counter(), collections.Counter(), \
        collections.defaultdict(list)
    for r in csv.DictReader(open(LOG, newline="", encoding="utf-8")):
        if r["field"] != "price":
            continue
        o, n = num(r["old"]), num(r["new"])
        if o is None or n is None or o <= 0:
            continue          # a unit arriving, not a repricing
        n_chg[r["date"]] += 1
        if n < o:
            downs[r["date"]] += 1
        deltas[r["date"]].append(n - o)

    listings = collections.Counter()
    stores = collections.Counter()
    for p in HIST:
        if not os.path.exists(p):
            continue
        for r in csv.DictReader(open(p, newline="", encoding="utf-8")):
            stores[r["date"]] += 1
            try:
                listings[r["date"]] += int(r["listings"] or 0)
            except ValueError:
                pass

    window = snapshot_window(SNAPS)
    first = min(n_chg) if n_chg else None
    if window and first:
        window = [d for d in window if d >= datetime.date.fromisoformat(first)]
    if not window:
        print("no snapshot window; refusing to emit", file=sys.stderr)
        return 1
    dates = [d.isoformat() for d in window]

    waves, quiet, zero = [], [], []
    for d in dates:
        c = n_chg[d]
        (waves if c >= args.min_changes else quiet).append(c)
        if c == 0:
            zero.append(d)

    def share(d):
        return n_chg[d] / listings[d] if listings.get(d) else None

    qs = [share(d) for d in dates if n_chg[d] < args.min_changes and share(d) is not None]
    ws = [share(d) for d in dates if n_chg[d] >= args.min_changes and share(d) is not None]

    directional = 0
    for d in dates:
        if n_chg[d] < args.min_changes:
            continue
        fd = downs[d] / n_chg[d]
        if fd >= args.skew or fd <= 1 - args.skew:
            directional += 1

    # The wave table itself. Emitted so that a thirteenth wave appears on the
    # page automatically rather than leaving the summary counts disagreeing
    # with a hardcoded twelve-row table.
    table = []
    for d in dates:
        c = n_chg[d]
        if c < args.min_changes:
            continue
        fd = downs[d] / c
        absd = sorted(abs(x) for x in deltas[d])
        tier = ("decrease" if fd >= args.skew else
                "increase" if fd <= 1 - args.skew else "mixed")
        table.append({
            "date": d,
            "dow": datetime.date.fromisoformat(d).strftime("%a"),
            "units": c,
            "share_pct": round(100 * n_chg[d] / listings[d], 1) if listings.get(d) else None,
            "pct_down": round(100 * fd, 1),
            "median": round(statistics.median(deltas[d])),
            "median_abs": round(statistics.median(absd)),
            "tier": tier,
        })

    stats = {
        "generated": datetime.datetime.now().isoformat(timespec="seconds"),
        "window_start": dates[0],
        "window_end": dates[-1],
        "days": len(dates),
        "days_word": word(len(dates)),
        "waves": len(waves),
        "waves_word": word(len(waves)),
        "waves_directional": directional,
        "quiet_days": len(quiet),
        "zero_change_days": len(zero),
        "zero_change_days_word": word(len(zero)),
        "wave_median": round(statistics.median(waves)) if waves else None,
        "wave_min": min(waves) if waves else None,
        "wave_max": max(waves) if waves else None,
        "quiet_median": round(statistics.median(quiet)) if quiet else None,
        "quiet_max": max(quiet) if quiet else None,
        "quiet_share_min_pct": round(100 * min(qs), 2) if qs else None,
        "quiet_share_max_pct": round(100 * max(qs), 2) if qs else None,
        "wave_share_min_pct": round(100 * min(ws), 2) if ws else None,
        "wave_share_max_pct": round(100 * max(ws), 2) if ws else None,
        "stores_latest": stores.get(dates[-1]),
        "listings_latest": listings.get(dates[-1]),
        "waves_table": table,
    }

    # A generated file that silently emits nulls is worse than one that fails.
    missing = [k for k, v in stats.items() if v is None]
    missing += [f"waves_table[{i}].{k}" for i, row in enumerate(table)
                for k, v in row.items() if v is None]
    if missing:
        print(f"refusing to write: null values for {missing}", file=sys.stderr)
        return 1

    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, indent=2)
        fh.write("\n")
    print(f"wrote {os.path.normpath(args.out)}")
    for k, v in stats.items():
        if k == "waves_table":
            print(f"  {k:24} {len(v)} rows")
            continue
        print(f"  {k:24} {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
