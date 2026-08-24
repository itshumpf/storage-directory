#!/usr/bin/env python3
"""Daily coverage: stores, listings and states actually observed.

WHAT THIS ANSWERS
-----------------
Every claim on this site of the form "prices moved, the sample didn't" needs a
denominator that someone can re-derive. This prints it per day: how many stores
reported, how many unit listings they carried, and how many states they span.

Run:
    python analysis/coverage_check.py
    python analysis/coverage_check.py --from 2026-08-17 --to 2026-08-23

WHY THIS FILE EXISTS
--------------------
On 2026-08-24 `repricing.html` was found to be claiming "10,173 tracked
listings on the 19th against 10,063 on the 22nd, a 0.4% change". Every figure
in that sentence was wrong: the real counts were 56,618 and 56,279, the real
change was 0.60%, and even the stated 0.4% did not match the stated counts
(10,173 -> 10,063 is 1.08%). The number appears to have come from an earlier
era of the dataset and was never re-derived when the dataset grew. It sat in
the paragraph defending the site's largest single-day finding.

Nothing detected it because no script produced it. That is the whole lesson:
a figure with no command behind it decays silently and in whichever direction
flatters the page.

WHAT THIS CANNOT SEE
--------------------
  * Units the site groups rather than lists individually. Store pages can show
    unit types that do not appear here as separate SKUs; coverage is a
    consistent sample per store, not a census of every physical unit.
  * Occupancy. `listings` counts advertised units, not rented ones.
  * Any operator other than the one scraped.
"""
import argparse
import collections
import csv
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
HIST = sorted(glob.glob(os.path.join(HERE, "..", "history", "20??-??.csv")))
LOCS = os.path.join(HERE, "..", "enriched_locations.json")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", default="")
    ap.add_argument("--to", dest="end", default="")
    args = ap.parse_args()

    # store -> state, used to count states per day. The mapping is stable, so
    # applying today's mapping to an earlier day is safe for a state COUNT.
    state = {}
    if os.path.exists(LOCS):
        try:
            state = {str(s["store_id"]): s.get("state")
                     for s in json.load(open(LOCS, encoding="utf-8"))}
        except (OSError, ValueError, KeyError):
            state = {}

    stores = collections.Counter()
    listings = collections.Counter()
    avail = collections.Counter()
    states = collections.defaultdict(set)
    for path in HIST:
        if not os.path.exists(path):
            continue
        for r in csv.DictReader(open(path, newline="", encoding="utf-8")):
            d = r["date"]
            stores[d] += 1
            for col, sink in (("listings", listings), ("units_avail", avail)):
                try:
                    sink[d] += int(r.get(col) or 0)
                except ValueError:
                    pass
            s = state.get(r["store_id"])
            if s:
                states[d].add(s)

    dates = sorted(stores)
    if args.start:
        dates = [d for d in dates if d >= args.start]
    if args.end:
        dates = [d for d in dates if d <= args.end]
    if not dates:
        print("no dates in range", file=sys.stderr)
        return 1

    print(f"{'date':12}{'stores':>9}{'listings':>11}{'units avail':>13}{'states':>8}"
          f"{'listings d/d':>14}")
    prev = None
    for d in dates:
        delta = "" if prev is None or not listings[prev] else \
            f"{(listings[d]-listings[prev])/listings[prev]:+.2%}"
        print(f"{d:12}{stores[d]:>9,}{listings[d]:>11,}{avail[d]:>13,}"
              f"{len(states[d]):>8}{delta:>14}")
        prev = d

    a, b = dates[0], dates[-1]
    print(f"\nover {a} .. {b}:")
    print(f"  stores   {stores[a]:>8,} -> {stores[b]:>8,}"
          f"   {(stores[b]-stores[a])/stores[a]:+.2%}")
    print(f"  listings {listings[a]:>8,} -> {listings[b]:>8,}"
          f"   {(listings[b]-listings[a])/listings[a]:+.2%}")
    allstates = {len(states[d]) for d in dates}
    print(f"  states   {'constant at '+str(allstates.pop()) if len(allstates)==1 else 'VARIES: '+str(sorted(allstates))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
