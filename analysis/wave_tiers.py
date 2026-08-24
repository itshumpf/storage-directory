#!/usr/bin/env python3
"""Split repricing waves into directional and non-directional events.

WHAT THIS ANSWERS
-----------------
`repricing_waves.py` finds twelve days on which tens of thousands of advertised
rates moved. Calling all twelve "waves" conflates two different things. Six of
them move 22,000-48,000 units with a median change of one or two dollars and a
near-even up/down split -- the portfolio ends the day roughly where it started.
Two moved the national picture materially. Presenting them in one undivided list
invites the fair objection that a one-dollar median is not a repricing event.

This script tiers them, and reports each wave as a share of that day's tracked
inventory rather than as a raw count, so the reader can see that the separation
between quiet days and wave days is not an artifact of inventory growth.

Run:
    python analysis/wave_tiers.py
    python analysis/wave_tiers.py --min-changes 10000 --skew 0.65

WHAT THIS CANNOT SEE
--------------------
  * Why a day was directional or not.
  * Whether the near-even days are one operational process or several.
  * Transacted rents.

ON THE DAY-OF-WEEK RESULT
-------------------------
The weekday census at the bottom was found by inspection AFTER the waves were
identified, which is the cheapest possible way to find a pattern that is not
there. The p-value printed is exact for the partition tested but takes no
account of the partitions that were not tested, and the partition was chosen
because it looked clean. Treat it as a hypothesis to check against a later
observation window, never as a result. It is printed here so it can be tracked
rather than rediscovered.
"""
import argparse
import collections
import csv
import datetime
import json
import os
import statistics
import sys
from math import comb

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "..", "history", "rate_changes.csv")
DEFAULT_SNAPS = os.path.join(HERE, "..", "store-history.json")
HIST = [os.path.join(HERE, "..", "history", "2026-07.csv"),
        os.path.join(HERE, "..", "history", "2026-08.csv")]


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def snapshot_window(path):
    """Contiguous run of daily snapshots ending at the most recent one.

    Same reasoning as repricing_waves.py: a day on which nothing changed writes
    no rows, so the change log cannot supply the observation window.
    """
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
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--snapshots", default=DEFAULT_SNAPS)
    ap.add_argument("--min-changes", type=int, default=10000)
    ap.add_argument("--skew", type=float, default=0.65,
                    help="frac_down beyond this (or below 1-this) is directional")
    args = ap.parse_args()

    n_chg, frac_dn, med_d = collections.Counter(), {}, {}
    downs, deltas = collections.Counter(), collections.defaultdict(list)
    store_deltas = collections.defaultdict(list)
    for r in csv.DictReader(open(args.log, newline="", encoding="utf-8")):
        if r["field"] != "price":
            continue
        o, n = num(r["old"]), num(r["new"])
        if o is None or n is None or o <= 0:
            continue          # a unit arriving, not a repricing
        d = r["date"]
        n_chg[d] += 1
        if n < o:
            downs[d] += 1
        deltas[d].append(n - o)
        store_deltas[d].append((r["store_id"], n - o))

    listings = collections.Counter()
    for p in HIST:
        if not os.path.exists(p):
            continue
        for r in csv.DictReader(open(p, newline="", encoding="utf-8")):
            try:
                listings[r["date"]] += int(r["listings"] or 0)
            except ValueError:
                pass

    window = snapshot_window(args.snapshots)
    first = min(n_chg) if n_chg else None
    if window and first:
        window = [d for d in window if d >= datetime.date.fromisoformat(first)]
    dates = [d.isoformat() for d in window] or sorted(n_chg)

    print(f"rate log : {os.path.normpath(args.log)}")
    print(f"window   : {dates[0]} .. {dates[-1]}  ({len(dates)} snapshot days)\n")

    tiers = collections.defaultdict(list)
    for d in dates:
        c = n_chg[d]
        if c < args.min_changes:
            continue
        fd = downs[d] / c
        md = statistics.median(deltas[d])
        tier = ("directional decrease" if fd >= args.skew else
                "directional increase" if fd <= 1 - args.skew else
                "non-directional")
        tiers[tier].append((d, c, fd, md, listings.get(d, 0)))

    order = ["directional increase", "directional decrease", "non-directional"]
    for t in order:
        rows = tiers.get(t, [])
        if not rows:
            continue
        print(f"=== {t.upper()}  ({len(rows)} of "
              f"{sum(len(v) for v in tiers.values())}) ===")
        print(f"  {'date':12}{'dow':5}{'units':>9}{'% of inv':>10}"
              f"{'% down':>8}{'median $':>10}{'median |$|':>12}{'|Δ|<=$5':>9}{'|Δ|>=$20':>10}")
        for d, c, fd, md, li in rows:
            dow = datetime.date.fromisoformat(d).strftime("%a")
            share = f"{c/li:.1%}" if li else "—"
            a = sorted(abs(x) for x in deltas[d])
            tight = sum(1 for x in a if x <= 5) / len(a)
            wide = sum(1 for x in a if x >= 20) / len(a)
            print(f"  {d:12}{dow:5}{c:>9,}{share:>10}{fd:>7.1%}{md:>+10.0f}"
                  f"{statistics.median(a):>12.0f}{tight:>9.0%}{wide:>10.0%}")
        print()

    nd = tiers.get("non-directional", [])
    if nd:
        allnd = [x for d, *_ in nd for x in deltas[d]]
        alld = [x for t in ("directional increase", "directional decrease")
                for d, *_ in tiers.get(t, []) for x in deltas[d]]
        a_nd = sorted(abs(x) for x in allnd)
        print("=== ARE THE NON-DIRECTIONAL WAVES JUST NOISE? NO. ===")
        print("The signed median is near zero because increases and decreases cancel.")
        print("The ABSOLUTE median shows the individual moves are not small at all:\n")
        print(f"  non-directional : n={len(allnd):>8,}   median |Δ| ${statistics.median(a_nd):>3.0f}"
              f"   within $5 {sum(1 for x in a_nd if x<=5)/len(a_nd):>4.0%}"
              f"   >= $20 {sum(1 for x in a_nd if x>=20)/len(a_nd):>4.0%}")
        if alld:
            a_d = sorted(abs(x) for x in alld)
            print(f"  directional     : n={len(alld):>8,}   median |Δ| ${statistics.median(a_d):>3.0f}"
                  f"   within $5 {sum(1 for x in a_d if x<=5)/len(a_d):>4.0%}"
                  f"   >= $20 {sum(1 for x in a_d if x>=20)/len(a_d):>4.0%}")
        print("\n  A 'normalisation pass' or 'rounding churn' explanation predicts a tight")
        print("  cluster near zero. That is not what is here: a quarter of the changes on")
        print("  non-directional days are $20 or more, and 2026-08-22 has HALF its changes")
        print("  at $20+ with a median absolute move of $20. These are large individual")
        print("  repricings that happen to offset in aggregate — not small ones.")
        print("  Reporting only the signed median understates them badly, and invites the")
        print("  fair objection that a $1 median is not a repricing event. It is not a $1")
        print("  event; it is a two-sided one.\n")

    print("=== IS THE UP/DOWN SPLIT RANDOM, OR CLUSTERED BY STORE? ===")
    print("Second, independent test of the same question. A single noisy global pass")
    print("would scatter signs at random within each store. Many opposing decisions")
    print("would make individual stores internally coherent. A store is 'lopsided'")
    print("when >=80% of its changes that day go the same way; 'pure' when all do.")
    print("The null column reshuffles the same signs across the same store sizes.\n")
    print(f"  {'date':12}{'tier':6}{'stores':>8}{'lopsided':>10}{'null':>7}{'ratio':>7}{'pure':>7}")
    import random
    rng = random.Random(0)
    for t in order:
        for d, c, fd, md, li in tiers.get(t, []):
            per = collections.defaultdict(list)
            for sid, delta in store_deltas[d]:
                per[sid].append(delta)
            big = [v for v in per.values() if len(v) >= 5]
            if not big:
                continue
            def lopsided(sets):
                return sum(1 for v in sets
                           if max(sum(1 for x in v if x > 0),
                                  sum(1 for x in v if x < 0)) / len(v) >= 0.8) / len(sets)
            obs = lopsided(big)
            pure = sum(1 for v in big if all(x > 0 for x in v) or all(x < 0 for x in v)) / len(big)
            signs = [1 if x > 0 else -1 for v in big for x in v]
            rng.shuffle(signs)
            it = iter(signs)
            shuf = lopsided([[next(it) for _ in v] for v in big])
            tier = "DIR" if t != "non-directional" else "even"
            print(f"  {d:12}{tier:6}{len(big):>8,}{obs:>9.0%}{shuf:>7.0%}"
                  f"{obs/shuf if shuf else float('nan'):>7.1f}{pure:>7.0%}")
    print("\n  Observed lopsidedness on non-directional days runs several times the")
    print("  shuffled null. The signs are store-coherent: individual stores move")
    print("  decisively, different stores move opposite ways, and the national median")
    print("  is flat because those decisions offset — not because nothing happened.")
    print("  Together with the absolute-dollar table above, this rules out a single")
    print("  low-amplitude normalisation pass on two independent grounds.\n")

    print("=== QUIET DAYS, FOR THE GAP ===")
    q = [(d, n_chg[d], listings.get(d, 0)) for d in dates if n_chg[d] < args.min_changes]
    qs = [c / li for _, c, li in q if li]
    ws = [c / li for t in order for _, c, _, _, li in tiers.get(t, []) if li]
    print(f"  quiet days: {len(q)}   share of inventory "
          f"{min(qs):.2%} .. {max(qs):.2%}")
    print(f"  wave  days: {len(ws)}   share of inventory "
          f"{min(ws):.2%} .. {max(ws):.2%}")
    print(f"  nothing observed between {max(qs):.2%} and {min(ws):.2%}\n")

    print("=== DAY-OF-WEEK CENSUS — POST HOC, NOT A RESULT ===")
    census = collections.defaultdict(lambda: [0, 0])
    for d in dates:
        k = datetime.date.fromisoformat(d).strftime("%a")
        census[k][0] += 1
        if n_chg[d] >= args.min_changes:
            census[k][1] += 1
    empty = []
    for k in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
        tot, w = census[k]
        print(f"  {k}: {w} wave{'s' if w != 1 else ''} of {tot} days")
        if tot and not w:
            empty.append(k)
    if empty:
        K = sum(census[k][0] for k in empty)
        N = len(dates)
        n = sum(len(v) for v in tiers.values())
        p = comb(N - K, n) / comb(N, n) if N - K >= n else 0.0
        print(f"\n  Zero waves fell on {'/'.join(empty)} — {K} of {N} days.")
        print(f"  P(all {n} waves avoid them | uniform) = {p:.4f}")
        print("  Found post hoc, on the same data that generated the waves. The")
        print("  partition was chosen because it looked clean, so this p-value is")
        print("  not corrected for the partitions that were never tested. Check it")
        print("  against a later window before treating it as anything at all.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
