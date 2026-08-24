#!/usr/bin/env python3
"""Test whether a price change and a promotion change were one decision or one write.

WHAT THIS ANSWERS
-----------------
On 2026-08-01 every unit that moved onto the promotion "40% off For 4 Month"
also had its advertised price raised the same day. That is either an operator
decision that coupled the two, or an artifact of a pipeline that writes both
fields from a single update path and therefore cannot help but couple them.

The two are distinguishable from this repository alone. If any unit ever has a
promotion change WITHOUT a price change on the same day, the fields are
independently writable and the coupling is signal rather than plumbing.

Run:
    python analysis/august1_containment.py
    python analysis/august1_containment.py --date 2026-08-20
    python analysis/august1_containment.py --promo "First month 50% off"

WHAT THIS CANNOT SEE
--------------------
  * Whether one operator decision or several produced the observed writes.
    Independent writability is necessary for the coupling to mean anything;
    it is not sufficient to prove intent.
  * Transacted rents. Advertised online rates only.
  * Why.

ON THE INDEPENDENCE NULL
------------------------
The expected-overlap figure assumes the two sets are drawn uniformly at random
from the tracked pool. If promoted units are systematically a different kind of
unit -- larger, urban, a particular facility vintage -- that assumption is
already violated before the date in question and the true expected overlap is
higher than reported. This is stated rather than corrected: the observed value
is the MAXIMUM possible, so no plausible null makes it unremarkable, but the
printed expectation should not be quoted as though the null were verified.
"""
import argparse
import collections
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "..", "history", "rate_changes.csv")
DEFAULT_HIST = [os.path.join(HERE, "..", "history", "2026-07.csv"),
                os.path.join(HERE, "..", "history", "2026-08.csv")]


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load(path):
    """(date -> {sku: (old, new)}) for price, and the same for promo."""
    price = collections.defaultdict(dict)
    promo = collections.defaultdict(dict)
    for r in csv.DictReader(open(path, newline="", encoding="utf-8")):
        if r["field"] == "price":
            o, n = num(r["old"]), num(r["new"])
            if o is not None and n is not None and o > 0:
                price[r["date"]][r["sku"]] = (o, n)
        elif r["field"] == "promo":
            promo[r["date"]][r["sku"]] = (r["old"] or "", r["new"] or "")
    return price, promo


def listings_on(date, paths):
    total = 0
    for p in paths:
        if not os.path.exists(p):
            continue
        for r in csv.DictReader(open(p, newline="", encoding="utf-8")):
            if r["date"] == date:
                try:
                    total += int(r["listings"] or 0)
                except ValueError:
                    pass
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--date", default="2026-08-01")
    ap.add_argument("--promo", default="40% off For 4 Month",
                    help="the promotion whose containment is being tested")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print(f"no rate log at {args.log}", file=sys.stderr)
        return 1

    price, promo = load(args.log)
    D = args.date
    if D not in price and D not in promo:
        print(f"no rows for {D}", file=sys.stderr)
        return 1

    pset = set(price[D])
    mset = set(promo[D])
    raised = {s for s, (o, n) in price[D].items() if n > o}
    cohort = {s for s, (o, n) in promo[D].items() if n == args.promo}

    print(f"rate log : {os.path.normpath(args.log)}")
    print(f"date     : {D}")
    print(f"promotion: {args.promo!r}\n")

    print("=== 1. ARE THE FIELDS INDEPENDENTLY WRITABLE? ===")
    print("If the bottom row is zero, a single write path cannot be ruled out and")
    print("the containment result below means nothing.\n")
    print(f"  price changed, promo did NOT : {len(pset - mset):>7,}")
    print(f"  both changed                 : {len(pset & mset):>7,}")
    print(f"  promo changed, price did NOT : {len(mset - pset):>7,}   <-- the decisive cell")
    verdict = ("INDEPENDENT — a coupled write path is ruled out on this date"
               if mset - pset else
               "NOT ESTABLISHED — every promo change came with a price change")
    print(f"\n  verdict: {verdict}\n")

    print("=== 2. THE SAME TEST ACROSS EVERY DATE IN THE LOG ===")
    print("A coupled pipeline would show zero promo-only days everywhere, not just here.\n")
    alldates = sorted(set(price) | set(promo))
    promo_only_days = 0
    zero_price_days = 0
    for d in alldates:
        po = set(promo.get(d, {})) - set(price.get(d, {}))
        if po:
            promo_only_days += 1
        if promo.get(d) and not price.get(d):
            zero_price_days += 1
    print(f"  dates in log                              : {len(alldates):>6,}")
    print(f"  dates with at least one promo-only change : {promo_only_days:>6,}")
    print(f"  dates with promo changes and NO price rows: {zero_price_days:>6,}")
    print("  (the last row is the strongest form: promotions rotating on days when")
    print("   no price moved at all, which a coupled writer could not produce)\n")

    print("=== 3. CONTAINMENT ===")
    inter = cohort & pset
    inter_raised = cohort & raised
    n_list = listings_on(D, DEFAULT_HIST)
    print(f"  price raised on {D}            : {len(raised):>7,}")
    print(f"  moved onto {args.promo!r:<26}: {len(cohort):>7,}")
    print(f"  cohort that ALSO had a price change    : {len(inter):>7,}"
          f"   ({len(inter)/len(cohort):.1%})" if cohort else "")
    print(f"  cohort whose price was RAISED          : {len(inter_raised):>7,}"
          f"   ({len(inter_raised)/len(cohort):.1%})" if cohort else "")
    if n_list and cohort:
        exp = len(raised) * len(cohort) / n_list
        print(f"\n  tracked listings on {D}        : {n_list:>7,}")
        print(f"  expected overlap if independent        : {exp:>7,.0f}"
              f"   ({len(raised):,} x {len(cohort):,} / {n_list:,})")
        print(f"  observed / expected                    : {len(inter_raised)/exp:>7.1f}x")
        print("\n  NOTE: the observed value is the maximum possible, not merely a high")
        print("  one. See the module docstring on why the expectation is indicative only.")

    print("\n=== 4. SIZE OF THE RISE, COHORT VS REST ===")
    if inter_raised:
        import statistics
        a = sorted(price[D][s][1] - price[D][s][0] for s in inter_raised)
        b = sorted(price[D][s][1] - price[D][s][0] for s in raised - cohort)
        print(f"  raised WITH the promotion : n={len(a):>6,}  median {statistics.median(a):+.0f}")
        if b:
            print(f"  raised WITHOUT it         : n={len(b):>6,}  median {statistics.median(b):+.0f}")

    print("\n=== 5. WHAT THE COHORT WAS ON BEFORE ===")
    prev = collections.Counter(promo[D][s][0] or "(no promotion)" for s in cohort)
    for t, n in prev.most_common():
        print(f"  {n:>6,}  ({n/len(cohort):5.1%})  {t}")
    none_before = prev.get("(no promotion)", 0)
    if cohort:
        print(f"\n  went from NO promotion to one: {none_before:,} "
              f"({none_before/len(cohort):.1%})")
        print("  The rest were switched from an existing promotion to a different one.")
        print("  'A discount appeared alongside a price rise' is therefore the wrong")
        print("  description of this event; 'the discount was replaced' is the right one.")

    print("\n=== 6. WHAT HAPPENED TO THE RAISED UNITS AFTERWARDS ===")
    later = sorted(d for d in price if d > D)
    if not later:
        print("  no later dates in the log")
        return 0
    # last observed price for each raised unit, at or after D
    last = {}
    for d in [D] + later:
        for s, (o, n) in price[d].items():
            if s in raised:
                last[s] = n
    print(f"  raised on {D}: {len(raised):,}\n")
    print("  A high re-touch rate looks like targeting until it is compared against")
    print("  the share of ALL inventory that same wave touched. The ratio column is")
    print("  the test: ~1.0 means the cohort was caught by a wide net, not aimed at.")
    print(f"\n  {'wave':12}{'cohort':>9}{'of cohort':>11}{'of inventory':>14}"
          f"{'ratio':>7}{'of those, down':>16}")
    for d in later:
        also = raised & set(price[d])
        if len(also) < len(raised) * 0.15:
            continue
        dn = sum(1 for s in also if price[d][s][1] < price[d][s][0])
        cs = len(also) / len(raised)
        inv = (len(price[d]) / listings_on(d, DEFAULT_HIST)) if listings_on(d, DEFAULT_HIST) else None
        ratio = f"{cs/inv:.2f}" if inv else "—"
        invs = f"{inv:.1%}" if inv else "—"
        print(f"  {d:12}{len(also):>9,}{cs:>10.1%}{invs:>14}{ratio:>7}"
              f"{dn:>10,} ({dn/len(also):.0%})")
    print("\n  Ratios at or below 1.0 mean the cohort was RE-touched no more often than")
    print("  a randomly chosen tracked unit — so a later wave that reverses these")
    print("  prices is not evidence that the wave was aimed at them. Distinctive")
    print("  direction is a finding; distinctive targeting would be a different and")
    print("  much stronger claim, and this table does not support it.")
    pre = {s: price[D][s][0] for s in raised}
    above = sum(1 for s in raised if last.get(s, pre[s]) > pre[s])
    below = sum(1 for s in raised if last.get(s, pre[s]) < pre[s])
    same = len(raised) - above - below
    import statistics as st
    print(f"\n  comparing each unit's pre-{D} price to its latest observed price"
          f" (as of {later[-1]}):")
    print(f"    still ABOVE : {above:>7,}  ({above/len(raised):5.1%})")
    print(f"    now BELOW   : {below:>7,}  ({below/len(raised):5.1%})")
    print(f"    unchanged   : {same:>7,}  ({same/len(raised):5.1%})")
    print(f"    median net move: ${st.median(last.get(s, pre[s]) - pre[s] for s in raised):+,.0f}")
    print("\n  A large reversal that leaves most units above their starting price is a")
    print("  partial retreat, not a return to baseline. State it that way.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
