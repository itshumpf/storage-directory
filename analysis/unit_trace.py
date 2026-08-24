#!/usr/bin/env python3
"""Reconstruct one unit's advertised price and promotion, day by day.

WHAT THIS ANSWERS
-----------------
Aggregate figures describe a population; they do not let a reader check
anything. This prints the full observed history of a single SKU -- list price,
promotion, the first month a new customer would pay, and the saving the page
advertises -- so that any claim about the mechanism can be verified one unit at
a time against a store page anyone can load.

Run:
    python analysis/unit_trace.py --sku V_599982
    python analysis/unit_trace.py --store 235 --size 5x5
    python analysis/unit_trace.py --sku V_599982 --admin 29 --insurance 20

HOW THE SERIES IS REBUILT
-------------------------
`rate_changes.csv` records only changes, so a price is carried forward until
the next change touches it. The first row for a SKU supplies the starting
value from its `old` field, which means the series begins at that unit's first
observed change, not at the start of the observation window. Dates before that
are unknown rather than unchanged, and are not printed.

WHAT THIS CANNOT SEE
--------------------
  * Transacted rents. Advertised online rates only.
  * Whether anyone rented this unit at any of these prices.
  * Any period before the unit's first logged change.
"""
import argparse
import collections
import csv
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "..", "history", "rate_changes.csv")

# Rent due in month m at list price P. Kept identical to promo_cost_model.py;
# see that file's header for which entries are verified and which are readings
# of advertising copy.
PROMO_MODEL = {
    "$1 first month rent": lambda P, m: 1.0 if m == 1 else P,
    "First month 50% off": lambda P, m: 0.5 * P if m == 1 else P,
    "50% off 1st Month":   lambda P, m: 0.5 * P if m == 1 else P,
    "2nd Month Free":      lambda P, m: 0.0 if m == 2 else P,
    "$1 Special":          lambda P, m: 1.0 if m == 1 else P,
    "40% off For 4 Month": lambda P, m: 0.6 * P if m <= 4 else P,
    "":                    lambda P, m: P,
}
PROMO_MONTHS = 12   # horizon for the "advertised saving" column


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def saving(P, promo, months=PROMO_MONTHS):
    """What the page would advertise as total savings over `months`."""
    fn = PROMO_MODEL.get(promo)
    if fn is None:
        return None
    return months * P - sum(fn(P, m) for m in range(1, months + 1))


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--sku")
    ap.add_argument("--store")
    ap.add_argument("--size")
    ap.add_argument("--admin", type=float, default=0.0,
                    help="one-off fee at signing, added to the month-1 column")
    ap.add_argument("--insurance", type=float, default=0.0,
                    help="monthly charge, added to the month-1 column")
    args = ap.parse_args()

    if not (args.sku or (args.store and args.size)):
        print("need --sku, or --store and --size", file=sys.stderr)
        return 1

    rows = collections.defaultdict(list)
    meta = {}
    for r in csv.DictReader(open(args.log, newline="", encoding="utf-8")):
        if args.sku and r["sku"] != args.sku:
            continue
        if not args.sku and (r["store_id"] != args.store or r["size"] != args.size):
            continue
        rows[r["sku"]].append(r)
        meta[r["sku"]] = (r["store_id"], r["site_number"], r["size"])

    if not rows:
        print("no rows matched", file=sys.stderr)
        return 1

    for sku in sorted(rows):
        store, site, size = meta[sku]
        ev = sorted(rows[sku], key=lambda r: r["date"])
        print(f"\n=== {sku}   store {store} (site {site})   {size} ===")
        price = promo = None
        for e in ev:
            if e["field"] == "price" and price is None:
                price = num(e["old"])
            if e["field"] == "promo" and promo is None:
                promo = e["old"] or ""
        if price is None:
            print("  no price history for this SKU")
            continue

        fee = args.admin + args.insurance
        hdr = (f"  {'date':12}{'list':>8}{'promotion':>24}{'month 1':>10}"
               f"{'advertised 12-mo saving':>26}")
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))

        def line(d, P, pr, note=""):
            fn = PROMO_MODEL.get(pr)
            if fn is None:
                m1 = sav = None
            else:
                m1 = fn(P, 1) + fee
                sav = saving(P, pr)
            print(f"  {d:12}{'$'+format(P,'.0f'):>8}{(pr or '(none)'):>24}"
                  f"{('$'+format(m1,'.0f')) if m1 is not None else '?':>10}"
                  f"{('$'+format(sav,'.0f')) if sav is not None else '?':>26}"
                  f"{note}")

        first = ev[0]["date"]
        line(f"< {first}", price, promo or "", "   <- carried back from first row")
        for e in ev:
            if e["field"] == "price":
                price = num(e["new"])
            elif e["field"] == "promo":
                promo = e["new"] or ""
            else:
                continue
            line(e["date"], price, promo or "")

        if fee:
            print(f"\n  month 1 includes ${args.admin:,.0f} at signing "
                  f"+ ${args.insurance:,.0f} insurance")
        print("\n  The advertised saving is a fixed fraction of the list price, so it")
        print("  rises when the list price rises. A larger saving on the page does not")
        print("  imply a lower price; both columns move together.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
