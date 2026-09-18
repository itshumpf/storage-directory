#!/usr/bin/env python3
"""What a new customer pays, before and after a promotion swap, by tenure.

WHAT THIS ANSWERS
-----------------
On 2026-08-01 4,904 units were moved onto "40% off For 4 Month" and had their
advertised rate raised the same day. Whether that left customers better or worse
off depends entirely on how long they stay, and the answer is not monotonic.

This script models cumulative out-of-pocket cost for a customer signing on the
day BEFORE the change against one signing on the day OF it, month by month, and
reports the month at which the second customer stops paying more.

Run:
    python analysis/promo_cost_model.py
    python analysis/promo_cost_model.py --admin 29 --insurance 20
    python analysis/promo_cost_model.py --months 12

WHY NOT JUST QUOTE THE FOUR-MONTH FIGURE
----------------------------------------
Because four months is the most flattering horizon in the first two years, and
picking it is an easy mistake to make -- the promotion says "4 Month", so four
months looks like the natural window. It is not. Month 4 is the last month
before the discount expires and rent steps up to the new, higher list price, so
it is a local MINIMUM of the share paying more. Quoting it alone understates the
effect by a wide margin. The tenure sweep below exists so that nobody has to
take one horizon on trust.

FEES
----
A flat admin fee at signing and a monthly insurance charge are identical on both
sides of the comparison, so they cancel out of every delta. They are included
anyway because they change the LEVEL, and the level is what a customer
recognises: "$1 first month rent" is a $50 day-one outlay once a $29 admin fee
and $20 of insurance are added. Set either to 0 to see rent alone.

WHAT THIS CANNOT SEE
--------------------
  * Existing tenants. These are advertised new-customer rates. Sitting tenants
    follow a separate process that is not in this dataset, and nothing here
    establishes that they were moved at all.

    DIRECTION OF THE RESULTING BIAS, stated because it is not symmetric: the
    model holds the list rate FLAT from month 5 to the end of the horizon. If
    an operator raises rates on sitting tenants at any point inside that
    window, every break-even month reported below is too EARLY and every
    "share paying more" is too LOW. The reported figures are therefore a
    floor, not an estimate, and not a worst case. This file asserts nothing
    about whether any such increase occurs -- it is invisible here, because a
    rented unit leaves the advertised inventory the scraper reads. Anyone
    wanting to close that gap should look for the operator's own disclosure of
    existing-customer rate practices in its SEC filings, which is a public,
    citable source; this dataset cannot substitute for it.
  * Real promotion terms. The models below read short advertising strings at
    face value. Deposits, minimum stays, eligibility rules, prorating and
    insurance waivers for customers with their own coverage are not visible.
  * Transacted rents, or whether any customer took any of these offers.
"""
import argparse
import collections
import csv
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_LOG = os.path.join(HERE, "..", "history", "rate_changes.csv")

# Rent due in month `m` (1-indexed) at list price P under each advertised promo.
# Stated as data so the assumption is auditable and arguable in one place.
#
# VERIFIED 2026-08-24 against a live store page (Costa Mesa CA, store 235).
# The page displays, per unit, "Month 1-4  40% OFF  <struck full price> <price>"
# and "Months 5-12  In-Store Rent  <full price>" — which is exactly
# 0.6*P for m<=4, P thereafter:
#
#     unit      full   shown m1-4   0.6*P    operator's stated
#                                            "12-Month Savings"   4*(P-d)
#     5x5       $110      $66       $66.0        $176              $176
#     5x10      $165      $99       $99.0        $264              $264
#     7.5x10    $237     $142      $142.2        $380              $380
#
# Two independent confirmations in one screenshot: the per-month figures match
# the multiplier, and the operator's own "Total Estimated 12-Month Savings"
# equals four months of discount exactly — so the entire quoted twelve-month
# saving accrues in months 1-4, which is what this model assumes and what the
# break-even result depends on. Displayed prices are truncated, not rounded
# ($142 from $142.20); the effect on any median here is nil.
#
# Disposition of each assumption in the "40% off For 4 Month" model:
#
#   discount is 40% of the list price   verified — displayed price = 0.6 x list
#                                       on three units, exactly
#   months 1-4, full rate from month 5  verified — the page states the two
#                                       windows explicitly, and the operator's
#                                       own 12-month savings total equals four
#                                       months of discount
#   fees sit outside the discount       stated in the published terms: the
#                                       rental fee and administrative fees are
#                                       separate line items
#   new sign-ups, not sitting tenants   stated in the published terms
#
# Nothing here rests on private knowledge, and this file names no individual as
# a source. Every line above is checkable by loading a store page and reading
# the published terms.
#
# The other promo strings below are NOT verified this way and remain readings
# of advertising copy. They matter less: they set the BEFORE side of the
# comparison, and the break-even headline is driven by the AFTER side.
PROMO_MODEL = {
    "$1 first month rent": lambda P, m: 1.0 if m == 1 else P,
    "First month 50% off": lambda P, m: 0.5 * P if m == 1 else P,
    "50% off 1st Month":   lambda P, m: 0.5 * P if m == 1 else P,
    "2nd Month Free":      lambda P, m: 0.0 if m == 2 else P,
    "$1 Special":          lambda P, m: 1.0 if m == 1 else P,
    "40% off For 4 Month": lambda P, m: 0.6 * P if m <= 4 else P,
    "":                    lambda P, m: P,
}


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def cumulative(P, promo, months, admin, insurance):
    """Total out-of-pocket through `months`, or None if the promo is unmodelled."""
    fn = PROMO_MODEL.get(promo)
    if fn is None:
        return None
    return sum(fn(P, m) for m in range(1, months + 1)) + admin + insurance * months


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", default=DEFAULT_LOG)
    ap.add_argument("--date", default="2026-08-01")
    ap.add_argument("--promo", default="40% off For 4 Month")
    ap.add_argument("--admin", type=float, default=29.0,
                    help="one-off fee at signing, charged either side (default 29)")
    ap.add_argument("--insurance", type=float, default=20.0,
                    help="monthly charge, charged either side (default 20)")
    ap.add_argument("--months", type=int, default=8, help="tenure sweep depth")
    ap.add_argument("--horizon", type=int, default=24,
                    help="how far to search for break-even (default 24)")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print(f"no rate log at {args.log}", file=sys.stderr)
        return 1

    price, promo = {}, {}
    for r in csv.DictReader(open(args.log, newline="", encoding="utf-8")):
        if r["date"] != args.date:
            continue
        if r["field"] == "price":
            o, n = num(r["old"]), num(r["new"])
            if o is not None and n is not None and o > 0:
                price[r["sku"]] = (o, n)
        elif r["field"] == "promo":
            promo[r["sku"]] = (r["old"] or "", r["new"] or "")

    recs, unmodelled = [], collections.Counter()
    for sku, (was, now) in promo.items():
        if now != args.promo or sku not in price:
            continue
        Po, Pn = price[sku]
        if was not in PROMO_MODEL:
            unmodelled[was] += 1
            continue
        recs.append((was, Po, Pn))

    if not recs:
        print("no modellable units", file=sys.stderr)
        return 1

    print(f"rate log  : {os.path.normpath(args.log)}")
    print(f"date      : {args.date}")
    print(f"switch    : prior promotion  ->  {args.promo!r}")
    print(f"fees      : ${args.admin:,.0f} at signing + ${args.insurance:,.0f}/month, "
          f"applied to BOTH sides (they cancel in every delta)")
    print(f"units     : {len(recs):,} modelled"
          + (f", {sum(unmodelled.values()):,} skipped" if unmodelled else ""))
    for t, n in unmodelled.most_common():
        print(f"            skipped {n:,} with unmodelled prior promo {t!r}")
    print()

    groups = collections.defaultdict(list)
    for was, Po, Pn in recs:
        groups[was or "(no promotion)"].append((was, Po, Pn))

    # Portfolio-style accounting for the event cohort.  This uses the same
    # definitions as september14_offer_value.py so the two waves can be
    # compared without mixing aggregate percentages and per-offer medians.
    base0 = sum(4 * Po for was, Po, Pn in recs)
    base1 = sum(4 * Pn for was, Po, Pn in recs)
    cost0 = sum(cumulative(Po, was, 4, 0, 0) for was, Po, Pn in recs)
    cost1 = sum(cumulative(Pn, args.promo, 4, 0, 0) for was, Po, Pn in recs)
    savings0, savings1 = base0 - cost0, base1 - cost1
    print("=== FOUR-MONTH ACCOUNTING FOR THE EVENT COHORT ===")
    print(f"headline value before promotions: ${base0:,.0f} -> ${base1:,.0f} "
          f"({base1 / base0 - 1:+.1%})")
    print(f"promotional discount supplied:   ${savings0:,.0f} -> ${savings1:,.0f} "
          f"({savings1 / savings0 - 1:+.1%})")
    print(f"effective customer cost:          ${cost0:,.0f} -> ${cost1:,.0f} "
          f"({cost1 / cost0 - 1:+.1%})")
    print(f"promo share of headline value:    {savings0 / base0:.1%} -> "
          f"{savings1 / base1:.1%}\n")

    def worse_share(rows, m):
        w = sum(1 for was, Po, Pn in rows
                if cumulative(Pn, args.promo, m, args.admin, args.insurance)
                > cumulative(Po, was, m, args.admin, args.insurance))
        return w / len(rows)

    print("=== SHARE PAYING MORE UNDER THE NEW REGIME, BY TENURE ===")
    hdr = f"{'prior promotion':24}{'n':>7}" + "".join(f"{'m'+str(m):>6}"
                                                      for m in range(1, args.months + 1))
    print(hdr)
    print("-" * len(hdr))
    order = sorted(groups.items(), key=lambda kv: -len(kv[1]))
    for k, rows in order:
        print(f"{k:24}{len(rows):>7,}"
              + "".join(f"{worse_share(rows, m):>5.0%} " for m in range(1, args.months + 1)))
    print("-" * len(hdr))
    allrows = [r for rows in groups.values() for r in rows]
    shares = [worse_share(allrows, m) for m in range(1, args.months + 1)]
    print(f"{'ALL':24}{len(allrows):>7,}" + "".join(f"{s:>5.0%} " for s in shares))

    lo = min(range(len(shares)), key=lambda i: shares[i]) + 1
    print(f"\n  minimum at month {lo} ({shares[lo-1]:.0%}); "
          f"by month {args.months}, {shares[-1]:.0%} pay more.")
    if lo <= 4:
        print(f"  Month {lo} is the most favourable horizon shown. Quoting it alone")
        print("  understates the effect — which is the trap this script exists to avoid.")

    print("\n=== LEVELS AT MONTH 1 AND MONTH 4 ===")
    print("  Δ is the median of the per-unit differences, NOT the difference of the")
    print("  two medians — those are not the same statistic and only the first one")
    print("  describes what a typical customer experiences.")
    print(f"{'prior promotion':24}{'n':>7}{'m1 old':>9}{'m1 new':>9}{'m1 Δ':>8}"
          f"{'m1 %worse':>11}{'m4 old':>9}{'m4 new':>9}{'m4 Δ':>8}{'m4 %worse':>11}")
    for k, rows in order:
        def med(m, which):
            return statistics.median(
                cumulative(Pn if which else Po, args.promo if which else was,
                           m, args.admin, args.insurance)
                for was, Po, Pn in rows)

        def meddelta(m):
            return statistics.median(
                cumulative(Pn, args.promo, m, args.admin, args.insurance)
                - cumulative(Po, was, m, args.admin, args.insurance)
                for was, Po, Pn in rows)
        print(f"{k:24}{len(rows):>7,}{med(1,0):>9.0f}{med(1,1):>9.0f}"
              f"{meddelta(1):>+8.0f}{worse_share(rows,1):>10.1%} {med(4,0):>9.0f}"
              f"{med(4,1):>9.0f}{meddelta(4):>+8.0f}{worse_share(rows,4):>10.1%}")
    def alldelta(m):
        return statistics.median(
            cumulative(Pn, args.promo, m, args.admin, args.insurance)
            - cumulative(Po, was, m, args.admin, args.insurance)
            for was, Po, Pn in allrows)
    print(f"{'ALL':24}{len(allrows):>7,}"
          f"{statistics.median(cumulative(Po, was, 1, args.admin, args.insurance) for was, Po, Pn in allrows):>9.0f}"
          f"{statistics.median(cumulative(Pn, args.promo, 1, args.admin, args.insurance) for was, Po, Pn in allrows):>9.0f}"
          f"{alldelta(1):>+8.0f}{worse_share(allrows,1):>10.1%} "
          f"{statistics.median(cumulative(Po, was, 4, args.admin, args.insurance) for was, Po, Pn in allrows):>9.0f}"
          f"{statistics.median(cumulative(Pn, args.promo, 4, args.admin, args.insurance) for was, Po, Pn in allrows):>9.0f}"
          f"{alldelta(4):>+8.0f}{worse_share(allrows,4):>10.1%}")

    print(f"\n=== BREAK-EVEN: FIRST MONTH THE NEW REGIME STOPS COSTING MORE ===")
    be = collections.Counter()
    for was, Po, Pn in allrows:
        hit = None
        for m in range(1, args.horizon + 1):
            if (cumulative(Pn, args.promo, m, args.admin, args.insurance)
                    <= cumulative(Po, was, m, args.admin, args.insurance)):
                hit = m
                break
        be[hit] += 1
    for k in sorted(be, key=lambda x: (x is None, x)):
        label = f"never within {args.horizon} months" if k is None else (
            "month 1 (never worse off)" if k == 1 else f"month {k}")
        print(f"  {label:28} {be[k]:>7,}  ({be[k]/len(allrows):6.1%})")
    never = be.get(None, 0)
    print(f"\n  {never:,} of {len(allrows):,} ({never/len(allrows):.1%}) never break even "
          f"within {args.horizon} months.")
    print("  Mechanism: the discount is temporary and the rate rise is not. Once the")
    print("  promotional months lapse, the customer pays the new higher list price.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
