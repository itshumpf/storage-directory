"""
check_enrichment.py — fail the run when Phase 7 stopped producing data.

WHY
---
On 2026-09-01, 0 of 60,132 units in enriched_locations.json carried `attrs`,
`price_min` or `price_max`. Public Storage had renamed the JSON-LD price fields
("price":"$38 - $57" became "lowPrice":"38","highPrice":"57") and OFFER_RE
matched nothing. The scrape reported success every day throughout, because a
regex that finds nothing is indistinguishable from a store that has no offers.
Nothing in the pipeline could tell those apart, so nobody found out until the
field was looked at directly. The number of days lost is not recoverable.

WHERE THIS RUNS, AND WHY IT MATTERS
-----------------------------------
This is deliberately a SEPARATE step that runs LAST, after the commit — not a
check inside daily_scraper.py.

If the scraper aborted on this condition, Phase 8 would never save and the
day's pricing would be lost to protect a secondary field. If a workflow step
before the commit failed on it, every later step would be skipped and the day
would be lost the same way. That is the exact failure this repo already ate
once, on 2026-08-31, when build_repricing_stats.py exited 1 and took 4,670
stores of collected data down with it.

So: collect, save, commit, and only THEN go red. The data is safe on disk and
in git before this file is allowed to have an opinion, and a non-zero exit here
turns the run red purely so it gets noticed.

ON THE THRESHOLD
----------------
MIN_UNIT_COVERAGE is a judgement call, not a measurement. Coverage has never
been observed healthy — the field was already broken by the time anyone looked
at it — so there is no historical baseline to derive a floor from. It is set
low on purpose: this is meant to catch "the parser died", not to police normal
variation. Once a few healthy runs are on record, set it from what they show
and say so here.

Zero is the unambiguous case and is always a failure regardless of threshold.
"""
import json
import os
import sys

DATA = "enriched_locations.json"
MIN_UNIT_COVERAGE = 0.25       # provisional; see note above


def main():
    if not os.path.exists(DATA):
        print(f"{DATA} not found — nothing to check.", file=sys.stderr)
        return 1

    with open(DATA, encoding="utf-8") as f:
        stores = json.load(f)

    units = attrs = ranges = 0
    stores_with_units = stores_enriched = 0
    for s in stores:
        us = s.get("units") or []
        if us:
            stores_with_units += 1
        hit = False
        for u in us:
            units += 1
            if u.get("attrs"):
                attrs += 1
                hit = True
            if u.get("price_min") is not None:
                ranges += 1
        if hit:
            stores_enriched += 1

    cov = attrs / units if units else 0.0
    print(f"stores:            {len(stores):,}")
    print(f"stores with units: {stores_with_units:,}")
    print(f"stores enriched:   {stores_enriched:,}")
    print(f"units:             {units:,}")
    print(f"units with attrs:  {attrs:,} ({cov:.1%})")
    print(f"units with range:  {ranges:,}")

    if units == 0:
        print("\nFAIL: no units at all. This is a pricing failure, not an "
              "enrichment one — check Phase 6 before looking at Phase 7.",
              file=sys.stderr)
        return 1

    if attrs == 0:
        print(f"\nFAIL: 0 of {units:,} units carry 'attrs'.\n"
              f"Phase 7 fetched store pages and parsed nothing out of them. "
              f"The likely cause is OFFER_RE no longer matching the page "
              f"markup — it has happened before, in exactly this way.\n"
              f"Run `python dump_offer_html.py` to see the current shape of "
              f"the offer object, then fix the pattern against that.\n"
              f"The day's data IS saved and committed; only the attribute and "
              f"price-range fields are missing.", file=sys.stderr)
        return 1

    if cov < MIN_UNIT_COVERAGE:
        print(f"\nFAIL: enrichment coverage {cov:.1%} is below the "
              f"{MIN_UNIT_COVERAGE:.0%} floor.\n"
              f"Partial, so the parser is not dead — more likely a subset of "
              f"store pages changed, or Phase 7 was cut short. Compare against "
              f"recent runs before changing the pattern.\n"
              f"The day's data IS saved and committed.", file=sys.stderr)
        return 1

    print(f"\nOK: enrichment coverage {cov:.1%}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
