"""Does one facility serve the same prices to two requests a minute apart?

    python check_price_stability.py 1049
    python check_price_stability.py 1049 --repeats 3 --delay 5

WHY THIS EXISTS
---------------
On 2026-09-02 two runs three hours apart produced sixteen change rows, all at
facility 1049, moving in both directions — 145 -> 199 and 40 -> 54 up, 32 -> 29
and 82 -> 79 down. The same pair of runs produced nine `delisted` rows that
each paired with a `listed` row of the same size at the same facility, which
says SKUs identify physical units rather than size classes.

Both observations have an innocent reading (the operator repriced; units
rented and the next one was advertised) and a fatal one (price and advertised
unit vary per *request*, so the whole series is noise). Three hours of elapsed
time cannot separate them. Three fetches a minute apart can.

WHAT A PASS DOES AND DOES NOT MEAN
----------------------------------
STABLE rules out per-request variation. It does **not** establish that prices
are stable over hours — this test cannot see that, and the 1049 changes may
still be real repricing. It only removes the explanation that would make every
number we collect meaningless.

A failure is decisive in the other direction: if two fetches sixty seconds
apart disagree, no amount of elapsed time is needed to explain a change row,
and the rate log is measuring the site's rendering rather than its prices.

This makes N requests to ONE facility, paced by the same PoliteSession the
collector uses, after the same robots check. Default 3.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import storagesense_scraper as sc
from storagesense_parser import parse_facility_html

FIELDS = ("price", "street_price", "promo", "available")


def _catalog_record(output: Path, site: str) -> dict:
    """Reuse the stored catalog row instead of re-fetching the whole catalog."""
    stored = sc._load_json(output, [])
    if not isinstance(stored, list) or not stored:
        raise SystemExit(f"No collected facilities in {output}. Run the collector first.")
    for row in stored:
        if isinstance(row, dict) and str(row.get("site_number")) == site:
            return row
    raise SystemExit(f"Site {site} is not in {output}.")


def _units(record: dict) -> dict[str, dict]:
    return {u["sku"]: u for u in record.get("units", []) if u.get("sku")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", help="site_number to re-read, e.g. 1049")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--delay", type=float, default=sc.DEFAULT_DELAY_SECONDS)
    parser.add_argument("--out", type=Path, default=sc.DEFAULT_OUTPUT)
    parser.add_argument("--state", type=Path, default=sc.DEFAULT_STATE)
    args = parser.parse_args()

    if args.repeats < 2:
        raise SystemExit("--repeats must be at least 2; one fetch compares to nothing")

    store = _catalog_record(args.out, str(args.site))
    session = sc.PoliteSession(args.state, args.delay)
    try:
        sc.check_robots(session)
    except sc.CrawlStopped as exc:
        print(f"STOPPED: {exc}")
        return 2

    reads: list[dict[str, dict]] = []
    for attempt in range(1, args.repeats + 1):
        try:
            record = parse_facility_html(sc.fetch_rendered_facility(session, store["url"]), store)
        except sc.CrawlStopped as exc:
            print(f"STOPPED: {exc}")
            return 2
        except Exception as exc:
            print(f"read {attempt}: FAILED {type(exc).__name__}: {exc}")
            return 2
        units = _units(record)
        reads.append(units)
        print(f"read {attempt}: {len(units)} unit classes")

    first = reads[0]
    sku_sets = [frozenset(r) for r in reads]
    rotates = len(set(sku_sets)) > 1
    shared = sorted(set.intersection(*(set(r) for r in reads)))

    varying: list[str] = []
    for sku in shared:
        for field in FIELDS:
            values = [r[sku].get(field) for r in reads]
            if len(set(values)) > 1:
                varying.append(f"  {first[sku].get('size','?'):>7} {sku} {field}: "
                               + " -> ".join(repr(v) for v in values))

    print()
    if rotates:
        stable_skus = frozenset.intersection(*sku_sets)
        print(f"SKU SET IS NOT STABLE across {args.repeats} reads seconds apart.")
        print(f"  {len(stable_skus)} SKUs in every read; "
              f"{len(frozenset.union(*sku_sets)) - len(stable_skus)} appeared in only some.")
        for index, skus in enumerate(sku_sets, 1):
            missing = sorted(frozenset.union(*sku_sets) - skus)
            if missing:
                print(f"  read {index} missing: {', '.join(missing)}")
        print("  => `delisted`/`listed` pairs are the site rotating which unit it")
        print("     advertises, NOT units renting. Size-level keys are required.")
    else:
        print(f"SKU set identical across {args.repeats} reads ({len(first)} SKUs).")

    if varying:
        print(f"\nPRICES VARY BETWEEN REQUESTS — {len(varying)} field(s):")
        for line in varying:
            print(line)
        print("\n  => the rate log is measuring rendering, not pricing. Do not build")
        print("     history on this until the cause is understood.")
    else:
        print(f"\nAll {len(shared)} shared SKUs served identical "
              f"{'/'.join(FIELDS)} in every read.")

    if rotates or varying:
        print("\nVERDICT: UNSTABLE — at least one of the 2026-09-02 findings is an")
        print("artifact of per-request variation rather than a real change.")
        return 1

    print("\nVERDICT: STABLE for per-request variation.")
    print("This does NOT show prices are stable over hours — it only removes the")
    print("explanation that would make every collected number meaningless. The")
    print("1049 moves and the SKU pairs remain unexplained, and need two reads")
    print("spaced by hours, not seconds, to settle.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
