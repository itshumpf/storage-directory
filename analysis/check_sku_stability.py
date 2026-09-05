"""
check_sku_stability.py — do this operator's unit IDs mean the same thing tomorrow?

    python analysis/check_sku_stability.py day1.json day2.json

Run this on two snapshots of the SAME operator taken on different days, BEFORE
committing to daily collection. It answers one question, and it is the question
the whole rate log rests on.

WHY IT HAS TO BE ASKED FIRST
----------------------------
update_rate_log.py finds repricing by matching yesterday's SKU to today's and
comparing the price. That works only if a SKU names the same unit on both days.
If an operator mints IDs per response -- a session token, a cache key, a row
number -- then every unit looks new every morning: the diff finds no matches,
logs nothing, and reports a clean run forever. Months of "no price changes"
that are really "no data".

The opposite failure is louder and worse. If IDs are positional, renting one
unit shifts every unit below it up a slot, and the next diff reports the entire
facility repricing at once. Every row false, every row indistinguishable from a
real event.

Neither failure raises an exception. Both are only visible by comparing two
days on purpose, which is what this does.

READING THE RESULT
------------------
  carried over    SKUs present on both days. This is the number that matters.
  new / dropped   normal in small amounts -- units sell out and come back.
  churn           dropped + new, as a share of the two days combined.

  < 10%   fine. Real inventory movement looks like this.
  10-40%  suspicious. Look at the examples before trusting a diff.
  > 40%   the IDs are not stable. Do not collect against them; the rate log
          will be fiction. Find a deterministic key first.

A carry-over of exactly zero is not "everything changed" -- it is almost
certainly a changed SKU *format* between the two files, which is a different
problem with a different fix, so it is called out separately.
"""
import collections
import json
import os
import sys


def load(path):
    if not os.path.exists(path):
        sys.exit(
            f"\nNo such file: {path}\n\n"
            f"Pass two real snapshots of the SAME operator, taken on different\n"
            f"days. 'day1.json' and 'day2.json' in the usage line are "
            f"placeholders,\nnot filenames that exist.\n\n"
            f"Public Storage writes enriched_locations.json each run and copies\n"
            f"the previous one to enriched_locations_backup.json, so on a "
            f"machine\nthat has run twice those two are the pair. Extra Space "
            f"and CubeSmart\nwrite their own output files — run each scraper "
            f"once, keep the file\nunder a dated name, run it again tomorrow, "
            f"then compare the two.\n"
        )
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                data = v
                break
    out = collections.defaultdict(dict)          # brand -> sku -> (price, store)
    stores = collections.defaultdict(set)        # brand -> store_ids seen
    for s in data:
        brand = s.get("brand") or "(untagged)"
        stores[brand].add(str(s.get("store_id")))
        for u in s.get("units") or []:
            sku = u.get("sku")
            if sku:
                out[brand][sku] = (u.get("price"), str(s.get("store_id")))
    return out, stores


def prefix(sku):
    """The leading token of a SKU, for spotting a format change."""
    for sep in ("_", "-", ":"):
        if sep in sku:
            return sku.split(sep, 1)[0] + sep
    return sku[:4]


def main():
    if len(sys.argv) != 3:
        print(__doc__.strip().split("\n\n")[1], file=sys.stderr)
        print("\nusage: python analysis/check_sku_stability.py "
              "<older-snapshot.json> <newer-snapshot.json>\n"
              "  e.g. python analysis/check_sku_stability.py "
              "enriched_locations_backup.json enriched_locations.json",
              file=sys.stderr)
        return 2

    (a, sa), (b, sb) = load(sys.argv[1]), load(sys.argv[2])
    brands = sorted(set(a) | set(b))
    if not brands:
        print("No branded units with SKUs in either file.", file=sys.stderr)
        return 1

    worst = 0.0
    evaluated = []          # brands that could actually be measured
    unevaluable = []        # brands that could not, and why it is not a pass
    for brand in brands:
        # Compare only stores that appear in BOTH files.
        #
        # Added 2026-09-01, after the first smoke test made it necessary. The
        # crawlers call random.shuffle() on the sitemap before crawling, so two
        # --limit runs visit two different random samples. Comparing those
        # whole-file would show near-total churn and condemn a perfectly stable
        # set of IDs — the SKUs did not change, the stores did.
        #
        # Restricting to the overlap makes a partial crawl answer the question
        # honestly, and changes nothing for two full crawls.
        shared = sa.get(brand, set()) & sb.get(brand, set())
        A = {k: v for k, v in a.get(brand, {}).items() if v[1] in shared}
        B = {k: v for k, v in b.get(brand, {}).items() if v[1] in shared}
        print(f"\n{'=' * 62}\n{brand}\n{'=' * 62}")
        print(f"  stores in both files: {len(shared):,} "
              f"(of {len(sa.get(brand, set())):,} and "
              f"{len(sb.get(brand, set())):,})")
        if not shared:
            print("  NO SHARED STORES — these two files cover different "
                  "facilities, so they cannot answer whether SKUs are stable. "
                  "The crawlers shuffle the sitemap, so two --limit runs will "
                  "do this. Crawl both days in full, or crawl enough of each "
                  "that they overlap.")
            unevaluable.append((brand, "no shared stores"))
            continue
        both = set(A) & set(B)
        only_a, only_b = set(A) - set(B), set(B) - set(A)
        total = len(set(A) | set(B))
        churn = (len(only_a) + len(only_b)) / total if total else 1.0
        worst = max(worst, churn)

        print(f"  {sys.argv[1]}: {len(A):,} priced SKUs")
        print(f"  {sys.argv[2]}: {len(B):,} priced SKUs")
        print(f"  carried over : {len(both):,}")
        print(f"  dropped      : {len(only_a):,}")
        print(f"  new          : {len(only_b):,}")
        print(f"  churn        : {churn:.1%}")

        evaluated.append(brand)

        if not both:
            pa = collections.Counter(prefix(s) for s in A).most_common(3)
            pb = collections.Counter(prefix(s) for s in B).most_common(3)
            print("\n  NOTHING CARRIED OVER. Before concluding the IDs are "
                  "unstable, check whether the SKU *format* changed between "
                  "these two files — a parser edit does this, and it looks "
                  "identical from here.")
            print(f"    {sys.argv[1]} prefixes: {pa}")
            print(f"    {sys.argv[2]} prefixes: {pb}")
            continue

        # The positional-SKU signature: SKUs survive, but an implausible share
        # of them changed price on the same night, because the list shifted.
        moved = sum(1 for s in both if A[s][0] != B[s][0])
        restored = sum(1 for s in both if A[s][1] != B[s][1])
        print(f"  of those carried over, {moved:,} changed price "
              f"({moved / len(both):.1%})")
        if moved / len(both) > 0.5:
            print("    WARNING: over half repriced overnight. Real repricing "
                  "is rarely this broad. If these SKUs are positional, a "
                  "single unit selling out shifts the list and fakes exactly "
                  "this pattern. Check a handful by hand against the site "
                  "before trusting any of it.")
        if restored:
            print(f"    {restored:,} SKUs moved to a different store_id — a SKU "
                  f"should never change store. Treat as a keying bug, not "
                  f"inventory.")

        for label, s in (("dropped", only_a), ("new", only_b)):
            if s:
                print(f"  example {label}: {sorted(s)[:3]}")

    print(f"\n{'=' * 62}")
    # An unmeasured brand is not a passing brand. Reporting "stable" because
    # nothing could be compared is the exact failure this script exists to
    # prevent, one level up.
    if unevaluable:
        for brand, why in unevaluable:
            print(f"UNEVALUABLE: {brand} — {why}. This is not a pass. Nothing "
                  f"was measured, so nothing is known.")
        if not evaluated:
            return 2
    if worst > 0.40:
        print(f"VERDICT: worst churn {worst:.1%} — the IDs are NOT stable "
              f"enough to diff against. Collecting daily against these keys "
              f"produces a rate log that looks full and means nothing. Find a "
              f"deterministic key before turning collection on.")
        return 1
    if worst > 0.10:
        print(f"VERDICT: worst churn {worst:.1%} — usable, but check the "
              f"examples above before trusting the first month of diffs.")
        return 0
    print(f"VERDICT: worst churn {worst:.1%} — stable. Safe to diff against.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
