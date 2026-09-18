#!/usr/bin/env python3
"""Classify Public Storage SKUs added between 2026-09-12 and 2026-09-13.

The classification uses every immutable Public Storage snapshot currently in
history/, the pre-September event ledger, and exact historical
enriched_locations.json objects in Git.  A key is (store_id, sku); an SKU alone
is not assumed globally unique.
"""
from __future__ import annotations

import json
import re
import statistics
import subprocess
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
HISTORY = ROOT / "history" / "publicstorage"
BEFORE = "2026-09-12"
AFTER = "2026-09-13"
DATE_RE = re.compile(r"20\d\d-\d\d-\d\d")


def load(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))


def unit_map(stores: list[dict]) -> dict[tuple[str, str], tuple[dict, dict]]:
    out = {}
    for store in stores:
        sid = str(store.get("store_id") or "")
        for unit in store.get("units") or []:
            sku = str(unit.get("sku") or "")
            if sid and sku:
                out[(sid, sku)] = (store, unit)
    return out


def historical_git_keys(targets: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Return target keys found in dated pre-AFTER Git snapshots."""
    found = set()
    raw = subprocess.check_output(
        ["git", "log", "--all", "--format=%H%x09%s", "--", "enriched_locations.json"],
        cwd=ROOT, text=True, encoding="utf-8", errors="replace")
    commits = []
    seen_dates = set()
    for line in raw.splitlines():
        commit, sep, subject = line.partition("\t")
        match = DATE_RE.search(subject)
        if not sep or not match or match.group() >= AFTER or match.group() in seen_dates:
            continue
        seen_dates.add(match.group())
        commits.append((match.group(), commit))
    for date, commit in sorted(commits, reverse=True):
        if found == targets:
            break
        try:
            blob = subprocess.check_output(
                ["git", "show", f"{commit}:enriched_locations.json"], cwd=ROOT)
            stores = json.loads(blob)
        except (subprocess.CalledProcessError, json.JSONDecodeError):
            continue
        keys = set(unit_map(stores))
        found |= targets & keys
        print(f"git history {date}: {len(keys):,} keys; "
              f"{len(found):,}/{len(targets):,} additions previously seen")
    return found


def ledger_keys_before(date: str) -> set[tuple[str, str]]:
    import csv
    path = ROOT / "history" / "rate_changes.csv"
    keys = set()
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("brand") == "publicstorage" and row.get("date", "") < date:
                keys.add((str(row.get("store_id") or ""), str(row.get("sku") or "")))
    return keys


def median(values):
    return statistics.median(values) if values else None


def pct(n, d):
    return f"{n / d:.1%}" if d else "n/a"


def main() -> None:
    before_stores = load(HISTORY / f"{BEFORE}.json")
    after_stores = load(HISTORY / f"{AFTER}.json")
    before, after = unit_map(before_stores), unit_map(after_stores)
    additions = set(after) - set(before)
    removals = set(before) - set(after)

    earlier_files = sorted(p for p in HISTORY.glob("*.json") if p.stem < BEFORE)
    prior_snapshot_keys = set()
    day_keys = {}
    for path in earlier_files:
        keys = set(unit_map(load(path)))
        day_keys[path.stem] = keys
        prior_snapshot_keys |= keys

    seen_snapshot = additions & prior_snapshot_keys
    remaining = additions - seen_snapshot
    seen_ledger = remaining & ledger_keys_before(AFTER)
    remaining -= seen_ledger
    seen_git = historical_git_keys(remaining)
    never = remaining - seen_git
    returning = additions - never

    def raw_stats(stores):
        records = [u for s in stores for u in (s.get("units") or [])]
        return (len(records), sum(not u.get("sku") for u in records),
                sum(int(u.get("count") or 0) for u in records))

    raw_before = raw_stats(before_stores)
    raw_after = raw_stats(after_stores)

    print("\n=== CLASSIFICATION ===")
    print(f"raw listing records: {raw_before[0]:,} -> {raw_after[0]:,} ({raw_after[0] - raw_before[0]:+,})")
    print(f"records without SKU: {raw_before[1]:,} -> {raw_after[1]:,}")
    print(f"advertised count sum: {raw_before[2]:,} -> {raw_after[2]:,} ({raw_after[2] - raw_before[2]:+,})")
    print(f"{BEFORE}: {len(before):,} store-SKU records")
    print(f"{AFTER}: {len(after):,} store-SKU records")
    print(f"additions: {len(additions):,}")
    print(f"removals:  {len(removals):,}")
    print(f"returning, seen in Sep snapshots: {len(seen_snapshot):,} ({pct(len(seen_snapshot), len(additions))})")
    print(f"returning, found only in ledger:  {len(seen_ledger):,} ({pct(len(seen_ledger), len(additions))})")
    print(f"returning, found only in Git:     {len(seen_git):,} ({pct(len(seen_git), len(additions))})")
    print(f"never observed before:            {len(never):,} ({pct(len(never), len(additions))})")

    def advertised_count(mapping, key):
        return int(mapping[key][1].get("count") or 0)

    added_inventory = sum(advertised_count(after, key) for key in additions)
    removed_inventory = sum(advertised_count(before, key) for key in removals)
    retained_keys = set(before) & set(after)
    retained_deltas = Counter(advertised_count(after, key) - advertised_count(before, key)
                              for key in retained_keys)
    retained_inventory_delta = sum(delta * n for delta, n in retained_deltas.items())
    print("\n=== ADVERTISED COUNT DECOMPOSITION ===")
    print(f"counts carried by added SKU records:   +{added_inventory:,}")
    print(f"  returning SKU records:               +{sum(advertised_count(after, key) for key in returning):,}")
    print(f"  never-seen SKU records:              +{sum(advertised_count(after, key) for key in never):,}")
    print(f"counts carried by removed SKU records: -{removed_inventory:,}")
    print(f"count changes on retained SKU records: {retained_inventory_delta:+,}")
    print(f"reconciled total change:               "
          f"{added_inventory - removed_inventory + retained_inventory_delta:+,}")
    print("retained SKU count deltas: " + ", ".join(
        f"{delta:+} on {n:,}" for delta, n in retained_deltas.most_common(10)))

    adds_by_store = Counter(sid for sid, sku in additions)
    never_by_store = Counter(sid for sid, sku in never)
    counts_before = Counter(sid for sid, sku in before)
    counts_after = Counter(sid for sid, sku in after)
    zero_before = sum(n for sid, n in adds_by_store.items() if counts_before[sid] == 0)
    print("\n=== DISTRIBUTION ACROSS STORES ===")
    print(f"stores with additions: {len(adds_by_store):,}")
    print(f"stores with never-seen additions: {len(never_by_store):,}")
    print(f"additions at stores with zero 9/12 records: {zero_before:,} ({pct(zero_before, len(additions))})")
    print(f"median additions per affected store: {median(list(adds_by_store.values())):,.0f}")

    # Locate the previously quoted 3,275-store pattern by distinguishing
    # listing-record counts from each listing's advertised availability count.
    listing_before = {str(s["store_id"]): len(s.get("units") or []) for s in before_stores}
    listing_after = {str(s["store_id"]): len(s.get("units") or []) for s in after_stores}
    avail_before = {str(s["store_id"]): sum(int(u.get("count") or 0) for u in (s.get("units") or []))
                    for s in before_stores}
    avail_after = {str(s["store_id"]): sum(int(u.get("count") or 0) for u in (s.get("units") or []))
                   for s in after_stores}
    common_stores = set(listing_before) & set(listing_after)
    print(f"stores whose listing-record count increased: "
          f"{sum(listing_after[s] > listing_before[s] for s in common_stores):,}")
    print(f"stores whose advertised count sum increased: "
          f"{sum(avail_after[s] > avail_before[s] for s in common_stores):,}")

    for day in ("2026-09-09", "2026-09-10", "2026-09-11"):
        keys = day_keys.get(day, set())
        overlap = additions & keys
        print(f"9/13 additions already present on {day}: {len(overlap):,} ({pct(len(overlap), len(additions))})")

    states = Counter(after[key][0].get("state") or "?" for key in additions)
    print("top states by additions: " + ", ".join(f"{s} {n:,}" for s, n in states.most_common(10)))

    # Store/size matched pricing comparison on 9/13.  This removes most of the
    # obvious mix distortion from comparing, say, added 10x30s with retained 5x5s.
    retained = set(after) & set(before)
    peer_prices = defaultdict(list)
    for key in retained:
        store, unit = after[key]
        price = unit.get("price")
        if isinstance(price, (int, float)) and price > 0:
            peer_prices[(key[0], str(unit.get("size") or ""))].append(float(price))

    def ratios(keys):
        values = []
        for key in keys:
            store, unit = after[key]
            price = unit.get("price")
            peers = peer_prices.get((key[0], str(unit.get("size") or "")), [])
            if isinstance(price, (int, float)) and price > 0 and peers:
                values.append(float(price) / statistics.median(peers))
        return values

    for label, keys in (("returning", returning), ("never-seen", never)):
        rs = ratios(keys)
        print(f"\n=== {label.upper()} PRICE VS RETAINED SAME-STORE/SAME-SIZE PEER ===")
        print(f"comparable records: {len(rs):,}/{len(keys):,}")
        if rs:
            print(f"median price ratio: {statistics.median(rs):.3f}")
            print(f"priced lower: {sum(r < 1 for r in rs):,} ({pct(sum(r < 1 for r in rs), len(rs))})")
            print(f"same price:   {sum(r == 1 for r in rs):,} ({pct(sum(r == 1 for r in rs), len(rs))})")
            print(f"priced higher:{sum(r > 1 for r in rs):,} ({pct(sum(r > 1 for r in rs), len(rs))})")

    attrs = sum(bool(after[key][1].get("attrs")) for key in additions)
    print("\n=== ENRICHMENT CHECK ===")
    print(f"added records carrying attrs on 9/13: {attrs:,}/{len(additions):,} ({pct(attrs, len(additions))})")
    print("Unit records originate in fetch_pricing() Phase 6; Phase 7 can only add attrs, price_min, and price_max to existing records.")

    print("\n=== CONSECUTIVE-DAY SKU CHURN ===")
    dated = []
    for path in sorted(HISTORY.glob("*.json")):
        if path.stem <= AFTER:
            dated.append((path.stem, set(unit_map(load(path)))))
    for (d0, k0), (d1, k1) in zip(dated, dated[1:]):
        print(f"{d0} -> {d1}: +{len(k1-k0):,} / -{len(k0-k1):,}; net {len(k1)-len(k0):+,}")


if __name__ == "__main__":
    main()
