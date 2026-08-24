"""
update_rate_log.py — Append street-rate and promo change events to the log.

Run after a scrape (from the repo root):
    python analysis/update_rate_log.py

Diffs enriched_locations_backup.json (yesterday's dataset, written by the
scraper before saving) against enriched_locations.json (today's), matching
units by SKU, and appends one row per change to history/rate_changes.csv:

    date, store_id, site_number, size, sku, field, old, new

field is "price" or "promo". Because the advertised min-max range is
mechanically price ±20%, price changes are the only real repricing signal —
this log is the closest public record of how Public Storage moves rates.
"""
import csv
import datetime
import json
import sys
from pathlib import Path

OLD = Path("enriched_locations_backup.json")
NEW = Path("enriched_locations.json")
LOG = Path("history/rate_changes.csv")

def sku_map(stores):
    """SKU -> (store_id, site_number, size, price, promo), priced units only.

    A unit is excluded until it carries a real price, so its first real price
    reads as a first sighting (no row) rather than as a change.

    Zero is rejected as well as None. `is not None` alone let zero-priced
    units into the map, and the next scrape logged them as `0 -> 81`, which
    is a unit arriving, not a repricing. 56 such rows exist in the log as of
    2026-08-23 — all of them the earliest row for their SKU, and strictly
    one-way (56 rows `0 -> price`, zero rows `price -> 0`). They are excluded
    from median and direction stats by the reader but were still counted in
    daily totals, so counts and statistics described different row sets.
    """
    out = {}
    for s in stores:
        for u in s.get("units", []):
            sku = u.get("sku")
            price = u.get("price")
            if sku and isinstance(price, (int, float)) and price > 0:
                out[sku] = (str(s.get("store_id")), s.get("site_number") or "",
                            u.get("size") or "", price, u.get("promo") or "")
    return out

def main():
    if not OLD.exists():
        print(f"No {OLD} to diff against — skipping (first run after a fresh checkout).")
        return
    date = datetime.date.today().isoformat()
    old = sku_map(json.loads(OLD.read_text(encoding="utf-8")))
    new = sku_map(json.loads(NEW.read_text(encoding="utf-8")))

    if new and not old:
        print(f"Warning: {OLD} yielded 0 SKUs (it may predate the 'sku' field "
              "in the scraper output — an older schema, not a real empty "
              "dataset). No changes can be logged against it. The log resumes "
              "once two same-schema snapshots are diffed back to back.")
    elif old and new and not (set(old) & set(new)):
        print(f"Warning: 0 of {len(new):,} SKUs in {NEW} matched any of the "
              f"{len(old):,} in {OLD} — likely a schema change rather than a "
              "real 100% inventory turnover. No changes will be logged this "
              "run; the log resumes once two same-schema snapshots are diffed "
              "back to back.")

    if LOG.exists():
        with open(LOG, newline="", encoding="utf-8") as f:
            if any(row.startswith(date + ",") for row in f):
                print(f"{date} already logged in {LOG} — skipping")
                return

    events = []
    for sku, (sid, site, size, price, promo) in new.items():
        prev = old.get(sku)
        if not prev:
            continue
        _, _, _, old_price, old_promo = prev
        if price != old_price:
            events.append([date, sid, site, size, sku, "price", old_price, price])
        if promo != old_promo:
            events.append([date, sid, site, size, sku, "promo", old_promo, promo])

    new_file = not LOG.exists()
    LOG.parent.mkdir(exist_ok=True)
    with open(LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new_file:
            w.writerow(["date", "store_id", "site_number", "size", "sku", "field", "old", "new"])
        w.writerows(events)
    hikes = sum(1 for e in events if e[5] == "price" and e[7] > e[6])
    cuts = sum(1 for e in events if e[5] == "price" and e[7] < e[6])
    promos = sum(1 for e in events if e[5] == "promo")
    print(f"Logged {len(events)} events for {date} ({hikes} hikes, {cuts} cuts, {promos} promo switches) -> {LOG}")

if __name__ == "__main__":
    main()
