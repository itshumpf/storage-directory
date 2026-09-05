"""
update_rate_log.py — Append street-rate and promo change events to the log.

Run after a scrape (from the repo root):
    python analysis/update_rate_log.py

Diffs enriched_locations_backup.json (yesterday's dataset, written by the
scraper before saving) against enriched_locations.json (today's), matching
units by SKU, and appends one row per change to history/rate_changes.csv:

    date, store_id, site_number, size, sku, field, old, new, brand

field is "price" or "promo". Because the advertised min-max range is
mechanically price ±20%, price changes are the only real repricing signal —
this log is the closest public record of how Public Storage moves rates.

ON THE brand COLUMN
-------------------
Added 2026-08-24, ahead of a second chain entering the dataset. Every row
names its operator explicitly; nothing infers one from absence. An untagged
row is indistinguishable from a row whose tag failed to be written, so this
script refuses to append rather than guessing:

  * if history/rate_changes.csv still has the old 8-column header, run
    analysis/backfill_brand.py first;
  * if the current scrape yields stores with no "brand" key, the scraper
    that produced it predates the tag — re-run daily_scraper.py.

Only the NEW side needs a tag. The backup snapshot supplies old prices only,
so a pre-tag backup is fine and the first run after this change works.
"""
import csv
import datetime
import json
import sys
import time
from pathlib import Path

OLD = Path("enriched_locations_backup.json")
NEW = Path("enriched_locations.json")
LOG = Path("history/rate_changes.csv")

HEADER = ["date", "store_id", "site_number", "size", "sku",
          "field", "old", "new", "brand"]

# ---------------------------------------------------------------------------
# Run log — added 2026-08-30
#
# Every path out of main() writes one row here, including the paths that write
# no rate rows at all. Without it, a day where the diff *could not run* and a
# day where *nothing changed* are the same thing from the outside: an absent
# date in rate_changes.csv.
#
# That is not hypothetical. 2026-07-22, 2026-08-08 and 2026-08-15 have no rows
# in the log. All three scraped normally — the snapshots for those days are
# present, distinct, and unique by content hash — so the collection worked and
# something here declined to write. Which of the paths below fired on each day
# is not recoverable, because nothing recorded it. Going forward it is.
#
# An empty result has to say which kind of empty it is: no data exists, the
# question could not be asked, or it was never asked at all.
# ---------------------------------------------------------------------------
RUNLOG = Path("history/rate_log_runs.csv")
RUNLOG_HEADER = ["date", "status", "baseline_age_days", "old_skus", "new_skus",
                 "matched_skus", "events", "note"]


def baseline_age_days():
    """Days between the baseline snapshot's mtime and now, or "" if unknown.

    Added 2026-08-31. A diff against a six-day-old baseline produces six days
    of change stamped with one date, and nothing in rate_changes.csv can tell
    that apart from a normal day. On 2026-08-31 the first run after a week of
    collection being off logged 55,011 events where a typical day is 1,400 to
    5,600 — a 10x anomaly with no field on the row explaining it.

    An interval is a property of the measurement, not a detail. If it is not
    recorded, every consumer of this log silently assumes it is one day.
    """
    try:
        age = time.time() - OLD.stat().st_mtime
        return round(age / 86400.0, 2)
    except OSError:
        return ""


def record(date, status, note="", old_n="", new_n="", matched="", events=""):
    """Append one row describing what this invocation did. Never raises."""
    try:
        RUNLOG.parent.mkdir(exist_ok=True)
        new_file = not RUNLOG.exists()
        with open(RUNLOG, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            if new_file:
                w.writerow(RUNLOG_HEADER)
            w.writerow([date, status, baseline_age_days(), old_n,
                        new_n, matched, events, note])
    except Exception as e:                       # never break the pipeline
        print(f"warning: could not write {RUNLOG}: {e}", file=sys.stderr)


def log_header(path):
    """The header actually on disk, or None if the file does not exist."""
    if not path.exists():
        return None
    with open(path, newline="", encoding="utf-8") as f:
        return next(csv.reader(f), None)


def sku_map(stores):
    """(brand, SKU) -> (store_id, site_number, size, price, promo, brand), priced only.

    KEYED ON (brand, sku), NOT sku — changed 2026-09-01, before a second and
    third operator start collecting.

    This map is flat across every store in the file, so the key has to be
    unique across every store in the file. A bare SKU is only unique inside one
    operator's own numbering. Public Storage's are 'V_1518528'; Extra Space's
    are '2555_1275' (unit-type and site joined); CubeSmart's are unknown until
    it runs. Nothing stops two operators minting the same string, and if they
    ever did, the second one silently overwrote the first — one unit's price
    would vanish from the diff and another unit's price change would be
    attributed to the wrong store, with no error and nothing on the row to show
    it happened.

    The odds are low. The failure is invisible, permanent, and would sit inside
    the one file this project exists to produce, so it is not worth carrying.
    Pairing the brand with the SKU costs nothing and removes the class.

    ON THE PRICE FILTER
    -------------------
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
        brand = s.get("brand") or ""
        for u in s.get("units", []):
            sku = u.get("sku")
            price = u.get("price")
            if sku and isinstance(price, (int, float)) and price > 0:
                out[(brand, sku)] = (str(s.get("store_id")),
                                     s.get("site_number") or "",
                                     u.get("size") or "", price,
                                     u.get("promo") or "", brand)
    return out

def main():
    date = datetime.date.today().isoformat()

    if not OLD.exists():
        print(f"No {OLD} to diff against — skipping (first run after a fresh checkout).")
        record(date, "no-baseline",
               f"{OLD} absent. It is gitignored, so it never arrives in a "
               f"checkout; it is written only by Phase 8 of daily_scraper.py, "
               f"which runs last. A scrape that ends before Phase 8 leaves no "
               f"baseline and this day cannot be diffed.")
        return

    old = sku_map(json.loads(OLD.read_text(encoding="utf-8")))
    new = sku_map(json.loads(NEW.read_text(encoding="utf-8")))

    # Keying on (brand, sku) would otherwise break the guarantee this file's
    # header makes: that a baseline predating the operator tag still diffs.
    # Such a baseline keys as ("", sku) and matches nothing.
    #
    # It is only safe to repair when the new side is a single operator — then
    # the untagged baseline can only have come from that operator. With two or
    # more it is genuinely unknowable which brand an untagged SKU belonged to,
    # and guessing would attribute one operator's price change to another. So
    # it says so and diffs nothing, rather than inventing an answer.
    if old and all(k[0] == "" for k in old):
        brands = {k[0] for k in new}
        if len(brands) == 1:
            only = next(iter(brands))
            old = {(only, sku): v for (_, sku), v in old.items()}
            print(f"Note: {OLD} predates the operator tag. Its SKUs have been "
                  f"read as '{only}', the only operator in {NEW}.")
        else:
            print(f"REFUSING: {OLD} carries no operator tag and {NEW} holds "
                  f"{len(brands)} operators ({', '.join(sorted(brands))}). An "
                  f"untagged SKU cannot be assigned to one of them without "
                  f"guessing, and a wrong guess credits one operator's price "
                  f"change to another. Re-run the scraper so the baseline is "
                  f"tagged. Nothing was written.", file=sys.stderr)
            record(date, "refused-untagged-baseline",
                   f"{OLD} untagged while {NEW} holds {len(brands)} operators.",
                   len(old), len(new), 0, 0)
            return 1

    matched = len(set(old) & set(new))
    status, note = "ok", ""

    if new and not old:
        print(f"Warning: {OLD} yielded 0 SKUs (it may predate the 'sku' field "
              "in the scraper output — an older schema, not a real empty "
              "dataset). No changes can be logged against it. The log resumes "
              "once two same-schema snapshots are diffed back to back.")
        status = "empty-baseline"
        note = (f"{OLD} yielded 0 priced SKUs against {len(new):,} in {NEW} — "
                f"an older schema, not a real empty dataset.")
    elif old and new and not (set(old) & set(new)):
        print(f"Warning: 0 of {len(new):,} SKUs in {NEW} matched any of the "
              f"{len(old):,} in {OLD} — likely a schema change rather than a "
              "real 100% inventory turnover. No changes will be logged this "
              "run; the log resumes once two same-schema snapshots are diffed "
              "back to back.")
        status = "schema-mismatch"
        note = (f"0 of {len(new):,} SKUs matched any of {len(old):,} — likely "
                f"a schema change, not 100% inventory turnover.")

    if LOG.exists():
        with open(LOG, newline="", encoding="utf-8") as f:
            if any(row.startswith(date + ",") for row in f):
                print(f"{date} already logged in {LOG} — skipping")
                record(date, "already-logged",
                       "rate_changes.csv already holds rows for this date; "
                       "this invocation wrote nothing and is not a second day "
                       "of data.", len(old), len(new), matched, 0)
                return

    # Refuse rather than guess: an untagged row cannot be told apart later from
    # one whose tag failed to write, and this file is append-only in practice.
    untagged = sum(1 for v in new.values() if not v[5])
    if untagged:
        print(f"REFUSING: {untagged:,} of {len(new):,} SKUs in {NEW} carry no "
              f"'brand' — that snapshot predates the operator tag.\n"
              f"Re-run daily_scraper.py so the tag is stamped, then run this "
              f"again. Nothing was written.", file=sys.stderr)
        record(date, "refused-untagged",
               f"{untagged:,} of {len(new):,} SKUs carry no 'brand' tag; that "
               f"snapshot predates the operator tag.",
               len(old), len(new), matched, 0)
        return 1

    existing = log_header(LOG)
    if existing is not None and existing != HEADER:
        print(f"REFUSING: {LOG} has header {existing}, expected {HEADER}.\n"
              f"Run `python analysis/backfill_brand.py` to add the column to "
              f"the existing rows first. Nothing was written.", file=sys.stderr)
        record(date, "refused-header",
               f"{LOG} header is {existing}, expected {HEADER}.",
               len(old), len(new), matched, 0)
        return 1

    events = []
    for key, (sid, site, size, price, promo, brand) in new.items():
        sku = key[1]                      # key is (brand, sku); the row wants the sku
        prev = old.get(key)
        if not prev:
            continue
        old_price, old_promo = prev[3], prev[4]
        if price != old_price:
            events.append([date, sid, site, size, sku, "price", old_price, price, brand])
        if promo != old_promo:
            events.append([date, sid, site, size, sku, "promo", old_promo, promo, brand])

    new_file = not LOG.exists()
    LOG.parent.mkdir(exist_ok=True)
    with open(LOG, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new_file:
            w.writerow(HEADER)
        w.writerows(events)
    hikes = sum(1 for e in events if e[5] == "price" and e[7] > e[6])
    cuts = sum(1 for e in events if e[5] == "price" and e[7] < e[6])
    promos = sum(1 for e in events if e[5] == "promo")
    print(f"Logged {len(events)} events for {date} ({hikes} hikes, {cuts} cuts, {promos} promo switches) -> {LOG}")

    # A genuine zero is a real observation and gets said out loud, so that it
    # is never again indistinguishable from a day the diff could not run.
    if not events and status == "ok":
        status = "ok-zero-changes"
        note = (f"Diff ran normally over {matched:,} matched SKUs and found no "
                f"price or promo change. This is a measured zero, not a "
                f"missing day.")
    record(date, status, note, len(old), len(new), matched, len(events))
    print(f"  run recorded as '{status}' -> {RUNLOG}")

if __name__ == "__main__":
    # Exit non-zero on the refusal paths so a scheduled run fails loudly
    # instead of reporting success while having written nothing.
    sys.exit(main() or 0)
