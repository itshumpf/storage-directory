"""
update_history.py — Append a snapshot's per-store aggregates to the history log.

Usage (from the repo root, after a scrape):
    python analysis/update_history.py                            # today's data
    python analysis/update_history.py snap.json 2026-04-29       # backfill a snapshot

Appends one row per store to history/YYYY-MM.csv:
    date, store_id, units_avail, cheapest_10x10, median_price, listings

Also appends state-by-size demand aggregates to history/sizes-YYYY-MM.csv:
    date, state, size, listings, units_avail, median_price
(~600 rows/day — the raw material for size-level demand curves over time.)

(The advertised min-max price "range" is not logged: it is mechanically
price ±20% for every unit, so it carries no information beyond the price.)

units_avail is the count of units advertised as rentable on the website.
This is advertised availability, not a physical vacancy count — the two can
differ, and the gap between them isn't directly observable from public data.

Idempotent: a date already present in the month file is skipped.

Stale-snapshot guard: if the rows produced are identical (ignoring the date
column) to the most recently logged date, the source JSON almost certainly was
not refreshed, so the snapshot is skipped and a warning is printed. The script
still exits 0 — a skipped day is a normal, recoverable condition and must not
fail the daily job. Pass --force to log the day anyway.
"""
import csv
import datetime
import json
import statistics
import sys
from pathlib import Path


def last_logged(out: Path):
    """(date, {row-without-date}) for the most recent date in a month file."""
    if not out.exists():
        return None, set()
    rows_by_date = {}
    with open(out, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 2 or row[0] == "date":
                continue
            rows_by_date.setdefault(row[0], set()).add(tuple(row[1:]))
    if not rows_by_date:
        return None, set()
    d = max(rows_by_date)
    return d, rows_by_date[d]


def main():
    argv = [a for a in sys.argv[1:] if a != "--force"]
    force = "--force" in sys.argv
    src = Path(argv[0]) if argv else Path("enriched_locations.json")
    date = argv[1] if len(argv) > 1 else datetime.date.today().isoformat()
    data = json.loads(src.read_text(encoding="utf-8"))

    hist = Path("history")
    hist.mkdir(exist_ok=True)
    out = hist / f"{date[:7]}.csv"
    if out.exists():
        with open(out, newline="", encoding="utf-8") as f:
            if any(row.startswith(date + ",") for row in f):
                print(f"{date} already logged in {out} — skipping")
                return

    seen, rows = set(), []
    for s in data:
        sid = str(s.get("store_id", ""))
        if not sid or sid in seen:
            continue
        seen.add(sid)
        units = [u for u in s.get("units", []) if u.get("available") and u.get("price")]
        tens = [u["price"] for u in units if u.get("size") == "10x10"]
        prices = [u["price"] for u in units]
        rows.append([
            date, sid,
            sum(int(u.get("count") or 0) for u in units),
            min(tens) if tens else "",
            round(statistics.median(prices), 2) if prices else "",
            len(units),
        ])

    prev_date, prev_rows = last_logged(out)
    this_rows = {tuple(str(c) for c in r[1:]) for r in rows}
    if prev_rows and this_rows == prev_rows and not force:
        print(
            f"Skipping {date}: the {len(rows):,} rows derived from {src} are "
            f"byte-identical to the snapshot already logged for {prev_date}, "
            f"which usually means {src} was not refreshed on this run. "
            f"Nothing was written for {date}. This is a warning, not an error — "
            f"the next run with fresh data will log normally. Re-run with "
            f"--force to record the day anyway."
        )
        return

    new_file = not out.exists()
    with open(out, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new_file:
            w.writerow(["date", "store_id", "units_avail", "cheapest_10x10",
                        "median_price", "listings"])
        w.writerows(rows)
    print(f"Logged {len(rows)} stores for {date} -> {out}")

    # state-by-size demand aggregates
    agg = {}
    seen2 = set()
    for s in data:
        sid = str(s.get("store_id", ""))
        if not sid or sid in seen2:
            continue
        seen2.add(sid)
        st = s.get("state") or ""
        for u in s.get("units", []):
            if not (u.get("available") and u.get("price") and u.get("size") and st):
                continue
            a = agg.setdefault((st, u["size"]), {"n": 0, "avail": 0, "prices": []})
            a["n"] += 1
            a["avail"] += int(u.get("count") or 0)
            a["prices"].append(u["price"])
    sout = hist / f"sizes-{date[:7]}.csv"
    new_file = not sout.exists()
    with open(sout, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new_file:
            w.writerow(["date", "state", "size", "listings", "units_avail", "median_price"])
        for (st, size), a in sorted(agg.items()):
            w.writerow([date, st, size, a["n"], a["avail"],
                        round(statistics.median(a["prices"]), 2)])
    print(f"Logged {len(agg)} state-size aggregates -> {sout}")

if __name__ == "__main__":
    main()
