"""
update_history.py — Append a snapshot's per-store aggregates to the history log.

Usage (from the repo root, after a scrape):
    python analysis/update_history.py                            # today's data
    python analysis/update_history.py snap.json 2026-04-29       # backfill a snapshot

Appends one row per store to history/YYYY-MM.csv:
    date, store_id, units_avail, cheapest_10x10, median_price, listings

(The advertised min-max price "range" is not logged: it is mechanically
price ±20% for every unit, so it carries no information beyond the price.)

units_avail is the count of units advertised as rentable on the website —
revenue management typically holds back part of the physically vacant
inventory, so this tracks marketed availability, not occupancy.

Idempotent: a date already present in the month file is skipped.
"""
import csv
import datetime
import json
import statistics
import sys
from pathlib import Path

def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("enriched_locations.json")
    date = sys.argv[2] if len(sys.argv) > 2 else datetime.date.today().isoformat()
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

    new_file = not out.exists()
    with open(out, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["date", "store_id", "units_avail", "cheapest_10x10",
                        "median_price", "listings"])
        w.writerows(rows)
    print(f"Logged {len(rows)} stores for {date} -> {out}")

if __name__ == "__main__":
    main()
