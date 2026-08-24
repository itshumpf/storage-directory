"""
update_stock.py — Rewrite history/psa-stock.csv from Yahoo Finance's chart API.

Run before build_trends.py (from the repo root):
    python analysis/update_stock.py

Fetches PSA (Public Storage, NYSE) daily closing prices from
query1.finance.yahoo.com's chart endpoint (no API key required — the same
undocumented endpoint the yfinance library wraps) and rewrites
history/psa-stock.csv with one row per trading day: date,close.

The fetch window starts at the earliest date logged in history/YYYY-MM.csv
(the 10x10 price history), so the stock chart and the 10x10 chart on
trends.html cover the same span and are visually comparable. Trading days
only — weekends and market holidays produce no row, so the series will have
fewer points than the calendar-day 10x10 series.

The whole file is rewritten from scratch every run rather than appended to.
Unlike the scraped store data, PSA's full price history is available from
the API in a single call, so there is nothing to accumulate day-by-day, and
a rewrite means a missed run (or the 60-day workflow-inactivity window)
self-heals instead of leaving a permanent gap.

This script must never fail the daily job: any network or parsing error is
caught, a warning is printed, and the script exits 0, leaving the existing
CSV (if any) untouched. A hard failure here would abort every later step in
the daily workflow, including the git commit, over a single stock quote.
"""
import csv
import datetime
import re
import sys
from pathlib import Path

import requests

TICKER = "PSA"
CHART_URL = f"https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}"
OUT = Path("history/psa-stock.csv")
MONTH_CSV = re.compile(r"^\d{4}-\d{2}\.csv$")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def earliest_10x10_date(default_days_back=90):
    """Earliest date logged across history/YYYY-MM.csv, or a 90-day-back
    fallback if that history doesn't exist yet."""
    earliest = None
    for p in sorted(Path("history").glob("*.csv")):
        if not MONTH_CSV.match(p.name):
            continue
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                d = row.get("date")
                if d and (earliest is None or d < earliest):
                    earliest = d
    if earliest:
        return datetime.date.fromisoformat(earliest)
    return datetime.date.today() - datetime.timedelta(days=default_days_back)


def fetch_closes(start: datetime.date):
    period1 = int(datetime.datetime.combine(start, datetime.time.min, tzinfo=datetime.timezone.utc).timestamp())
    period2 = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
    r = requests.get(CHART_URL, headers=HEADERS, timeout=15,
                      params={"period1": period1, "period2": period2, "interval": "1d"})
    r.raise_for_status()
    result = r.json()["chart"]["result"][0]
    timestamps = result["timestamp"]
    closes = result["indicators"]["quote"][0]["close"]

    rows = {}
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        d = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).date().isoformat()
        rows[d] = round(close, 2)
    return sorted(rows.items())


def main():
    try:
        start = earliest_10x10_date()
        rows = fetch_closes(start)
        if not rows:
            print(f"No PSA closes returned for range starting {start} — leaving {OUT} untouched.")
            return
    except Exception as e:
        print(f"update_stock.py: fetch failed ({e}) — leaving {OUT} untouched.", file=sys.stderr)
        return

    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "close"])
        w.writerows(rows)
    print(f"Wrote {OUT} ({len(rows)} trading days, {rows[0][0]} to {rows[-1][0]})")


if __name__ == "__main__":
    main()
