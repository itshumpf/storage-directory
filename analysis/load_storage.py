"""
load_storage.py — Load the FindStorage location dataset into SQLite.

Usage (from the repo root):
    python analysis/load_storage.py enriched_locations.json

Creates storage.db with the current snapshot:
    stores(store_id, name, address, city, state, zip, lat, lng, phone)
    units(store_id, size, width, length, sqft, price, promo_name, promo2,
          available, unit_count, attrs, price_min, price_max)

plus the accumulated time series (loaded from history/*.csv), so temporal
questions are plain SQL instead of CSV munging:
    store_daily(date, store_id, units_avail, cheapest_10x10, median_price, listings)
    state_size_daily(date, state, size, listings, units_avail, median_price)
    rate_events(date, store_id, site_number, size, sku, field, old, new)
    pipeline(store_id, url, city, state, first_seen, opened)

attrs is the unit's physical description (climate control, floor, access);
price_min/price_max is the advertised range the revenue-management system
prices within — the current price is one point inside it.

The loader is defensive: it discovers whatever keys the JSON actually has,
prints them, and maps the common ones. Missing fields are stored as NULL.
"""
import json
import re
import sqlite3
import sys
from pathlib import Path

# ---------------------------------------------------------------- helpers
def first(d, *keys):
    """Return the first present, non-empty value among candidate keys."""
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None

def to_float(v):
    if v is None:
        return None
    try:
        return float(re.sub(r"[^0-9.\-]", "", str(v))) if str(v).strip() else None
    except ValueError:
        return None

def parse_size(size_str):
    """'10x10' -> (10.0, 10.0, 100.0). Returns (w, l, sqft) or Nones."""
    if not size_str:
        return None, None, None
    m = re.search(r"(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)", str(size_str))
    if not m:
        return None, None, None
    w, l = float(m.group(1)), float(m.group(2))
    return w, l, w * l

# ---------------------------------------------------------------- main
def main():
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "enriched_locations.json")
    if not src.exists():
        sys.exit(f"Can't find {src} — pass the dataset path: "
                 f"python analysis/load_storage.py enriched_locations.json")

    data = json.loads(src.read_text(encoding="utf-8"))
    if isinstance(data, dict):  # in case top level is {"stores": [...]} etc.
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                data = v
                break
    print(f"Loaded {len(data)} store records.")
    print("Sample keys on first record:", sorted(data[0].keys()))

    db = sqlite3.connect("storage.db")
    db.executescript("""
        DROP TABLE IF EXISTS stores;
        DROP TABLE IF EXISTS units;
        CREATE TABLE stores (
            store_id TEXT PRIMARY KEY, name TEXT, address TEXT, city TEXT,
            state TEXT, zip TEXT, lat REAL, lng REAL, phone TEXT,
            rating REAL, reviews INTEGER
        );
        CREATE TABLE units (
            store_id TEXT, size TEXT, width REAL, length REAL, sqft REAL,
            price REAL, promo_name TEXT, promo2 TEXT,
            available INTEGER, unit_count INTEGER,
            attrs TEXT, price_min REAL, price_max REAL
        );
        CREATE INDEX idx_units_store ON units(store_id);
        CREATE INDEX idx_units_size  ON units(size);
        CREATE INDEX idx_stores_state ON stores(state);
    """)

    n_units = 0
    seen = set()
    for s in data:
        sid = str(first(s, "store_id", "id", "storeId", "site_id") or "")
        if sid in seen:
            continue  # duplicate record — keep the first, avoid double-counting units
        seen.add(sid)
        db.execute(
            "INSERT OR REPLACE INTO stores VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                sid,
                first(s, "name", "store_name", "title"),
                first(s, "address", "street", "address1"),
                first(s, "city"),
                first(s, "state", "state_code", "region"),
                str(first(s, "zip", "zipcode", "postal_code") or "") or None,
                to_float(first(s, "lat", "latitude")),
                to_float(first(s, "lng", "lon", "longitude")),
                first(s, "phone", "phone_number"),
                to_float(first(s, "rating")),
                first(s, "reviews"),
            ),
        )
        # unit listings might live under several names
        listings = first(s, "units", "unit_prices", "listings", "prices", "pricing") or []
        if isinstance(listings, dict):
            listings = list(listings.values())
        for u in listings:
            if not isinstance(u, dict):
                continue
            size = first(u, "size", "unit_size", "dimensions")
            w, l, sqft = parse_size(size)
            count = first(u, "unit_count", "count")
            db.execute(
                "INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    sid, size, w, l, sqft,
                    to_float(first(u, "price", "rate", "monthly_rate", "web_rate")),
                    first(u, "promo_name", "promo", "special", "discount"),
                    first(u, "promo2"),
                    1 if u.get("available") else 0,
                    int(count) if isinstance(count, (int, float)) else None,
                    first(u, "attrs"),
                    to_float(first(u, "price_min")),
                    to_float(first(u, "price_max")),
                ),
            )
            n_units += 1

    db.commit()

    # ---- time-series tables from history/ ----
    import csv
    import re as _re
    db.executescript("""
        DROP TABLE IF EXISTS store_daily;
        DROP TABLE IF EXISTS state_size_daily;
        DROP TABLE IF EXISTS rate_events;
        DROP TABLE IF EXISTS pipeline;
        CREATE TABLE store_daily (date TEXT, store_id TEXT, units_avail INTEGER,
            cheapest_10x10 REAL, median_price REAL, listings INTEGER);
        CREATE TABLE state_size_daily (date TEXT, state TEXT, size TEXT,
            listings INTEGER, units_avail INTEGER, median_price REAL);
        CREATE TABLE rate_events (date TEXT, store_id TEXT, site_number TEXT,
            size TEXT, sku TEXT, field TEXT, old TEXT, new TEXT);
        CREATE TABLE pipeline (store_id TEXT, url TEXT, city TEXT, state TEXT,
            first_seen TEXT, opened TEXT);
        CREATE INDEX idx_sd_date ON store_daily(date);
        CREATE INDEX idx_sd_store ON store_daily(store_id);
        CREATE INDEX idx_re_date ON rate_events(date);
    """)
    hist = Path("history")
    n_hist = {"store_daily": 0, "state_size_daily": 0, "rate_events": 0, "pipeline": 0}
    if hist.is_dir():
        def rows(p):
            with open(p, newline="", encoding="utf-8") as f:
                yield from csv.DictReader(f)
        for p in sorted(hist.glob("*.csv")):
            if _re.match(r"^\d{4}-\d{2}\.csv$", p.name):
                for r in rows(p):
                    db.execute("INSERT INTO store_daily VALUES (?,?,?,?,?,?)",
                               (r["date"], r["store_id"], int(r["units_avail"] or 0),
                                float(r["cheapest_10x10"]) if r["cheapest_10x10"] else None,
                                float(r["median_price"]) if r["median_price"] else None,
                                int(r["listings"] or 0)))
                    n_hist["store_daily"] += 1
            elif p.name.startswith("sizes-"):
                for r in rows(p):
                    db.execute("INSERT INTO state_size_daily VALUES (?,?,?,?,?,?)",
                               (r["date"], r["state"], r["size"], int(r["listings"] or 0),
                                int(r["units_avail"] or 0),
                                float(r["median_price"]) if r["median_price"] else None))
                    n_hist["state_size_daily"] += 1
            elif p.name == "rate_changes.csv":
                for r in rows(p):
                    db.execute("INSERT INTO rate_events VALUES (?,?,?,?,?,?,?,?)",
                               (r["date"], r["store_id"], r["site_number"], r["size"],
                                r["sku"], r["field"], r["old"], r["new"]))
                    n_hist["rate_events"] += 1
            elif p.name == "pipeline.csv":
                for r in rows(p):
                    db.execute("INSERT INTO pipeline VALUES (?,?,?,?,?,?)",
                               (r["store_id"], r["url"], r["city"], r["state"],
                                r["first_seen"], r["opened"]))
                    n_hist["pipeline"] += 1
        db.commit()

    stores = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    priced = db.execute("SELECT COUNT(*) FROM units WHERE price IS NOT NULL").fetchone()[0]
    print(f"Done: {stores} stores, {n_units} unit listings ({priced} with prices) -> storage.db")
    print("History: " + ", ".join(f"{v:,} {k}" for k, v in n_hist.items()))
    print("\nNext: python analysis/run_queries.py")

if __name__ == "__main__":
    main()
