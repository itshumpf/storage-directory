"""
load_storage.py — Load the FindStorage location dataset into SQLite.

Usage (from the repo root):
    python analysis/load_storage.py enriched_locations.json

Creates storage.db with two tables:
    stores(store_id, name, address, city, state, zip, lat, lng, phone)
    units(store_id, size, width, length, sqft, price, promo_name, promo2,
          available, unit_count, attrs, price_min, price_max)

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

def phone_text(v):
    """A phone column value, whatever shape the operator publishes.

    Public Storage gives a bare string. Extra Space gives an object:
    {"existingCustomerNumber": ..., "internetSearchNumber": ...}. sqlite3
    cannot bind a dict and raises InterfaceError, so the first Extra Space
    store would have taken down the whole load.

    The keys are kept alongside the numbers rather than one being picked as
    "the" phone number: they are different lines for different purposes, and
    choosing silently would put a sales-tracking number in a field everyone
    will read as the store's number.
    """
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, dict):
        return "; ".join(f"{k}: {x}" for k, x in v.items() if x) or None
    if isinstance(v, (list, tuple)):
        return "; ".join(str(x) for x in v if x) or None
    return str(v)


# Flat keys on a unit that are a price under their own name. Nested rate cards
# (Extra Space's "rates" object) are read separately, below.
FLAT_RATE_KEYS = ("price", "street_price", "price_min", "price_max")


def load_rates(db, brand, sid, sku, u):
    """Insert one row per named price this unit carries. Returns the count.

    A unit with no SKU is skipped rather than stored under an empty key: the
    rates table is keyed by SKU, so several such units would collapse onto one
    another and the survivor would be arbitrary. Losing an unkeyable rate is
    recoverable from the source file; silently keeping the wrong one is not.

    rate_type is the operator's own field name, untranslated. See the table
    comment for why.
    """
    if not sku:
        return 0
    rows = []
    for k in FLAT_RATE_KEYS:
        v = to_float(u.get(k))
        if v is not None:
            rows.append((brand, sid, sku, k, v))
    nested = u.get("rates")
    if isinstance(nested, dict):
        for k, v in nested.items():
            v = to_float(v)
            if v is not None:
                # Namespaced so a nested 'price' can never silently collide
                # with the flat 'price' column above; they are different
                # numbers from different places in the record.
                rows.append((brand, sid, sku, f"rates.{k}", v))
    db.executemany(
        "INSERT OR REPLACE INTO rates VALUES (?,?,?,?,?)", rows)
    return len(rows)


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
        DROP TABLE IF EXISTS rates;
        -- brand added and made part of the primary key 2026-09-01, ahead of a
        -- second and third operator. store_id alone is only unique inside one
        -- operator's numbering: Public Storage uses short numerics, Extra Space
        -- uses UUIDs, CubeSmart is unknown until it runs. As a bare PRIMARY KEY
        -- a collision across operators would silently drop a whole store and
        -- its units via INSERT OR REPLACE, with no error anywhere.
        CREATE TABLE stores (
            brand TEXT, store_id TEXT, name TEXT, address TEXT, city TEXT,
            state TEXT, zip TEXT, lat REAL, lng REAL, phone TEXT,
            rating REAL, reviews INTEGER,
            PRIMARY KEY (brand, store_id)
        );
        CREATE TABLE units (
            brand TEXT, store_id TEXT, sku TEXT, size TEXT, width REAL,
            length REAL, sqft REAL, price REAL, promo_name TEXT, promo2 TEXT,
            available INTEGER, unit_count INTEGER,
            attrs TEXT, price_min REAL, price_max REAL
        );

        -- Added 2026-09-01. Operators publish different numbers of prices per
        -- unit and a fixed set of columns can only hold the ones they share.
        -- Public Storage gives one price plus an advertised min-max envelope.
        -- Extra Space gives a whole ladder -- web, street, walkIn, nsc and
        -- three tiers -- which is the actual rate card its revenue system
        -- prices on, and all of it was being dropped on the floor.
        --
        -- rate_type is THE OPERATOR'S OWN FIELD NAME, copied verbatim and not
        -- mapped to any common vocabulary. Public Storage does not call its
        -- price "web"; calling it that here would be an inference printed as a
        -- measurement, and it would be indistinguishable from a real 'web'
        -- rate a year from now. Any cross-operator comparison therefore has to
        -- state its own mapping out loud, which is the correct place for that
        -- judgement to live -- in the query, visible, not buried in the loader.
        CREATE TABLE rates (
            brand TEXT, store_id TEXT, sku TEXT, rate_type TEXT, price REAL,
            PRIMARY KEY (brand, store_id, sku, rate_type)
        );

        CREATE INDEX idx_units_store ON units(brand, store_id);
        CREATE INDEX idx_units_size  ON units(size);
        CREATE INDEX idx_units_sku   ON units(brand, sku);
        CREATE INDEX idx_rates_unit  ON rates(brand, store_id, sku);
        CREATE INDEX idx_rates_type  ON rates(rate_type);
        CREATE INDEX idx_stores_state ON stores(state);
        CREATE INDEX idx_stores_brand ON stores(brand);
    """)

    n_units = 0
    n_rates = 0
    seen = set()
    for s in data:
        brand = first(s, "brand") or ""
        sid = str(first(s, "store_id", "id", "storeId", "site_id") or "")
        if (brand, sid) in seen:
            continue  # duplicate record — keep the first, avoid double-counting units
        seen.add((brand, sid))
        db.execute(
            "INSERT OR REPLACE INTO stores VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                brand,
                sid,
                first(s, "name", "store_name", "title"),
                # 'line1' is Extra Space's street field; it has no 'address'.
                first(s, "address", "street", "address1", "line1"),
                first(s, "city"),
                first(s, "state", "state_code", "region"),
                str(first(s, "zip", "zipcode", "postal_code") or "") or None,
                to_float(first(s, "lat", "latitude")),
                to_float(first(s, "lng", "lon", "longitude")),
                phone_text(first(s, "phone", "phone_number")),
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
            sku = first(u, "sku")
            db.execute(
                "INSERT INTO units VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    brand, sid, sku, size, w, l, sqft,
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
            n_rates += load_rates(db, brand, sid, sku, u)

    db.commit()
    stores = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    priced = db.execute("SELECT COUNT(*) FROM units WHERE price IS NOT NULL").fetchone()[0]
    print(f"Done: {stores} stores, {n_units} unit listings ({priced} with prices), "
          f"{n_rates} named rates -> storage.db")

    # Per operator, so a source that stops publishing its rate card shows up as
    # a number that moved rather than as nothing at all.
    # Counted in separate subqueries, not by joining units to rates. Joining
    # both to stores multiplies one by the other -- 1,063 units and 10,002
    # rates came out as 462,429 of each, a number with no meaning that looked
    # like a big successful load.
    by_brand = db.execute("""
        SELECT b.brand,
               (SELECT COUNT(*) FROM stores WHERE brand = b.brand),
               (SELECT COUNT(*) FROM units  WHERE brand = b.brand),
               (SELECT COUNT(*) FROM rates  WHERE brand = b.brand)
        FROM (SELECT DISTINCT brand FROM stores) b
        ORDER BY b.brand
    """).fetchall()
    if len(by_brand) > 1 or (by_brand and by_brand[0][0]):
        print("\n  operator          stores    units    rates")
        for b, ns, nu, nr in by_brand:
            print(f"  {b or '(untagged)':<16} {ns:>6} {nu:>8} {nr:>8}")

    types = db.execute("SELECT brand, rate_type, COUNT(*) FROM rates "
                       "GROUP BY brand, rate_type ORDER BY brand, rate_type").fetchall()
    if types:
        print("\n  rate types present (operator's own field names):")
        for b, t, n in types:
            print(f"    {b or '(untagged)':<16} {t:<16} {n:>8}")

    print("\nNext: python analysis/run_queries.py")

if __name__ == "__main__":
    main()
