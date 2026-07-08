"""
run_queries.py — Runs the core analysis query set against storage.db and
prints formatted results.

Usage (from the repo root, after load_storage.py):
    python analysis/run_queries.py          (runs all)
    python analysis/run_queries.py 4        (runs just query #4)
"""
import sqlite3
import sys

QUERIES = [
    ("1. Facility count by state", """
        SELECT state, COUNT(*) AS stores
        FROM stores
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY stores DESC
        LIMIT 15;
    """),

    ("2. Average 10x10 price by state", """
        SELECT s.state,
               COUNT(*)                 AS listings,
               ROUND(AVG(u.price), 2)   AS avg_price,
               MIN(u.price)             AS cheapest,
               MAX(u.price)             AS priciest
        FROM units u
        JOIN stores s ON s.store_id = u.store_id
        WHERE u.sqft = 100 AND u.price IS NOT NULL AND s.state IS NOT NULL
        GROUP BY s.state
        HAVING COUNT(*) >= 10
        ORDER BY avg_price DESC;
    """),

    ("3. Most expensive markets per square foot", """
        SELECT s.city, s.state,
               ROUND(AVG(u.price / u.sqft), 2) AS avg_price_per_sqft,
               COUNT(*) AS listings
        FROM units u
        JOIN stores s ON s.store_id = u.store_id
        WHERE u.price IS NOT NULL AND u.sqft > 0
        GROUP BY s.city, s.state
        HAVING COUNT(*) >= 20
        ORDER BY avg_price_per_sqft DESC
        LIMIT 15;
    """),

    ("4. Cheapest markets for a 10x10", """
        SELECT s.city, s.state,
               ROUND(AVG(u.price), 2) AS avg_10x10,
               COUNT(*) AS listings
        FROM units u
        JOIN stores s ON s.store_id = u.store_id
        WHERE u.sqft = 100 AND u.price IS NOT NULL
        GROUP BY s.city, s.state
        HAVING COUNT(*) >= 5
        ORDER BY avg_10x10 ASC
        LIMIT 15;
    """),

    ("5. Deepest promotional discounts", """
        SELECT s.city, s.state, u.size, u.price, u.promo_price,
               ROUND(100.0 * (u.price - u.promo_price) / u.price, 1) AS pct_off,
               u.promo_name
        FROM units u
        JOIN stores s ON s.store_id = u.store_id
        WHERE u.promo_price IS NOT NULL AND u.price > u.promo_price
        ORDER BY pct_off DESC
        LIMIT 20;
    """),

    ("6. Promotion frequency and average depth", """
        SELECT promo_name, COUNT(*) AS uses,
               ROUND(AVG(100.0 * (price - promo_price) / price), 1) AS avg_pct_off
        FROM units
        WHERE promo_name IS NOT NULL AND price > 0 AND promo_price IS NOT NULL
        GROUP BY promo_name
        ORDER BY uses DESC
        LIMIT 15;
    """),

    ("7. Unit size mix across the network", """
        SELECT size, COUNT(*) AS listings,
               ROUND(AVG(price), 2) AS avg_price
        FROM units
        WHERE size IS NOT NULL AND price IS NOT NULL
        GROUP BY size
        ORDER BY listings DESC
        LIMIT 12;
    """),

    ("8. In-city price spread for identical 10x10 units", """
        SELECT s.city, s.state,
               MAX(u.price) - MIN(u.price) AS spread,
               MIN(u.price) AS low, MAX(u.price) AS high,
               COUNT(*) AS listings
        FROM units u
        JOIN stores s ON s.store_id = u.store_id
        WHERE u.sqft = 100 AND u.price IS NOT NULL
        GROUP BY s.city, s.state
        HAVING COUNT(*) >= 8
        ORDER BY spread DESC
        LIMIT 15;
    """),

    ("9. Most saturated metros by facility count", """
        SELECT city, state, stores
        FROM (
            SELECT city, state, COUNT(*) AS stores
            FROM stores
            WHERE city IS NOT NULL
            GROUP BY city, state
        )
        ORDER BY stores DESC
        LIMIT 15;
    """),

    ("10. Outliers — units priced at 3x+ their state average", """
        WITH state_avg AS (
            SELECT s.state, u.sqft, AVG(u.price) AS avg_p
            FROM units u JOIN stores s ON s.store_id = u.store_id
            WHERE u.price IS NOT NULL AND u.sqft > 0
            GROUP BY s.state, u.sqft
        )
        SELECT st.city, st.state, u.size, u.price,
               ROUND(sa.avg_p, 2) AS state_avg,
               ROUND(u.price / sa.avg_p, 1) AS multiple
        FROM units u
        JOIN stores st ON st.store_id = u.store_id
        JOIN state_avg sa ON sa.state = st.state AND sa.sqft = u.sqft
        WHERE u.price > 3 * sa.avg_p
        ORDER BY multiple DESC
        LIMIT 15;
    """),
]

def run(db, title, sql):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    cur = db.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    if not rows:
        print("(no rows — field may be empty in your dataset)")
        return
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(str(v if v is not None else "").ljust(widths[i]) for i, v in enumerate(r)))

def main():
    db = sqlite3.connect("storage.db")
    pick = int(sys.argv[1]) if len(sys.argv) > 1 else None
    for i, (title, sql) in enumerate(QUERIES, 1):
        if pick is None or pick == i:
            run(db, title, sql)
    print("\nDone.")

if __name__ == "__main__":
    main()
