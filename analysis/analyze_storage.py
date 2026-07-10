"""
analyze_storage.py — Full analysis pass over the FindStorage dataset.

Run after load_storage.py has created storage.db (from the repo root):
    python analysis/analyze_storage.py

Does three things:
  1. Data quality audit (console) — stores with no units, missing prices,
     missing coordinates/state.
  2. Market analysis (console) — state pricing, price per sqft, local
     variance, live inventory, promotions and true move-in cost, geography,
     vehicle storage, size mix, saturation.
  3. Generates insights.html — a self-contained report page served alongside
     the directory. Pure stdlib, no dependencies.
"""
import sqlite3, html, datetime, math, sys

DB = "storage.db"
OUT = "insights.html"

# 2024 state population estimates, millions (US Census Bureau)
STATE_POP = {
    "AL": 5.16, "AK": 0.74, "AZ": 7.58, "AR": 3.09, "CA": 39.43, "CO": 5.96,
    "CT": 3.68, "DE": 1.05, "DC": 0.70, "FL": 23.00, "GA": 11.18, "HI": 1.45,
    "ID": 2.00, "IL": 12.71, "IN": 6.92, "IA": 3.24, "KS": 2.97, "KY": 4.59,
    "LA": 4.60, "ME": 1.41, "MD": 6.26, "MA": 7.14, "MI": 10.14, "MN": 5.79,
    "MS": 2.94, "MO": 6.22, "MT": 1.14, "NE": 2.00, "NV": 3.27, "NH": 1.41,
    "NJ": 9.50, "NM": 2.13, "NY": 19.87, "NC": 11.05, "ND": 0.80, "OH": 11.88,
    "OK": 4.09, "OR": 4.27, "PA": 13.08, "RI": 1.11, "SC": 5.46, "SD": 0.93,
    "TN": 7.23, "TX": 31.29, "UT": 3.50, "VT": 0.65, "VA": 8.81, "WA": 7.96,
    "WV": 1.77, "WI": 5.96, "WY": 0.59,
}

# Effective monthly cost over the first 3 months, given the advertised promos
EFFECTIVE_3MO = """
    CASE
        WHEN u.promo_name = '$1 first month rent' THEN (1.0 + 2*u.price) / 3
        WHEN u.promo_name = 'First month 50% off' THEN 2.5 * u.price / 3
        WHEN u.promo_name = '2nd Month Free'      THEN 2.0 * u.price / 3
        ELSE u.price
    END
"""

def q(db, sql):
    cur = db.execute(sql)
    return [d[0] for d in cur.description], cur.fetchall()

def console(title, cols, rows, limit=15):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)
    if not rows:
        print("(no data)"); return
    widths = [max(len(str(c)), *(len(str(r[i])) for r in rows[:limit])) for i, c in enumerate(cols)]
    print("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols)))
    for r in rows[:limit]:
        print("  ".join(str(v if v is not None else "").ljust(widths[i]) for i, v in enumerate(r)))

def table_html(cols, rows, limit=15):
    if not rows:
        return "<p class='empty'>No data available for this section.</p>"
    h = "<table><thead><tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in cols) + "</tr></thead><tbody>"
    for r in rows[:limit]:
        h += "<tr>" + "".join(f"<td>{html.escape(str(v if v is not None else '—'))}</td>" for v in r) + "</tr>"
    return h + "</tbody></table>"

def bars_html(rows, label_i, value_i, prefix="$", limit=10):
    """CSS bar chart from rows: label col index, numeric col index."""
    rows = [r for r in rows if r[value_i] is not None][:limit]
    if not rows:
        return ""
    mx = max(float(r[value_i]) for r in rows) or 1
    out = "<div class='bars'>"
    for r in rows:
        pct = 100.0 * float(r[value_i]) / mx
        val = float(r[value_i])
        shown = f"{prefix}{val:,.2f}" if val < 10 and prefix == "$" else f"{prefix}{val:,.0f}"
        out += (f"<div class='bar-row'><span class='bar-label'>{html.escape(str(r[label_i]))}</span>"
                f"<span class='bar-track'><span class='bar-fill' style='width:{pct:.1f}%'></span></span>"
                f"<span class='bar-val'>{shown}</span></div>")
    return out + "</div>"

def haversine(lat1, lng1, lat2, lng2):
    """Distance in miles."""
    r = 3958.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lng2 - lng1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2 * r * math.asin(math.sqrt(a))

def median(vals):
    vals = sorted(vals)
    n = len(vals)
    if not n:
        return None
    return vals[n//2] if n % 2 else (vals[n//2 - 1] + vals[n//2]) / 2

def main():
    try:
        db = sqlite3.connect(DB)
        total = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    except Exception as e:
        sys.exit(f"Couldn't open {DB} — run analysis/load_storage.py first. ({e})")
    if not total:
        sys.exit("storage.db has 0 stores — check load_storage.py output.")

    S = {}  # sections: key -> (title, note, cols, rows, bars_html_or_empty)

    # ============ KPIs ============
    kpi = {}
    kpi["stores"] = total
    kpi["units"] = db.execute("SELECT COUNT(*) FROM units").fetchone()[0]
    kpi["inventory"] = db.execute("SELECT SUM(unit_count) FROM units").fetchone()[0] or 0
    kpi["no_units"] = db.execute(
        "SELECT COUNT(*) FROM stores s WHERE NOT EXISTS (SELECT 1 FROM units u WHERE u.store_id=s.store_id)"
    ).fetchone()[0]
    kpi["no_coords"] = db.execute("SELECT COUNT(*) FROM stores WHERE lat IS NULL OR lng IS NULL").fetchone()[0]
    kpi["no_state"] = db.execute("SELECT COUNT(*) FROM stores WHERE state IS NULL OR state=''").fetchone()[0]
    med_rows = db.execute("SELECT price FROM units WHERE sqft=100 AND price IS NOT NULL").fetchall()
    kpi["median_10x10"] = median([r[0] for r in med_rows])

    # ============ 1. LIVE INVENTORY & SCARCITY ============
    cols, rows = q(db, """
        SELECT s.city, s.state, SUM(u.unit_count) units_available, COUNT(DISTINCT s.store_id) stores
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE s.city IS NOT NULL AND u.unit_count IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(DISTINCT s.store_id) >= 3
        ORDER BY units_available DESC LIMIT 15""")
    S["inv_deep"] = ("Deepest inventory — where storage is easiest to get",
        f"The pricing feed reports how many units of each size are actually rentable right now — "
        f"{kpi['inventory']:,} units nationwide at last scrape. These metros have the most open doors.",
        cols, rows, bars_html(rows, 0, 2, prefix=""))

    cols, rows = q(db, """
        SELECT s.city, s.state, SUM(u.unit_count) units_available,
               COUNT(DISTINCT s.store_id) stores,
               ROUND(1.0*SUM(u.unit_count)/COUNT(DISTINCT s.store_id),1) units_per_store
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE s.city IS NOT NULL AND u.unit_count IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(DISTINCT s.store_id) >= 5
        ORDER BY units_per_store ASC LIMIT 15""")
    S["inv_tight"] = ("Tightest markets — where storage is scarce",
        "Fewest available units per store among cities with 5+ facilities. Scarcity like this is "
        "usually invisible to renters until they start calling around.",
        cols, rows, "")

    under = {t: db.execute("""
        SELECT COUNT(*) FROM (
            SELECT s.store_id, COALESCE(SUM(u.unit_count),0) avail
            FROM stores s LEFT JOIN units u ON u.store_id=s.store_id
            GROUP BY s.store_id) WHERE avail < ?""", (t,)).fetchone()[0]
        for t in (5, 10, 20)}
    cols, rows = q(db, """
        SELECT s.address, s.city, s.state, COALESCE(SUM(u.unit_count),0) units_left
        FROM stores s LEFT JOIN units u ON u.store_id=s.store_id
        GROUP BY s.store_id HAVING units_left > 0
        ORDER BY units_left ASC, s.state LIMIT 15""")
    S["inv_low"] = ("Nearly full — stores running out of space",
        f"Store-level scarcity: {under[5]} facilities have fewer than 5 rentable units left, "
        f"{under[10]} fewer than 10, and {under[20]} ({100*under[20]/total:.0f}% of the network) fewer "
        "than 20. These are the last-unit stores — the directory flags them with an 'Almost Full' badge.",
        cols, rows, "")

    cols, rows = q(db, """
        SELECT s.name, s.address, s.city, s.state
        FROM stores s WHERE NOT EXISTS (SELECT 1 FROM units u WHERE u.store_id=s.store_id)
        ORDER BY s.state, s.city""")
    S["soldout"] = ("Completely sold out",
        f"{kpi['no_units']} facilities currently advertise zero rentable units. Public Storage removes "
        "these from its own sitemaps, but they're still operating stores — this directory keeps them "
        "listed with their site numbers.",
        cols, rows, "")

    # ============ 2. STATE PRICING (10x10) ============
    cols, rows = q(db, """
        SELECT s.state, COUNT(*) listings, ROUND(AVG(u.price),0) avg_10x10,
               MIN(u.price) cheapest, MAX(u.price) priciest
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.sqft=100 AND u.price IS NOT NULL AND s.state IS NOT NULL
        GROUP BY s.state HAVING COUNT(*)>=10 ORDER BY avg_10x10 DESC""")
    S["state"] = ("What a 10x10 costs, by state",
        "Average advertised monthly rate for a standard 10x10 unit. Coastal and dense metros predictably "
        "top the chart — the interesting part is the spread between neighbors.",
        cols, rows, bars_html(rows, 0, 2))

    # ============ 3. LOCAL PRICE VARIANCE ============
    cols, rows = q(db, """
        SELECT s.city, s.state, MIN(u.price) low, MAX(u.price) high,
               MAX(u.price)-MIN(u.price) spread, COUNT(*) listings
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.sqft=100 AND u.price IS NOT NULL AND s.city IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(*)>=8
        ORDER BY spread DESC LIMIT 15""")
    S["variance"] = ("Local price variance — same unit, same city, wildly different price",
        "The gap between the cheapest and priciest 10x10 within a single city. In the top cities, "
        "picking the right facility saves renters serious money for an identical unit — and it shows "
        "how loosely rates track location within a metro.",
        cols, rows, bars_html(rows, 0, 4))

    # ============ 4. PROMO DECODER & TRUE MOVE-IN COST ============
    cols, rows = q(db, f"""
        SELECT COALESCE(u.promo_name,'(no promotion)') promotion,
               COUNT(*) listings,
               ROUND(100.0*COUNT(*)/(SELECT COUNT(*) FROM units),1) pct_of_all,
               ROUND(AVG(u.price),0) avg_advertised,
               ROUND(AVG({EFFECTIVE_3MO}),0) avg_effective_3mo
        FROM units u WHERE u.price IS NOT NULL
        GROUP BY u.promo_name ORDER BY listings DESC""")
    S["promo"] = ("The promo decoder — what the discounts are really worth",
        "Public Storage runs exactly three promotions network-wide. Averaged over the first three "
        "months, the famous '$1 first month' and '2nd Month Free' are near-identical (~33% off), while "
        "'First month 50% off' is barely half the discount it sounds like (~17%). Notice the tiering: "
        "the cheapest units get the '$1' offer, the priciest get '2nd Month Free' — the promo itself "
        "signals the unit's price band. The effective column is true average monthly cost for months 1-3.",
        cols, rows, "")

    cols, rows = q(db, f"""
        SELECT s.city, s.state, u.size, u.price advertised, u.promo_name,
               ROUND({EFFECTIVE_3MO},0) effective_3mo
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.sqft=100 AND u.price IS NOT NULL AND u.promo_name IS NOT NULL
        ORDER BY {EFFECTIVE_3MO} ASC LIMIT 15""")
    S["deals"] = ("The best real move-in deals in America (10x10)",
        "Ranked by effective monthly cost over the first three months, promo included.",
        cols, rows, "")

    # ============ 4b. THE PRICING MODEL ============
    # Feature premiums: same store, same footprint, different attributes.
    # The drive-up comparison excludes climate-controlled units on both sides,
    # otherwise the climate premium contaminates it.
    prem_rows = []
    for label, yes_cond, no_cond in [
        ("Climate control",
         "attrs LIKE '%Climate%'", "attrs NOT LIKE '%Climate%'"),
        ("Ground/1st floor (vs upstairs)",
         "attrs LIKE '%1st Floor%'", "attrs LIKE '%Upstairs%'"),
        ("Drive-up access (vs inside, non-climate)",
         "attrs LIKE '%Drive%' AND attrs NOT LIKE '%Climate%'",
         "attrs LIKE '%Inside%' AND attrs NOT LIKE '%Climate%'"),
    ]:
        r = db.execute(f"""
            WITH g AS (
                SELECT store_id, size,
                       AVG(CASE WHEN {yes_cond} THEN price END) AS yes,
                       AVG(CASE WHEN {no_cond} THEN price END) AS no
                FROM units WHERE price IS NOT NULL AND attrs IS NOT NULL
                GROUP BY store_id, size)
            SELECT COUNT(*), ROUND(AVG(100.0*(yes-no)/no),1) FROM g
            WHERE yes IS NOT NULL AND no IS NOT NULL AND no > 0""").fetchone()
        if r[0]:
            prem_rows.append((label, r[0], f"{r[1]:+.1f}%"))
    S["premiums"] = ("What features actually cost — paired premiums",
        "Each comparison pairs units of the SAME size at the SAME store that differ in one attribute, "
        "so location and demand cancel out. This is the attribute pricing inside Public Storage's model.",
        ["feature", "store-size pairs", "avg premium"], prem_rows, "")

    # The advertised min-max "range": we tested whether it is a real pricing
    # envelope. It is not — min and max are mechanically price*0.8 and
    # price*1.2 for every unit, so the range moves with the price, not the
    # other way around. Publish the falsification; it is the honest finding.
    RANGED = "u.price IS NOT NULL AND u.price_max > u.price_min AND u.price >= u.price_min"
    env = db.execute(f"""
        SELECT COUNT(*),
               SUM(CASE WHEN ABS(u.price - (u.price_min+u.price_max)/2.0) <= 1 THEN 1 ELSE 0 END)
        FROM units u WHERE {RANGED}""").fetchone()
    S["envelope"] = ("The ±20% illusion — what the advertised price range really is",
        f"Every unit page shows a min-max price range that looks like a pricing band. We tested whether "
        f"street rates move within it. They don't — across {env[0]:,} listings, {100.0*env[1]/env[0]:.1f}% "
        "sit exactly at the midpoint, because the displayed range is mechanically today's price ±20% "
        "(rounded). It's a disclaimer construct, not a revenue-management envelope: when the range "
        "moves, that IS the price moving. Real rate movement is tracked on the daily trends page.",
        [], [], "")

    cols, rows = q(db, """
        SELECT CASE WHEN u.unit_count <= 5 THEN '1-5 left'
                    WHEN u.unit_count <= 20 THEN '6-20 left'
                    WHEN u.unit_count <= 50 THEN '21-50 left'
                    ELSE '51+ left' END AS units_remaining,
               COUNT(*) listings,
               ROUND(AVG(u.price),0) avg_10x10_price
        FROM units u
        WHERE u.sqft = 100 AND u.price IS NOT NULL AND u.unit_count IS NOT NULL
        GROUP BY units_remaining ORDER BY MIN(u.unit_count)""")
    S["scarcity_price"] = ("The scarcity dial — fewer units left, higher price",
        "Same unit size (10x10 only), bucketed by how many are left at that store. The gradient is "
        "steep and monotonic. Causality runs both ways — high prices slow sell-through, and low "
        "availability pushes prices up — but the correlation is exactly what a demand-based "
        "revenue-management system produces.",
        cols, rows, bars_html(rows, 0, 2))

    # ============ 4c. RATINGS vs PRICE ============
    rated = db.execute("SELECT COUNT(*), ROUND(AVG(rating),2), SUM(reviews) FROM stores WHERE rating IS NOT NULL").fetchone()
    if rated[0] and rated[0] > 100:
        cols, rows = q(db, """
            SELECT CASE WHEN s.rating < 4.0 THEN 'under 4.0'
                        WHEN s.rating < 4.7 THEN '4.0 - 4.6'
                        WHEN s.rating < 4.9 THEN '4.7 - 4.8'
                        ELSE '4.9 - 5.0' END AS rating_band,
                   COUNT(DISTINCT s.store_id) stores,
                   ROUND(AVG(u.price),0) avg_10x10,
                   ROUND(AVG(s.reviews),0) avg_reviews
            FROM stores s JOIN units u ON u.store_id=s.store_id
            WHERE s.rating IS NOT NULL AND u.sqft=100 AND u.price IS NOT NULL
            GROUP BY rating_band ORDER BY MIN(s.rating)""")
        S["ratings"] = ("Do better-rated stores charge more?",
            f"Google-style review data from each store page: {rated[0]:,} rated stores, "
            f"{rated[2]:,.0f} total reviews, {rated[1]} average. The bands compare each rating tier's "
            "average 10x10 street rate.",
            cols, rows, "")

        cols, rows = q(db, """
            SELECT s.address, s.city, s.state, s.rating, s.reviews
            FROM stores s WHERE s.rating IS NOT NULL AND s.reviews >= 25
            ORDER BY s.rating ASC LIMIT 10""")
        S["worst_rated"] = ("Lowest-rated facilities (25+ reviews)",
            "The bottom of the network by customer rating.", cols, rows, "")

    # ============ 4d. AFFORDABILITY (IRS SOI income join) ============
    try:
        import csv as _csv
        with open("data/zip_income.csv", newline="", encoding="utf-8") as f:
            zinc = {r["zip"]: float(r["avg_income_per_return"]) for r in _csv.DictReader(f)}
    except OSError:
        zinc = {}
    if zinc:
        _, srows = q(db, """
            SELECT s.city, s.state, s.zip, AVG(u.price)
            FROM stores s JOIN units u ON u.store_id=s.store_id
            WHERE u.sqft=100 AND u.price IS NOT NULL AND s.zip IS NOT NULL
            GROUP BY s.store_id""")
        agg = {}
        for city, st, z, p in srows:
            inc = zinc.get(str(z)[:5])
            if inc and city:
                a = agg.setdefault((city, st), [0, 0.0, 0.0])
                a[0] += 1; a[1] += p; a[2] += inc
        burden = []
        for (city, st), (n, psum, isum) in agg.items():
            if n >= 5:
                price, inc = psum / n, isum / n
                burden.append((city, st, n, round(price), f"${inc:,.0f}",
                               round(100 * price / (inc / 12), 1)))
        burden.sort(key=lambda r: -r[5])
        cols = ["city", "state", "stores", "avg_10x10", "avg_income_per_return", "pct_of_monthly_income"]
        nat = [b[5] for b in burden]
        S["afford"] = ("The storage burden — price vs local income",
            f"Average 10x10 rate as a share of average monthly income in the store's zip codes "
            f"(income: IRS SOI 2022, AGI per tax return). Across {len(burden)} cities the median burden "
            f"is {median(nat):.1f}% of a month's income — but the spread is enormous, and the most "
            "burdened markets are rarely the richest ones.",
            cols, burden[:15], "")

    # ============ 5. BULK DISCOUNT CURVE ============
    cols, rows = q(db, """
        SELECT size, ROUND(AVG(price/sqft),2) avg_per_sqft, COUNT(*) listings
        FROM units WHERE sqft > 0 AND price IS NOT NULL
          AND size IN ('5x5','5x10','5x15','10x10','10x15','10x20','10x25','10x30')
        GROUP BY size ORDER BY AVG(price/sqft) DESC""")
    S["curve"] = ("The bulk discount curve — small units cost multiples more per square foot",
        "Price per square foot falls steeply as units get bigger. A 5x5 renter pays roughly "
        "double the rate per square foot of a 10x30 renter for the same building.",
        cols, rows, bars_html(rows, 0, 1))

    # ============ 6. PRICE PER SQFT MARKETS ============
    cols, rows = q(db, """
        SELECT s.city, s.state, ROUND(AVG(u.price/u.sqft),2) avg_per_sqft, COUNT(*) listings
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.price IS NOT NULL AND u.sqft>0 AND s.city IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(*)>=20
        ORDER BY avg_per_sqft DESC LIMIT 15""")
    S["sqft"] = ("Most expensive markets per square foot",
        "Normalizing by square footage makes markets directly comparable regardless of unit mix.",
        cols, rows, "")

    # ============ 7. CHEAPEST MARKETS ============
    cols, rows = q(db, """
        SELECT s.city, s.state, ROUND(AVG(u.price),0) avg_10x10, COUNT(*) listings
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.sqft=100 AND u.price IS NOT NULL AND s.city IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(*)>=5
        ORDER BY avg_10x10 ASC LIMIT 15""")
    S["cheap"] = ("Cheapest markets for a 10x10", "", cols, rows, "")

    # ============ 8. EXTREME UNITS ============
    cols, rows = q(db, """
        SELECT u.size, u.price, s.address, s.city, s.state
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.price IS NOT NULL AND u.sqft >= 25
        ORDER BY u.price ASC LIMIT 10""")
    S["cheapest_units"] = ("The 10 cheapest storage units in America",
        "Real, currently listed units (5x5 or larger).", cols, rows, "")

    cols, rows = q(db, """
        SELECT u.size, u.price, s.address, s.city, s.state
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.price IS NOT NULL
        ORDER BY u.price DESC LIMIT 10""")
    S["priciest_units"] = ("...and the 10 most expensive",
        "The other end of the market.", cols, rows, "")

    # ============ 9. STORES PER CAPITA ============
    _, srows = q(db, "SELECT state, COUNT(*) FROM stores WHERE state IS NOT NULL GROUP BY state")
    percap = []
    for st, n in srows:
        pop = STATE_POP.get(st)
        if pop and n >= 5:
            percap.append((st, n, pop, round(n / pop, 1)))
    percap.sort(key=lambda r: -r[3])
    cols = ["state", "stores", "pop_millions", "stores_per_million"]
    S["percap"] = ("Storage saturation per capita",
        "Facilities per million residents. Sun-belt states dominate — a mix of population growth, "
        "cheap land, and a car-first culture that generates overflow stuff.",
        cols, percap[:15], bars_html(percap[:15], 0, 3, prefix=""))

    # ============ 10. NEAREST-NEIGHBOR CLUSTERING ============
    _, crows = q(db, """
        SELECT city, state, store_id, lat, lng FROM stores
        WHERE city IS NOT NULL AND lat IS NOT NULL AND lng IS NOT NULL""")
    by_city = {}
    for city, st, sid, lat, lng in crows:
        by_city.setdefault((city, st), []).append((lat, lng))
    clusters = []
    for (city, st), pts in by_city.items():
        if len(pts) < 8:
            continue
        dists = []
        for i, (la, ln) in enumerate(pts):
            nearest = min(haversine(la, ln, lb, lm) for j, (lb, lm) in enumerate(pts) if j != i)
            dists.append(nearest)
        clusters.append((city, st, len(pts), round(sum(dists) / len(dists), 2)))
    clusters.sort(key=lambda r: r[3])
    cols = ["city", "state", "stores", "avg_miles_to_nearest"]
    S["cluster"] = ("Elbow-to-elbow — average distance to the next Public Storage",
        "For cities with 8+ facilities: how far is each store from its nearest sibling, on average? "
        "In the tightest metros, the same brand competes with itself just blocks apart.",
        cols, clusters[:15], "")

    # ============ 11. VEHICLE / RV / PARKING ============
    cols, rows = q(db, """
        SELECT s.state, COUNT(*) listings, ROUND(AVG(u.price),0) avg_parking,
               MIN(u.price) cheapest
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.size='Parking' AND u.price IS NOT NULL AND s.state IS NOT NULL
        GROUP BY s.state HAVING COUNT(*)>=10 ORDER BY avg_parking DESC LIMIT 15""")
    pk_stores = db.execute("SELECT COUNT(DISTINCT store_id) FROM units WHERE size='Parking'").fetchone()[0]
    pk_ratio = db.execute("""
        SELECT ROUND(100.0*AVG(CASE WHEN size='Parking' THEN price END) /
                     AVG(CASE WHEN sqft=100 THEN price END), 0) FROM units WHERE price IS NOT NULL
    """).fetchone()[0]
    S["parking"] = ("The vehicle storage market",
        f"{pk_stores:,} facilities rent uncovered vehicle/RV/boat spaces. Nationally a parking spot "
        f"advertises at about {pk_ratio:.0f}% of a 10x10's rate — driveway arbitrage for anyone with "
        "a project car and an HOA.",
        cols, rows, bars_html(rows, 0, 2))

    # ============ 12. SIZE MIX ============
    cols, rows = q(db, """
        SELECT size, COUNT(*) listings, ROUND(AVG(price),0) avg_price
        FROM units WHERE size IS NOT NULL AND price IS NOT NULL
        GROUP BY size ORDER BY listings DESC LIMIT 12""")
    S["mix"] = ("Unit size mix", "What the network actually stocks, by listing volume.", cols, rows, "")

    # ============ 13. SATURATION ============
    cols, rows = q(db, """
        SELECT city, state, COUNT(*) stores FROM stores
        WHERE city IS NOT NULL GROUP BY city, state
        ORDER BY stores DESC LIMIT 15""")
    S["sat"] = ("Most saturated metros", "Facility count by city — where the footprint concentrates.",
        cols, rows, bars_html(rows, 0, 2, prefix=""))

    # ============ 14. DATA GAPS (audit) ============
    cols, rows = q(db, """
        SELECT s.state, COUNT(*) AS stores_without_units
        FROM stores s
        WHERE NOT EXISTS (SELECT 1 FROM units u WHERE u.store_id = s.store_id)
          AND s.state IS NOT NULL
        GROUP BY s.state ORDER BY stores_without_units DESC LIMIT 15""")
    S["audit"] = ("Data notes",
        "Stores with no unit listings (they're fully sold out — see the scarcity section) and any "
        "records with missing fields surface here so the pipeline's health is visible.",
        cols, rows, "")

    # ---- console output ----
    print(f"\nDATASET: {kpi['stores']:,} stores · {kpi['units']:,} unit listings · "
          f"{kpi['inventory']:,} units available now · median 10x10 ${kpi['median_10x10']:,.0f} · "
          f"{kpi['no_units']:,} stores sold out · {kpi['no_coords']:,} missing coords · "
          f"{kpi['no_state']:,} missing state")
    for key in S:
        t, note, cols, rows, _ = S[key]
        console(t, cols, rows)

    # ---- HTML report ----
    today = datetime.date.today().strftime("%B %d, %Y")
    secs = ""
    for key in S:
        t, note, cols, rows, bars = S[key]
        secs += f"<section><h2>{html.escape(t)}</h2>"
        if note: secs += f"<p class='note'>{html.escape(note)}</p>"
        if bars: secs += bars
        if cols:  # prose-only sections carry their finding in the note
            secs += table_html(cols, rows)
        secs += "</section>"

    page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Self-Storage Pricing Insights — FindStorage</title>
<meta name="description" content="Original analysis of {kpi['stores']:,} self-storage facilities: live inventory, true promo cost, state pricing, local variance, and market saturation.">
<style>
:root{{--bg:#101418;--card:#161c22;--line:#232c35;--txt:#e8edf2;--dim:#8fa0af;--acc:#f0a44b;--bar:#2b3a47}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--txt);font-family:system-ui,'Segoe UI',sans-serif;line-height:1.6;font-size:16px}}
.wrap{{max-width:860px;margin:0 auto;padding:0 20px}}
header{{padding:56px 0 30px;border-bottom:1px solid var(--line)}}
header h1{{font-size:clamp(1.7rem,5vw,2.5rem);line-height:1.15}}
header h1 span{{color:var(--acc)}}
header p.meta{{color:var(--dim);margin-top:10px;font-size:.92rem}}
header p.meta a{{color:var(--acc);text-decoration:none}}
.kpis{{display:flex;flex-wrap:wrap;gap:14px;margin:26px 0 8px}}
.kpi{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:14px 18px;min-width:130px}}
.kpi .n{{font-size:1.5rem;font-weight:700;color:var(--acc)}}
.kpi .l{{font-size:.75rem;color:var(--dim);text-transform:uppercase;letter-spacing:.08em}}
section{{padding:34px 0;border-bottom:1px solid var(--line)}}
h2{{font-size:1.25rem;margin-bottom:8px}}
.note{{color:var(--dim);font-size:.93rem;max-width:640px;margin-bottom:16px}}
table{{width:100%;border-collapse:collapse;font-size:.88rem;margin-top:6px}}
th{{text-align:left;color:var(--dim);font-weight:600;padding:8px 10px;border-bottom:1px solid var(--line);
text-transform:uppercase;font-size:.7rem;letter-spacing:.08em}}
td{{padding:8px 10px;border-bottom:1px solid var(--line)}}
tr:hover td{{background:var(--card)}}
.bars{{margin:14px 0 20px}}
.bar-row{{display:flex;align-items:center;gap:10px;margin-bottom:7px;font-size:.85rem}}
.bar-label{{flex:0 0 130px;color:var(--dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;text-align:right}}
.bar-track{{flex:1;height:16px;background:var(--bar);border-radius:3px;overflow:hidden}}
.bar-fill{{display:block;height:100%;background:var(--acc)}}
.bar-val{{flex:0 0 70px;color:var(--txt)}}
.empty{{color:var(--dim);font-style:italic}}
footer{{padding:34px 0 50px;color:var(--dim);font-size:.85rem}}
footer a{{color:var(--acc);text-decoration:none}}
@media(max-width:600px){{.bar-label{{flex-basis:90px}}table{{font-size:.78rem}}td,th{{padding:6px}}}}
</style></head><body>
<header><div class="wrap">
<h1>Self-Storage Pricing Insights<br><span>{kpi['stores']:,} facilities, analyzed</span></h1>
<p class="meta">Original research built on the FindStorage dataset · updated {today} · analysis by Braeden Keena · <a href="/">directory</a> · <a href="/trends.html">daily trends</a></p>
<div class="kpis">
<div class="kpi"><div class="n">{kpi['stores']:,}</div><div class="l">facilities</div></div>
<div class="kpi"><div class="n">{kpi['units']:,}</div><div class="l">unit listings</div></div>
<div class="kpi"><div class="n">{kpi['inventory']:,}</div><div class="l">units available now</div></div>
<div class="kpi"><div class="n">${kpi['median_10x10']:,.0f}</div><div class="l">median 10x10 / mo</div></div>
<div class="kpi"><div class="n">{kpi['no_units']:,}</div><div class="l">stores sold out</div></div>
</div></div></header>
<main class="wrap">{secs}</main>
<footer><div class="wrap">Data collected from publicly advertised rates. Part of
<a href="https://findstorage.netlify.app">FindStorage</a> — an independent self-storage directory.
Built with Python + SQLite.</div></footer>
</body></html>"""

    with open(OUT, "w", encoding="utf-8") as f:
        f.write(page)
    print(f"\nWrote {OUT}")

if __name__ == "__main__":
    main()
