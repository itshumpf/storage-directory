"""
analyze_storage.py — Full analysis pass over the FindStorage dataset.

Run after load_storage.py has created storage.db (from the repo root):
    python analysis/analyze_storage.py

Does three things:
  1. Data quality audit (console) — stores with no units, missing prices,
     missing coordinates/state.
  2. Market analysis (console) — state pricing, price per sqft, local
     variance, promotions, size mix, saturation, outliers.
  3. Generates insights.html — a self-contained report page served alongside
     the directory. Pure stdlib, no dependencies.
"""
import sqlite3, html, datetime, sys

DB = "storage.db"
OUT = "insights.html"

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

def bars_html(rows, label_i, value_i, prefix="$"):
    """CSS bar chart from rows: label col index, numeric col index."""
    rows = [r for r in rows if r[value_i] is not None][:10]
    if not rows:
        return ""
    mx = max(float(r[value_i]) for r in rows) or 1
    out = "<div class='bars'>"
    for r in rows:
        pct = 100.0 * float(r[value_i]) / mx
        out += (f"<div class='bar-row'><span class='bar-label'>{html.escape(str(r[label_i]))}</span>"
                f"<span class='bar-track'><span class='bar-fill' style='width:{pct:.1f}%'></span></span>"
                f"<span class='bar-val'>{prefix}{float(r[value_i]):,.0f}</span></div>")
    return out + "</div>"

def main():
    try:
        db = sqlite3.connect(DB)
        total = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    except Exception as e:
        sys.exit(f"Couldn't open {DB} — run analysis/load_storage.py first. ({e})")
    if not total:
        sys.exit("storage.db has 0 stores — check load_storage.py output.")

    S = {}  # sections: key -> (title, note, cols, rows, bars_html_or_empty)

    # ============ 1. DATA QUALITY AUDIT ============
    kpi = {}
    kpi["stores"] = total
    kpi["units"] = db.execute("SELECT COUNT(*) FROM units").fetchone()[0]
    kpi["priced"] = db.execute("SELECT COUNT(*) FROM units WHERE price IS NOT NULL").fetchone()[0]
    kpi["no_units"] = db.execute(
        "SELECT COUNT(*) FROM stores s WHERE NOT EXISTS (SELECT 1 FROM units u WHERE u.store_id=s.store_id)"
    ).fetchone()[0]
    kpi["no_coords"] = db.execute("SELECT COUNT(*) FROM stores WHERE lat IS NULL OR lng IS NULL").fetchone()[0]
    kpi["no_state"] = db.execute("SELECT COUNT(*) FROM stores WHERE state IS NULL OR state=''").fetchone()[0]

    cols, rows = q(db, """
        SELECT s.state, COUNT(*) AS stores_without_units
        FROM stores s
        WHERE NOT EXISTS (SELECT 1 FROM units u WHERE u.store_id = s.store_id)
          AND s.state IS NOT NULL
        GROUP BY s.state ORDER BY stores_without_units DESC LIMIT 15""")
    S["audit"] = ("Data gaps — stores with no unit listings",
        "These facilities returned no pricing data. Either the scraper needs a retry pass on them, "
        "or they genuinely list no availability online — both are worth knowing. This is the re-scrape fix list.",
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
        "operators how loosely rates track location within a metro.",
        cols, rows, bars_html(rows, 0, 4))

    # ============ 4. PRICE PER SQFT ============
    cols, rows = q(db, """
        SELECT s.city, s.state, ROUND(AVG(u.price/u.sqft),2) avg_per_sqft, COUNT(*) listings
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.price IS NOT NULL AND u.sqft>0 AND s.city IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(*)>=20
        ORDER BY avg_per_sqft DESC LIMIT 15""")
    S["sqft"] = ("Most expensive markets per square foot",
        "Normalizing by square footage makes markets directly comparable regardless of unit mix.",
        cols, rows, "")

    # ============ 5. CHEAPEST MARKETS ============
    cols, rows = q(db, """
        SELECT s.city, s.state, ROUND(AVG(u.price),0) avg_10x10, COUNT(*) listings
        FROM units u JOIN stores s ON s.store_id=u.store_id
        WHERE u.sqft=100 AND u.price IS NOT NULL AND s.city IS NOT NULL
        GROUP BY s.city, s.state HAVING COUNT(*)>=5
        ORDER BY avg_10x10 ASC LIMIT 15""")
    S["cheap"] = ("Cheapest markets for a 10x10", "", cols, rows, "")

    # ============ 6. PROMO ANALYSIS ============
    cols, rows = q(db, """
        SELECT promo_name, COUNT(*) uses,
               ROUND(AVG(100.0*(price-promo_price)/price),1) avg_pct_off
        FROM units WHERE promo_name IS NOT NULL AND price>0 AND promo_price IS NOT NULL
        GROUP BY promo_name ORDER BY uses DESC LIMIT 12""")
    S["promo"] = ("Promotion strategy — which discounts run, and how deep",
        "Advertised promotional pricing across the network: which offers are deployed most, "
        "and the real average percentage off.",
        cols, rows, "")

    # ============ 7. SIZE MIX ============
    cols, rows = q(db, """
        SELECT size, COUNT(*) listings, ROUND(AVG(price),0) avg_price
        FROM units WHERE size IS NOT NULL AND price IS NOT NULL
        GROUP BY size ORDER BY listings DESC LIMIT 12""")
    S["mix"] = ("Unit size mix", "What the network actually stocks, by listing volume.", cols, rows, "")

    # ============ 8. SATURATION ============
    cols, rows = q(db, """
        SELECT city, state, COUNT(*) stores FROM stores
        WHERE city IS NOT NULL GROUP BY city, state
        ORDER BY stores DESC LIMIT 15""")
    S["sat"] = ("Most saturated metros", "Facility count by city — where the footprint concentrates.",
        cols, rows, bars_html(rows, 0, 2, prefix=""))

    # ---- console output ----
    print(f"\nDATASET: {kpi['stores']:,} stores · {kpi['units']:,} unit listings "
          f"({kpi['priced']:,} priced) · {kpi['no_units']:,} stores with NO units · "
          f"{kpi['no_coords']:,} missing coords · {kpi['no_state']:,} missing state")
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
        secs += table_html(cols, rows) + "</section>"

    page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Self-Storage Pricing Insights — FindStorage</title>
<meta name="description" content="Original analysis of {kpi['stores']:,} self-storage facilities: state pricing, local variance, promotions, and market saturation.">
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
<p class="meta">Original research built on the FindStorage dataset · updated {today} · analysis by Braeden Keena · <a href="/">back to the directory</a></p>
<div class="kpis">
<div class="kpi"><div class="n">{kpi['stores']:,}</div><div class="l">facilities</div></div>
<div class="kpi"><div class="n">{kpi['units']:,}</div><div class="l">unit listings</div></div>
<div class="kpi"><div class="n">{kpi['priced']:,}</div><div class="l">with live pricing</div></div>
<div class="kpi"><div class="n">{kpi['no_units']:,}</div><div class="l">no listings (gaps)</div></div>
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
