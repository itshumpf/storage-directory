"""
build_trends.py — Generate trends.html from the daily history log.

Run after update_history.py (from the repo root):
    python analysis/build_trends.py

Reads history/*.csv and the current enriched_locations.json, then writes a
daily-changing report: national inventory and price trend charts, the
fastest-renting stores, restocks, the biggest price hikes and cuts, and
tracking-coverage changes. Pure stdlib.
"""
import csv
import datetime
import html
import json
import re
import statistics
import sys
from pathlib import Path

OUT = "trends.html"

# Only the monthly per-store aggregate files (YYYY-MM.csv). Other history
# files (pipeline.csv, sizes-*.csv, rate_changes.csv) have different schemas.
MONTH_CSV = re.compile(r"^\d{4}-\d{2}\.csv$")

def load_history():
    snaps = {}  # date -> {sid: dict}
    for p in sorted(Path("history").glob("*.csv")):
        if not MONTH_CSV.match(p.name):
            continue
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                d = row["date"]
                snaps.setdefault(d, {})[row["store_id"]] = {
                    "units": int(row["units_avail"] or 0),
                    "ten": float(row["cheapest_10x10"]) if row["cheapest_10x10"] else None,
                    "med": float(row["median_price"]) if row["median_price"] else None,
                }
    return dict(sorted(snaps.items()))

def svg_line(series, fmt="{:,.0f}", prefix=""):
    """series: [(date, value)] -> responsive SVG line chart."""
    if not series:
        return "<p class='empty'>No data yet.</p>"
    W, H, PAD = 760, 170, 34
    vals = [v for _, v in series]
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1
    n = len(series)
    def x(i): return PAD + (W - 2 * PAD) * (i / max(n - 1, 1))
    def y(v): return H - PAD - (H - 2 * PAD) * ((v - lo) / span)
    pts = " ".join(f"{x(i):.1f},{y(v):.1f}" for i, (_, v) in enumerate(series))
    dots = "".join(f"<circle cx='{x(i):.1f}' cy='{y(v):.1f}' r='3.5' fill='#f0a44b'/>"
                   for i, (_, v) in enumerate(series))
    first_d, last_d = series[0][0], series[-1][0]
    return f"""<svg viewBox="0 0 {W} {H}" role="img" style="width:100%;height:auto">
<line x1="{PAD}" y1="{H-PAD}" x2="{W-PAD}" y2="{H-PAD}" stroke="#232c35"/>
<polyline points="{pts}" fill="none" stroke="#f0a44b" stroke-width="2.5"/>{dots}
<text x="{PAD}" y="16" fill="#8fa0af" font-size="12">{prefix}{fmt.format(hi)}</text>
<text x="{PAD}" y="{H-PAD+16}" fill="#8fa0af" font-size="12">{first_d}</text>
<text x="{W-PAD}" y="{H-PAD+16}" fill="#8fa0af" font-size="12" text-anchor="end">{last_d}</text>
<text x="{PAD}" y="{H-PAD-6}" fill="#8fa0af" font-size="12">{prefix}{fmt.format(lo)}</text>
</svg>"""

def table(cols, rows, limit=15):
    if not rows:
        return "<p class='empty'>Not enough history yet — check back after a few daily runs.</p>"
    h = "<table><thead><tr>" + "".join(f"<th>{html.escape(str(c))}</th>" for c in cols) + "</tr></thead><tbody>"
    for r in rows[:limit]:
        h += "<tr>" + "".join(f"<td>{html.escape(str(v if v is not None else '—'))}</td>" for v in r) + "</tr>"
    return h + "</tbody></table>"

def main():
    snaps = load_history()
    if not snaps:
        sys.exit("No history found — run analysis/update_history.py first.")
    dates = list(snaps)
    latest = dates[-1]

    # baseline: most recent snapshot at least 7 days older than latest, else oldest
    lat_d = datetime.date.fromisoformat(latest)
    base = next((d for d in reversed(dates[:-1])
                 if (lat_d - datetime.date.fromisoformat(d)).days >= 7), dates[0] if len(dates) > 1 else None)

    meta = {}
    for s in json.loads(Path("enriched_locations.json").read_text(encoding="utf-8")):
        meta[str(s["store_id"])] = (s.get("site_number") or "?", s.get("address") or "",
                                    s.get("city") or "", s.get("state") or "")

    # national series
    units_series, price_series = [], []
    for d, stores in snaps.items():
        units_series.append((d, sum(v["units"] for v in stores.values())))
        tens = [v["ten"] for v in stores.values() if v["ten"]]
        if tens:
            price_series.append((d, statistics.median(tens)))

    movers = hikes = cuts = restock = []
    added = removed = []
    period = ""
    if base:
        b, l = snaps[base], snaps[latest]
        days = (lat_d - datetime.date.fromisoformat(base)).days
        period = f"{base} → {latest} ({days} days)"
        common = set(b) & set(l)
        deltas = []
        for sid in common:
            du = b[sid]["units"] - l[sid]["units"]  # positive = units rented away
            deltas.append((sid, b[sid], l[sid], du))
        def row(sid, bv, lv, val):
            m = meta.get(sid, ("?", "", "", ""))
            return (f"#{m[0]}", m[1], m[2], m[3], val)
        movers = [row(s, b_, l_, f"{b_['units']} → {l_['units']}  (−{d})")
                  for s, b_, l_, d in sorted(deltas, key=lambda t: -t[3]) if d > 0][:15]
        restock = [row(s, b_, l_, f"{b_['units']} → {l_['units']}  (+{-d})")
                   for s, b_, l_, d in sorted(deltas, key=lambda t: t[3]) if d < 0][:10]
        pdeltas = [(s, b[s]["ten"], l[s]["ten"], l[s]["ten"] - b[s]["ten"])
                   for s in common if b[s]["ten"] and l[s]["ten"] and l[s]["ten"] != b[s]["ten"]]
        hikes = [row(s, None, None, f"${o:,.0f} → ${n:,.0f}  (+${d:,.0f})")
                 for s, o, n, d in sorted(pdeltas, key=lambda t: -t[3]) if d > 0][:10]
        cuts = [row(s, None, None, f"${o:,.0f} → ${n:,.0f}  (−${-d:,.0f})")
                for s, o, n, d in sorted(pdeltas, key=lambda t: t[3]) if d < 0][:10]
        added = sorted(set(l) - set(b), key=int)
        removed = sorted(set(b) - set(l), key=int)

    mover_cols = ["site #", "address", "city", "state", "available units"]
    price_cols = ["site #", "address", "city", "state", "cheapest 10x10"]

    today = datetime.date.today().strftime("%B %d, %Y")
    total_now = units_series[-1][1]
    med_now = price_series[-1][1] if price_series else 0

    sections = f"""
<section><h2>National advertised inventory</h2>
<p class='note'>Total units marked rentable across the network, per snapshot. Counts are what the
website advertises — revenue management holds back part of the physically vacant inventory, so a
store with several hundred empty units may advertise far fewer. Trends here track marketed
availability, which moves with real demand.</p>
{svg_line(units_series)}</section>

<section><h2>National median 10x10 price</h2>
<p class='note'>Median of each store's cheapest available 10x10, per snapshot.</p>
{svg_line(price_series, fmt="{:,.0f}", prefix="$")}</section>

<section><h2>Renting the fastest</h2>
<p class='note'>Biggest drop in advertised available units over the period {html.escape(period)}.
A shrinking count means units are being rented (or pulled from marketing) faster than they're freed up.</p>
{table(mover_cols, movers)}</section>

<section><h2>Biggest restocks</h2>
<p class='note'>The opposite end — stores that added the most advertised availability.</p>
{table(mover_cols, restock, 10)}</section>

<section><h2>10x10 price hikes</h2>
<p class='note'>Largest increases in a store's cheapest 10x10 over the period.</p>
{table(price_cols, hikes, 10)}</section>

<section><h2>10x10 price cuts</h2>
<p class='note'>Largest decreases — often a signal of soft demand or fresh supply nearby.</p>
{table(price_cols, cuts, 10)}</section>

<section><h2>Tracking coverage</h2>
<p class='note'>{len(added):,} stores are tracked now that weren't in the {html.escape(base or "previous")}
snapshot, and {len(removed):,} dropped out. Additions can reflect improved discovery as well as new
openings; drops are usually closures. Coverage change becomes a clean openings/closures signal once
the baseline stabilizes.</p></section>
"""

    page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Daily Trends — FindStorage</title>
<meta name="description" content="Daily-updated self-storage trends: national inventory, median pricing, fastest-renting stores, and the biggest price moves.">
<style>
:root{{--bg:#101418;--card:#161c22;--line:#232c35;--txt:#e8edf2;--dim:#8fa0af;--acc:#f0a44b}}
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
.empty{{color:var(--dim);font-style:italic}}
footer{{padding:34px 0 50px;color:var(--dim);font-size:.85rem}}
footer a{{color:var(--acc);text-decoration:none}}
@media(max-width:600px){{table{{font-size:.75rem}}td,th{{padding:6px}}}}
</style></head><body>
<header><div class="wrap">
<h1>Daily Trends<br><span>what moved in the storage market</span></h1>
<p class="meta">Updated {today} from {len(dates)} snapshot{'s' if len(dates)!=1 else ''} ·
<a href="/">directory</a> · <a href="/insights.html">insights</a></p>
<div class="kpis">
<div class="kpi"><div class="n">{total_now:,}</div><div class="l">units available now</div></div>
<div class="kpi"><div class="n">${med_now:,.0f}</div><div class="l">median 10x10 / mo</div></div>
<div class="kpi"><div class="n">{len(snaps[latest]):,}</div><div class="l">stores tracked</div></div>
</div></div></header>
<main class="wrap">{sections}</main>
<footer><div class="wrap">History begins April 29, 2026; snapshots accumulate daily. Part of
<a href="https://findstorage.netlify.app">FindStorage</a> — an independent self-storage directory.
Built with Python + SQLite.</div></footer>
</body></html>"""

    Path(OUT).write_text(page, encoding="utf-8")
    print(f"Wrote {OUT} ({len(dates)} snapshots, period: {period or 'n/a'})")

if __name__ == "__main__":
    main()
