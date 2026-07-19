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

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

OUT = "trends.html"

# Only the monthly per-store aggregate files (YYYY-MM.csv). sizes-*.csv and
# rate_changes.csv have their own schemas and their own loaders below.
MONTH_CSV = re.compile(r"^\d{4}-\d{2}\.csv$")
SIZE_CSV = re.compile(r"^sizes-\d{4}-\d{2}\.csv$")
SIZE_ORDER = ["Locker", "5x5", "5x10", "5x15", "10x10", "10x15", "10x20", "10x25", "10x30", "Parking"]

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

def load_size_history():
    """date -> size -> {listings, avail, wsum} from history/sizes-YYYY-MM.csv
    (state-by-size aggregates). wsum is listings-weighted price*listings, so a
    national per-size average is a weighted mean of state medians, not a true
    national median — the raw per-listing prices aren't retained at this
    granularity. Good for a trendline, not a precise figure."""
    agg = {}
    for p in sorted(Path("history").glob("sizes-*.csv")):
        if not SIZE_CSV.match(p.name):
            continue
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                d, sz = row["date"], row["size"]
                cell = agg.setdefault(d, {}).setdefault(sz, {"listings": 0, "avail": 0, "wsum": 0.0})
                n = int(row["listings"] or 0)
                cell["listings"] += n
                cell["avail"] += int(row["units_avail"] or 0)
                if row["median_price"]:
                    cell["wsum"] += float(row["median_price"]) * n
    return dict(sorted(agg.items()))

def load_rate_changes():
    """Every logged SKU-level price change from history/rate_changes.csv."""
    p = Path("history/rate_changes.csv")
    if not p.exists():
        return []
    out = []
    with open(p, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("field") != "price":
                continue
            try:
                old, new = float(row["old"]), float(row["new"])
            except (TypeError, ValueError):
                continue
            if old <= 0:
                continue
            out.append({"date": row["date"], "site": row["site_number"] or "?",
                        "size": row["size"] or "?", "old": old, "new": new,
                        "delta": new - old, "pct": 100.0 * (new - old) / old})
    return out

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

def table(cols, rows, limit=15, empty="No rows to show."):
    """empty: the specific, accurate reason this table has nothing in it.

    Never claim "check back after a few daily runs" — that asserts the
    pipeline is healthy and merely young, which is a claim this function is
    in no position to make. The caller diagnoses the real reason and passes
    it in."""
    if not rows:
        return f"<p class='empty'>{empty}</p>"
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

    # ---- pipeline freshness diagnosis -------------------------------------
    # The movers tables can be empty for several very different reasons, and
    # the page must say which one is true rather than defaulting to a
    # reassuring "check back soon".
    today_d = datetime.date.today()
    stale_days = (today_d - lat_d).days
    identical_baseline = False
    if base:
        b_, l_ = snaps[base], snaps[latest]
        identical_baseline = (
            set(b_) == set(l_)
            and all(b_[s] == l_[s] for s in b_)
        )

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

    # ---- the accurate reason a movers table is empty ----------------------
    def why_empty(kind):
        if not base:
            return (f"Only one snapshot has ever been logged ({latest}). A comparison "
                    f"needs two, so there is nothing to diff yet.")
        if identical_baseline:
            return (f"The {latest} snapshot matches {base} at every store, so the two "
                    f"dates carry the same reading and no {kind} can be computed from "
                    f"them. Treat this as 'no comparison available', not as a market "
                    f"that held perfectly still.")
        return (f"No {kind} recorded between {base} and {latest}.")

    # A single, prominent banner shown only when the two most recent readings
    # can't be compared, or when the newest one is several days old. Silence
    # here means the history log is current.
    if identical_baseline:
        freshness_banner = f"""<div class="alert"><b>No day-over-day comparison available.</b> The
most recent snapshot ({latest}) matches {base} at every store, count and price, so the movers
tables below have nothing to diff and are empty for that reason rather than because the market was
flat. {len(dates)} distinct snapshot{'s' if len(dates)!=1 else ''} in the history log.</div>"""
    elif stale_days >= 3:
        freshness_banner = f"""<div class="alert"><b>Data is {stale_days} days old.</b> The most
recent snapshot in the history log is {latest}, so everything below describes the market as of
that date.</div>"""
    else:
        freshness_banner = ""

    today = datetime.date.today().strftime("%B %d, %Y")
    total_now = units_series[-1][1]
    med_now = price_series[-1][1] if price_series else 0

    # ---- biggest movers (SKU-level rate-change log) ----
    rate_events = load_rate_changes()
    if rate_events:
        rc_cols = ["date", "site #", "size", "old", "new", "change", "change %"]
        def rc_row(r):
            sign = "+" if r["delta"] > 0 else ""
            return (r["date"], f"#{r['site']}", r["size"], f"${r['old']:,.2f}", f"${r['new']:,.2f}",
                    f"{sign}${r['delta']:,.2f}", f"{sign}{r['pct']:.1f}%")
        rc_hikes = [rc_row(r) for r in sorted(rate_events, key=lambda r: -r["delta"])[:10] if r["delta"] > 0]
        rc_cuts = [rc_row(r) for r in sorted(rate_events, key=lambda r: r["delta"])[:10] if r["delta"] < 0]
        n_days = len(set(r["date"] for r in rate_events))
        movers_section = f"""
<section><h2>SKU-level rate changes</h2>
<p class='note'>Every individual change in an advertised unit price the daily rate log has captured,
ranked by size of change. {len(rate_events):,} price changes logged across
{n_days} day{'s' if n_days != 1 else ''} so far. These are observed changes in published online
rates between two snapshots; no cause is inferred.</p>
<h3 class="sub">Largest increases</h3>{table(rc_cols, rc_hikes, 10,
    "No advertised price increases in the log yet.")}
<h3 class="sub">Largest decreases</h3>{table(rc_cols, rc_cuts, 10,
    "No advertised price decreases in the log yet.")}</section>"""
    else:
        rc_reason = (
            "history/rate_changes.csv is empty — zero events have ever been logged. The log is "
            "built by diffing the previous dataset snapshot against the current one, matching "
            "units by SKU. That diff has never produced a result, for two separate reasons: the "
            "previous-snapshot file is excluded from version control, so it does not exist at all "
            "in the automated run, and the local copy predates the <code>sku</code> field, so it "
            "carries no SKUs to match against. Both are collection-side faults. This section will "
            "populate once two consecutive same-schema snapshots have actually been captured — it "
            "is not waiting on the market to move."
        )
        movers_section = f"""
<section><h2>SKU-level rate changes</h2>
<p class='note'>Individual unit-price changes from the daily rate-change log
(history/rate_changes.csv).</p>
<p class='empty'>{rc_reason}</p></section>"""

    # ---- trends by size (state-by-size demand log) ----
    size_hist = load_size_history()
    size_dates = list(size_hist)
    sizes_present = sorted({sz for d in size_hist.values() for sz in d},
                           key=lambda s: SIZE_ORDER.index(s) if s in SIZE_ORDER else len(SIZE_ORDER))
    size_panels = ""
    for i, sz in enumerate(sizes_present):
        avail_sz = [(d, size_hist[d][sz]["avail"]) for d in size_dates if sz in size_hist[d]]
        price_sz = [(d, size_hist[d][sz]["wsum"] / size_hist[d][sz]["listings"])
                    for d in size_dates if sz in size_hist[d] and size_hist[d][sz]["listings"]]
        style = "" if i == 0 else " style=\"display:none\""
        size_panels += f"""<div class="size-panel" data-size="{html.escape(sz)}"{style}>
<div class="chart-pair">
<div><h3 class="sub">Advertised availability — {html.escape(sz)}</h3>{svg_line(avail_sz)}</div>
<div><h3 class="sub">Weighted-avg price — {html.escape(sz)}</h3>{svg_line(price_sz, fmt="{:,.0f}", prefix="$")}</div>
</div></div>"""
    if sizes_present:
        size_options = "".join(f'<option value="{html.escape(sz)}">{html.escape(sz)}</option>' for sz in sizes_present)
        size_section = f"""
<section><h2>Trends by size</h2>
<p class='note'>Availability and price over time, split by unit size, from the state-by-size demand log
(history/sizes-YYYY-MM.csv). Price is a national average weighted by each state's listing count, since
raw per-listing prices aren't retained at this granularity — read it as a trendline, not a precise
national median. {len(size_dates)} day{'s' if len(size_dates) != 1 else ''} logged so far — the chart
needs at least two distinct days to show a line, and gains one day per successful collection run.</p>
<select id="size-picker" onchange="pickSize(this)">{size_options}</select>
{size_panels}</section>"""
    else:
        size_section = """
<section><h2>Trends by size</h2>
<p class='note'>Not enough data yet — this fills in once history/sizes-YYYY-MM.csv has logged a day.</p></section>"""

    sections = f"""{freshness_banner}
<section><h2>National advertised inventory</h2>
<p class='note'>Total units marked rentable across the network, per snapshot. This reflects advertised
availability — units published as rentable — which may differ from physical vacancy; the two aren't
directly comparable from public data. Read this as a measure of what is being advertised over time,
not as an occupancy figure.</p>
{svg_line(units_series)}</section>

<section><h2>National median 10x10 price</h2>
<p class='note'>Median of each store's cheapest available 10x10, per snapshot.</p>
{svg_line(price_series, fmt="{:,.0f}", prefix="$")}</section>

<section><h2>Largest declines in advertised availability</h2>
<p class='note'>Stores whose count of units listed as available fell the most over the period
{html.escape(period)}. This measures what the website advertised on two dates and nothing more:
a smaller number means fewer units were listed as available on the later date. Advertised
availability is not a physical vacancy count, and a change in it can arise from many ordinary
causes — rentals, listing and pricing updates, unit reclassification, or site changes — which
public data cannot distinguish between.</p>
{table(mover_cols, movers, 15, why_empty("declines in advertised availability"))}</section>

<section><h2>Largest increases in advertised availability</h2>
<p class='note'>The opposite end — stores whose count of listed-available units rose the most
over the period. The same caveat applies in reverse: this is a change in what was advertised
between two dates, not a measured change in physical occupancy.</p>
{table(mover_cols, restock, 10, why_empty("increases in advertised availability"))}</section>

<section><h2>10x10 advertised price increases</h2>
<p class='note'>Largest increases in a store's cheapest advertised 10x10 over the period.
Prices shown are the advertised online rates on each date.</p>
{table(price_cols, hikes, 10, why_empty("10x10 price increases"))}</section>

<section><h2>10x10 advertised price decreases</h2>
<p class='note'>Largest decreases in a store's cheapest advertised 10x10 over the period.
Published rates move for many reasons; this table reports the change without inferring a cause.</p>
{table(price_cols, cuts, 10, why_empty("10x10 price decreases"))}</section>
{movers_section}
{size_section}

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
h3.sub{{font-size:.95rem;margin:18px 0 8px;color:var(--txt)}}
.note{{color:var(--dim);font-size:.93rem;max-width:640px;margin-bottom:16px}}
table{{width:100%;border-collapse:collapse;font-size:.88rem;margin-top:6px}}
th{{text-align:left;color:var(--dim);font-weight:600;padding:8px 10px;border-bottom:1px solid var(--line);
text-transform:uppercase;font-size:.7rem;letter-spacing:.08em}}
td{{padding:8px 10px;border-bottom:1px solid var(--line)}}
tr:hover td{{background:var(--card)}}
.empty{{color:var(--dim);font-style:italic;max-width:640px}}
.empty code{{font-family:'DM Mono',monospace,monospace;font-size:.9em;color:var(--acc);font-style:normal}}
.alert{{background:#2a1d12;border:1px solid #6b4423;border-left:4px solid var(--acc);border-radius:8px;
padding:16px 18px;margin:28px 0 0;font-size:.92rem;color:#f3dcc4;max-width:760px}}
.alert b{{color:var(--acc)}}
#size-picker{{background:var(--card);border:1px solid var(--line);border-radius:8px;color:var(--txt);
padding:8px 12px;font-family:inherit;font-size:.88rem;margin-bottom:12px}}
.chart-pair{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}
@media(max-width:700px){{.chart-pair{{grid-template-columns:1fr}}}}
footer{{padding:34px 0 50px;color:var(--dim);font-size:.85rem}}
footer a{{color:var(--acc);text-decoration:none}}
@media(max-width:600px){{table{{font-size:.75rem}}td,th{{padding:6px}}}}
</style></head><body>
<header><div class="wrap">
<h1>Daily Trends<br><span>what moved in the storage market</span></h1>
<p class="meta">Updated {today} from {len(dates)} snapshot{'s' if len(dates)!=1 else ''} ·
<a href="/">directory</a> · <a href="/insights.html">insights</a> · <a href="/markets.html">metro markets</a></p>
<div class="kpis">
<div class="kpi"><div class="n">{total_now:,}</div><div class="l">units available now</div></div>
<div class="kpi"><div class="n">${med_now:,.0f}</div><div class="l">median 10x10 / mo</div></div>
<div class="kpi"><div class="n">{len(snaps[latest]):,}</div><div class="l">stores tracked</div></div>
</div></div></header>
<main class="wrap">{sections}</main>
<footer><div class="wrap">History begins April 29, 2026; snapshots accumulate daily. Part of
<a href="https://findstorage.netlify.app">FindStorage</a> — an independent self-storage directory.
Built with Python + SQLite.</div></footer>
<script>
function pickSize(sel){{
  var v = sel.value;
  document.querySelectorAll('.size-panel').forEach(function(p){{
    p.style.display = (p.dataset.size === v) ? 'block' : 'none';
  }});
}}
</script>
</body></html>"""

    Path(OUT).write_text(page, encoding="utf-8")
    print(f"Wrote {OUT} ({len(dates)} snapshots, period: {period or 'n/a'})")

if __name__ == "__main__":
    main()
