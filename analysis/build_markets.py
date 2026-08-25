"""
build_markets.py — Generate markets.html: an automatic per-ZIP3-prefix market
breakdown (every ZIP3 with a store gets a row — no hand-curated list) plus a
simple Renter Leverage Score heuristic for each market.

Run after load_storage.py, from the repo root:
    python analysis/build_markets.py

Reads storage.db for the current snapshot and history/*.csv (via
build_trends.load_history) for the trend columns. Pure stdlib.

Market labels default to the dominant city/state among a ZIP3's stores.
An optional override file, data/market_names.json ({"662": "Custom label"}),
can replace any label by hand later; it's entirely optional and the page
works with zero config.

Markets with fewer than MIN_STORES stores still show their store count (this
is the one number that's never noisy) but have their price/promo/score
columns suppressed — a median of 1-2 stores is not a market rate, it's an
anecdote wearing a market's clothes.
"""
import html
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))
from analyze_storage import median, table_html  # noqa: E402
from build_trends import load_history  # noqa: E402

DB = "storage.db"
OUT = "markets.html"
DETAIL_OUT = "markets-detail.json"   # lazily fetched by the drill-down panel
OVERRIDES = Path("data/market_names.json")
MIN_STORES = 3          # below this: suppress medians/promo/score, keep store count
MIN_SIZE_SAMPLE = 3     # min listings for a market-level size median to be shown
KEY_SIZES = ("10x10", "10x20")

# Size vocabulary shared by the drill-down payload: each market ships an array
# indexed against this list rather than repeating size names 354 times.
SIZE_VOCAB = ["Locker", "5x5", "5x10", "5x15", "10x10", "10x15",
              "10x20", "10x25", "10x30", "Parking"]
SIZE_SQFT = {"5x5": 25, "5x10": 50, "5x15": 75, "10x10": 100,
             "10x15": 150, "10x20": 200, "10x25": 250, "10x30": 300}


def dominant_label(city_state_counts, override=None):
    if override:
        return override
    top = city_state_counts.most_common()
    if not top:
        return "Unknown"
    (city0, state0), n0 = top[0]
    if len(top) > 1:
        (city1, state1), n1 = top[1]
        if state1 == state0 and n1 >= 0.4 * n0 and city1 != city0:
            return f"{city0} / {city1}, {state0}"
    return f"{city0}, {state0}"


def leverage_score(units_per_store, promo_pct, price_ratio):
    """0-100 heuristic. Each component maps to 0-100 with 50 = 'about average'."""
    avail_score = max(0.0, min(100.0, units_per_store / 40.0 * 100.0))
    promo_score = max(0.0, min(100.0, promo_pct))
    price_score = max(0.0, min(100.0, 50.0 - (price_ratio - 1.0) * 200.0))
    return round((avail_score + promo_score + price_score) / 3.0)


def tier(score):
    if score >= 67:
        return "Renter-favorable"
    if score >= 34:
        return "Balanced"
    return "Tight"


def blurb(score):
    if score >= 67:
        return "Lots of open units and heavy promos — room to negotiate."
    if score >= 34:
        return "Mixed signals — some room to negotiate, some tight spots."
    return "Tight and close to full price — less room to negotiate."


def main():
    try:
        db = sqlite3.connect(DB)
        total_stores = db.execute("SELECT COUNT(*) FROM stores").fetchone()[0]
    except Exception as e:
        sys.exit(f"Couldn't open {DB} — run analysis/load_storage.py first. ({e})")
    if not total_stores:
        sys.exit("storage.db has 0 stores — check load_storage.py output.")

    overrides = {}
    if OVERRIDES.exists():
        try:
            overrides = json.loads(OVERRIDES.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"Warning: couldn't parse {OVERRIDES}: {e}")

    stores = db.execute(
        "SELECT store_id, city, state, zip FROM stores WHERE zip IS NOT NULL AND LENGTH(zip) >= 3"
    ).fetchall()
    store_zip3 = {}
    mkt = defaultdict(lambda: {"stores": set(), "city_state": Counter()})
    for sid, city, state, zp in stores:
        z3 = zp[:3]
        if not z3.isdigit():
            continue
        store_zip3[sid] = z3
        mkt[z3]["stores"].add(sid)
        if city and state:
            mkt[z3]["city_state"][(city, state)] += 1

    units = db.execute(
        "SELECT store_id, size, price, available, unit_count, promo_name FROM units"
    ).fetchall()

    nat_by_size = defaultdict(list)
    for sid, size, price, avail, count, promo in units:
        z3 = store_zip3.get(sid)
        if avail and price is not None and size:
            nat_by_size[size].append(price)
        if not z3:
            continue
        m = mkt[z3]
        m.setdefault("units_avail", 0)
        m.setdefault("listings", 0)
        m.setdefault("promo", 0)
        m.setdefault("prices_by_size", defaultdict(list))
        if avail and price is not None:
            m["units_avail"] += count or 0
            m["listings"] += 1
            if promo:
                m["promo"] += 1
            if size:
                m["prices_by_size"][size].append(price)

    nat_median = {sz: median(v) for sz, v in nat_by_size.items()}
    nat_10x10 = nat_median.get("10x10")

    # ---- trend: earliest vs latest available snapshot ----
    snaps = load_history()
    dates = sorted(snaps)
    trend_period = ""
    market_trend = {}

    # Full per-market series across every logged snapshot, for the drill-down.
    # [units_avail, median cheapest-10x10] per date, aligned to `dates`.
    market_series = {}
    for z3, m in mkt.items():
        sids = m["stores"]
        series = []
        for d in dates:
            snap = snaps.get(d, {})
            present = [sid for sid in sids if sid in snap]
            if not present:
                series.append([None, None])
                continue
            u = sum(snap[sid]["units"] for sid in present)
            tens = [snap[sid]["ten"] for sid in present if snap[sid]["ten"]]
            series.append([u, round(median(tens)) if tens else None])
        market_series[z3] = series

    if len(dates) >= 2:
        d0, d1 = dates[0], dates[-1]
        trend_period = f"{d0} -> {d1}"
        for z3, m in mkt.items():
            sids = m["stores"]
            s0, s1 = snaps.get(d0, {}), snaps.get(d1, {})
            u0 = sum(s0[sid]["units"] for sid in sids if sid in s0)
            u1 = sum(s1[sid]["units"] for sid in sids if sid in s1)
            common = [sid for sid in sids if sid in s0 and sid in s1 and s0[sid]["ten"] and s1[sid]["ten"]]
            if common:
                p0 = median([s0[sid]["ten"] for sid in common])
                p1 = median([s1[sid]["ten"] for sid in common])
                dp = round(p1 - p0, 2)
            else:
                dp = None
            market_trend[z3] = (u1 - u0, dp)

    rows = []
    n_reliable = 0
    for z3, m in sorted(mkt.items(), key=lambda kv: -len(kv[1]["stores"])):
        n_stores = len(m["stores"])
        label = dominant_label(m["city_state"], overrides.get(z3))
        units_avail = m.get("units_avail", 0)
        listings = m.get("listings", 0)
        promo_ct = m.get("promo", 0)
        pbs = m.get("prices_by_size", {})
        upstore = units_avail / n_stores if n_stores else 0
        du, dp = market_trend.get(z3, (None, None))

        reliable = n_stores >= MIN_STORES
        if reliable:
            n_reliable += 1
            promo_pct = round(100.0 * promo_ct / listings, 1) if listings else None
            med10 = median(pbs["10x10"]) if len(pbs.get("10x10", [])) >= MIN_SIZE_SAMPLE else None
            med20 = median(pbs["10x20"]) if len(pbs.get("10x20", [])) >= MIN_SIZE_SAMPLE else None
            if med10 and nat_10x10 and promo_pct is not None:
                ratio = med10 / nat_10x10
                score = leverage_score(upstore, promo_pct, ratio)
            else:
                score = None
        else:
            promo_pct = med10 = med20 = score = None

        # per-size medians + sample counts for the drill-down, indexed against
        # SIZE_VOCAB. null where the sample is too thin to quote.
        by_size, n_size = [], []
        for sz in SIZE_VOCAB:
            vals = pbs.get(sz, [])
            if reliable and len(vals) >= MIN_SIZE_SAMPLE:
                by_size.append(round(median(vals)))
                n_size.append(len(vals))
            else:
                by_size.append(None)
                n_size.append(len(vals))

        rows.append({
            "zip3": z3, "label": label, "n_stores": n_stores,
            "units_avail": units_avail, "upstore": round(upstore, 1),
            "med10": med10, "med20": med20, "promo_pct": promo_pct,
            "du": du, "dp": dp, "score": score, "reliable": reliable,
            "by_size": by_size, "n_size": n_size, "listings": listings,
            "promo_ct": promo_ct,
        })

    today_iso = __import__("datetime").date.today().isoformat()

    def cell(v, fmt="{:,.0f}", dash="—"):
        return dash if v is None else fmt.format(v)

    body_rows = []
    for r in rows:
        if r["reliable"]:
            score_cell = f"{r['score']} · {tier(r['score'])}" if r["score"] is not None else "—"
        else:
            score_cell = "insufficient sample"
        du_cell = ("+" if (r["du"] or 0) > 0 else "") + cell(r["du"]) if r["du"] is not None else "—"
        dp_cell = ("+" if (r["dp"] or 0) > 0 else "") + cell(r["dp"], "${:,.2f}") if r["dp"] is not None else "—"
        body_rows.append((
            r["zip3"],
            r["label"] if r["reliable"] else f"{r['label']} (n={r['n_stores']})",
            r["n_stores"],
            f"{r['units_avail']:,}",
            r["upstore"],
            f"${r['med10']:,.0f}" if r["med10"] else "—",
            f"${r['med20']:,.0f}" if r["med20"] else "—",
            f"{r['promo_pct']:.0f}%" if r["promo_pct"] is not None else "—",
            du_cell, dp_cell, score_cell,
        ))

    cols = ["zip3", "market", "stores", "units avail", "units/store",
            "median 10x10", "median 10x20", "promo %", "Δ units", "Δ 10x10", "leverage"]

    # Table built inline (rather than via table_html) so each row can carry the
    # zip3 key the drill-down selects on. Everything already rendered in a cell
    # is read back out of the DOM by the detail panel instead of being shipped
    # a second time in JSON — that keeps the added payload to the genuinely new
    # fields only.
    thead = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)
    tbody = ""
    for br in body_rows:
        cells = "".join(f"<td>{html.escape(str(v if v is not None else '—'))}</td>" for v in br)
        # No data-* key and no aria-label: the zip3 is already the first cell and
        # the row's own text is what a screen reader announces. Repeating either
        # as an attribute would have cost ~24KB across 354 rows for zero
        # information gain. tabindex alone makes the row keyboard-reachable.
        tbody += f'<tr tabindex="0">{cells}</tr>'
    table = (f'<table id="mkt-table"><thead><tr>{thead}</tr></thead>'
             f'<tbody>{tbody}</tbody></table>')

    # ---- drill-down payload, shipped as a SEPARATE lazily-fetched file ----
    # Inlining this in markets.html costs every visitor ~12KB gzipped whether
    # or not they ever open a market. As a sidecar fetched on the first click,
    # the page itself gets *smaller* than the inline version and most visitors
    # never download it at all. Only the fields not already in the table markup
    # are included; the panel reads the rest back out of the row's cells.
    detail = {}
    for r in rows:
        detail[r["zip3"]] = [
            r["by_size"], r["n_size"], r["listings"], r["promo_ct"],
            market_series.get(r["zip3"], []), 1 if r["reliable"] else 0,
        ]
    payload = {
        "v": SIZE_VOCAB,
        "sq": [SIZE_SQFT.get(s) for s in SIZE_VOCAB],
        "d": dates,
        "n": [round(nat_median[s]) if nat_median.get(s) else None for s in SIZE_VOCAB],
        "min": MIN_SIZE_SAMPLE,
        "minst": MIN_STORES,
        "m": detail,
    }
    Path(DETAIL_OUT).write_text(
        json.dumps(payload, separators=(",", ":")), encoding="utf-8")

    today = __import__("datetime").date.today().strftime("%B %d, %Y")
    period_note = (f"Trend columns compare {trend_period.replace('->', '→')}."
                   if trend_period else "Trend columns fill in once a second daily snapshot exists.")

    page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Metro Markets — FindStorage</title>
<meta name="description" content="Every U.S. ZIP3 market with a tracked self-storage facility: store count, availability, pricing by size, and a renter-leverage heuristic — {len(rows):,} markets, automatically derived.">
<style>
:root{{--bg:#101418;--card:#161c22;--line:#232c35;--txt:#e8edf2;--dim:#8fa0af;--acc:#f0a44b;--bar:#2b3a47}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--txt);font-family:system-ui,'Segoe UI',sans-serif;line-height:1.6;font-size:16px}}
.wrap{{max-width:1100px;margin:0 auto;padding:0 20px}}
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
.note{{color:var(--dim);font-size:.93rem;max-width:760px;margin-bottom:16px}}
.method{{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:18px 20px;font-size:.87rem;color:var(--dim)}}
.method b{{color:var(--txt)}}
.method code{{color:var(--acc);font-family:'DM Mono',monospace,monospace;font-size:.85em}}
table{{width:100%;border-collapse:collapse;font-size:.82rem;margin-top:6px}}
th{{text-align:left;color:var(--dim);font-weight:600;padding:8px 10px;border-bottom:1px solid var(--line);
text-transform:uppercase;font-size:.66rem;letter-spacing:.06em;position:sticky;top:0;background:var(--bg)}}
td{{padding:7px 10px;border-bottom:1px solid var(--line);white-space:nowrap}}
tr:hover td{{background:var(--card)}}
.tablewrap{{overflow-x:auto;max-height:80vh;overflow-y:auto}}
.empty{{color:var(--dim);font-style:italic}}
#mkt-table tbody tr{{cursor:pointer}}
#mkt-table tbody tr.sel td{{background:#22303c;box-shadow:inset 3px 0 0 var(--acc)}}
#mkt-table tbody tr:focus{{outline:2px solid var(--acc);outline-offset:-2px}}
#mkt-search{{width:100%;max-width:380px;background:var(--card);border:1px solid var(--line);
border-radius:8px;color:var(--txt);padding:9px 12px;font-family:inherit;font-size:.9rem;margin-bottom:14px}}
#mkt-detail{{position:relative;background:var(--card);border:1px solid var(--line);
border-left:4px solid var(--acc);border-radius:8px;padding:20px 22px;margin-bottom:18px}}
#mkt-detail h3{{font-size:1.15rem;margin-bottom:4px}}
#mkt-detail h3 .z3{{color:var(--dim);font-size:.72rem;font-weight:400;letter-spacing:.08em;
text-transform:uppercase;margin-left:8px}}
#mkt-detail h4{{font-size:.82rem;color:var(--dim);text-transform:uppercase;letter-spacing:.08em;
margin:20px 0 6px}}
#mkt-detail .dgrid{{display:flex;flex-wrap:wrap;gap:12px;margin-top:12px}}
#mkt-detail .dgrid>div{{background:var(--bg);border:1px solid var(--line);border-radius:6px;
padding:10px 14px;min-width:104px}}
#mkt-detail .dn{{display:block;font-size:1.15rem;font-weight:700;color:var(--acc)}}
#mkt-detail .dl{{display:block;font-size:.68rem;color:var(--dim);text-transform:uppercase;
letter-spacing:.07em}}
#mkt-detail .dtable{{font-size:.83rem}}
#mkt-detail .dtable td,#mkt-detail .dtable th{{white-space:nowrap}}
#mkt-detail .dnote{{color:var(--dim);font-size:.83rem;margin-top:10px;max-width:640px}}
#mkt-detail .dim{{color:var(--dim);font-style:italic}}
#mkt-detail .empty{{color:var(--dim);font-style:italic;font-size:.85rem}}
.closebtn{{position:absolute;top:12px;right:14px;background:none;border:0;color:var(--dim);
font-size:1.5rem;line-height:1;cursor:pointer;padding:2px 6px}}
.closebtn:hover{{color:var(--txt)}}
footer{{padding:34px 0 50px;color:var(--dim);font-size:.85rem}}
footer a{{color:var(--acc);text-decoration:none}}
@media(max-width:600px){{table{{font-size:.72rem}}td,th{{padding:5px}}}}
</style></head><body>
<header><div class="wrap">
<h1>Metro Markets<br><span>every ZIP3 area, automatically</span></h1>
<p class="meta">Derived from the current snapshot, updated {today} · no hand-curated list — every ZIP3
prefix with a tracked store gets a row · <a href="/">directory</a> · <a href="/insights.html">insights</a> ·
<a href="/trends.html">daily trends</a> · <a href="/merger.html">merger before/after</a> ·
<a href="/repricing.html">repricing waves</a></p>
<div class="kpis">
<div class="kpi"><div class="n">{len(rows):,}</div><div class="l">ZIP3 markets</div></div>
<div class="kpi"><div class="n">{n_reliable:,}</div><div class="l">with {MIN_STORES}+ stores</div></div>
<div class="kpi"><div class="n">{total_stores:,}</div><div class="l">stores covered</div></div>
<div class="kpi"><div class="n">${nat_10x10:,.0f}</div><div class="l">national median 10x10</div></div>
</div></div></header>
<main class="wrap">

<section><h2>Methodology</h2>
<div class="method">
<p><b>Market definition:</b> a market is every distinct ZIP3 prefix (first 3 digits of a store's zip code)
that has at least one tracked store — {len(rows):,} of them nationwide, generated automatically with no
hand-curated list. Each is labeled by its dominant city/state (the city with the most stores in that ZIP3;
a second city is shown when it's close to a tie in the same state). <code>data/market_names.json</code> can
override any label by hand later — it's optional and empty by default.</p>
<p style="margin-top:10px"><b>Statistical honesty:</b> store count is always shown, but markets with fewer
than {MIN_STORES} stores have their price, promo, and leverage-score columns suppressed — a median of one
or two stores is an anecdote, not a market rate.</p>
<p style="margin-top:10px"><b>Renter Leverage Score — a HEURISTIC, not a measurement:</b> a simple 0-100
blend of three signals, equally weighted, each scaled so 50 ≈ "about average":</p>
<ul style="margin:8px 0 0 22px">
<li><code>availability</code> — units advertised per store, scaled so 40+/store = 100</li>
<li><code>promos</code> — % of listings currently carrying a promotion, used directly as 0-100</li>
<li><code>price</code> — this market's median 10x10 vs. the national median 10x10; cheaper than
national scores above 50, pricier scores below</li>
</ul>
<p style="margin-top:10px">The three are averaged into one score. 67+ is labeled "Renter-favorable" (lots
of open units and heavy promos — room to negotiate), 34-66 "Balanced", under 34 "Tight" (full-priced and
close to capacity). It's a rough signal built from public advertised data, not a substitute for calling
around — treat it as a starting point, not an appraisal.</p>
<p style="margin-top:10px">{period_note}</p>
</div>
</section>

<section><h2>All markets</h2>
<p class="note">Sorted by store count, largest first. Δ columns compare the earliest and latest available
daily snapshot. <b>Select a row</b> — or search below — for a full readout of that market.</p>
<input id="mkt-search" type="search" placeholder="Filter markets — ZIP3, city, or state…"
 aria-label="Filter markets">
<div id="mkt-detail" hidden></div>
<div class="tablewrap">{table}</div>
</section>

</main>
<script>
(function(){{
  var D = null, loading = null;
  var tbl = document.getElementById('mkt-table');
  var panel = document.getElementById('mkt-detail');
  var search = document.getElementById('mkt-search');
  var esc = function(s){{ var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }};
  var money = function(v){{ return v == null ? '—' : '$' + Number(v).toLocaleString(); }};

  function spark(series, idx, prefix){{
    var pts = [], i;
    for (i = 0; i < series.length; i++) if (series[i][idx] != null) pts.push([i, series[i][idx]]);
    if (pts.length < 2) return '<p class="empty">Only ' + pts.length +
      ' snapshot' + (pts.length === 1 ? '' : 's') +
      ' logged for this market — a trend line needs at least two.</p>';
    var vals = pts.map(function(p){{ return p[1]; }});
    var lo = Math.min.apply(null, vals), hi = Math.max.apply(null, vals);
    var span = (hi - lo) || 1, W = 460, H = 90, P = 26;
    var xy = pts.map(function(p, k){{
      var x = P + (W - 2 * P) * (pts.length > 1 ? k / (pts.length - 1) : 0);
      var y = H - P - (H - 2 * P) * ((p[1] - lo) / span);
      return x.toFixed(1) + ',' + y.toFixed(1);
    }});
    var dots = xy.map(function(c){{
      var p = c.split(',');
      return '<circle cx="' + p[0] + '" cy="' + p[1] + '" r="3" fill="#f0a44b"/>';
    }}).join('');
    return '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%;height:auto" role="img">' +
      '<polyline points="' + xy.join(' ') + '" fill="none" stroke="#f0a44b" stroke-width="2"/>' + dots +
      '<text x="' + P + '" y="12" fill="#8fa0af" font-size="11">' + prefix + hi.toLocaleString() + '</text>' +
      '<text x="' + P + '" y="' + (H - 6) + '" fill="#8fa0af" font-size="11">' + prefix + lo.toLocaleString() + '</text>' +
      '<text x="' + (W - P) + '" y="' + (H - 6) + '" fill="#8fa0af" font-size="11" text-anchor="end">' +
      D.d[0] + ' → ' + D.d[D.d.length - 1] + '</text></svg>';
  }}

  function show(tr){{
    var z = tr.cells[0].textContent.trim();
    var rec = D.m[z];
    if (!rec) return;
    var td = tr.cells;
    var bySize = rec[0], nSize = rec[1], listings = rec[2], promoCt = rec[3],
        series = rec[4], reliable = rec[5];

    var sizeRows = '', k;
    for (k = 0; k < D.v.length; k++) {{
      if (!nSize[k]) continue;
      var med = bySize[k], sq = D.sq[k], nat = D.n[k];
      var psf = (med != null && sq) ? '$' + (med / sq).toFixed(2) : '—';
      var vs = '—';
      if (med != null && nat) {{
        var pct = Math.round((med / nat - 1) * 100);
        vs = (pct > 0 ? '+' : '') + pct + '%';
      }}
      // Two distinct reasons a median is not quoted, and they must not be
      // conflated: too few stores in the market, or too few listings at
      // this size.
      var why;
      if (med != null) {{
        why = money(med);
      }} else if (!reliable) {{
        why = '<span class="dim">market has fewer than ' + D.minst + ' stores</span>';
      }} else {{
        why = '<span class="dim">n=' + nSize[k] + ', below the ' + D.min +
              '-listing minimum</span>';
      }}
      sizeRows += '<tr><td>' + esc(D.v[k]) + '</td><td>' + why +
        '</td><td>' + psf + '</td><td>' + vs + '</td><td>' + nSize[k] + '</td></tr>';
    }}
    if (!sizeRows) sizeRows = '<tr><td colspan="5" class="dim">No priced listings in this market.</td></tr>';

    var promoTxt = listings ? (Math.round(1000 * promoCt / listings) / 10) + '% of ' +
      listings.toLocaleString() + ' listings' : '—';

    panel.innerHTML =
      '<button class="closebtn" id="mkt-close" aria-label="Close market detail">×</button>' +
      '<h3>' + esc(td[1].textContent) + ' <span class="z3">ZIP3 ' + esc(z) + '</span></h3>' +
      '<div class="dgrid">' +
        '<div><span class="dn">' + esc(td[2].textContent) + '</span><span class="dl">stores</span></div>' +
        '<div><span class="dn">' + esc(td[3].textContent) + '</span><span class="dl">units advertised</span></div>' +
        '<div><span class="dn">' + esc(td[4].textContent) + '</span><span class="dl">units / store</span></div>' +
        '<div><span class="dn">' + esc(td[7].textContent) + '</span><span class="dl">promo share</span></div>' +
        '<div><span class="dn">' + esc(td[10].textContent) + '</span><span class="dl">leverage score</span></div>' +
      '</div>' +
      '<h4>Median advertised price by size</h4>' +
      '<table class="dtable"><thead><tr><th>size</th><th>median</th><th>$ / sq ft</th>' +
      '<th>vs national</th><th>listings</th></tr></thead><tbody>' + sizeRows + '</tbody></table>' +
      '<p class="dnote">Promotions: ' + promoTxt + ' currently carry one. Medians are of advertised ' +
      'online rates in the current snapshot. ' + (reliable
        ? 'Sizes with fewer than ' + D.min + ' listings are shown but not quoted — a median of one ' +
          'or two listings is an anecdote, not a market rate.'
        : 'This market has fewer than ' + D.minst + ' stores, so no median is quoted and the ' +
          'trend lines below describe those one or two sites specifically, not a market.') + '</p>' +
      '<h4>Advertised availability over time</h4>' + spark(series, 0, '') +
      '<h4>Median cheapest 10x10 over time</h4>' + spark(series, 1, '$');

    panel.hidden = false;
    Array.prototype.forEach.call(tbl.tBodies[0].rows, function(r){{ r.classList.remove('sel'); }});
    tr.classList.add('sel');
    document.getElementById('mkt-close').onclick = function(){{
      panel.hidden = true; tr.classList.remove('sel');
    }};
    if (panel.scrollIntoView) panel.scrollIntoView({{block: 'nearest', behavior: 'smooth'}});
  }}

  // The detail data is a sidecar file, fetched once on the first drill-down.
  function open_(tr){{
    if (D) return show(tr);
    panel.hidden = false;
    panel.innerHTML = '<p class="empty">Loading market detail…</p>';
    if (!loading) loading = fetch('{DETAIL_OUT}').then(function(r){{
      if (!r.ok) throw new Error(r.status);
      return r.json();
    }});
    loading.then(function(j){{ D = j; show(tr); }}).catch(function(){{
      loading = null;   // let the next click retry rather than fail forever
      panel.innerHTML = '<p class="empty">Could not load {DETAIL_OUT} — the ' +
        'market detail file did not fetch. Select the row again to retry; ' +
        'the table above is unaffected.</p>';
    }});
  }}

  tbl.addEventListener('click', function(e){{
    var tr = e.target.closest('#mkt-table tbody tr');
    if (tr) open_(tr);
  }});
  tbl.addEventListener('keydown', function(e){{
    if (e.key !== 'Enter' && e.key !== ' ') return;
    var tr = e.target.closest('#mkt-table tbody tr');
    if (tr) {{ e.preventDefault(); open_(tr); }}
  }});
  // Warm the cache on hover/focus so the first click feels instant.
  ['mouseover', 'focusin'].forEach(function(ev){{
    tbl.addEventListener(ev, function(){{
      if (!D && !loading) loading = fetch('{DETAIL_OUT}')
        .then(function(r){{ return r.json(); }})
        .catch(function(e){{ loading = null; throw e; }});
    }}, {{once: true}});
  }});
  search.addEventListener('input', function(){{
    var q = this.value.trim().toLowerCase();
    Array.prototype.forEach.call(tbl.tBodies[0].rows, function(r){{
      r.style.display = (!q || r.textContent.toLowerCase().indexOf(q) !== -1) ? '' : 'none';
    }});
  }});
}})();
</script>
<footer><div class="wrap">Data collected from publicly advertised rates. Part of
<a href="https://findstorage.netlify.app">FindStorage</a> — an independent self-storage directory.
Built with Python + SQLite.</div></footer>
</body></html>"""

    Path(OUT).write_text(page, encoding="utf-8")
    print(f"Wrote {OUT} ({len(rows):,} markets, {n_reliable:,} with {MIN_STORES}+ stores)")


if __name__ == "__main__":
    main()
