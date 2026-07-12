"""
build_trends.py — Generate trends.html from the daily history logs.

Run after update_history.py + update_rate_log.py (from the repo root):
    python analysis/build_trends.py

Reads history/*.csv (per-store snapshots), history/rate_changes.csv (per-SKU
repricing events) and the current enriched_locations.json, then writes the
daily trends report:

  · yesterday's repricing (hikes vs cuts, magnitudes, promo switches)
  · net repricing pressure by unit size
  · a chain-linked national price index (robust to coverage changes:
    each day-over-day link uses only stores present in both snapshots)
  · mavericks — stores moving against their own metro
  · momentum streaks (auto-unlocks once enough consecutive snapshots exist)
  · fastest-renting stores, restocks, biggest 10x10 moves, coverage

Pure stdlib. Charts come from viz.py (validated palette, see that file).
"""
import csv
import datetime
import html
import json
import re
import statistics
import sys
from pathlib import Path

import viz

OUT = "trends.html"
MONTH_CSV = re.compile(r"^\d{4}-\d{2}\.csv$")
MAX_LINK_GAP = 3  # days; snapshot pairs further apart than this break streaks

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

def load_rate_events():
    """date -> {'hikes': [pct..], 'cuts': [pct..], 'promo': n, 'by_size': {}}"""
    p = Path("history/rate_changes.csv")
    days = {}
    if not p.exists():
        return days
    with open(p, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            d = days.setdefault(r["date"], {"hikes": [], "cuts": [], "promo": 0,
                                            "by_size": {}})
            if r["field"] == "promo":
                d["promo"] += 1
                continue
            try:
                old, new = float(r["old"]), float(r["new"])
            except ValueError:
                continue
            if not old:
                continue
            pct = 100.0 * (new - old) / old
            (d["hikes"] if pct > 0 else d["cuts"]).append(pct)
            d["by_size"].setdefault(r["size"] or "?", []).append(pct)
    return dict(sorted(days.items()))

def gap_days(d1, d2):
    return (datetime.date.fromisoformat(d2) - datetime.date.fromisoformat(d1)).days

def chained_index(snaps, key):
    """Chain-linked index (first date = 100). Each link is the median ratio
    across stores present in BOTH snapshots, so coverage growth can't move it."""
    dates = list(snaps)
    series, idx = [(dates[0], 100.0)], 100.0
    for d1, d2 in zip(dates, dates[1:]):
        a, b = snaps[d1], snaps[d2]
        ratios = [b[s][key] / a[s][key] for s in set(a) & set(b)
                  if a[s][key] and b[s][key]]
        if ratios:
            idx *= statistics.median(ratios)
        series.append((d2, round(idx, 2)))
    return series

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
    events = load_rate_events()
    dates = list(snaps)
    latest = dates[-1]
    lat_d = datetime.date.fromisoformat(latest)

    meta = {}
    for s in json.loads(Path("enriched_locations.json").read_text(encoding="utf-8")):
        meta[str(s["store_id"])] = (s.get("site_number") or "?", s.get("address") or "",
                                    s.get("city") or "", s.get("state") or "")

    # ---------------- rate-event analytics (the daily heartbeat) ----------------
    ev_secs = ""
    kpi_extra = ""
    if events:
        ed = list(events)[-1]                       # latest event date
        e = events[ed]
        n_re = len(e["hikes"]) + len(e["cuts"])
        med_h = statistics.median(e["hikes"]) if e["hikes"] else 0
        med_c = statistics.median(e["cuts"]) if e["cuts"] else 0
        kpi_extra = (
            f"<div class='kpi'><div class='n'>{n_re:,}</div><div class='l'>units repriced ({ed})</div></div>"
            f"<div class='kpi'><div class='n'>{len(e['hikes']):,} <span class='d up'>↑</span> "
            f"{len(e['cuts']):,} <span class='d down'>↓</span></div><div class='l'>hikes vs cuts</div></div>")

        # 2 series: cuts (down) = blue slot 1; hikes (up) wear the diverging red
        day_series = [("cuts", [(d, len(v["cuts"])) for d, v in events.items()]),
                      ("hikes", [(d, len(v["hikes"])) for d, v in events.items()])]
        chart = viz.multiline(day_series, fmt="{:,.0f}").replace(viz.SERIES[1], viz.POS)

        ev_secs += f"""
<section><h2>The daily repricing pulse</h2>
<p class='note'>Every street-rate move the network made, caught by diffing consecutive daily
scrapes SKU by SKU. On {html.escape(ed)}: {n_re:,} units repriced — {len(e['hikes']):,} hikes
(median {med_h:+.1f}%) against {len(e['cuts']):,} cuts (median {med_c:+.1f}%), plus
{e['promo']:,} promotion switches. Cuts outnumber hikes but hikes run steeper — the signature
of algorithmic yield management: trim soft inventory broadly, squeeze scarce units hard.
This series gets a point every scrape day.</p>
<div class='fig'>{chart}</div></section>"""

        sizes = {}
        for v in events.values():
            for sz, pcts in v["by_size"].items():
                sizes.setdefault(sz, []).extend(pcts)
        sz_rows = [(sz, round(statistics.median(p), 1)) for sz, p in sizes.items()
                   if len(p) >= 30 and re.match(r"^\d+x\d+$", sz)]
        sz_rows.sort(key=lambda r: r[1])
        if sz_rows:
            ev_secs += f"""
<section><h2>Repricing pressure by unit size</h2>
<p class='note'>Median percentage move among repriced units of each size (sizes with 30+
events, all days pooled). Where the bar points down, the network is discounting that size;
up, it's squeezing. Watch small units drift up when demand tightens — they're the scarcest
inventory.</p>
<div class='fig'>{viz.diverging(sz_rows, fmt="{:+.1f}", suffix="%")}</div></section>"""

    # ---------------- chained national price index ----------------
    idx10 = chained_index(snaps, "ten")
    idxmed = chained_index(snaps, "med")
    long_links = sum(1 for d1, d2 in zip(dates, dates[1:]) if gap_days(d1, d2) > MAX_LINK_GAP)
    idx_note = ""
    if long_links:
        idx_note = (f" {long_links} link{'s' if long_links > 1 else ''} in this chain spans a "
                    "multi-week gap in the record (the pipeline wasn't running daily yet) — "
                    "treat the early segment as two endpoints, not a path.")
    index_chart = viz.multiline([("cheapest 10x10", idx10), ("store median", idxmed)],
                                fmt="{:,.1f}")

    # ---------------- divergence: stores vs their metro ----------------
    div_rows, div_period = [], ""
    if len(dates) >= 2:
        d1, d2 = dates[-2], dates[-1]
        div_period = f"{d1} → {d2}"
        a, b = snaps[d1], snaps[d2]
        changes = {}
        for sid in set(a) & set(b):
            if a[sid]["med"] and b[sid]["med"]:
                changes[sid] = 100.0 * (b[sid]["med"] / a[sid]["med"] - 1)
        metro_ch = {}
        for sid, ch in changes.items():
            m = meta.get(sid)
            if m and m[2]:
                metro_ch.setdefault((m[2], m[3]), []).append(ch)
        metro_med = {k: statistics.median(v) for k, v in metro_ch.items() if len(v) >= 5}
        for sid, ch in changes.items():
            m = meta.get(sid)
            key = (m[2], m[3]) if m else None
            if key in metro_med:
                div_rows.append((f"#{m[0]}", m[1], m[2], m[3],
                                 f"{ch:+.1f}%", f"{metro_med[key]:+.1f}%",
                                 ch - metro_med[key]))
        div_rows.sort(key=lambda r: -abs(r[6]))
        div_rows = [r[:6] + (f"{r[6]:+.1f}pp",) for r in div_rows if abs(r[6]) >= 1][:12]

    # ---------------- momentum streaks (unlocks with consecutive days) ----------------
    pair_ok = [gap_days(d1, d2) <= MAX_LINK_GAP for d1, d2 in zip(dates, dates[1:])]
    run = best_run = 0
    for ok in pair_ok:
        run = run + 1 if ok else 0
        best_run = max(best_run, run)
    streak_rows = []
    if best_run >= 2:
        for sid in snaps[latest]:
            streak, direction, start = 0, 0, None
            for i, ((d1, d2), ok) in enumerate(zip(zip(dates, dates[1:]), pair_ok)):
                s1, s2 = snaps[d1].get(sid), snaps[d2].get(sid)
                if not ok or not s1 or not s2 or not s1["med"] or not s2["med"] or s1["med"] == s2["med"]:
                    streak, direction, start = 0, 0, None
                    continue
                sgn = 1 if s2["med"] > s1["med"] else -1
                if sgn == direction:
                    streak += 1
                else:
                    streak, start = 1, d1
                direction = sgn
            if streak >= 2 and start:
                m = meta.get(sid, ("?", "", "", ""))
                total = 100.0 * (snaps[latest][sid]["med"] / snaps[start][sid]["med"] - 1)
                streak_rows.append((f"#{m[0]}", m[1], m[2], m[3],
                                    f"{streak} moves {'↑' if direction > 0 else '↓'}",
                                    f"{total:+.1f}%"))
        streak_rows.sort(key=lambda r: (-int(r[4].split()[0]), r[3]))
        momentum_html = table(["site #", "address", "city", "state", "streak", "total move"],
                              streak_rows[:12])
        momentum_note = ("Stores whose median price moved the same direction on consecutive "
                         "scrape days. Persistence separates deliberate repricing from daily "
                         "algorithmic wobble.")
    else:
        have = best_run + 1
        momentum_html = (f"<div class='unlock'>🔒 Unlocks at 3 consecutive daily snapshots — "
                         f"{have} banked so far. The daily pipeline fills this in automatically; "
                         "streaks of same-direction price moves will appear here.</div>")
        momentum_note = ("The section that needs a streak of its own: day-over-day persistence "
                         "can't be computed until several uninterrupted daily snapshots exist.")

    # ---------------- movers / hikes / cuts / coverage (period tables) ----------------
    base = next((d for d in reversed(dates[:-1])
                 if (lat_d - datetime.date.fromisoformat(d)).days >= 7), dates[0] if len(dates) > 1 else None)
    movers = restock = hikes = cuts = []
    added = removed = []
    period = ""
    if base:
        b, l = snaps[base], snaps[latest]
        period = f"{base} → {latest} ({gap_days(base, latest)} days)"
        common = set(b) & set(l)
        def row(sid, val):
            m = meta.get(sid, ("?", "", "", ""))
            return (f"#{m[0]}", m[1], m[2], m[3], val)
        deltas = sorted(((sid, b[sid]["units"] - l[sid]["units"]) for sid in common),
                        key=lambda t: -t[1])
        movers = [row(s, f"{b[s]['units']} → {l[s]['units']}  (−{d})") for s, d in deltas if d > 0][:15]
        restock = [row(s, f"{b[s]['units']} → {l[s]['units']}  (+{-d})")
                   for s, d in sorted(deltas, key=lambda t: t[1]) if d < 0][:10]
        pdeltas = [(s, b[s]["ten"], l[s]["ten"], l[s]["ten"] - b[s]["ten"])
                   for s in common if b[s]["ten"] and l[s]["ten"] and l[s]["ten"] != b[s]["ten"]]
        hikes = [row(s, f"${o:,.0f} → ${n:,.0f}  (+${d:,.0f})")
                 for s, o, n, d in sorted(pdeltas, key=lambda t: -t[3]) if d > 0][:10]
        cuts = [row(s, f"${o:,.0f} → ${n:,.0f}  (−${-d:,.0f})")
                for s, o, n, d in sorted(pdeltas, key=lambda t: t[3]) if d < 0][:10]
        added = sorted(set(l) - set(b), key=int)
        removed = sorted(set(b) - set(l), key=int)

    units_series = [(d, sum(v["units"] for v in s.values())) for d, s in snaps.items()]

    mover_cols = ["site #", "address", "city", "state", "available units"]
    price_cols = ["site #", "address", "city", "state", "cheapest 10x10"]
    div_cols = ["site #", "address", "city", "state", "store Δ", "metro Δ", "divergence"]

    today = datetime.date.today().strftime("%B %d, %Y")
    total_now = units_series[-1][1]
    tens_now = [v["ten"] for v in snaps[latest].values() if v["ten"]]
    med_now = statistics.median(tens_now) if tens_now else 0

    sections = ev_secs + f"""
<section><h2>The national price index</h2>
<p class='note'>Advertised price level indexed to the first snapshot = 100. Each day-over-day
link uses only stores present in both snapshots (median ratio), so the index measures price
movement — not the tracker's own coverage growing from 3,092 to {len(snaps[latest]):,}
stores.{html.escape(idx_note)}</p>
<div class='fig'>{index_chart}</div></section>

<section><h2>National advertised inventory</h2>
<p class='note'>Total units marked rentable across the network, per snapshot. Counts are what
the website advertises — revenue management holds back part of the physically vacant
inventory. This is the raw total, so coverage growth shows up in it; the price index above is
the coverage-corrected series.</p>
<div class='fig'>{viz.multiline([("units available", units_series)], fmt="{:,.0f}")}</div></section>

<section><h2>Mavericks — stores moving against their market</h2>
<p class='note'>Stores whose median price moved differently from their own metro's median move
({html.escape(div_period)}, metros with 5+ tracked stores, divergence ≥ 1 percentage point).
A store hiking while its market cuts is the most information-dense row in this dataset:
something changed at that address.</p>
{table(div_cols, div_rows, 12)}</section>

<section><h2>Momentum — multi-day repricing streaks</h2>
<p class='note'>{momentum_note}</p>
{momentum_html}</section>

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
<meta name="description" content="Daily-updated self-storage trends: every price move in the network, a chain-linked price index, repricing pressure by size, and stores moving against their market.">
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
{viz.CSS}
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
{kpi_extra}
</div></div></header>
<main class="wrap">{sections}</main>
<footer><div class="wrap">History begins April 29, 2026; snapshots accumulate daily. Part of
<a href="https://findstorage.netlify.app">FindStorage</a> — an independent self-storage directory.
Built with Python + SQLite.</div></footer>
</body></html>"""

    Path(OUT).write_text(page, encoding="utf-8")
    print(f"Wrote {OUT} ({len(dates)} snapshots, {len(events)} event days, "
          f"{len(div_rows)} mavericks, momentum {'live' if streak_rows else 'locked'})")

if __name__ == "__main__":
    main()
