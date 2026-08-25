"""
build_merger_report.py — Generate merger.html: a per-ZIP3 comparison of
advertised store count and Renter Leverage Score bracketing a specific
market event (Public Storage's integration of National Storage Affiliates
inventory), computed from two pinned historical snapshots already committed
to this repo's git history.

Run manually from the repo root:
    python analysis/build_merger_report.py

BEFORE_REF / AFTER_REF are git revisions, each read via
`git show <ref>:enriched_locations.json`. Defaults bracket the observed
inventory jump: d3ed020 (2026-07-19, 3,539 stores) -> ca0e21b (2026-07-24,
4,637 stores).

This is a ONE-TIME computed page, not wired into the daily workflow. Two
reasons:
  1. The event is a fixed historical bracket (two specific dates), not an
     ongoing daily comparison — regenerating it every day would just
     re-plot the same two endpoints forever.
  2. The daily GitHub Actions job checks out with the default shallow clone
     (actions/checkout@v4, fetch-depth omitted = depth 1), so a historical
     commit like d3ed020 isn't available to it without a separate
     fetch-depth change to the workflow. Re-run this manually (with a full
     local clone, as this one is) if the reference commits ever need to
     move — e.g. to compare a different event.

Wording note: this describes an OBSERVED change in advertised inventory and
pricing signals across a public market event. It infers no operator intent
and draws no cause-and-effect conclusion. Leverage score carries the same
heuristic disclaimer as on markets.html: a rough signal from public
advertised data, not a measurement.
"""
import html
import json
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))
from analyze_storage import median  # noqa: E402
from build_markets import leverage_score, tier, dominant_label, MIN_STORES, MIN_SIZE_SAMPLE  # noqa: E402

OUT = "merger.html"
BEFORE_REF = "d3ed020"
AFTER_REF = "ca0e21b"
BEFORE_LABEL = "July 19, 2026"
AFTER_LABEL = "July 24, 2026"


def git_show_json(ref):
    out = subprocess.run(["git", "show", f"{ref}:enriched_locations.json"],
                          capture_output=True, check=True)
    return json.loads(out.stdout.decode("utf-8"))


def zip3_stats(stores):
    """Per-ZIP3 store count, dominant label, and Renter Leverage Score for one
    snapshot — the same computation build_markets.py does, but over a parsed
    JSON list instead of storage.db, since a historical snapshot has no
    corresponding database."""
    mkt = defaultdict(lambda: {"stores": set(), "city_state": Counter(),
                                "units_avail": 0, "listings": 0, "promo": 0,
                                "prices_by_size": defaultdict(list)})
    nat_by_size = defaultdict(list)
    for s in stores:
        sid = s.get("store_id")
        zp = s.get("zip") or ""
        z3 = zp[:3]
        valid_zip = z3.isdigit() and len(z3) == 3
        if valid_zip:
            m = mkt[z3]
            m["stores"].add(sid)
            city, state = s.get("city"), s.get("state")
            if city and state:
                m["city_state"][(city, state)] += 1
        for u in s.get("units", []):
            if not (u.get("available") and u.get("price") is not None and u.get("size")):
                continue
            nat_by_size[u["size"]].append(u["price"])
            if valid_zip:
                mkt[z3]["units_avail"] += u.get("count") or 0
                mkt[z3]["listings"] += 1
                if u.get("promo"):
                    mkt[z3]["promo"] += 1
                mkt[z3]["prices_by_size"][u["size"]].append(u["price"])

    nat_10x10 = median(nat_by_size["10x10"]) if nat_by_size.get("10x10") else None

    rows = {}
    for z3, m in mkt.items():
        n_stores = len(m["stores"])
        reliable = n_stores >= MIN_STORES
        label = dominant_label(m["city_state"])
        upstore = m["units_avail"] / n_stores if n_stores else 0
        score = med10 = promo_pct = None
        if reliable:
            promo_pct = round(100.0 * m["promo"] / m["listings"], 1) if m["listings"] else None
            pbs = m["prices_by_size"]
            med10 = median(pbs["10x10"]) if len(pbs.get("10x10", [])) >= MIN_SIZE_SAMPLE else None
            if med10 and nat_10x10 and promo_pct is not None:
                score = leverage_score(upstore, promo_pct, med10 / nat_10x10)
        rows[z3] = {"label": label, "n_stores": n_stores, "reliable": reliable, "score": score}
    return rows, nat_10x10


def main():
    print(f"Reading {BEFORE_REF}:enriched_locations.json ({BEFORE_LABEL})...")
    before_stores = git_show_json(BEFORE_REF)
    print(f"Reading {AFTER_REF}:enriched_locations.json ({AFTER_LABEL})...")
    after_stores = git_show_json(AFTER_REF)

    before, nat_before = zip3_stats(before_stores)
    after, nat_after = zip3_stats(after_stores)

    all_z3 = set(before) | set(after)
    combined = []
    for z3 in all_z3:
        b = before.get(z3, {"label": "", "n_stores": 0, "reliable": False, "score": None})
        a = after.get(z3, {"label": "", "n_stores": 0, "reliable": False, "score": None})
        label = a["label"] or b["label"]
        d_stores = a["n_stores"] - b["n_stores"]
        d_score = (a["score"] - b["score"]) if (a["score"] is not None and b["score"] is not None) else None
        combined.append({
            "zip3": z3, "label": label,
            "n_before": b["n_stores"], "n_after": a["n_stores"], "d_stores": d_stores,
            "score_before": b["score"], "score_after": a["score"], "d_score": d_score,
            "new_market": b["n_stores"] == 0 and a["n_stores"] > 0,
        })

    total_before = sum(r["n_before"] for r in combined)
    total_after = sum(r["n_after"] for r in combined)
    n_new_markets = sum(1 for r in combined if r["new_market"])
    n_both_scored = sum(1 for r in combined if r["d_score"] is not None)

    by_store_delta = sorted(combined, key=lambda r: -abs(r["d_stores"]))[:30]
    by_score_delta = sorted([r for r in combined if r["d_score"] is not None],
                             key=lambda r: -abs(r["d_score"]))[:20]

    def cell(v, fmt="{:,.0f}", dash="—"):
        return dash if v is None else fmt.format(v)

    def store_row(r):
        sign = "+" if r["d_stores"] > 0 else ""
        tag = " (new)" if r["new_market"] else ""
        return (r["zip3"], html.escape(r["label"]) + tag,
                r["n_before"], r["n_after"], f"{sign}{r['d_stores']:,}")

    def score_row(r):
        sign = "+" if r["d_score"] > 0 else ""
        b_cell = f"{r['score_before']} · {tier(r['score_before'])}"
        a_cell = f"{r['score_after']} · {tier(r['score_after'])}"
        return (r["zip3"], html.escape(r["label"]), b_cell, a_cell, f"{sign}{r['d_score']:.0f}")

    store_thead = "".join(f"<th>{c}</th>" for c in
        ["zip3", "market", f"stores {BEFORE_LABEL}", f"stores {AFTER_LABEL}", "Δ stores"])
    store_tbody = "".join(
        "<tr>" + "".join(f"<td>{v}</td>" for v in store_row(r)) + "</tr>" for r in by_store_delta)

    score_thead = "".join(f"<th>{c}</th>" for c in
        ["zip3", "market", f"leverage {BEFORE_LABEL}", f"leverage {AFTER_LABEL}", "Δ score"])
    score_tbody = "".join(
        "<tr>" + "".join(f"<td>{v}</td>" for v in score_row(r)) + "</tr>" for r in by_score_delta)
    score_section = (f'<table><thead><tr>{score_thead}</tr></thead><tbody>{score_tbody}</tbody></table>'
                      if by_score_delta else
                      '<p class="empty">No ZIP3 market had a reliable leverage score (3+ stores, '
                      f'enough size-priced listings) on both {BEFORE_LABEL} and {AFTER_LABEL}.</p>')

    page = f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Merger Before/After — FindStorage</title>
<meta name="description" content="Per-ZIP3 change in advertised store count and Renter Leverage Score across a public market event, computed from two committed dataset snapshots.">
<style>
:root{{--bg:#101418;--card:#161c22;--line:#232c35;--txt:#e8edf2;--dim:#8fa0af;--acc:#f0a44b}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--txt);font-family:system-ui,'Segoe UI',sans-serif;line-height:1.6;font-size:16px}}
.wrap{{max-width:980px;margin:0 auto;padding:0 20px}}
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
table{{width:100%;border-collapse:collapse;font-size:.85rem;margin-top:6px}}
th{{text-align:left;color:var(--dim);font-weight:600;padding:8px 10px;border-bottom:1px solid var(--line);
text-transform:uppercase;font-size:.66rem;letter-spacing:.06em}}
td{{padding:7px 10px;border-bottom:1px solid var(--line);white-space:nowrap}}
tr:hover td{{background:var(--card)}}
.tablewrap{{overflow-x:auto}}
.empty{{color:var(--dim);font-style:italic}}
footer{{padding:34px 0 50px;color:var(--dim);font-size:.85rem}}
footer a{{color:var(--acc);text-decoration:none}}
@media(max-width:600px){{table{{font-size:.72rem}}td,th{{padding:5px}}}}
</style></head><body>
<header><div class="wrap">
<h1>Merger Before/After<br><span>advertised inventory across a market event</span></h1>
<p class="meta">Comparing two committed snapshots: {BEFORE_LABEL} vs. {AFTER_LABEL} ·
<a href="/">directory</a> · <a href="/insights.html">insights</a> · <a href="/trends.html">daily trends</a> ·
<a href="/markets.html">metro markets</a> · <a href="/repricing.html">repricing waves</a></p>
<div class="kpis">
<div class="kpi"><div class="n">{total_before:,}</div><div class="l">stores, {BEFORE_LABEL}</div></div>
<div class="kpi"><div class="n">{total_after:,}</div><div class="l">stores, {AFTER_LABEL}</div></div>
<div class="kpi"><div class="n">+{total_after-total_before:,}</div><div class="l">net change</div></div>
<div class="kpi"><div class="n">{n_new_markets:,}</div><div class="l">ZIP3 markets newly tracked</div></div>
</div></div></header>
<main class="wrap">

<section><h2>What this is</h2>
<div class="method">
<p><b>Observed data, not an interpretation:</b> the dataset this site tracks grew from
{total_before:,} to {total_after:,} advertised stores between {BEFORE_LABEL} and {AFTER_LABEL} — consistent
with the publicly reported integration of National Storage Affiliates (NSA) inventory into the tracked
operator's site. This page reports the resulting change in advertised store count and pricing signals, market by
market. It draws no conclusion about operator intent and makes no claim about why any individual store's
numbers moved — it measures what changed in the two snapshots and nothing more.</p>
<p style="margin-top:10px"><b>Method:</b> each snapshot is grouped by ZIP3 (first 3 digits of a store's zip)
exactly as on <code>/markets.html</code>, using the same {MIN_STORES}-store reliability threshold and the
same Renter Leverage Score heuristic (0-100, 50 ≈ average — see markets.html for the full methodology and
its caveats). A "new market" is a ZIP3 with zero tracked stores on {BEFORE_LABEL} and at least one on
{AFTER_LABEL} — that can reflect newly-integrated stores, improved discovery, or both; the two aren't
distinguishable from this data alone.</p>
<p style="margin-top:10px">This is a one-time computed comparison of two fixed dates, not a daily-refreshing
page — see the header link back to <a href="/markets.html">metro markets</a> for the current, live figures.</p>
</div>
</section>

<section><h2>Markets that changed most by store count</h2>
<p class="note">Every ZIP3 market with a tracked store on either date, ranked by the size of the change —
gains and losses both. Top 30 shown.</p>
<div class="tablewrap"><table><thead><tr>{store_thead}</tr></thead><tbody>{store_tbody}</tbody></table></div>
</section>

<section><h2>Leverage-score shift</h2>
<p class="note">Markets with a reliable leverage score ({MIN_STORES}+ stores, enough priced 10x10 listings)
on <b>both</b> dates, ranked by the size of the shift. A heuristic signal, not a measurement — see
markets.html for what feeds it.</p>
{score_section}
</section>

</main>
<footer><div class="wrap">Data collected from publicly advertised rates. Part of
<a href="https://findstorage.netlify.app">FindStorage</a> — an independent self-storage directory.
Built with Python + SQLite.</div></footer>
</body></html>"""

    Path(OUT).write_text(page, encoding="utf-8")
    print(f"Wrote {OUT} ({len(combined):,} ZIP3 markets compared, "
          f"{total_before:,} -> {total_after:,} stores, {n_both_scored:,} scored on both dates)")


if __name__ == "__main__":
    main()
