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
STORE_SIZE_CSV = re.compile(r"^store-sizes-\d{4}-\d{2}\.csv$")
SIZE_ORDER = ["Locker", "5x5", "5x10", "5x15", "10x10", "10x15", "10x20", "10x25", "10x30", "Parking"]

# ---------------------------------------------------------------- movers guards
# The movers tables used to rank a single snapshot against the one beside it. That
# ranking is dominated by collection artifacts rather than by the market: a store's
# first day in the dataset has no prior reading, so its "change" is invented; a
# placeholder price differences against a real one; and a listing that stops being
# published reads as a price collapse. All three produce larger numbers than any
# real repricing, which put the least reliable rows at the top of the page.
#
# Four guards, each derived from the observed data rather than hardcoded, and none
# of them silently dropping a row — every exclusion is written out with its reason
# and the values that triggered it (see Quarantine, and the same pattern in the
# facility-discovery pipeline's out/quarantine).
WINDOW = 7                  # snapshots per comparison window; cadence is daily,
                            # so a window is one week and the comparison is
                            # week-over-week rather than day-over-day.
MIN_OBS = 6                 # real observations required inside a WINDOW. At 6 of
                            # 7 a median is taken over >=6 points, so no single
                            # bad scrape can move it.
BASELINE_N = 2 * WINDOW      # consecutive snapshots a listing must carry a real
                            # price across before it may appear at all. Spans
                            # both windows: a store that just entered the dataset
                            # cannot satisfy it.
P_LO, P_HI = 0.005, 0.995   # plausible-price percentiles, taken per unit size

# The date the tracked population changed size. Detected from the data rather than
# asserted (see detect_composition_break); this is only the floor on how large a
# one-day change in store count has to be to count as a change in the population
# rather than ordinary discovery drift.
BREAK_MIN_STORES = 250

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

# ------------------------------------------------------- PSA share price series
# history/psa-stock.csv holds official daily closes, trading days only. The
# scraper publishes every calendar day, so roughly two snapshot dates in seven
# have no close of their own. To let the two series be read against each other
# on one x-axis, each snapshot date is given the most recent close on or before
# it — and the chart then has to make the repeats visible, because a carried
# value drawn identically to a fresh one is a claim about a day the market
# never priced.
MAX_CARRY_DAYS = 5          # a close older than this is not carried onto a
                            # snapshot date; the point is left blank instead. 5
                            # spans the longest ordinary US market closure (a
                            # holiday Monday: Friday's close read on Tuesday),
                            # so weekends and holidays bridge normally while a
                            # feed that has actually stopped updating decays to
                            # a gap within a week rather than a flat line that
                            # reads as an unmoving share price.

def load_stock_history():
    """[(date, close)] from history/psa-stock.csv, or [] if it doesn't exist
    yet (e.g. update_stock.py hasn't run or its fetch failed on this run)."""
    p = Path("history/psa-stock.csv")
    if not p.exists():
        return []
    out = []
    with open(p, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                out.append((row["date"], float(row["close"])))
            except (TypeError, ValueError):
                continue
    return sorted(out)

def align_stock_to_snapshots(closes, snap_dates, max_carry=MAX_CARRY_DAYS):
    """Put the close series onto the scraper's snapshot dates.

    Returns [(snapshot_date, close_or_None, as_of_date_or_None, is_fresh)], one
    entry per snapshot date so the x-axis matches the charts above it.

    is_fresh is True when this row is the first snapshot date to show that
    particular close, and False when the same close has already been plotted on
    an earlier snapshot date — i.e. no new close had landed since. That is the
    distinction the chart draws, and it is the one that survives the timing of
    the run: the daily job fires at 11:00 UTC, before the 16:00 ET close, so the
    newest completed close available on snapshot date D is normally D-1's. Keying
    "fresh" off same-calendar-day would therefore mark almost every point stale
    and tell the reader nothing. Keying it off "have we drawn this close before"
    marks exactly the weekend and holiday repeats.
    """
    if not closes:
        return []
    dates = [datetime.date.fromisoformat(d) for d, _ in closes]
    vals = [v for _, v in closes]
    out = []
    seen = set()
    i = 0
    for sd in snap_dates:
        d = datetime.date.fromisoformat(sd)
        while i < len(dates) - 1 and dates[i + 1] <= d:
            i += 1
        if dates[i] > d or (d - dates[i]).days > max_carry:
            out.append((sd, None, None, False))     # nothing usable for this date
            continue
        as_of = dates[i].isoformat()
        out.append((sd, vals[i], as_of, as_of not in seen))
        seen.add(as_of)
    return out

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

def load_store_sizes():
    """(store_id, size) -> {date: (price, advertised_units)} from
    history/store-sizes-YYYY-MM.csv.

    This is the movers tables' source because it carries LEVELS. The rate-change
    log (rate_changes.csv) records only deltas, and a delta can be judged solely
    against the value beside it — a placeholder differenced against a real price
    is indistinguishable from a repricing. With a level series each listing brings
    its own history, so a single bad reading can be outvoted by the ones around
    it, and "this listing has no prior at all" becomes a fact we can check rather
    than a change we compute.

    Granularity is (store, size). That is exactly what the tables display — they
    have always shown site and size, never an individual unit id — and it folds
    the several same-size units at one store into one listing, which is one fewer
    way for the same site to occupy four rows of a ten-row table."""
    panel = {}
    for p in sorted(Path("history").glob("store-sizes-*.csv")):
        if not STORE_SIZE_CSV.match(p.name):
            continue
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                price = row["price"].strip()
                panel.setdefault((row["store_id"], row["size"]), {})[row["date"]] = (
                    float(price) if price else None,
                    int(row["available"] or 0),
                )
    return panel


def percentile(vals_sorted, q):
    """Linear-interpolated percentile of an already-sorted list."""
    if not vals_sorted:
        return None
    k = (len(vals_sorted) - 1) * q
    f = int(k)
    c = min(f + 1, len(vals_sorted) - 1)
    return vals_sorted[f] + (vals_sorted[c] - vals_sorted[f]) * (k - f)


def price_bounds(panel):
    """size -> (low, high) plausible advertised price, from the observed
    distribution of that size and nothing else.

    A 10x10 at $1,438 is a placeholder, not a price, but the number that makes
    it one is a property of the 10x10 distribution rather than a figure anybody
    should be typing into this file. Percentile-based so the bounds move when the
    market does, and per size because $600 is unremarkable for a 10x30 and
    impossible for a Locker."""
    bysize = {}
    for (_sid, sz), series in panel.items():
        for price, _av in series.values():
            if price and price > 0:
                bysize.setdefault(sz, []).append(price)
    out = {}
    for sz, vals in bysize.items():
        vals.sort()
        out[sz] = (percentile(vals, P_LO), percentile(vals, P_HI), len(vals))
    return out


class Quarantine:
    """Excluded rows go here, never to the floor.

    Same contract as the facility-discovery pipeline's out/quarantine: a row that
    does not ship is written out IN FULL alongside the reason it did not and the
    values that triggered it, the counts are printed by the build, and the total
    is stated on the page. "We filtered N listings as suspected artifacts" is then
    a number a reader can check rather than a silence they cannot see.

    One primary reason per row, assigned by the first guard that fires, because a
    row excluded for four reasons at once is still most usefully described by the
    first fact that disqualified it. Collapsing distinct reasons into one bucket is
    what makes an exclusion count untrustworthy, so the reasons stay separate."""

    def __init__(self):
        self.rows = []

    def add(self, scope, site, size, reason, detail):
        self.rows.append({"scope": scope, "site_number": site, "size": size,
                          "reason": reason, "detail": detail})

    def counts(self, scope=None):
        c = {}
        for r in self.rows:
            if scope and r["scope"] != scope:
                continue
            c[r["reason"]] = c.get(r["reason"], 0) + 1
        return dict(sorted(c.items(), key=lambda kv: -kv[1]))

    def __len__(self):
        return len(self.rows)

    def write(self, path):
        """Always written, including when it is empty — an absent file and a
        clean run are different facts and should not look the same on disk."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, ["scope", "site_number", "size", "reason", "detail"])
            w.writeheader()
            w.writerows(self.rows)
        return p


def windows(dates):
    """(prior, recent) — two adjacent WINDOW-length blocks at the end of dates,
    or (None, None) if the log is not yet BASELINE_N snapshots long."""
    if len(dates) < BASELINE_N:
        return None, None
    return dates[-BASELINE_N:-WINDOW], dates[-WINDOW:]


def price_movers(panel, dates, stores_by_date, bounds, quarantine):
    """Week-over-week change in a listing's TRAILING MEDIAN price.

    Returns (moves, unadvertised). moves are (site, size, prior_med, recent_med,
    delta, pct); unadvertised are listings that left the size feed and are a
    disappearance rather than a repricing, so they get their own table.

    Comparing medians rather than endpoints is the structural fix: a single bad
    scrape lands in a window of seven and is outvoted, where against one adjacent
    snapshot it was the entire signal."""
    prior, recent = windows(dates)
    if not prior:
        return [], []
    span = prior + recent
    moves, unadvertised = [], []
    for (sid, sz), series in sorted(panel.items()):
        site = STORE_SITE.get(sid, "?")
        lo, hi, _n = bounds.get(sz, (0.0, float("inf"), 0))

        # 1. The store itself has to have been tracked across the whole span.
        #    A store that entered the dataset partway through has no prior for
        #    anything, and differencing against a value that was never observed
        #    is arithmetic on a number nobody collected.
        missing = [d for d in span if sid not in stores_by_date.get(d, ())]
        if missing:
            quarantine.add("price", site, sz, "store_not_tracked_through_window",
                           f"store absent from {len(missing)} of the {BASELINE_N} "
                           f"snapshots {span[0]}..{span[-1]} (first gap {missing[0]})")
            continue

        priced_prior = [series[d][0] for d in prior if d in series and series[d][0]]
        priced_recent = [series[d][0] for d in recent if d in series and series[d][0]]

        # 2. A real price across the baseline. Checked on the prior window first,
        #    because "no baseline" and "stopped being listed" are different events
        #    and the one that comes first in time is the one that describes the row.
        if len(priced_prior) < MIN_OBS:
            quarantine.add("price", site, sz, "no_baseline",
                           f"{len(priced_prior)} of {WINDOW} snapshots priced in "
                           f"{prior[0]}..{prior[-1]}; needs {MIN_OBS}")
            continue

        # 3. Availability, not price. A listing that had units advertised and now
        #    has none has disappeared; that is not a repricing and does not belong
        #    in a table of them.
        if len(priced_recent) < MIN_OBS:
            prior_units = [series[d][1] for d in prior if d in series]
            med_units = statistics.median(prior_units) if prior_units else 0
            unadvertised.append((site, sz, med_units, len(priced_recent)))
            quarantine.add("price", site, sz, "no_longer_advertised",
                           f"{len(priced_recent)} of {WINDOW} snapshots priced in "
                           f"{recent[0]}..{recent[-1]} after {len(priced_prior)} of "
                           f"{WINDOW} in the prior week; {med_units:,.0f} units were "
                           f"advertised before it left the feed")
            continue

        # 4. Plausible price. Applied to every observation in the span, not just
        #    the endpoints: one placeholder anywhere in the series is enough to
        #    say the series cannot be trusted to a dollar.
        bad = [p for p in priced_prior + priced_recent if p < lo or p > hi]
        if bad:
            quarantine.add("price", site, sz, "price_out_of_bounds",
                           f"{sz} observed at ${min(bad):,.0f}"
                           + (f"-${max(bad):,.0f}" if max(bad) != min(bad) else "")
                           + f"; plausible range for {sz} is ${lo:,.0f}-${hi:,.0f} "
                           f"(p{P_LO*100:g}-p{P_HI*100:g} of observed)")
            continue

        pm = statistics.median(priced_prior)
        rm = statistics.median(priced_recent)
        if pm <= 0 or rm == pm:
            continue
        moves.append((site, sz, pm, rm, rm - pm, 100.0 * (rm - pm) / pm))
    return moves, unadvertised


def unit_movers(snaps, dates, quarantine):
    """Week-over-week change in a store's TRAILING MEDIAN advertised unit count,
    and separately the stores that stopped advertising any availability at all.

    A store reading exactly zero after a week in the hundreds has stopped
    publishing, not rented out; it is the largest number in the table and the
    least likely to mean what the table says it means."""
    prior, recent = windows(dates)
    if not prior:
        return [], [], []
    span = prior + recent
    declines, rises, stopped = [], [], []
    for sid in sorted(snaps[dates[-1]], key=int):
        site = STORE_SITE.get(sid, "?")
        missing = [d for d in span if sid not in snaps.get(d, {})]
        if missing:
            quarantine.add("availability", site, "", "store_not_tracked_through_window",
                           f"store absent from {len(missing)} of the {BASELINE_N} "
                           f"snapshots {span[0]}..{span[-1]} (first gap {missing[0]})")
            continue
        pu = [snaps[d][sid]["units"] for d in prior]
        ru = [snaps[d][sid]["units"] for d in recent]
        pm, rm = statistics.median(pu), statistics.median(ru)
        # Zero on the majority of the recent window, having advertised before it.
        # Tested on the median rather than on every snapshot in the window, so a
        # store that goes to zero partway through and stays there is caught on the
        # day it crosses instead of a week later.
        if pm > 0 and rm == 0:
            n_zero = len([d for d in recent if snaps[d][sid]["units"] == 0])
            stopped.append((site, pm, n_zero))
            quarantine.add("availability", site, "", "stopped_advertising_availability",
                           f"median {pm:,.0f} units advertised in {prior[0]}..{prior[-1]}, then 0 "
                           f"on {n_zero} of the {WINDOW} snapshots {recent[0]}..{recent[-1]} "
                           f"(median 0) — a store that stopped publishing availability, which is "
                           f"not the same event as units being rented")
            continue
        # The mirror of the same event. A store whose median was zero and now
        # advertises stock did not restock — it started publishing. Ranked as an
        # increase it is the largest one on the page, for the same bad reason.
        if pm == 0 and rm > 0:
            quarantine.add("availability", site, "", "started_advertising_availability",
                           f"median 0 units advertised in {prior[0]}..{prior[-1]}, then "
                           f"{rm:,.0f} in {recent[0]}..{recent[-1]} — a store that began "
                           f"publishing availability, which is not the same event as units "
                           f"being returned to stock")
            continue
        if rm < pm:
            declines.append((site, pm, rm, pm - rm))
        elif rm > pm:
            rises.append((site, pm, rm, rm - pm))
    return declines, rises, stopped


def ten_movers(snaps, dates, bounds, quarantine):
    """Week-over-week change in the trailing median of a store's cheapest 10x10,
    under the same baseline and plausible-range guards as the per-size table."""
    prior, recent = windows(dates)
    if not prior:
        return []
    span = prior + recent
    lo, hi, _n = bounds.get("10x10", (0.0, float("inf"), 0))
    out = []
    for sid in sorted(snaps[dates[-1]], key=int):
        site = STORE_SITE.get(sid, "?")
        missing = [d for d in span if sid not in snaps.get(d, {})]
        if missing:
            quarantine.add("10x10", site, "10x10", "store_not_tracked_through_window",
                           f"store absent from {len(missing)} of the {BASELINE_N} "
                           f"snapshots {span[0]}..{span[-1]} (first gap {missing[0]})")
            continue
        pt = [snaps[d][sid]["ten"] for d in prior if snaps[d][sid]["ten"]]
        rt = [snaps[d][sid]["ten"] for d in recent if snaps[d][sid]["ten"]]
        if len(pt) < MIN_OBS:
            quarantine.add("10x10", site, "10x10", "no_baseline",
                           f"{len(pt)} of {WINDOW} snapshots carried a 10x10 price in "
                           f"{prior[0]}..{prior[-1]}; needs {MIN_OBS}")
            continue
        if len(rt) < MIN_OBS:
            quarantine.add("10x10", site, "10x10", "no_longer_advertised",
                           f"{len(rt)} of {WINDOW} snapshots carried a 10x10 price in "
                           f"{recent[0]}..{recent[-1]}; the store stopped advertising a "
                           f"10x10 rather than repricing one")
            continue
        bad = [p for p in pt + rt if p < lo or p > hi]
        if bad:
            quarantine.add("10x10", site, "10x10", "price_out_of_bounds",
                           f"10x10 observed at ${min(bad):,.0f}"
                           + (f"-${max(bad):,.0f}" if max(bad) != min(bad) else "")
                           + f"; plausible range is ${lo:,.0f}-${hi:,.0f} "
                           f"(p{P_LO*100:g}-p{P_HI*100:g} of observed)")
            continue
        pm, rm = statistics.median(pt), statistics.median(rt)
        if pm <= 0 or rm == pm:
            continue
        out.append((site, pm, rm, rm - pm, 100.0 * (rm - pm) / pm))
    return out


def detect_composition_break(snaps, dates):
    """Dates where the tracked population itself changed size, read off the data.

    Returns [(date, n_entered, stores_before, stores_after)]. A step of this size
    in the number of stores tracked is a change in what is being counted, so the
    series either side of it is measuring two different populations and the charts
    have to say so. Discovery adds a handful of stores most days; the threshold
    separates that from a step change and is the only judgement here."""
    out = []
    for i in range(1, len(dates)):
        before, after = snaps[dates[i - 1]], snaps[dates[i]]
        entered = len(set(after) - set(before))
        if entered >= BREAK_MIN_STORES:
            out.append((dates[i], entered, len(before), len(after)))
    return out


def svg_line(series, fmt="{:,.0f}", prefix="", breaks=()):
    """series: [(date, value)] -> responsive SVG line chart.

    breaks: dates at which the series stops being a like-for-like comparison. Drawn
    as a labelled rule rather than mentioned in prose underneath, because the
    misreading being prevented here is someone glancing at the shape of the line."""
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

    # A rule through the plot at every break, labelled on the chart itself. The
    # reader who takes nothing from this section but the shape of the line still
    # cannot come away thinking the step was the market moving.
    rules = ""
    idx = {d: i for i, (d, _v) in enumerate(series)}
    for bd in breaks:
        if bd not in idx:
            continue
        bx = x(idx[bd])
        anchor = "end" if bx > W / 2 else "start"
        tx = bx - 6 if anchor == "end" else bx + 6
        rules += (
            f"<line x1='{bx:.1f}' y1='{PAD-14}' x2='{bx:.1f}' y2='{H-PAD}' "
            f"stroke='#6b8ea8' stroke-width='1.5' stroke-dasharray='5 4'/>"
            f"<text x='{tx:.1f}' y='{PAD-18}' fill='#9fc0d8' font-size='11' "
            f"text-anchor='{anchor}'>{bd} · tracked population changed; "
            f"not comparable across this line</text>"
        )
    return f"""<svg viewBox="0 0 {W} {H}" role="img" style="width:100%;height:auto">
<line x1="{PAD}" y1="{H-PAD}" x2="{W-PAD}" y2="{H-PAD}" stroke="#232c35"/>
{rules}<polyline points="{pts}" fill="none" stroke="#f0a44b" stroke-width="2.5"/>{dots}
<text x="{PAD}" y="16" fill="#8fa0af" font-size="12">{prefix}{fmt.format(hi)}</text>
<text x="{PAD}" y="{H-PAD+16}" fill="#8fa0af" font-size="12">{first_d}</text>
<text x="{W-PAD}" y="{H-PAD+16}" fill="#8fa0af" font-size="12" text-anchor="end">{last_d}</text>
<text x="{PAD}" y="{H-PAD-6}" fill="#8fa0af" font-size="12">{prefix}{fmt.format(lo)}</text>
</svg>"""

def svg_stock_line(aligned):
    """Render align_stock_to_snapshots() output on the snapshot-date x-axis.

    Separate from svg_line because this chart has to draw three states per
    point, not one: a fresh close, a repeat of the close already shown on an
    earlier snapshot date, and no usable close at all. Filled dot, hollow dot,
    no dot — and the line breaks across the gaps rather than bridging them, so
    a stretch with no data cannot be read as a flat stretch of price.
    """
    pts_val = [(i, v) for i, (_d, v, _a, _f) in enumerate(aligned) if v is not None]
    if not pts_val:
        return "<p class='empty'>No PSA closes line up with the snapshot dates — run <code>analysis/update_stock.py</code>.</p>"
    W, H, PAD = 760, 170, 34
    vals = [v for _i, v in pts_val]
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1
    n = len(aligned)
    def x(i): return PAD + (W - 2 * PAD) * (i / max(n - 1, 1))
    def y(v): return H - PAD - (H - 2 * PAD) * ((v - lo) / span)

    # polyline per run of consecutive plotted snapshot dates; a blank breaks the run
    lines, run = "", []
    for i, (_d, v, _a, _f) in enumerate(aligned):
        if v is None:
            if len(run) > 1:
                lines += f"<polyline points=\"{' '.join(run)}\" fill='none' stroke='#f0a44b' stroke-width='2.5'/>"
            run = []
        else:
            run.append(f"{x(i):.1f},{y(v):.1f}")
    if len(run) > 1:
        lines += f"<polyline points=\"{' '.join(run)}\" fill='none' stroke='#f0a44b' stroke-width='2.5'/>"

    dots = ""
    for i, (d, v, as_of, fresh) in enumerate(aligned):
        if v is None:
            continue
        tip = (f"{d}: ${v:,.2f} close of {as_of}"
               + ("" if fresh else " (carried forward — no new close since)"))
        if fresh:
            dots += (f"<circle cx='{x(i):.1f}' cy='{y(v):.1f}' r='3.5' fill='#f0a44b'>"
                     f"<title>{html.escape(tip)}</title></circle>")
        else:
            dots += (f"<circle cx='{x(i):.1f}' cy='{y(v):.1f}' r='3.2' fill='none' "
                     f"stroke='#f0a44b' stroke-width='1.6'>"
                     f"<title>{html.escape(tip)}</title></circle>")

    # No composition-break rules here. Those mark the date the tracked *store*
    # population changed, which has no bearing on a share price series; drawing
    # the same dashed rule on this chart would either need a caption that does
    # not apply to it or stand unexplained. The x-axis still aligns with the
    # charts above, where the breaks are annotated.

    # legend on the chart itself — the filled/hollow distinction is the whole
    # point of this chart and must not live only in the prose above it
    lx = W - PAD - 232
    legend = (f"<circle cx='{lx}' cy='12' r='3.5' fill='#f0a44b'/>"
              f"<text x='{lx+9}' y='16' fill='#8fa0af' font-size='11'>close from a new trading day</text>"
              f"<circle cx='{lx}' cy='27' r='3.2' fill='none' stroke='#f0a44b' stroke-width='1.6'/>"
              f"<text x='{lx+9}' y='31' fill='#8fa0af' font-size='11'>previous close carried forward</text>")

    return f"""<svg viewBox="0 0 {W} {H}" role="img" style="width:100%;height:auto">
<line x1="{PAD}" y1="{H-PAD}" x2="{W-PAD}" y2="{H-PAD}" stroke="#232c35"/>
{lines}{dots}{legend}
<text x="{PAD}" y="16" fill="#8fa0af" font-size="12">${hi:,.2f}</text>
<text x="{PAD}" y="{H-PAD+16}" fill="#8fa0af" font-size="12">{aligned[0][0]}</text>
<text x="{W-PAD}" y="{H-PAD+16}" fill="#8fa0af" font-size="12" text-anchor="end">{aligned[-1][0]}</text>
<text x="{PAD}" y="{H-PAD-6}" fill="#8fa0af" font-size="12">${lo:,.2f}</text>
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

STORE_SITE = {}   # store_id -> site_number, filled by main()


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
    STORE_SITE.clear()
    STORE_SITE.update({sid: m[0] for sid, m in meta.items()})
    site_meta = {m[0]: m for m in meta.values()}     # site_number -> meta tuple

    # ---- movers inputs -----------------------------------------------------
    panel = load_store_sizes()
    bounds = price_bounds(panel)
    stores_by_date = {d: set(v) for d, v in snaps.items()}
    quarantine = Quarantine()
    panel_dates = sorted({d for s in panel.values() for d in s})
    p_prior, p_recent = windows(panel_dates)
    s_prior, s_recent = windows(dates)
    breaks = detect_composition_break(snaps, dates)
    break_dates = [b[0] for b in breaks]

    price_moves, unadvertised = price_movers(panel, panel_dates, stores_by_date,
                                             bounds, quarantine)
    av_declines, av_rises, av_stopped = unit_movers(snaps, dates, quarantine)
    ten_moves = ten_movers(snaps, dates, bounds, quarantine)

    # national series
    units_series, price_series = [], []
    for d, stores in snaps.items():
        units_series.append((d, sum(v["units"] for v in stores.values())))
        tens = [v["ten"] for v in stores.values() if v["ten"]]
        if tens:
            price_series.append((d, statistics.median(tens)))

    # PSA closes placed on the snapshot dates, so this chart's x-axis is the
    # same as the two above it and the three can be read across.
    stock_aligned = align_stock_to_snapshots(load_stock_history(), dates)
    n_fresh = sum(1 for _d, v, _a, f in stock_aligned if v is not None and f)
    n_carried = sum(1 for _d, v, _a, f in stock_aligned if v is not None and not f)
    n_blank = sum(1 for _d, v, _a, _f in stock_aligned if v is None)
    if stock_aligned:
        _plotted = [(d, v, a) for d, v, a, _f in stock_aligned if v is not None]
        stock_counts = (
            f"{n_fresh + n_carried} of {len(stock_aligned)} snapshot dates carry a price: "
            f"{n_fresh} are a close from a trading day not already plotted, "
            f"{n_carried} repeat the previous close (weekend, market holiday, or no newer close "
            f"at scrape time), and {n_blank} are left blank because the newest close available was "
            f"more than {MAX_CARRY_DAYS} days old."
            + (f" Latest plotted point: {_plotted[-1][0]} showing the close of {_plotted[-1][2]} "
               f"(${_plotted[-1][1]:,.2f})." if _plotted else "")
        )
    else:
        stock_counts = ""

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

    # ---- movers rows -------------------------------------------------------
    # Every table below compares a trailing median over one week against the
    # trailing median over the week before, on rows that cleared all four guards.
    # Nothing here differences one snapshot against the one beside it.
    period = f"{s_prior[0]} → {s_recent[-1]}" if s_prior else ""
    price_period = f"{p_prior[0]} → {p_recent[-1]}" if p_prior else ""

    def srow(site, tail):
        m = site_meta.get(site, (site, "", "", ""))
        return (f"#{site}", m[1], m[2], m[3]) + tuple(tail)

    def money(prior, recent, delta, pct):
        sign = "+" if delta > 0 else "−"
        return (f"${prior:,.0f}", f"${recent:,.0f}",
                f"{sign}${abs(delta):,.0f}", f"{'+' if pct > 0 else '−'}{abs(pct):.1f}%")

    movers = [srow(s, (f"{pm:,.0f} → {rm:,.0f}", f"−{d:,.0f}"))
              for s, pm, rm, d in sorted(av_declines, key=lambda t: -t[3])][:15]
    restock = [srow(s, (f"{pm:,.0f} → {rm:,.0f}", f"+{d:,.0f}"))
               for s, pm, rm, d in sorted(av_rises, key=lambda t: -t[3])][:10]
    stopped_rows = [srow(s, (f"{pm:,.0f}", f"{n} of {WINDOW}"))
                    for s, pm, n in sorted(av_stopped, key=lambda t: -t[1])][:15]
    hikes = [srow(s, money(pm, rm, d, p))
             for s, pm, rm, d, p in sorted(ten_moves, key=lambda t: -t[3]) if d > 0][:10]
    cuts = [srow(s, money(pm, rm, d, p))
            for s, pm, rm, d, p in sorted(ten_moves, key=lambda t: t[3]) if d < 0][:10]

    if base:
        added = sorted(set(snaps[latest]) - set(snaps[base]), key=int)
        removed = sorted(set(snaps[base]) - set(snaps[latest]), key=int)
    else:
        added = removed = []

    mover_cols = ["site #", "address", "city", "state", "median units (prior → recent)", "change"]
    stopped_cols = ["site #", "address", "city", "state", "median units advertised before", "snapshots at zero"]
    price_cols = ["site #", "address", "city", "state", "prior median", "recent median", "change", "change %"]

    # ---- the accurate reason a movers table is empty ----------------------
    def why_empty(kind):
        if not base:
            return (f"Only one snapshot has ever been logged ({latest}). A comparison "
                    f"needs two, so there is nothing to diff yet.")
        if not s_prior:
            return (f"The history log holds {len(dates)} snapshot"
                    f"{'s' if len(dates) != 1 else ''}, and a week-over-week comparison "
                    f"needs {BASELINE_N}. Nothing is being withheld — the two "
                    f"{WINDOW}-snapshot windows this table compares do not both exist yet.")
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

    # ---- composition change in the tracked population ----------------------
    # A step change in how many stores are tracked is a change in what is being
    # counted. Every series that crosses one is measuring two different
    # populations either side of it, and a reader who is not told that will read
    # the step as inventory surging and prices falling. Stated as a fact about the
    # dataset — how many stores entered, on what date, and what the totals were
    # before and after. No cause is attributed and no party is named, because the
    # only thing the data supports is that the tracked population changed size.
    if breaks:
        items = "".join(
            f"<li><b>{d}</b>: {n:,} stores entered the tracked population "
            f"({b:,} → {a:,} stores). Advertised inventory moved "
            f"{sum(v['units'] for v in snaps[dates[dates.index(d) - 1]].values()):,} → "
            f"{sum(v['units'] for v in snaps[d].values()):,} units and the median 10x10 moved "
            f"${next(v for dd, v in price_series if dd == dates[dates.index(d) - 1]):,.0f} → "
            f"${next(v for dd, v in price_series if dd == d):,.0f} across the same step.</li>"
            for d, n, b, a in breaks)
        break_banner = f"""<div class="alert break"><b>The tracked population changed size, so
figures either side of these dates are not comparable.</b>
<ul>{items}</ul>
The step is a change in which stores are being counted, not a measured change in the market. Totals,
medians and any before-and-after read across one of these dates describe two different populations of
stores. Series that cross a break are marked on the chart.</div>"""
        break_caption = ("<p class='note break-note'>Marked on the chart: the tracked population "
                         "changed size on " + ", ".join(break_dates) + ". Values before and after "
                         "that line count different sets of stores and are not comparable.</p>")
    else:
        break_banner = ""
        break_caption = ""

    today = datetime.date.today().strftime("%B %d, %Y")
    total_now = units_series[-1][1]
    med_now = price_series[-1][1] if price_series else 0

    # ---- per-size price moves (trailing medians off the store-size panel) ----
    rate_events = load_rate_changes()
    n_events = len(rate_events)
    n_event_days = len(set(r["date"] for r in rate_events))
    if p_prior:
        pm_cols = ["site #", "size", "prior median", "recent median", "change", "change %"]
        def pm_row(t):
            s, sz, pm, rm, d, p = t
            return (f"#{s}", sz) + money(pm, rm, d, p)
        pm_hikes = [pm_row(t) for t in sorted(price_moves, key=lambda t: -t[4]) if t[4] > 0][:10]
        pm_cuts = [pm_row(t) for t in sorted(price_moves, key=lambda t: t[4]) if t[4] < 0][:10]
        bound_note = "; ".join(
            f"{sz} ${bounds[sz][0]:,.0f}–${bounds[sz][1]:,.0f}"
            for sz in sorted(bounds, key=lambda s: SIZE_ORDER.index(s) if s in SIZE_ORDER else len(SIZE_ORDER)))
        movers_section = f"""
<section><h2>Advertised price moves by unit size</h2>
<p class='note'>Change in a listing's <b>trailing median</b> advertised price: the median across
{p_recent[0]} → {p_recent[-1]} against the median across {p_prior[0]} → {p_prior[-1]}, per store and
unit size. Medians rather than two endpoints, because a single mis-scraped reading inside a
{WINDOW}-snapshot window is outvoted by the readings around it, where against one adjacent snapshot
it would be the whole signal. {len(price_moves):,} listings moved; {len(quarantine.counts('price')) and sum(quarantine.counts('price').values()):,}
were held back as suspected collection artifacts and are itemised below. These are observed changes
in published online rates; no cause is inferred.</p>
<p class='note'>A listing must carry a real price across {BASELINE_N} consecutive snapshots before it
can appear here, and every observation must fall inside the plausible range for its size, taken from
the observed distribution of that size (p{P_LO*100:g}–p{P_HI*100:g}): {bound_note}.</p>
<h3 class="sub">Largest increases</h3>{table(pm_cols, pm_hikes, 10,
    "No increases cleared the baseline and plausible-range guards this week.")}
<h3 class="sub">Largest decreases</h3>{table(pm_cols, pm_cuts, 10,
    "No decreases cleared the baseline and plausible-range guards this week.")}
<h3 class="sub">No longer advertised — not a price move</h3>
<p class='note'>Listings that were priced through {p_prior[0]} → {p_prior[-1]} and then stopped being
published. A listing that disappears is a disappearance, not a repricing, so it is counted here
rather than shown as a price falling to nothing.</p>
{table(["site #", "size", "units advertised before it left the feed"],
       [(f"#{s}", sz, f"{u:,.0f}") for s, sz, u, _n in sorted(unadvertised, key=lambda t: -t[2])], 15,
       "No priced listing left the size feed this week.")}
<p class='note'>The rate-change log (history/rate_changes.csv) holds {n_events:,} individual price
changes across {n_event_days} day{'s' if n_event_days != 1 else ''}. It records deltas only, so it
cannot support a trailing median or tell a placeholder from a repricing; the tables above are built
from the per-listing price levels in history/store-sizes-YYYY-MM.csv instead.</p></section>"""
    else:
        movers_section = f"""
<section><h2>Advertised price moves by unit size</h2>
<p class='note'>Change in a listing's trailing median advertised price, per store and unit size.</p>
<p class='empty'>The per-listing price log (history/store-sizes-YYYY-MM.csv) covers
{len(panel_dates)} snapshot{'s' if len(panel_dates) != 1 else ''}, and this comparison needs
{BASELINE_N} — two {WINDOW}-snapshot windows to take a median over. Nothing is being withheld and
nothing has gone wrong; the second window does not exist yet.</p></section>"""

    # ---- quarantine: the count, the reasons, and where the rows are ----------
    qcounts = quarantine.counts()
    REASON_TEXT = {
        "no_baseline":
            f"no real price across the {WINDOW} snapshots before the comparison window. A store that "
            f"has just entered the dataset has no prior reading, so any change computed for it is "
            f"arithmetic on a value nobody observed.",
        "store_not_tracked_through_window":
            f"the store was not in the dataset for all {BASELINE_N} snapshots being compared.",
        "no_longer_advertised":
            "priced before the comparison window and not priced through it — the listing stopped "
            "being published. A disappearance, not a repricing.",
        "stopped_advertising_availability":
            "advertised units went to exactly zero and stayed there, which is a store that stopped "
            "publishing availability rather than one that rented everything out.",
        "started_advertising_availability":
            "advertised units were zero and are not any more, which is a store that began "
            "publishing availability rather than one that returned units to stock.",
        "price_out_of_bounds":
            f"at least one observation fell outside the p{P_LO*100:g}–p{P_HI*100:g} range of observed "
            f"prices for that unit size, which marks it as a placeholder rather than a rate.",
    }
    SCOPE_TEXT = {"price": "price by size", "availability": "availability", "10x10": "10x10 price"}
    qrows = [(SCOPE_TEXT.get(sc, sc), r, f"{n:,}", REASON_TEXT.get(r, ""))
             for sc in ("price", "10x10", "availability")
             for r, n in quarantine.counts(sc).items()]
    qpath = "quarantine/movers_quarantine.csv"
    n_price = sum(quarantine.counts("price").values())
    n_store = sum(quarantine.counts("availability").values()) + sum(quarantine.counts("10x10").values())
    quarantine_section = f"""
<section><h2>Excluded rows</h2>
<p class='note'><b>{len(quarantine):,} rows were held back from the movers tables above as suspected
collection artifacts</b> — {n_price:,} of the {len(panel):,} tracked store-size listings, and
{n_store:,} store-level rows across the {len(snaps[latest]):,} stores tracked. Nothing is dropped
silently: every excluded row is written to <code>{qpath}</code> with its reason and the values that
triggered it, so this count can be checked rather than taken on trust.</p>
{table(["table", "reason", "rows", "what it means"], qrows, 20,
       "No rows were excluded — every listing cleared all four guards.")}</section>"""

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
<div><h3 class="sub">Advertised availability — {html.escape(sz)}</h3>{svg_line(avail_sz, breaks=break_dates)}</div>
<div><h3 class="sub">Weighted-avg price — {html.escape(sz)}</h3>{svg_line(price_sz, fmt="{:,.0f}", prefix="$", breaks=break_dates)}</div>
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
{break_caption}
{size_panels}</section>"""
    else:
        size_section = """
<section><h2>Trends by size</h2>
<p class='note'>Not enough data yet — this fills in once history/sizes-YYYY-MM.csv has logged a day.</p></section>"""

    sections = f"""{freshness_banner}{break_banner}
<section><h2>National advertised inventory</h2>
<p class='note'>Total units marked rentable across the network, per snapshot. This reflects advertised
availability — units published as rentable — which may differ from physical vacancy; the two aren't
directly comparable from public data. Read this as a measure of what is being advertised over time,
not as an occupancy figure.</p>
{break_caption}
{svg_line(units_series, breaks=break_dates)}</section>

<section><h2>National median 10x10 price</h2>
<p class='note'>Median of each store's cheapest available 10x10, per snapshot.</p>
{break_caption}
{svg_line(price_series, fmt="{:,.0f}", prefix="$", breaks=break_dates)}</section>

<section><h2>Public Storage (NYSE: PSA) closing share price</h2>
<p class='note'>PSA's official daily closing price, plotted on the same scraper snapshot dates as the
charts above so the x-axes line up.</p>
<p class='note'><b>How non-trading days are handled.</b> The scraper publishes every calendar day;
the market does not. Each snapshot date shows PSA's most recent close on or before that date. A
<b>filled dot</b> is a close from a trading day not already plotted. A <b>hollow dot</b> is the same
close repeated onto a later snapshot date because no newer close existed yet — a carried-forward
value, not that day's price. Where the most recent close is more than {MAX_CARRY_DAYS} days old the
point is <b>left blank and the line breaks</b> rather than continuing flat. The daily job runs at
11:00 UTC, ahead of the 16:00 ET close, so the newest close available on any snapshot date is
normally the previous trading day's. Because the series is drawn on snapshot dates, closes from
trading days with no snapshot are not plotted.</p>
<p class='note'>{stock_counts}</p>
{svg_stock_line(stock_aligned) if stock_aligned else
    "<p class='empty'>No PSA price data yet — run <code>analysis/update_stock.py</code>.</p>"}</section>

<section><h2>Largest declines in advertised availability</h2>
<p class='note'>Stores whose <b>trailing median</b> count of units listed as available fell the most:
the median across {html.escape(s_recent[-1] if s_recent else latest)} and the {WINDOW - 1} snapshots
before it, against the {WINDOW} snapshots before those ({html.escape(period)}). This measures what the
website advertised and nothing more: a smaller number means fewer units were listed as available.
Advertised availability is not a physical vacancy count, and a change in it can arise from many
ordinary causes — rentals, listing and pricing updates, unit reclassification, or site changes —
which public data cannot distinguish between. Stores that stopped publishing availability altogether
are listed separately below rather than shown here as the largest decline.</p>
{table(mover_cols, movers, 15, why_empty("declines in advertised availability"))}</section>

<section><h2>Largest increases in advertised availability</h2>
<p class='note'>The opposite end — stores whose trailing median count of listed-available units rose
the most over the same two windows. The same caveat applies in reverse: this is a change in what was
advertised, not a measured change in physical occupancy.</p>
{table(mover_cols, restock, 10, why_empty("increases in advertised availability"))}</section>

<section><h2>Stopped advertising availability — not a decline</h2>
<p class='note'>Stores that advertised units through {html.escape(s_prior[0] if s_prior else '?')} →
{html.escape(s_prior[-1] if s_prior else '?')} and then reported exactly zero on every snapshot since.
A store reading zero after a week in the hundreds has stopped publishing availability; reading that as
units rented would make it the largest mover on the page and the least likely to mean what the table
says it means. Kept separate for that reason.</p>
{table(stopped_cols, stopped_rows, 15,
       "No tracked store went to zero advertised units across the whole recent window.")}</section>

<section><h2>10x10 advertised price increases</h2>
<p class='note'>Largest increases in the <b>trailing median</b> of a store's cheapest advertised
10x10, over the same two windows ({html.escape(period)}). Prices shown are the advertised online
rates. A store must carry a 10x10 price across {BASELINE_N} consecutive snapshots, all inside the
plausible range for a 10x10, before it can appear.</p>
{table(price_cols, hikes, 10, why_empty("10x10 price increases"))}</section>

<section><h2>10x10 advertised price decreases</h2>
<p class='note'>Largest decreases in the trailing median of a store's cheapest advertised 10x10 over
the same two windows. Published rates move for many reasons; this table reports the change without
inferring a cause.</p>
{table(price_cols, cuts, 10, why_empty("10x10 price decreases"))}</section>
{movers_section}
{quarantine_section}
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
.alert.break{{background:#12212b;border-color:#2f5163;border-left-color:#6b8ea8;color:#cfe3ef}}
.alert.break b{{color:#9fc0d8}}
.alert.break ul{{margin:10px 0 10px 20px}}
.alert.break li{{margin:4px 0}}
p.break-note{{color:#9fc0d8;font-size:.86rem;border-left:3px solid #6b8ea8;padding-left:10px;
margin:0 0 10px}}
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
<a href="/">directory</a> · <a href="/insights.html">insights</a> · <a href="/markets.html">metro markets</a> ·
<a href="/merger.html">merger before/after</a> · <a href="/repricing.html">repricing waves</a></p>
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
    written = quarantine.write(qpath)
    print(f"Wrote {OUT} ({len(dates)} snapshots, period: {period or 'n/a'})")
    if p_prior:
        print(f"  price windows: {p_prior[0]}..{p_prior[-1]} vs {p_recent[0]}..{p_recent[-1]} "
              f"({WINDOW} snapshots each, {MIN_OBS} priced minimum, baseline {BASELINE_N})")
    # The exclusion count belongs in the build output as well as on the page: a
    # number only a reader of the rendered HTML can see is not an audit trail.
    print(f"  quarantined {len(quarantine):,} of {len(panel):,} store-size listings "
          f"as suspected artifacts -> {written}")
    for reason, n in quarantine.counts().items():
        print(f"    {reason:34} {n:>7,}")
    for d, n, b, a in breaks:
        print(f"  composition break {d}: {n:,} stores entered ({b:,} -> {a:,}); "
              f"annotated on every chart that crosses it")
    if not breaks:
        print("  no composition break detected "
              f"(no single day added {BREAK_MIN_STORES:,}+ stores)")

if __name__ == "__main__":
    main()
