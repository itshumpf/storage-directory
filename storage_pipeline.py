#!/usr/bin/env python3
"""
storage_pipeline.py — one front door for every operator's collector.

The six collectors (Public Storage, CubeSmart, Storage Sense, U-Haul, StorageMart, SmartStop) already
emit the same record shape — brand / store_id / site_number / address / lat /
lng / url / units[{size, price, street_price, available, count, promo, promo2,
sku, attrs}] — but each one lands it somewhere different and on its own clock.
This script is the seam that joins them:

    history/<brand>/<YYYY-MM-DD>.json      one immutable snapshot per brand per day
    history/combined/latest.json           every brand's freshest snapshot, merged (gitignored, rebuildable)
    history/combined/latest.manifest.json  what went into it and how old each part is
    history/combined/daily-YYYY-MM.csv     per-store daily aggregates, all brands
    history/combined/sizes-YYYY-MM.csv     per-brand, per-state, per-size daily aggregates
    history/combined/store-sizes-YYYY-MM.csv  per-store, per-size cheapest price + count (store popups, movers)
    all_locations.json                     every brand in the full record shape — what index.html reads
    history/combined/rate_changes-YYYY-MM.csv
                                           per-SKU price / street / promo / listed / delisted events, all brands
    history/combined/record_runs.csv       what has been recorded (idempotency ledger)
    dashboard-data.json                    the compact file dashboard.html reads

Commands (run from the repo root):

    python storage_pipeline.py status                     freshness of every brand, at a glance
    python storage_pipeline.py audit                      field completeness per brand (catches a parser reading the wrong node)
    python storage_pipeline.py snapshot <brand> [--source PATH] [--date D]
                                                          normalize a collector's output into history/<brand>/
    python storage_pipeline.py import                     sweep every brand's known output locations into snapshots
    python storage_pipeline.py merge   [--date D] [--max-age-days N]
    python storage_pipeline.py record  [--date D]         record every (brand, date) snapshot not yet in the ledger
    python storage_pipeline.py rebuild-changes <brand>    recompute one brand's derived rate events from snapshots
    python storage_pipeline.py backfill-ps                seed daily-*.csv from the legacy history/YYYY-MM.csv series
    python storage_pipeline.py build-dashboard            write dashboard-data.json
    python storage_pipeline.py daily   [--date D]         import -> merge -> record -> build-dashboard
    python storage_pipeline.py run <brand> [-- extra args] launch that brand's collector, then snapshot its output

Design rules, all inherited from the existing collectors:
  * Snapshots are immutable. `snapshot` refuses to overwrite one unless --force.
  * Every write is atomic (temp file + replace).
  * Nothing here scrapes on its own. `run` only shells out to the collector you already have.
  * Every command is idempotent — re-running a day is a no-op, never a duplicate row.
  * Fails loudly on suspicious input (empty list, count below the brand floor, wrong shape).

Stdlib only, so it runs anywhere the collectors run (Windows, the Actions runner, this VM).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shutil
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

from independent_registry import validate_operator_registry

ROOT = Path(__file__).resolve().parent
HISTORY = ROOT / "history"
COMBINED = HISTORY / "combined"
DASHBOARD_DATA = ROOT / "dashboard-data.json"
DASHBOARD_DATA_ANON = ROOT / "dashboard-data-anon.json"   # same shape, brands renamed "Storage Company N" for portfolio use
INDEPENDENT_REGISTRY = ROOT / "independent_operators.json"

# Fixed display order everywhere (charts, tables, legends). Never re-sorted by size.
BRANDS = {
    "publicstorage": {
        "label": "Public Storage",
        "short": "PS",
        # Where the collector leaves its output today.
        "sources": ["enriched_locations.json"],
        # Below this many stores the file is a partial scrape, not a snapshot.
        "floor": 3000,
        "run": [sys.executable, "daily_scraper.py"],
    },
    "cubesmart": {
        "label": "CubeSmart",
        "short": "CS",
        "sources": ["history/cubesmart/{date}.json", "cubesmart_locations.json", "cubesmart_{date}.json"],
        "floor": 1200,
        # --out points at the dated snapshot so the crawler's own resume logic
        # continues *today's* file if it is interrupted, instead of skipping
        # every store it saw yesterday.
        "run": [sys.executable, "cubesmart_scraper.py", "--out", "history/cubesmart/{date}.json"],
    },
    "storagesense": {
        "label": "Storage Sense",
        "short": "SS",
        "sources": ["history/storagesense/{date}.json", "history/storagesense_locations.json"],
        "floor": 250,
        "run": [sys.executable, "storagesense_scraper.py",
                "--out", "history/storagesense_locations.json",
                "--state", "history/storagesense_state.json",
                "--report", "history/storagesense_last_run.json",
                "--delay", "5", "--budget", "0", "--refresh-hours", "0"],
    },
    "uhaul": {
        "label": "U-Haul",
        "short": "UH",
        "sources": ["history/uhaul/{date}.json", "history/uhaul_locations.json"],
        "floor": 1600,
        "run": [sys.executable, "uhaul_scraper.py", "--delay", "5"],
    },
    "storagemart": {
        "label": "StorageMart",
        "short": "SM",
        "sources": ["history/storagemart/{date}.json"],
        "floor": 150,
        "run": [sys.executable, "storagemart_scraper.py", "--delay", "5"],
    },
    "smartstop": {
        "label": "SmartStop",
        "short": "ST",
        "sources": ["history/smartstop/{date}.json"],
        # Match the collector's U.S.-only publish floor. The sitemap also lists
        # Canadian locations, which are intentionally excluded before collection.
        "floor": 185,
        "run": [sys.executable, "smartstop_scraper.py", "--delay", "10"],
    },
    "independent": {
        "label": "Independent",
        "short": "IN",
        "sources": ["history/independent/{date}.json"],
        "floor": 67,
        "run": [sys.executable, "independent_full_scraper.py", "--delay", "10"],
    },
}
BRAND_ORDER = list(BRANDS)
MAX_DROP = 0.10  # a snapshot this much smaller than the last one is a partial crawl, not a market

DAILY_HEADER = ["date", "brand", "store_id", "state", "listings", "units_avail",
                "cheapest_10x10", "median_price", "median_ppsf"]
SIZES_HEADER = ["date", "brand", "state", "size", "listings", "units_avail", "median_price"]
STORE_SIZES_HEADER = ["date", "brand", "store_id", "size", "price", "available"]
ALL_LOCATIONS = ROOT / "all_locations.json"   # every brand, full record shape — what index.html reads
CHANGES_HEADER = ["date", "brand", "store_id", "site_number", "size", "sku", "field", "old", "new"]
LEDGER_HEADER = ["recorded_at", "brand", "date", "previous_date", "stores", "events", "note"]

SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*[xX×]\s*(\d+(?:\.\d+)?)\s*$")


# --------------------------------------------------------------------------- utils
def today() -> str:
    return dt.date.today().isoformat()


def atomic_write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def independent_pilot_payload(path: Path = INDEPENDENT_REGISTRY) -> dict:
    """Dashboard-safe discovery metadata; never turns candidates into store observations."""
    if not path.exists():
        return {"as_of": None, "counts": {}, "operators": []}
    registry = validate_operator_registry(path)
    operators = [{
        "id": o["operator_id"],
        "name": o["name"],
        "domain": o["domain"],
        "url": o["representative_url"],
        "platform": o["platform_hint"],
        "status": o["status"],
        "evidence": o["discovery_basis"],
        "robots_reviewed": o["robots_reviewed"],
        "terms_reviewed": o["terms_reviewed"],
        "policy_reason": o.get("policy_reason", ""),
    } for o in registry["operators"]]
    counts = {status: sum(o["status"] == status for o in operators)
              for status in ("candidate", "probe_ready", "policy_pending", "policy_hold")}
    counts["enabled"] = sum(bool(o.get("enabled")) for o in registry["operators"])
    return {"as_of": registry.get("created_date"), "counts": counts, "operators": operators}


def append_csv(path: Path, header: list[str], rows: list[list]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists() or path.stat().st_size == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        if new:
            w.writerow(header)
        w.writerows(rows)


def atomic_write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(rows)
    tmp.replace(path)


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def snapshot_dir(brand: str) -> Path:
    return HISTORY / brand


def snapshot_dates(brand: str) -> list[str]:
    d = snapshot_dir(brand)
    if not d.exists():
        return []
    return sorted(p.stem for p in d.glob("????-??-??.json"))


def snapshot_path(brand: str, date: str) -> Path:
    return snapshot_dir(brand) / f"{date}.json"


def load_snapshot(brand: str, date: str) -> list[dict]:
    """Read a snapshot through normalize(), so records the collectors wrote themselves
    (Storage Sense, U-Haul) carry the same keys as the ones this script wrote."""
    return normalize(brand, read_json(snapshot_path(brand, date)))


def parse_size(size: str):
    m = SIZE_RE.match(size or "")
    if not m:
        return None, None, None
    w, d = float(m.group(1)), float(m.group(2))
    return w, d, w * d


def num(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- normalize
def normalize_store(brand: str, s: dict) -> dict | None:
    """Coerce one collector record into the shared shape. Returns None if unusable."""
    sid = s.get("store_id") or s.get("site_number")
    if not sid:
        return None
    units = []
    for u in s.get("units") or []:
        size = str(u.get("size") or "").strip()
        price = num(u.get("price"))
        w, d, sqft = parse_size(size)
        if u.get("sqft"):
            sqft = num(u["sqft"])
        street = num(u.get("street_price"))
        if street is None and isinstance(u.get("rates"), dict):
            street = num(u["rates"].get("street"))
        units.append({
            "size": size,
            "price": price,
            "street_price": street,
            "available": bool(u.get("available", True)),
            "count": int(u.get("count") or 0),
            "promo": (u.get("promo") or "").strip(),
            "promo2": (u.get("promo2") or "").strip(),
            "sku": str(u.get("sku") or ""),
            "attrs": (u.get("attrs") or "").strip(),
            "sqft": sqft,
            # Operator-specific extras ride along untouched when present.
            **{k: u[k] for k in ("total", "promo_price", "standard_rate", "promo_terms") if k in u},
        })
    return {
        "brand": brand,
        "store_id": str(sid),
        "site_number": str(s.get("site_number") or ""),
        "name": s.get("name") or "",
        "address": s.get("address") or "",
        "city": s.get("city") or "",
        "state": (s.get("state") or "").upper(),
        "zip": str(s.get("zip") or ""),
        "phone": s.get("phone") or "",
        "lat": num(s.get("lat")),
        "lng": num(s.get("lng")),
        "url": s.get("url") or "",
        "rating": num(s.get("rating")),
        "reviews": int(num(str(s.get("reviews")).replace(",", "")) or 0) if s.get("reviews") not in (None, "") else None,
        **({"inventory_semantics": s["inventory_semantics"]} if s.get("inventory_semantics") else {}),
        **({"operator_id": s["operator_id"]} if s.get("operator_id") else {}),
        **({"platform": s["platform"]} if s.get("platform") else {}),
        **({"facility_id": s["facility_id"]} if s.get("facility_id") else {}),
        **({"software_provider": s["software_provider"]} if s.get("software_provider") else {}),
        "units": units,
    }


def normalize(brand: str, raw) -> list[dict]:
    if isinstance(raw, dict):
        # U-Haul checkpoint shape, or a keyed map.
        if "stores" in raw and isinstance(raw["stores"], list):
            raw = raw["stores"]
        else:
            raw = list(raw.values())
    if not isinstance(raw, list):
        raise SystemExit(f"{brand}: expected a list of stores, got {type(raw).__name__}")
    out, seen = [], set()
    for s in raw:
        if not isinstance(s, dict):
            continue
        if s.get("brand") and s["brand"] != brand:
            raise SystemExit(f"{brand}: record tagged brand={s['brand']!r} — wrong file for this brand")
        rec = normalize_store(brand, s)
        if rec and rec["store_id"] not in seen:
            seen.add(rec["store_id"])
            out.append(rec)
    out.sort(key=lambda r: r["store_id"])
    return out


DATE_IN_NAME = re.compile(r"(\d{4}-\d{2}-\d{2})")


def source_date(brand: str, source: Path, run_date: str) -> str:
    """The date the data in `source` was collected — NOT the date this script runs.

    A rolling file (enriched_locations.json, *_locations.json) is whatever the last
    successful collector run left behind. On a clone that has fallen behind origin, or a
    day the collector did not run, that can be days old. Filing it under today's date would
    fabricate a day of "no change", so the date comes from the collector's own record:
      * a date in the file name wins (history/<brand>/2026-09-04.json, cubesmart_2026-09-04.json)
      * Public Storage: the last date update_history.py logged in history/YYYY-MM.csv —
        it is written by the same job, from the same file
      * Storage Sense / U-Haul rolling files: the last-run report next to them
      * otherwise the file's modification date
    """
    m = DATE_IN_NAME.search(source.name)
    if m:
        return m.group(1)
    if brand == "publicstorage":
        months = sorted(HISTORY.glob("????-??.csv"))
        if months:
            rows = read_csv(months[-1])
            if rows:
                return max(r["date"] for r in rows)
    if brand == "storagesense":
        rep = HISTORY / "storagesense_last_run.json"
        if rep.exists():
            r = read_json(rep)
            if r.get("finished_at"):
                return r["finished_at"][:10]
    if brand == "uhaul":
        rep = HISTORY / "uhaul_last_run.json"
        if rep.exists():
            r = read_json(rep)
            if r.get("status", "").startswith("complete") and r.get("date"):
                return r["date"]
    return dt.date.fromtimestamp(source.stat().st_mtime).isoformat()


CORE_FIELDS = ("address", "city", "state", "zip", "phone", "lat", "lng", "url")
UNIT_FIELDS = ("size", "price", "sku")


def completeness(recs: list[dict]) -> dict[str, float]:
    """Share of records with each core field EMPTY. The check that would have
    caught U-Haul on 2026-09-03: 1,182 facilities, every address "", every lat
    null, and the run green — an empty string is indistinguishable from a
    facility that did not publish one unless someone counts them."""
    n = max(len(recs), 1)
    out = {f: sum(1 for r in recs if r.get(f) in (None, "")) / n for f in CORE_FIELDS}
    units = [u for r in recs for u in r["units"]]
    m = max(len(units), 1)
    out.update({f"unit.{f}": sum(1 for u in units if u.get(f) in (None, "")) / m for f in UNIT_FIELDS})
    return out


def warn_if_hollow(brand: str, date: str, recs: list[dict], threshold: float = 0.5) -> list[str]:
    hollow = [f"{f} {v:.0%} empty" for f, v in completeness(recs).items() if v > threshold]
    if hollow:
        print(f"WARNING {brand} {date}: fields mostly empty — {', '.join(hollow)}. "
              f"The collector ran but its parser may be reading the wrong node.", file=sys.stderr)
    return hollow


def cmd_audit() -> None:
    """Field completeness for every brand's latest snapshot, side by side."""
    rows = {}
    for brand in BRAND_ORDER:
        dates = snapshot_dates(brand)
        if dates:
            rows[brand] = (dates[-1], completeness(load_snapshot(brand, dates[-1])))
    if not rows:
        print("no snapshots yet"); return
    fields = list(CORE_FIELDS) + [f"unit.{f}" for f in UNIT_FIELDS]
    print(f"{'% empty':<12}" + "".join(f"{BRANDS[b]['short']} {d[5:]:<9}" for b, (d, _) in rows.items()))
    for f in fields:
        line = f"{f:<12}"
        for b, (_, c) in rows.items():
            v = c[f]
            line += f"{('—' if v == 0 else f'{v:.0%}'):>5}{'  !!' if v > 0.5 else '    '} "
        print(line)
    print("\n!! = more than half empty. Some are by design (Public Storage publishes no street rate);\n"
          "   a core field like address or lat at !! means the parser is reading the wrong node.")


def cmd_snapshot(brand: str, source: Path | None, date: str | None, force: bool = False,
                 quiet: bool = False, trust_date: bool = False) -> Path | None:
    cfg = BRANDS[brand]
    run_date = date or today()
    if source is None:
        for pattern in cfg["sources"]:
            cand = ROOT / pattern.format(date=run_date)
            if cand.exists():
                source = cand
                break
        if source is None:
            if not quiet:
                print(f"{brand}: no source found for {run_date} (looked for {cfg['sources']})")
            return None
    detected = source_date(brand, source, run_date)
    if trust_date and date and DATE_IN_NAME.search(source.name) is None:
        pass  # the collector just ran under this date; the caller knows better than the heuristics
    else:
        if date and detected != date and not quiet:
            print(f"{brand}: {source.name} holds data from {detected}, not {date}; filing it under {detected}")
        date = detected
    dest = snapshot_path(brand, date)
    if dest.exists() and not force:
        if source.resolve() == dest.resolve():
            # The collector already wrote the immutable snapshot itself. Validate it in place.
            recs = normalize(brand, read_json(dest))
            if len(recs) < cfg["floor"]:
                raise SystemExit(f"{brand} {date}: {len(recs)} stores is below the floor of {cfg['floor']}")
            warn_if_hollow(brand, date, recs)
            if not quiet:
                print(f"{brand} {date}: snapshot already in place ({len(recs):,} stores)")
            return dest
        if not quiet:
            print(f"{brand} {date}: snapshot exists, not overwriting (use --force)")
        return dest
    recs = normalize(brand, read_json(source))
    if len(recs) < cfg["floor"]:
        raise SystemExit(f"{brand} {date}: {len(recs)} stores is below the floor of {cfg['floor']} — "
                         f"refusing to snapshot a partial crawl from {source}")
    # A snapshot must be newer than what it replaces: a stale rolling file re-imported
    # under today's date would fabricate a day of "no change".
    prior = [d for d in snapshot_dates(brand) if d < date]
    if prior and not force:
        prev = load_snapshot(brand, prior[-1])
        if json.dumps(prev, sort_keys=True) == json.dumps(recs, sort_keys=True):
            print(f"{brand} {date}: {source.name} is byte-identical to the {prior[-1]} snapshot — "
                  f"the collector did not run. Nothing written (use --force to record it anyway).")
            return None
        # Same 10% rule the collectors use: a sharp drop is an interrupted crawl, and
        # snapshotting it would log hundreds of phantom "delisted" events.
        if len(recs) < len(prev) * (1 - MAX_DROP):
            raise SystemExit(f"{brand} {date}: {len(recs):,} stores is more than {MAX_DROP:.0%} below the "
                             f"{prior[-1]} snapshot ({len(prev):,}) — looks like a partial crawl. "
                             f"Nothing written (use --force to record it anyway).")
    warn_if_hollow(brand, date, recs)
    atomic_write_json(dest, recs)
    units = sum(len(r["units"]) for r in recs)
    print(f"{brand} {date}: {len(recs):,} stores, {units:,} units -> {dest.relative_to(ROOT)}")
    return dest


def backfill_publicstorage_git(date: str) -> None:
    """Recover missing immutable PS snapshots that arrived through a multi-day pull.

    Public Storage commits its rolling ``enriched_locations.json`` once per collection
    day.  A pull can therefore advance that file across several versions before
    ``import`` sees it.  The discarded versions are still exact Git objects, so recover
    only dates explicitly named by those commits; never manufacture a snapshot from the
    aggregate history.
    """
    existing = set(snapshot_dates("publicstorage"))
    if not existing:
        return
    try:
        output = subprocess.check_output(
            ["git", "log", "--all", "--format=%H%x09%s", "--", "enriched_locations.json"],
            cwd=ROOT, text=True, encoding="utf-8", errors="replace",
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"publicstorage: could not inspect Git history for missing snapshots: {exc}",
              file=sys.stderr)
        return

    commits = {}
    for line in output.splitlines():
        commit, sep, subject = line.partition("\t")
        match = DATE_IN_NAME.search(subject)
        if sep and match:
            commits.setdefault(match.group(1), commit)

    first = min(existing)
    for missing_date in sorted(d for d in commits if first <= d <= date and d not in existing):
        try:
            raw = subprocess.check_output(
                ["git", "show", f"{commits[missing_date]}:enriched_locations.json"], cwd=ROOT
            )
            recs = normalize("publicstorage", json.loads(raw))
        except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
            print(f"publicstorage {missing_date}: Git snapshot recovery failed: {exc}", file=sys.stderr)
            continue
        floor = BRANDS["publicstorage"]["floor"]
        if len(recs) < floor:
            print(f"publicstorage {missing_date}: Git version has {len(recs):,} stores, below "
                  f"the {floor:,} floor; not recovering it", file=sys.stderr)
            continue
        warn_if_hollow("publicstorage", missing_date, recs)
        dest = snapshot_path("publicstorage", missing_date)
        atomic_write_json(dest, recs)
        existing.add(missing_date)
        print(f"publicstorage {missing_date}: recovered exact snapshot from Git "
              f"({len(recs):,} stores, {sum(len(r['units']) for r in recs):,} units)")


def cmd_import(date: str) -> None:
    """Sweep every known output location. Also picks up dated CubeSmart files left in the root."""
    backfill_publicstorage_git(date)
    for brand in BRAND_ORDER:
        cmd_snapshot(brand, None, date)
    # Dated CubeSmart crawls left in the root by hand runs: cubesmart_YYYY-MM-DD.json
    for p in sorted(ROOT.glob("cubesmart_????-??-??.json")):
        d = p.stem.split("_")[1]
        if not snapshot_path("cubesmart", d).exists():
            cmd_snapshot("cubesmart", p, d)


# --------------------------------------------------------------------------- merge
def latest_snapshot(brand: str, date: str, max_age_days: int):
    dates = [d for d in snapshot_dates(brand) if d <= date]
    if not dates:
        return None, None, None
    d = dates[-1]
    age = (dt.date.fromisoformat(date) - dt.date.fromisoformat(d)).days
    if age > max_age_days:
        return d, age, None
    return d, age, load_snapshot(brand, d)


def cmd_merge(date: str, max_age_days: int) -> dict:
    stores, manifest = [], {"date": date, "built_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                            "max_age_days": max_age_days, "brands": {}}
    for brand in BRAND_ORDER:
        d, age, recs = latest_snapshot(brand, date, max_age_days)
        entry = {"label": BRANDS[brand]["label"], "snapshot_date": d, "age_days": age,
                 "included": recs is not None, "stores": 0, "priced_stores": 0, "units": 0}
        if recs is not None:
            entry.update(stores=len(recs),
                         priced_stores=sum(1 for r in recs if r["units"]),
                         units=sum(len(r["units"]) for r in recs))
            stores.extend(recs)
        manifest["brands"][brand] = entry
    manifest["stores"] = len(stores)
    if not stores:
        raise SystemExit("merge: no brand had a snapshot within the age window — nothing written")
    atomic_write_json(COMBINED / "latest.json", stores)
    atomic_write_json(COMBINED / "latest.manifest.json", manifest)
    for b, e in manifest["brands"].items():
        flag = "" if e["included"] else ("  (STALE — excluded)" if e["snapshot_date"] else "  (no snapshot yet)")
        print(f"  {BRANDS[b]['label']:<15} {e['snapshot_date'] or '—':<10} {e['stores']:>6,} stores "
              f"{e['units']:>8,} units{flag}")
    print(f"merged {len(stores):,} stores -> {COMBINED / 'latest.json'}")
    return manifest


# --------------------------------------------------------------------------- record
def store_rows(date: str, brand: str, recs: list[dict]):
    daily, sizes = [], defaultdict(lambda: {"n": 0, "avail": 0, "prices": []})
    for s in recs:
        units = [u for u in s["units"] if u["available"] and u["price"]]
        tens = [u["price"] for u in units if u["size"] == "10x10"]
        prices = [u["price"] for u in units]
        ppsf = [u["price"] / u["sqft"] for u in units if u.get("sqft")]
        daily.append([date, brand, s["store_id"], s["state"], len(units),
                      sum(u["count"] for u in units),
                      min(tens) if tens else "",
                      round(statistics.median(prices), 2) if prices else "",
                      round(statistics.median(ppsf), 3) if ppsf else ""])
        for u in units:
            if u["size"] and s["state"]:
                a = sizes[(s["state"], u["size"])]
                a["n"] += 1
                a["avail"] += u["count"]
                a["prices"].append(u["price"])
    size_rows = [[date, brand, st, sz, a["n"], a["avail"], round(statistics.median(a["prices"]), 2)]
                 for (st, sz), a in sorted(sizes.items())]
    # (store, size) levels: cheapest advertised price and summed count for each
    # size at each store — the series the store popup and the movers tables read.
    store_size_rows = []
    for s in recs:
        per = {}
        for u in s["units"]:
            if not (u["available"] and u["price"] and u["size"]):
                continue
            e = per.setdefault(u["size"], [u["price"], 0])
            e[0] = min(e[0], u["price"])
            e[1] += u["count"]
        for sz, (price, avail) in sorted(per.items()):
            store_size_rows.append([date, brand, s["store_id"], sz, price, avail])
    return daily, size_rows, store_size_rows


def sku_map(recs: list[dict]) -> dict:
    out = {}
    for s in recs:
        for u in s["units"]:
            if u["sku"] and u["price"]:
                out[(s["store_id"], u["sku"])] = (s, u)
    return out


def offer_key(store: dict, unit: dict) -> tuple:
    """A conservative identity for a U-Haul room when its inventory GUID changes."""
    return (store["store_id"], unit.get("size"), unit.get("width"), unit.get("depth"),
            unit.get("height"), unit.get("attrs", ""), bool(unit.get("rent_now")),
            bool(unit.get("reserve")))


def cheapest_offers(recs: list[dict]) -> dict[tuple, tuple]:
    out = {}
    for s in recs:
        for u in s["units"]:
            if not (u.get("available") and u.get("price") and u.get("size")):
                continue
            key = (s["store_id"], u["size"])
            if key not in out or u["price"] < out[key][1]:
                out[key] = (s, u["price"])
    return out


def change_rows(date: str, brand: str, prev: list[dict], cur: list[dict]) -> tuple[list[list], str]:
    old, new = sku_map(prev), sku_map(cur)
    if not old or not new:
        return [], "no priced SKUs on one side"
    overlap = len(set(old) & set(new)) / max(len(new), 1)
    if overlap < 0.5:
        return [], f"SKU overlap only {overlap:.0%} — schema change or full turnover; log withheld"
    # A store present on only one side was not observed (skipped, dead link,
    # partial crawl) — that is a coverage gap, not a listing event. listed /
    # delisted rows are only written for stores seen on both days.
    old_stores = {key[0] for key in old}
    new_stores = {key[0] for key in new}
    both = old_stores & new_stores
    rows = []

    def promo(u):
        return " | ".join(p for p in (u.get("promo", ""), u.get("promo2", "")) if p)

    def compare(old_pair, new_pair, output_sku):
        _old_store, old_unit = old_pair
        new_store, new_unit = new_pair
        sid, site, size = new_store["store_id"], new_store["site_number"], new_unit["size"]
        if old_unit["price"] != new_unit["price"]:
            rows.append([date, brand, sid, site, size, output_sku, "price",
                         old_unit["price"], new_unit["price"]])
        # U-Haul publishes one monthly rate, not separate street and web rates.
        if (brand != "uhaul" and old_unit.get("street_price") != new_unit.get("street_price")
                and new_unit.get("street_price") is not None and old_unit.get("street_price") is not None):
            rows.append([date, brand, sid, site, size, output_sku, "street_price",
                         old_unit["street_price"], new_unit["street_price"]])
        if promo(old_unit) != promo(new_unit):
            rows.append([date, brand, sid, site, size, output_sku, "promo",
                         promo(old_unit), promo(new_unit)])

    exact = set(old) & set(new)
    for key in exact:
        compare(old[key], new[key], key[1])

    matched_old, matched_new = set(), set()
    if brand == "uhaul":
        old_groups, new_groups = defaultdict(list), defaultdict(list)
        for key in set(old) - exact:
            old_groups[offer_key(*old[key])].append(key)
        for key in set(new) - exact:
            new_groups[offer_key(*new[key])].append(key)
        # Only a one-to-one replacement is safe. Ambiguous groups remain ordinary
        # listed/delisted inventory rather than guessed repricing.
        for fingerprint in set(old_groups) & set(new_groups):
            if len(old_groups[fingerprint]) == len(new_groups[fingerprint]) == 1:
                old_key, new_key = old_groups[fingerprint][0], new_groups[fingerprint][0]
                matched_old.add(old_key); matched_new.add(new_key)
                compare(old[old_key], new[new_key], new_key[1])

    for key, (s, u) in new.items():
        if key not in exact and key not in matched_new and s["store_id"] in both:
            rows.append([date, brand, s["store_id"], s["site_number"], u["size"], key[1],
                         "listed", "", u["price"]])
    for key, (s, u) in old.items():
        if key not in exact and key not in matched_old and s["store_id"] in both:
            rows.append([date, brand, s["store_id"], s["site_number"], u["size"], key[1],
                         "delisted", u["price"], ""])

    if brand == "uhaul":
        old_offers, new_offers = cheapest_offers(prev), cheapest_offers(cur)
        for key in sorted(set(old_offers) & set(new_offers)):
            old_store, old_price = old_offers[key]
            new_store, new_price = new_offers[key]
            if old_price != new_price:
                rows.append([date, brand, key[0], new_store["site_number"], key[1],
                             f"uhaul_offer_{key[1]}", "offer_price", old_price, new_price])
    return rows, ""


def cmd_rebuild_changes(brand: str, upto: str) -> None:
    """Recompute a brand's reproducible event log without touching source snapshots."""
    dates = [d for d in snapshot_dates(brand) if d <= upto]
    if not dates:
        raise SystemExit(f"{brand}: no snapshots through {upto}")
    rebuilt = defaultdict(list)
    counts = {}
    notes = {}
    for i, d in enumerate(dates):
        rows, note = [], "first snapshot"
        if i:
            rows, note = change_rows(d, brand, load_snapshot(brand, dates[i - 1]), load_snapshot(brand, d))
        rebuilt[d[:7]].extend(rows)
        counts[d], notes[d] = len(rows), note
    for month in sorted({d[:7] for d in dates}):
        path = COMBINED / f"rate_changes-{month}.csv"
        keep = [[r.get(h, "") for h in CHANGES_HEADER] for r in read_csv(path)
                if r["brand"] != brand or r["date"] > upto]
        atomic_write_csv(path, CHANGES_HEADER, keep + rebuilt[month])
    ledger = read_csv(COMBINED / "record_runs.csv")
    index = {(r["brand"], r["date"]): r for r in ledger}
    for i, d in enumerate(dates):
        if (brand, d) in index:
            index[(brand, d)].update(previous_date=dates[i - 1] if i else "",
                                      events=str(counts[d]), note=notes[d])
    atomic_write_csv(COMBINED / "record_runs.csv", LEDGER_HEADER,
                     [[r.get(h, "") for h in LEDGER_HEADER] for r in ledger])
    print(f"rebuilt {brand}: {sum(counts.values()):,} events across {len(dates):,} snapshots")


def cmd_record(upto: str) -> None:
    ledger = read_csv(COMBINED / "record_runs.csv")
    done = {(r["brand"], r["date"]) for r in ledger}
    ledger_dirty = False
    for brand in BRAND_ORDER:
        dates = [d for d in snapshot_dates(brand) if d <= upto]
        for i, d in enumerate(dates):
            # Each series file is independently idempotent (a (brand, date) that is
            # already there is never appended again), so a series added later —
            # store-sizes arrived 2026-09-05 — fills in for already-recorded days.
            # The ledger gates only the rate-change diff.
            missing = [name for name, hdr in (("daily", DAILY_HEADER), ("sizes", SIZES_HEADER),
                                              ("store-sizes", STORE_SIZES_HEADER))
                       if not any(r["brand"] == brand and r["date"] == d
                                  for r in read_csv(COMBINED / f"{name}-{d[:7]}.csv"))]
            expected_prev = dates[i - 1] if i > 0 else ""
            old_ledger = next((r for r in ledger if r["brand"] == brand and r["date"] == d), None)
            repair_diff = old_ledger is not None and old_ledger["previous_date"] != expected_prev
            if (brand, d) in done and not missing and not repair_diff:
                continue
            cur = load_snapshot(brand, d)
            daily, sizes, store_sizes = store_rows(d, brand, cur)
            if "daily" in missing:
                append_csv(COMBINED / f"daily-{d[:7]}.csv", DAILY_HEADER, daily)
            if "sizes" in missing:
                append_csv(COMBINED / f"sizes-{d[:7]}.csv", SIZES_HEADER, sizes)
            if "store-sizes" in missing:
                append_csv(COMBINED / f"store-sizes-{d[:7]}.csv", STORE_SIZES_HEADER, store_sizes)
                print(f"filled store-sizes for {brand} {d}: {len(store_sizes):,} rows")
            if (brand, d) in done:
                if repair_diff:
                    rows, note = [], "first snapshot"
                    if expected_prev:
                        rows, note = change_rows(d, brand, load_snapshot(brand, expected_prev), cur)
                    changes_path = COMBINED / f"rate_changes-{d[:7]}.csv"
                    keep = [[r.get(h, "") for h in CHANGES_HEADER]
                            for r in read_csv(changes_path)
                            if not (r["brand"] == brand and r["date"] == d)]
                    atomic_write_csv(changes_path, CHANGES_HEADER, keep + rows)
                    old_ledger.update(
                        recorded_at=dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                        previous_date=expected_prev, stores=str(len(cur)), events=str(len(rows)), note=note,
                    )
                    ledger_dirty = True
                    print(f"repaired {brand} {d}: predecessor {expected_prev or '—'}, "
                          f"{len(rows):,} rate events")
                continue
            prev_date, events, note = "", 0, "first snapshot"
            if i > 0:
                prev_date = dates[i - 1]
                prev = load_snapshot(brand, prev_date)
                rows, note = change_rows(d, brand, prev, cur)
                append_csv(COMBINED / f"rate_changes-{d[:7]}.csv", CHANGES_HEADER, rows)
                events = len(rows)
            ledger_values = [dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                             brand, d, prev_date, len(cur), events, note]
            append_csv(COMBINED / "record_runs.csv", LEDGER_HEADER, [ledger_values])
            ledger.append(dict(zip(LEDGER_HEADER, map(str, ledger_values))))
            done.add((brand, d))
            print(f"recorded {brand} {d}: {len(cur):,} stores, {events:,} rate events"
                  + (f" ({note})" if note and note != "first snapshot" else ""))
    if ledger_dirty:
        atomic_write_csv(COMBINED / "record_runs.csv", LEDGER_HEADER,
                         [[r.get(h, "") for h in LEDGER_HEADER] for r in ledger])


def cmd_backfill_ps() -> None:
    """Seed daily-*.csv from the legacy Public Storage series (history/YYYY-MM.csv).

    Those files predate the brand column and carry no state; the state is filled from the
    newest Public Storage snapshot. Days already present for publicstorage are skipped, so this
    is safe to run any number of times.
    """
    latest = snapshot_dates("publicstorage")
    state_by_id = {}
    if latest:
        for s in load_snapshot("publicstorage", latest[-1]):
            state_by_id[s["store_id"]] = s["state"]
    elif (ROOT / "enriched_locations.json").exists():
        for s in normalize("publicstorage", read_json(ROOT / "enriched_locations.json")):
            state_by_id[s["store_id"]] = s["state"]
    for legacy in sorted(HISTORY.glob("????-??.csv")):
        month = legacy.stem
        target = COMBINED / f"daily-{month}.csv"
        have = {r["date"] for r in read_csv(target) if r["brand"] == "publicstorage"}
        rows, dates = [], set()
        for r in read_csv(legacy):
            if r["date"] in have:
                continue
            dates.add(r["date"])
            rows.append([r["date"], "publicstorage", r["store_id"], state_by_id.get(r["store_id"], ""),
                         r["listings"], r["units_avail"], r["cheapest_10x10"], r["median_price"], ""])
        append_csv(target, DAILY_HEADER, rows)
        print(f"backfill {month}: {len(dates)} days, {len(rows):,} rows")
    # Legacy per-store per-size levels, same treatment.
    for legacy in sorted(HISTORY.glob("store-sizes-????-??.csv")):
        month = legacy.stem.split("-", 2)[2]
        target = COMBINED / f"store-sizes-{month}.csv"
        have = {r["date"] for r in read_csv(target) if r["brand"] == "publicstorage"}
        rows = [[r["date"], "publicstorage", r["store_id"], r["size"], r["price"], r["available"]]
                for r in read_csv(legacy) if r["date"] not in have and r["price"]]
        append_csv(target, STORE_SIZES_HEADER, rows)
        print(f"backfill store-sizes {month}: {len(rows):,} rows")
    # Legacy state-by-size series, same treatment.
    for legacy in sorted(HISTORY.glob("sizes-????-??.csv")):
        month = legacy.stem.split("-", 1)[1]
        target = COMBINED / f"sizes-{month}.csv"
        have = {r["date"] for r in read_csv(target) if r["brand"] == "publicstorage"}
        rows = [[r["date"], "publicstorage", r["state"], r["size"], r["listings"], r["units_avail"], r["median_price"]]
                for r in read_csv(legacy) if r["date"] not in have]
        append_csv(target, SIZES_HEADER, rows)


# --------------------------------------------------------------------------- dashboard data
COMMON_SIZES = ["5x5", "5x10", "10x10", "10x15", "10x20", "10x25", "10x30"]


def median(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 2) if xs else None


def _anonymized_dashboard_payload(payload: dict) -> dict:
    """A portfolio-safe twin of the dashboard payload: real brand names replaced
    with numbered stand-ins (stable day to day, since it is keyed off BRAND_ORDER),
    and any brand name baked into a free-text facility name or listing URL scrubbed
    too. Everything else -- prices, counts, trends, states -- is the real data; only
    the labels and the two identifying text/url fields are touched, so this stays
    correct automatically as build-dashboard runs on future days.
    """
    anon = json.loads(json.dumps(payload))  # cheap, dependency-free deep copy
    numbered = {b: f"Storage Company {i + 1}" for i, b in enumerate(BRAND_ORDER)}
    short_numbered = {b: f"C{i + 1}" for i, b in enumerate(BRAND_ORDER)}

    def scrub(text: str, brand: str) -> str:
        real = BRANDS.get(brand, {}).get("label")
        if not text or not real:
            return text
        return re.sub(re.escape(real), numbered.get(brand, real), text, flags=re.IGNORECASE)

    for b, cfg in anon["brands"].items():
        cfg["label"] = numbered.get(b, cfg["label"])
        cfg["short"] = short_numbered.get(b, cfg["short"])
    for s in anon["stores"]:
        s["n"] = scrub(s.get("n", ""), s["b"])
        s["u"] = ""  # the real listing URL'''s domain would name the brand outright
    for m in anon["movers"]:
        m["n"] = scrub(m.get("n", ""), m["b"])
    pilot = anon.get("independent_pilot", {})
    for i, operator in enumerate(pilot.get("operators", []), 1):
        operator["name"] = f"Independent Operator {i}"
        operator["domain"] = ""
        operator["url"] = ""
    return anon


def cmd_build_dashboard(days_of_changes: int = 30) -> None:
    latest_path, man_path = COMBINED / "latest.json", COMBINED / "latest.manifest.json"
    if not latest_path.exists():
        raise SystemExit("build-dashboard: run `merge` first")
    stores, manifest = read_json(latest_path), read_json(man_path)

    # 1. Compact store list. Units collapse to one entry per size: the cheapest advertised
    #    price, its street price, promo, and total advertised count for that size.
    out_stores = []
    size_brand = defaultdict(list)             # (brand, size) -> prices
    state_brand = defaultdict(lambda: {"n": 0, "tens": [], "prices": [], "avail": 0})
    for s in stores:
        by_size = {}
        avail_units = [u for u in s["units"] if u["available"] and u["price"]]
        for u in avail_units:
            e = by_size.get(u["size"])
            cc = "climate" in u["attrs"].lower()
            if e is None or u["price"] < e[0]:
                by_size[u["size"]] = [u["price"], u["street_price"], (e[2] if e else 0) + u["count"],
                                      u["promo"] or u["promo2"], cc, (e[5] if e else 0) + int(u.get("total") or 0)]
            else:
                e[2] += u["count"]
                e[5] += int(u.get("total") or 0)
            size_brand[(s["brand"], u["size"])].append(u["price"])
        tens = [u["price"] for u in avail_units if u["size"] == "10x10"]
        prices = [u["price"] for u in avail_units]
        sb = state_brand[(s["state"], s["brand"])]
        sb["n"] += 1
        sb["avail"] += sum(u["count"] for u in avail_units)
        if tens:
            sb["tens"].append(min(tens))
        sb["prices"].extend(prices)
        out_stores.append({
            "b": s["brand"], "id": s["store_id"], "sn": s["site_number"], "n": s["name"],
            "a": s["address"], "c": s["city"], "s": s["state"], "z": s["zip"],
            "lat": s["lat"], "lng": s["lng"], "u": s["url"], "r": s["rating"], "rv": s["reviews"],
            "l": len(avail_units), "av": sum(u["count"] for u in avail_units),
            "t": min(tens) if tens else None, "m": median(prices),
            "sz": dict(sorted(by_size.items(), key=lambda kv: (parse_size(kv[0])[2] or 1e9, kv[0]))),
            **({"op": s["operator_id"]} if s.get("operator_id") else {}),
            **({"pf": s["platform"]} if s.get("platform") else {}),
        })

    # 2. National size x brand medians (only sizes with a real sample).
    size_table = {}
    for (b, sz), ps in size_brand.items():
        if len(ps) >= 20:
            size_table.setdefault(sz, {})[b] = {"median": median(ps), "n": len(ps),
                                               "p25": round(sorted(ps)[len(ps) // 4], 2),
                                               "p75": round(sorted(ps)[3 * len(ps) // 4], 2)}
    size_order = sorted(size_table, key=lambda z: (parse_size(z)[2] or 1e9, z))

    # 3. State x brand.
    states = {}
    for (st, b), a in state_brand.items():
        if not st:
            continue
        states.setdefault(st, {})[b] = {"stores": a["n"], "avail": a["avail"],
                                        "median_10x10": median(a["tens"]), "median_price": median(a["prices"])}

    # 4. Trends from the recorded daily series: one point per brand per day.
    trend = defaultdict(lambda: {"stores": 0, "avail": 0, "tens": [], "med": []})
    for f in sorted(COMBINED.glob("daily-????-??.csv")):
        for r in read_csv(f):
            t = trend[(r["date"], r["brand"])]
            t["stores"] += 1
            t["avail"] += int(r["units_avail"] or 0)
            if r["cheapest_10x10"]:
                t["tens"].append(float(r["cheapest_10x10"]))
            if r["median_price"]:
                t["med"].append(float(r["median_price"]))
    observed_trends = defaultdict(dict)
    for (d, b), t in sorted(trend.items()):
        observed_trends[b][d] = {"d": d, "stores": t["stores"], "avail": t["avail"],
                                  "t": median(t["tens"]), "m": median(t["med"])}

    # Missing collection days must be explicit nulls.  If they are simply omitted,
    # Chart.js connects the observations on either side and visually invents data.
    trends = defaultdict(list)
    end_date = dt.date.fromisoformat(manifest["date"])
    for b in BRAND_ORDER:
        observed = observed_trends[b]
        if not observed:
            continue
        cursor = dt.date.fromisoformat(min(observed))
        while cursor <= end_date:
            d = cursor.isoformat()
            trends[b].append(observed.get(d, {"d": d, "stores": None, "avail": None,
                                               "t": None, "m": None}))
            cursor += dt.timedelta(days=1)

    # 5. Rate-change activity: per brand per day counts, plus the biggest recent movers.
    cutoff = (dt.date.fromisoformat(manifest["date"]) - dt.timedelta(days=days_of_changes)).isoformat()
    activity = defaultdict(lambda: {"up": 0, "down": 0, "promo": 0, "listed": 0, "delisted": 0})
    movers = []
    name_of = {(s["b"], s["id"]): s for s in out_stores}
    for f in sorted(COMBINED.glob("rate_changes-????-??.csv")):
        if f.stem.split("-", 1)[1] < cutoff[:7]:
            continue
        for r in read_csv(f):
            if r["date"] < cutoff:
                continue
            a = activity[(r["date"], r["brand"])]
            # U-Haul's consumer-facing series is the cheapest currently offered
            # room per facility/size. Exact SKU reprices remain in the audit log,
            # but using both here would double-count a change to the cheapest SKU.
            is_display_price = (r["field"] == "price" and r["brand"] != "uhaul") or (
                r["field"] == "offer_price" and r["brand"] == "uhaul")
            if is_display_price:
                o, n = float(r["old"]), float(r["new"])
                a["up" if n > o else "down"] += 1
                st = name_of.get((r["brand"], r["store_id"]))
                movers.append({"d": r["date"], "b": r["brand"], "id": r["store_id"], "sz": r["size"],
                               "old": o, "new": n, "pct": round((n - o) / o * 100, 1) if o else None,
                               "n": (st["n"] or f"#{st['sn']}") if st else "",
                               "c": st["c"] if st else "", "s": st["s"] if st else ""})
            elif r["field"] in activity[(r["date"], r["brand"])]:
                a[r["field"]] += 1
            elif r["field"] == "promo":
                a["promo"] += 1
    movers.sort(key=lambda m: -abs(m["pct"] or 0))
    activity_rows = [{"d": d, "b": b, **v} for (d, b), v in sorted(activity.items())]

    payload = {
        "built_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "date": manifest["date"],
        "brands": {b: {"label": BRANDS[b]["label"], "short": BRANDS[b]["short"],
                       **manifest["brands"].get(b, {"snapshot_date": None, "age_days": None, "included": False,
                                                    "stores": 0, "priced_stores": 0, "units": 0})}
                   for b in BRAND_ORDER},
        "brand_order": BRAND_ORDER,
        "size_order": size_order,
        "size_table": size_table,
        "states": states,
        "trends": trends,
        "activity": activity_rows,
        "movers": movers[:300],
        "stores": out_stores,
        "independent_pilot": independent_pilot_payload(),
    }
    atomic_write_json(DASHBOARD_DATA, payload)
    atomic_write_json(DASHBOARD_DATA_ANON, _anonymized_dashboard_payload(payload))
    # The directory's file: the full record shape index.html was written against,
    # every brand, units trimmed to the keys the page reads.
    # Kept under Cloudflare's 25 MiB per-asset limit: no SKUs (the page never
    # shows them), no empty strings or nulls, integers where the value is one.
    keep = ("size", "price", "street_price", "available", "count", "promo", "promo2", "attrs", "total")
    tidy = lambda v: int(v) if isinstance(v, float) and v.is_integer() else v
    slim = []
    for s in stores:
        rec = {k: tidy(v) for k, v in s.items() if k != "units" and v not in (None, "", [])}
        rec["units"] = [{k: tidy(u[k]) for k in keep if k in u and u[k] not in (None, "", 0) or k in ("price", "available")}
                        for u in s["units"]]
        slim.append(rec)
    atomic_write_json(ALL_LOCATIONS, slim)
    print(f"all_locations.json: {len(slim):,} stores, {ALL_LOCATIONS.stat().st_size / 1e6:.1f} MB")
    mb = DASHBOARD_DATA.stat().st_size / 1e6
    print(f"dashboard-data.json: {len(out_stores):,} stores, {len(size_order)} sizes, "
          f"{len(states)} states, {sum(len(v) for v in trends.values())} trend points, {mb:.1f} MB")
    print(f"dashboard-data-anon.json: same shape, brands numbered 1-{len(BRAND_ORDER)} for portfolio use")


# --------------------------------------------------------------------------- status / run / daily
def cmd_status(date: str) -> None:
    print(f"{'brand':<15} {'latest':<11} {'age':>4} {'stores':>7} {'units':>8}  source of truth")
    for brand in BRAND_ORDER:
        dates = snapshot_dates(brand)
        if not dates:
            print(f"{BRANDS[brand]['label']:<15} {'—':<11} {'':>4} {'':>7} {'':>8}  no snapshot in history/{brand}/")
            continue
        d = dates[-1]
        recs = load_snapshot(brand, d)
        age = (dt.date.fromisoformat(date) - dt.date.fromisoformat(d)).days
        print(f"{BRANDS[brand]['label']:<15} {d:<11} {age:>3}d {len(recs):>7,} "
              f"{sum(len(r['units']) for r in recs):>8,}  history/{brand}/ ({len(dates)} days)")
    man = COMBINED / "latest.manifest.json"
    if man.exists():
        m = read_json(man)
        print(f"\ncombined: {m['stores']:,} stores as of {m['date']} (built {m['built_at'][:19]}Z)")
    ledger = read_csv(COMBINED / "record_runs.csv")
    if ledger:
        last = ledger[-1]
        print(f"ledger:   {len(ledger)} recordings, last {last['brand']} {last['date']} ({last['events']} events)")


def cmd_run(brand: str, extra: list[str], date: str) -> int:
    cmd = [a.format(date=date) for a in BRANDS[brand]["run"]] + extra
    snapshot_dir(brand).mkdir(parents=True, exist_ok=True)
    print("$", " ".join(cmd), flush=True)
    rc = subprocess.call(cmd, cwd=ROOT)
    if rc not in (0, 3):  # U-Haul returns 3 for complete-with-warnings
        print(f"{brand}: collector exited {rc}; not snapshotting", file=sys.stderr)
        return rc
    cmd_snapshot(brand, None, date, trust_date=True)
    return 0


def cmd_daily(date: str, max_age_days: int) -> None:
    print("== import"); cmd_import(date)
    print("== merge"); cmd_merge(date, max_age_days)
    print("== record"); cmd_record(date)
    print("== dashboard"); cmd_build_dashboard()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", default=today(), help="run date (YYYY-MM-DD), default today")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    sub.add_parser("audit")
    p = sub.add_parser("snapshot"); p.add_argument("brand", choices=BRAND_ORDER)
    p.add_argument("--source", type=Path); p.add_argument("--force", action="store_true")
    p.add_argument("--trust-date", action="store_true", help="file under --date even if the source looks older")
    sub.add_parser("import")
    p = sub.add_parser("merge"); p.add_argument("--max-age-days", type=int, default=3)
    sub.add_parser("record")
    p = sub.add_parser("rebuild-changes"); p.add_argument("brand", choices=BRAND_ORDER)
    sub.add_parser("backfill-ps")
    p = sub.add_parser("build-dashboard"); p.add_argument("--days", type=int, default=30)
    p = sub.add_parser("daily"); p.add_argument("--max-age-days", type=int, default=3)
    p = sub.add_parser("run"); p.add_argument("brand", choices=BRAND_ORDER)
    p.add_argument("extra", nargs=argparse.REMAINDER)
    a = ap.parse_args(argv)

    if a.cmd == "status":
        cmd_status(a.date)
    elif a.cmd == "audit":
        cmd_audit()
    elif a.cmd == "snapshot":
        cmd_snapshot(a.brand, a.source, a.date, a.force, trust_date=a.trust_date)
    elif a.cmd == "import":
        cmd_import(a.date)
    elif a.cmd == "merge":
        cmd_merge(a.date, a.max_age_days)
    elif a.cmd == "record":
        cmd_record(a.date)
    elif a.cmd == "rebuild-changes":
        cmd_rebuild_changes(a.brand, a.date)
    elif a.cmd == "backfill-ps":
        cmd_backfill_ps()
    elif a.cmd == "build-dashboard":
        cmd_build_dashboard(a.days)
    elif a.cmd == "daily":
        cmd_daily(a.date, a.max_age_days)
    elif a.cmd == "run":
        extra = [x for x in a.extra if x != "--"]
        return cmd_run(a.brand, extra, a.date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
