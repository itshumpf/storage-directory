"""
cubesmart_scraper.py — Polite, resumable, all-or-nothing daily snapshot of CubeSmart rates.

    python cubesmart_scraper.py                      # today's snapshot -> history/cubesmart/<date>.json
    python cubesmart_scraper.py --delay 4 --limit 20  # smoke test: twenty stores, then stop (never publishes)

Engineering principles, in the order they were learned:
- Polite pacing: base delay (default 3s) plus randomized human-like jitter; a
  403/429 backs off and retries the SAME page, and a streak of refusals stops
  the run — a host that is saying no is not asked again.
- Clean discovery: direct store URLs from https://www.cubesmart.com/sitemap-facility.xml.
- Atomic checkpointing: the dated snapshot is written every 25 stores via temp-file swap.
- Resumption: a second run on the same day continues that day's file; it never
  skips a store because yesterday's file already had it.
- All-or-nothing publication: the snapshot is only declared complete when every
  sitemap URL has been fetched or recorded as dead, the dead count is under a
  cap, and the store count clears both an absolute floor and a 10% day-over-day
  drop check against the previous snapshot. Anything else exits non-zero and
  writes a report, exactly like daily_scraper.py / uhaul_scraper.py.
- FindStorage schema: records match enriched_locations.json (brand "cubesmart").

REWRITTEN 2026-09-04. The previous version had no completion check at all: a
crawl that lost 160 stores to transient errors on 2026-09-02 (1,355 of 1,519)
was saved and reported "Crawl complete!", and the rate-change log then recorded
those stores as delisted and, the next day, listed again. It also defaulted to
a rolling output file whose resume logic skipped every store present from the
previous run — so a second day run with the default path would have collected
nothing and reported success.
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List
from xml.etree import ElementTree

from curl_cffi import requests

from cubesmart_parser import parse_facility_html

BRAND = "cubesmart"
SITEMAP_FACILITY_URL = "https://www.cubesmart.com/sitemap-facility.xml"
IMPERSONATE = "chrome120"

DEFAULT_SNAPSHOT_DIR = Path("history/cubesmart")
DEFAULT_REPORT = Path("history/cubesmart_last_run.json")
DEFAULT_DELAY = 3.0
CHECKPOINT_EVERY = 25

# Safety rails. The sitemap held 1,519 facilities on 2026-09-04.
MIN_SITEMAP = 500          # below this the sitemap itself is broken
MIN_STORES = 1200          # absolute floor for a publishable snapshot
MAX_DROP = 0.10            # vs the previous snapshot, same rule as every other collector
MAX_DEAD_RATIO = 0.02      # 404s / off-site redirects tolerated before it is a template change
MAX_ATTEMPTS = 4           # per page, for 403/429/5xx and transport errors
MAX_CONSECUTIVE_REFUSALS = 5   # 403/429 streak (after retries) => the host is refusing us; stop

# Console UTF-8 setup for Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class Refused(Exception):
    """403/429 that survived every retry."""


class Dead(Exception):
    """404, or a redirect off the facility page. The store is gone, not broken."""


def _save_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
    tmp.replace(path)


def get_facility_urls(session: requests.Session) -> List[str]:
    """Every direct store URL in sitemap-facility.xml."""
    print("1. Downloading CubeSmart facility sitemap...", flush=True)
    r = session.get(SITEMAP_FACILITY_URL, impersonate=IMPERSONATE, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to fetch sitemap: HTTP {r.status_code}")
    root = ElementTree.fromstring(r.text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = sorted({loc.text.strip() for loc in root.findall(".//sm:loc", ns)
                   if loc.text and loc.text.strip().endswith(".html")})
    if len(urls) < MIN_SITEMAP:
        raise RuntimeError(f"Sitemap returned suspicious count: {len(urls)} stores (expected ~1,500+)")
    print(f"   [OK] Discovered {len(urls):,} facilities in sitemap", flush=True)
    return urls


def site_number_of(url: str) -> str | None:
    m = re.search(r"/(\d{3,6})\.html$", url)
    return m.group(1) if m else None


def load_snapshot(path: Path) -> Dict[str, dict]:
    """Today's partial snapshot, keyed by site number, so a rerun resumes it."""
    if not path.exists():
        return {}
    try:
        recs = json.loads(path.read_text(encoding="utf-8"))
        return {str(r.get("site_number")): r for r in recs if r.get("site_number")}
    except Exception as e:
        print(f"  Warning: could not read existing {path} ({e}), starting the day fresh.", flush=True)
        return {}


def previous_snapshot(snapshot_dir: Path, run_date: str) -> list:
    candidates = sorted(p for p in snapshot_dir.glob("????-??-??.json") if p.stem < run_date)
    if not candidates:
        return []
    try:
        return json.loads(candidates[-1].read_text(encoding="utf-8"))
    except Exception:
        return []


def fetch_store(session: requests.Session, url: str) -> str:
    """One facility page, retried politely. Raises Dead / Refused / the transport error."""
    site = site_number_of(url)
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = session.get(url, impersonate=IMPERSONATE, timeout=30)
        except Exception as e:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            time.sleep(15 * (attempt + 1))
            continue
        if r.status_code == 200:
            if site and f"/{site}.html" not in str(r.url):
                raise Dead(f"redirected to {r.url}")
            return r.text
        if r.status_code in (404, 410):
            raise Dead(f"HTTP {r.status_code}")
        if r.status_code in (403, 429, 500, 502, 503, 504):
            if attempt == MAX_ATTEMPTS - 1:
                if r.status_code in (403, 429):
                    raise Refused(f"HTTP {r.status_code} after {MAX_ATTEMPTS} attempts")
                raise RuntimeError(f"HTTP {r.status_code} after {MAX_ATTEMPTS} attempts")
            wait = 40 * (attempt + 1) + random.uniform(5, 15)
            print(f"  [!] HTTP {r.status_code} on {url} — backoff {wait:.0f}s", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"HTTP {r.status_code}")
    raise RuntimeError("unreachable")


def run_crawl(snapshot_dir: Path, report_path: Path, delay: float, limit: int, run_date: str) -> int:
    started = datetime.now(timezone.utc)
    snapshot_path = snapshot_dir / f"{run_date}.json"
    # Checkpoints go to the .partial file; the dated name is written once, on
    # completion, so a file called <date>.json always means a complete day.
    partial_path = snapshot_dir / f"{run_date}.partial.json"
    report = {"date": run_date, "started_at": started.isoformat(), "status": "running",
              "snapshot": str(snapshot_path)}
    if snapshot_path.exists():
        report.update({"status": "already_complete", "completed_at": started.isoformat()})
        _save_atomic(report_path, report)
        print(f"Complete snapshot already exists for {run_date}; no requests made.", flush=True)
        return 0
    print("=" * 60)
    print("CUBESMART STORAGE — POLITE DAILY SNAPSHOT")
    print(f"Snapshot: {snapshot_path}   Base delay: {delay}s (+ jitter)")
    print("=" * 60, flush=True)

    dead: Dict[str, str] = {}
    done: Dict[str, dict] = {}
    session = requests.Session()
    try:
        urls = get_facility_urls(session)
        catalog = {site_number_of(u): u for u in urls if site_number_of(u)}
        if len(catalog) != len(urls):
            raise RuntimeError(f"{len(urls) - len(catalog)} sitemap URLs carry no site number; URL pattern changed")
        done = load_snapshot(partial_path)
        report.update({"catalog_count": len(catalog), "resumed_count": len(done)})
        print(f"2. Resumption check: {len(done):,} facilities already in today's partial file", flush=True)

        order = list(catalog)
        random.shuffle(order)   # polite: never the same neighbourhood in a burst
        max_dead = int(len(catalog) * MAX_DEAD_RATIO)
        new_count, refusals = 0, 0
        for i, site in enumerate(order, 1):
            if site in done:
                continue
            if limit and new_count >= limit:
                print(f"\nReached requested limit of {limit} facilities. Stopping without publishing.", flush=True)
                report.update({"status": "limited", "completed_count": len(done),
                               "completed_at": datetime.now(timezone.utc).isoformat()})
                _save_atomic(report_path, report)
                return 4
            url = catalog[site]
            report.update({"current_index": i, "current_site": site, "current_url": url})
            try:
                record = parse_facility_html(fetch_store(session, url), facility_url=url, site_number=site)
                refusals = 0
            except Dead as e:
                dead[site] = str(e)
                print(f"  [dead] #{site}: {e}", flush=True)
                if len(dead) > max_dead:
                    raise RuntimeError(f"{len(dead)} dead facilities, more than {MAX_DEAD_RATIO:.0%} of the "
                                       f"sitemap — a template or URL change, not closures") from e
                continue
            except Refused as e:
                refusals += 1
                print(f"  [refused] #{site}: {e}", flush=True)
                if refusals >= MAX_CONSECUTIVE_REFUSALS:
                    raise RuntimeError(f"{refusals} facilities refused in a row; the host is declining "
                                       f"this session. Stopping — do not retry today without a cooldown.") from e
                continue
            except Exception as e:
                # A parse failure on one page is worth a line, not the run;
                # the completion check below decides whether the day is usable.
                dead[site] = f"{type(e).__name__}: {e}"
                print(f"  [error] #{site}: {type(e).__name__}: {e}", flush=True)
                if len(dead) > max_dead:
                    raise RuntimeError(f"{len(dead)} facilities failed, more than {MAX_DEAD_RATIO:.0%} of the "
                                       f"sitemap — parser drift, not bad luck") from e
                continue
            done[record["site_number"]] = record
            new_count += 1
            report["completed_count"] = len(done)
            print(f"[{i}/{len(catalog)}] #{record['site_number']} ({record['city']}, {record['state']}): "
                  f"{len(record['units'])} units", flush=True)
            if new_count % CHECKPOINT_EVERY == 0:
                _save_atomic(partial_path, list(done.values()))
            time.sleep(max(1.5, delay + random.uniform(-0.8, 1.8)))

        # ---- completion: every sitemap entry accounted for, and the counts make sense
        missing = sorted(set(catalog) - set(done) - set(dead))
        if missing:
            raise RuntimeError(f"Snapshot incomplete: {len(missing)} facilities never fetched")
        current = sorted(done.values(), key=lambda r: r["site_number"])
        if len(current) < MIN_STORES:
            raise RuntimeError(f"Safety floor failed: {len(current):,} stores < {MIN_STORES:,}")
        previous = previous_snapshot(snapshot_dir, run_date)
        if previous and len(current) < len(previous) * (1 - MAX_DROP):
            raise RuntimeError(f"Store count fell more than {MAX_DROP:.0%}: {len(previous):,} -> {len(current):,}")
        prev_units = sum(len(s.get("units", [])) for s in previous)
        units = sum(len(s["units"]) for s in current)
        if prev_units and units < prev_units * (1 - MAX_DROP):
            raise RuntimeError(f"Unit count fell more than {MAX_DROP:.0%}: {prev_units:,} -> {units:,}")

        _save_atomic(snapshot_path, current)
        try:
            partial_path.unlink()
        except FileNotFoundError:
            pass
        report.update({
            "status": "complete_with_warnings" if dead else "complete",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "facility_count": len(current), "unit_count": units,
            "priced_facility_count": sum(1 for s in current if s["units"]),
            "dead": dead,
        })
        _save_atomic(report_path, report)
        print(f"\n[OK] Snapshot complete: {len(current):,} stores, {units:,} units -> {snapshot_path}"
              + (f"  ({len(dead)} dead/failed, see report)" if dead else ""), flush=True)
        return 3 if dead else 0
    except Exception as exc:
        # Keep what was collected in the .partial file: a rerun today resumes
        # from it instead of repeating polite requests, and nothing downstream
        # ever reads a .partial file as a snapshot.
        if done:
            _save_atomic(partial_path, list(done.values()))
        report.update({"status": "failed", "completed_at": datetime.now(timezone.utc).isoformat(),
                       "error": f"{type(exc).__name__}: {exc}", "completed_count": len(done), "dead": dead})
        _save_atomic(report_path, report)
        print(f"\nSTOPPED without publishing: {exc}", flush=True)
        return 2


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CubeSmart polite daily snapshot")
    parser.add_argument("--snapshot-dir", type=Path, default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--out", type=Path, default=None,
                        help="explicit snapshot path (default: <snapshot-dir>/<date>.json)")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="base seconds between requests")
    parser.add_argument("--limit", type=int, default=0, help="smoke test: stop after N new stores, never publish")
    parser.add_argument("--date", default=None, help="snapshot date override (YYYY-MM-DD)")
    a = parser.parse_args()
    if a.delay < 2.0:
        parser.error("--delay may not be below the polite floor of 2 seconds")
    return a


if __name__ == "__main__":
    a = arguments()
    run_date = a.date or date.today().isoformat()
    snapshot_dir = a.out.parent if a.out else a.snapshot_dir
    if a.out and a.out.stem != run_date:
        # A snapshot is named by its date; anything else would break resume,
        # the drop check, and the pipeline's date detection all at once.
        sys.exit(f"--out must be <dir>/{run_date}.json (got {a.out})")
    sys.exit(run_crawl(snapshot_dir, a.report, a.delay, a.limit, run_date))
