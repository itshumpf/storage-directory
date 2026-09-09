"""
cubesmart_scraper.py — Polite, resumable, all-or-nothing daily snapshot of CubeSmart rates.

    python cubesmart_scraper.py                      # today's snapshot -> history/cubesmart/<date>.json
    python cubesmart_scraper.py --delay 4 --limit 20  # smoke test: twenty stores, then stop (never publishes)

Engineering principles, in the order they were learned:
- Polite pacing: base delay (default 3s) plus randomized human-like jitter;
  the first 403/429 stops the run immediately and is never retried in that
  session — a host that is saying no is not asked again.
- Clean discovery: one fresh, validated facility sitemap is saved per day and
  only URLs in that daily catalog are visited. Same-day resumes reuse it.
- Atomic checkpointing: a partial file is written every 25 stores via temp-file swap.
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
MAX_DEAD_RATIO = 0.02      # proportional cap, further limited by MAX_DEAD_ABSOLUTE
MAX_DEAD_ABSOLUTE = 5      # never probe dozens of stale sitemap links in one session
MAX_ATTEMPTS = 3           # only 5xx and transport errors are retried

# Console UTF-8 setup for Windows terminals
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class Refused(Exception):
    """A 403/429 response; never retried in the same session."""


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


def daily_catalog(session: requests.Session, snapshot_dir: Path, run_date: str) -> tuple[List[str], Path, bool]:
    """Fetch the sitemap once per day, then reuse the exact catalog on resumes."""
    path = snapshot_dir / "catalog" / f"{run_date}.json"
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        urls = payload.get("urls") if isinstance(payload, dict) else None
        if (not isinstance(urls, list) or len(urls) < MIN_SITEMAP
                or not all(isinstance(u, str) and u.startswith("https://www.cubesmart.com/")
                           and site_number_of(u) for u in urls)):
            raise RuntimeError(f"Saved daily catalog is invalid: {path}")
        print(f"1. Reusing today's saved sitemap catalog: {len(urls):,} facilities", flush=True)
        return urls, path, True

    urls = get_facility_urls(session)
    invalid = [u for u in urls if not u.startswith("https://www.cubesmart.com/") or not site_number_of(u)]
    if invalid:
        raise RuntimeError(f"Sitemap contains {len(invalid)} invalid or off-site facility URLs")
    _save_atomic(path, {
        "date": run_date,
        "source": SITEMAP_FACILITY_URL,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "urls": urls,
    })
    print(f"   Saved today's fixed catalog -> {path}", flush=True)
    return urls, path, False


def site_number_of(url: str) -> str | None:
    # 1 to 6 digits. On 2026-09-05 the sitemap grew from 1,519 to 1,580 and the
    # 59 newcomers carry one- and two-digit numbers (/tucson-self-storage/3.html,
    # /mesa-self-storage/69.html). The previous \d{3,6} treated every one of
    # them as "no site number" — and the pre-rewrite crawler had silently
    # dropped the same URLs as parse errors. The catalog check turned it into a
    # stop instead of a hole.
    m = re.search(r"/(\d{1,6})\.html$", url)
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
    """Fetch one page; a single 403/429 ends the session without retrying it."""
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
        if r.status_code in (403, 429):
            raise Refused(f"HTTP {r.status_code}; stopped on the first refusal")
        if r.status_code in (500, 502, 503, 504):
            if attempt == MAX_ATTEMPTS - 1:
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
    dead_path = snapshot_dir / f"{run_date}.dead.json"
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
    failed: Dict[str, str] = {}
    done: Dict[str, dict] = {}
    session = requests.Session()
    try:
        urls, catalog_path, reused_catalog = daily_catalog(session, snapshot_dir, run_date)
        catalog = {site_number_of(u): u for u in urls if site_number_of(u)}
        if len(catalog) != len(urls):
            raise RuntimeError(f"{len(urls) - len(catalog)} sitemap URLs carry no site number; URL pattern changed")
        previous = previous_snapshot(snapshot_dir, run_date)
        previous_ids = {str(r.get("site_number")) for r in previous if r.get("site_number")}
        removed = sorted(previous_ids - set(catalog))
        added = sorted(set(catalog) - previous_ids)
        if previous_ids and len(removed) > len(previous_ids) * MAX_DROP:
            raise RuntimeError(f"Fresh sitemap dropped {len(removed):,} of {len(previous_ids):,} prior stores; stopping before facility requests")

        all_done = load_snapshot(partial_path)
        stale_partial = sorted(set(all_done) - set(catalog))
        done = {site: record for site, record in all_done.items() if site in catalog}
        if dead_path.exists():
            saved_dead = json.loads(dead_path.read_text(encoding="utf-8"))
            if isinstance(saved_dead, dict):
                dead = {str(site): str(reason) for site, reason in saved_dead.items() if str(site) in catalog}
        report.update({"catalog_count": len(catalog), "catalog_path": str(catalog_path),
                       "catalog_reused": reused_catalog, "catalog_added": len(added),
                       "catalog_removed": len(removed), "stale_partial_dropped": len(stale_partial),
                       "resumed_count": len(done), "known_dead_count": len(dead)})
        print(f"2. Resumption check: {len(done):,} facilities already in today's partial file", flush=True)
        print(f"   Catalog delta: +{len(added):,} / -{len(removed):,} versus last complete snapshot", flush=True)

        order = list(catalog)
        random.shuffle(order)   # polite: never the same neighbourhood in a burst
        max_dead = max(1, min(MAX_DEAD_ABSOLUTE, int(len(catalog) * MAX_DEAD_RATIO)))
        if len(dead) >= max_dead:
            raise RuntimeError(f"Daily catalog already reached its {max_dead}-URL dead-link cap; stopping before more facility requests")
        new_count = 0
        for i, site in enumerate(order, 1):
            if site in done or site in dead:
                continue
            if limit and new_count >= limit:
                print(f"\nReached requested limit of {limit} facilities. Stopping without publishing.", flush=True)
                if done:
                    _save_atomic(partial_path, list(done.values()))
                report.update({"status": "limited", "completed_count": len(done),
                               "completed_at": datetime.now(timezone.utc).isoformat()})
                _save_atomic(report_path, report)
                return 4
            url = catalog[site]
            report.update({"current_index": i, "current_site": site, "current_url": url})
            try:
                record = parse_facility_html(fetch_store(session, url), facility_url=url, site_number=site)
            except Dead as e:
                dead[site] = str(e)
                _save_atomic(dead_path, dead)
                print(f"  [dead] #{site}: {e}", flush=True)
                if len(dead) >= max_dead:
                    raise RuntimeError(f"{len(dead)} dead facilities reached today's cap of {max_dead}; "
                                       "stopping instead of probing more stale links") from e
                continue
            except Refused as e:
                print(f"  [refused] #{site}: {e}", flush=True)
                raise RuntimeError("CubeSmart refused a facility request; stopped immediately and will not retry this session") from e
            except Exception as e:
                # A parse failure on one page is worth a line, not the run;
                # the completion check below decides whether the day is usable.
                failed[site] = f"{type(e).__name__}: {e}"
                print(f"  [error] #{site}: {type(e).__name__}: {e}", flush=True)
                if len(dead) + len(failed) >= max_dead:
                    raise RuntimeError(f"{len(dead) + len(failed)} facilities failed, reaching today's cap of {max_dead}; "
                                       "stopping before more requests") from e
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
        missing = sorted(set(catalog) - set(done) - set(dead) - set(failed))
        if missing:
            raise RuntimeError(f"Snapshot incomplete: {len(missing)} facilities never fetched")
        current = sorted(done.values(), key=lambda r: r["site_number"])
        if len(current) < MIN_STORES:
            raise RuntimeError(f"Safety floor failed: {len(current):,} stores < {MIN_STORES:,}")
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
        try:
            dead_path.unlink()
        except FileNotFoundError:
            pass
        report.update({
            "status": "complete_with_warnings" if dead or failed else "complete",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "facility_count": len(current), "unit_count": units,
            "priced_facility_count": sum(1 for s in current if s["units"]),
            "dead": dead, "failed": failed,
        })
        _save_atomic(report_path, report)
        print(f"\n[OK] Snapshot complete: {len(current):,} stores, {units:,} units -> {snapshot_path}"
              + (f"  ({len(dead) + len(failed)} dead/failed, see report)" if dead or failed else ""), flush=True)
        return 3 if dead or failed else 0
    except Exception as exc:
        # Keep what was collected in the .partial file: a rerun today resumes
        # from it instead of repeating polite requests, and nothing downstream
        # ever reads a .partial file as a snapshot.
        if done:
            _save_atomic(partial_path, list(done.values()))
        report.update({"status": "failed", "completed_at": datetime.now(timezone.utc).isoformat(),
                       "error": f"{type(exc).__name__}: {exc}", "completed_count": len(done),
                       "dead": dead, "failed": failed})
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
