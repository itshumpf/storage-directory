"""
storagemart_scraper.py — Polite, resumable, all-or-nothing daily snapshot of StorageMart rates.

    python storagemart_scraper.py                     # today's snapshot -> history/storagemart/<date>.json
    python storagemart_scraper.py --limit 3           # smoke test: three stores, then stop (never publishes)

Same contract as the other collectors (see daily_scraper.py, uhaul_scraper.py,
cubesmart_scraper.py):
  * robots.txt is read first and a disallow stops the run — it is policy, not a page
  * one serial client, a five-second floor between requests, Retry-After honoured
  * a facility that redirects away, 404s, serves another store's data, or fails
    to parse is recorded as skipped with its reason and the run continues;
    skips are capped (2% of the catalog) and a streak of ten stops the run
  * a 403/429 that survives its retries counts toward a refusal streak of five,
    after which the run stops rather than keep asking a host that is saying no
  * checkpoints go to <date>.partial.json; the dated <date>.json is written once,
    on completion, so a dated file always means a complete day
  * completion = every catalog entry collected or skipped, store count over the
    floor, and no more than a 10% drop in stores or unit groups against the
    previous snapshot. Anything else exits non-zero, keeps the partial, writes
    history/storagemart_last_run.json, and publishes nothing

Discovery is https://www.storage-mart.com/sitemap.xml — a plain urlset of every
page on the site (~700 URLs). Facility pages are picked out by path shape
(/<metro>[/<city>]/<4-digit store>-<street>-<zip5>); Canadian and UK stores
carry non-5-digit postal codes and are excluded, and the parser refuses any
page whose address.country is not US.

Facility pages are ~2 MB each (the whole site state is embedded), so a full run
moves roughly 600 MB. The parser reads one JSON object from that; nothing else
on the page is used.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import urllib.robotparser
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from storagemart_parser import BRAND, BASE, NotUS, parse_facility_html, parse_sitemap_xml

ROBOTS_URL = f"{BASE}/robots.txt"
SITEMAP_URL = f"{BASE}/sitemap.xml"
USER_AGENT = "FindStorageResearch/1.0 (public advertised storage rates; contact: braeden@thekeenas.com)"

DEFAULT_SNAPSHOT_DIR = Path("history/storagemart")
DEFAULT_REPORT = Path("history/storagemart_last_run.json")
DEFAULT_DELAY = 5.0
CHECKPOINT_EVERY = 10

# Safety rails. sitemap.xml held ~200 facility-shaped U.S. URLs on 2026-09-05;
# the first complete run settles the real store count — re-set these to ~90% of it.
MIN_SITEMAP_FACILITIES = 180   # 206 candidate URLs on 2026-09-05
MIN_STORES = 160               # re-set to ~90% of the first complete run's count
MAX_DROP = 0.10
MAX_SKIP_RATIO = 0.02
MAX_CONSECUTIVE_FAILURES = 10
MAX_CONSECUTIVE_REFUSALS = 5
MAX_ATTEMPTS = 3

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class Refused(Exception):
    """403/429 that survived every retry."""


class Dead(Exception):
    """404/410, or a redirect off the facility page."""


def _save_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


class PoliteSession:
    """One deliberately serial HTTP client with spacing and bounded retries."""
    def __init__(self, delay: float):
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT,
                                     "Accept": "text/html,application/xml;q=0.9,*/*;q=0.8"})
        self.last_request = 0.0

    def get(self, url: str) -> requests.Response:
        for attempt in range(MAX_ATTEMPTS):
            wait = self.delay - (time.monotonic() - self.last_request)
            if wait > 0:
                time.sleep(wait + random.uniform(0, 1.5))
            try:
                r = self.session.get(url, timeout=60)
                self.last_request = time.monotonic()
            except requests.RequestException:
                if attempt == MAX_ATTEMPTS - 1:
                    raise
                time.sleep(30 * (attempt + 1))
                continue
            if r.status_code == 200:
                return r
            if r.status_code in (404, 410):
                raise Dead(f"HTTP {r.status_code}")
            if r.status_code in (403, 429, 500, 502, 503, 504) and attempt < MAX_ATTEMPTS - 1:
                retry = r.headers.get("Retry-After")
                try:
                    backoff = max(60, int(retry)) if retry else 60 * (attempt + 1)
                except ValueError:
                    backoff = 60 * (attempt + 1)
                print(f"  [!] HTTP {r.status_code} on {url} — backoff {backoff}s", flush=True)
                time.sleep(backoff + random.uniform(0, 10))
                continue
            if r.status_code in (403, 429):
                raise Refused(f"HTTP {r.status_code} after {MAX_ATTEMPTS} attempts")
            r.raise_for_status()
        raise RuntimeError(f"unreachable retry state for {url}")


def _robot_policy(client: PoliteSession) -> urllib.robotparser.RobotFileParser:
    parser = urllib.robotparser.RobotFileParser(ROBOTS_URL)
    parser.parse(client.get(ROBOTS_URL).text.splitlines())
    return parser


def _assert_allowed(policy, url: str) -> None:
    if not policy.can_fetch(USER_AGENT, url):
        raise RuntimeError(f"robots.txt disallows {url}; stopping without publishing")


def discover(client: PoliteSession, policy) -> list[dict]:
    _assert_allowed(policy, SITEMAP_URL)
    catalog = parse_sitemap_xml(client.get(SITEMAP_URL).text)
    if len(catalog) < MIN_SITEMAP_FACILITIES:
        raise RuntimeError(f"Catalog safety floor failed: {len(catalog)} facility URLs < {MIN_SITEMAP_FACILITIES}")
    print(f"catalog: {len(catalog):,} U.S. facility pages in sitemap.xml", flush=True)
    return catalog


def load_partial(path: Path) -> tuple[dict[str, dict], dict[str, str]]:
    if not path.exists():
        return {}, {}
    try:
        v = json.loads(path.read_text(encoding="utf-8"))
        return {r["url"]: r for r in v.get("stores", [])}, dict(v.get("skipped", {}))
    except (OSError, json.JSONDecodeError, KeyError, AttributeError):
        return {}, {}


def write_partial(path: Path, stores: dict[str, dict], skipped: dict[str, str]) -> None:
    _save_atomic(path, {"stores": sorted(stores.values(), key=lambda x: x["store_id"]), "skipped": skipped})


def previous_snapshot(snapshot_dir: Path, run_date: str) -> list[dict]:
    c = sorted(p for p in snapshot_dir.glob("????-??-??.json") if p.stem < run_date)
    if not c:
        return []
    try:
        return json.loads(c[-1].read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []


def run(args: argparse.Namespace) -> int:
    started = datetime.now(timezone.utc)
    run_date = args.date or date.today().isoformat()
    snapshot_path = args.snapshot_dir / f"{run_date}.json"
    partial_path = args.snapshot_dir / f"{run_date}.partial.json"
    report = {"date": run_date, "started_at": started.isoformat(), "status": "running",
              "snapshot": str(snapshot_path)}
    if snapshot_path.exists():
        report.update({"status": "already_complete", "completed_at": started.isoformat()})
        _save_atomic(args.report, report)
        print(f"Complete snapshot already exists for {run_date}; no requests made.", flush=True)
        return 0
    deadline = time.monotonic() + args.max_runtime_minutes * 60 if args.max_runtime_minutes else None
    client = PoliteSession(args.delay)
    # A skip is per attempt, not per day: a facility that 404'd at 06:10 may be
    # back at 08:00, and re-asking ~40 pages is cheap. Only collected stores resume.
    done, _ = load_partial(partial_path)
    skipped: dict[str, str] = {}
    try:
        policy = _robot_policy(client)
        catalog = discover(client, policy)
        random.Random(run_date).shuffle(catalog)
        # Keyed by URL, not store number: most sitemap URLs carry no store number
        # and the page settles it. A record remembers the URL it was fetched from.
        wanted = {c["url"] for c in catalog}
        done = {k: v for k, v in done.items() if k in wanted}
        max_skips = int(len(catalog) * MAX_SKIP_RATIO)
        report.update({"catalog_count": len(catalog), "resumed_count": len(done)})
        print(f"resuming {len(done):,} from today's partial", flush=True)
        failures = refusals = new_count = 0
        excluded: dict[str, str] = {}
        for index, cat in enumerate(catalog, 1):
            sid = cat["url"]
            if sid in done or sid in skipped:
                continue
            if args.limit and new_count >= args.limit:
                write_partial(partial_path, done, skipped)
                report.update({"status": "limited", "completed_count": len(done),
                               "completed_at": datetime.now(timezone.utc).isoformat()})
                _save_atomic(args.report, report)
                print(f"Reached --limit {args.limit}; stopping without publishing.", flush=True)
                return 4
            if deadline is not None and time.monotonic() >= deadline:
                write_partial(partial_path, done, skipped)
                raise RuntimeError(f"Reached {args.max_runtime_minutes}-minute runtime ceiling after {len(done)} facilities")
            report.update({"current_index": index, "current_url": cat["url"]})
            _assert_allowed(policy, cat["url"])
            try:
                r = client.get(cat["url"])
                if r.url.rstrip("/") != cat["url"].rstrip("/"):
                    raise Dead(f"redirected to {r.url}")
                record = parse_facility_html(r.text, cat)
                refusals = 0
            except NotUS as exc:
                # Canadian/UK stores whose URL looked like a store number. Not a
                # hole in the U.S. snapshot, so not a skip and not against the cap.
                excluded[sid] = str(exc)
                print(f"facility {index}/{len(catalog)}: {cat['url']} excluded — {exc}", flush=True)
                continue
            except Refused as exc:
                refusals += 1
                print(f"facility {index}/{len(catalog)}: {cat['url']} REFUSED — {exc}", flush=True)
                write_partial(partial_path, done, skipped)
                if refusals >= MAX_CONSECUTIVE_REFUSALS:
                    raise RuntimeError(f"{refusals} facilities refused in a row; the host is declining this "
                                       f"session. Stopping — do not retry today without a cooldown.") from exc
                continue
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                skipped[sid] = reason
                failures += 1
                print(f"facility {index}/{len(catalog)}: {cat['url']} SKIPPED — {reason}", flush=True)
                write_partial(partial_path, done, skipped)
                if failures >= MAX_CONSECUTIVE_FAILURES:
                    raise RuntimeError(f"{failures} facilities failed in a row (last: {reason}); the host or "
                                       f"the template has changed, not the pages") from exc
                if len(skipped) > max_skips:
                    raise RuntimeError(f"{len(skipped)} facilities skipped, more than {MAX_SKIP_RATIO:.0%} of the "
                                       f"catalog; refusing to publish a snapshot with that many holes") from exc
                continue
            failures = 0
            done[cat["url"]] = record
            new_count += 1
            report["completed_count"] = len(done)
            if new_count % CHECKPOINT_EVERY == 0:
                write_partial(partial_path, done, skipped)
            print(f"facility {index}/{len(catalog)}: {record['site_number']} {record['city']}, {record['state']} "
                  f"({len(record['units'])} unit groups)", flush=True)

        if set(done) | set(skipped) | set(excluded) != wanted:
            missing = sorted(wanted - set(done) - set(skipped) - set(excluded))
            write_partial(partial_path, done, skipped)
            raise RuntimeError(f"Snapshot incomplete: {len(missing)} facilities never fetched")
        if len(skipped) > max_skips:
            raise RuntimeError(f"{len(skipped)} facilities skipped, more than {MAX_SKIP_RATIO:.0%} of the catalog")
        # Two URLs can resolve to one store (an old path kept alive). One record per
        # store number; the duplicate is noted, not counted.
        by_store: dict[str, dict] = {}
        for rec in done.values():
            if rec["store_id"] in by_store:
                skipped[rec["url"]] = f"duplicate of store {rec['site_number']} ({by_store[rec['store_id']]['url']})"
                continue
            by_store[rec["store_id"]] = rec
        current = sorted(by_store.values(), key=lambda x: x["store_id"])
        if len(current) < MIN_STORES:
            raise RuntimeError(f"Safety floor failed: {len(current)} stores < {MIN_STORES}")
        previous = previous_snapshot(args.snapshot_dir, run_date)
        if previous and len(current) < len(previous) * (1 - MAX_DROP):
            raise RuntimeError(f"Store count fell more than {MAX_DROP:.0%}: {len(previous)} -> {len(current)}")
        units = sum(len(s["units"]) for s in current)
        prev_units = sum(len(s.get("units", [])) for s in previous)
        if prev_units and units < prev_units * (1 - MAX_DROP):
            raise RuntimeError(f"Unit-group count fell more than {MAX_DROP:.0%}: {prev_units:,} -> {units:,}")

        _save_atomic(snapshot_path, current)
        try:
            partial_path.unlink()
        except FileNotFoundError:
            pass
        warnings = [f"{len(skipped)} facilities skipped; see 'skipped'"] if skipped else []
        report.update({
            "status": "complete_with_warnings" if warnings else "complete",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "facility_count": len(current), "unit_group_count": units,
            "priced_facility_count": sum(1 for s in current if s["units"]),
            "skipped": skipped, "excluded_non_us": excluded, "warnings": warnings,
        })
        _save_atomic(args.report, report)
        print(f"\n[OK] Snapshot complete: {len(current):,} stores, {units:,} unit groups -> {snapshot_path}", flush=True)
        return 3 if warnings else 0
    except Exception as exc:
        if done or skipped:
            write_partial(partial_path, done, skipped)
        report.update({"status": "failed", "completed_at": datetime.now(timezone.utc).isoformat(),
                       "error": f"{type(exc).__name__}: {exc}", "completed_count": len(done), "skipped": skipped})
        _save_atomic(args.report, report)
        print(f"\nSTOPPED without publishing: {exc}", flush=True)
        return 2


def arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="StorageMart polite daily snapshot")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    p.add_argument("--max-runtime-minutes", type=int, default=120)
    p.add_argument("--date", help="snapshot date override (YYYY-MM-DD)")
    p.add_argument("--snapshot-dir", type=Path, default=DEFAULT_SNAPSHOT_DIR)
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    p.add_argument("--limit", type=int, default=0, help="smoke test: stop after N stores, never publish")
    a = p.parse_args()
    if a.delay < DEFAULT_DELAY:
        p.error(f"--delay may not be less than the polite floor of {DEFAULT_DELAY} seconds")
    return a


if __name__ == "__main__":
    raise SystemExit(run(arguments()))
