"""Slow, resumable, all-or-nothing U.S. SmartStop daily snapshot collector."""
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

from smartstop_parser import BASE, BRAND, parse_facility_html, parse_sitemap_xml


ROBOTS_URL = f"{BASE}/robots.txt"
SITEMAP_URL = f"{BASE}/xml-sitemap"
USER_AGENT = "FindStorageResearch/1.0"
DEFAULT_DELAY = 10.0
DEFAULT_SNAPSHOT_DIR = Path("history/smartstop")
DEFAULT_REPORT = Path("history/smartstop_last_run.json")
CHECKPOINT_EVERY = 10
MIN_CATALOG = 190              # 213 U.S. URLs on 2026-09-09; 62 Canadian URLs are excluded
MIN_STORES = 185
MIN_UNIT_GROUPS = 1000
MIN_PRICED_FACILITY_RATIO = 0.50
MAX_DROP = 0.10
MAX_SKIP_RATIO = 0.02
MAX_CONSECUTIVE_FAILURES = 8
MAX_ATTEMPTS = 3

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


class Refused(Exception):
    """The host explicitly declined this session; never retry it today."""


class Dead(Exception):
    """A sitemap URL is gone or redirects away from its facility page."""


def _save_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


class PoliteSession:
    def __init__(self, delay: float, session: requests.Session | None = None):
        self.delay = delay
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT,
                                     "Accept": "text/html,application/xml;q=0.9,*/*;q=0.8"})
        self.last_request = 0.0

    def get(self, url: str) -> requests.Response:
        for attempt in range(MAX_ATTEMPTS):
            wait = self.delay - (time.monotonic() - self.last_request)
            if wait > 0:
                time.sleep(wait + random.uniform(0, 1.5))
            try:
                response = self.session.get(url, timeout=60)
                self.last_request = time.monotonic()
            except requests.RequestException:
                if attempt == MAX_ATTEMPTS - 1:
                    raise
                time.sleep(30 * (attempt + 1))
                continue
            if response.status_code == 200:
                return response
            if response.status_code in (403, 429):
                raise Refused(f"HTTP {response.status_code}; stopped on the first refusal")
            if response.status_code in (404, 410):
                raise Dead(f"HTTP {response.status_code}")
            if response.status_code in (500, 502, 503, 504) and attempt < MAX_ATTEMPTS - 1:
                retry = response.headers.get("Retry-After")
                try:
                    backoff = max(60, int(retry)) if retry else 60 * (attempt + 1)
                except ValueError:
                    backoff = 60 * (attempt + 1)
                time.sleep(backoff + random.uniform(0, 10))
                continue
            response.raise_for_status()
        raise RuntimeError("unreachable retry state")


def _robot_policy(client: PoliteSession):
    parser = urllib.robotparser.RobotFileParser(ROBOTS_URL)
    parser.parse(client.get(ROBOTS_URL).text.splitlines())
    declared = parser.crawl_delay("*")
    if declared is not None and client.delay < declared:
        raise RuntimeError(f"robots.txt requires {declared}s crawl delay; configured {client.delay}s")
    return parser


def _assert_allowed(policy, url: str) -> None:
    if not policy.can_fetch(USER_AGENT, url):
        raise RuntimeError(f"robots.txt disallows {url}; stopping without publishing")


def _daily_catalog(client: PoliteSession, policy, snapshot_dir: Path, run_date: str):
    path = snapshot_dir / "catalog" / f"{run_date}.json"
    if path.exists():
        value = json.loads(path.read_text(encoding="utf-8"))
        rows = value.get("facilities") if isinstance(value, dict) else None
        if not isinstance(rows, list) or len(rows) < MIN_CATALOG:
            raise RuntimeError(f"saved daily catalog is invalid: {path}")
        print(f"reusing today's fixed sitemap catalog: {len(rows):,} facilities", flush=True)
        return rows, path
    _assert_allowed(policy, SITEMAP_URL)
    rows = parse_sitemap_xml(client.get(SITEMAP_URL).text)
    if len(rows) < MIN_CATALOG:
        raise RuntimeError(f"catalog safety floor failed: {len(rows):,} < {MIN_CATALOG:,}")
    _save_atomic(path, {"date": run_date, "source": SITEMAP_URL,
                        "fetched_at": datetime.now(timezone.utc).isoformat(), "facilities": rows})
    print(f"saved today's fixed sitemap catalog: {len(rows):,} U.S. facilities", flush=True)
    return rows, path


def _load_partial(path: Path):
    if not path.exists():
        return {}, {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return {s["url"]: s for s in value.get("stores", [])}, dict(value.get("skipped", {}))
    except (OSError, json.JSONDecodeError, KeyError, AttributeError):
        return {}, {}


def _write_partial(path: Path, stores: dict, skipped: dict) -> None:
    _save_atomic(path, {"stores": sorted(stores.values(), key=lambda s: s["store_id"]), "skipped": skipped})


def _previous(snapshot_dir: Path, run_date: str):
    files = sorted(p for p in snapshot_dir.glob("????-??-??.json") if p.stem < run_date)
    return json.loads(files[-1].read_text(encoding="utf-8")) if files else []


def run(args: argparse.Namespace) -> int:
    started = datetime.now(timezone.utc)
    run_date = args.date or date.today().isoformat()
    snapshot_path = args.snapshot_dir / f"{run_date}.json"
    partial_path = args.snapshot_dir / f"{run_date}.partial.json"
    report = {"date": run_date, "started_at": started.isoformat(), "status": "running",
              "snapshot": str(snapshot_path)}
    if snapshot_path.exists():
        report.update(status="already_complete", completed_at=started.isoformat())
        _save_atomic(args.report, report)
        print(f"complete snapshot already exists for {run_date}; no requests made.", flush=True)
        return 0
    client = PoliteSession(args.delay)
    done, _old_skips = _load_partial(partial_path)
    skipped = {}
    deadline = time.monotonic() + args.max_runtime_minutes * 60 if args.max_runtime_minutes else None
    try:
        policy = _robot_policy(client)
        catalog, catalog_path = _daily_catalog(client, policy, args.snapshot_dir, run_date)
        random.Random(run_date).shuffle(catalog)
        wanted = {c["url"] for c in catalog}
        done = {url: store for url, store in done.items() if url in wanted}
        max_skips = max(1, int(len(catalog) * MAX_SKIP_RATIO))
        report.update(catalog_count=len(catalog), catalog_path=str(catalog_path), resumed_count=len(done))
        failures = new_count = 0
        for index, cat in enumerate(catalog, 1):
            url = cat["url"]
            if url in done or url in skipped:
                continue
            if args.limit and new_count >= args.limit:
                _write_partial(partial_path, done, skipped)
                report.update(status="limited", completed_count=len(done),
                              completed_at=datetime.now(timezone.utc).isoformat())
                _save_atomic(args.report, report)
                print(f"reached --limit {args.limit}; stopped without publishing", flush=True)
                return 4
            if deadline and time.monotonic() >= deadline:
                raise RuntimeError(f"runtime ceiling reached after {len(done):,} facilities")
            _assert_allowed(policy, url)
            report.update(current_index=index, current_url=url)
            try:
                response = client.get(url)
                if response.url.rstrip("/") != url.rstrip("/"):
                    raise Dead(f"redirected to {response.url}")
                record = parse_facility_html(response.text, cat)
            except Refused:
                _write_partial(partial_path, done, skipped)
                raise RuntimeError("SmartStop refused this session; stop and wait until tomorrow")
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                skipped[url] = reason
                failures += 1
                _write_partial(partial_path, done, skipped)
                print(f"facility {index}/{len(catalog)} SKIPPED — {reason}", flush=True)
                if failures >= MAX_CONSECUTIVE_FAILURES or len(skipped) > max_skips:
                    raise RuntimeError(f"collection safety limit reached ({len(skipped)} skipped, "
                                       f"{failures} consecutive failures)")
                continue
            failures = 0
            done[url] = record
            new_count += 1
            if new_count % CHECKPOINT_EVERY == 0:
                _write_partial(partial_path, done, skipped)
            print(f"facility {index}/{len(catalog)}: {record['site_number']} "
                  f"({len(record['units'])} offers)", flush=True)

        if set(done) | set(skipped) != wanted:
            raise RuntimeError(f"snapshot incomplete: {len(wanted - set(done) - set(skipped))} URLs missing")
        stores = sorted(done.values(), key=lambda s: s["store_id"])
        if len(stores) < MIN_STORES:
            raise RuntimeError(f"store safety floor failed: {len(stores):,} < {MIN_STORES:,}")
        units = sum(len(s["units"]) for s in stores)
        priced = sum(bool(s["units"]) for s in stores)
        if units < MIN_UNIT_GROUPS or priced / len(stores) < MIN_PRICED_FACILITY_RATIO:
            raise RuntimeError(f"pricing safety floor failed: {units:,} offers across {priced:,}/{len(stores):,} stores")
        previous = _previous(args.snapshot_dir, run_date)
        if previous and len(stores) < len(previous) * (1 - MAX_DROP):
            raise RuntimeError(f"store count fell more than {MAX_DROP:.0%}: {len(previous)} -> {len(stores)}")
        previous_units = sum(len(s.get("units", [])) for s in previous)
        if previous_units and units < previous_units * (1 - MAX_DROP):
            raise RuntimeError(f"offer count fell more than {MAX_DROP:.0%}: {previous_units} -> {units}")
        _save_atomic(snapshot_path, stores)
        try:
            partial_path.unlink()
        except FileNotFoundError:
            pass
        warnings = [f"{len(skipped)} sitemap facilities skipped"] if skipped else []
        report.update(status="complete_with_warnings" if warnings else "complete",
                      completed_at=datetime.now(timezone.utc).isoformat(), facility_count=len(stores),
                      priced_facility_count=priced, offer_count=units, skipped=skipped, warnings=warnings)
        _save_atomic(args.report, report)
        print(f"snapshot complete: {len(stores):,} stores, {units:,} offers -> {snapshot_path}", flush=True)
        return 3 if warnings else 0
    except Exception as exc:
        if done or skipped:
            _write_partial(partial_path, done, skipped)
        report.update(status="failed", completed_at=datetime.now(timezone.utc).isoformat(),
                      error=f"{type(exc).__name__}: {exc}", completed_count=len(done), skipped=skipped)
        _save_atomic(args.report, report)
        print(f"STOPPED without publishing: {exc}", flush=True)
        return 2


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--max-runtime-minutes", type=int, default=90)
    parser.add_argument("--date")
    parser.add_argument("--snapshot-dir", type=Path, default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--limit", type=int, default=0, help="smoke test; never publishes")
    args = parser.parse_args()
    if args.delay < DEFAULT_DELAY:
        parser.error(f"--delay may not be less than robots.txt's {DEFAULT_DELAY:g}-second floor")
    return args


if __name__ == "__main__":
    raise SystemExit(run(arguments()))
