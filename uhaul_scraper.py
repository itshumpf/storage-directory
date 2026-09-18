"""Slow, resumable, all-or-nothing daily snapshots of public U-Haul rates."""
from __future__ import annotations

import argparse
import csv
import json
import random
import time
import urllib.robotparser
from datetime import date, datetime, timezone
from pathlib import Path
from xml.etree import ElementTree

import requests

from uhaul_parser import parse_facility_html, parse_sitemap_xml


BASE = "https://www.uhaul.com"
ROBOTS_URL = f"{BASE}/robots.txt"
INDEX_URL = f"{BASE}/SitemapIndex.xml"
BRAND = "uhaul"
USER_AGENT = "FindStorageResearch/1.0 (public advertised storage rates; contact: braeden@thekeenas.com)"
DEFAULT_DELAY = 5.0
# Corporate centers use regional six-digit entity codes beginning 7, 8, or 9.
# The earlier 7xxxxx-only discovery found 1,076 but omitted whole regions.
# Keep a conservative bootstrap floor; after the first complete snapshot the
# stricter day-over-day guards automatically follow real portfolio changes.
MIN_US_FACILITIES = 1600
MIN_PRICED_FACILITY_RATIO = 0.50
MIN_UNIT_TYPES = 10_000
MAX_ATTEMPTS = 3
MAX_DROP = 0.10
CHECKPOINT_EVERY = 25
# PER-FACILITY FAILURE POLICY. Added 2026-09-04.
#
# Until this date any exception on any single facility ended the run: the
# 2026-09-03 collection died at 1,182 of 2,023 because facility 936078
# redirected to /Error/ — a page U-Haul had removed between the sitemap fetch
# and the request three hours later. Three hours of polite requests and a
# day of prices were lost to one dead link. daily_scraper.py has never worked
# that way: it logs the store, moves on, and judges the RUN by counts.
#
# So now a facility that redirects away or fails to parse is recorded as
# skipped, with its reason, and the run continues. The all-or-nothing guarantee
# moves up a level: the run is complete only when every catalog entry is either
# collected or skipped, and skips stay under these caps. Past the cap it is no
# longer "a few dead links" but a template change or a block, and the run
# stops without publishing exactly as before.
MAX_SKIP_RATIO = 0.02          # ~40 of 2,000 facilities
MAX_CONSECUTIVE_FAILURES = 10  # a streak this long is the host, not the page
US_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY", "DC",
}

DEFAULT_OUTPUT = Path("history/uhaul_locations.json")
DEFAULT_CHECKPOINT = Path("history/uhaul_checkpoint.json")
DEFAULT_REPORT = Path("history/uhaul_last_run.json")
DEFAULT_SNAPSHOT_DIR = Path("history/uhaul")
DEFAULT_CHANGE_LOG = Path("history/uhaul_rate_changes.csv")


def _save_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


class PoliteSession:
    """One deliberately serial HTTP client with spacing and bounded retries."""
    def __init__(self, delay: float, session: requests.Session | None = None):
        self.delay = delay
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xml;q=0.9,*/*;q=0.8"})
        self.last_request = 0.0

    def get(self, url: str) -> requests.Response:
        for attempt in range(MAX_ATTEMPTS):
            wait = self.delay - (time.monotonic() - self.last_request)
            if wait > 0:
                time.sleep(wait)
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
            if response.status_code in (403, 429, 500, 502, 503, 504) and attempt < MAX_ATTEMPTS - 1:
                retry = response.headers.get("Retry-After")
                try:
                    backoff = max(60, int(retry)) if retry else 60 * (attempt + 1)
                except ValueError:
                    backoff = 60 * (attempt + 1)
                time.sleep(backoff + random.uniform(0, 10))
                continue
            response.raise_for_status()
        raise RuntimeError(f"unreachable retry state for {url}")


def _robot_policy(client: PoliteSession) -> urllib.robotparser.RobotFileParser:
    text = client.get(ROBOTS_URL).text
    parser = urllib.robotparser.RobotFileParser(ROBOTS_URL)
    parser.parse(text.splitlines())
    return parser


def _assert_allowed(policy: urllib.robotparser.RobotFileParser, url: str) -> None:
    if not policy.can_fetch(USER_AGENT, url):
        raise RuntimeError(f"robots.txt disallows {url}; stopping without publishing")


def discover_us_facilities(client: PoliteSession, policy: urllib.robotparser.RobotFileParser) -> list[dict]:
    _assert_allowed(policy, INDEX_URL)
    root = ElementTree.fromstring(client.get(INDEX_URL).text)
    maps = sorted({node.text.strip() for node in root.iter() if node.tag.endswith("loc") and node.text
                   and "Sitemap-for-Storage-in-" in node.text
                   and node.text.rsplit("-", 1)[-1].split(".", 1)[0] in US_CODES})
    if len(maps) != len(US_CODES):
        raise RuntimeError(f"Expected {len(US_CODES)} U.S. storage sitemaps, found {len(maps)}")
    stores: dict[str, dict] = {}
    for number, url in enumerate(maps, 1):
        _assert_allowed(policy, url)
        found = parse_sitemap_xml(client.get(url).text)
        for store in found:
            # U-Haul corporate centers use regional six-digit 7xxxxx, 8xxxxx,
            # and 9xxxxx entity numbers. Marketplace affiliates use shorter or
            # zero-prefixed dealer numbers. Georgia's 55 7xxxxx pages exactly
            # match U-Haul's Sep-2025 owned-store disclosure, while official NJ
            # corporate pages demonstrate both the 8xxxxx and 9xxxxx ranges.
            # Facility parsing still checks the visible affiliate label so a
            # future numbering change fails rather than contaminating history.
            if not (len(store["site_number"]) == 6 and store["site_number"][0] in "789"):
                continue
            existing = stores.get(store["store_id"])
            if existing and existing["url"] != store["url"]:
                raise RuntimeError(f"Entity collision for {store['store_id']}")
            stores[store["store_id"]] = store
        print(f"catalog {number}/{len(maps)}: {len(stores):,} unique facilities", flush=True)
    result = sorted(stores.values(), key=lambda x: x["store_id"])
    if len(result) < MIN_US_FACILITIES:
        raise RuntimeError(f"Catalog safety floor failed: {len(result):,} < {MIN_US_FACILITIES:,}")
    return result


def _load_checkpoint(path: Path, run_date: str, catalog_ids: list[str]) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if value.get("date") != run_date:
        return {}
    # CHANGED 2026-09-04. This used to require catalog_ids to match exactly, so
    # one facility added to or dropped from U-Haul's sitemaps between two
    # attempts on the same day threw away every observation already made —
    # hours of five-second-spaced requests, repeated for nothing. The catalog
    # is re-fetched every attempt anyway; what matters is that each kept row
    # is still a facility we intend to collect today.
    wanted = set(catalog_ids)
    return {row["store_id"]: row for row in value.get("stores", []) if row["store_id"] in wanted}


def _load_skipped(path: Path, run_date: str) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value.get("skipped", {})) if value.get("date") == run_date else {}


def _write_checkpoint(path: Path, run_date: str, catalog_ids: list[str], stores: dict[str, dict],
                      skipped: dict[str, str] | None = None) -> None:
    _save_atomic(path, {"date": run_date, "catalog_ids": catalog_ids, "skipped": skipped or {},
                        "stores": sorted(stores.values(), key=lambda x: x["store_id"])})


def _previous_snapshot(snapshot_dir: Path, run_date: str) -> list[dict]:
    candidates = sorted(p for p in snapshot_dir.glob("????-??-??.json") if p.stem < run_date)
    if not candidates:
        return []
    return json.loads(candidates[-1].read_text(encoding="utf-8"))


def _sku_overlap(previous: list[dict], current: list[dict]) -> float | None:
    old = {(s["store_id"], u["sku"]) for s in previous for u in s.get("units", [])}
    new = {(s["store_id"], u["sku"]) for s in current for u in s.get("units", [])}
    if not old or not new:
        return None
    return len(old & new) / min(len(old), len(new))


def _offer_key(store: dict, unit: dict) -> tuple:
    return (store["store_id"], unit.get("size"), unit.get("width"), unit.get("depth"),
            unit.get("height"), unit.get("attrs", ""), bool(unit.get("rent_now")),
            bool(unit.get("reserve")))


def _cheapest_offers(stores: list[dict]) -> dict[tuple, tuple]:
    out = {}
    for store in stores:
        for unit in store.get("units", []):
            if not (unit.get("available") and unit.get("price") and unit.get("size")):
                continue
            key = (store["store_id"], unit["size"])
            if key not in out or unit["price"] < out[key][1]:
                out[key] = (store, unit["price"])
    return out


def _append_changes(path: Path, today: str, previous: list[dict], current: list[dict]) -> None:
    if not previous:
        return
    old_stores = {s["store_id"]: s for s in previous}
    old = {(s["store_id"], u["sku"]): u for s in previous for u in s.get("units", [])}
    new = {(s["store_id"], u["sku"]): (s, u) for s in current for u in s.get("units", [])}
    rows = []
    exact = set(old) & set(new)

    def compare(before, store, unit, sku):
        # There is one advertised monthly price; no duplicate street-price event.
        for field in ("price", "promo", "count", "available"):
            if before.get(field) != unit.get(field):
                rows.append([today, BRAND, store["store_id"], store["site_number"], unit["size"],
                             sku, field, before.get(field), unit.get(field)])

    for key in exact:
        store, unit = new[key]
        compare(old[key], store, unit, key[1])

    old_groups, new_groups = {}, {}
    for key in set(old) - exact:
        old_groups.setdefault(_offer_key(old_stores[key[0]], old[key]), []).append(key)
    for key in set(new) - exact:
        store, unit = new[key]
        new_groups.setdefault(_offer_key(store, unit), []).append(key)
    matched_old, matched_new = set(), set()
    for fingerprint in set(old_groups) & set(new_groups):
        if len(old_groups[fingerprint]) == len(new_groups[fingerprint]) == 1:
            old_key, new_key = old_groups[fingerprint][0], new_groups[fingerprint][0]
            matched_old.add(old_key); matched_new.add(new_key)
            store, unit = new[new_key]
            compare(old[old_key], store, unit, new_key[1])

    for key, before in old.items():
        if key not in exact and key not in matched_old:
            store_id, sku = key
            old_store = old_stores[store_id]
            rows.append([today, BRAND, store_id, old_store["site_number"], before.get("size", ""),
                         sku, "listed", True, False])
    for key, (store, unit) in new.items():
        if key not in exact and key not in matched_new:
            rows.append([today, BRAND, store["store_id"], store["site_number"], unit.get("size", ""),
                         unit["sku"], "listed", False, True])

    old_offers, new_offers = _cheapest_offers(previous), _cheapest_offers(current)
    for key in sorted(set(old_offers) & set(new_offers)):
        _old_store, old_price = old_offers[key]
        store, new_price = new_offers[key]
        if old_price != new_price:
            rows.append([today, BRAND, key[0], store["site_number"], key[1],
                         f"uhaul_offer_{key[1]}", "offer_price", old_price, new_price])
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if not exists:
            writer.writerow(["date", "brand", "store_id", "site_number", "size", "sku",
                             "field", "old", "new"])
        writer.writerows(rows)


def run(args: argparse.Namespace) -> int:
    started = datetime.now(timezone.utc)
    deadline = time.monotonic() + args.max_runtime_minutes * 60 if args.max_runtime_minutes else None
    run_date = args.date or date.today().isoformat()
    report = {"date": run_date, "started_at": started.isoformat(), "status": "running"}
    snapshot_path = args.snapshot_dir / f"{run_date}.json"
    if snapshot_path.exists():
        report.update({"status": "already_complete", "completed_at": started.isoformat(),
                       "snapshot": str(snapshot_path)})
        _save_atomic(args.report, report)
        print(f"Complete snapshot already exists for {run_date}; no requests made.", flush=True)
        return 0
    client = PoliteSession(args.delay)
    try:
        policy = _robot_policy(client)
        catalog = discover_us_facilities(client, policy)
        # A site-wide serial crawl spans hours. Rotate its order by observation
        # date so the same regions are not permanently measured early or late.
        random.Random(run_date).shuffle(catalog)
        catalog_ids = [x["store_id"] for x in catalog]
        done = _load_checkpoint(args.checkpoint, run_date, catalog_ids)
        skipped = _load_skipped(args.checkpoint, run_date)
        max_skips = int(len(catalog) * MAX_SKIP_RATIO)
        report.update({"catalog_count": len(catalog), "resumed_count": len(done), "skipped": skipped})
        print(f"catalog complete: {len(catalog):,}; resuming {len(done):,}", flush=True)
        consecutive_failures = 0
        for index, catalog_store in enumerate(catalog, 1):
            sid = catalog_store["store_id"]
            if sid in done or sid in skipped:
                continue
            if deadline is not None and time.monotonic() >= deadline:
                _write_checkpoint(args.checkpoint, run_date, catalog_ids, done, skipped)
                raise RuntimeError(
                    f"Reached {args.max_runtime_minutes}-minute runtime ceiling after {len(done)} facilities")
            report.update({"current_index": index, "current_store_id": sid, "current_url": catalog_store["url"]})
            # robots.txt is policy, not a page: a disallow is never "skip one".
            _assert_allowed(policy, catalog_store["url"])
            try:
                response = client.get(catalog_store["url"])
                if f"/{catalog_store['site_number']}/" not in response.url:
                    # Removed between the sitemap fetch and now; U-Haul sends
                    # these to /Error/. A dead link, not a parser problem.
                    raise LookupError(f"redirected to {response.url}")
                record = parse_facility_html(response.text, catalog_store)
                if record["is_affiliate"]:
                    raise LookupError("corporate entity pattern selected an affiliate")
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                skipped[sid] = reason
                consecutive_failures += 1
                print(f"facility {index}/{len(catalog)}: {catalog_store['site_number']} SKIPPED — {reason}",
                      flush=True)
                # Save every observation so far, so a same-day repair does not
                # repeat polite requests.
                _write_checkpoint(args.checkpoint, run_date, catalog_ids, done, skipped)
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    raise RuntimeError(
                        f"{consecutive_failures} facilities failed in a row (last: {reason}); "
                        f"the host or the template has changed, not the pages") from exc
                if len(skipped) > max_skips:
                    raise RuntimeError(
                        f"{len(skipped)} facilities skipped, more than {MAX_SKIP_RATIO:.0%} of the catalog; "
                        f"refusing to publish a snapshot with that many holes") from exc
                continue
            consecutive_failures = 0
            done[record["store_id"]] = record
            report["completed_count"] = len(done)
            if len(done) % CHECKPOINT_EVERY == 0 or len(done) + len(skipped) == len(catalog):
                _write_checkpoint(args.checkpoint, run_date, catalog_ids, done, skipped)
            print(f"facility {index}/{len(catalog)}: {record['site_number']} ({len(record['units'])} units)", flush=True)

        if set(done) | set(skipped) != set(catalog_ids):
            missing = sorted(set(catalog_ids) - set(done) - set(skipped))
            raise RuntimeError(f"Snapshot incomplete: {len(missing)} facilities missing")
        if skipped:
            print(f"complete with {len(skipped)} skipped facilities (cap {max_skips})", flush=True)
        current = sorted(done.values(), key=lambda x: x["store_id"])
        previous = _previous_snapshot(args.snapshot_dir, run_date)
        if previous and len(current) < len(previous) * (1 - MAX_DROP):
            raise RuntimeError(f"Snapshot fell more than {MAX_DROP:.0%}: {len(previous)} -> {len(current)}")
        unit_count = sum(len(x["units"]) for x in current)
        priced_facilities = sum(bool(x["units"]) for x in current)
        priced_ratio = priced_facilities / len(current)
        if unit_count < MIN_UNIT_TYPES:
            raise RuntimeError(f"Unit-type safety floor failed: {unit_count:,} < {MIN_UNIT_TYPES:,}")
        if priced_ratio < MIN_PRICED_FACILITY_RATIO:
            raise RuntimeError(
                f"Priced-facility safety floor failed: {priced_facilities:,}/{len(current):,} ({priced_ratio:.1%})")
        previous_units = sum(len(x.get("units", [])) for x in previous)
        if previous_units and unit_count < previous_units * (1 - MAX_DROP):
            raise RuntimeError(
                f"Unit-type count fell more than {MAX_DROP:.0%}: {previous_units:,} -> {unit_count:,}")

        _save_atomic(snapshot_path, current)
        warnings = []
        try:
            _save_atomic(args.output, current)
        except Exception as exc:  # the immutable dated snapshot is authoritative
            warnings.append(f"rolling output failed: {type(exc).__name__}: {exc}")
        if skipped:
            warnings.append(f"{len(skipped)} facilities skipped (dead links or unparseable); see 'skipped'")
            # Not observed is not delisted: keep a skipped facility's rooms out
            # of the diff rather than logging every one of them as gone.
            previous = [x for x in previous if x["store_id"] not in skipped]
        overlap = _sku_overlap(previous, current)
        if overlap is not None and overlap < 0.70:
            warnings.append(f"SKU overlap only {overlap:.1%}; rate-change log withheld")
        else:
            try:
                _append_changes(args.change_log, run_date, previous, current)
            except Exception as exc:  # snapshots are primary; this log is reproducible from them
                warnings.append(f"rate-change log failed: {type(exc).__name__}: {exc}")
        try:
            args.checkpoint.unlink()
        except FileNotFoundError:
            pass
        report.update({
            "status": "complete", "completed_at": datetime.now(timezone.utc).isoformat(),
            "facility_count": len(current),
            "owned_managed_count": sum(not x["is_affiliate"] for x in current),
            "affiliate_count": sum(x["is_affiliate"] for x in current),
            "unit_count": unit_count,
            "priced_facility_count": priced_facilities,
            "priced_facility_ratio": priced_ratio,
            "snapshot": str(snapshot_path),
            "sku_overlap": overlap,
            "skipped": skipped,
            "warnings": warnings,
        })
        if warnings:
            report["status"] = "complete_with_warnings"
        _save_atomic(args.report, report)
        return 3 if warnings else 0
    except Exception as exc:
        report.update({"status": "failed", "completed_at": datetime.now(timezone.utc).isoformat(),
                       "error": f"{type(exc).__name__}: {exc}"})
        _save_atomic(args.report, report)
        print(f"STOPPED without publishing: {exc}", flush=True)
        return 2


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--max-runtime-minutes", type=int, default=330,
                        help="stop cleanly with a checkpoint before the CI job timeout; 0 disables")
    parser.add_argument("--date", help="snapshot date override for controlled backfills/tests")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--snapshot-dir", type=Path, default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--change-log", type=Path, default=DEFAULT_CHANGE_LOG)
    result = parser.parse_args()
    if result.delay < DEFAULT_DELAY:
        parser.error(f"--delay may not be less than the polite floor of {DEFAULT_DELAY} seconds")
    return result


if __name__ == "__main__":
    raise SystemExit(run(arguments()))
