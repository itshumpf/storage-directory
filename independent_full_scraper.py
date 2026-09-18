"""Slow, resumable, all-or-nothing Storable independent snapshot collector."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from independent_registry import deduplicate_facilities
from independent_scraper import (MIN_DELAY, USER_AGENT, HostClient, Operator, Refused,
                                 facility_url_score, load_operators, pull_catalog)
from storable_adapter import NotUS, extract_storable_data, parse_storable_facility
from storagely_parser import parse_facility_html as parse_storagely_facility

CONFIG = Path("independent_collection.json")
REGISTRY = Path("independent_operators.json")
SNAPSHOT_DIR = Path("history/independent")
CATALOG_DIR = SNAPSHOT_DIR / "daily_catalog"
REPORT = SNAPSHOT_DIR / "last_run.json"


def save_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def declared_sitemaps(robots_text: str, origin: str) -> list[str]:
    host = urlparse(origin).netloc.lower()
    result = []
    for match in re.finditer(r"^\s*Sitemap:\s*(\S+)\s*$", robots_text, re.I | re.M):
        url = match.group(1).strip()
        if urlparse(url).netloc.lower() == host and url not in result:
            result.append(url)
    return result


def selected_facility_urls(urls: list[str], minimum_score: int) -> list[str]:
    return sorted({url for url in urls if facility_url_score(url) >= minimum_score})


def canary_first_work(cohort: list[tuple[Operator, dict]],
                      selected_by_operator: dict[str, list[str]]) -> list[tuple[Operator, str, str]]:
    """Try one real page per adapter tenant before expanding multi-location operators."""
    first, remainder = [], []
    for operator, _config in cohort:
        urls = selected_by_operator[operator.operator_id]
        if urls:
            first.append((operator, urls[0], "canary"))
            remainder.extend((operator, url, "collection") for url in urls[1:])
    return first + remainder


def load_config(path: Path, registry_path: Path) -> tuple[list[tuple[Operator, dict]], float]:
    config = json.loads(path.read_text(encoding="utf-8"))
    default_adapter = config.get("adapter")
    supported_adapters = {"storable_apollo", "storagely_html"}
    if default_adapter not in supported_adapters:
        raise ValueError("unsupported independent adapter")
    rows = config.get("operators", [])
    ids = [row.get("operator_id") for row in rows]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("collection config needs unique operators")
    operators = {row.operator_id: row for row in load_operators(registry_path, set(ids))}
    cohort = []
    for row in rows:
        row = dict(row)
        row["adapter"] = row.get("adapter", default_adapter)
        if row["adapter"] not in supported_adapters:
            raise ValueError(f"{row['operator_id']}: unsupported independent adapter")
        operator = operators[row["operator_id"]]
        sitemap = str(row.get("sitemap_url") or "")
        if urlparse(sitemap).netloc.lower() != urlparse(operator.origin).netloc.lower():
            raise ValueError(f"{operator.operator_id}: sitemap is not on the reviewed host")
        if int(row.get("minimum_facilities") or 0) < 1:
            raise ValueError(f"{operator.operator_id}: invalid facility floor")
        cohort.append((operator, row))
    return cohort, float(config.get("max_drop", 0.10))


def parse_facility_page(operator: Operator, url: str, html: str,
                        adapter: str = "storable_apollo") -> dict:
    if adapter == "storagely_html":
        record = parse_storagely_facility(
            html, {"url": url}, operator_id=operator.operator_id)
        if (not record["units"] and record.get("inventory_status") != "sold_out"):
            raise ValueError("facility has no advertised unit groups")
        if any(not unit.get("size") for unit in record["units"]):
            raise ValueError("facility has no advertised unit groups")
        if not record["address"] or not record["state"] or not record["zip"]:
            raise ValueError("facility identity is incomplete")
        return record
    if adapter != "storable_apollo":
        raise ValueError(f"unsupported independent adapter: {adapter}")
    facilities = extract_storable_data(html)["facilities"]["allFacilities"]
    hydrated = [facility for facility in facilities if facility.get("unitGroups")]
    if len(hydrated) != 1:
        raise ValueError(f"expected one hydrated facility, found {len(hydrated)}")
    try:
        record = parse_storable_facility(
            hydrated[0], {"url": url}, brand="independent",
            store_id_prefix=f"ind_{operator.operator_id}", sku_prefix=operator.operator_id,
            fallback_name=operator.name, operator_id=operator.operator_id)
    except NotUS as exc:
        raise ValueError(str(exc)) from exc
    if not record["units"] or any(not unit.get("size") for unit in record["units"]):
        raise ValueError("facility has no advertised unit groups")
    if not record["address"] or not record["state"] or not record["zip"]:
        raise ValueError("facility identity is incomplete")
    return record


def complete_checkpoint_record(record: dict) -> bool:
    units = record.get("units")
    complete_inventory = (isinstance(units, list) and (
        bool(units) or record.get("inventory_status") == "sold_out"))
    return bool(record.get("url") and record.get("address") and record.get("state")
                and record.get("zip") and complete_inventory
                and all(unit.get("size") and unit.get("sku") for unit in units))


def previous_snapshot(snapshot_dir: Path, run_date: str) -> list[dict]:
    paths = sorted(path for path in snapshot_dir.glob("????-??-??.json") if path.stem < run_date)
    return json.loads(paths[-1].read_text(encoding="utf-8")) if paths else []


def fetch_page(client: HostClient, url: str):
    response = client._request(url, spaced=True)
    if 300 <= response.status_code < 400:
        raise RuntimeError(f"unchecked redirect to {response.headers.get('Location', '')}")
    if response.status_code != 200:
        raise RuntimeError(f"facility returned HTTP {response.status_code}")
    return response


def run(args: argparse.Namespace) -> int:
    run_date = args.date or date.today().isoformat()
    snapshot_path = args.snapshot_dir / f"{run_date}.json"
    partial_path = args.snapshot_dir / f"{run_date}.partial.json"
    cohort, max_drop = load_config(args.config, args.registry)
    adapters = {operator.operator_id: config["adapter"] for operator, config in cohort}
    if args.plan:
        rows = [{
            "operator_id": operator.operator_id,
            "minimum_facilities": int(config["minimum_facilities"]),
            "minimum_url_score": int(config["minimum_url_score"]),
            "sitemap_url": config["sitemap_url"],
        } for operator, config in cohort]
        print(json.dumps({
            "network_requests": 0, "publication_enabled": False,
            "operators": rows,
            "minimum_total_facilities": sum(row["minimum_facilities"] for row in rows),
            "request_delay_seconds": args.delay,
        }, indent=2))
        return 0
    if snapshot_path.exists():
        print(f"complete independent snapshot already exists for {run_date}; no requests made")
        return 0
    if args.report.exists():
        old = json.loads(args.report.read_text(encoding="utf-8"))
        if old.get("date") == run_date and old.get("status") == "refused":
            print("a host already refused this run today; no requests made")
            return 2
        if old.get("date") == run_date and old.get("status") == "failed" and not args.retry_failed:
            print("today's run already failed; inspect the report, then use --retry-failed")
            return 2

    partial = json.loads(partial_path.read_text(encoding="utf-8")) if partial_path.exists() else {}
    done = {row["url"]: row for row in partial.get("stores", [])
            if complete_checkpoint_record(row)}
    for record in done.values():
        record.setdefault("identity_source", "facility_id" if record.get("site_number") == record.get("facility_id")
                          else "store_number")
    report = {"date": run_date, "status": "running",
              "started_at": datetime.now(timezone.utc).isoformat(),
              "completed_pages": len(done), "publication_enabled": True}
    save_atomic(args.report, report)
    selected_by_operator = {}
    clients = {}
    try:
        for operator, config in cohort:
            daily_path = args.catalog_dir / run_date / f"{operator.operator_id}.json"
            if daily_path.exists():
                daily = json.loads(daily_path.read_text(encoding="utf-8"))
                if daily.get("date") != run_date or not daily.get("robots_allowed"):
                    raise RuntimeError(f"invalid saved daily catalog: {daily_path}")
                selected = daily["facility_urls"]
                print(f"catalog {operator.operator_id}: reusing {len(selected):,} frozen URLs", flush=True)
            else:
                client = HostClient(operator, args.delay)
                policy = client.robots()
                expected = config["sitemap_url"]
                if expected not in declared_sitemaps(client.robots_text, operator.origin):
                    raise RuntimeError(
                        f"{operator.operator_id}: expected sitemap is no longer declared in robots.txt")
                catalog = pull_catalog(operator, expected, args.delay, client=client, policy=policy)
                selected = selected_facility_urls(catalog["urls"], int(config["minimum_url_score"]))
                floor = int(config["minimum_facilities"])
                if len(selected) < floor:
                    raise RuntimeError(
                        f"{operator.operator_id}: {len(selected)} facilities is below the floor of {floor}")
                if any(not policy.can_fetch(USER_AGENT, url) for url in selected):
                    raise RuntimeError(f"{operator.operator_id}: robots disallows a selected facility")
                daily = {
                    "date": run_date, "operator_id": operator.operator_id,
                    "robots_url": operator.robots_url,
                    "robots_sha256": hashlib.sha256(client.robots_text.encode()).hexdigest(),
                    "robots_allowed": True, "sitemap_files": catalog["sitemap_files"],
                    "url_count": catalog["url_count"], "facility_urls": selected,
                }
                save_atomic(daily_path, daily)
                clients[operator.operator_id] = client
                print(f"catalog {operator.operator_id}: froze {len(selected):,} facility URLs", flush=True)
            selected_by_operator[operator.operator_id] = selected

        wanted = {url for urls in selected_by_operator.values() for url in urls}
        done = {url: record for url, record in done.items() if url in wanted}
        new_pages = 0
        total = len(wanted)
        for operator, url, phase in canary_first_work(cohort, selected_by_operator):
            client = clients.get(operator.operator_id)
            if client is None:
                client = HostClient(operator, args.delay)
                clients[operator.operator_id] = client
            if url in done:
                continue
            if args.limit and new_pages >= args.limit:
                save_atomic(partial_path, {"date": run_date, "stores": list(done.values())})
                report.update(status="limited", completed_pages=len(done),
                              completed_at=datetime.now(timezone.utc).isoformat())
                save_atomic(args.report, report)
                print(f"reached --limit {args.limit}; checkpointed without publishing")
                return 4
            report.update(current_operator=operator.operator_id, current_url=url, phase=phase)
            save_atomic(args.report, report)
            print(f"{phase} {len(done)+1}/{total}: {operator.operator_id}", flush=True)
            response = fetch_page(client, url)
            done[url] = parse_facility_page(
                operator, url, response.text, adapters[operator.operator_id])
            new_pages += 1
            save_atomic(partial_path, {"date": run_date, "stores": list(done.values())})

        if set(done) != wanted:
            raise RuntimeError(f"snapshot incomplete: {len(wanted - set(done))} URLs missing")
        stores = deduplicate_facilities(list(done.values()))
        if not stores or any(not complete_checkpoint_record(store) for store in stores):
            raise RuntimeError("snapshot contains a facility without advertised units")
        previous = previous_snapshot(args.snapshot_dir, run_date)
        units = sum(len(store["units"]) for store in stores)
        previous_units = sum(len(store.get("units", [])) for store in previous)
        if previous and len(stores) < len(previous) * (1 - max_drop):
            raise RuntimeError(f"store count fell more than {max_drop:.0%}")
        if previous_units and units < previous_units * (1 - max_drop):
            raise RuntimeError(f"unit-group count fell more than {max_drop:.0%}")
        save_atomic(snapshot_path, sorted(stores, key=lambda row: row["store_id"]))
        if partial_path.exists():
            partial_path.unlink()
        report.update(status="complete", completed_at=datetime.now(timezone.utc).isoformat(),
                      catalog_pages=len(wanted), facility_count=len(stores), unit_group_count=units)
        save_atomic(args.report, report)
        print(f"snapshot complete: {len(stores):,} stores, {units:,} unit groups -> {snapshot_path}")
        return 0
    except Refused as exc:
        report.update(status="refused", error=str(exc),
                      completed_at=datetime.now(timezone.utc).isoformat(),
                      completed_pages=len(done))
    except Exception as exc:
        report.update(status="failed", error=f"{type(exc).__name__}: {exc}",
                      completed_at=datetime.now(timezone.utc).isoformat(),
                      completed_pages=len(done))
    if done:
        save_atomic(partial_path, {"date": run_date, "stores": list(done.values())})
    save_atomic(args.report, report)
    print(f"STOPPED without publishing: {report['error']}")
    return 2


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="show the cohort with zero network requests")
    parser.add_argument("--date")
    parser.add_argument("--delay", type=float, default=MIN_DELAY)
    parser.add_argument("--limit", type=int, default=0, help="checkpoint after N new pages; never publish")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--catalog-dir", type=Path, default=CATALOG_DIR)
    parser.add_argument("--report", type=Path, default=REPORT)
    args = parser.parse_args()
    if args.delay < MIN_DELAY:
        parser.error(f"--delay cannot be less than {MIN_DELAY:g} seconds")
    return args


if __name__ == "__main__":
    raise SystemExit(run(arguments()))
