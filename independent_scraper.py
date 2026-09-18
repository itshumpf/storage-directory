#!/usr/bin/env python3
"""Robots-first platform probe for independent storage operators.

The default command is a zero-network plan. ``probe`` makes exactly two requests
per operator (robots.txt, then one representative page after the required delay)
and never publishes a price snapshot. Daily collection stays locked until an
operator has a confirmed adapter and explicit request budget.
"""
from __future__ import annotations

import argparse
import json
import random
import re
import time
import urllib.robotparser
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from independent_registry import validate_operator_registry
from platform_probe import classify_html
from storable_adapter import NotUS, extract_storable_data, parse_storable_facility
from storagely_parser import parse_facility_html as parse_storagely_facility


USER_AGENT = "FindStorageResearch/1.0 (public advertised storage rates; contact: braeden@thekeenas.com)"
MIN_DELAY = 10.0
REGISTRY = Path("independent_operators.json")
PROBE_DIR = Path("history/independent/probes")
CATALOG_DIR = Path("history/independent/catalog")
SMOKE_DIR = Path("history/independent/smoke")
SITEMAP_MANIFEST = Path("independent_sitemaps.json")
ELIGIBLE_STATUSES = {"probe_ready"}
MAX_SITEMAPS_PER_OPERATOR = 50
MAX_URLS_PER_OPERATOR = 5000


class Refused(RuntimeError):
    """A host said no; do not ask it for another page during this run."""


@dataclass(frozen=True)
class Operator:
    operator_id: str
    name: str
    url: str
    platform_hint: str

    @property
    def origin(self) -> str:
        parsed = urlparse(self.url)
        return f"{parsed.scheme}://{parsed.netloc}"

    @property
    def robots_url(self) -> str:
        return urljoin(self.origin, "/robots.txt")


def _save_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def load_operators(path: Path, requested: set[str] | None = None) -> list[Operator]:
    registry = validate_operator_registry(path)
    rows = []
    for item in registry["operators"]:
        if requested and item["operator_id"] not in requested:
            continue
        if not requested and item["status"] not in ELIGIBLE_STATUSES:
            continue
        rows.append(Operator(item["operator_id"], item["name"], item["representative_url"],
                             item["platform_hint"]))
    missing = (requested or set()) - {row.operator_id for row in rows}
    if missing:
        raise ValueError(f"unknown operator id(s): {', '.join(sorted(missing))}")
    return sorted(rows, key=lambda row: row.operator_id)


class HostClient:
    """One serial client scoped to one origin; redirects never cross policy boundaries."""

    def __init__(self, operator: Operator, minimum_delay: float, session=None, sleep=time.sleep):
        self.operator = operator
        self.minimum_delay = minimum_delay
        self.delay = minimum_delay
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT,
                                     "Accept": "text/html,text/plain;q=0.9,*/*;q=0.8"})
        self.sleep = sleep
        self.last_request = 0.0
        self.robots_text = ""

    def _request(self, url: str, *, spaced: bool) -> requests.Response:
        if urlparse(url).netloc.lower() != urlparse(self.operator.origin).netloc.lower():
            raise RuntimeError(f"refusing unchecked host: {url}")
        if spaced:
            wait = self.delay - (time.monotonic() - self.last_request)
            if wait > 0:
                self.sleep(wait + random.uniform(0, 1.0))
        response = self.session.get(url, timeout=60, allow_redirects=False)
        self.last_request = time.monotonic()
        if response.status_code in (403, 429):
            raise Refused(f"HTTP {response.status_code}; host stopped for this run")
        return response

    def robots(self) -> urllib.robotparser.RobotFileParser:
        response = self._request(self.operator.robots_url, spaced=False)
        if response.status_code != 200:
            raise RuntimeError(f"robots.txt returned HTTP {response.status_code}")
        self.robots_text = response.text
        policy = urllib.robotparser.RobotFileParser(self.operator.robots_url)
        policy.parse(response.text.splitlines())
        declared = policy.crawl_delay(USER_AGENT)
        if declared is None:
            declared = policy.crawl_delay("*")
        self.delay = max(self.minimum_delay, float(declared or 0))
        return policy

    def page(self, policy: urllib.robotparser.RobotFileParser, url: str) -> requests.Response:
        if not policy.can_fetch(USER_AGENT, url):
            raise RuntimeError(f"robots.txt disallows {url}")
        response = self._request(url, spaced=True)
        if 300 <= response.status_code < 400:
            raise RuntimeError(f"unchecked redirect to {response.headers.get('Location', '')}")
        if response.status_code != 200:
            raise RuntimeError(f"page returned HTTP {response.status_code}")
        return response


def parse_storable_probe(operator: Operator, html: str) -> dict:
    """Parse every fully hydrated facility on the one probed page, without more requests."""
    data = extract_storable_data(html)
    facilities = data["facilities"]["allFacilities"]
    records, incomplete = [], 0
    for facility in facilities:
        if not facility.get("unitGroups"):
            incomplete += 1
            continue
        try:
            records.append(parse_storable_facility(
                facility, {"url": operator.url}, brand="independent",
                store_id_prefix=f"ind_{operator.operator_id}", sku_prefix=operator.operator_id,
                fallback_name=operator.name, operator_id=operator.operator_id,
            ))
        except NotUS:
            continue
    return {"payload_facilities": len(facilities), "parsed_facilities": len(records),
            "facilities_needing_own_page": incomplete,
            "unit_groups": sum(len(record["units"]) for record in records)}


def parse_storagely_probe(operator: Operator, html: str) -> dict:
    """Validate one Storagely facility page without making another request."""
    record = parse_storagely_facility(
        html, {"url": operator.url}, operator_id=operator.operator_id)
    return {
        "parsed_facilities": 1,
        "unit_groups": len(record["units"]),
        "inventory_status": record.get("inventory_status"),
        "address_complete": bool(record.get("address") and record.get("state") and record.get("zip")),
    }


def parse_sitemap(xml_text: str) -> tuple[str, list[str]]:
    root = ET.fromstring(xml_text)
    kind = root.tag.rsplit("}", 1)[-1].lower()
    if kind not in {"urlset", "sitemapindex"}:
        raise ValueError(f"unexpected sitemap root: {kind}")
    locations = []
    for element in root.iter():
        if element.tag.rsplit("}", 1)[-1].lower() == "loc" and element.text:
            locations.append(element.text.strip())
    return kind, locations


def likely_facility_url(url: str) -> bool:
    path = urlparse(url).path.lower().rstrip("/")
    reject = ("/blog", "/faq", "/contact", "/privacy", "/terms", "/size-guide",
              "/calculator", "/pay-online", "/payonline", "/about")
    if not path or any(token in path for token in reject):
        return False
    signals = ("/storage-units", "/storage-locations/", "/locations/", "/location/",
               "/self-storage", "/units")
    if any(token in path for token in signals):
        return True
    final = path.rsplit("/", 1)[-1]
    return bool(re.match(r"^\d.*-\d{5}$", final) or re.match(r"^self-storage-.+-\d+$", final))


def facility_url_score(url: str) -> int:
    """Rank likely canonical facility pages without requesting any of them."""
    path = urlparse(url).path.lower().strip("/")
    parts = path.split("/") if path else []
    if not parts:
        return 0
    reject = ("blog", "faq", "contact", "privacy", "terms", "size-guide", "calculator",
              "pay-online", "payonline", "about", "reviews", "features", "hours-directions",
              "photo-gallery", "virtual-tours", "climate-controlled", "rv-storage", "boat-storage")
    if any(token in path for token in reject) or "ppc" in path or "duplicate" in path:
        return 0
    final = parts[-1]
    score = 0
    if re.match(r"^\d.*-\d{5}$", final):
        score += 100
    if re.match(r"^self-storage-.+-\d+$", final):
        score += 100
    if re.search(r"-f\d+$", final):
        score += 100
    if path.startswith("storage-units/") and len(parts) >= 4:
        score += 80
    if path.startswith("storage-locations/") and len(parts) >= 4:
        score += 80
        street = r"(?:^|-)(?:st|street|rd|road|ave|avenue|blvd|boulevard|dr|drive|way|hwy|highway|pkwy|parkway|ln|lane|us|state|track|a1a)(?:-|$)"
        if re.match(r"^\d", final) and (re.search(street, final) or re.search(r"-\d+$", final)):
            score += 30
    if path.startswith("self-storage/") and len(parts) == 4:
        score += 70
    if path.startswith("locations/") and len(parts) >= 2:
        score += 40
    return score


def select_smoke_url(urls: list[str]) -> str | None:
    ranked = sorted(((facility_url_score(url), url) for url in urls),
                    key=lambda item: (-item[0], item[1]))
    return ranked[0][1] if ranked and ranked[0][0] > 0 else None


def pull_catalog(operator: Operator, sitemap_url: str, delay: float, session=None,
                 sleep=time.sleep, client: HostClient | None = None,
                 policy: urllib.robotparser.RobotFileParser | None = None) -> dict:
    client = client or HostClient(operator, delay, session=session, sleep=sleep)
    queue, fetched, urls = [sitemap_url], [], []
    while queue:
        if len(fetched) >= MAX_SITEMAPS_PER_OPERATOR:
            raise RuntimeError(f"sitemap index exceeds {MAX_SITEMAPS_PER_OPERATOR} files")
        current = queue.pop(0)
        if current in fetched:
            continue
        if policy is not None and not policy.can_fetch(USER_AGENT, current):
            raise RuntimeError(f"robots.txt disallows sitemap {current}")
        response = client._request(current, spaced=True)
        if 300 <= response.status_code < 400:
            raise RuntimeError(f"sitemap redirects to unchecked URL: {response.headers.get('Location', '')}")
        if response.status_code != 200:
            raise RuntimeError(f"sitemap returned HTTP {response.status_code}")
        kind, locations = parse_sitemap(response.text)
        fetched.append(current)
        if kind == "sitemapindex":
            for location in locations:
                if urlparse(location).netloc.lower() != urlparse(operator.origin).netloc.lower():
                    raise RuntimeError(f"sitemap index points to unchecked host: {location}")
                if location not in fetched and location not in queue:
                    queue.append(location)
        else:
            urls.extend(locations)
        if len(urls) > MAX_URLS_PER_OPERATOR:
            raise RuntimeError(f"catalog exceeds {MAX_URLS_PER_OPERATOR} URLs")
    unique = sorted(set(urls))
    foreign = [url for url in unique if urlparse(url).netloc.lower() != urlparse(operator.origin).netloc.lower()]
    if foreign:
        raise RuntimeError(f"catalog contains {len(foreign)} URLs on unchecked hosts")
    likely = [url for url in unique if likely_facility_url(url)]
    return {"operator_id": operator.operator_id, "status": "complete",
            "sitemap_files": fetched, "url_count": len(unique),
            "likely_facility_count": len(likely), "likely_facility_urls": likely,
            "urls": unique}


def probe_operator(operator: Operator, delay: float, session=None, sleep=time.sleep) -> dict:
    result = {"operator_id": operator.operator_id, "name": operator.name,
              "url": operator.url, "platform_hint": operator.platform_hint,
              "started_at": datetime.now(timezone.utc).isoformat()}
    client = HostClient(operator, delay, session=session, sleep=sleep)
    try:
        policy = client.robots()
        response = client.page(policy, operator.url)
        classification = classify_html(operator.url, response.text)
        result.update(status="classified", robots_status=200, crawl_delay=client.delay,
                      page_status=200, **classification)
        if classification["platform"] == "storable":
            try:
                result["parse"] = parse_storable_probe(operator, response.text)
            except (KeyError, TypeError, ValueError) as exc:
                result["parse_error"] = f"{type(exc).__name__}: {exc}"
        elif classification["platform"] == "storagely":
            try:
                result["parse"] = parse_storagely_probe(operator, response.text)
            except (KeyError, TypeError, ValueError) as exc:
                result["parse_error"] = f"{type(exc).__name__}: {exc}"
    except Refused as exc:
        result.update(status="refused", error=str(exc))
    except Exception as exc:
        result.update(status="failed", error=f"{type(exc).__name__}: {exc}")
    result["completed_at"] = datetime.now(timezone.utc).isoformat()
    return result


def run(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "probe", "catalog", "smoke"), nargs="?", default="plan")
    parser.add_argument("--operator", action="append", default=[])
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--delay", type=float, default=MIN_DELAY)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out", type=Path)
    parser.add_argument("--sitemap-manifest", type=Path, default=SITEMAP_MANIFEST)
    parser.add_argument("--catalog", type=Path)
    args = parser.parse_args(argv)
    if args.delay < MIN_DELAY:
        parser.error(f"--delay cannot be less than {MIN_DELAY:g} seconds")
    operators = load_operators(args.registry, set(args.operator) or None)
    if args.command == "plan":
        print(json.dumps({"network_requests": 0, "operators": [o.__dict__ for o in operators],
                          "probe_request_budget": len(operators) * 2,
                          "publication_enabled": False}, indent=2))
        return 0
    if args.command == "catalog":
        manifest = json.loads(args.sitemap_manifest.read_text(encoding="utf-8"))
        if manifest.get("review_date") != args.date:
            raise SystemExit("sitemap manifest is not from today; refresh robots before catalog collection")
        output = args.out or CATALOG_DIR / f"{args.date}.json"
        existing = {}
        if output.exists():
            saved = json.loads(output.read_text(encoding="utf-8"))
            if saved.get("date") == args.date:
                existing = {row["operator_id"]: row for row in saved.get("results", [])}
        for index, operator in enumerate(operators, 1):
            previous = existing.get(operator.operator_id)
            if previous and previous.get("status") in {"complete", "no_sitemap_declared"}:
                print(f"catalog {index}/{len(operators)}: {operator.operator_id} (already complete)", flush=True)
                continue
            sitemap_url = manifest.get("operators", {}).get(operator.operator_id)
            print(f"catalog {index}/{len(operators)}: {operator.operator_id}", flush=True)
            if not sitemap_url:
                result = {"operator_id": operator.operator_id, "status": "no_sitemap_declared"}
            else:
                try:
                    result = pull_catalog(operator, sitemap_url, args.delay)
                except Exception as exc:
                    result = {"operator_id": operator.operator_id, "status": "failed",
                              "error": f"{type(exc).__name__}: {exc}"}
            existing[operator.operator_id] = result
            _save_atomic(output, {"date": args.date, "publication_enabled": False,
                                  "results": sorted(existing.values(), key=lambda row: row["operator_id"])})
        results = sorted(existing.values(), key=lambda row: row["operator_id"])
        print(f"catalog report only; no facility pages requested -> {output}")
        return 0 if all(r["status"] in {"complete", "no_sitemap_declared"} for r in results) else 2
    if args.command == "smoke":
        catalog_path = args.catalog or CATALOG_DIR / f"{args.date}.json"
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        if catalog.get("date") != args.date:
            raise SystemExit("catalog is not from today; refusing to probe stale URLs")
        catalog_rows = {row["operator_id"]: row for row in catalog.get("results", [])}
        output = args.out or SMOKE_DIR / f"{args.date}.json"
        existing = {}
        if output.exists():
            saved = json.loads(output.read_text(encoding="utf-8"))
            if saved.get("date") == args.date:
                existing = {row["operator_id"]: row for row in saved.get("results", [])}
        for index, operator in enumerate(operators, 1):
            if operator.operator_id in existing:
                print(f"smoke {index}/{len(operators)}: {operator.operator_id} (already attempted)", flush=True)
                continue
            row = catalog_rows.get(operator.operator_id, {})
            url = select_smoke_url(row.get("urls", [])) if row.get("status") == "complete" else None
            print(f"smoke {index}/{len(operators)}: {operator.operator_id}", flush=True)
            if not url:
                result = {"operator_id": operator.operator_id, "name": operator.name,
                          "status": "no_facility_url", "catalog_status": row.get("status", "missing")}
            else:
                target = Operator(operator.operator_id, operator.name, url, operator.platform_hint)
                result = probe_operator(target, args.delay)
            existing[operator.operator_id] = result
            _save_atomic(output, {"date": args.date, "publication_enabled": False,
                                  "max_pages_per_operator": 1,
                                  "results": sorted(existing.values(), key=lambda item: item["operator_id"])})
        results = sorted(existing.values(), key=lambda item: item["operator_id"])
        print(f"smoke report only; no snapshot published -> {output}")
        return 0 if all(row["status"] in {"classified", "no_facility_url"} for row in results) else 2
    results = []
    for index, operator in enumerate(operators, 1):
        print(f"probe {index}/{len(operators)}: {operator.operator_id}", flush=True)
        result = probe_operator(operator, args.delay)
        results.append(result)
        if result["status"] == "refused":
            print(f"  refused; no more requests to {operator.origin} today", flush=True)
    output = args.out or PROBE_DIR / f"{args.date}.json"
    _save_atomic(output, {"date": args.date, "request_budget": len(operators) * 2,
                          "publication_enabled": False, "results": results})
    print(f"probe report only; no snapshot published -> {output}")
    return 0 if all(result["status"] == "classified" for result in results) else 2


if __name__ == "__main__":
    raise SystemExit(run())
