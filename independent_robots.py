"""Checkpointed, robots-only policy audit for disabled independent candidates.

This stage never requests a homepage, sitemap, facility page, or inventory endpoint.
It records only the response from each candidate host's /robots.txt URL so a human
can decide which operators may proceed to the one-page platform probe.
"""
from __future__ import annotations

import argparse
import json
import re
import urllib.robotparser
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

from independent_scraper import USER_AGENT


REGISTRY = Path("independent_operators.json")
OUTPUT_DIR = Path("history/independent/robots")


def _save_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def declared_sitemaps(text: str, origin: str) -> list[str]:
    values = []
    for line in text.splitlines():
        match = re.match(r"\s*sitemap\s*:\s*(\S+)", line, re.I)
        if not match:
            continue
        url = urljoin(origin + "/", match.group(1).strip())
        if urlparse(url).scheme in {"http", "https"}:
            values.append(url)
    return sorted(set(values))


def inspect(operator: dict, session=None) -> dict:
    page_url = str(operator["representative_url"]).split("#", 1)[0]
    parsed = urlparse(page_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = urljoin(origin, "/robots.txt")
    result = {
        "operator_id": operator["operator_id"], "name": operator["name"],
        "domain": operator["domain"], "representative_url": page_url,
        "robots_url": robots_url, "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    client = session or requests.Session()
    try:
        response = client.get(
            robots_url,
            headers={"User-Agent": USER_AGENT, "Accept": "text/plain,*/*;q=0.5"},
            timeout=45,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        result.update(status="unavailable", error=f"{type(exc).__name__}: {exc}")
        return result
    result["http_status"] = response.status_code
    if 300 <= response.status_code < 400:
        result.update(status="redirect_review", location=response.headers.get("Location", ""))
        return result
    if response.status_code in {403, 429}:
        result["status"] = "refused"
        return result
    if response.status_code != 200:
        result["status"] = "unavailable"
        return result
    policy = urllib.robotparser.RobotFileParser(robots_url)
    policy.parse(response.text.splitlines())
    delay = policy.crawl_delay(USER_AGENT)
    if delay is None:
        delay = policy.crawl_delay("*")
    result.update(
        status="reviewed",
        representative_allowed=policy.can_fetch(USER_AGENT, page_url),
        crawl_delay=delay,
        sitemaps=declared_sitemaps(response.text, origin),
        robots_sha256=__import__("hashlib").sha256(response.content).hexdigest(),
    )
    return result


def run(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--operator", action="append", default=[])
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)

    registry = json.loads(args.registry.read_text(encoding="utf-8"))
    requested = set(args.operator)
    operators = [row for row in registry["operators"]
                 if (row.get("status") == "candidate" and not requested)
                 or row.get("operator_id") in requested]
    missing = requested - {row["operator_id"] for row in operators}
    if missing:
        parser.error("unknown operator id(s): " + ", ".join(sorted(missing)))
    output = args.out or OUTPUT_DIR / f"{args.date}.json"
    existing = {}
    if output.exists():
        saved = json.loads(output.read_text(encoding="utf-8"))
        if saved.get("date") == args.date:
            existing = {row["operator_id"]: row for row in saved.get("results", [])}
    for index, operator in enumerate(operators, 1):
        if operator["operator_id"] in existing:
            print(f"robots {index}/{len(operators)}: {operator['operator_id']} (already checked)", flush=True)
            continue
        print(f"robots {index}/{len(operators)}: {operator['operator_id']}", flush=True)
        existing[operator["operator_id"]] = inspect(operator)
        _save_atomic(output, {
            "date": args.date, "requests": "robots.txt only", "publication_enabled": False,
            "results": sorted(existing.values(), key=lambda row: row["operator_id"]),
        })
    print(f"robots-only report; no site pages requested -> {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
