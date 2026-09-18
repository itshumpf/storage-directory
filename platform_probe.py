"""Robots-first, one-page platform fingerprinting for user-supplied operator URLs.

This is classification, not discovery or collection.  It refuses to fetch the
page when robots.txt is missing, unavailable, or disallows our user agent.
"""
from __future__ import annotations

import argparse
import json
import time
import urllib.robotparser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


USER_AGENT = "FindStorageResearch/1.0"
MIN_DELAY = 10.0


def classify_html(url: str, html: str) -> dict:
    text = html.lower()
    host = urlparse(url).hostname or ""
    candidates = []

    def hit(platform, *needles):
        evidence = [needle for needle in needles if needle.lower() in text or needle.lower() in host]
        if evidence:
            candidates.append((len(evidence), platform, evidence))

    hit("storable", ".website.storedge.com", "uploads.website.storedge.com",
        "rental-center.storedge.com", "softwareProvider", "allFacilities", "unitGroups")
    hit("storagely", "static.storagely.link", "storagely-prod-public-assets",
        "storagely")
    hit("g5_marketing_cloud", "inventory.g5marketingcloud.com", "g5dxm.com", "g5_store_id",
        "self-storage-filtered-plus")
    hit("storage_essentials", "storage_essentials", "secompanyid", "seapikey")
    hit("sitelink", "sitelinkstore.com", "smdservers.net", "powered by sitelink")
    hit("tenant_inc", "tenantinc.com", "hummingbird", "mariposa", "superlease")
    hit("self_storage_manager", "selfstoragemanager.com", "self storage manager")
    hit("webselfstorage", "webselfstorage.com", "webselfstorage")
    if not candidates:
        return {"platform": "unknown", "confidence": "none", "evidence": []}
    score, platform, evidence = max(candidates, key=lambda row: row[0])
    return {"platform": platform, "confidence": "high" if score >= 2 else "possible", "evidence": evidence}


def probe_url(url: str, *, session=None, sleep=time.sleep, minimum_delay: float = MIN_DELAY) -> dict:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"not an HTTP(S) URL: {url}")
    page_url = url.split("#", 1)[0]
    origin = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = urljoin(origin, "/robots.txt")
    client = session or requests.Session()
    headers = {"User-Agent": USER_AGENT, "Accept": "text/plain,text/html;q=0.9,*/*;q=0.8"}

    try:
        # Redirects can cross onto a different host whose policy we have not read.
        # Refuse them here; the canonical URL can be reviewed and submitted separately.
        robots = client.get(robots_url, headers=headers, timeout=45, allow_redirects=False)
    except requests.RequestException as exc:
        return {"url": page_url, "robots_url": robots_url, "status": "robots_unavailable",
                "error": f"{type(exc).__name__}: {exc}"}
    if robots.status_code != 200:
        return {"url": page_url, "robots_url": robots_url, "status": "robots_unavailable",
                "robots_status": robots.status_code}
    policy = urllib.robotparser.RobotFileParser(robots_url)
    policy.parse(robots.text.splitlines())
    delay = policy.crawl_delay(USER_AGENT)
    if delay is None:
        delay = policy.crawl_delay("*")
    delay = max(minimum_delay, float(delay or 0))
    result = {"url": page_url, "robots_url": robots_url, "robots_status": 200,
              "crawl_delay": delay}
    if not policy.can_fetch(USER_AGENT, page_url):
        result.update(status="disallowed", platform="unknown", evidence=[])
        return result

    sleep(delay)
    try:
        page = client.get(page_url, headers=headers, timeout=60, allow_redirects=False)
    except requests.RequestException as exc:
        result.update(status="page_unavailable", error=f"{type(exc).__name__}: {exc}")
        return result
    if page.status_code in (403, 429):
        result.update(status="refused", page_status=page.status_code)
        return result
    if page.status_code != 200:
        result.update(status="page_unavailable", page_status=page.status_code)
        return result
    result.update(status="classified", page_status=200, **classify_html(page_url, page.text))
    return result


def _write_atomic(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("urls", nargs="+", help="representative public operator or facility URLs")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--delay", type=float, default=MIN_DELAY)
    args = parser.parse_args(argv)
    if args.delay < MIN_DELAY:
        parser.error(f"--delay cannot be less than {MIN_DELAY:g} seconds")
    results = [probe_url(url, minimum_delay=args.delay) for url in args.urls]
    if args.out:
        _write_atomic(args.out, {"schema_version": 1, "probes": results})
    print(json.dumps(results, indent=2))
    return 0 if all(r["status"] in ("classified", "disallowed") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
