"""
cubesmart_scraper.py — Polite, stealth crawler for CubeSmart Self Storage.

Engineering Principles:
- Polite Pacing: Base delay (2.5s - 4.0s) + randomized human-like jitter.
- Clean Sitemap Discovery: Reads direct store URLs from https://www.cubesmart.com/sitemap-facility.xml.
- Atomic Checkpointing: Saves to disk every 25 stores via temporary file swap.
- Resilient Resumption: Skips already-cached facilities on rerun.
- Fail-Loud Error Handling: Strict checks prevent silent empty writes or corrupt data.
- FindStorage Schema: Produces JSON compatible with FindStorage.pages.dev / enriched_locations.json.
"""
from __future__ import annotations
import argparse
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple
from xml.etree import ElementTree

from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

from cubesmart_parser import parse_facility_html

SITEMAP_FACILITY_URL = "https://www.cubesmart.com/sitemap-facility.xml"
DEFAULT_OUTPUT = Path("cubesmart_locations.json")

# Console UTF-8 setup
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def get_facility_urls(session: requests.Session) -> List[str]:
    """Download and parse sitemap-facility.xml to extract all direct store URLs."""
    print("1. Downloading CubeSmart facility sitemap...", flush=True)
    r = session.get(SITEMAP_FACILITY_URL, impersonate="chrome120", timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"Failed to fetch sitemap: HTTP {r.status_code}")

    root = ElementTree.fromstring(r.text)
    # XML namespace handling
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = []
    for loc in root.findall(".//sm:loc", ns):
        if loc.text and loc.text.endswith(".html"):
            urls.append(loc.text.strip())

    if len(urls) < 500:
        raise ValueError(f"Sitemap returned suspicious count: {len(urls)} stores (expected ~1,500+)")

    print(f"   [OK] Discovered {len(urls):,} facilities in sitemap", flush=True)
    return urls


def load_progress(file_path: Path) -> Dict[str, dict]:
    """Load previously saved progress so a crawl resumes seamlessly."""
    if file_path.exists():
        try:
            recs = json.loads(file_path.read_text(encoding="utf-8"))
            return {str(r.get("site_number")): r for r in recs if r.get("site_number")}
        except Exception as e:
            print(f"  Warning: could not read existing {file_path} ({e}), starting fresh.", flush=True)
    return {}


def atomic_save(file_path: Path, records: List[dict]):
    """Save records atomically via temporary file swap to prevent data corruption."""
    temp_path = file_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    temp_path.replace(file_path)


def run_crawl(output_file: Path = DEFAULT_OUTPUT, delay: float = 3.0, limit: int = 0):
    """Run polite crawl loop for CubeSmart facilities."""
    print("=" * 60, flush=True)
    print("CUBESMART STORAGE — SAFE & POLITE CRAWLER", flush=True)
    print(f"Target Output: {output_file}", flush=True)
    print(f"Base Delay: {delay}s (+ randomized human jitter)", flush=True)
    print("=" * 60, flush=True)

    session = requests.Session()
    urls = get_facility_urls(session)
    done = load_progress(output_file)
    print(f"2. Resumption check: {len(done):,} facilities already cached", flush=True)

    # Polite shuffle
    random.shuffle(urls)

    new_count = 0
    total = len(urls)

    for i, url in enumerate(urls, 1):
        # Extract site number from URL: e.g. /4243.html -> 4243
        m = re.search(r"/(\d{3,6})\.html", url)
        site_num = m.group(1) if m else None
        
        if site_num and site_num in done:
            continue

        try:
            r = session.get(url, impersonate="chrome120", timeout=30)
            if r.status_code == 200:
                store_record = parse_facility_html(r.text, facility_url=url, site_number=site_num)
                key = store_record["site_number"]
                done[key] = store_record
                new_count += 1
                
                u_cnt = len(store_record["units"])
                print(f"[{i}/{total}] Site #{key} ({store_record['city']}, {store_record['state']}): {u_cnt} units", flush=True)
            elif r.status_code in (403, 429):
                wait = 40 + random.uniform(5, 15)
                print(f"  [!] HTTP {r.status_code} on {url} — backoff {wait:.1f}s", flush=True)
                time.sleep(wait)
            else:
                print(f"  [!] HTTP {r.status_code} on {url} — skipping", flush=True)

        except Exception as e:
            print(f"  [ERROR] Failed to process {url}: {e}", flush=True)

        # Checkpoint save every 25 stores
        if new_count and new_count % 25 == 0:
            atomic_save(output_file, list(done.values()))
            print(f"  --> Checkpoint saved: {len(done):,} total facilities on disk", flush=True)

        if limit and new_count >= limit:
            print(f"\nReached requested limit of {limit} facilities. Stopping.", flush=True)
            break

        # Polite human-like jittered pause
        sleep_dur = max(1.5, delay + random.uniform(-0.8, 1.8))
        time.sleep(sleep_dur)

    atomic_save(output_file, list(done.values()))
    print(f"\n✅ Crawl complete! {len(done):,} total CubeSmart stores saved in {output_file}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CubeSmart Polite Crawler")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="Output JSON path")
    parser.add_argument("--delay", type=float, default=3.0, help="Base delay between requests (seconds)")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N new stores (0 = all)")
    args = parser.parse_args()

    run_crawl(output_file=args.out, delay=args.delay, limit=args.limit)
