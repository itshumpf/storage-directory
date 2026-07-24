"""
extraspace_scraper.py — Crawl every Extra Space facility into our schema.

Updated with stealth enhancements:
  - Uses curl_cffi to spoof browser TLS/JA3 fingerprints.
  - Implements an HTTP Session to carry cookies naturally.
  - Adds aggressive randomized jitter to sleep intervals.
"""
import argparse
import json
import re
import sys
import time
import random
from pathlib import Path

# Upgraded from urllib to curl_cffi for TLS impersonation
from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

from extraspace_parser import parse_facility  # your parser, unchanged

SITEMAP = "https://www.extraspace.com/facility-sitemap.xml"
HOME = "https://www.extraspace.com/storage/facilities/"
FALLBACK_BUILD_ID = "mI_3xeiHvHEYu5qAIouLH"
OUT = Path("extraspace_locations.json")

# A pool of modern user agents to blend in
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

URL_RE = re.compile(r"/storage/facilities/us/([a-z_]+)/([a-z0-9_]+)/(\d+)/")
BUILDID_RE = re.compile(r'"buildId":"([^"]+)"')


def get_with_session(session, url, as_json=False, retries=3):
    """Fetch a URL using the persistent session + browser impersonation."""
    for attempt in range(retries):
        try:
            # Rotate headers slightly per request path
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.extraspace.com/storage/facilities/",
                "Connection": "keep-alive",
            }
            
            # 'impersonate="chrome120"' executes a true Chrome TLS handshake signature
            response = session.get(url, headers=headers, impersonate="chrome120", timeout=30)
            
            if response.status_code == 200:
                return response.json() if as_json else response.text
                
            if response.status_code in (403, 429):
                wait = (20 * (attempt + 1)) + random.uniform(5, 15)  # Multiplicative backoff + chaotic jitter
                print(f"    Blocked ({response.status_code}) on {url.split('/')[-1]} — Backing off {wait:.1f}s")
                time.sleep(wait)
                continue
                
            response.raise_for_status()
            
        except RequestsError as e:
            if attempt < retries - 1:
                time.sleep(10)
                continue
            print(f"    Network error: {e}")
            raise


def live_build_id(session):
    """Scrape the current Next.js buildId using the safe session."""
    try:
        html = get_with_session(session, HOME)
        m = BUILDID_RE.search(html)
        if m:
            return m.group(1)
        print("  buildId not found in page — using fallback")
    except Exception as e:
        print(f"  homepage blocked ({e}) — using fallback buildId")
    return FALLBACK_BUILD_ID


def facility_triples(session):
    """Yield (state, city, site) for every facility in the sitemap."""
    xml = get_with_session(session, SITEMAP)
    seen = set()
    for state, city, site in URL_RE.findall(xml):
        if site not in seen:
            seen.add(site)
            yield state, city, site


def load_progress():
    if OUT.exists():
        try:
            recs = json.loads(OUT.read_text())
            return {str(r.get("site_number")): r for r in recs}
        except Exception:
            pass
    return {}


def main():
    ap = argparse.ArgumentParser()
    # Shifted default delay to 4.0 seconds to prevent aggressive firewall alarms
    ap.add_argument("--delay", type=float, default=4.0, help="base seconds between requests")
    ap.add_argument("--limit", type=int, default=0, help="stop after N new facilities (0 = all)")
    ap.add_argument("--build-id", default=None, help="override the Next.js buildId manually")
    a = ap.parse_args()

    # Instantiate a persistent Session to maintain automated cookie/session handshakes
    session = requests.Session()

    build_id = a.build_id or live_build_id(session)
    print(f"buildId = {build_id}")

    done = load_progress()
    print(f"{len(done)} facilities already saved — resuming\n")

    triples = list(facility_triples(session))
    
    # Shuffle the list so you are not hitting facilities sequentially by ID
    random.shuffle(triples)
    print(f"{len(triples)} facilities in sitemap (shuffled for crawling)\n")

    new = 0
    for i, (state, city, site) in enumerate(triples, 1):
        if site in done:
            continue
            
        url = (f"https://www.extraspace.com/_next/data/{build_id}"
               f"/en-US/storage/facilities/us/{state}/{city}/{site}.json")
        try:
            j = get_with_session(session, url, as_json=True)
            rec = parse_facility(j, site_number=site)
            rec["state_slug"], rec["city_slug"] = state, city
            done[site] = rec
            new += 1
            print(f"[{i}/{len(triples)}] {site} {city},{state} — {len(rec['units'])} units")
        except Exception as e:
            print(f"[{i}] {site} error passing through — skipping: {e}")

        # Continuous save check
        if new and new % 25 == 0:
            OUT.write_text(json.dumps(list(done.values()), indent=2))
            print(f"  …saved {len(done)} total records to json")
            
        if a.limit and new >= a.limit:
            print(f"\nHit --limit {a.limit}, stopping.")
            break
            
        # Jitter: sleep base delay plus or minus a randomized percentage
        actual_sleep = a.delay + random.uniform(-1.5, 4.5)
        time.sleep(max(1.0, actual_sleep))

    OUT.write_text(json.dumps(list(done.values()), indent=2))
    print(f"\nDone. {len(done)} facilities in {OUT}")


if __name__ == "__main__":
    main()