"""
extraspace_scraper.py — Safe, polite crawler for Extra Space Storage.

Engineering Principles:
- Native TLS Signature: Uses curl_cffi's built-in Chrome headers and TLS cipher ordering
  without manual User-Agent overrides to eliminate WAF/JA3 signature mismatches.
- Polite Pacing: Base delay with human-like randomized jitter to minimize server load.
- Resilient & Resumable: Atomic saves every 25 stores; resumes cleanly if interrupted.
- Fail Loudly: Explicit error logging and structure checks to prevent corrupt data writes.
- Schema Compatibility: Outputs clean JSON ready for FindStorage.pages.dev.
"""
from __future__ import annotations
import argparse
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

from extraspace_parser import parse_facility

SITEMAP_URL = "https://www.extraspace.com/facility-sitemap.xml"
FACILITIES_HOME = "https://www.extraspace.com/storage/facilities/"
DEFAULT_OUTPUT = Path("extraspace_locations.json")
SKIPPED_FILE = Path("extraspace_skipped.json")

# Attempts per facility before it is logged as skipped and the crawl moves on.
MAX_FACILITY_ATTEMPTS = 3

# Consecutive facility failures before the whole crawl stops. A handful of dead
# slugs scattered through the sitemap is normal; dozens in a row is the site
# telling us no, and continuing would be 4,000 more requests it has refused.
CIRCUIT_BREAKER = 15

URL_RE = re.compile(r"/storage/facilities/us/([a-z_]+)/([a-z0-9_]+)/(\d+)/")
BUILDID_RE = re.compile(r'"buildId":"([^"]+)"')


class SessionRefused(RuntimeError):
    """The site is refusing this session outright, not just one facility.

    Raised when an ordinary top-level page will not load. There is no retry
    strategy for this that is not just pushing harder against a no, so it ends
    the crawl immediately instead of being counted toward the skip budget.
    """


class HttpStatus(RuntimeError):
    """A non-200 the caller may want to interpret rather than just retry.

    A 403 here is ambiguous on its own: it can mean the buildId went stale
    because Extra Space redeployed mid-crawl, or that this particular facility
    has no static JSON twin, or that we are genuinely being refused. Those want
    three different responses, so the status is carried up rather than swallowed.
    """

    def __init__(self, status: int, url: str):
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} on {url}")


def get_with_session(session: requests.Session, url: str, as_json: bool = False,
                     retries: int = 3, backoff: bool = True) -> Any:
    """Fetch URL using curl_cffi's native Chrome 120 TLS & HTTP/2 signature.

    NOTE: We do NOT pass custom User-Agent headers here. Overriding headers breaks
    the JA3/Client-Hint fingerprint and triggers anti-bot WAFs.

    backoff=False raises HttpStatus immediately on 403/404/429 instead of
    sleeping. The per-facility loop uses that so it can check for a redeploy
    before deciding a refusal is real; the sitemap and buildId fetches keep the
    old sleep-and-retry behaviour because for those there is nothing else to try.
    """
    for attempt in range(retries):
        try:
            # Let curl_cffi generate authentic Chrome headers natively
            response = session.get(url, impersonate="chrome120", timeout=30)

            if response.status_code == 200:
                return response.json() if as_json else response.text

            if response.status_code in (403, 404, 429):
                if not backoff:
                    raise HttpStatus(response.status_code, url)
                wait = (30 * (attempt + 1)) + random.uniform(10, 20)
                print(f"  [!] HTTP {response.status_code} on {url} — polite backoff {wait:.1f}s (attempt {attempt+1}/{retries})", flush=True)
                time.sleep(wait)
                continue

            response.raise_for_status()

        except RequestsError as e:
            if attempt < retries - 1:
                time.sleep(10 + (attempt * 5))
                continue
            raise RuntimeError(f"Network request failed after {retries} retries: {url} -> {e}")

    raise RuntimeError(f"Failed to fetch {url} (status code returned blocked/unreachable)")


def get_live_build_id(session: requests.Session) -> str:
    """Extract the current Next.js buildId from the live facilities landing page.

    Doubles as the session warm-up: this is an ordinary top-level page fetch on
    the same session, so whatever cookies the site sets for a normal visitor are
    set here, before any deep link is requested. Nothing here inspects, forges or
    works around a bot-detection token -- if the site refuses us, the crawler's
    answer is to slow down and then stop, not to try harder.
    """
    html = get_with_session(session, FACILITIES_HOME)
    m = BUILDID_RE.search(html)
    if not m:
        raise ValueError("Could not extract Next.js buildId from Extra Space facilities homepage!")
    return m.group(1)


def probe_build_id(session: requests.Session) -> Optional[str]:
    """One cheap look at the landing page. Never sleeps, never raises.

    Used only for the mid-crawl "did they redeploy, or are we blocked?" check.
    That check must be fast: the version of this that called get_live_build_id()
    inherited its sleep-and-retry, so a single refused facility cost four
    minutes -- two backing off the facility, two backing off the landing page --
    and the circuit breaker could never reach 15 in any useful time.

    Returns None if the landing page itself will not load. That is not a
    redeploy and not a dead slug: an ordinary top-level page refusing us means
    the session is being refused, and the caller should stop rather than probe
    further.
    """
    try:
        html = get_with_session(session, FACILITIES_HOME, retries=1, backoff=False)
    except Exception:
        return None
    m = BUILDID_RE.search(html)
    return m.group(1) if m else None


def facility_url(build_id: str, state: str, city: str, site: str) -> str:
    return (f"https://www.extraspace.com/_next/data/{build_id}"
            f"/en-US/storage/facilities/us/{state}/{city}/{site}.json")


def get_facility_triples(session: requests.Session) -> List[Tuple[str, str, str]]:
    """Download facility sitemap and extract all (state, city, site_number) listings."""
    xml = get_with_session(session, SITEMAP_URL)
    triples = []
    seen = set()
    for state, city, site in URL_RE.findall(xml):
        if site not in seen:
            seen.add(site)
            triples.append((state, city, site))
    
    if len(triples) < 1000:
        raise ValueError(f"Sitemap returned an unusually low store count ({len(triples)}). Expected ~3,500+.")
    
    return triples


def load_progress(file_path: Path) -> Dict[str, dict]:
    """Load previously saved progress so a crawl can resume seamlessly."""
    if file_path.exists():
        try:
            recs = json.loads(file_path.read_text(encoding="utf-8"))
            return {str(r.get("site_number")): r for r in recs if r.get("site_number")}
        except Exception as e:
            print(f"  Warning: could not read existing {file_path} ({e}), starting fresh.", flush=True)
    return {}


def atomic_save(file_path: Path, records: List[dict]):
    """Save records atomically via a temporary file swap to prevent data corruption."""
    temp_path = file_path.with_suffix(".tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    temp_path.replace(file_path)


def run_crawl(output_file: Path = DEFAULT_OUTPUT, delay: float = 3.0, limit: int = 0):
    """Run the polite crawl loop."""
    print("=" * 60, flush=True)
    print("EXTRA SPACE STORAGE — SAFE & POLITE CRAWLER", flush=True)
    print(f"Target Output: {output_file}", flush=True)
    print(f"Base Delay: {delay}s (+ randomized human jitter)", flush=True)
    print("=" * 60, flush=True)

    session = requests.Session()
    
    print("1. Extracting live Next.js buildId...", flush=True)
    build_id = get_live_build_id(session)
    print(f"   ✓ Live buildId: {build_id}", flush=True)

    print("2. Downloading facility sitemap...", flush=True)
    triples = get_facility_triples(session)
    print(f"   ✓ Discovered {len(triples):,} facilities in sitemap", flush=True)

    done = load_progress(output_file)
    print(f"3. Resumption check: {len(done):,} facilities already cached", flush=True)

    # Randomize order so we don't hammer facilities in strict sequential numerical order
    random.shuffle(triples)

    new_count = 0
    total = len(triples)
    skipped: List[dict] = []
    consecutive_fail = 0
    redeploys = 0

    for i, (state, city, site) in enumerate(triples, 1):
        if site in done:
            continue

        try:
            data = None
            for attempt in range(1, MAX_FACILITY_ATTEMPTS + 1):
                url = facility_url(build_id, state, city, site)
                try:
                    data = get_with_session(session, url, as_json=True,
                                            retries=1, backoff=False)
                    break
                except HttpStatus as e:
                    # Do not assume a ban. A redeploy invalidates every buildId
                    # URL at once and looks identical to being blocked.
                    live = probe_build_id(session)
                    if live is None:
                        # The landing page will not load either. Nothing about
                        # this facility is special; the session is being refused.
                        raise SessionRefused(
                            f"landing page {FACILITIES_HOME} also refused while "
                            f"checking site #{site}") from e
                    if live != build_id:
                        redeploys += 1
                        print(f"  [~] buildId rotated {build_id} -> {live} "
                              f"(redeploy #{redeploys}); retrying site #{site}", flush=True)
                        build_id = live
                        continue
                    if attempt < MAX_FACILITY_ATTEMPTS:
                        wait = (30 * attempt) + random.uniform(10, 20)
                        print(f"  [!] HTTP {e.status} on site #{site}, buildId unchanged "
                              f"— backoff {wait:.1f}s (attempt {attempt}/{MAX_FACILITY_ATTEMPTS})", flush=True)
                        time.sleep(wait)
                        continue
                    raise

            store_record = parse_facility(data, site_number=site)
            done[site] = store_record
            new_count += 1
            consecutive_fail = 0

            unit_count = len(store_record["units"])
            avail_count = sum(1 for u in store_record["units"] if u.get("available"))
            print(f"[{i}/{total}] Site #{site} ({city}, {state}): {unit_count} units ({avail_count} avail)", flush=True)

        except SessionRefused as e:
            print(f"\n[STOP] {e}", flush=True)
            print(f"       Two ordinary pages in a row were refused, so this is not "
                  f"a dead slug and not a redeploy.\n"
                  f"       Stopping at {len(done):,} facilities. Progress is saved; "
                  f"resume later or crawl slower.", flush=True)
            break

        except Exception as e:
            # One stubborn facility must not end a 4,400-store crawl. Extra Space
            # absorbed Life Storage and other operators, so some sitemap slugs
            # plausibly no longer have a static JSON twin -- but that is a theory,
            # and skipping is the right move whichever way it turns out.
            consecutive_fail += 1
            status = getattr(e, "status", None)
            skipped.append({"site_number": site, "city": city, "state": state,
                            "status": status, "error": str(e)})
            print(f"  [SKIP] Site #{site} ({city}, {state}) after "
                  f"{MAX_FACILITY_ATTEMPTS} attempts: {e}", flush=True)

            if consecutive_fail >= CIRCUIT_BREAKER:
                print(f"\n[STOP] {consecutive_fail} facilities failed in a row. That is a "
                      f"refusal, not a run of dead links.\nStopping rather than sending "
                      f"{total - i:,} more requests they have said no to. "
                      f"Progress is saved; resume later.", flush=True)
                break

        # Atomic checkpoint save every 25 stores
        if new_count and new_count % 25 == 0:
            atomic_save(output_file, list(done.values()))
            print(f"  --> Checkpoint saved: {len(done):,} total facilities on disk", flush=True)

        if limit and new_count >= limit:
            print(f"\nReached requested limit of {limit} facilities. Stopping.", flush=True)
            break

        # Polite human-paced jitter: base delay +/- randomized variation
        sleep_dur = max(1.5, delay + random.uniform(-0.8, 1.8))
        time.sleep(sleep_dur)

    atomic_save(output_file, list(done.values()))
    print(f"\n✅ Crawl complete! {len(done):,} total stores saved in {output_file}", flush=True)

    if skipped:
        SKIPPED_FILE.write_text(json.dumps(skipped, indent=2), encoding="utf-8")
        by_status = {}
        for s in skipped:
            by_status[s["status"]] = by_status.get(s["status"], 0) + 1
        print(f"   {len(skipped):,} skipped -> {SKIPPED_FILE}  "
              f"({', '.join(f'{k}: {v}' for k, v in sorted(by_status.items(), key=lambda x: str(x[0])))})", flush=True)
    if redeploys:
        print(f"   {redeploys} buildId rotation(s) handled mid-crawl.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extra Space Polite Crawler")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT, help="Output JSON path")
    parser.add_argument("--delay", type=float, default=3.0, help="Base delay between requests (seconds)")
    parser.add_argument("--limit", type=int, default=0, help="Stop after N new stores (0 = all)")
    args = parser.parse_args()

    run_crawl(output_file=args.out, delay=args.delay, limit=args.limit)
