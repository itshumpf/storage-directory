"""
test_cubesmart.py — Polite verification and validation suite for CubeSmart pipeline.

Runs:
1. Live sitemap discovery test (verifies sitemap-facility.xml). (Pause 2.5s)
2. Live single facility fetch & parse test.
3. Offline schema compliance check (matching FindStorage.pages.dev / enriched_locations.json).
4. Offline "Fail Loudly" assertions on malformed payloads.
"""
import time
import sys
from pathlib import Path
from curl_cffi import requests

# Reconfigure stdout for UTF-8 on Windows consoles
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from cubesmart_parser import parse_facility_html
from cubesmart_scraper import get_facility_urls
import cubesmart_scraper


# ---------------------------------------------------------------- offline run() tests
# Added 2026-09-04 with the rewrite of run_crawl. These drive the collector
# against an in-memory site so the completion policy is tested without a
# single request — the live tests above stay for the parser.
import json
from datetime import date
from tempfile import TemporaryDirectory

SITE_URL = "https://www.cubesmart.com/texas-self-storage/pearland-self-storage/{}.html"


def _record(site):
    return {"brand": "cubesmart", "store_id": f"cube_{site}", "site_number": site, "name": f"CubeSmart {site}",
            "address": "1 Main", "city": "Pearland", "state": "TX", "zip": "77584", "phone": "", "lat": 0,
            "lng": 0, "url": SITE_URL.format(site), "units": [{"size": "10x10", "price": 100, "sku": f"cube_{site}_a"}]}


def _drive(pages, sites, folder, run_date="2026-09-04", limit=0):
    """pages: site -> exception instance to raise, or None for a good page."""
    class FakeSession:
        pass
    saved = (cubesmart_scraper.requests.Session, cubesmart_scraper.get_facility_urls,
             cubesmart_scraper.fetch_store, cubesmart_scraper.parse_facility_html,
             cubesmart_scraper.MIN_STORES, cubesmart_scraper.time.sleep)
    def fake_fetch(session, url):
        site = cubesmart_scraper.site_number_of(url)
        if pages.get(site):
            raise pages[site]
        return site
    cubesmart_scraper.requests.Session = FakeSession
    cubesmart_scraper.get_facility_urls = lambda session: [SITE_URL.format(x) for x in sites]
    cubesmart_scraper.fetch_store = fake_fetch
    cubesmart_scraper.parse_facility_html = lambda html, facility_url, site_number: _record(site_number)
    cubesmart_scraper.MIN_STORES = 10
    cubesmart_scraper.time.sleep = lambda *_: None
    try:
        root = Path(folder)
        code = cubesmart_scraper.run_crawl(root / "snap", root / "report.json", 3.0, limit, run_date)
        return code, json.loads((root / "report.json").read_text(encoding="utf-8")), root / "snap"
    finally:
        (cubesmart_scraper.requests.Session, cubesmart_scraper.get_facility_urls,
         cubesmart_scraper.fetch_store, cubesmart_scraper.parse_facility_html,
         cubesmart_scraper.MIN_STORES, cubesmart_scraper.time.sleep) = saved


def test_offline_one_dead_store_still_publishes():
    sites = [str(4000 + i) for i in range(100)]
    with TemporaryDirectory() as folder:
        code, report, snap = _drive({"4042": cubesmart_scraper.Dead("HTTP 404")}, sites, folder)
        assert code == 3 and report["status"] == "complete_with_warnings", report
        assert list(report["dead"]) == ["4042"] and report["facility_count"] == 99
        assert (snap / "2026-09-04.json").exists() and not (snap / "2026-09-04.partial.json").exists()
        print("  [OK] one dead link is recorded, the day still publishes")


def test_offline_too_many_dead_stores_does_not_publish():
    sites = [str(4000 + i) for i in range(100)]
    pages = {s: cubesmart_scraper.Dead("HTTP 404") for s in ("4010", "4040", "4070")}   # cap is 2
    with TemporaryDirectory() as folder:
        code, report, snap = _drive(pages, sites, folder)
        assert code == 2 and report["status"] == "failed" and "dead" in report["error"], report
        assert not (snap / "2026-09-04.json").exists() and (snap / "2026-09-04.partial.json").exists()
        print("  [OK] past the dead cap the run stops, keeps its partial, publishes nothing")


def test_offline_refusal_streak_stops_the_run():
    sites = [str(4000 + i) for i in range(100)]
    pages = {s: cubesmart_scraper.Refused("HTTP 403 after 4 attempts") for s in sites}
    with TemporaryDirectory() as folder:
        code, report, _ = _drive(pages, sites, folder)
        assert code == 2 and "refused in a row" in report["error"], report
        print("  [OK] a 403 streak stops the run instead of hammering the host")


def test_offline_same_day_rerun_resumes_and_complete_day_makes_no_requests():
    sites = [str(4000 + i) for i in range(100)]
    with TemporaryDirectory() as folder:
        # First attempt dies past the dead cap and leaves a partial file behind.
        pages = {s: cubesmart_scraper.Dead("HTTP 404") for s in ("4010", "4040", "4070")}
        _drive(pages, sites, folder)
        partial = json.loads((Path(folder) / "snap" / "2026-09-04.partial.json").read_text())
        assert partial, "partial should hold what was collected"
        # Second attempt, site healthy again: resumes, completes.
        code, report, snap = _drive({}, sites, folder)
        assert code == 0 and report["resumed_count"] == len(partial), report
        # Third attempt: already complete, no requests.
        code, report, _ = _drive({}, sites, folder)
        assert code == 0 and report["status"] == "already_complete"
        print("  [OK] same-day rerun resumes the partial; a complete day is idempotent")


def test_offline_drop_against_previous_snapshot_does_not_publish():
    with TemporaryDirectory() as folder:
        snap = Path(folder) / "snap"; snap.mkdir()
        (snap / "2026-09-03.json").write_text(json.dumps([_record(str(4000 + i)) for i in range(100)]))
        code, report, _ = _drive({}, [str(4000 + i) for i in range(80)], folder)   # 80 < 90% of 100
        assert code == 2 and "fell more than" in report["error"], report
        print("  [OK] a 20% smaller sitemap day is refused against the previous snapshot")


def test_offline_limit_never_publishes():
    with TemporaryDirectory() as folder:
        code, report, snap = _drive({}, [str(4000 + i) for i in range(50)], folder, limit=5)
        assert code == 4 and report["status"] == "limited" and not (snap / "2026-09-04.json").exists()
        print("  [OK] --limit is a smoke test, not a snapshot")


def run_offline_tests():
    print("\n[OFFLINE] run_crawl completion policy...")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_offline_"):
            fn()
    test_fail_loud()


def test_sitemap_discovery(session):
    print("\n[TEST 1] Testing live sitemap discovery...")
    urls = get_facility_urls(session)
    assert len(urls) > 500, f"Expected >500 facilities, got {len(urls)}"
    print(f"  [OK] Sitemap parsed: {len(urls):,} facilities found")
    return urls


def test_live_facility_parsing(session, sample_url):
    print(f"\n[TEST 2] Testing live facility fetch & parsing for:\n         {sample_url}")
    
    # Polite pause
    print("  ...polite pause (2.5s)...")
    time.sleep(2.5)

    r = session.get(sample_url, impersonate="chrome120", timeout=30)
    assert r.status_code == 200, f"Expected HTTP 200, got {r.status_code}"
    
    parsed = parse_facility_html(r.text, facility_url=sample_url)
    
    print(f"  [OK] Parsed store: {parsed['name']}")
    print(f"  [OK] Address: {parsed['address']}, {parsed['city']}, {parsed['state']} {parsed['zip']}")
    print(f"  [OK] Phone: {parsed['phone']}")
    print(f"  [OK] Lat/Lng: ({parsed['lat']}, {parsed['lng']})")
    print(f"  [OK] Rating: {parsed['rating']} ({parsed['reviews']} reviews)")
    print(f"  [OK] Units extracted: {len(parsed['units'])}")
    
    if parsed["units"]:
        u = parsed["units"][0]
        print(f"  [SAMPLE UNIT] Size: {u['size']} | Web: ${u['price']} | Street: ${u['street_price']} | Promo: {u['promo']} | Attrs: {u['attrs']}")
    
    return parsed


def test_schema_compliance(store):
    print("\n[TEST 3] (Offline) Testing schema compatibility with FindStorage (enriched_locations.json)...")
    
    # Store-level required fields
    required_store_keys = [
        "brand", "store_id", "site_number", "name", "address", "city",
        "state", "zip", "phone", "lat", "lng", "url", "units"
    ]
    for k in required_store_keys:
        assert k in store, f"Missing required store key: '{k}'"
    
    assert store["brand"] == "cubesmart", f"Expected brand 'cubesmart', got '{store['brand']}'"
    assert store["store_id"].startswith("cube_"), f"Expected store_id prefix 'cube_', got '{store['store_id']}'"
    assert isinstance(store["phone"], str), f"Phone must be formatted string, got {type(store['phone'])}"
    assert store["url"].startswith("https://www.cubesmart.com/"), f"Invalid facility URL: {store['url']}"
    assert len(store["state"]) == 2, f"State must be 2-letter uppercase code, got '{store['state']}'"
    
    # Unit-level required fields
    required_unit_keys = [
        "size", "price", "street_price", "available", "count",
        "promo", "promo2", "sku", "attrs", "rates"
    ]
    
    assert len(store["units"]) > 0, "Store should contain at least 1 priced unit"
    for u in store["units"]:
        for uk in required_unit_keys:
            assert uk in u, f"Unit missing required key: '{uk}'"
        assert isinstance(u["price"], int) and u["price"] > 0, f"Invalid unit price: {u['price']}"
        assert isinstance(u["available"], bool), f"available must be bool, got {type(u['available'])}"
        assert u["sku"].startswith("cube_"), f"Unit SKU must start with 'cube_', got '{u['sku']}'"

    print(f"  [OK] Store record & all {len(store['units'])} units strictly adhere to FindStorage schema.")


def test_fail_loud():
    print("\n[TEST 4] (Offline) Testing 'Fail Loudly' assertions on malformed inputs...")
    
    # Test 1: Empty string
    try:
        parse_facility_html("", facility_url="https://www.cubesmart.com/test/123.html")
        assert False, "Should have raised ValueError on empty HTML"
    except ValueError:
        print("  [OK] Empty HTML correctly raised ValueError")

    # Test 2: HTML missing JSON-LD
    try:
        parse_facility_html("<html><body>No Data</body></html>", facility_url="https://www.cubesmart.com/test/123.html")
        assert False, "Should have raised KeyError on missing JSON-LD"
    except KeyError:
        print("  [OK] Missing JSON-LD correctly raised KeyError")


if __name__ == "__main__":
    if "--offline" in sys.argv:
        # CI runs this: no requests, ever.
        run_offline_tests()
        print("\nOFFLINE TESTS PASSED")
        sys.exit(0)
    print("=" * 60)
    print("RUNNING CUBESMART PIPELINE VALIDATION (POLITE MODE)")
    print("=" * 60)

    try:
        session = requests.Session()
        urls = test_sitemap_discovery(session)
        
        # Pick Auburn, AL 4243.html or the first available
        sample_url = next((u for u in urls if "4243.html" in u), urls[0])
        sample_store = test_live_facility_parsing(session, sample_url)
        
        test_schema_compliance(sample_store)
        run_offline_tests()
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED! CUBESMART PIPELINE IS 100% VERIFIED.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
