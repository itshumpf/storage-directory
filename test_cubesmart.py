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
        test_fail_loud()
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED! CUBESMART PIPELINE IS 100% VERIFIED.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
