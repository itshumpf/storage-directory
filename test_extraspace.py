"""
test_extraspace.py — Polite verification and validation suite for Extra Space pipeline.

Runs 3 gentle requests with pacing delays:
1. Live buildId extraction. (Pause 2.5s)
2. Sitemap enumeration. (Pause 2.5s)
3. Single facility fetch & parse.
4. Offline schema verification (matching FindStorage.pages.dev / enriched_locations.json).
5. Offline "Fail Loudly" assertions on malformed payloads.
"""
import time
import sys
from pathlib import Path
from curl_cffi import requests

# Reconfigure stdout for UTF-8 on Windows cp1252 consoles
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from extraspace_parser import parse_facility, parse_unit
from extraspace_scraper import get_live_build_id, get_facility_triples, get_with_session


def test_live_discovery(session):
    print("\n[TEST 1] Testing live buildId discovery & sitemap parsing...")
    build_id = get_live_build_id(session)
    assert build_id and isinstance(build_id, str), "Failed to retrieve buildId string"
    print(f"  [OK] Live buildId extracted: {build_id}")

    # Polite pause
    print("  ...polite pause (2.5s)...")
    time.sleep(2.5)

    triples = get_facility_triples(session)
    assert len(triples) > 1000, f"Expected >1000 facilities, got {len(triples)}"
    print(f"  [OK] Sitemap parsed successfully: {len(triples):,} facilities found")
    return build_id, triples


def test_live_store_parsing(session, build_id, sample_triple):
    state, city, site = sample_triple
    print(f"\n[TEST 2] Testing live facility fetch & parsing for site #{site} ({city}, {state})...")
    
    # Polite pause
    print("  ...polite pause (2.5s)...")
    time.sleep(2.5)

    url = f"https://www.extraspace.com/_next/data/{build_id}/en-US/storage/facilities/us/{state}/{city}/{site}.json"
    raw_json = get_with_session(session, url, as_json=True)
    
    parsed = parse_facility(raw_json, site_number=site)
    
    print(f"  [OK] Parsed store: {parsed['name']}")
    print(f"  [OK] Address: {parsed['address']}, {parsed['city']}, {parsed['state']} {parsed['zip']}")
    print(f"  [OK] Phone: {parsed['phone']}")
    print(f"  [OK] Facility URL: {parsed['url']}")
    print(f"  [OK] Units extracted: {len(parsed['units'])}")
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
    
    assert store["brand"] == "extraspace", f"Expected brand 'extraspace', got '{store['brand']}'"
    assert store["store_id"].startswith("exr_"), f"Expected store_id prefix 'exr_', got '{store['store_id']}'"
    assert isinstance(store["phone"], str), f"Phone must be formatted string, got {type(store['phone'])}"
    assert store["url"].startswith("https://www.extraspace.com/"), f"Invalid facility URL: {store['url']}"
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
        assert isinstance(u["count"], int), f"count must be int, got {type(u['count'])}"
        assert u["sku"].startswith("exr_"), f"Unit SKU must start with 'exr_', got '{u['sku']}'"
        assert "web" in u["rates"] and "street" in u["rates"], "Rates ladder missing web/street keys"

    print(f"  [OK] Store record & all {len(store['units'])} units strictly adhere to FindStorage schema.")


def test_fail_loud():
    print("\n[TEST 4] (Offline) Testing 'Fail Loudly' assertions on malformed inputs...")
    
    # Test 1: Empty / invalid store payload
    try:
        parse_facility({})
        assert False, "Should have raised KeyError on empty dictionary"
    except KeyError:
        print("  [OK] Empty dictionary correctly raised KeyError")

    # Test 2: Missing rates in unit
    try:
        parse_unit({"dimensions": {"width": 5, "depth": 5}}, site_number="100")
        assert False, "Should have raised ValueError on unit missing rates"
    except ValueError:
        print("  [OK] Missing rates dict correctly raised ValueError")

    # Test 3: Corrupt price
    try:
        parse_unit({"rates": {"web": -10}, "dimensions": {}}, site_number="100")
        assert False, "Should have raised ValueError on negative price"
    except ValueError:
        print("  [OK] Negative price correctly raised ValueError")


if __name__ == "__main__":
    print("=" * 60)
    print("RUNNING EXTRA SPACE PIPELINE VALIDATION (POLITE MODE)")
    print("=" * 60)

    try:
        session = requests.Session()
        build_id, triples = test_live_discovery(session)
        
        sample_triple = next((t for t in triples if t[2] == "1275"), triples[0])
        sample_store = test_live_store_parsing(session, build_id, sample_triple)
        
        test_schema_compliance(sample_store)
        test_fail_loud()
        
        print("\n" + "=" * 60)
        print("ALL TESTS PASSED! PIPELINE IS 100% OPERATIONAL & VERIFIED.")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[FAIL] TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
