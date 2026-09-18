"""Offline tests for the generic Storable adapter, safe probe, and deduper."""
from __future__ import annotations

import json
from pathlib import Path

from independent_registry import deduplicate_facilities, facility_match_key, validate_operator_registry
from independent_intake import merge_candidates, parse_inbox
from independent_scraper import (likely_facility_url, load_operators, parse_sitemap, probe_operator,
                                 pull_catalog, select_smoke_url)
from platform_probe import classify_html, probe_url
from storage_pipeline import independent_pilot_payload
from storable_adapter import extract_storable_data, parse_storable_facility
from storagemart_parser import extract_data


FIXTURE = Path("fixture_storagemart_1658.html").read_text(encoding="utf-8")


def test_storage_mart_fixture_runs_through_brand_neutral_core():
    facility = extract_data(FIXTURE)["facilities"]["allFacilities"][0]
    rec = parse_storable_facility(
        facility, {"url": "https://independent.example/units", "city": "Gardner", "zip": "66030"},
        brand="independent", store_id_prefix="indie", sku_prefix="storable",
        fallback_name="Independent Storage", operator_id="example_storage",
    )
    assert rec["brand"] == "independent" and rec["store_id"] == "indie_1658"
    assert rec["platform"] == "storable" and rec["operator_id"] == "example_storage"
    assert len(rec["units"]) == 10 and next(u for u in rec["units"] if u["size"] == "10x10")["price"] == 79


def test_json_hydration_wrapper_is_found_recursively():
    original = extract_data(FIXTURE)
    page = '<script id="__NEXT_DATA__" type="application/json">' + json.dumps(
        {"props": {"pageProps": {"state": original}}}) + "</script>"
    found = extract_storable_data(page)
    assert found["facilities"]["allFacilities"][0]["storeNumber"] == "1658"


def test_normalized_apollo_cache_is_hydrated_into_facility_and_units():
    cache = {
        "Facility:facility-1": {
            "name": "Little Storage",
            "location": {"type": "id", "generated": True, "id": "$Facility:facility-1.location"},
            "settings": {"type": "id", "generated": True, "id": "$Facility:facility-1.settings"},
            "unitGroups": [{"type": "id", "generated": False, "id": "UnitGroup:unit-1"}],
        },
        "$Facility:facility-1.location": {
            "address1": "123 Main St", "city": "Nashville", "state": "TN",
            "postalCode": "37211", "country": "US", "latitude": 36.1, "longitude": -86.7,
        },
        "$Facility:facility-1.settings": {"softwareProvider": "storedge"},
        "UnitGroup:unit-1": {
            "id": "unit-1", "size": "10x10", "width": 10, "length": 10,
            "price": 99, "discountedPrice": 89, "availableUnitsCount": 3,
            "totalUnitsCount": 10, "amenities": [{"name": "Drive-Up"}],
        },
    }
    page_state = {"page": {"facilityId": "facility-1", "facility": {"storeNumber": "L001"}}}
    page = ('<script>window.__APOLLO_STATE__=' + json.dumps(cache) + "</script>"
            '<script type="application/json">' + json.dumps(page_state) + "</script>")
    facility = extract_storable_data(page)["facilities"]["allFacilities"][0]
    rec = parse_storable_facility(
        facility, {"url": "https://little.example/123-main-st"}, brand="independent",
        store_id_prefix="ind_little", operator_id="little", fallback_name="Little Storage")
    assert rec["store_id"] == "ind_little_L001" and rec["address"] == "123 Main St"
    assert rec["software_provider"] == "storedge"
    assert len(rec["units"]) == 1 and rec["units"][0]["price"] == 89


def test_storable_size_falls_back_to_unit_group_name():
    facility = {
        "storeNumber": "1", "address": {"address1": "1 Main", "city": "Austin",
        "state": "TX", "postal": "78701", "country": "US"},
        "unitGroups": [{"id": "u1", "name": "10' x 15' Climate Controlled", "type": "Drive Up",
                        "price": 99, "availableUnitsCount": 1}],
    }
    rec = parse_storable_facility(facility, {"url": "https://example.test/1-main"},
                                  brand="independent", store_id_prefix="ind_test")
    assert rec["units"][0]["size"] == "10x15" and rec["units"][0]["sqft"] == 150
    assert rec["units"][0]["attrs"] == "Drive Up" and rec["units"][0]["category"] == "Drive Up"


def test_storable_facility_id_is_stable_fallback_when_store_number_is_blank():
    facility = {
        "id": "5b471f69-dffa-42e8-9bd4-32b218c6195b", "storeNumber": "",
        "address": {"address1": "1782 Grand Island Blvd", "city": "Grand Island",
                    "state": "NY", "postal": "14072", "country": "US"},
        "unitGroups": [{"id": "u1", "name": "5 x 10", "price": 70,
                        "availableUnitsCount": 2}],
    }
    rec = parse_storable_facility(facility, {"url": "https://example.test/facility"},
                                  brand="independent", store_id_prefix="ind_grand")
    assert rec["site_number"] == facility["id"]
    assert rec["store_id"] == "ind_grand_" + facility["id"]
    assert rec["identity_source"] == "facility_id"


def test_fingerprints_storable_storagely_and_sitelink_without_network():
    storable = classify_html("https://example.com/units", '<script>"allFacilities" "unitGroups" "softwareProvider"</script>')
    assert storable["platform"] == "storable" and storable["confidence"] == "high"
    storagely = classify_html(
        "https://example.com/units",
        '<script src="https://static.storagely.link/public/build/js/widgets.js"></script>'
        "storagely-prod-public-assets",
    )
    assert storagely["platform"] == "storagely" and storagely["confidence"] == "high"
    sitelink = classify_html("https://example.com/units", '<a href="https://www.smdservers.net/">Powered by SiteLink</a>')
    assert sitelink["platform"] == "sitelink" and sitelink["confidence"] == "high"


def test_fingerprints_g5_and_storage_essentials_ahead_of_incidental_storedge_links():
    g5 = classify_html("https://example.com", "inventory.g5marketingcloud.com G5_STORE_ID rental-center.storedge.com")
    assert g5["platform"] == "g5_marketing_cloud" and g5["confidence"] == "high"
    essentials = classify_html("https://example.com", 'storage_essentials seCompanyId allFacilitiesPage')
    assert essentials["platform"] == "storage_essentials" and essentials["confidence"] == "high"


class Response:
    def __init__(self, status, text="", headers=None):
        self.status_code, self.text, self.headers = status, text, headers or {}


def test_probe_never_fetches_a_robots_disallowed_page():
    class Session:
        def __init__(self):
            self.urls = []
            self.headers = {}
        def get(self, url, **_kwargs):
            self.urls.append(url)
            return Response(200, "User-agent: *\nDisallow: /\n")
    session = Session()
    result = probe_url("https://example.com/facility", session=session,
                       sleep=lambda _seconds: (_ for _ in ()).throw(AssertionError("must not sleep")))
    assert result["status"] == "disallowed" and session.urls == ["https://example.com/robots.txt"]


def test_probe_obeys_declared_delay_before_fetching_page():
    class Session:
        def __init__(self):
            self.urls = []
        def get(self, url, **_kwargs):
            self.urls.append(url)
            if url.endswith("robots.txt"):
                return Response(200, "User-agent: *\nCrawl-delay: 12\nAllow: /\n")
            return Response(200, 'uploads.website.storedge.com "allFacilities"')
    waited = []
    session = Session()
    result = probe_url("https://example.com/facility", session=session, sleep=waited.append)
    assert result["status"] == "classified" and result["platform"] == "storable"
    assert waited == [12.0] and len(session.urls) == 2


def test_address_dedup_keeps_aliases_and_provenance():
    one = {"name": "Old Storage", "operator_id": "old", "platform": "sitelink",
           "address": "123 North Main Street", "city": "Austin", "state": "TX", "zip": "78701-1234",
           "url": "https://old.example"}
    two = {"name": "New Storage", "operator_id": "new", "platform": "storable",
           "address": "123 N Main St.", "city": "Austin", "state": "TX", "zip": "78701",
           "url": "https://new.example"}
    assert facility_match_key(one) == facility_match_key(two)
    merged = deduplicate_facilities([one, two])
    assert len(merged) == 1 and merged[0]["aliases"] == ["New Storage", "Old Storage"]
    assert merged[0]["operator_ids"] == ["new", "old"] and merged[0]["platforms"] == ["sitelink", "storable"]


def test_po_box_uses_coordinates_for_physical_property_identity():
    record = {"address": "P.O. Box 312", "city": "Grand Island", "state": "NY", "zip": "14072",
              "lat": 43.0171854, "lng": -78.9627624}
    assert facility_match_key(record) == ("geo", "43.0172,-78.9628")


def test_operator_registry_is_unique_and_fails_closed():
    registry = validate_operator_registry("independent_operators.json")
    assert registry["batch"] == "batch-01+02"
    assert len(registry["operators"]) == 50
    assert all(operator["enabled"] is False for operator in registry["operators"])
    holds = [operator for operator in registry["operators"] if operator["status"] == "policy_hold"]
    assert len(holds) == 9
    assert sum(operator["terms_reviewed"] is True for operator in holds) == 8
    pending = [operator for operator in registry["operators"] if operator["status"] == "policy_pending"]
    assert len(pending) == 2 and all(operator["terms_status"] == "not_found" for operator in pending)
    probe_ready = [operator for operator in registry["operators"] if operator["status"] == "probe_ready"]
    assert len(probe_ready) == 14
    assert all(operator["robots_reviewed"] is True and operator["terms_reviewed"] is False
               for operator in probe_ready)
    candidates = [operator for operator in registry["operators"] if operator["status"] == "candidate"]
    assert len(candidates) == 25
    assert all(operator["robots_reviewed"] is False for operator in candidates)


def test_dashboard_pilot_keeps_candidates_out_of_store_totals():
    pilot = independent_pilot_payload()
    assert pilot["counts"] == {
        "candidate": 25, "probe_ready": 14, "policy_pending": 2, "policy_hold": 9, "enabled": 0,
    }
    assert len(pilot["operators"]) == 50
    assert all("units" not in operator and "stores" not in operator for operator in pilot["operators"])


def test_independent_default_cohort_is_the_14_robots_reviewed_operators():
    operators = load_operators(Path("independent_operators.json"))
    assert len(operators) == 14
    assert all(operator.operator_id != "storage_choice" for operator in operators)


def test_independent_probe_is_two_requests_with_ten_second_floor():
    operator = load_operators(Path("independent_operators.json"), {"atlantic_self_storage"})[0]
    class Session:
        def __init__(self):
            self.urls = []
            self.headers = {}
        def get(self, url, **_kwargs):
            self.urls.append(url)
            if url.endswith("robots.txt"):
                return Response(200, "User-agent: *\nAllow: /\n")
            return Response(200, "uploads.website.storedge.com")
    session, waited = Session(), []
    result = probe_operator(operator, 10, session=session, sleep=waited.append)
    assert result["status"] == "classified" and result["platform"] == "storable"
    assert len(session.urls) == 2 and 10 <= waited[0] <= 11


def test_sitemap_parser_handles_indexes_and_urlsets():
    kind, urls = parse_sitemap(
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        '<sitemap><loc>https://example.com/one.xml</loc></sitemap></sitemapindex>')
    assert kind == "sitemapindex" and urls == ["https://example.com/one.xml"]
    kind, urls = parse_sitemap(
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        '<url><loc>https://example.com/locations/a</loc></url></urlset>')
    assert kind == "urlset" and urls == ["https://example.com/locations/a"]


def test_catalog_follows_only_same_host_sitemaps_and_obeys_delay():
    operator = load_operators(Path("independent_operators.json"), {"atlantic_self_storage"})[0]
    documents = {
        "https://www.atlanticselfstorage.com/sitemap.xml":
            '<sitemapindex><sitemap><loc>https://www.atlanticselfstorage.com/locations.xml</loc></sitemap></sitemapindex>',
        "https://www.atlanticselfstorage.com/locations.xml":
            '<urlset><url><loc>https://www.atlanticselfstorage.com/locations/jax</loc></url>'
            '<url><loc>https://www.atlanticselfstorage.com/blog/news</loc></url></urlset>',
    }
    class Session:
        def __init__(self):
            self.urls = []
            self.headers = {}
        def get(self, url, **_kwargs):
            self.urls.append(url)
            return Response(200, documents[url])
    session, waited = Session(), []
    result = pull_catalog(operator, operator.origin + "/sitemap.xml", 10,
                          session=session, sleep=waited.append)
    assert result["status"] == "complete" and result["url_count"] == 2
    assert result["likely_facility_urls"] == ["https://www.atlanticselfstorage.com/locations/jax"]
    assert len(session.urls) == 2 and all(10 <= seconds <= 11 for seconds in waited)


def test_facility_url_filter_rejects_content_pages():
    assert likely_facility_url("https://example.com/self-storage/texas/austin")
    assert not likely_facility_url("https://example.com/blog/self-storage-tips")


def test_smoke_url_selection_prefers_canonical_facility_over_marketing_pages():
    urls = [
        "https://example.com/storage-locations/fl/jacksonville",
        "https://example.com/storage-locations/fl/jacksonville/climate-controlled-storage",
        "https://example.com/storage-locations/fl/jacksonville/123-main-st",
    ]
    assert select_smoke_url(urls) == "https://example.com/storage-locations/fl/jacksonville/123-main-st"


def test_smoke_url_selection_does_not_mistake_numbered_blog_post_for_facility():
    urls = [
        "https://example.com/5-things-to-look-for-in-a-storage-unit/",
        "https://example.com/locations/fort-knox-athens-ga/",
    ]
    assert select_smoke_url(urls) == "https://example.com/locations/fort-knox-athens-ga/"


def test_text_inbox_adds_only_new_domains_as_disabled_candidates():
    registry = validate_operator_registry("independent_operators.json")
    original_count = len(registry["operators"])
    rows = parse_inbox("# comment\nExample Storage | https://example-storage.test/units\n"
                       "https://example-storage.test/duplicate\n")
    merged, added = merge_candidates(registry, rows)
    assert len(rows) == 1 and len(added) == 1
    assert len(merged["operators"]) == original_count + 1
    assert added[0]["status"] == "candidate" and added[0]["enabled"] is False
    assert added[0]["robots_reviewed"] is False


if __name__ == "__main__":
    tests = [(name, fn) for name, fn in sorted(globals().items()) if name.startswith("test_") and callable(fn)]
    failures = []
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as exc:
            failures.append(name)
            print(f"  FAIL  {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - len(failures)}/{len(tests)} passed")
    raise SystemExit(bool(failures))
