"""Offline regression tests for U-Haul discovery and parsing."""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory

from uhaul_parser import parse_facility_html, parse_sitemap_xml
import uhaul_scraper
from uhaul_scraper import (MAX_SKIP_RATIO, MIN_US_FACILITIES, _load_checkpoint, _sku_overlap,
                           _write_checkpoint, run)


STORE = {
    "brand": "uhaul", "store_id": "uhaul_776053", "site_number": "776053",
    "city": "Atlanta", "state": "GA", "zip": "30313",
    "url": "https://www.uhaul.com/Locations/Self-Storage-near-Atlanta-GA-30313/776053/",
}


def expect_raises(exc, callback):
    try:
        callback()
    except exc:
        return
    raise AssertionError(f"expected {exc.__name__}")


def sitemap(*urls: str) -> str:
    body = "".join(f"<url><loc>{url}</loc></url>" for url in urls)
    return f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{body}</urlset>'


def facility_page(*, affiliate: bool = False, rooms: bool = True) -> str:
    metadata = {
        "@context": "https://schema.org", "@graph": [{
            "@type": "SelfStorage", "name": "U-Haul at Peters St", "telephone": "404-555-0100",
            "address": {"streetAddress": "300 Peters St SW", "addressLocality": "Atlanta",
                        "addressRegion": "GA", "postalCode": "30313"},
            "geo": {"latitude": 33.75, "longitude": -84.40},
        }],
    }
    affiliate_text = "U-Haul Self-Storage Affiliate" if affiliate else ""
    if rooms:
        room = '''<ul class="uhjs-unit-list"><li>
          <h4>Small <span class="nowrap">5' x 10' x 9'</span></h4>
          <ul class="collapse condensed"><li>Interior</li><li>3rd Floor</li><li>No Climate</li></ul>
          <span>Free Lock with Rent Now</span>
          <ul class="tabs"><li class="tabs-title rent">Rent Now</li><li class="tabs-title reserve">Reserve</li></ul>
          <form>
            <input Id="RentableInventoryPk_4ac3cedf-ee9c-4df7-ad3b-4eeb0f43ad28_NA"
              Value="4ac3cedf-ee9c-4df7-ad3b-4eeb0f43ad28" id="RentableInventoryPk"
              name="RentableInventoryPk" value="">
            <input Value="2" id="VacantUnitsCount" name="VacantUnitsCount" value="">
            <input Value="$124.95" id="Price" name="Price" value="">
          </form></li></ul>'''
    else:
        room = "<p>No storage units currently available</p>"
    return (f'<html><body><script type="application/ld+json">{json.dumps(metadata)}</script>'
            f'{affiliate_text}<div id="roomTypes">{room}</div></body></html>')


def live_shaped_page() -> str:
    """The four ld+json blocks a real facility page carries, as captured from
    facility 700026 on 2026-09-04 (abridged). Address, phone and coordinates are
    NOT on the SelfStorage node — that is the whole point of this fixture."""
    reviews_graph = {"@context": "https://schema.org", "@graph": [{
        "@type": "SelfStorage", "name": "U-Haul Storage of Bend",
        "aggregateRating": {"@type": "AggregateRating", "ratingValue": 4.5, "ratingCount": 11389},
        "review": [{"@type": "Review", "reviewBody": "fine"}],
    }]}
    local_business = {
        "@context": "https://schema.org", "@type": "LocalBusiness",
        "@id": "https://www.uhaul.com/Locations/Self-Storage-near-Bend-OR-97701/700026/#storage",
        "name": "U-Haul Storage of Bend",
        "sameAs": ["https://maps.google.com/maps?cid=1", "https://maps.apple.com/place/?ll=44.10086916,-121.2996572"],
        "address": {"@type": "PostalAddress", "addressLocality": "Bend", "addressRegion": "OR",
                    "postalCode": "97701", "streetAddress": "63370 N Hwy 97"},
        "areaServed": {"@type": "GeoCircle", "geoMidpoint": {"@type": "GeoCoordinates",
                       "latitude": 44.10086916, "longitude": -121.2996572}, "geoRadius": 16093},
        "contactPoint": {"@type": "ContactPoint", "telephone": "+15413880671"},
        "department": [{"@type": "LocalBusiness", "name": "Truck Rental",
                        "address": {"streetAddress": "WRONG"}, "telephone": "+10000000000"}],
    }
    breadcrumbs = {"@context": "https://schema.org", "@type": "BreadcrumbList", "itemListElement": []}
    bare_type_graph = {"@context": "https://schema.org", "@graph": [{
        "type": ["SelfStorage", "LocalBusiness"], "name": "U-Haul Moving & Storage of Bend",
        "address": {"@type": "PostalAddress", "streetAddress": "63370 N Hwy 97"},
    }]}
    scripts = "".join(f'<script type="application/ld+json">{json.dumps(b)}</script>'
                      for b in (reviews_graph, local_business, breadcrumbs, bare_type_graph))
    return f'<html><body>{scripts}<div id="roomTypes"><p>No storage units currently available</p></div></body></html>'


def test_live_shaped_page_yields_address_phone_and_coordinates():
    # Regression for 2026-09-03: 1,182 of 1,182 collected facilities had
    # address "", phone "" and lat/lng null, and the run reported success.
    record = parse_facility_html(live_shaped_page(), {**STORE, "site_number": "700026", "store_id": "uhaul_700026"})
    assert record["address"] == "63370 N Hwy 97", record["address"]
    assert record["city"] == "Bend" and record["state"] == "OR" and record["zip"] == "97701"
    assert record["phone"] == "541-388-0671", record["phone"]
    assert record["lat"] == 44.10086916 and record["lng"] == -121.2996572
    assert record["rating"] == 4.5 and record["reviews"] == 11389
    assert record["name"] == "U-Haul Storage of Bend"


def test_coordinates_fall_back_to_the_apple_maps_link():
    page = live_shaped_page().replace('"areaServed"', '"areaIgnored"')
    record = parse_facility_html(page, {**STORE, "site_number": "700026", "store_id": "uhaul_700026"})
    assert record["lat"] == 44.10086916 and record["lng"] == -121.2996572


def test_checkpoint_survives_a_catalog_edit_and_drops_removed_facilities():
    with TemporaryDirectory() as folder:
        path = Path(folder) / "checkpoint.json"
        stores = {"uhaul_1": {"store_id": "uhaul_1"}, "uhaul_2": {"store_id": "uhaul_2"}}
        _write_checkpoint(path, "2026-09-04", ["uhaul_1", "uhaul_2"], stores, {"uhaul_9": "dead"})
        # Catalog gained uhaul_3 and lost uhaul_2 between attempts: keep uhaul_1, drop uhaul_2.
        kept = _load_checkpoint(path, "2026-09-04", ["uhaul_1", "uhaul_3"])
        assert set(kept) == {"uhaul_1"}, kept
        assert _load_checkpoint(path, "2026-09-05", ["uhaul_1"]) == {}
        assert uhaul_scraper._load_skipped(path, "2026-09-04") == {"uhaul_9": "dead"}


class _FakeResponse:
    def __init__(self, url, text):
        self.url, self.text = url, text


def _run_with_fake_site(pages, monkeypatch_ids, folder, date="2026-09-04"):
    """Drive run() against an in-memory site. `pages` maps entity -> (final_url, html)."""
    class FakeClient:
        def __init__(self, delay, session=None):
            self.calls = []
        def get(self, url):
            self.calls.append(url)
            if url == uhaul_scraper.ROBOTS_URL:
                return _FakeResponse(url, "User-agent: *\nAllow: /\n")
            if url == uhaul_scraper.INDEX_URL:
                return _FakeResponse(url, "<x/>")
            entity = url.rstrip("/").rsplit("/", 1)[-1]
            final, html = pages[entity]
            return _FakeResponse(final, html)
    catalog = [{"brand": "uhaul", "store_id": f"uhaul_{e}", "site_number": e, "city": "Bend", "state": "OR",
                "zip": "97701", "url": f"https://www.uhaul.com/Locations/Self-Storage-near-Bend-OR-97701/{e}/"}
               for e in monkeypatch_ids]
    saved = (uhaul_scraper.PoliteSession, uhaul_scraper.discover_us_facilities,
             uhaul_scraper.MIN_UNIT_TYPES, uhaul_scraper.MIN_PRICED_FACILITY_RATIO)
    uhaul_scraper.PoliteSession = FakeClient
    uhaul_scraper.discover_us_facilities = lambda client, policy: list(catalog)
    uhaul_scraper.MIN_UNIT_TYPES = 0
    uhaul_scraper.MIN_PRICED_FACILITY_RATIO = 0.0
    try:
        root = Path(folder)
        args = Namespace(date=date, delay=5, max_runtime_minutes=0, output=root / "rolling.json",
                         checkpoint=root / "checkpoint.json", report=root / "report.json",
                         snapshot_dir=root / "snapshots", change_log=root / "changes.csv")
        code = run(args)
        return code, json.loads(args.report.read_text(encoding="utf-8")), args
    finally:
        (uhaul_scraper.PoliteSession, uhaul_scraper.discover_us_facilities,
         uhaul_scraper.MIN_UNIT_TYPES, uhaul_scraper.MIN_PRICED_FACILITY_RATIO) = saved


def test_one_dead_link_is_skipped_and_the_run_still_publishes():
    # The 2026-09-03 failure mode: facility 936078 redirected to /Error/.
    ids = [str(700000 + i) for i in range(100)]
    good = "https://www.uhaul.com/Locations/Self-Storage-near-Bend-OR-97701/{}/"
    pages = {e: (good.format(e), facility_page()) for e in ids}
    pages["700042"] = ("https://www.uhaul.com/Error/", "<html>error</html>")
    with TemporaryDirectory() as folder:
        code, report, args = _run_with_fake_site(pages, ids, folder)
        assert code == 3, (code, report.get("error"))                      # complete_with_warnings
        assert report["status"] == "complete_with_warnings"
        assert list(report["skipped"]) == ["uhaul_700042"]
        assert report["facility_count"] == 99
        assert (args.snapshot_dir / "2026-09-04.json").exists()


def test_too_many_dead_links_stops_without_publishing():
    ids = [str(700000 + i) for i in range(100)]
    good = "https://www.uhaul.com/Locations/Self-Storage-near-Bend-OR-97701/{}/"
    pages = {e: (good.format(e), facility_page()) for e in ids}
    # Just over the cap (2% of 100 = 2 allowed), spread out so the streak rule does not fire first.
    for e in ("700010", "700040", "700070"):
        pages[e] = ("https://www.uhaul.com/Error/", "<html>error</html>")
    with TemporaryDirectory() as folder:
        code, report, args = _run_with_fake_site(pages, ids, folder)
        assert code == 2 and report["status"] == "failed", report
        assert "skipped" in report["error"] and not (args.snapshot_dir / "2026-09-04.json").exists()
        assert len(json.loads(args.checkpoint.read_text())["skipped"]) == 3   # kept for the resume


def test_a_failure_streak_stops_without_publishing():
    ids = [str(700000 + i) for i in range(1000)]   # cap is 20 here, so the streak rule fires first
    good = "https://www.uhaul.com/Locations/Self-Storage-near-Bend-OR-97701/{}/"
    pages = {e: ("https://www.uhaul.com/Error/", "<html>error</html>") for e in ids}
    pages["700000"] = (good.format("700000"), facility_page())
    with TemporaryDirectory() as folder:
        code, report, args = _run_with_fake_site(pages, ids, folder)
        assert code == 2 and "in a row" in report["error"], report


def test_sitemap_keeps_only_direct_facilities_and_deduplicates():
    direct = STORE["url"]
    rows = parse_sitemap_xml(sitemap(direct, direct, "https://www.uhaul.com/Storage/Atlanta-GA/Results/"))
    assert len(rows) == 1
    assert rows[0]["store_id"] == "uhaul_776053"
    assert rows[0]["city"] == "Atlanta"


def test_parser_preserves_razor_source_values():
    record = parse_facility_html(facility_page(), STORE)
    unit = record["units"][0]
    assert record["address"] == "300 Peters St SW" and record["is_affiliate"] is False
    assert unit["sku"] == "uhaul_4ac3cedfee9c4df7ad3b4eeb0f43ad28"
    assert unit["price"] == 124.95 and unit["count"] == 2
    assert unit["size"] == "5x10" and unit["height"] == 9
    assert unit["rent_now"] and unit["reserve"] and "No Climate" in unit["attrs"]


def test_affiliate_is_explicit_and_sold_out_is_valid():
    record = parse_facility_html(facility_page(affiliate=True, rooms=False), STORE)
    assert record["is_affiliate"] is True and record["units"] == []


def test_intact_empty_inventory_container_is_valid():
    page = facility_page(rooms=False).replace("No storage units currently available", "inventory loading")
    assert parse_facility_html(page, STORE)["units"] == []


def test_explicit_sold_out_template_without_inventory_container_is_valid():
    page = facility_page(rooms=False).replace(
        '<div id="roomTypes"><p>No storage units currently available</p></div>',
        '<h4>There are no rooms available online at this time.</h4>',
    )
    record = parse_facility_html(page, STORE)
    assert record["units"] == []
    assert record["inventory_status"] == "sold_out_online"


def test_missing_schema_fails_loudly():
    expect_raises(KeyError, lambda: parse_facility_html('<div id="roomTypes"></div>', STORE))


def test_existing_snapshot_is_idempotent_and_makes_no_requests():
    with TemporaryDirectory() as folder:
        root = Path(folder)
        snapshots = root / "snapshots"
        snapshots.mkdir()
        (snapshots / "2026-09-03.json").write_text("[]", encoding="utf-8")
        args = Namespace(
            date="2026-09-03", delay=5, max_runtime_minutes=330,
            output=root / "rolling.json", checkpoint=root / "checkpoint.json",
            report=root / "report.json", snapshot_dir=snapshots,
            change_log=root / "changes.csv",
        )
        assert run(args) == 0
        report = json.loads(args.report.read_text(encoding="utf-8"))
        assert report["status"] == "already_complete"


def test_sku_overlap_detects_identifier_churn():
    before = [{"store_id": "uhaul_1", "units": [{"sku": "uhaul_a"}, {"sku": "uhaul_b"}]}]
    stable = [{"store_id": "uhaul_1", "units": [{"sku": "uhaul_a"}, {"sku": "uhaul_b"}]}]
    churned = [{"store_id": "uhaul_1", "units": [{"sku": "uhaul_x"}, {"sku": "uhaul_y"}]}]
    assert _sku_overlap(before, stable) == 1
    assert _sku_overlap(before, churned) == 0


def test_bootstrap_floor_rejects_the_known_incomplete_7_prefix_catalog():
    assert MIN_US_FACILITIES > 1076


if __name__ == "__main__":
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    failures = []
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as exc:
            failures.append((name, exc))
            print(f"  FAIL  {name}: {exc}")
    print(f"\n{len(tests) - len(failures)}/{len(tests)} passed")
    raise SystemExit(1 if failures else 0)
