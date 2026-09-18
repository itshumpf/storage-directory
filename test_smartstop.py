"""Offline regression tests for SmartStop discovery, parsing, and run safety."""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory

import smartstop_scraper
from smartstop_parser import BASE, parse_facility_html, parse_sitemap_xml
from storage_pipeline import BRANDS


def expect_raises(exc, fn):
    try:
        fn()
    except exc:
        return
    raise AssertionError(f"expected {exc.__name__}")


def sitemap(*urls):
    body = "".join(f"<url><loc>{url}</loc></url>" for url in urls)
    return f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{body}</urlset>'


def facility(url=BASE + "/find-storage/al/foley/8141-al-59", site="6032"):
    value = {
        "@context": "https://schema.org", "@type": "SelfStorage", "@id": f"facility-{site}",
        "url": url, "name": f"SmartStop Self Storage #{site}", "telephone": "+1 (251) 555-0100",
        "address": {"@type": "PostalAddress", "streetAddress": "8141 AL-59", "addressLocality": "Foley",
                    "addressRegion": "AL", "postalCode": "36535", "addressCountry": "US"},
        "geo": {"latitude": 30.4, "longitude": -87.7},
        "aggregateRating": {"ratingValue": "4.8", "reviewCount": "123"},
        "makesOffer": [
            {"@type": "Offer", "price": "25.00", "availability": "https://schema.org/InStock",
             "itemOffered": {"@type": "Product", "sku": f"{site}_58", "name": "5' x 10' Storage Unit",
                             "description": "5' x 10' Storage Unit - Drive-Up Access, Ground Floor"}},
            {"@type": "Offer", "price": 80, "availability": "https://schema.org/OutOfStock",
             "itemOffered": {"@type": "Product", "sku": f"{site}_99", "name": "10 x 10 Storage Unit",
                             "description": "10 x 10 Storage Unit - Climate Controlled"}},
        ],
    }
    return '<html><script type="application/ld+json">' + json.dumps(value) + "</script></html>"


STORE = {"brand": "smartstop", "url": BASE + "/find-storage/al/foley/8141-al-59",
         "state": "AL", "city": "Foley"}


def test_sitemap_keeps_only_exact_us_facility_paths():
    rows = parse_sitemap_xml(sitemap(
        STORE["url"], STORE["url"] + "/", BASE + "/find-storage/tx/austin/123-main-st",
        BASE + "/find-storage/on/toronto/123-main-st", BASE + "/find-storage/al/foley",
        BASE + "/about-us", "https://example.com/find-storage/al/foley/8141-al-59"))
    assert [r["url"] for r in rows] == [STORE["url"], BASE + "/find-storage/tx/austin/123-main-st"]


def test_parser_maps_schema_offers_without_inventing_vacancy_counts():
    rec = parse_facility_html(facility(), STORE)
    assert rec["store_id"] == "smartstop_6032" and rec["site_number"] == "6032"
    assert rec["address"] == "8141 AL-59" and rec["state"] == "AL" and rec["phone"] == "251-555-0100"
    assert rec["lat"] == 30.4 and rec["lng"] == -87.7 and rec["rating"] == 4.8 and rec["reviews"] == 123
    assert rec["inventory_semantics"] == "advertised_offers"
    small, large = rec["units"]
    assert small["size"] == "5x10" and small["price"] == 25 and small["count"] == 1
    assert small["attrs"] == "Drive-Up Access, Ground Floor" and small["sqft"] == 50
    assert large["size"] == "10x10" and large["available"] is False and large["count"] == 0


def test_wrong_canonical_and_missing_schema_fail_loudly():
    expect_raises(LookupError, lambda: parse_facility_html(facility(BASE + "/find-storage/al/mobile/x"), STORE))
    expect_raises(KeyError, lambda: parse_facility_html("<html>no inventory</html>", STORE))


class _Resp:
    def __init__(self, url, text):
        self.url, self.text = url, text


def _drive(folder, dead=None, refused=None, limit=0):
    urls = [f"{BASE}/find-storage/al/city-{i}/{6000+i}-main-st" for i in range(6)]

    class FakeClient:
        def __init__(self, delay):
            self.delay = delay
        def get(self, url):
            if url == smartstop_scraper.ROBOTS_URL:
                return _Resp(url, "User-agent: *\nCrawl-delay: 10\nDisallow: /umbraco/\n")
            if url == smartstop_scraper.SITEMAP_URL:
                return _Resp(url, sitemap(*urls))
            if url == refused:
                raise smartstop_scraper.Refused("HTTP 403")
            if url == dead:
                raise smartstop_scraper.Dead("HTTP 404")
            site = url.rsplit("/", 1)[-1].split("-", 1)[0]
            return _Resp(url, facility(url, site))

    saved = (smartstop_scraper.PoliteSession, smartstop_scraper.MIN_CATALOG,
             smartstop_scraper.MIN_STORES, smartstop_scraper.MIN_UNIT_GROUPS)
    smartstop_scraper.PoliteSession = FakeClient
    smartstop_scraper.MIN_CATALOG = 5
    smartstop_scraper.MIN_STORES = 4
    smartstop_scraper.MIN_UNIT_GROUPS = 5
    try:
        root = Path(folder)
        args = Namespace(date="2026-09-09", delay=10, max_runtime_minutes=0,
                         snapshot_dir=root / "snap", report=root / "report.json", limit=limit)
        code = smartstop_scraper.run(args)
        return code, json.loads(args.report.read_text()), root / "snap", urls
    finally:
        (smartstop_scraper.PoliteSession, smartstop_scraper.MIN_CATALOG,
         smartstop_scraper.MIN_STORES, smartstop_scraper.MIN_UNIT_GROUPS) = saved


def test_one_dead_sitemap_url_can_publish_with_warning():
    with TemporaryDirectory() as folder:
        url = f"{BASE}/find-storage/al/city-3/6003-main-st"
        code, report, snap, _ = _drive(folder, dead=url)
        assert code == 3 and report["status"] == "complete_with_warnings"
        assert report["facility_count"] == 5 and (snap / "2026-09-09.json").exists()


def test_first_refusal_stops_and_never_publishes():
    with TemporaryDirectory() as folder:
        url = f"{BASE}/find-storage/al/city-3/6003-main-st"
        code, report, snap, _ = _drive(folder, refused=url)
        assert code == 2 and "wait until tomorrow" in report["error"]
        assert not (snap / "2026-09-09.json").exists() and (snap / "2026-09-09.partial.json").exists()


def test_limit_never_publishes_and_complete_day_is_idempotent():
    with TemporaryDirectory() as folder:
        code, report, snap, _ = _drive(folder, limit=2)
        assert code == 4 and report["status"] == "limited" and not (snap / "2026-09-09.json").exists()
        code, report, snap, _ = _drive(folder)
        assert code == 0 and report["resumed_count"] == 2 and (snap / "2026-09-09.json").exists()
        code, report, _, _ = _drive(folder)
        assert code == 0 and report["status"] == "already_complete"


def test_robot_delay_is_a_hard_floor():
    class Client:
        delay = 9.9
        def get(self, url):
            return _Resp(url, "User-agent: *\nCrawl-delay: 10\n")
    expect_raises(RuntimeError, lambda: smartstop_scraper._robot_policy(Client()))


def test_pipeline_and_collector_share_the_us_store_floor():
    assert BRANDS["smartstop"]["floor"] == smartstop_scraper.MIN_STORES


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failed = []
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as exc:
            failed.append(name)
            print(f"  FAIL  {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    raise SystemExit(bool(failed))
