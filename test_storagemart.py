"""Offline regression tests for StorageMart discovery, parsing, and the run policy.

    python test_storagemart.py

No requests are made. The facility fixture is the window.__data object captured
from the live page for store 1658 (Gardner, KS) on 2026-09-05, trimmed to the
fields the parser reads, and wrapped in the same one-statement <script> the
storEDGE template emits.
"""
from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory

import storagemart_scraper
from storagemart_parser import BASE, extract_data, parse_facility_html, parse_sitemap_xml
from storagemart_scraper import run

HERE = Path(__file__).parent
FIXTURE = (HERE / "fixture_storagemart_1658.html").read_text(encoding="utf-8")
STORE = {"brand": "storagemart", "store_id": "smart_1658", "site_number": "1658", "city": "Gardner",
         "zip": "66030", "url": f"{BASE}/kansas-city/gardner/1658-east-warren-st-66030"}


def expect_raises(exc, fn):
    try:
        fn()
    except exc:
        return
    raise AssertionError(f"expected {exc.__name__}")


def sitemap(*urls):
    body = "".join(f"<url><loc>{u}</loc></url>" for u in urls)
    return f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{body}</urlset>'


def test_sitemap_catalogs_all_four_facility_url_shapes_and_nothing_else():
    rows = parse_sitemap_xml(sitemap(
        f"{BASE}/kansas-city/gardner/1658-east-warren-st-66030",     # store# + street + zip
        f"{BASE}/kansas-city/gardner/1658-east-warren-st-66030/",    # duplicate with slash
        f"{BASE}/kansas-city/olathe/66061-south-enterprise",         # zip first, no store#
        f"{BASE}/kansas-city/lees-summit/465-oldham-pkwy",           # street number, no zip (store 0155)
        f"{BASE}/8028-highway-n-cottleville-63304",                  # root level
        f"{BASE}/kansas-city/8032-bradleys-pkwy-peculiar",           # store#, no zip, no city segment
        f"{BASE}/kansas-city/10x10-storage",                         # size page
        f"{BASE}/kansas-city/olathe/car-storage",                    # service page
        f"{BASE}/kansas-city/shawnee",                               # city hub
        f"{BASE}/toronto/1234-yonge-st-m4t1x3",                      # looks like a store# — kept; the page's country refuses it
        f"{BASE}/about-us",
    ))
    got = {r["url"][len(BASE):]: r for r in rows}
    assert set(got) == {"/kansas-city/gardner/1658-east-warren-st-66030", "/kansas-city/olathe/66061-south-enterprise",
                        "/kansas-city/lees-summit/465-oldham-pkwy", "/8028-highway-n-cottleville-63304",
                        "/kansas-city/8032-bradleys-pkwy-peculiar", "/toronto/1234-yonge-st-m4t1x3"}, sorted(got)
    assert got["/kansas-city/gardner/1658-east-warren-st-66030"]["site_number"] == "1658"
    assert got["/kansas-city/olathe/66061-south-enterprise"]["site_number"] == ""
    assert got["/kansas-city/olathe/66061-south-enterprise"]["zip"] == "66061"
    assert got["/kansas-city/olathe/66061-south-enterprise"]["city"] == "Olathe"
    assert got["/kansas-city/lees-summit/465-oldham-pkwy"]["site_number"] == "465"
    assert got["/8028-highway-n-cottleville-63304"]["site_number"] == "8028"
    assert got["/8028-highway-n-cottleville-63304"]["zip"] == "63304"


def test_page_store_number_is_accepted_when_the_url_has_none():
    rec = parse_facility_html(FIXTURE, {"brand": "storagemart", "store_id": "", "site_number": "", "city": "Gardner",
                                        "zip": "66030", "url": f"{BASE}/kansas-city/gardner/1658-east-warren-st-66030"})
    assert rec["store_id"] == "smart_1658"


def test_page_whose_own_paths_do_not_include_the_asked_url_is_refused():
    expect_raises(LookupError, lambda: parse_facility_html(
        FIXTURE, {"brand": "storagemart", "store_id": "", "site_number": "",
                  "url": f"{BASE}/kansas-city/olathe/66061-south-enterprise"}))


def test_extract_data_reads_the_second_global_in_the_one_statement_script():
    data = extract_data(FIXTURE)
    assert data["facilities"]["allFacilities"][0]["storeNumber"] == "1658"


def test_parser_maps_the_live_unit_group_shape():
    rec = parse_facility_html(FIXTURE, STORE)
    assert rec["store_id"] == "smart_1658" and rec["name"] == "1658 StorageMart"
    assert rec["address"] == "850 E Warren St" and rec["city"] == "Gardner" and rec["state"] == "KS" and rec["zip"] == "66030"
    assert rec["phone"] == "913-884-6370"
    assert rec["lat"] == 38.8089123 and abs(rec["lng"] + 94.9102684) < 1e-6
    assert rec["software_provider"] == "storedge"
    assert len(rec["units"]) == 10
    tens = next(u for u in rec["units"] if u["size"] == "10x10")
    assert tens["sku"] == "smart_1d21bc72-5de4-4641-8a06-d6f32238b579"
    assert tens["price"] == 79 and tens["street_price"] == 89 and tens["standard_rate"] == 89
    assert tens["promo_price"] == 39.5 and tens["promo"] == "50% Off for 3 Months"
    assert tens["promo_terms"] == [{"month": m, "type": "percent", "amount": 50.0} for m in (1, 2, 3)]
    assert tens["count"] == 4 and tens["total"] == 24 and tens["available"] is True
    assert tens["attrs"] == "Drive-Up, Non-CC" and tens["sqft"] == 100 and tens["width"] == 10 and tens["depth"] == 10
    assert rec["units"][0]["size"] == "5x10", "units are sorted by area"


def test_managed_rate_above_standard_is_recorded_not_corrected():
    rec = parse_facility_html(FIXTURE, STORE)
    big = next(u for u in rec["units"] if u["size"] == "10x40")
    assert big["street_price"] == 426 and big["standard_rate"] == 394 and big["price"] == 380


def test_sold_out_groups_are_kept_unavailable_with_their_list_price():
    rec = parse_facility_html(FIXTURE, STORE)
    out = next(u for u in rec["units"] if u["size"] == "15x30")
    assert out["available"] is False and out["count"] == 0 and out["total"] == 2
    assert out["price"] == 436 and out["promo"] == "" and out["promo_terms"] == [] and out["promo_price"] is None


def test_url_number_is_not_trusted_for_identity_but_page_paths_are():
    # 465 Oldham Pkwy is store 0155: the catalog's guess from the URL is wrong and
    # must not matter, because the page lists the fetched path as its own.
    page = FIXTURE.replace('"storeNumber": "1658"', '"storeNumber": "0155"')
    rec = parse_facility_html(page, {**STORE, "site_number": "465", "store_id": "smart_465"})
    assert rec["store_id"] == "smart_0155" and rec["site_number"] == "0155"
    # ...and a page that does NOT list the fetched path is refused regardless of numbers.
    expect_raises(LookupError, lambda: parse_facility_html(
        FIXTURE, {**STORE, "url": f"{BASE}/kansas-city/lees-summit/465-oldham-pkwy"}))


def test_country_is_checked_before_identity():
    from storagemart_parser import NotUS
    page = FIXTURE.replace('"country": "US"', '"country": "CA"')
    expect_raises(NotUS, lambda: parse_facility_html(page, {**STORE, "url": f"{BASE}/edmonton/127-street-northwest"}))


def test_non_us_facility_is_refused():
    from storagemart_parser import NotUS
    page = FIXTURE.replace('"country": "US"', '"country": "CA"')
    expect_raises(NotUS, lambda: parse_facility_html(page, STORE))


def test_non_us_pages_are_excluded_without_counting_against_the_skip_cap():
    stores = [str(1000 + i) for i in range(100)]
    ca = FIXTURE.replace('"country": "US"', '"country": "CA"')
    pages = {s: (f"{BASE}/kansas-city/{s}-main-st-66030", ca.replace('"storeNumber": "1658"', f'"storeNumber": "{s}"')
                 .replace('"path": "/kansas-city/gardner/1658-east-warren-st-66030"', f'"path": "/kansas-city/{s}-main-st-66030"'))
             for s in ("1005", "1015", "1025", "1035", "1045", "1055")}   # six: well past the cap of 2
    with TemporaryDirectory() as folder:
        code, report, _ = _drive(pages, stores, folder)
        assert code == 0 and report["status"] == "complete", report
        assert len(report["excluded_non_us"]) == 6 and report["facility_count"] == 94


def test_missing_data_fails_loudly():
    expect_raises(KeyError, lambda: parse_facility_html("<html><body>nothing</body></html>", STORE))
    expect_raises(KeyError, lambda: parse_facility_html(
        FIXTURE.replace('"allFacilities": [', '"allFacilities": [], "x": ['), STORE))


# ----------------------------------------------------------------- run policy
class _Resp:
    def __init__(self, url, text):
        self.url, self.text = url, text


def _drive(pages, stores, folder, date="2026-09-05", limit=0):
    """pages: store -> (final_url, html) or an exception to raise."""
    class FakeClient:
        def __init__(self, delay):
            pass
        def get(self, url):
            if url == storagemart_scraper.ROBOTS_URL:
                return _Resp(url, "User-agent: *\nDisallow: /admin/\n")
            if url == storagemart_scraper.SITEMAP_URL:
                return _Resp(url, sitemap(*[f"{BASE}/kansas-city/{s}-main-st-66030" for s in stores]))
            store = url.rsplit("/", 1)[-1].split("-")[0]
            v = pages.get(store)
            if isinstance(v, Exception):
                raise v
            final, html = v if v else (url, FIXTURE.replace('"storeNumber": "1658"', f'"storeNumber": "{store}"')
                                       .replace('"path": "/kansas-city/gardner/1658-east-warren-st-66030"',
                                                f'"path": "/kansas-city/{store}-main-st-66030"'))
            return _Resp(final, html)
    saved = (storagemart_scraper.PoliteSession, storagemart_scraper.MIN_SITEMAP_FACILITIES,
             storagemart_scraper.MIN_STORES)
    storagemart_scraper.PoliteSession = FakeClient
    storagemart_scraper.MIN_SITEMAP_FACILITIES = 5
    storagemart_scraper.MIN_STORES = 5
    try:
        root = Path(folder)
        args = Namespace(date=date, delay=5, max_runtime_minutes=0, snapshot_dir=root / "snap",
                         report=root / "report.json", limit=limit)
        code = run(args)
        return code, json.loads(args.report.read_text(encoding="utf-8")), root / "snap"
    finally:
        (storagemart_scraper.PoliteSession, storagemart_scraper.MIN_SITEMAP_FACILITIES,
         storagemart_scraper.MIN_STORES) = saved


def test_one_dead_link_is_skipped_and_the_run_publishes():
    stores = [str(1000 + i) for i in range(100)]
    with TemporaryDirectory() as folder:
        code, report, snap = _drive({"1042": (f"{BASE}/", "<html>home</html>")}, stores, folder)
        assert code == 3 and report["status"] == "complete_with_warnings", report
        assert list(report["skipped"]) == [f"{BASE}/kansas-city/1042-main-st-66030"] and report["facility_count"] == 99
        assert (snap / "2026-09-05.json").exists() and not (snap / "2026-09-05.partial.json").exists()


def test_too_many_dead_links_keeps_the_partial_and_publishes_nothing():
    stores = [str(1000 + i) for i in range(100)]
    pages = {s: storagemart_scraper.Dead("HTTP 404") for s in ("1010", "1040", "1070")}
    with TemporaryDirectory() as folder:
        code, report, snap = _drive(pages, stores, folder)
        assert code == 2 and "skipped" in report["error"], report
        assert not (snap / "2026-09-05.json").exists() and (snap / "2026-09-05.partial.json").exists()


def test_refusal_streak_stops_the_run():
    stores = [str(1000 + i) for i in range(100)]
    pages = {s: storagemart_scraper.Refused("HTTP 403 after 3 attempts") for s in stores}
    with TemporaryDirectory() as folder:
        code, report, _ = _drive(pages, stores, folder)
        assert code == 2 and "refused in a row" in report["error"], report


def test_same_day_rerun_resumes_and_a_complete_day_is_idempotent():
    stores = [str(1000 + i) for i in range(100)]
    with TemporaryDirectory() as folder:
        _drive({s: storagemart_scraper.Dead("HTTP 404") for s in ("1010", "1040", "1070")}, stores, folder)
        partial = json.loads((Path(folder) / "snap" / "2026-09-05.partial.json").read_text())
        assert partial["stores"], "partial should hold what was collected"
        code, report, _ = _drive({}, stores, folder)
        assert code == 0 and report["resumed_count"] == len(partial["stores"]), report
        code, report, _ = _drive({}, stores, folder)
        assert code == 0 and report["status"] == "already_complete"


def test_drop_against_previous_snapshot_does_not_publish():
    with TemporaryDirectory() as folder:
        snap = Path(folder) / "snap"
        snap.mkdir()
        rec = parse_facility_html(FIXTURE, STORE)
        snap.joinpath("2026-09-04.json").write_text(json.dumps([{**rec, "store_id": f"smart_{1000+i}"} for i in range(100)]))
        code, report, _ = _drive({}, [str(1000 + i) for i in range(80)], folder)
        assert code == 2 and "fell more than" in report["error"], report


def test_limit_never_publishes():
    with TemporaryDirectory() as folder:
        code, report, snap = _drive({}, [str(1000 + i) for i in range(20)], folder, limit=3)
        assert code == 4 and report["status"] == "limited" and not (snap / "2026-09-05.json").exists()


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failures = []
    for name, fn in tests:
        try:
            fn()
            print(f"  PASS  {name}")
        except Exception as exc:
            failures.append(name)
            print(f"  FAIL  {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - len(failures)}/{len(tests)} passed")
    raise SystemExit(1 if failures else 0)
