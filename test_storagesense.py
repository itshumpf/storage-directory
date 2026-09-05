"""Offline, fixture-driven tests for the Storage Sense collector.

    python test_storagesense.py          # or: python -m pytest -q test_storagesense.py

No network. Every fixture is a minimal hand-built document, so a failure here
is a statement about our code and never about the site being up.

WHAT IS AND IS NOT VERIFIED
---------------------------
**Verified:** every raise path in the parser, the robots Allow-under-Disallow
case the custom parser exists for, the refresh-cycle arithmetic, the
change-log diff including its interval column, and that a second snapshot
write on the same day merges rather than replaces.

**Not verified:** that Storage Sense's live pages have the shape these
fixtures assume. The fixtures are built from the structure the parser expects,
so they prove the parser is self-consistent — not that the site still emits
`candee_js_variables`, `.unitsTable`, or a Schema.org ItemList. Only a live
run settles that, and the parser is written to raise loudly if any of them
has moved.
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import storagesense_scraper as sc
from storagesense_parser import parse_catalog_html, parse_facility_html


# --------------------------------------------------------------- fixtures ---

def catalog_html(facilities: list[dict]) -> str:
    return (
        "<html><body><script>\n"
        "var candee_js_variables = "
        + json.dumps({"facilities": facilities})
        + ";\n</script></body></html>"
    )


def facility(fid: str = "101", **over) -> dict:
    base = {
        "facility_id": fid,
        "permalink": f"https://www.storagesense.com/locations/site-{fid}/",
        "facility_name": f"Storage Sense {fid}",
        "facility_address": "1 Main St",
        "facility_city": "Olathe",
        "facility_region": "ks",
        "facility_zipcode": "66061",
        "facility_phone": "(913) 555-0100",
        "lat": "38.88",
        "lng": "-94.82",
        "facility_features": [{"text": "Drive-up"}],
    }
    base.update(over)
    return base


def unit_page(units: list[dict], *, ld_units: list[dict] | None = None) -> str:
    """A facility page. `units` drive the cards; `ld_units` the JSON-LD."""
    ld = ld_units if ld_units is not None else units
    item_list = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "itemListElement": [
            {"@type": "ListItem", "item": {
                "@type": "Product",
                "name": u["name"],
                **({"sku": u["sku"]} if u.get("sku") is not None else {}),
                "additionalProperty": [
                    {"name": "Unit Size", "value": f"{u.get('sqft', 50)} sq ft"},
                    {"name": "Feature", "value": "Climate Controlled"},
                ],
            }} for u in ld
        ],
    }
    cards = "".join(
        f'<div class="unitsTable">'
        f'<span class="unitName">{u["name"]}</span>'
        f'<span class="currentUnit-price">${u["price"]}</span>'
        + (f'<span class="strikeThrough-price">${u["street"]}</span>' if u.get("street") else "")
        + (f'<span class="discountText">{u["promo"]}</span>' if u.get("promo") else "")
        + f'<span class="unitFeatureItem" data-value="Climate"></span>'
        f'<button>Choose Unit</button></div>'
        for u in units
    )
    return (f'<html><body><script type="application/ld+json">{json.dumps(item_list)}'
            f"</script>{cards}</body></html>")


CATALOG_STORE = {
    "brand": "storagesense", "store_id": "sense_101", "site_number": "101",
    "name": "Storage Sense 101", "address": "1 Main St", "city": "Olathe",
    "state": "KS", "zip": "66061", "phone": "913-555-0100",
    "lat": 38.88, "lng": -94.82, "url": "https://x/", "facility_features": [],
}


def expect_raises(exc_types, fn, label: str) -> None:
    try:
        fn()
    except exc_types:
        return
    except Exception as other:  # pragma: no cover - a wrong exception is a failure
        raise AssertionError(f"{label}: raised {type(other).__name__}, expected {exc_types}") from other
    raise AssertionError(f"{label}: did not raise")


# ---------------------------------------------------------------- catalog ---

def test_catalog_happy_path():
    rows = parse_catalog_html(catalog_html([facility("101"), facility("102")]))
    assert len(rows) == 2
    assert rows[0]["store_id"] == "sense_101"
    assert rows[0]["site_number"] == "101"
    assert rows[0]["state"] == "KS", "region must be upper-cased"
    assert rows[0]["phone"] == "913-555-0100"
    assert rows[0]["lat"] == 38.88


def test_catalog_rejects_duplicate_facility_id():
    expect_raises(ValueError,
                  lambda: parse_catalog_html(catalog_html([facility("101"), facility("101")])),
                  "duplicate facility id")


def test_catalog_rejects_facility_without_url():
    bad = facility("103")
    del bad["permalink"]
    expect_raises(ValueError, lambda: parse_catalog_html(catalog_html([bad])),
                  "facility with no canonical URL")


def test_catalog_rejects_empty_and_missing():
    expect_raises(ValueError, lambda: parse_catalog_html(""), "empty catalog HTML")
    expect_raises(ValueError, lambda: parse_catalog_html(catalog_html([])), "zero facilities")
    expect_raises(KeyError, lambda: parse_catalog_html("<html><body>nothing</body></html>"),
                  "missing candee_js_variables")


def test_catalog_survives_braces_inside_strings():
    """A regex brace-matcher would truncate here; raw_decode does not."""
    rows = parse_catalog_html(catalog_html([facility("104", facility_name="A {weird} name")]))
    assert rows[0]["name"] == "A {weird} name"


# --------------------------------------------------------------- facility ---

def test_facility_happy_path():
    page = unit_page([{"name": "5x10", "sku": "U1", "price": 99, "street": 130, "promo": "First month free"}])
    record = parse_facility_html(page, CATALOG_STORE)
    assert len(record["units"]) == 1
    unit = record["units"][0]
    assert unit["sku"] == "sense_U1", "SKUs must be brand-prefixed"
    assert unit["size"] == "5x10" and unit["width"] == 5 and unit["depth"] == 10
    assert unit["price"] == 99 and unit["street_price"] == 130
    assert unit["available"] is True
    assert record["store_id"] == "sense_101"


def test_facility_accepts_repeated_unit_names():
    """A name is a description, not a key. 8% of live facilities repeat one.

    'Medium 10x10 Drive Up' can cover several SKUs at one facility — different
    buildings, floors or price tiers. Treating names as unique rejected 4 of
    the first 50 facilities on 2026-09-02.
    """
    units = [{"name": "Medium 10x10 Drive Up", "sku": "U1", "price": 149},
             {"name": "Medium 10x10 Drive Up", "sku": "U2", "price": 149},
             {"name": "Small 5x5 Lockers", "sku": "U3", "price": 39}]
    record = parse_facility_html(unit_page(units), CATALOG_STORE)
    assert sorted(u["sku"] for u in record["units"]) == ["sense_U1", "sense_U2", "sense_U3"]


def test_facility_flags_ambiguity_only_when_it_changes_a_number():
    """Identical twins need no flag; differing ones do."""
    same = [{"name": "Medium 10x10 Drive Up", "sku": "U1", "price": 149},
            {"name": "Medium 10x10 Drive Up", "sku": "U2", "price": 149}]
    for unit in parse_facility_html(unit_page(same), CATALOG_STORE)["units"]:
        assert unit["name_ambiguous"] is False, "identical cards make pairing immaterial"

    differ = [{"name": "Medium 10x10 Drive Up", "sku": "U1", "price": 149},
              {"name": "Medium 10x10 Drive Up", "sku": "U2", "price": 179}]
    for unit in parse_facility_html(unit_page(differ), CATALOG_STORE)["units"]:
        assert unit["name_ambiguous"] is True, "price-to-SKU binding here rests on order"


def test_facility_rejects_uneven_name_group():
    """Two cards, one JSON-LD entry of that name: a real disagreement."""
    page = unit_page(
        [{"name": "Medium 10x10 Drive Up", "sku": "U1", "price": 149},
         {"name": "Medium 10x10 Drive Up", "sku": "U2", "price": 179}],
        ld_units=[{"name": "Medium 10x10 Drive Up", "sku": "U1", "price": 149},
                  {"name": "Small 5x5 Lockers", "sku": "U3", "price": 39}],
    )
    expect_raises(ValueError, lambda: parse_facility_html(page, CATALOG_STORE),
                  "uneven cards/JSON-LD within one name")


def test_facility_rejects_count_mismatch():
    page = unit_page(
        [{"name": "5x10", "sku": "U1", "price": 99}],
        ld_units=[{"name": "5x10", "sku": "U1", "price": 99},
                  {"name": "10x10", "sku": "U2", "price": 149}],
    )
    expect_raises(ValueError, lambda: parse_facility_html(page, CATALOG_STORE),
                  "JSON-LD/card count disagreement")


def test_facility_rejects_card_with_no_json_ld_twin():
    page = unit_page(
        [{"name": "5x10", "sku": "U1", "price": 99}],
        ld_units=[{"name": "10x10", "sku": "U2", "price": 99}],
    )
    expect_raises(ValueError, lambda: parse_facility_html(page, CATALOG_STORE),
                  "card with no matching JSON-LD unit")


def test_facility_rejects_unit_without_sku():
    page = unit_page([{"name": "5x10", "sku": None, "price": 99}])
    expect_raises(ValueError, lambda: parse_facility_html(page, CATALOG_STORE),
                  "JSON-LD unit carrying no SKU")


def test_facility_rejects_foreign_catalog_store():
    page = unit_page([{"name": "5x10", "sku": "U1", "price": 99}])
    expect_raises(ValueError,
                  lambda: parse_facility_html(page, {**CATALOG_STORE, "brand": "cubesmart"}),
                  "catalog_store from another brand")


def test_facility_position_is_never_the_identity():
    """Reordering the cards must not change any SKU.

    This is the defect that made CubeSmart's fallback dangerous: a positional
    key turns one unit selling out into every unit below it repricing.
    """
    units = [{"name": "5x10", "sku": "U1", "price": 99},
             {"name": "10x10", "sku": "U2", "price": 149}]
    forward = parse_facility_html(unit_page(units), CATALOG_STORE)
    reversed_ = parse_facility_html(unit_page(list(reversed(units))), CATALOG_STORE)
    assert ({u["sku"]: u["price"] for u in forward["units"]}
            == {u["sku"]: u["price"] for u in reversed_["units"]})


# --------------------------------------------------------------- coverage ---

def test_coverage_closes_when_budget_is_sufficient():
    result = sc._coverage(catalog_size=250, budget=45, refresh_hours=24 * 7)
    assert result["cycle_closes"] is True
    assert "warning" not in result


def test_coverage_fails_and_says_what_would_fix_it():
    result = sc._coverage(catalog_size=400, budget=45, refresh_hours=24 * 7)
    assert result["cycle_closes"] is False
    assert "warning" in result
    # 400 / 7 days = 57.1/day, so the suggested budget must clear the real need.
    assert result["refreshes_per_window"] == 315.0
    assert 400 / (result["daily_budget"] * result["refresh_window_days"]) > 1


def test_full_daily_snapshot_uses_all_facilities_not_a_fixed_cap():
    result = sc._coverage(catalog_size=309, budget=0, refresh_hours=0)
    assert result["cycle_closes"] is True
    assert result["daily_budget"] == "all"
    now = datetime(2026, 9, 2, 17, 43, tzinfo=timezone.utc)
    catalog = [{"site_number": "101"}, {"site_number": "102"}]
    existing = {"101": {"last_checked_at": now.isoformat()},
                "102": {"last_checked_at": now.isoformat()}}
    selected, due_total = sc._select_due(catalog, existing, budget=0, refresh_hours=0, now=now)
    assert due_total == 2 and len(selected) == 2


# ------------------------------------------------------------ change diff ---

def _record(price: int, *, checked: datetime, street: int = 130, promo: str = "") -> dict:
    return {"brand": "storagesense", "store_id": "sense_101", "site_number": "101",
            "last_checked_at": checked.isoformat(),
            "units": [{"sku": "sense_U1", "size": "5x10", "price": price,
                       "street_price": street, "promo": promo}]}


def test_change_diff_records_price_move_and_its_interval():
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    rows = sc._unit_changes(_record(99, checked=then),
                            _record(109, checked=then + timedelta(days=7)),
                            "2026-09-08")
    assert len(rows) == 1
    row = dict(zip(sc.CHANGE_HEADER, rows[0]))
    assert row["field"] == "price" and row["old"] == 99 and row["new"] == 109
    assert row["sku"] == "sense_U1"
    assert row["days_since_previous"] == 7.0, "the interval is variable and must be on the row"


def test_change_diff_is_silent_when_nothing_moved():
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    assert sc._unit_changes(_record(99, checked=then),
                            _record(99, checked=then + timedelta(days=7)),
                            "2026-09-08") == []


def test_change_diff_ignores_a_unit_going_unavailable():
    """No price means rented, not repriced. 120 -> None -> 120 is one rental."""
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    rented = _record(99, checked=then + timedelta(days=7))
    rented["units"][0]["price"] = None
    assert sc._unit_changes(_record(99, checked=then), rented, "2026-09-08") == []
    # and coming back is not a change either
    assert sc._unit_changes(rented, _record(99, checked=then + timedelta(days=14)),
                            "2026-09-15") == []


def test_change_diff_skips_first_sighting():
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    empty = {"brand": "storagesense", "store_id": "sense_101", "site_number": "101",
             "last_checked_at": then.isoformat(), "units": []}
    assert sc._unit_changes(empty, _record(99, checked=then + timedelta(days=7)),
                            "2026-09-08") == []


# ------------------------------------------------------------ availability ---

def test_availability_logs_a_size_going_unbookable():
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    gone = _record(99, checked=then + timedelta(days=7))
    gone["units"][0]["price"] = None
    rows = sc._availability_changes(_record(99, checked=then), gone, "2026-09-08")
    assert len(rows) == 1
    row = dict(zip(sc.AVAIL_HEADER, rows[0]))
    assert row["transition"] == "unpriced"
    assert row["last_price"] == 99, "the price it was last bookable at is the useful part"
    assert row["days_since_previous"] == 7.0


def test_availability_separates_delisted_from_unpriced():
    """Whether a sold-out size vanishes or stays unpriced is not yet known.

    Both are recorded under their own name so the first runs of the log
    answer the question, instead of one being quietly folded into the other.
    """
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    vanished = _record(99, checked=then + timedelta(days=7))
    vanished["units"] = []
    rows = sc._availability_changes(_record(99, checked=then), vanished, "2026-09-08")
    assert [dict(zip(sc.AVAIL_HEADER, r))["transition"] for r in rows] == ["delisted"]


def test_availability_logs_a_size_coming_back():
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    was_gone = _record(99, checked=then)
    was_gone["units"][0]["price"] = None
    rows = sc._availability_changes(was_gone, _record(120, checked=then + timedelta(days=7)),
                                    "2026-09-08")
    assert [dict(zip(sc.AVAIL_HEADER, r))["transition"] for r in rows] == ["repriced_from_unpriced"]


def test_availability_is_silent_on_first_sighting():
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    empty = {"brand": "storagesense", "store_id": "sense_101", "site_number": "101",
             "last_checked_at": then.isoformat(), "units": []}
    assert sc._availability_changes(empty, _record(99, checked=then + timedelta(days=7)),
                                    "2026-09-08") == []


def test_availability_and_rate_logs_do_not_overlap():
    """A rental must not land in the rate log as a price change."""
    then = datetime(2026, 9, 1, tzinfo=timezone.utc)
    gone = _record(99, checked=then + timedelta(days=7))
    gone["units"][0]["price"] = None
    before = _record(99, checked=then)
    assert sc._unit_changes(before, gone, "2026-09-08") == []
    assert len(sc._availability_changes(before, gone, "2026-09-08")) == 1


# --------------------------------------------------------------- snapshot ---

def test_snapshot_merges_a_second_run_on_the_same_day():
    """Two runs in one day must not lose the first run's observations."""
    original = sc.SNAPSHOT_DIR
    tmp = Path(tempfile.mkdtemp())
    try:
        sc.SNAPSHOT_DIR = tmp / "storagesense"
        first = [{"site_number": "101", "units": []}]
        second = [{"site_number": "102", "units": []}]
        _, count_a = sc._write_snapshot(first, "2026-09-02")
        path, count_b = sc._write_snapshot(second, "2026-09-02")
        assert count_a == 1 and count_b == 2, "second run must merge, not replace"
        sites = {r["site_number"] for r in json.loads(path.read_text())}
        assert sites == {"101", "102"}
    finally:
        sc.SNAPSHOT_DIR = original
        shutil.rmtree(tmp, ignore_errors=True)


# ----------------------------------------------------------------- robots ---

ROBOTS = """User-agent: *
Disallow: /wp-admin/
Allow: /wp-admin/admin-ajax.php
Crawl-delay: 8

User-agent: BadBot
Disallow: /
"""


def test_robots_allow_beats_broader_disallow():
    """The case the custom parser exists for."""
    allowed, delay = sc._robots_can_fetch(ROBOTS, sc.USER_AGENT,
                                          "https://www.storagesense.com/wp-admin/admin-ajax.php")
    assert allowed is True, "an explicit Allow must win under a broader Disallow"
    assert delay == 8


def test_robots_still_blocks_the_rest_of_wp_admin():
    allowed, _ = sc._robots_can_fetch(ROBOTS, sc.USER_AGENT,
                                      "https://www.storagesense.com/wp-admin/options.php")
    assert allowed is False


def test_robots_allows_an_unmentioned_path():
    allowed, _ = sc._robots_can_fetch(ROBOTS, sc.USER_AGENT,
                                      "https://www.storagesense.com/locations/")
    assert allowed is True


# ------------------------------------------------------------ retry-after ---

def test_retry_after_seconds_forms():
    assert sc._retry_after_seconds("120") == 120
    assert sc._retry_after_seconds(None) == sc.DEFAULT_COOLDOWN_HOURS * 3600
    assert sc._retry_after_seconds("not a date") == sc.DEFAULT_COOLDOWN_HOURS * 3600
    now = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
    assert sc._retry_after_seconds("Wed, 02 Sep 2026 12:05:00 GMT", now) == 300


# -------------------------------------------------------------------- run ---

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
