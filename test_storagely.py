from independent_scraper import Operator, parse_storagely_probe
from storagely_parser import parse_facility_html


HTML = """
<html><head><title>Right Move Storage</title>
<script type="application/ld+json">{
  "@type":"SelfStorage", "name":"Right Move Storage - Alvin",
  "telephone":"(281) 555-0100",
  "address":{"streetAddress":"301 Medic Lane","addressLocality":"Alvin",
             "addressRegion":"TX","postalCode":"77511","addressCountry":"US"},
  "geo":{"latitude":29.4,"longitude":-95.2}
}</script></head><body>
<script src="https://static.storagely.link/public/build/js/widgets.js"></script>
<div class="unit-card">
  <div class="unit-type-listing-name">Smart Units, Climate Control<span class="d-none">1256</span></div>
  <h2 class="widthHeight">5' WIDTH x 5' DEPTH</h2>
  <p class="page_discount"><span class="offer__content">1st Month's Rent Free</span></p>
  <h3 class="actualMoPrice">$0<small>/month</small></h3>
  <h3 class="withoutDiscntprice"><span>$98</span><small>/month</small></h3>
</div></body></html>
"""


def test_parses_server_rendered_storagely_unit_card():
    record = parse_facility_html(
        HTML, {"url": "https://example.com/storage-units/texas/alvin/medic-lane"},
        operator_id="right_move_storage")
    assert record["platform"] == "storagely"
    assert record["store_id"].endswith("storage_units_texas_alvin_medic_lane")
    assert record["address"] == "301 Medic Lane" and record["state"] == "TX"
    assert len(record["units"]) == 1
    unit = record["units"][0]
    assert unit["size"] == "5x5" and unit["price"] == 98
    assert unit["promo_price"] == 0 and unit["promo"] == "1st Month's Rent Free"
    assert unit["attrs"] == "Smart Units, Climate Control"


def test_preserves_explicitly_sold_out_facility_with_zero_offers():
    sold_out = HTML.replace(
        '<div class="unit-card">', '<div class="unit-card" style="display:none">').replace(
        '<div class="unit-type-listing-name">', '<div class="removed-unit-type-name">').replace(
        "</body>", "<p>Join the Waitlist</p></body>")
    record = parse_facility_html(
        sold_out, {"url": "https://example.com/storage-units/texas/brookshire/fm-359-north"},
        operator_id="right_move_storage")
    assert record["inventory_status"] == "sold_out"
    assert record["units"] == []


def test_smoke_probe_reports_parser_completeness():
    result = parse_storagely_probe(
        Operator("right_move", "Right Move",
                 "https://example.com/storage-units/texas/alvin/medic-lane", "storagely"),
        HTML,
    )
    assert result == {
        "parsed_facilities": 1, "unit_groups": 1, "inventory_status": "available",
        "address_complete": True,
    }


def test_parses_new_theme_json_ld_inventory_and_deduplicates_sku():
    item_list = """{
      "@type":"ItemList", "itemListElement":[{"@type":"ListItem", "item":{
        "@type":"Product", "name":"10' × 20' Heated Drive-Up Storage Unit",
        "category":"SelfStorageUnit", "sku":"5859832",
        "description":"Size: 10' x 20'. Amenities: Heated, Drive-Up. Facility: Arctic Blvd.",
        "offers":{"price":408,"priceCurrency":"USD",
                  "availability":"https://schema.org/InStock"}
      }}]
    }"""
    html = f"""<html><head><title>Storage Star</title>
      <script type="application/ld+json">{{
        "@type":"SelfStorage", "name":"Storage Star - Arctic",
        "address":{{"streetAddress":"123 Arctic Blvd","addressLocality":"Anchorage",
                    "addressRegion":"AK","postalCode":"99503","addressCountry":"US"}}
      }}</script>
      <script type="application/ld+json">{item_list}</script>
      <script type="application/ld+json">{item_list}</script>
      <script src="https://cdn.storagely.io/app.js"></script></head><body></body></html>"""
    record = parse_facility_html(
        html, {"url": "https://example.com/storage-units/alaska/anchorage/arctic-blvd"},
        operator_id="storage_star")
    assert len(record["units"]) == 1
    unit = record["units"][0]
    assert unit["sku"] == "storage_star_5859832"
    assert unit["size"] == "10x20" and unit["price"] == 408
    assert unit["attrs"] == "Heated, Drive-Up" and unit["available"] is True
