"""storagemart_parser.py — StorageMart's sitemap and facility pages into the shared record shape.

WHERE THE DATA IS (verified live 2026-09-05, facility 1658, Gardner KS)
----------------------------------------------------------------------
StorageMart's site runs on storEDGE's "voyager" template. Every facility page
is server-rendered with one <script> that assigns two globals in a single
statement:

    window.__APOLLO_STATE__= {...},window.__data= {...}

__APOLLO_STATE__ is a normalized GraphQL cache and its UnitGroup entries carry
only name and price. The complete facility — address, coordinates, phone,
store number, and every unit group with pricing, promotions and counts — is
in __data.facilities.allFacilities[0]. That object is what this parser reads.
The JSON-LD SelfStorage block is also present and is used as a cross-check for
the address, nothing more.

The DOM is NOT read. The unit list is rendered client-side from the same data,
and on the page observed it rendered "Call for Availability" while __data
carried ten priced unit groups — so the DOM is the less complete source.

WHAT A UNIT GROUP GIVES US, AND HOW IT MAPS
-------------------------------------------
A "unit group" is one size × amenity class ("10x10 Self Storage Drive-Up"),
with a stable GUID id. That id is the SKU — the same class keeps the same id
day to day, which is exactly the property the rate log needs and the property
CubeSmart's per-unit GUIDs lack.

  price          ←  discountedPrice   the recurring online rate (89 → 79 when
                                      "enableUsePromoModifiedPricing" is on)
  street_price   ←  price             the managed full rate. NOTE: this can sit
                                      ABOVE standardRate (10x40: 426 vs 394) —
                                      managed/dynamic pricing on top of the base
  standard_rate  ←  standardRate      the base rate, kept for the record
  promo_price    ←  promoPrice        what month 1 costs (39.5 = 50% of 79)
  promo          ←  the auto-applied discount plan's publicDescription
  promo_terms    ←  the plan's discountPlanDiscounts, month by month — the
                    first operator in this dataset to publish promotion terms
                    as data rather than copy
  count          ←  availableUnitsCount
  total          ←  totalUnitsCount   units of this class at the facility. New
                                      to this dataset: count/total is an
                                      advertised-occupancy signal no other
                                      operator exposes
  available      ←  availableUnitsCount > 0
  attrs          ←  amenities[].name joined ("Drive-Up, Non-CC")

Zero-availability groups are kept with available=False and their list price,
the same convention Storage Sense uses.
"""
from __future__ import annotations

import json
import re
from xml.etree import ElementTree

from storable_adapter import NotUS, extract_storable_data, parse_storable_facility

BRAND = "storagemart"
BASE = "https://www.storage-mart.com"


# Facility URLs come in FOUR shapes (sitemap.xml read 2026-09-05, 984 URLs):
#   /kansas-city/gardner/1658-east-warren-st-66030     store# + street + zip   (99 of these)
#   /kansas-city/olathe/66061-south-enterprise         zip + street, NO store#  (the majority)
#   /kansas-city/lees-summit/465-oldham-pkwy           STREET number (store 0155), no zip
#   /des-moines/waukee/225-ne-venture-dr-1071          street number first, store# LAST
#   /8028-highway-n-cottleville-63304                  root level, store# + zip
# and metro/city hubs, size pages (/kansas-city/10x10-storage), service pages
# (/olathe/car-storage), and Canadian/UK stores with non-5-digit postcodes.
#
# So the URL is only a CANDIDATE, and a number in it may be a store number, a
# street number, or a zip. The store number is read from the page's own
# storeNumber; nothing in the URL is trusted for identity.
# The first version keyed on the store#+zip shape alone and found 99 of ~200;
# the catalog floor caught it before a single facility page was fetched.
LAST_SEGMENT_RE = re.compile(r"^(?:(?P<lead>\d{3,5})-[a-z0-9-]+|[a-z0-9-]+-(?P<zip>\d{5}))$")
SIZE_PAGE_RE = re.compile(r"^\d+x\d+-")


def catalog_entry(url: str) -> dict | None:
    """A catalog stub for a facility-looking URL, or None for anything else."""
    if not url.startswith(BASE):
        return None
    path = url[len(BASE):].rstrip("/")
    parts = path.strip("/").split("/")
    if not parts or not parts[-1]:
        return None
    last = parts[-1]
    if SIZE_PAGE_RE.match(last):
        return None
    m = LAST_SEGMENT_RE.match(last)
    if not m:
        return None
    lead, zip5 = m.group("lead"), m.group("zip")
    store = None
    if lead and len(lead) in (3, 4):
        store = lead
    elif lead and len(lead) == 5:
        zip5 = zip5 or lead            # zip-first shape
    if not zip5:
        tail = re.search(r"-(\d{5})$", last)   # store#-only shape with a trailing zip
        zip5 = tail.group(1) if tail else ""
    city = parts[-2] if len(parts) >= 2 else parts[0]
    return {
        "brand": BRAND,
        "store_id": f"smart_{store}" if store else "",   # settled by the page
        "site_number": store or "",
        "city": city.replace("-", " ").title(),
        "zip": zip5 or "",
        "url": BASE + path,
    }


def parse_sitemap_xml(xml_text: str) -> list[dict]:
    """Every facility-looking page in sitemap.xml, as catalog stubs keyed by URL."""
    if not xml_text:
        raise ValueError("sitemap must be non-empty")
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        raise ValueError("sitemap is not valid XML") from exc
    out, seen = [], set()
    for node in root.iter():
        if not node.tag.endswith("loc") or not node.text:
            continue
        entry = catalog_entry(node.text.strip())
        if entry and entry["url"] not in seen:
            seen.add(entry["url"])
            out.append(entry)
    return out


def extract_data(html_text: str) -> dict:
    """The window.__data object. Raises KeyError if the page does not carry it."""
    return extract_storable_data(html_text)


def _self_storage_ld(html_text: str) -> dict:
    for m in re.finditer(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html_text, re.S):
        try:
            obj = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and obj.get("@type") == "SelfStorage":
            return obj
    return {}


def parse_facility_html(html_text: str, catalog_store: dict) -> dict:
    if not html_text:
        raise ValueError("Facility HTML must be non-empty")
    if catalog_store.get("brand") != BRAND or not catalog_store.get("url"):
        raise ValueError("catalog_store must come from the StorageMart sitemap")
    data = extract_data(html_text)
    facilities = (data.get("facilities") or {}).get("allFacilities") or []
    if not facilities:
        raise KeyError("window.__data carries no facilities.allFacilities — template changed")
    asked = catalog_store["url"][len(BASE):].rstrip("/")
    return parse_storable_facility(
        facilities[0], catalog_store, brand=BRAND, store_id_prefix="smart", sku_prefix="smart",
        fallback_name="StorageMart", expected_path=asked, json_ld=_self_storage_ld(html_text),
        operator_id="storagemart",
    )
