"""
extraspace_parser.py — Turn an Extra Space facility JSON into our normalized schema.

Extra Space is a Next.js site. Each facility has a data twin at:
    https://www.extraspace.com/_next/data/<BUILD_ID>/en-US/storage/facilities/us/<state>/<city>/<siteNumber>.json

Inside that JSON the useful parts live at:
    pageProps.pageData.data.facilityData.data.store          -> the store
    pageProps.pageData.data.unitClasses.data.unitClasses[]    -> the units

IMPORTANT — the <BUILD_ID> (e.g. "mI_3xeiHvHEYu5qAIouLH") changes every time
Extra Space redeploys. Do NOT hardcode it. Read it live from any page's
window.__NEXT_DATA__.buildId (or scrape it from the __NEXT_DATA__ <script> in the
facility HTML), then build the URL. build_facility_url() below shows the shape.

Enumeration: the search endpoint
    /_next/data/<BUILD_ID>/en-US/storage.json?searchTerm=<ZIP>
returns store summaries (site numbers) under pageProps.pageData.data — walk ZIPs
or the state/city index to collect every siteNumber, then fetch each facility JSON.

This module only PARSES already-fetched JSON. Fetching/enumeration is separate so
this stays easy to unit-test on a saved sample.

Output matches the PS `units` shape (so update_history.py works unchanged), plus:
  - brand              "extraspace"
  - street_price       the real standard rate (PS never gave us a clean one)
  - rates              the full ladder {web, street, walkIn, nsc, tier1..3}
"""
from __future__ import annotations


def _dig(d, *path, default=None):
    """Safely walk nested dict keys; return default if any hop is missing."""
    for k in path:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
    return d if d is not None else default


def build_facility_url(build_id, state, city, site_number):
    """Construct the _next/data URL for one facility. build_id must be read live."""
    return (f"https://www.extraspace.com/_next/data/{build_id}"
            f"/en-US/storage/facilities/us/{state}/{city}/{site_number}.json")


def parse_unit(u: dict) -> dict:
    """Map one Extra Space unitClass into our normalized unit dict."""
    dims = u.get("dimensions") or {}
    avail = u.get("availability") or {}
    rates = u.get("rates") or {}

    # normalize size to PS style: 5' x 5' -> "5x5"
    w, dp = dims.get("width"), dims.get("depth")
    size = f"{w}x{dp}" if w and dp else (dims.get("display") or "").replace("'", "").replace(" ", "")

    # attrs as a PS-style comma string (so is_climate() and display keep working)
    feats = [f.get("display") for f in (u.get("features") or []) if f.get("display")]
    attrs = ", ".join(feats)

    # promo: join distinct promotion descriptions
    promos = []
    for p in (u.get("promotions") or []):
        desc = _dig(p, "discount", "description")
        if desc and desc not in promos:
            promos.append(desc)
    promo = "; ".join(promos)

    count = avail.get("available")
    return {
        "size": size,
        "price": rates.get("web"),            # web rate = PS "price" analog
        "street_price": rates.get("street"),  # NEW: real standard rate
        "rates": {k: rates.get(k) for k in           # full ladder, for later analysis
                  ("web", "street", "walkIn", "nsc", "tier1", "tier2", "tier3")},
        "available": bool(count) or bool(avail.get("showAvailable")),
        "count": count or 0,
        "sku": u.get("uid"),                  # stable id: unitClass_site
        "attrs": attrs,
        "promo": promo,
        "promo2": "",
        "sqft": dims.get("squareFoot"),
        "width": w,
        "depth": dp,
        "size_class": dims.get("size"),       # Small / Medium / Large
    }


def parse_facility(facility_json: dict, site_number=None, brand="extraspace") -> dict:
    """Turn a fetched facility JSON into one normalized store record with units[].

    Accepts either the full raw feed (pageProps.pageData.data...) or a trimmed
    test shape: {"store": {...}, "unit": {...}} or {"store": {...}, "units": [...]}.
    """
    if "pageProps" in facility_json:
        data = _dig(facility_json, "pageProps", "pageData", "data", default={})
        store = _dig(data, "facilityData", "data", "store", default={})
        raw_units = _dig(data, "unitClasses", "data", "unitClasses", default=[]) or []
    else:  # trimmed test shape
        store = facility_json.get("store") or {}
        raw_units = facility_json.get("units") or (
            [facility_json["unit"]] if facility_json.get("unit") else [])
    addr = store.get("address") or {}

    units = [parse_unit(u) for u in raw_units]
    units = [u for u in units if u["price"] is not None]  # keep priced units

    return {
        "brand": brand,
        "store_id": store.get("storeId"),
        # NOTE: confirm the exact store key for the public site number against a
        # full store object (51 keys). URL path site number is the reliable fallback.
        "site_number": store.get("siteNumber") or site_number,
        "name": store.get("name") or store.get("displayName"),
        "line1": addr.get("line1"),
        "city": addr.get("city"),
        "state": addr.get("stateAbbreviation"),
        "zip": addr.get("postalCode"),
        "lat": store.get("latitude") or _dig(store, "geo", "latitude"),
        "lng": store.get("longitude") or _dig(store, "geo", "longitude"),
        "phone": store.get("phone"),
        "units": units,
    }


if __name__ == "__main__":
    import json, sys
    src = sys.argv[1]
    site = sys.argv[2] if len(sys.argv) > 2 else None
    rec = parse_facility(json.loads(open(src).read()), site_number=site)
    print(f"brand={rec['brand']} site={rec['site_number']} "
          f"{rec['city']},{rec['state']} {rec['zip']} — {len(rec['units'])} priced units")
    for u in rec["units"][:8]:
        print(f"  {u['size']:>7} {'CC' if 'Climate' in u['attrs'] else '  '} "
              f"web ${u['price']:<4} street ${u['street_price']:<4} "
              f"avail {u['count']:<3} {u['promo']}")