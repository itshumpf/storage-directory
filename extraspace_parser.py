"""
extraspace_parser.py — Turn an Extra Space facility JSON into the FindStorage normalized schema.

Designed for FindStorage.pages.dev:
- Fails loudly on schema mismatch rather than creating default/hallucinated data.
- Maps all store & unit fields to match enriched_locations.json exactly.
- Preserves full rate ladder {web, street, walkIn, nsc, tier1..3} for deep analytics.
"""
from __future__ import annotations
import hashlib
import re
from typing import Any, Dict, List, Optional


def _clean_phone(phone_data: Any) -> str:
    """Format phone number object or string into standard XXX-XXX-XXXX format."""
    if not phone_data:
        return ""
    if isinstance(phone_data, dict):
        raw = phone_data.get("internetSearchNumber") or phone_data.get("existingCustomerNumber") or ""
    else:
        raw = str(phone_data)
    
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return raw


def _clean_size(dims: dict) -> str:
    """Normalize size to standard format (e.g. 5x5, 10x10, 10x20)."""
    w = dims.get("width")
    dp = dims.get("depth")
    if w and dp:
        w_str = str(int(w)) if isinstance(w, (int, float)) and w == int(w) else str(w)
        dp_str = str(int(dp)) if isinstance(dp, (int, float)) and dp == int(dp) else str(dp)
        return f"{w_str}x{dp_str}"
    
    display = (dims.get("display") or "").replace("'", "").replace(" ", "").replace("X", "x")
    return display or "Other"


def parse_unit(u: dict, site_number: str) -> dict:
    """Map one Extra Space unitClass into normalized unit dict.
    
    Fails loudly if required pricing or SKU structures are corrupt.
    """
    if not isinstance(u, dict):
        raise ValueError(f"Unit payload must be a dict, got {type(u)}")

    rates = u.get("rates")
    if not rates or not isinstance(rates, dict):
        raise ValueError(f"Missing 'rates' dict in unit payload: {u}")

    web_rate = rates.get("web")
    street_rate = rates.get("street")
    
    if web_rate is not None:
        if not isinstance(web_rate, (int, float)) or web_rate < 0:
            raise ValueError(f"Invalid web rate '{web_rate}' in unit: {u}")
        web_rate = int(round(web_rate))

    if street_rate is not None:
        street_rate = int(round(street_rate))

    dims = u.get("dimensions") or {}
    avail = u.get("availability") or {}
    
    size = _clean_size(dims)
    
    # Features & attributes
    feats = [f.get("display") for f in (u.get("features") or []) if isinstance(f, dict) and f.get("display")]
    attrs = ", ".join(feats)

    # Promotions: join distinct promotion descriptions
    promos = []
    for p in (u.get("promotions") or []):
        if isinstance(p, dict):
            disc = p.get("discount")
            if isinstance(disc, dict):
                desc = disc.get("description")
                if desc and desc not in promos:
                    promos.append(desc.strip())
    promo = promos[0] if promos else ""
    promo2 = promos[1] if len(promos) > 1 else ""

    # Unit availability & counts
    raw_count = avail.get("available")
    count = int(raw_count) if isinstance(raw_count, (int, float)) else 0
    is_available = bool(count > 0) or bool(avail.get("showAvailable"))

    raw_uid = u.get("uid") or u.get("id")
    if raw_uid:
        sku = f"exr_{raw_uid}"
    else:
        # The old fallback was f"exr_unit_{site_number}_{size}". A facility
        # routinely offers several unit classes at the same size -- climate
        # controlled and not, ground floor and upper -- so that key collapsed
        # them onto one another. The last one parsed won, silently, and the
        # rate log would then read two different units' prices as one unit
        # flip-flopping day to day.
        #
        # Size plus the unit's own features and floor is enough to tell those
        # apart, and it is stable as long as the unit is. Price is excluded on
        # purpose: a repricing must show up as a changed price on the same SKU,
        # not as one unit disappearing and another arriving.
        #
        # 'exrh_' marks it as derived, so it is never mistaken later for a real
        # Extra Space uid.
        seed = "|".join([
            str(site_number), size, attrs or "",
            str(dims.get("squareFoot") or ""), str(u.get("floor") or ""),
            str(u.get("unitTypeId") or u.get("classId") or ""),
        ])
        digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
        sku = f"exrh_{site_number}_{digest}"

    return {
        "size": size,
        "price": web_rate,                    # Web rate (used by FindStorage UI)
        "street_price": street_rate,          # Street rate for markdown comparison
        "available": is_available,
        "count": count,
        "promo": promo,
        "promo2": promo2,
        "sku": sku,
        "attrs": attrs,
        "sqft": dims.get("squareFoot"),
        "width": dims.get("width"),
        "depth": dims.get("depth"),
        "size_class": dims.get("size"),       # Small / Medium / Large
        "rates": {                            # Full rate ladder for analytics
            "web": web_rate,
            "street": street_rate,
            "walkIn": rates.get("walkIn"),
            "nsc": rates.get("nsc"),
            "tier1": rates.get("tier1"),
            "tier2": rates.get("tier2"),
            "tier3": rates.get("tier3")
        }
    }


def parse_facility(facility_json: dict, site_number: Optional[str] = None) -> dict:
    """Turn fetched Next.js facility JSON into a FindStorage store record.
    
    Fails loudly if expected Next.js store structure is missing.
    """
    if not isinstance(facility_json, dict):
        raise ValueError(f"Facility JSON must be a dict, got {type(facility_json)}")

    # Navigate Next.js pageProps
    if "pageProps" in facility_json:
        page_props = facility_json.get("pageProps") or {}
        page_data = page_props.get("pageData")
        if not page_data or not isinstance(page_data, dict):
            raise KeyError("Malformed Next.js JSON: 'pageProps.pageData' is missing or not a dict")
        
        data_block = page_data.get("data") or {}
        facility_data = data_block.get("facilityData") or {}
        store = (facility_data.get("data") or {}).get("store")
        if not store or not isinstance(store, dict):
            raise KeyError("Malformed Next.js JSON: 'store' object not found under facilityData.data")
        
        unit_classes_block = data_block.get("unitClasses") or {}
        raw_units = (unit_classes_block.get("data") or {}).get("unitClasses")
        if raw_units is None:
            raise KeyError("Malformed Next.js JSON: 'unitClasses' list not found under unitClasses.data")
    else:
        # Fallback / trimmed test shape
        store = facility_json.get("store")
        if not store or not isinstance(store, dict):
            raise KeyError("Missing 'store' dictionary in JSON payload")
        raw_units = facility_json.get("units") or facility_json.get("unitClasses") or []

    # Store metadata extraction
    resolved_site_number = str(store.get("siteNumber") or site_number or "")
    if not resolved_site_number:
        raise ValueError(f"Store has no identifiable siteNumber: {store}")

    addr = store.get("address") or {}
    line1 = addr.get("line1") or ""
    city = addr.get("city") or ""
    state = addr.get("stateAbbreviation") or addr.get("state") or ""
    zip_code = addr.get("postalCode") or ""

    # Lat / Lng resolution
    lat = store.get("latitude")
    if lat is None and isinstance(store.get("geo"), dict):
        lat = store["geo"].get("latitude")
    
    lng = store.get("longitude")
    if lng is None and isinstance(store.get("geo"), dict):
        lng = store["geo"].get("longitude")

    try:
        lat = float(lat) if lat is not None else None
    except (ValueError, TypeError):
        lat = None

    try:
        lng = float(lng) if lng is not None else None
    except (ValueError, TypeError):
        lng = None

    # Canonical facility URL on extraspace.com
    state_slug = (store.get("stateAbbreviation") or state or "").lower()
    city_slug = re.sub(r"[^a-z0-9]+", "_", city.lower()).strip("_")
    canonical_url = (f"https://www.extraspace.com/storage/facilities/us/{state_slug}/{city_slug}/{resolved_site_number}/"
                     if state_slug and city_slug else f"https://www.extraspace.com/storage/facilities/us/{resolved_site_number}/")

    # Parse and validate units
    units = []
    for raw_u in raw_units:
        u = parse_unit(raw_u, site_number=resolved_site_number)
        if u["price"] is not None:
            units.append(u)

    # Construct schema matching enriched_locations.json
    return {
        "brand": "extraspace",
        "store_id": f"exr_{resolved_site_number}",
        "site_number": resolved_site_number,
        "name": store.get("name") or store.get("displayName") or f"Extra Space #{resolved_site_number}",
        "address": line1,
        "city": city,
        "state": state.upper(),
        "zip": zip_code,
        "phone": _clean_phone(store.get("phone")),
        "lat": lat,
        "lng": lng,
        "url": canonical_url,
        "units": units,
        "rating": float(store["rating"]) if store.get("rating") is not None else None,
        "reviews": int(store["reviewCount"]) if store.get("reviewCount") is not None else None
    }
