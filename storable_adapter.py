"""Brand-neutral parser for Storable/storEDGE facility and unit-group data.

Discovery and HTML bootstrapping remain operator-specific.  Once an operator
page yields a Storable ``facility`` object, this module maps it into the shared
FindStorage record without knowing whether the sign says StorageMart or a
single-location independent operator.
"""
from __future__ import annotations

import html
import json
import re
from typing import Any


class NotUS(LookupError):
    """A valid Storable facility outside the U.S.; excluded, not failed."""


def _find_facility_payload(value, *, allow_empty: bool = True):
    if isinstance(value, dict):
        facilities = value.get("facilities")
        if (isinstance(facilities, dict) and isinstance(facilities.get("allFacilities"), list)
                and (allow_empty or facilities["allFacilities"])):
            return value
        if isinstance(value.get("allFacilities"), list) and (allow_empty or value["allFacilities"]):
            return {"facilities": {"allFacilities": value["allFacilities"]}}
        for child in value.values():
            found = _find_facility_payload(child, allow_empty=allow_empty)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_facility_payload(child, allow_empty=allow_empty)
            if found is not None:
                return found
    return None


def _apollo_reference(value) -> str | None:
    if not isinstance(value, dict):
        return None
    if isinstance(value.get("__ref"), str):
        return value["__ref"]
    if value.get("type") == "id" and isinstance(value.get("id"), str):
        return value["id"]
    return None


def _hydrate_apollo(value, cache: dict, stack: frozenset[str] = frozenset()):
    reference = _apollo_reference(value)
    if reference:
        if reference in stack or reference not in cache:
            return {"id": reference.split(":", 1)[-1]}
        return _hydrate_apollo(cache[reference], cache, stack | {reference})
    if isinstance(value, list):
        return [_hydrate_apollo(item, cache, stack) for item in value]
    if isinstance(value, dict):
        return {key: _hydrate_apollo(child, cache, stack) for key, child in value.items()
                if key not in {"type", "generated", "typename", "__typename"}}
    return value


def _find_apollo_payload(value):
    """Turn a normalized Apollo Facility/UnitGroup cache into Storable records."""
    if isinstance(value, dict):
        facility_keys = [key for key in value if isinstance(key, str) and key.startswith("Facility:")]
        if facility_keys:
            facilities = []
            for key in facility_keys:
                raw = value.get(key)
                if not isinstance(raw, dict) or not raw.get("unitGroups"):
                    continue
                facility = _hydrate_apollo(raw, value, frozenset({key}))
                facility.setdefault("id", key.split(":", 1)[1])
                location = facility.get("location") or {}
                if not facility.get("address") and isinstance(location, dict):
                    facility["address"] = {
                        "address1": location.get("address1") or location.get("address"),
                        "address2": location.get("address2"),
                        "city": location.get("city"),
                        "state": location.get("state") or location.get("region"),
                        "postal": location.get("postal") or location.get("postalCode"),
                        "country": location.get("country") or location.get("countryCode") or "US",
                        "latitude": location.get("latitude"),
                        "longitude": location.get("longitude"),
                    }
                facilities.append(facility)
            if facilities:
                return {"facilities": {"allFacilities": facilities}}
        for child in value.values():
            found = _find_apollo_payload(child)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _find_apollo_payload(child)
            if found is not None:
                return found
    return None


def _payload_from_value(value):
    return (_find_facility_payload(value, allow_empty=False)
            or _find_apollo_payload(value))


def _collect_page_facilities(value, found: dict[str, dict]) -> None:
    """Collect facility metadata embedded in the separate Voyager page state."""
    if isinstance(value, dict):
        facility_id = value.get("facilityId")
        facility = value.get("facility")
        if isinstance(facility_id, str) and isinstance(facility, dict) and facility.get("storeNumber"):
            found.setdefault(facility_id, facility)
        for child in value.values():
            _collect_page_facilities(child, found)
    elif isinstance(value, list):
        for child in value:
            _collect_page_facilities(child, found)


def _merge_page_facilities(payload: dict, metadata: dict[str, dict]) -> dict:
    for facility in payload["facilities"]["allFacilities"]:
        extra = metadata.get(str(facility.get("id") or ""), {})
        for key, value in extra.items():
            if facility.get(key) in (None, "", []):
                facility[key] = value
    return payload


def extract_storable_data(html_text: str) -> dict:
    """Find Storable facility state in known JS assignments or JSON hydration scripts."""
    empty = None
    apollo = None
    page_facilities: dict[str, dict] = {}
    for marker in ("window.__data=", "window.__data =", "window.__INITIAL_STATE__=",
                   "window.__INITIAL_STATE__ =", "window.__APOLLO_STATE__=",
                   "window.__APOLLO_STATE__ ="):
        index = html_text.find(marker)
        if index >= 0:
            start = html_text.find("{", index + len(marker))
            if start >= 0:
                try:
                    value, _end = json.JSONDecoder().raw_decode(html_text, start)
                except json.JSONDecodeError:
                    continue
                found = _find_facility_payload(value, allow_empty=False)
                if found is not None:
                    return found
                apollo = apollo or _find_apollo_payload(value)
                _collect_page_facilities(value, page_facilities)
                empty = empty or _find_facility_payload(value)
    for match in re.finditer(r'<script[^>]+(?:type=["\']application/json["\']|id=["\']__NEXT_DATA__["\'])[^>]*>'
                             r'(.*?)</script>', html_text, re.I | re.S):
        try:
            value = json.loads(html.unescape(match.group(1)).strip())
        except (json.JSONDecodeError, TypeError):
            continue
        found = _find_facility_payload(value, allow_empty=False)
        if found is not None:
            return found
        apollo = apollo or _find_apollo_payload(value)
        _collect_page_facilities(value, page_facilities)
        empty = empty or _find_facility_payload(value)
    if apollo is not None:
        return _merge_page_facilities(apollo, page_facilities)
    if empty is not None:
        return empty
    raise KeyError("Missing Storable facilities.allFacilities payload")


def _number(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else str(value or "").strip()


def _promotion(plans: list[dict]) -> tuple[str, list[dict]]:
    live = [p for p in plans or [] if p.get("turnedOn", True) and p.get("autoApply")]
    if not live:
        return "", []
    live.sort(key=lambda p: (p.get("priority") is None, p.get("priority") or 0))
    plan = live[0]
    terms = [{"month": d.get("monthNumber"), "type": d.get("discountType"),
              "amount": _number(d.get("amount"))}
             for d in plan.get("discountPlanDiscounts") or []]
    return str(plan.get("publicDescription") or plan.get("name") or "").strip(), terms


def parse_storable_facility(
    facility: dict,
    catalog_store: dict,
    *,
    brand: str,
    store_id_prefix: str,
    sku_prefix: str | None = None,
    fallback_name: str = "Self Storage",
    expected_path: str | None = None,
    json_ld: dict | None = None,
    operator_id: str | None = None,
) -> dict:
    """Normalize one Storable facility object and its complete unit-group list."""
    if not isinstance(facility, dict):
        raise TypeError("Storable facility must be an object")
    address = facility.get("address") or {}
    country = str(address.get("country") or "US").upper()
    if country not in ("US", "USA"):
        raise NotUS(f"facility is in {country}, not the U.S.")
    facility_id = str(facility.get("id") or "").strip()
    store_number = str(facility.get("storeNumber") or "").strip()
    store = store_number or facility_id
    if not store:
        raise KeyError("Storable facility carries neither storeNumber nor facility id")

    if expected_path:
        own_paths = {str(p.get("path") or "").rstrip("/") for p in facility.get("pagePaths") or []}
        if own_paths and expected_path.rstrip("/") not in own_paths:
            raise LookupError(f"page for store {store} does not list {expected_path} among its own paths "
                              f"{sorted(own_paths)}")

    ld = json_ld or {}
    ld_address = ld.get("address") or {}
    sku_prefix = sku_prefix or store_id_prefix
    units = []
    for group in facility.get("unitGroups") or []:
        if not group.get("id"):
            continue
        available = int(group.get("availableUnitsCount") or 0)
        promo, terms = _promotion(group.get("discountPlans") or [])
        web = _number(group.get("discountedPrice"))
        managed = _number(group.get("price"))
        if web is None:
            web = managed
        width, depth = _number(group.get("width")), _number(group.get("length"))
        size = str(group.get("size") or "").strip()
        if not size and width and depth:
            size = f"{width:g}x{depth:g}"
        if not size:
            dimensions = re.search(r"(\d+(?:\.\d+)?)\s*['\"]?\s*[x×]\s*(\d+(?:\.\d+)?)",
                                   str(group.get("name") or ""), re.I)
            if dimensions:
                width, depth = float(dimensions.group(1)), float(dimensions.group(2))
                size = f"{width:g}x{depth:g}"
        amenities = ", ".join(str(a.get("name")) for a in group.get("amenities") or [] if a.get("name"))
        if not amenities:
            amenities = str(group.get("type") or "").strip()
        units.append({
            "size": size, "price": web, "street_price": managed,
            "standard_rate": _number(group.get("standardRate")),
            "promo_price": _number(group.get("promoPrice")),
            "available": available > 0, "count": available,
            "total": int(group.get("totalUnitsCount") or 0),
            "promo": promo, "promo2": "", "promo_terms": terms,
            "sku": f"{sku_prefix}_{group['id']}",
            "attrs": amenities,
            "sqft": _number(group.get("area")) or (width * depth if width and depth else None),
            "width": width, "depth": depth, "height": _number(group.get("height")),
            "category": str(group.get("categoryName") or group.get("type") or ""),
            "rates": {"web": web, "street": managed},
        })
    units.sort(key=lambda unit: (unit["sqft"] or 0, unit["size"], unit["sku"]))

    record = {
        "brand": brand, "store_id": f"{store_id_prefix}_{store}", "site_number": store,
        "name": str(facility.get("name") or f"{fallback_name} {store}"),
        "address": str(address.get("address1") or ld_address.get("streetAddress") or "").strip(),
        "city": str(address.get("city") or ld_address.get("addressLocality")
                    or catalog_store.get("city", "")).strip(),
        "state": str(address.get("state") or ld_address.get("addressRegion") or "").strip().upper(),
        "zip": str(address.get("postal") or ld_address.get("postalCode")
                   or catalog_store.get("zip", "")).strip(),
        "phone": _phone(facility.get("directPhone") or facility.get("phone") or ld.get("telephone")),
        "lat": _number(address.get("latitude")), "lng": _number(address.get("longitude")),
        "url": catalog_store.get("url", ""), "rating": _number(facility.get("rating")), "reviews": None,
        "facility_id": facility_id,
        "identity_source": "store_number" if store_number else "facility_id",
        "software_provider": str((facility.get("settings") or {}).get("softwareProvider") or ""),
        "platform": "storable", "units": units,
    }
    if operator_id:
        record["operator_id"] = operator_id
    return record
