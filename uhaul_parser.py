"""Parse U-Haul's public state sitemaps and self-storage facility pages."""
from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import unquote
from xml.etree import ElementTree

from bs4 import BeautifulSoup


BRAND = "uhaul"
FACILITY_URL_RE = re.compile(
    r"^https://www\.uhaul\.com/Locations/Self-Storage-near-"
    r"(?P<city>.+)-(?P<state>[A-Z]{2})-(?P<zip>\d{5})/(?P<entity>\d+)/?$",
    re.I,
)


def _money(value: Any) -> float | None:
    match = re.search(r"\d[\d,]*(?:\.\d+)?", str(value or ""))
    return float(match.group(0).replace(",", "")) if match else None


def _phone(value: Any) -> str:
    """E.164 "+15413880671" -> "541-388-0671", the format every other brand records."""
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return str(value or "").strip()


def parse_sitemap_xml(xml_text: str) -> list[dict]:
    """Return only direct facility pages; result/city pages are discovery noise."""
    if not xml_text:
        raise ValueError("Storage sitemap must be non-empty")
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        raise ValueError("Storage sitemap is not valid XML") from exc
    facilities: list[dict] = []
    seen: set[str] = set()
    for node in root.iter():
        if not node.tag.endswith("loc") or not node.text:
            continue
        url = node.text.strip().replace("http://", "https://")
        match = FACILITY_URL_RE.match(url)
        if not match:
            continue
        entity = match.group("entity")
        if entity in seen:
            continue
        seen.add(entity)
        facilities.append({
            "brand": BRAND,
            "store_id": f"uhaul_{entity}",
            "site_number": entity,
            "city": unquote(match.group("city")).replace("-", " "),
            "state": match.group("state").upper(),
            "zip": match.group("zip"),
            "url": url,
        })
    return facilities


def _json_ld_objects(soup: BeautifulSoup) -> list[dict]:
    out: list[dict] = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            value = json.loads(script.string or script.get_text())
        except (json.JSONDecodeError, TypeError):
            continue
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            graph = candidate.get("@graph")
            out.extend(x for x in graph if isinstance(x, dict)) if isinstance(graph, list) else out.append(candidate)
    return out


def _kinds(item: dict) -> list[str]:
    # Live pages emit one block with a bare "type" key instead of "@type"
    # (verified 2026-09-04 on facility 700026). Read both.
    kinds = item.get("@type") or item.get("type") or []
    return [kinds] if isinstance(kinds, str) else list(kinds)


def _self_storage(soup: BeautifulSoup) -> dict:
    """The facility's metadata, MERGED across every JSON-LD block that describes it.

    REWRITTEN 2026-09-04. A live facility page carries four ld+json blocks, and
    the fields are spread across them:

      * an @graph whose SelfStorage node has name + aggregateRating + reviews —
        and NO address, NO geo, NO telephone;
      * a LocalBusiness node with address, contactPoint.telephone, and the
        coordinates under areaServed.geoMidpoint (no top-level "geo" at all);
      * a BreadcrumbList;
      * a second @graph whose node is typed ["SelfStorage","LocalBusiness"]
        under a bare "type" key, with address and amenityFeature.

    The previous version returned the FIRST SelfStorage node and read address,
    geo and telephone from it. That node has none of them, so every one of the
    1,182 facilities collected on 2026-09-03 had address "", phone "", and
    lat/lng null — and the run reported success, because an empty string looks
    exactly like a facility that did not publish one. The offline fixture in
    test_uhaul.py put everything on one node, so the test passed too.

    This merges: the first non-empty value wins per field, taken from any node
    typed SelfStorage or LocalBusiness that is not a department (truck/trailer
    rental nodes nest under "department" and are skipped by not walking into
    it). It raises only if there is no SelfStorage node at all.
    """
    nodes = _json_ld_objects(soup)
    facility = [n for n in nodes if "SelfStorage" in _kinds(n)]
    if not facility:
        raise KeyError("Missing Schema.org SelfStorage metadata")
    related = facility + [n for n in nodes
                          if "LocalBusiness" in _kinds(n) and "SelfStorage" not in _kinds(n)]
    merged: dict = {}
    for node in related:
        for key, value in node.items():
            if key == "address" and isinstance(value, dict):
                # Blocks disagree on completeness (one has only streetAddress);
                # merge the parts rather than picking a block.
                target = merged.setdefault("address", {})
                if isinstance(target, dict):
                    for part, text in value.items():
                        if text not in (None, "") and target.get(part) in (None, ""):
                            target[part] = text
                continue
            if key not in merged or merged[key] in (None, "", {}, []):
                merged[key] = value
        contact = node.get("contactPoint") or {}
        if isinstance(contact, dict) and contact.get("telephone") and not merged.get("telephone"):
            merged["telephone"] = contact["telephone"]
        area = node.get("areaServed") or {}
        mid = area.get("geoMidpoint") if isinstance(area, dict) else None
        if isinstance(mid, dict) and not merged.get("geo"):
            merged["geo"] = mid
    return merged


def _address_parts(item: dict) -> tuple[str, str, str, str]:
    address = item.get("address") or {}
    if isinstance(address, str):
        return address, "", "", ""
    return (
        str(address.get("streetAddress") or "").strip(),
        str(address.get("addressLocality") or "").strip(),
        str(address.get("addressRegion") or "").strip().upper(),
        str(address.get("postalCode") or "").strip(),
    )


def _room_nodes(root: BeautifulSoup) -> list:
    nodes, seen = [], set()
    for field in root.find_all("input", attrs={"name": "RentableInventoryPk"}):
        sku = str(field.get("data-source-value") or field.get("value") or "").replace("-", "").lower()
        if not sku or sku in seen:
            continue
        card = field.find_parent("li")
        if card is None:
            raise ValueError(f"Room {sku} has no containing list item")
        seen.add(sku)
        nodes.append((sku, card))
    return nodes


def parse_facility_html(html_text: str, catalog_store: dict) -> dict:
    if not html_text:
        raise ValueError("Facility HTML must be non-empty")
    if catalog_store.get("brand") != BRAND or not catalog_store.get("site_number"):
        raise ValueError("catalog_store must come from a U-Haul storage sitemap")
    # Razor emits invalid duplicate attributes such as `Value="$124.95"`
    # followed by `value=""`. Preserve the populated server value before an
    # HTML parser correctly folds those case-insensitive names together.
    normalized = re.sub(r"(?<=\s)Value=", "data-source-value=", html_text)
    normalized = re.sub(r"(?<=\s)Id=", "data-source-id=", normalized)
    soup = BeautifulSoup(normalized, "html.parser")
    metadata = _self_storage(soup)
    page_text = soup.get_text(" ", strip=True)
    room_root = soup.find(id="roomTypes")
    if room_root is None:
        # Fully booked facilities use a separate server-rendered template. It
        # deliberately omits #roomTypes and states that no rooms are available
        # online. This is a measured zero-inventory state, not parser drift.
        sold_out_online = bool(re.search(
            r"there are no (?:storage )?rooms available online at this time", page_text, re.I))
        if not sold_out_online:
            raise KeyError("Missing #roomTypes inventory container and explicit sold-out message")
    else:
        sold_out_online = False

    address, city, state, postal = _address_parts(metadata)
    geo = metadata.get("geo") or {}
    try:
        lat = float(geo.get("latitude"))
        lng = float(geo.get("longitude"))
    except (TypeError, ValueError):
        lat = lng = None
    if lat is None:
        # Last resort: the Apple Maps link in sameAs carries "?ll=lat,lng".
        for link in metadata.get("sameAs") or []:
            found = re.search(r"[?&]ll=(-?[\d.]+),(-?[\d.]+)", str(link))
            if found:
                lat, lng = float(found.group(1)), float(found.group(2))
                break

    affiliate = "U-Haul Self-Storage Affiliate" in page_text
    units: list[dict] = []
    for sku, card in _room_nodes(room_root) if room_root is not None else []:
        size_node = card.select_one("h4 .nowrap")
        if size_node is None:
            raise ValueError(f"Room {sku} has no dimensions")
        dimensions = re.search(
            r"([\d.]+)\s*['\u2032]?\s*x\s*([\d.]+)\s*['\u2032]?"
            r"(?:\s*x\s*([\d.]+)\s*['\u2032]?)?",
            size_node.get_text(" ", strip=True), re.I,
        )
        if not dimensions:
            raise ValueError(f"Room {sku} has unparseable dimensions")
        width, depth = float(dimensions.group(1)), float(dimensions.group(2))
        height = float(dimensions.group(3)) if dimensions.group(3) else None
        price_field = card.find("input", attrs={"name": "Price"})
        count_field = card.find("input", attrs={"name": "VacantUnitsCount"})
        price = _money(price_field.get("data-source-value") or price_field.get("value")) if price_field else None
        if price is None:
            raise ValueError(f"Available room {sku} has no price")
        try:
            count_value = count_field.get("data-source-value") or count_field.get("value")
            count = int(count_value) if count_value not in (None, "") else 0
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Room {sku} has invalid vacant count") from exc
        attrs: list[str] = []
        for candidate in card.select("ul.collapse.condensed li"):
            text = candidate.get_text(" ", strip=True)
            if text and text not in attrs:
                attrs.append(text)
        card_text = card.get_text(" ", strip=True)
        promos = []
        if "Free Lock" in card_text:
            promos.append("Free Lock")
        rent_now = card.select_one(".tabs-title.rent") is not None
        reserve = card.select_one(".tabs-title.reserve") is not None
        size = f"{dimensions.group(1)}x{dimensions.group(2)}"
        units.append({
            "size": size,
            "price": price,
            # The page publishes one advertised monthly rate. Recording that same
            # number as a second "street" rate creates duplicate change events and
            # falsely implies a web discount.
            "street_price": None,
            "available": True,
            "count": count,
            "promo": "; ".join(promos),
            "promo2": "",
            "sku": f"uhaul_{sku}",
            "attrs": ", ".join(attrs),
            "sqft": width * depth,
            "width": width,
            "depth": depth,
            "height": height,
            "rent_now": rent_now,
            "reserve": reserve,
            "rates": {"web": price},
        })

    # Some live corporate pages render the intact #roomTypes container with no
    # room cards and no sold-out copy. That is a valid observation of zero
    # advertised inventory. Global parser drift is guarded at snapshot level by
    # minimum priced-facility and unit-type counts, not guessed from one store.

    entity = str(catalog_store["site_number"])
    rating = metadata.get("aggregateRating") or {}
    return {
        "brand": BRAND,
        "store_id": f"uhaul_{entity}",
        "site_number": entity,
        "name": str(metadata.get("name") or f"U-Haul #{entity}"),
        "address": address,
        "city": city or catalog_store.get("city", ""),
        "state": state or catalog_store.get("state", ""),
        "zip": postal or catalog_store.get("zip", ""),
        "phone": _phone(metadata.get("telephone")),
        "rating": _money(rating.get("ratingValue")),
        "reviews": int(rating["ratingCount"]) if str(rating.get("ratingCount", "")).isdigit() else None,
        "lat": lat,
        "lng": lng,
        "url": catalog_store["url"],
        "is_affiliate": affiliate,
        "inventory_status": "sold_out_online" if sold_out_online else ("available" if units else "no_listings"),
        "units": units,
    }
