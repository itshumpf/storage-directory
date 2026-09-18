"""Parse public, server-rendered Storagely facility and unit-card HTML."""
from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup


SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*['’]?\s*[x×]\s*(\d+(?:\.\d+)?)", re.I)


def _number(value):
    match = re.search(r"-?\d+(?:,\d{3})*(?:\.\d+)?", str(value or ""))
    return float(match.group().replace(",", "")) if match else None


def _clean_dimension(value: str) -> str:
    number = float(value)
    return str(int(number)) if number.is_integer() else str(number)


def _phone(value) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else str(value or "").strip()


def _json_ld_objects(soup: BeautifulSoup):
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            value = json.loads(script.string or script.get_text())
        except (json.JSONDecodeError, TypeError):
            continue
        pending = value if isinstance(value, list) else [value]
        while pending:
            item = pending.pop(0)
            if not isinstance(item, dict):
                continue
            yield item
            graph = item.get("@graph")
            if isinstance(graph, list):
                pending.extend(graph)


def _facility_json_ld(soup: BeautifulSoup) -> dict:
    fallback = None
    for item in _json_ld_objects(soup):
        if not isinstance(item.get("address"), dict):
            continue
        types = item.get("@type") or []
        types = [types] if isinstance(types, str) else types
        lowered = {str(value).lower() for value in types}
        if lowered & {"selfstorage", "storagefacility", "localbusiness"}:
            return item
        fallback = fallback or item
    if fallback:
        return fallback
    raise KeyError("Missing facility JSON-LD address")


def _unit_container(name_node):
    node = name_node
    for _ in range(9):
        node = node.parent
        if node is None:
            break
        if (len(node.select(".unit-type-listing-name")) == 1
                and node.select_one(".widthHeight")
                and node.select_one(".actualMoPrice, .withoutDiscntprice")):
            return node
    raise ValueError("Storagely unit card container is missing")


def _dimensions(text: str):
    match = SIZE_RE.search(text)
    if match:
        return match.group(1), match.group(2)
    # Storagely inserts visible WIDTH/DEPTH labels between the numbers and x.
    values = re.findall(r"\d+(?:\.\d+)?", text)
    return (values[0], values[1]) if len(values) >= 2 else None


def _item_list_units(soup: BeautifulSoup, operator_id: str) -> list[dict]:
    """Parse the newer Storagely theme's Schema.org Product inventory."""
    units, seen = [], set()
    for value in _json_ld_objects(soup):
        types = value.get("@type") or []
        types = [types] if isinstance(types, str) else types
        if "ItemList" not in types:
            continue
        for element in value.get("itemListElement") or []:
            product = element.get("item") if isinstance(element, dict) else None
            if not isinstance(product, dict):
                continue
            product_types = product.get("@type") or []
            product_types = [product_types] if isinstance(product_types, str) else product_types
            if "Product" not in product_types:
                continue
            identity = str(product.get("sku") or "").strip()
            if not identity or identity in seen:
                continue
            name = str(product.get("name") or "").strip()
            description = str(product.get("description") or "").strip()
            dimensions = _dimensions(f"{name} {description}")
            if not dimensions:
                continue
            offers = product.get("offers") or {}
            if isinstance(offers, list):
                offers = next((offer for offer in offers if isinstance(offer, dict)), {})
            if not isinstance(offers, dict):
                offers = {}
            price = _number(offers.get("price"))
            if price is None:
                raise ValueError(f"Storagely unit {identity} has no advertised rate")
            seen.add(identity)
            width, depth = map(float, dimensions)
            availability = str(offers.get("availability") or "").lower()
            available = not availability or availability.endswith("instock")
            amenity_match = re.search(
                r"Amenities:\s*(.*?)(?:\.\s*(?:Facility|Location)\s*:|$)",
                description, re.I)
            attrs = amenity_match.group(1).strip(" .") if amenity_match else ""
            category = str(product.get("category") or name or "SelfStorageUnit").strip()
            units.append({
                "size": f"{_clean_dimension(dimensions[0])}x{_clean_dimension(dimensions[1])}",
                "price": price, "street_price": None,
                "standard_rate": None, "promo_price": None,
                "available": available, "count": 1 if available else 0,
                "promo": "", "promo2": "", "sku": f"{operator_id}_{identity}",
                "attrs": attrs, "category": category,
                "sqft": width * depth, "width": width, "depth": depth,
                "rates": {"web": price},
            })
    return units


def parse_facility_html(html_text: str, catalog_store: dict, *, operator_id: str) -> dict:
    if not html_text:
        raise ValueError("Storagely facility HTML must be non-empty")
    url = str(catalog_store.get("url") or "").rstrip("/")
    if not url or not operator_id:
        raise ValueError("Storagely parsing requires a URL and operator id")
    soup = BeautifulSoup(html_text, "html.parser")
    if not soup.select_one('[src*="storagely"], [href*="storagely"]'):
        raise ValueError("Page does not carry a Storagely asset signature")
    facility = _facility_json_ld(soup)
    address = facility.get("address") or {}
    country = str(address.get("addressCountry") or "US").upper()
    if country not in {"US", "USA", "UNITED STATES"}:
        raise ValueError("facility is not in the United States")

    units = _item_list_units(soup, operator_id)
    seen = {unit["sku"].removeprefix(f"{operator_id}_") for unit in units}
    for name_node in soup.select(".unit-type-listing-name"):
        container = _unit_container(name_node)
        size_text = container.select_one(".widthHeight").get_text(" ", strip=True)
        dimensions = _dimensions(size_text)
        identity_node = name_node.select_one(".d-none")
        identity = (identity_node.get_text(" ", strip=True) if identity_node else "").strip()
        if not dimensions or not identity or identity in seen:
            continue
        seen.add(identity)
        width, depth = map(float, dimensions)
        actual = _number(container.select_one(".actualMoPrice").get_text(" ", strip=True)
                         if container.select_one(".actualMoPrice") else None)
        standard = _number(container.select_one(".withoutDiscntprice").get_text(" ", strip=True)
                           if container.select_one(".withoutDiscntprice") else None)
        promo_node = container.select_one(".offer__content, .promoText")
        promo = promo_node.get_text(" ", strip=True) if promo_node else ""
        sold_out = "sold out" in container.get_text(" ", strip=True).lower()
        # A zero-dollar first-month promotion is not the recurring monthly rate.
        recurring = standard if standard is not None and (actual is None or actual <= 0) else actual
        if recurring is None:
            raise ValueError(f"Storagely unit {identity} has no advertised rate")
        promo_price = (actual if standard is not None and actual is not None and actual < standard
                       else None)
        type_text = " ".join(
            text.strip() for text in name_node.find_all(string=True, recursive=False) if text.strip())
        units.append({
            "size": f"{_clean_dimension(dimensions[0])}x{_clean_dimension(dimensions[1])}",
            "price": recurring, "street_price": standard,
            "standard_rate": standard, "promo_price": promo_price,
            "available": not sold_out, "count": 0 if sold_out else 1,
            "promo": promo, "promo2": "", "sku": f"{operator_id}_{identity}",
            "attrs": type_text, "category": type_text,
            "sqft": width * depth, "width": width, "depth": depth,
            "rates": {"web": recurring, "street": standard},
        })
    sold_out = not units and "join the waitlist" in soup.get_text(" ", strip=True).lower()
    if not units and not sold_out:
        raise ValueError("Storagely facility has no advertised unit cards or sold-out message")
    units.sort(key=lambda unit: (unit["sqft"], unit["size"], unit["sku"]))

    parsed = urlparse(url)
    path = parsed.path.strip("/")
    site = re.sub(r"[^a-z0-9]+", "_", path.lower()).strip("_")
    if not site:
        raise ValueError("Storagely facility URL has no stable path identity")
    geo = facility.get("geo") or {}
    title = soup.title.get_text(" ", strip=True) if soup.title else "Self Storage"
    name = str(facility.get("name") or title)
    return {
        "brand": "independent", "operator_id": operator_id,
        "store_id": f"ind_{operator_id}_{site}", "site_number": site,
        "name": name, "address": str(address.get("streetAddress") or "").strip(),
        "city": str(address.get("addressLocality") or "").strip(),
        "state": str(address.get("addressRegion") or "").strip().upper(),
        "zip": str(address.get("postalCode") or "").strip(),
        "phone": _phone(facility.get("telephone")),
        "lat": _number(geo.get("latitude")), "lng": _number(geo.get("longitude")),
        "url": url, "rating": None, "reviews": None,
        "platform": "storagely", "inventory_semantics": "advertised_offers",
        "inventory_status": "sold_out" if sold_out else "available",
        "units": units,
    }
