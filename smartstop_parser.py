"""Parse SmartStop's XML sitemap and server-rendered Schema.org facility offers."""
from __future__ import annotations

import json
import re
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree

from bs4 import BeautifulSoup


BASE = "https://smartstopselfstorage.com"
BRAND = "smartstop"
US_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY", "DC",
}
FACILITY_PATH = re.compile(r"^/find-storage/(?P<state>[a-z]{2})/(?P<city>[^/]+)/(?P<slug>[^/]+)/?$", re.I)
SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)\s*['’]?\s*[x×]\s*(\d+(?:\.\d+)?)", re.I)


def _number(value):
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _clean_dimension(value: str) -> str:
    n = float(value)
    return str(int(n)) if n.is_integer() else str(n)


def _phone(value) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits) == 10 else str(value or "").strip()


def parse_sitemap_xml(xml_text: str) -> list[dict]:
    if not xml_text:
        raise ValueError("SmartStop sitemap must be non-empty")
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        raise ValueError("SmartStop sitemap is not valid XML") from exc
    out, seen = [], set()
    for node in root.iter():
        if not node.tag.endswith("loc") or not node.text:
            continue
        url = node.text.strip().replace("http://", "https://").rstrip("/")
        parsed = urlparse(url)
        match = FACILITY_PATH.match(parsed.path)
        if parsed.netloc.lower() != "smartstopselfstorage.com" or not match:
            continue
        state = match.group("state").upper()
        if state not in US_CODES or url in seen:
            continue
        seen.add(url)
        out.append({"brand": BRAND, "url": url, "state": state,
                    "city": unquote(match.group("city")).replace("-", " ").title()})
    return sorted(out, key=lambda r: r["url"])


def _self_storage_json(html_text: str) -> dict:
    soup = BeautifulSoup(html_text, "html.parser")
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            value = json.loads(script.string or script.get_text())
        except (json.JSONDecodeError, TypeError):
            continue
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            types = candidate.get("@type") if isinstance(candidate, dict) else None
            if isinstance(candidate, dict) and (
                types == "SelfStorage" or isinstance(types, list) and "SelfStorage" in types
            ):
                return candidate
    raise KeyError("Missing Schema.org SelfStorage data")


def parse_facility_html(html_text: str, catalog_store: dict) -> dict:
    if not html_text:
        raise ValueError("Facility HTML must be non-empty")
    if catalog_store.get("brand") != BRAND or not catalog_store.get("url"):
        raise ValueError("catalog_store must come from the SmartStop sitemap")
    item = _self_storage_json(html_text)
    canonical = str(item.get("url") or "").rstrip("/")
    asked = str(catalog_store["url"]).rstrip("/")
    if canonical and canonical != asked:
        raise LookupError(f"page identifies itself as {canonical}, not {asked}")
    address = item.get("address") or {}
    if str(address.get("addressCountry") or "US").upper() not in ("US", "USA"):
        raise ValueError("facility is not in the United States")

    identity = re.search(r"facility-(\d+)", str(item.get("@id") or ""), re.I)
    if not identity:
        identity = re.search(r"store(\d+)@", str(item.get("email") or ""), re.I)
    offers = item.get("makesOffer") or []
    offers = offers if isinstance(offers, list) else [offers]
    if not identity:
        for offer in offers:
            sku = str((offer.get("itemOffered") or {}).get("sku") or "") if isinstance(offer, dict) else ""
            identity = re.match(r"(\d+)_", sku)
            if identity:
                break
    if not identity:
        raise KeyError("SmartStop facility number is missing")
    site = identity.group(1)

    units = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        product = offer.get("itemOffered") or {}
        price = _number(offer.get("price"))
        sku = str(product.get("sku") or "").strip()
        text = " ".join(str(product.get(k) or "") for k in ("name", "description"))
        dimensions = SIZE_RE.search(text)
        if price is None or not sku or not dimensions:
            continue
        width, depth = map(float, dimensions.groups())
        availability = str(offer.get("availability") or "").lower()
        available = not availability or availability.endswith("instock")
        description = str(product.get("description") or "")
        attrs = description.split(" - ", 1)[1].strip() if " - " in description else ""
        units.append({
            "size": f"{_clean_dimension(dimensions.group(1))}x{_clean_dimension(dimensions.group(2))}",
            "price": price, "street_price": None, "available": available,
            # Schema.org says whether the offer exists, not how many physical rooms remain.
            "count": 1 if available else 0, "promo": "", "promo2": "",
            "sku": f"smartstop_{sku}", "attrs": attrs, "sqft": width * depth,
            "width": width, "depth": depth, "rates": {"web": price},
        })
    units.sort(key=lambda u: (u["sqft"], u["size"], u["price"], u["sku"]))

    rating = item.get("aggregateRating") or {}
    geo = item.get("geo") or {}
    return {
        "brand": BRAND, "store_id": f"smartstop_{site}", "site_number": site,
        "name": str(item.get("name") or f"SmartStop Self Storage #{site}"),
        "address": str(address.get("streetAddress") or ""),
        "city": str(address.get("addressLocality") or catalog_store.get("city") or ""),
        "state": str(address.get("addressRegion") or catalog_store.get("state") or "").upper(),
        "zip": str(address.get("postalCode") or ""), "phone": _phone(item.get("telephone")),
        "lat": _number(geo.get("latitude")), "lng": _number(geo.get("longitude")),
        "url": asked, "rating": _number(rating.get("ratingValue")),
        "reviews": int(rating["reviewCount"]) if str(rating.get("reviewCount", "")).isdigit() else None,
        "inventory_semantics": "advertised_offers", "units": units,
    }
