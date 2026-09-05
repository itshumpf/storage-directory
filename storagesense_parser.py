"""Parse Storage Sense's public catalog and facility pages.

Storage Sense exposes two complementary public surfaces:

* ``/locations/`` embeds a Candee JSON catalog containing every facility's
  stable property id, address, coordinates, features, and canonical URL.
* Each facility page emits Schema.org ItemList JSON-LD containing stable unit
  SKUs and recurring advertised rates.  The rendered cards add current
  availability, explicit inventory counts, and promo copy.

The parser joins those two surfaces deliberately.  A card's position is never
used as a SKU: JSON-LD is the identity source, and a schema/card disagreement
raises instead of silently creating a false rate-change history.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable

from bs4 import BeautifulSoup


BRAND = "storagesense"


def _money(value: Any) -> int | None:
    if value is None:
        return None
    try:
        number = float(re.sub(r"[^0-9.]", "", str(value)))
    except ValueError:
        return None
    return int(round(number)) if number > 0 else None


def _phone(value: Any) -> str:
    raw = str(value or "")
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return raw.strip()


def _json_after_assignment(script_text: str, variable: str) -> dict:
    """Return JSON assigned to a JavaScript variable without regex-brace bugs."""
    marker = f"var {variable}"
    start = script_text.find(marker)
    if start < 0:
        raise KeyError(f"Missing JavaScript variable {variable!r}")
    equals = script_text.find("=", start)
    if equals < 0:
        raise ValueError(f"Malformed assignment for {variable!r}")
    payload = script_text[equals + 1:].lstrip()
    value, _ = json.JSONDecoder().raw_decode(payload)
    if not isinstance(value, dict):
        raise ValueError(f"{variable!r} did not contain a JSON object")
    return value


def parse_catalog_html(html_text: str) -> list[dict]:
    """Extract the public Candee facility catalog from ``/locations/``."""
    if not html_text:
        raise ValueError("Catalog HTML must be non-empty")
    soup = BeautifulSoup(html_text, "html.parser")
    variables = None
    for script in soup.find_all("script"):
        text = script.string or script.get_text() or ""
        if "var candee_js_variables" in text and '"facilities"' in text:
            variables = _json_after_assignment(text, "candee_js_variables")
            break
    if variables is None:
        raise KeyError("Could not find candee_js_variables.facilities in catalog page")

    facilities = variables.get("facilities")
    if not isinstance(facilities, list):
        raise KeyError("candee_js_variables.facilities is not a list")

    out: list[dict] = []
    seen: set[str] = set()
    for raw in facilities:
        if not isinstance(raw, dict):
            continue
        facility_id = str(raw.get("facility_id") or raw.get("prop_id") or "")
        url = str(raw.get("permalink") or raw.get("custom_website_url") or "")
        if not facility_id or not url:
            raise ValueError(f"Catalog facility is missing id or canonical URL: {raw!r}")
        if facility_id in seen:
            raise ValueError(f"Duplicate Storage Sense facility id {facility_id}")
        seen.add(facility_id)
        try:
            lat = float(raw["lat"])
            lng = float(raw["lng"])
        except (KeyError, TypeError, ValueError):
            lat = lng = None
        out.append({
            "brand": BRAND,
            "store_id": f"sense_{facility_id}",
            "site_number": facility_id,
            "name": str(raw.get("facility_name") or f"Storage Sense #{facility_id}"),
            "address": str(raw.get("facility_address") or ""),
            "city": str(raw.get("facility_city") or ""),
            "state": str(raw.get("facility_region") or "").upper(),
            "zip": str(raw.get("facility_zipcode") or ""),
            "phone": _phone(raw.get("facility_phone")),
            "lat": lat,
            "lng": lng,
            "url": url.replace("\\/", "/"),
            "facility_features": [x.get("text", "") for x in raw.get("facility_features", [])
                                  if isinstance(x, dict) and x.get("text")],
        })
    if not out:
        raise ValueError("Catalog parsed successfully but contained no facilities")
    return out


def _item_list(soup: BeautifulSoup) -> list[dict]:
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or script.get_text())
        except (json.JSONDecodeError, TypeError):
            continue
        candidates: Iterable[Any]
        if isinstance(data, dict) and data.get("@type") == "ItemList":
            candidates = [data]
        elif isinstance(data, dict) and isinstance(data.get("@graph"), list):
            candidates = data["@graph"]
        elif isinstance(data, list):
            candidates = data
        else:
            candidates = []
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("@type") == "ItemList":
                rows = candidate.get("itemListElement")
                if isinstance(rows, list):
                    return [x.get("item") for x in rows if isinstance(x, dict) and isinstance(x.get("item"), dict)]
    raise KeyError("Missing Schema.org ItemList of storage units")


def _card_rows(soup: BeautifulSoup) -> list[dict]:
    cards = soup.select(".unitsTable")
    if not cards:
        raise KeyError("Missing rendered .unitsTable unit cards")
    out: list[dict] = []
    for card in cards:
        name_node = card.select_one(".unitName")
        if name_node is None:
            raise ValueError("Unit card has no .unitName")
        size_node = card.select_one(".unitSize")
        name = name_node.get_text(" ", strip=True)
        if size_node:
            name = name.replace(size_node.get_text(" ", strip=True), "").strip()
        price_node = card.select_one(".currentUnit-price")
        price = _money(price_node.get_text(" ", strip=True)) if price_node else None
        action = card.get_text(" ", strip=True).upper()
        available = "CHOOSE UNIT" in action and price is not None
        quantity = 0
        availability_node = card.select_one("[class*='available-']")
        if availability_node:
            match = re.search(r"available-(\d+)", " ".join(availability_node.get("class", [])))
            if match and match.group(1) != "999":
                quantity = int(match.group(1))
        features = [x.get("data-value", "").strip() for x in card.select(".unitFeatureItem[data-value]")]
        promo_node = card.select_one(".discountText")
        promo = promo_node.get_text(" ", strip=True) if promo_node else ""
        street_node = card.select_one(".strikeThrough-price")
        out.append({
            "name": name,
            "price": price,
            "street_price": _money(street_node.get_text(" ", strip=True)) if street_node else price,
            "available": available,
            "count": quantity,
            "features": [x for x in features if x],
            "promo": promo,
        })
    return out


def parse_facility_html(html_text: str, catalog_store: dict) -> dict:
    """Map one public facility page to the normalized FindStorage schema."""
    if not html_text:
        raise ValueError("Facility HTML must be non-empty")
    if catalog_store.get("brand") != BRAND or not catalog_store.get("site_number"):
        raise ValueError("catalog_store must be a Storage Sense record from parse_catalog_html")
    soup = BeautifulSoup(html_text, "html.parser")
    products = _item_list(soup)
    cards = _card_rows(soup)
    if len(products) != len(cards):
        raise ValueError(f"JSON-LD/card unit count disagreement: {len(products)} != {len(cards)}")

    # A unit name is a DESCRIPTION, not a key.
    #
    # "Medium 10x10 Drive Up" is size plus features, and one facility can list
    # several distinct SKUs that answer to it — different buildings, floors or
    # price tiers. Treating the name as unique rejected 4 of the first 50
    # facilities on 2026-09-02 ('Small 5x5 Lockers', 'Medium 10x10 Climate
    # Control Ground Level', and two more), which is a rate of about 8%: normal
    # site behaviour, not a defect.
    #
    # Names are therefore grouped, and a group's cards are paired with its
    # products in document order. Both lists are rendered from one underlying
    # collection by one template, so their order agrees. Note this is a much
    # weaker assumption than the positional SKUs rejected elsewhere in this
    # repo: the SKU still comes from the JSON-LD, so it stays stable across
    # runs no matter how the page is ordered. Only the price-to-SKU binding
    # *within a single same-named group on a single page* rests on order.
    #
    # Where that binding could actually change a number — the group's cards
    # disagree on price, street price or promo — the units are flagged
    # `name_ambiguous`, so a change-log row can be discounted rather than
    # silently trusted. Where the group's cards are identical the pairing is
    # immaterial and nothing is flagged.
    products_by_name: dict[str, list[dict]] = {}
    for product in products:
        product_name = str(product.get("name") or "").strip()
        if not product_name:
            raise ValueError("JSON-LD unit is missing a name")
        products_by_name.setdefault(product_name, []).append(product)

    cards_by_name: dict[str, list[dict]] = {}
    for card in cards:
        cards_by_name.setdefault(card["name"], []).append(card)

    for group_name, group in cards_by_name.items():
        available = products_by_name.get(group_name, [])
        if len(available) != len(group):
            raise ValueError(
                f"Rendered cards and JSON-LD disagree for {group_name!r}: "
                f"{len(group)} cards, {len(available)} JSON-LD units")

    ambiguous_names = {
        group_name for group_name, group in cards_by_name.items()
        if len(group) > 1
        and len({(c["price"], c["street_price"], c["promo"]) for c in group}) > 1
    }

    taken: dict[str, int] = {}
    units: list[dict] = []
    for card in cards:
        product_name = card["name"]
        position = taken.get(product_name, 0)
        taken[product_name] = position + 1
        product = products_by_name[product_name][position]
        sku = str(product.get("sku") or "")
        if not sku:
            raise ValueError(f"Unit {product_name!r} has no public JSON-LD SKU")
        properties = product.get("additionalProperty") or []
        sqft = None
        size_class = ""
        json_features: list[str] = []
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            label, value = prop.get("name"), str(prop.get("value") or "")
            if label == "Unit Size":
                match = re.search(r"(\d+(?:\.\d+)?)", value)
                sqft = float(match.group(1)) if match else None
            elif label == "Feature" and value:
                json_features.append(value)
        if json_features and json_features[-1] in {"Small", "Medium", "Large"}:
            size_class = json_features[-1]
        attrs = card["features"] or json_features
        dimensions = re.search(r"(\d+(?:\.\d+)?)\s*[xX]\s*(\d+(?:\.\d+)?)", product_name)
        width = float(dimensions.group(1)) if dimensions else None
        depth = float(dimensions.group(2)) if dimensions else None
        size = (f"{dimensions.group(1)}x{dimensions.group(2)}" if dimensions else product_name)
        units.append({
            "size": size,
            "price": card["price"] if card["available"] else None,
            "street_price": card["street_price"] if card["available"] else None,
            "available": card["available"],
            "count": card["count"],  # zero means public page gives no exact count
            "promo": card["promo"],
            "promo2": "",
            "sku": f"sense_{sku}",
            "attrs": ", ".join(attrs),
            "sqft": sqft,
            "width": width,
            "depth": depth,
            "size_class": size_class,
            "name_ambiguous": product_name in ambiguous_names,
            "rates": {"web": card["price"] if card["available"] else None,
                      "street": card["street_price"] if card["available"] else None},
        })
    unmatched = sorted(name for name, group in products_by_name.items()
                       if taken.get(name, 0) != len(group))
    if unmatched:
        raise ValueError(f"JSON-LD units missing rendered cards: {unmatched}")
    return {**{k: catalog_store[k] for k in ("brand", "store_id", "site_number", "name", "address", "city", "state", "zip", "phone", "lat", "lng", "url")}, "units": units}
