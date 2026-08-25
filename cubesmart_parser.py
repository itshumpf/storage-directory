"""
cubesmart_parser.py — Turn a CubeSmart facility page HTML into the FindStorage normalized schema.

Designed for FindStorage.pages.dev:
- Traverses Schema.org JSON-LD (@graph) for store metadata (SelfStorage).
- Parses li.csStorageSizeDimension containers for online/web rate, in-store street rate, promos, and features.
- Normalizes US longitude coordinates to standard negative decimal degrees.
- Fails loudly if essential metadata is missing.
- Maps all store & unit fields to match enriched_locations.json exactly.
"""
from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup


def _clean_phone(phone_str: Optional[str]) -> str:
    """Format phone string into standard XXX-XXX-XXXX format."""
    if not phone_str:
        return ""
    digits = re.sub(r"\D", "", str(phone_str))
    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
    return phone_str.strip()


def _clean_size(size_str: str) -> str:
    """Normalize size to standard format (e.g. 5'x5'x8' -> 5x5, 10' x 10' -> 10x10)."""
    if not size_str:
        return "Other"
    
    cleaned = size_str.replace("'", "").replace("’", "").replace(" ", "").replace("X", "x").strip()
    cleaned = cleaned.replace("*", "")
    parts = cleaned.split("x")
    if len(parts) >= 2:
        w, dp = parts[0], parts[1]
        try:
            w_num = float(w)
            dp_num = float(dp)
            w_str = str(int(w_num)) if w_num == int(w_num) else str(w_num)
            dp_str = str(int(dp_num)) if dp_num == int(dp_num) else str(dp_num)
            return f"{w_str}x{dp_str}"
        except ValueError:
            return f"{w}x{dp}"
    return cleaned or "Other"


def _clean_price(price_val: Any) -> Optional[int]:
    """Parse numeric price string into rounded integer (e.g. '$57.60' -> 58, '$96.00' -> 96)."""
    if price_val is None:
        return None
    if isinstance(price_val, (int, float)):
        return int(round(price_val)) if price_val > 0 else None
    
    cleaned = re.sub(r"[^\d.]", "", str(price_val))
    try:
        val = float(cleaned)
        return int(round(val)) if val > 0 else None
    except (ValueError, TypeError):
        return None


def parse_facility_html(html_text: str, facility_url: str, site_number: Optional[str] = None) -> dict:
    """Parse CubeSmart facility HTML into a normalized FindStorage store record."""
    if not html_text or not isinstance(html_text, str):
        raise ValueError("HTML content must be a non-empty string")

    soup = BeautifulSoup(html_text, "html.parser")
    
    # 1. Extract Schema.org JSON-LD blocks (traversing @graph)
    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    if not json_ld_scripts:
        raise KeyError(f"No application/ld+json scripts found in facility page: {facility_url}")

    store_meta = None
    product_items = []

    for script in json_ld_scripts:
        try:
            content = script.string
            if not content:
                continue
            data = json.loads(content)
            
            # Unpack list or @graph
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("@graph", [data])

            for item in items:
                item_type = item.get("@type")
                if item_type == "SelfStorage":
                    store_meta = item
                elif item_type == "ItemList" and "itemListElement" in item:
                    product_items.extend(item.get("itemListElement", []))
        except json.JSONDecodeError:
            continue

    if not store_meta:
        raise KeyError(f"Missing Schema.org 'SelfStorage' object in JSON-LD for {facility_url}")

    # Extract store attributes
    resolved_site = site_number
    if not resolved_site:
        m = re.search(r"/(\d{3,6})\.html", facility_url)
        resolved_site = m.group(1) if m else ""
    
    if not resolved_site:
        raise ValueError(f"Could not determine site_number for facility URL: {facility_url}")

    store_name = store_meta.get("name") or f"CubeSmart #{resolved_site}"
    phone = _clean_phone(store_meta.get("telephone"))
    
    addr_obj = store_meta.get("address") or {}
    street_addr = addr_obj.get("streetAddress") or ""
    city = addr_obj.get("addressLocality") or ""
    state = (addr_obj.get("addressRegion") or "").upper()
    zip_code = addr_obj.get("postalCode") or ""

    geo_obj = store_meta.get("geo") or {}
    lat = None
    lng = None
    try:
        if geo_obj.get("latitude") is not None:
            lat = float(geo_obj["latitude"])
        if geo_obj.get("longitude") is not None:
            lng_val = float(geo_obj["longitude"])
            # Normalize US Western longitudes (e.g. 119.261 -> -119.261)
            if lng_val > 0 and (65.0 <= lng_val <= 170.0):
                lng_val = -lng_val
            lng = lng_val
    except (ValueError, TypeError):
        pass

    rating = None
    reviews = None
    agg_rating = store_meta.get("aggregateRating") or {}
    try:
        if agg_rating.get("ratingValue") is not None:
            rating = float(agg_rating["ratingValue"])
        if agg_rating.get("ratingCount") is not None:
            reviews = int(agg_rating["ratingCount"])
    except (ValueError, TypeError):
        pass

    # 2. Extract Units & Pricing from DOM (li.csStorageSizeDimension)
    units = []
    unit_lis = soup.find_all("li", class_=re.compile(r"csStorageSizeDimension", re.I))

    for li in unit_lis:
        sku_guid = li.get("id") or ""
        sku = f"cube_{sku_guid}" if sku_guid else f"cube_{resolved_site}_{len(units)+1}"
        
        # Prices
        listing_div = li.find(class_=re.compile(r"csUnitFacilityListing", re.I))
        
        # Online / Web price
        web_price = None
        if listing_div and listing_div.get("data-unitprice"):
            web_price = _clean_price(listing_div.get("data-unitprice"))
        
        if web_price is None:
            disc_span = li.find(class_=re.compile(r"ptDiscountPriceSpan", re.I))
            if disc_span:
                web_price = _clean_price(disc_span.get_text())

        # In-store / Street price
        street_price = None
        orig_span = li.find(class_=re.compile(r"ptOriginalPriceSpan", re.I)) or li.find(["s", "strike"])
        if orig_span:
            street_price = _clean_price(orig_span.get_text())
        
        if street_price is None:
            street_price = web_price

        if web_price is None:
            continue

        # Size extraction: look for size text e.g. "5'x5'*" or "10'x10'"
        size_tag = li.find(class_=re.compile(r"csUnitDataSize|unit-size|size", re.I))
        size_raw = size_tag.get_text() if size_tag else ""
        if not size_raw:
            m_size = re.search(r"(\d+(?:\.\d+)?['’]?\s*[xX]\s*\d+(?:\.\d+)?(?:['’]?\s*[xX]\s*\d+(?:\.\d+)?)?)", li.get_text(" ", strip=True))
            size_raw = m_size.group(1) if m_size else "Other"
        norm_size = _clean_size(size_raw)

        # Features & attributes
        feats = []
        full_text = li.get_text(" ", strip=True)
        if re.search(r"climate\s*controlled?", full_text, re.I):
            feats.append("Climate Controlled")
        if re.search(r"1st\s*floor|ground\s*floor", full_text, re.I):
            feats.append("1st Floor Access")
        elif re.search(r"elevator", full_text, re.I):
            feats.append("Elevator Access")
        if re.search(r"drive[- ]?up", full_text, re.I):
            feats.append("Drive-up Access")
        if re.search(r"indoor", full_text, re.I):
            feats.append("Indoor")
        attrs = ", ".join(feats)

        # Promotions
        promo_banner = li.find(class_=re.compile(r"promotions-text|promotions-banner", re.I))
        promo_text = promo_banner.get_text(" ", strip=True) if promo_banner else ""
        
        promos = []
        if re.search(r"\d+%\s*off", promo_text, re.I):
            m_pct = re.search(r"(\d+%\s*off)", promo_text, re.I)
            if m_pct:
                promos.append(m_pct.group(1).title())
        if re.search(r"(?:1st|first)\s*month\s*free", promo_text, re.I):
            promos.append("First Month Free")
        elif re.search(r"(?:2nd|second)\s*month\s*free", promo_text, re.I):
            promos.append("2nd Month Free")

        promo = promos[0] if promos else ""
        promo2 = promos[1] if len(promos) > 1 else ""

        units.append({
            "size": norm_size,
            "price": web_price,
            "street_price": street_price,
            "available": True,
            "count": 1,
            "promo": promo,
            "promo2": promo2,
            "sku": sku,
            "attrs": attrs,
            "rates": {
                "web": web_price,
                "street": street_price
            }
        })

    # Schema output strictly aligned with FindStorage.pages.dev / enriched_locations.json
    return {
        "brand": "cubesmart",
        "store_id": f"cube_{resolved_site}",
        "site_number": resolved_site,
        "name": store_name,
        "address": street_addr,
        "city": city,
        "state": state,
        "zip": zip_code,
        "phone": phone,
        "lat": lat,
        "lng": lng,
        "url": facility_url,
        "units": units,
        "rating": rating,
        "reviews": reviews
    }
