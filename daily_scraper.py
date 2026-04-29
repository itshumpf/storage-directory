"""
Storage Directory — Daily Scraper
Runs every morning via GitHub Actions
Outputs: enriched_locations.json
"""

import requests
import json
import re
import time
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE = "https://www.publicstorage.com"
PRICING_API = BASE + "/on/demandware.store/Sites-publicstorage-Site/default/AP-GetSoostonePromo?sites={}"
SITEMAP_STATE_INDEX = BASE + "/site-map-states"
SEARCH_URL = BASE + "/self-storage-search?location={}"
BATCH_SIZE = 20
DELAY = 0.4
OUTPUT_FILE = "enriched_locations.json"

# Zip codes for gap-filling sweep
ZIP_CODES = [
    "35203","35401","36104","99501","99701","85001","85201","85701","86001",
    "72201","72701","90001","90210","91601","92101","92501","93101","93701",
    "94101","94301","94601","95101","95401","95901","96001","80201","80901",
    "81001","81501","06101","06501","06901","19801","20601","21201","21701",
    "32201","32801","33101","33601","34101","30301","30901","31401","96801",
    "83201","83701","60601","61101","61701","62201","46201","47201","50301",
    "51001","52401","66101","66501","67101","67501","40201","41101","42001",
    "70101","70801","71101","04101","20901","21001","01101","02101","02601",
    "48201","49001","55101","55801","56001","38701","39201","63101","64101",
    "65201","59101","68501","89101","89501","03101","07101","08101","87101",
    "87501","10001","11001","12201","13201","14201","27101","27601","28201",
    "28601","58101","43201","44101","45201","46001","73101","74101","97201",
    "97701","15201","16101","17101","18101","19101","02901","29101","29601",
    "57101","37201","38101","75201","76101","77001","78201","79101","84101",
    "84401","05401","22201","23201","24001","98101","98501","99201","25301",
    "53201","54701","82001","20001","66213","66217","64108","63101","77942",
]
ZIP_CODES = list(dict.fromkeys(ZIP_CODES))


def get_state_sitemap_urls():
    resp = requests.get(SITEMAP_STATE_INDEX, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        if "site-map-states-" in a["href"]:
            href = a["href"]
            urls.append(href if href.startswith("http") else BASE + href)
    return list(set(urls))


def parse_stores_from_html(html):
    stores = []
    soup = BeautifulSoup(html, "html.parser")
    for inp in soup.find_all("input", class_="googleMapMarkerData"):
        try:
            data = json.loads(inp.get("value", ""))
            content = data.get("content", {})
            title = data.get("title", "")
            m = re.match(r"^(\d{4,6})\s*-", title)
            stores.append({
                "store_id": str(data.get("storeID", "")),
                "site_number": m.group(1) if m else None,
                "address": content.get("storeAddress", ""),
                "city": content.get("city", ""),
                "state": content.get("stateCode", ""),
                "zip": content.get("postalCode", ""),
                "phone": content.get("storePhone", ""),
                "lat": data.get("mlat"),
                "lng": data.get("mlng"),
                "url": BASE + content.get("plpLink", ""),
                "units": []
            })
        except Exception:
            pass
    return stores


def fetch_pricing_batch(store_ids):
    joined = "%2C".join(store_ids)
    try:
        resp = requests.get(PRICING_API.format(joined), headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = {}
        for store_data in data.get("promoInfoArr", []):
            sid = str(store_data.get("storeID", ""))
            units = []
            for unit in store_data.get("info", []):
                units.append({
                    "size": unit.get("name", ""),
                    "price": unit.get("saleprice"),
                    "available": unit.get("availability", False),
                    "count": unit.get("count", 0),
                    "promo": unit.get("promotionName", "") or "",
                    "promo2": unit.get("promotionName2", "") or "",
                })
            results[sid] = units
        return results
    except Exception as e:
        print(f"  Pricing error: {e}")
        return {}


def main():
    print("=" * 60)
    print("STORAGE DIRECTORY — DAILY SCRAPER")
    print("=" * 60)

    # ── Phase 1: Sitemap scrape ──────────────────────────────
    print("\n[1/4] Fetching state sitemaps...")
    state_urls = get_state_sitemap_urls()
    print(f"      Found {len(state_urls)} states")

    all_stores = {}
    for i, url in enumerate(sorted(state_urls)):
        state_name = url.split("site-map-states-")[-1].replace("-", " ").title()
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            stores = parse_stores_from_html(resp.text)
            new = 0
            for s in stores:
                if s["store_id"] and s["store_id"] not in all_stores:
                    all_stores[s["store_id"]] = s
                    new += 1
            print(f"      [{i+1}/{len(state_urls)}] {state_name}: {new} stores")
        except Exception as e:
            print(f"      [{i+1}/{len(state_urls)}] {state_name}: ERROR {e}")
        time.sleep(DELAY)

    print(f"\n      Sitemap total: {len(all_stores)} stores")

    # ── Phase 2: Zip code sweep for missing stores ───────────
    print(f"\n[2/4] Sweeping {len(ZIP_CODES)} zip codes for missing stores...")
    new_found = 0
    for i, zipcode in enumerate(ZIP_CODES):
        try:
            resp = requests.get(SEARCH_URL.format(zipcode), headers=HEADERS, timeout=15)
            stores = parse_stores_from_html(resp.text)
            for s in stores:
                if s["store_id"] and s["store_id"] not in all_stores:
                    all_stores[s["store_id"]] = s
                    new_found += 1
                    print(f"      NEW: Site#{s.get('site_number','?')} {s['address']}, {s['city']}, {s['state']}")
        except Exception as e:
            print(f"      ZIP {zipcode}: ERROR {e}")
        if i % 20 == 0:
            print(f"      [{i+1}/{len(ZIP_CODES)}] scanned... ({new_found} new so far)")
        time.sleep(DELAY)

    print(f"\n      Total stores after sweep: {len(all_stores)}")

    # ── Phase 3: Fetch pricing ───────────────────────────────
    print(f"\n[3/4] Fetching unit pricing...")
    store_list = list(all_stores.values())
    store_ids = [s["store_id"] for s in store_list]
    batches = [store_ids[i:i+BATCH_SIZE] for i in range(0, len(store_ids), BATCH_SIZE)]
    pricing_map = {}

    for i, batch in enumerate(batches):
        result = fetch_pricing_batch(batch)
        pricing_map.update(result)
        if i % 20 == 0:
            print(f"      Batch {i+1}/{len(batches)}...")
        time.sleep(DELAY)

    for s in store_list:
        s["units"] = pricing_map.get(s["store_id"], [])

    priced = sum(1 for s in store_list if s["units"])
    print(f"      Stores with pricing: {priced}/{len(store_list)}")

    # ── Phase 4: Save ────────────────────────────────────────
    print(f"\n[4/4] Saving {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w") as f:
        json.dump(store_list, f)

    print(f"\n✅ Done! {len(store_list)} stores saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
