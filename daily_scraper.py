"""
Storage Directory — Daily Scraper
Safety: will NEVER overwrite existing data with fewer stores
"""

import requests, json, re, time, os, sys, shutil
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
BASE = "https://www.publicstorage.com"
PRICING_API = BASE + "/on/demandware.store/Sites-publicstorage-Site/default/AP-GetSoostonePromo?sites={}"
SITEMAP_URL = BASE + "/site-map-states"
SEARCH_URL  = BASE + "/self-storage-search?location={}"
OUTPUT_FILE = "enriched_locations.json"
BACKUP_FILE = "enriched_locations_backup.json"
BATCH_SIZE  = 20
DELAY       = 0.4
MIN_STORES  = 3000   # safety floor

ZIP_CODES = list(dict.fromkeys([
    "35203","35401","36104","99501","99701","85001","85201","85301","85701","86001","86301",
    "72201","72401","72701","72901","71601","90001","90210","91001","91601","92101","92501",
    "93101","93401","93701","94101","94301","94601","94701","95101","95401","95901","96001",
    "80201","80501","80901","81001","81501","06101","06301","06501","06901","19701","19801",
    "19901","32201","32801","33101","33401","33601","33901","34101","34401","34701","30301",
    "30501","30901","31401","96801","83201","83401","83701","60101","60601","61101","61701",
    "62201","62901","46201","46901","47201","47901","50301","51001","52401","52801","66101",
    "66201","66501","66801","67101","67501","67901","40201","40901","41101","42001","42701",
    "70101","70501","70801","71101","04101","04401","04901","20601","20901","21201","21701",
    "01101","01601","02101","02301","02601","02901","48201","48701","49001","49501","49901",
    "55101","55401","55801","56001","56501","38701","39201","39601","63101","63701","64101",
    "64501","65201","65901","59101","59401","59801","68101","68501","68901","69101","89101",
    "89401","89501","89801","03101","03301","03801","07101","07401","07701","08101","08401",
    "08701","08901","87101","87301","87501","87901","88001","88301","88501","10001","10601",
    "11001","11201","11701","12201","12601","13001","13201","13601","14001","14201","14701",
    "27101","27401","27601","27901","28201","28501","28801","29001","58101","58401","58801",
    "43201","43601","44001","44101","44401","44701","45001","45201","45501","45801","73101",
    "73501","73901","74101","74501","74901","97201","97401","97701","97901","15201","15901",
    "16101","16501","16901","17101","17501","17901","18101","18401","18701","19101","19401",
    "02801","02860","29101","29401","29701","29901","57101","57401","57801","37101","37401",
    "37801","38101","38501","38901","73301","75001","75201","75501","75701","76001","76201",
    "76501","76901","77001","77201","77501","77801","78101","78401","78701","79001","79201",
    "79501","79801","84001","84201","84401","84601","05101","05301","05601","22001","22201",
    "22601","22901","23201","23601","24001","24401","98001","98101","98401","98601","98901",
    "99201","99401","25301","25701","26101","26501","26901","53001","53201","53701","54101",
    "54501","54901","82001","82301","82601","82901","20001","20003",
    # KC area
    "66213","66217","66106","66062","66061","64108","64111","64114","64131","64138",
]))


def get_sitemap_state_urls():
    r = requests.get(SITEMAP_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    return list({(a["href"] if a["href"].startswith("http") else BASE+a["href"])
                 for a in soup.find_all("a", href=True) if "site-map-states-" in a["href"]})


def parse_stores(html):
    stores = []
    for inp in BeautifulSoup(html, "html.parser").find_all("input", class_="googleMapMarkerData"):
        try:
            d = json.loads(inp.get("value",""))
            c = d.get("content",{})
            t = d.get("title","")
            m = re.match(r"^(\d{4,6})\s*-", t)
            stores.append({
                "store_id":   str(d.get("storeID","")),
                "site_number": m.group(1) if m else None,
                "address":    c.get("storeAddress",""),
                "city":       c.get("city",""),
                "state":      c.get("stateCode",""),
                "zip":        c.get("postalCode",""),
                "phone":      c.get("storePhone",""),
                "lat":        d.get("mlat"),
                "lng":        d.get("mlng"),
                "url":        BASE + c.get("plpLink",""),
                "units":      []
            })
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue
    return stores


def fetch_pricing(store_ids):
    results = {}
    batches = [store_ids[i:i+BATCH_SIZE] for i in range(0,len(store_ids),BATCH_SIZE)]
    for i, batch in enumerate(batches):
        try:
            r = requests.get(PRICING_API.format("%2C".join(batch)), headers=HEADERS, timeout=15)
            r.raise_for_status()
            for sd in r.json().get("promoInfoArr",[]):
                sid = str(sd.get("storeID",""))
                results[sid] = [{
                    "size":      u.get("name",""),
                    "price":     u.get("saleprice"),
                    "available": u.get("availability", False),
                    "count":     u.get("count", 0),
                    "promo":     u.get("promotionName","") or "",
                    "promo2":    u.get("promotionName2","") or "",
                } for u in sd.get("info",[])]
        except Exception as e:
            print(f"  Pricing batch {i+1} error: {e}")
        if i % 25 == 0:
            print(f"      Pricing batch {i+1}/{len(batches)}...")
        time.sleep(DELAY)
    return results


def main():
    print("="*60)
    print("STORAGE DIRECTORY — DAILY SCRAPER")
    print("="*60)

    # Load existing as fallback
    existing_count = 0
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                existing_count = len(json.load(f))
            print(f"\nExisting data: {existing_count} stores (safety fallback)")
        except (json.JSONDecodeError, OSError) as e:
            print(f"\nWARNING: couldn't read existing {OUTPUT_FILE}: {e}")

    # Phase 1 — Sitemap
    print("\n[1/4] Fetching state sitemaps...")
    try:
        state_urls = get_sitemap_state_urls()
        print(f"      {len(state_urls)} states found")
    except Exception as e:
        print(f"      FAILED: {e} — aborting, keeping existing data")
        sys.exit(1)

    all_stores = {}
    for i, url in enumerate(sorted(state_urls)):
        name = url.split("site-map-states-")[-1].replace("-"," ").title()
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            new_count = 0
            for a in soup.find_all("a", href=True):
                href = a["href"]
                m2 = re.search(r"/self-storage-([a-z]{2})-([a-z0-9-]+)/(\d{4,6})\.html$", href)
                if m2:
                    sid = m2.group(3)
                    if sid not in all_stores:
                        label = a.get_text(strip=True)
                        am = re.search(r"Self Storage Near (.+?) in ", label, re.IGNORECASE)
                        all_stores[sid] = {
                            "store_id": sid, "site_number": None,
                            "address": am.group(1).strip() if am else "",
                            "city": m2.group(2).replace("-"," ").title(),
                            "state": m2.group(1).upper(),
                            "zip": "", "phone": "", "lat": None, "lng": None,
                            "url": href if href.startswith("http") else BASE+href,
                            "units": []
                        }
                        new_count += 1
            print(f"      [{i+1}/{len(state_urls)}] {name}: {new_count} new ({len(all_stores)} total)")
        except Exception as e:
            print(f"      [{i+1}/{len(state_urls)}] {name}: ERROR {e}")
        time.sleep(DELAY)

    print(f"\n      After sitemaps: {len(all_stores)} stores")

    # Phase 2 — Zip sweep
    print(f"\n[2/4] Zip code sweep ({len(ZIP_CODES)} zips)...")
    new_found = 0
    for i, z in enumerate(ZIP_CODES):
        try:
            r = requests.get(SEARCH_URL.format(z), headers=HEADERS, timeout=15)
            for s in parse_stores(r.text):
                if s["store_id"] and s["store_id"] not in all_stores:
                    all_stores[s["store_id"]] = s
                    new_found += 1
                    print(f"      NEW: Site#{s.get('site_number','?')} {s['address']}, {s['city']}, {s['state']}")
        except Exception as e:
            print(f"      ZIP {z}: ERROR {e}")
        if i % 50 == 0:
            print(f"      [{i+1}/{len(ZIP_CODES)}] scanned, {new_found} new stores found")
        time.sleep(DELAY)

    print(f"\n      Total after sweep: {len(all_stores)} stores")

    # Safety checks
    if len(all_stores) < MIN_STORES:
        print(f"\n⚠️  SAFETY CHECK FAILED — only {len(all_stores)} stores found (min: {MIN_STORES})")
        print(f"   Keeping existing {existing_count} stores. Check connection and retry.")
        sys.exit(1)

    if existing_count > 0 and len(all_stores) < existing_count * 0.90:
        print(f"\n⚠️  SAFETY CHECK FAILED — {len(all_stores)} stores vs {existing_count} previously")
        print(f"   Drop >10% detected. Keeping existing data.")
        sys.exit(1)

    # Phase 3 — Pricing
    print(f"\n[3/4] Fetching pricing...")
    store_list = list(all_stores.values())
    pricing = fetch_pricing([s["store_id"] for s in store_list])
    for s in store_list:
        s["units"] = pricing.get(s["store_id"], [])
    priced = sum(1 for s in store_list if s["units"])
    print(f"      {priced}/{len(store_list)} stores have pricing")

    # Phase 4 — Save
    print(f"\n[4/4] Saving...")
    if os.path.exists(OUTPUT_FILE):
        shutil.copy(OUTPUT_FILE, BACKUP_FILE)
        print(f"      Backed up existing → {BACKUP_FILE}")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(store_list, f)

    diff = len(store_list) - existing_count
    print(f"\n✅ Done! {len(store_list)} stores saved ({'+' if diff>=0 else ''}{diff} from last run)")


if __name__ == "__main__":
    main()
