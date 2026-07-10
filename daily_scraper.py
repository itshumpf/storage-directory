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
CATEGORY_SITEMAP = BASE + "/sitemap_1-category.xml"   # every city landing page
PRODUCT_SITEMAP  = BASE + "/sitemap_0-product.xml"    # store pages (excludes delisted/full stores)
SEARCH_URL  = BASE + "/self-storage-search?location={}"
OUTPUT_FILE = "enriched_locations.json"
BACKUP_FILE = "enriched_locations_backup.json"
BATCH_SIZE  = 20
DELAY       = 0.4
MIN_STORES  = 3300   # safety floor
ENRICH_CAP  = 400    # max store pages fetched per run to backfill missing site numbers

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


def get_city_page_urls():
    """Every city landing page from the category XML sitemap. City pages embed
    googleMapMarkerData for nearby stores INCLUDING full/delisted ones, which the
    product sitemap omits — this is the primary discovery source."""
    r = requests.get(CATEGORY_SITEMAP, headers=HEADERS, timeout=30)
    r.raise_for_status()
    urls = re.findall(r"<loc>(https://www\.publicstorage\.com/self-storage-[a-z]{2}-[a-z0-9-]+)</loc>", r.text)
    return sorted(set(urls))


def get_product_store_urls():
    """Store page URLs from the product XML sitemap (supplemental)."""
    r = requests.get(PRODUCT_SITEMAP, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return sorted(set(re.findall(
        r"<loc>(https://www\.publicstorage\.com/self-storage-([a-z]{2})-([a-z0-9-]+)/(\d{4,6})\.html)</loc>", r.text)))


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


OFFER_RE = re.compile(
    r'"price":"\$([\d,]+)(?:\s*-\s*\$([\d,]+))?"[^{]*"itemOffered":\{[^}]*?"name":"[^"]*"'
    r'[^}]*?"description":"([^"]+)"[^}]*?"sku":"([^"]+)"')
RATING_RE = re.compile(r'"aggregateRating":\{[^}]*?"ratingCount":"(\d+)","ratingValue":"([\d.]+)"')
PIPELINE_FILE = "history/pipeline.csv"

def fetch_store_rating(html_text):
    """Store's aggregate review rating from page JSON-LD: (rating, count) or None."""
    m = RATING_RE.search(html_text)
    return (float(m.group(2)), int(m.group(1))) if m else None


def update_pipeline(all_stores, candidates):
    """Maintain the coming-soon store pipeline (history/pipeline.csv).

    candidates: {store_id: (url, city, state)} — live store pages found by the
    deep probe that don't confirm as operating stores yet (usually 'coming
    soon' placeholders). Rows get first_seen on entry; when the store later
    shows up in the operating dataset, opened is stamped — turning the file
    into an openings timeline.
    """
    import csv
    today = time.strftime("%Y-%m-%d")
    rows, known = [], set()
    if os.path.exists(PIPELINE_FILE):
        with open(PIPELINE_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                known.add(row["store_id"])
                if not row["opened"] and row["store_id"] in all_stores:
                    row["opened"] = today
                    print(f"      OPENED: pipeline store {row['store_id']} ({row['city']}, {row['state']})")
                rows.append(row)
    added = 0
    for sid, (url, city, state) in candidates.items():
        if sid not in known and sid not in all_stores:
            rows.append({"store_id": sid, "url": url, "city": city, "state": state,
                         "first_seen": today, "opened": ""})
            added += 1
    os.makedirs("history", exist_ok=True)
    with open(PIPELINE_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["store_id", "url", "city", "state", "first_seen", "opened"])
        w.writeheader()
        w.writerows(rows)
    if added or rows:
        pending = sum(1 for r in rows if not r["opened"])
        print(f"      Pipeline: {added} new coming-soon stores, {pending} pending open")

def fetch_unit_attrs(html_text):
    """Parse a store page's JSON-LD offers into {sku: (attrs, price_min, price_max)}.

    Each offer carries the unit's physical attributes (climate control, floor,
    inside vs drive-up) and the advertised price RANGE — the envelope the
    revenue-management system prices within. The pricing API only exposes the
    current point price, so this is the only public source for both.
    """
    out = {}
    for lo, hi, desc, sku in OFFER_RE.findall(html_text):
        desc = desc.replace(" (Prices are not guaranteed)", "")
        attrs = desc.split(" ", 1)[1] if " " in desc else desc  # drop leading size
        pmin = int(lo.replace(",", ""))
        pmax = int(hi.replace(",", "")) if hi else pmin
        out[sku] = (attrs, pmin, pmax)
    return out


def fetch_pricing(store_ids):
    results = {}
    batches = [store_ids[i:i+BATCH_SIZE] for i in range(0,len(store_ids),BATCH_SIZE)]
    consecutive_failures = 0
    for i, batch in enumerate(batches):
        ok = False
        for attempt in range(4):
            try:
                r = requests.get(PRICING_API.format("%2C".join(batch)), headers=HEADERS, timeout=25)
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
                        "sku":       u.get("id","") or "",
                    } for u in sd.get("info",[])]
                ok = True
                break
            except Exception as e:
                if attempt == 3:
                    print(f"  Pricing batch {i+1} failed after retries: {e}")
                else:
                    time.sleep(3 * (attempt + 1) ** 2)  # 3s, 12s, 27s
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 25:
                print(f"  ABORTING pricing after {consecutive_failures} consecutive failed batches "
                      f"— endpoint appears down; keeping {len(results)} stores' pricing")
                break
        if i % 25 == 0:
            print(f"      Pricing batch {i+1}/{len(batches)}...")
        time.sleep(DELAY)
    return results


def main():
    print("="*60)
    print("STORAGE DIRECTORY — DAILY SCRAPER")
    print("="*60)

    # Load existing as fallback and carry-forward source
    existing_data = []
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                existing_data = json.load(f)
            print(f"\nExisting data: {len(existing_data)} stores (safety fallback)")
        except (json.JSONDecodeError, OSError) as e:
            print(f"\nWARNING: couldn't read existing {OUTPUT_FILE}: {e}")
    existing_count = len(existing_data)

    # Phase 1 — City pages (primary discovery; markers include full/delisted stores)
    print("\n[1/8] Fetching city pages from category sitemap...")
    try:
        city_urls = get_city_page_urls()
        print(f"      {len(city_urls)} city pages found")
    except Exception as e:
        print(f"      FAILED: {e} — aborting, keeping existing data")
        sys.exit(1)

    all_stores = {}
    for i, url in enumerate(city_urls):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            for s in parse_stores(r.text):
                if s["store_id"] and s["store_id"] not in all_stores:
                    all_stores[s["store_id"]] = s
        except Exception as e:
            print(f"      {url}: ERROR {e}")
        if (i + 1) % 100 == 0:
            print(f"      [{i+1}/{len(city_urls)}] city pages scanned, {len(all_stores)} stores")
        time.sleep(DELAY)

    print(f"\n      After city pages: {len(all_stores)} stores")

    # Phase 2 — Product sitemap (stub records for anything the markers missed)
    print("\n[2/8] Checking product sitemap...")
    try:
        added = 0
        for url, st, city, sid in get_product_store_urls():
            if sid not in all_stores:
                all_stores[sid] = {
                    "store_id": sid, "site_number": None, "address": "",
                    "city": city.replace("-", " ").title(), "state": st.upper(),
                    "zip": "", "phone": "", "lat": None, "lng": None,
                    "url": url, "units": []
                }
                added += 1
        print(f"      {added} new stub stores ({len(all_stores)} total)")
    except Exception as e:
        print(f"      WARNING: product sitemap failed: {e}")

    # Phase 3 — Zip sweep (gap filler; also merges fields into sparse records)
    print(f"\n[3/8] Zip code sweep ({len(ZIP_CODES)} zips)...")
    new_found = 0
    for i, z in enumerate(ZIP_CODES):
        try:
            r = requests.get(SEARCH_URL.format(z), headers=HEADERS, timeout=15)
            for s in parse_stores(r.text):
                sid = s["store_id"]
                if not sid:
                    continue
                if sid not in all_stores:
                    all_stores[sid] = s
                    new_found += 1
                    print(f"      NEW: Site#{s.get('site_number','?')} {s['address']}, {s['city']}, {s['state']}")
                else:
                    ex = all_stores[sid]
                    for k, v in s.items():
                        if v not in (None, "", []) and ex.get(k) in (None, "", []):
                            ex[k] = v
        except Exception as e:
            print(f"      ZIP {z}: ERROR {e}")
        if i % 50 == 0:
            print(f"      [{i+1}/{len(ZIP_CODES)}] scanned, {new_found} new stores found")
        time.sleep(DELAY)

    print(f"\n      Total after sweep: {len(all_stores)} stores")

    # Phase 4 — Backfill missing site numbers from individual store pages.
    # The 5-digit site code is the core datum of the directory, so sparse
    # records earn a direct page fetch (capped per run; the rest catch up
    # on subsequent daily runs).
    missing = [s for s in all_stores.values()
               if not s.get("site_number") and s.get("url") and s["url"] != BASE]
    todo = missing[:ENRICH_CAP]
    print(f"\n[4/8] Backfilling site numbers: {len(missing)} missing, fetching {len(todo)}...")
    filled = 0
    for i, s in enumerate(todo):
        try:
            r = requests.get(s["url"], headers=HEADERS, timeout=15)
            for p in parse_stores(r.text):
                if p["store_id"] == s["store_id"]:
                    for k, v in p.items():
                        if v not in (None, "", []) and s.get(k) in (None, "", []):
                            s[k] = v
                    if s.get("site_number"):
                        filled += 1
                    break
        except Exception as e:
            print(f"      store {s['store_id']}: ERROR {e}")
        if (i + 1) % 50 == 0:
            print(f"      [{i+1}/{len(todo)}] fetched, {filled} filled")
        time.sleep(DELAY)
    print(f"      Backfilled {filled} site numbers")

    # Phase 5 — Carry forward previously known stores that discovery missed.
    # City pages cap at ~10 markers, so delisted (often sold-out) stores in
    # dense metros can evade every discovery pass. If a store we knew about
    # still has a live page, it stays in the directory; only 404s drop out.
    lost = [s for s in existing_data if str(s.get("store_id")) not in all_stores]
    print(f"\n[5/8] Carry-forward check: {len(lost)} previously known stores not rediscovered...")
    kept, dropped = 0, 0
    for s in lost:
        sid = str(s.get("store_id"))
        url = s.get("url", "")
        if not sid or not url or not url.startswith("http"):
            continue
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                fresh = next((p for p in parse_stores(r.text) if p["store_id"] == sid), None)
                all_stores[sid] = fresh if fresh else {**s, "units": []}
                kept += 1
            else:
                print(f"      DROPPED ({r.status_code}): Site#{s.get('site_number','?')} {s.get('address')}, {s.get('city')}, {s.get('state')}")
                dropped += 1
        except Exception as e:
            # network hiccup — keep the store rather than lose it
            all_stores[sid] = {**s, "units": []}
            kept += 1
            print(f"      store {sid}: ERROR {e} — carried forward anyway")
        time.sleep(DELAY)
    print(f"      Carried forward {kept}, dropped {dropped}")

    # Optional deep pass (--deep, run weekly in CI) — exhaustively probe the
    # numeric store-ID space via canonical redirects. A real ID 301s to its
    # canonical /self-storage-{st}-{city}/{id}.html URL; the store's own page
    # must then confirm it with a matching map marker, which filters out
    # coming-soon placeholders and closed stores whose redirects linger.
    pipeline_candidates = {}
    if "--deep" in sys.argv:
        max_id = max((int(sid) for sid in all_stores if sid.isdigit()), default=7000)
        candidates = [i for i in range(1, max_id + 500) if str(i) not in all_stores]
        print(f"\n[deep] Probing {len(candidates)} unknown store IDs up to {max_id + 500}...")
        canon = re.compile(r"(https://www\.publicstorage\.com/self-storage-([a-z]{2})-([a-z0-9-]+)/(\d+)\.html)")
        deep_found = 0
        for n, i in enumerate(candidates):
            try:
                r = requests.get(f"{BASE}/self-storage-zz-probe/{i}.html",
                                 headers=HEADERS, timeout=15, allow_redirects=False)
                m = canon.match(r.headers.get("Location", ""))
                if m and m.group(4) == str(i):
                    rp = requests.get(m.group(1), headers=HEADERS, timeout=15)
                    fresh = next((p for p in parse_stores(rp.text) if p["store_id"] == str(i)),
                                 None) if rp.status_code == 200 else None
                    if fresh:
                        all_stores[str(i)] = fresh
                        deep_found += 1
                        print(f"      FOUND Site#{fresh.get('site_number','?')} {fresh.get('address')}, {fresh.get('city')}, {fresh.get('state')}")
                    elif rp.status_code == 200:
                        # live page, no confirming marker — usually a coming-soon store
                        pipeline_candidates[str(i)] = (
                            m.group(1), m.group(3).replace("-", " ").title(), m.group(2).upper())
                    time.sleep(DELAY)
            except Exception as e:
                print(f"      id {i}: ERROR {e}")
            if (n + 1) % 500 == 0:
                print(f"      [{n+1}/{len(candidates)}] probed, {deep_found} found")
            time.sleep(0.3)
        print(f"      Deep probe complete: {deep_found} new stores, "
              f"{len(pipeline_candidates)} coming-soon candidates")

    update_pipeline(all_stores, pipeline_candidates)

    # Safety checks
    if len(all_stores) < MIN_STORES:
        print(f"\n⚠️  SAFETY CHECK FAILED — only {len(all_stores)} stores found (min: {MIN_STORES})")
        print(f"   Keeping existing {existing_count} stores. Check connection and retry.")
        sys.exit(1)

    if existing_count > 0 and len(all_stores) < existing_count * 0.90:
        print(f"\n⚠️  SAFETY CHECK FAILED — {len(all_stores)} stores vs {existing_count} previously")
        print(f"   Drop >10% detected. Keeping existing data.")
        sys.exit(1)

    # Phase 6 — Pricing
    print(f"\n[6/8] Fetching pricing...")
    store_list = list(all_stores.values())
    pricing = fetch_pricing([s["store_id"] for s in store_list])
    for s in store_list:
        s["units"] = pricing.get(s["store_id"], [])
    priced = sum(1 for s in store_list if s["units"])
    coded = sum(1 for s in store_list if s.get("site_number"))
    print(f"      {priced}/{len(store_list)} stores have pricing")
    print(f"      {coded}/{len(store_list)} stores have a site number")

    # Phase 7 — Unit attributes & price ranges from store-page JSON-LD.
    # Ties each SKU to its physical attributes (climate, floor, drive-up) and
    # the advertised min-max price envelope the pricing algorithm works within.
    print(f"\n[7/8] Fetching unit attributes & price ranges ({len(store_list)} store pages)...")
    enriched = 0
    for i, s in enumerate(store_list):
        url = s.get("url", "")
        if not s["units"] or not url or not url.startswith("http"):
            continue
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                attrs = fetch_unit_attrs(r.text)
                hit = False
                for u in s["units"]:
                    a = attrs.get(u.get("sku"))
                    if a:
                        u["attrs"], u["price_min"], u["price_max"] = a
                        hit = True
                if hit:
                    enriched += 1
                rating = fetch_store_rating(r.text)
                if rating:
                    s["rating"], s["reviews"] = rating
        except Exception as e:
            print(f"      store {s['store_id']}: ERROR {e}")
        if (i + 1) % 250 == 0:
            print(f"      [{i+1}/{len(store_list)}] pages fetched, {enriched} stores enriched")
        time.sleep(DELAY)
    print(f"      Attributes captured for {enriched} stores")

    # Phase 8 — Save
    print(f"\n[8/8] Saving...")
    if os.path.exists(OUTPUT_FILE):
        shutil.copy(OUTPUT_FILE, BACKUP_FILE)
        print(f"      Backed up existing → {BACKUP_FILE}")

    with open(OUTPUT_FILE, "w") as f:
        json.dump(store_list, f)

    diff = len(store_list) - existing_count
    print(f"\n✅ Done! {len(store_list)} stores saved ({'+' if diff>=0 else ''}{diff} from last run)")


if __name__ == "__main__":
    main()
