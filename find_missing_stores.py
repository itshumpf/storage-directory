# ============================================================
# PUBLIC STORAGE — MISSING STORE FINDER
# Sweeps US zip codes to find stores missing from sitemap
# Upload your enriched_locations.json when prompted
# Outputs: final_locations.json (complete merged dataset)
# ============================================================

# !pip install requests beautifulsoup4 openpyxl

import requests
import json
import re
import time
from bs4 import BeautifulSoup
from google.colab import files

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE = "https://www.publicstorage.com"
PRICING_API = BASE + "/on/demandware.store/Sites-publicstorage-Site/default/AP-GetSoostonePromo?sites={}"
SEARCH_URL = BASE + "/self-storage-search?location={}"
DELAY = 0.5
BATCH_SIZE = 20
SAVE_EVERY = 50

# ── Representative zip codes covering all US metro areas ─────
# Spread across all states, ~600 zips for good national coverage
ZIP_CODES = [
    # Alabama
    "35203","35401","35601","36104","36201","36301","36532",
    # Alaska
    "99501","99701","99901",
    # Arizona
    "85001","85201","85301","85501","85701","86001","86301","86401",
    # Arkansas
    "72201","72401","72701","72901","71601","71901",
    # California
    "90001","90210","91001","91301","91601","91901","92101","92201",
    "92301","92401","92501","92601","92701","92801","93001","93101",
    "93201","93301","93401","93501","93601","93701","93901","94001",
    "94101","94201","94301","94401","94501","94601","94701","94801",
    "94901","95101","95201","95301","95401","95501","95601","95701",
    "95901","96001","96101",
    # Colorado
    "80201","80401","80501","80601","80701","80901","81001","81101",
    "81201","81301","81401","81501","81601",
    # Connecticut
    "06001","06101","06201","06301","06401","06501","06601","06701",
    "06801","06901",
    # Delaware
    "19701","19801","19901",
    # Florida
    "32004","32101","32201","32301","32401","32501","32601","32701",
    "32801","32901","33001","33101","33201","33301","33401","33501",
    "33601","33701","33801","33901","34101","34201","34301","34401",
    "34601","34701","34901","32003",
    # Georgia
    "30001","30101","30201","30301","30401","30501","30601","30701",
    "30801","30901","31001","31101","31201","31301","31401","31501",
    "31601","31701","31901",
    # Hawaii
    "96701","96801","96901",
    # Idaho
    "83201","83301","83401","83501","83601","83701","83801",
    # Illinois
    "60001","60101","60201","60301","60401","60501","60601","60701",
    "60801","60901","61001","61101","61201","61301","61401","61501",
    "61601","61701","61801","61901","62001","62201","62301","62401",
    "62501","62601","62701","62801","62901",
    # Indiana
    "46001","46101","46201","46301","46401","46501","46601","46701",
    "46801","46901","47001","47201","47301","47401","47501","47601",
    "47701","47901","47802",
    # Iowa
    "50001","50101","50201","50301","50401","50501","50601","50701",
    "51001","51101","51201","51301","51401","51501","52001","52101",
    "52201","52301","52401","52501","52601","52701","52801",
    # Kansas
    "66002","66101","66201","66401","66501","66601","66701","66801",
    "66901","67001","67101","67201","67301","67401","67501","67601",
    "67701","67801","67901",
    # Kentucky
    "40001","40101","40201","40301","40401","40501","40601","40701",
    "40801","40901","41001","41101","41201","41301","41501","41601",
    "41701","41801","42001","42101","42201","42301","42401","42501",
    "42601","42701",
    # Louisiana
    "70001","70101","70301","70401","70501","70601","70701","70801",
    "70901","71001","71101","71201","71301","71401","71601","71701",
    # Maine
    "03901","04001","04101","04210","04330","04401","04530","04601",
    "04730","04901",
    # Maryland
    "20601","20701","20801","20901","21001","21101","21201","21401",
    "21501","21601","21701","21801","21901",
    # Massachusetts
    "01001","01101","01201","01301","01401","01501","01601","01701",
    "01801","01901","02001","02101","02201","02301","02401","02501",
    "02601","02701","02801","02901",
    # Michigan
    "48001","48101","48201","48301","48401","48501","48601","48701",
    "48801","48901","49001","49101","49201","49301","49401","49501",
    "49601","49701","49801","49901",
    # Minnesota
    "55001","55101","55301","55401","55501","55601","55701","55801",
    "55901","56001","56101","56201","56301","56401","56501","56601",
    "56701","56801","56901",
    # Mississippi
    "38601","38701","38801","38901","39001","39101","39201","39301",
    "39401","39501","39601","39701","39801","39901",
    # Missouri
    "63001","63101","63201","63301","63401","63501","63601","63701",
    "63801","63901","64001","64101","64401","64501","64601","64701",
    "64801","64901","65001","65101","65201","65301","65401","65501",
    "65601","65701","65801","65901","66801",
    # Montana
    "59001","59101","59201","59301","59401","59501","59601","59701",
    "59801","59901",
    # Nebraska
    "68001","68101","68301","68401","68501","68601","68701","68801",
    "68901","69001","69101","69201","69301","69401",
    # Nevada
    "89001","89101","89301","89401","89501","89701","89801","89901",
    # New Hampshire
    "03031","03101","03201","03301","03431","03570","03801","03901",
    # New Jersey
    "07001","07101","07201","07301","07401","07501","07601","07701",
    "07801","07901","08001","08101","08201","08301","08401","08501",
    "08601","08701","08801","08901",
    # New Mexico
    "87001","87101","87301","87401","87501","87701","87801","87901",
    "88001","88101","88201","88301","88401","88501",
    # New York
    "10001","10101","10301","10401","10501","10601","10701","10901",
    "11001","11101","11201","11501","11701","11901","12001","12101",
    "12201","12301","12401","12501","12601","12701","12801","12901",
    "13001","13101","13201","13301","13401","13601","13701","13901",
    "14001","14101","14201","14301","14420","14580","14701","14830",
    # North Carolina
    "27006","27101","27201","27301","27401","27501","27601","27701",
    "27801","27901","28001","28101","28201","28301","28401","28501",
    "28601","28701","28801","28901","29001",
    # North Dakota
    "58001","58101","58201","58301","58401","58501","58601","58701",
    "58801","58901",
    # Ohio
    "43001","43101","43201","43301","43401","43501","43601","43701",
    "43801","43901","44001","44101","44201","44301","44401","44501",
    "44601","44701","44801","44901","45001","45101","45201","45301",
    "45401","45501","45601","45701","45801","45901","46001",
    # Oklahoma
    "73001","73101","73401","73501","73601","73701","73801","73901",
    "74001","74101","74301","74401","74501","74601","74701","74801",
    "74901","75001",
    # Oregon
    "97001","97101","97201","97301","97401","97501","97601","97701",
    "97801","97901",
    # Pennsylvania
    "15001","15101","15201","15301","15401","15501","15601","15701",
    "15801","15901","16001","16101","16201","16301","16401","16501",
    "16601","16701","16801","16901","17001","17101","17201","17301",
    "17401","17501","17601","17701","17801","17901","18001","18101",
    "18201","18301","18401","18501","18601","18701","18801","18901",
    "19001","19101","19301","19401","19601",
    # Rhode Island
    "02801","02830","02860","02893","02901",
    # South Carolina
    "29001","29101","29201","29301","29401","29501","29601","29701",
    "29801","29901",
    # South Dakota
    "57001","57101","57201","57301","57401","57501","57601","57701",
    "57801","57901",
    # Tennessee
    "37010","37101","37201","37301","37401","37501","37601","37701",
    "37801","37901","38001","38101","38201","38301","38401","38501",
    "38601","38701","38801","38901",
    # Texas
    "73301","75001","75101","75201","75301","75401","75501","75601",
    "75701","75801","75901","76001","76101","76201","76301","76401",
    "76501","76601","76701","76801","76901","77001","77101","77201",
    "77301","77401","77501","77601","77701","77801","77901","78001",
    "78101","78201","78301","78401","78501","78601","78701","78801",
    "78901","79001","79101","79201","79301","79401","79501","79601",
    "79701","79801","79901",
    # Utah
    "84001","84101","84201","84301","84401","84501","84601","84701",
    # Vermont
    "05001","05101","05201","05301","05401","05601","05701","05819",
    # Virginia
    "20101","20151","22001","22101","22201","22301","22401","22501",
    "22601","22701","22801","22901","23001","23101","23201","23301",
    "23401","23601","23701","23801","23901","24001","24101","24201",
    "24301","24401","24501","24601",
    # Washington
    "98001","98101","98201","98301","98401","98501","98601","98701",
    "98801","98901","99001","99101","99201","99301","99401",
    # West Virginia
    "24701","24801","24901","25001","25101","25201","25301","25401",
    "25501","25601","25701","25801","26003","26101","26201","26301",
    "26401","26501","26601","26701","26801","26901",
    # Wisconsin
    "53001","53101","53201","53401","53501","53601","53701","53801",
    "53901","54001","54101","54201","54301","54401","54501","54601",
    "54701","54801","54901","55001",
    # Wyoming
    "82001","82101","82201","82301","82401","82501","82601","82701",
    "82801","82901",
    # DC
    "20001","20002","20003","20004","20005",
]
# Deduplicate
ZIP_CODES = list(dict.fromkeys(ZIP_CODES))


def parse_marker_data(html):
    """Extract all googleMapMarkerData from a search results page."""
    stores = []
    soup = BeautifulSoup(html, "html.parser")
    for inp in soup.find_all("input", class_="googleMapMarkerData"):
        try:
            raw = inp.get("value", "")
            data = json.loads(raw)
            content = data.get("content", {})
            title = data.get("title", "")
            m = re.match(r"^(\d{4,6})\s*-", title)
            site_number = m.group(1) if m else None
            stores.append({
                "store_id": str(data.get("storeID", "")),
                "site_number": site_number,
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
    url = PRICING_API.format(joined)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
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
    print("PUBLIC STORAGE — MISSING STORE FINDER")
    print("=" * 60)

    # Upload existing data
    print("\nUpload your enriched_locations.json...")
    uploaded = files.upload()
    fname = list(uploaded.keys())[0]
    with open(fname) as f:
        existing = json.load(f)

    # Build lookup of known store IDs
    known_ids = {s["store_id"] for s in existing}
    print(f"\nLoaded {len(existing)} existing stores")
    print(f"Sweeping {len(ZIP_CODES)} zip codes to find missing stores...\n")

    # Sweep zip codes
    found_new = {}  # store_id -> store dict
    errors = 0

    for i, zipcode in enumerate(ZIP_CODES):
        try:
            url = SEARCH_URL.format(zipcode)
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            stores = parse_marker_data(resp.text)

            new_in_zip = 0
            for s in stores:
                sid = s["store_id"]
                if sid and sid not in known_ids and sid not in found_new:
                    found_new[sid] = s
                    new_in_zip += 1

            if new_in_zip > 0:
                print(f"  [{i+1}/{len(ZIP_CODES)}] ZIP {zipcode} — found {new_in_zip} NEW stores! (total new: {len(found_new)})")
            elif i % 50 == 0:
                print(f"  [{i+1}/{len(ZIP_CODES)}] ZIP {zipcode} — {len(stores)} stores, no new ones (total new so far: {len(found_new)})")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [{i+1}/{len(ZIP_CODES)}] ZIP {zipcode} — ERROR: {e}")

        time.sleep(DELAY)

    print(f"\n✅ Sweep complete!")
    print(f"   New stores found: {len(found_new)}")
    print(f"   Errors: {errors}")

    if not found_new:
        print("\nNo new stores found — your data is already complete!")
        files.download(fname)
        return

    # Fetch pricing for new stores
    print(f"\nFetching pricing for {len(found_new)} new stores...")
    new_list = list(found_new.values())
    store_ids = [s["store_id"] for s in new_list]
    batches = [store_ids[i:i+BATCH_SIZE] for i in range(0, len(store_ids), BATCH_SIZE)]
    pricing_map = {}
    for i, batch in enumerate(batches):
        print(f"  Batch {i+1}/{len(batches)}...", end=" ", flush=True)
        result = fetch_pricing_batch(batch)
        pricing_map.update(result)
        print(f"got {len(result)} responses")
        time.sleep(0.4)

    for s in new_list:
        s["units"] = pricing_map.get(s["store_id"], [])

    # Print what we found
    print(f"\nNew stores discovered:")
    for s in sorted(new_list, key=lambda x: x.get("site_number") or ""):
        print(f"  Site#{s.get('site_number','?')} | {s['address']}, {s['city']}, {s['state']} | {len(s['units'])} unit types")

    # Merge and save
    final = existing + new_list
    print(f"\nMerging: {len(existing)} existing + {len(new_list)} new = {len(final)} total stores")

    with open("final_locations.json", "w") as f:
        json.dump(final, f)
    print("Saved final_locations.json")

    files.download("final_locations.json")
    print(f"\n✅ Done! Download your complete dataset.")


main()
