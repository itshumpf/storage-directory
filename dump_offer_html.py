"""
dump_offer_html.py — one-off diagnostic. Not part of the daily pipeline.

WHY THIS EXISTS
---------------
As of 2026-09-01, 0 of 60,132 units in enriched_locations.json carry `attrs`,
`price_min` or `price_max`. Phase 7 of daily_scraper.py runs and does fetch the
store pages: 4,620 of 4,670 stores came back with a `rating`, and that rating is
parsed by RATING_RE from the *same* HTML string, in the *same* loop iteration,
as OFFER_RE. So the pages return 200, the JSON-LD is present, and RATING_RE
still matches it.

That isolates the fault to OFFER_RE itself. The offers block's shape has
changed underneath the pattern.

Fixing the pattern requires seeing what the block looks like now. This script
fetches ONE store page and prints the relevant slice of it, so the regex can be
rewritten against the real current markup rather than guessed at.

USAGE (from the repo root):
    python dump_offer_html.py

It writes offer_sample.txt and prints a summary. Send me offer_sample.txt.

It makes exactly one request, to one store page that daily_scraper.py already
fetches every day as part of its normal run.
"""
import json
import re
import sys

import requests

from daily_scraper import HEADERS, OFFER_RE, RATING_RE

OUT = "offer_sample.txt"


def pick_store():
    """A store that has priced units, so its page is certain to carry offers."""
    with open("enriched_locations.json", encoding="utf-8") as f:
        stores = json.load(f)
    for s in stores:
        if s.get("units") and s.get("url", "").startswith("http") and s.get("rating"):
            return s
    return None


def main():
    s = pick_store()
    if not s:
        print("No suitable store found in enriched_locations.json", file=sys.stderr)
        return 1

    print(f"Store {s['store_id']} — {s.get('city')}, {s.get('state')}")
    print(f"  {len(s['units'])} units, first sku: {s['units'][0].get('sku')}")
    print(f"  {s['url']}")

    r = requests.get(s["url"], headers=HEADERS, timeout=20)
    print(f"  HTTP {r.status_code}, {len(r.text):,} bytes")
    if r.status_code != 200:
        return 1

    html = r.text

    # What still matches, and what doesn't.
    print(f"\n  OFFER_RE matches:  {len(OFFER_RE.findall(html))}")
    print(f"  RATING_RE matches: {len(RATING_RE.findall(html))}")

    # Does the first known SKU appear in the page at all? If it does not, the
    # offers have moved out of the server-rendered HTML entirely (client-side
    # hydration), which is a different problem from a changed pattern and no
    # regex will fix it.
    sku = s["units"][0].get("sku", "")
    print(f"  first sku present in HTML: {sku in html}")
    for token in ('"sku"', '"itemOffered"', '"price"', '"description"',
                  '"Offer"', 'application/ld+json'):
        print(f"    {token:<22} {html.count(token):>5} occurrences")

    # Save the neighbourhood of the first "sku" occurrence — that is where the
    # offer object lives, whatever shape it now has.
    chunks = []
    for m in list(re.finditer(r'"sku"', html))[:3]:
        lo, hi = max(0, m.start() - 1500), min(len(html), m.start() + 800)
        chunks.append(f"--- context around '\"sku\"' at offset {m.start()} ---\n"
                      + html[lo:hi])

    if not chunks:
        for m in list(re.finditer(r'application/ld\+json', html))[:2]:
            lo, hi = m.start(), min(len(html), m.start() + 4000)
            chunks.append(f"--- ld+json block at offset {m.start()} ---\n"
                          + html[lo:hi])

    with open(OUT, "w", encoding="utf-8") as f:
        f.write(f"store {s['store_id']} {s.get('city')}, {s.get('state')}\n")
        f.write(f"url {s['url']}\n")
        f.write(f"first known sku from pricing API: {sku}\n")
        f.write(f"OFFER_RE matches {len(OFFER_RE.findall(html))}, "
                f"RATING_RE matches {len(RATING_RE.findall(html))}\n\n")
        f.write("\n\n".join(chunks) if chunks
                else "NO '\"sku\"' AND NO ld+json IN PAGE\n")

    print(f"\n  wrote {OUT} ({len(chunks)} context chunk(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())
