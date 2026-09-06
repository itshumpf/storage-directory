# FindStorage

A self-storage directory and pricing-analysis project covering **3,500+ U.S. Public Storage facilities** — every operating store the publicstorage.com platform exposes (the company reports 3,546 including third-party-managed sites) — with unit-level pricing, updated daily by an automated pipeline.

A defining goal: the directory lists **every** store with its 5-digit site number, *including sold-out locations*. Public Storage removes full stores from its own sitemaps (~900 stores are missing from them), so the pipeline recovers those through city-page map markers, per-store page fetches, a carry-forward check against the previous dataset, and a weekly exhaustive probe of the numeric store-ID space.

**Live site:** https://findstorage.netlify.app
**Pricing analysis:** https://findstorage.netlify.app/insights.html
**Daily trends:** https://findstorage.netlify.app/trends.html

## What it does

- **Directory** — searchable, filterable card/map views of every facility: address, phone, site number, and current advertised unit prices with promotions. Location-aware search (city/state/zip/radius) plus free-text search, built with vanilla JavaScript and Leaflet marker clustering.
- **Daily data pipeline** — a scheduled GitHub Actions job re-scrapes the full dataset every morning, rebuilds the SQLite analysis database, regenerates the insights report, and commits the results. Netlify redeploys automatically on push.
- **Market analysis** — a SQL analysis suite over the dataset: state-by-state 10x10 pricing, price per square foot, in-city price variance, live inventory and scarcity, true promotional move-in cost, store clustering, per-capita saturation, and the vehicle-storage market. Results are published as a self-contained report page.
- **Time series** — every daily run appends per-store aggregates (advertised availability, cheapest 10x10, median price) to an append-only history log, and regenerates a trends page: national inventory and price charts, the fastest-renting stores, and the biggest price hikes and cuts. History was backfilled from git snapshots of the dataset, so the series starts April 29, 2026.
- **Pricing-model analysis** — the scraper captures each unit's physical attributes (climate control, floor, drive-up access) and advertised price range from store-page structured data. Pairing same-size units at the same store isolates what each attribute costs, and bucketing prices by remaining inventory exposes the scarcity gradient. One negative result worth reading: the advertised min-max "range" turned out to be mechanically price ±20% on every unit — a disclaimer construct, not a pricing envelope — so it is documented as such rather than tracked. Existing-tenant rate increases (ECRI) are not public and are explicitly out of scope; this models new-customer street rates only.
- **Derived datasets** — beyond the directory itself, the pipeline maintains: a per-SKU **rate-change event log** (every street-rate move and promo switch, daily), **state-by-size demand aggregates** (size-level availability and pricing over time), a **coming-soon store pipeline** (openings tracker fed by the weekly ID probe), **store review ratings** (from page structured data), and an **affordability join** against IRS income data by zip.

## Architecture

```
publicstorage.com (city pages + XML sitemaps + pricing API)
        │
        ▼
daily_scraper.py ──────────► enriched_locations.json
  (GitHub Actions, daily)          │
        │                          ├──► index.html (client-side directory)
        ▼                          │
analysis/load_storage.py ──► storage.db
        │
        ▼
analysis/analyze_storage.py ──► insights.html (published report)
```

Discovery runs in layered passes, because no single source is complete:

1. **City pages** (~1,370 from the category sitemap) — embedded map-marker data is the primary source and the only one that reliably includes delisted/sold-out stores.
2. **Product sitemap** — stub records for anything the markers missed.
3. **Zip-code sweep** — search-results gap filler that also merges missing fields into sparse records.
4. **Site-number backfill** — stores still missing their 5-digit code get their own page fetched.
5. **Carry-forward** — previously known stores that discovery missed stay in the dataset as long as their page is still live; only 404s drop out.
6. **Deep ID probe** (weekly, `--deep`) — exhaustively probes the numeric store-ID space via canonical redirects; a candidate only counts if its own page confirms it with matching marker data, which filters out coming-soon placeholders and lingering redirects for closed stores.
7. **Pricing** — batched lookups against the pricing API for all stores.

The scraper has safety rails: it aborts without writing if it finds fewer than a floor count of stores, or more than a 10% drop from the previous run, so a partial scrape can never clobber good data. Requests are rate-limited (0.4s delay, batched pricing lookups).

## Project structure

| Path | Purpose |
|---|---|
| `index.html` | The directory frontend (single file, no build step) |
| `daily_scraper.py` | Production scraper run daily by GitHub Actions |
| `uhaul_scraper.py` | Slow, resumable, all-or-nothing U.S. U-Haul owned/managed daily snapshot collector |
| `uhaul_parser.py` | U-Haul sitemap and server-rendered room parser |
| `enriched_locations.json` | The dataset: ~3,500 facilities with unit-level pricing |
| `analysis/load_storage.py` | Loads the dataset into a normalized SQLite database |
| `analysis/run_queries.py` | Core analysis query set (run all, or one by number) |
| `analysis/analyze_storage.py` | Data-quality audit + full analysis + report generator |
| `analysis/update_history.py` | Appends per-store daily aggregates + state-size demand aggregates |
| `analysis/update_rate_log.py` | Appends per-SKU price/promo change events to `history/rate_changes.csv` |
| `analysis/build_trends.py` | Generates the daily trends page from the history log |
| `history/` | Append-only time series: store aggregates, size demand, rate-change events, and the coming-soon store pipeline |
| `data/zip_income.csv` | IRS SOI 2022 average income per tax return, for the affordability analysis |
| `legacy/` | One-off Colab scripts used to bootstrap the original dataset |
| `daily_update.bat`, `setup_task.ps1` | Optional local Windows Task Scheduler alternative to CI |

## Running it locally

```bash
pip install -r requirements.txt

# Scrape a fresh dataset (~25-30 min, rate-limited)
python daily_scraper.py

# Separate full U-Haul snapshot (~3+ hours at a five-second request floor)
python uhaul_scraper.py

# Build the analysis database and run the query suite
python analysis/load_storage.py enriched_locations.json
python analysis/run_queries.py

# Generate the insights report
python analysis/analyze_storage.py

# Serve the site
python -m http.server
```

## Tech

Python (requests, BeautifulSoup), SQLite, vanilla JavaScript, Leaflet, GitHub Actions, Netlify.

## Data & fair-use note

This is an independent research/portfolio project, not affiliated with or endorsed by Public Storage. All data is collected from publicly advertised rates at conservative request rates, and is presented with links back to the original listings.

---

Built by Braeden Keena.

## Multi-operator pipeline (September 2026)

Collection now covers four operators. Each collector is unchanged and still owns its own
brand; `storage_pipeline.py` is the seam that joins them.

| Brand | Collector | Snapshot lands in |
|---|---|---|
| Public Storage | `daily_scraper.py` (Actions, `daily.yml`) | `enriched_locations.json` → `history/publicstorage/<date>.json` |
| CubeSmart | `cubesmart_scraper.py` (Actions, `cubesmart.yml`) | `history/cubesmart/<date>.json` |
| Storage Sense | `storagesense_scraper.py` (Actions, `storagesense.yml`) | `history/storagesense/<date>.json` |
| U-Haul | `uhaul_scraper.py` (Actions, `uhaul.yml`) | `history/uhaul/<date>.json` |

All four emit the same record shape, so one immutable dated snapshot per brand per day is the
whole contract. `assemble.yml` runs after any collector finishes and does:

```bash
python storage_pipeline.py daily      # import -> merge -> record -> build-dashboard
python storage_pipeline.py status     # freshness of every brand at a glance
```

which maintains `history/combined/` (cross-brand daily store aggregates, state × size
aggregates, and a per-SKU rate-change log — all brand-tagged, all idempotent) and writes
`dashboard-data.json` for the private dashboard:

```bash
python -m http.server 8777      # then open http://localhost:8777/dashboard.html
```

The dashboard is the replacement for the sunset FindStorage site: overview tiles per brand,
size-by-size price comparison, state table, store search with map and per-size detail,
trends, and a rate-change feed. It is a single file with no build step, reads only
`dashboard-data.json`, and is not published anywhere.

### FindStorage, five operators (2026-09-05)

The original directory and reports now run on the combined dataset:

- `index.html` reads `all_locations.json` (every operator; falls back to `enriched_locations.json`), with operator chips, brand-coloured cards and markers, and street rates struck through where an operator publishes one.
- `analysis/build_trends.py` and `analysis/build_store_history.py` read `history/combined/` (falling back to the legacy files) and add per-operator chart sections; store-history popups cover every operator's stores.
- `pricer.html` — one map, all operators: search a place, pick a size, see every store in range ranked by price with per-operator medians.
- `dashboard.html` — the internal monitor (freshness, comparisons, trends, rate-change feed).

If the Cloudflare/Netlify build copies specific files into `dist/`, add `all_locations.json`, `dashboard-data.json`, `dashboard.html` and `pricer.html` to that list.
