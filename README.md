# FindStorage

A self-storage directory and pricing-analysis project covering **~3,500 U.S. Public Storage facilities** — over 98% of the company's reported footprint — with unit-level pricing, updated daily by an automated pipeline.

A defining goal: the directory lists **every** store with its 5-digit site number, *including sold-out locations*. Public Storage removes full stores from its own sitemaps (~900 stores are missing from them), so the pipeline recovers those through city-page map markers, per-store page fetches, and a carry-forward check against the previous dataset.

**Live site:** https://findstorage.netlify.app
**Pricing analysis:** https://findstorage.netlify.app/insights.html

## What it does

- **Directory** — searchable, filterable card/map views of every facility: address, phone, site number, and current advertised unit prices with promotions. Location-aware search (city/state/zip/radius) plus free-text search, built with vanilla JavaScript and Leaflet marker clustering.
- **Daily data pipeline** — a scheduled GitHub Actions job re-scrapes the full dataset every morning, rebuilds the SQLite analysis database, regenerates the insights report, and commits the results. Netlify redeploys automatically on push.
- **Market analysis** — a SQL analysis suite over the dataset: state-by-state 10x10 pricing, price per square foot, in-city price variance, promotion frequency and depth, unit size mix, market saturation, and price outliers. Results are published as a self-contained report page.

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
6. **Pricing** — batched lookups against the pricing API for all stores.

The scraper has safety rails: it aborts without writing if it finds fewer than a floor count of stores, or more than a 10% drop from the previous run, so a partial scrape can never clobber good data. Requests are rate-limited (0.4s delay, batched pricing lookups).

## Project structure

| Path | Purpose |
|---|---|
| `index.html` | The directory frontend (single file, no build step) |
| `daily_scraper.py` | Production scraper run daily by GitHub Actions |
| `enriched_locations.json` | The dataset: ~3,500 facilities with unit-level pricing |
| `analysis/load_storage.py` | Loads the dataset into a normalized SQLite database |
| `analysis/run_queries.py` | Core analysis query set (run all, or one by number) |
| `analysis/analyze_storage.py` | Data-quality audit + full analysis + report generator |
| `legacy/` | One-off Colab scripts used to bootstrap the original dataset |
| `daily_update.bat`, `setup_task.ps1` | Optional local Windows Task Scheduler alternative to CI |

## Running it locally

```bash
pip install -r requirements.txt

# Scrape a fresh dataset (~25-30 min, rate-limited)
python daily_scraper.py

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
