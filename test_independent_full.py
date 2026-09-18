"""Offline safety tests for the full independent collector."""
from pathlib import Path

from independent_full_scraper import (canary_first_work, complete_checkpoint_record,
                                      declared_sitemaps, load_config, selected_facility_urls)
from storage_pipeline import BRANDS


def test_full_cohort_contains_reviewed_adapters_only():
    cohort, max_drop = load_config(
        Path("independent_collection.json"), Path("independent_operators.json"))
    ids = {operator.operator_id for operator, _config in cohort}
    assert len(ids) == 9 and max_drop == 0.10
    assert "atlantic_self_storage" not in ids
    assert "san_diego_self_storage" not in ids
    assert "fort_knox_storage" not in ids
    assert "price_self_storage" not in ids
    assert "right_move_storage" in ids
    assert "hornet_storage_ii" not in ids
    assert BRANDS["independent"]["floor"] == sum(
        int(config["minimum_facilities"]) for _operator, config in cohort)


def test_robots_sitemaps_are_same_host_only():
    text = (
        "User-agent: *\nAllow: /\n"
        "Sitemap: https://little.example/sitemap.xml\n"
        "Sitemap: https://foreign.example/sitemap.xml\n")
    assert declared_sitemaps(text, "https://little.example") == [
        "https://little.example/sitemap.xml"]


def test_full_catalog_selector_deduplicates_and_excludes_numbered_articles():
    urls = [
        "https://little.example/123-main-st-city-tn-37211",
        "https://little.example/123-main-st-city-tn-37211",
        "https://little.example/5-things-to-look-for-in-storage",
    ]
    assert selected_facility_urls(urls, 100) == [
        "https://little.example/123-main-st-city-tn-37211"]


def test_atlantic_style_marketing_page_is_not_an_address_candidate():
    urls = [
        "https://little.example/storage-locations/fl/jacksonville/24-hour-storage",
        "https://little.example/storage-locations/fl/jacksonville/10601-alta-dr",
    ]
    assert selected_facility_urls(urls, 100) == [
        "https://little.example/storage-locations/fl/jacksonville/10601-alta-dr"]


def test_work_order_runs_one_canary_per_operator_before_expansion():
    cohort, _ = load_config(Path("independent_collection.json"), Path("independent_operators.json"))
    cohort = cohort[:2]
    urls = {cohort[0][0].operator_id: ["a1", "a2", "a3"],
            cohort[1][0].operator_id: ["b1", "b2"]}
    work = canary_first_work(cohort, urls)
    assert [(url, phase) for _operator, url, phase in work] == [
        ("a1", "canary"), ("b1", "canary"),
        ("a2", "collection"), ("a3", "collection"), ("b2", "collection")]


def test_config_supports_per_operator_adapter(tmp_path):
    config = tmp_path / "mixed.json"
    config.write_text('''{
      "adapter": "storable_apollo", "operators": [{
        "operator_id": "right_move_storage", "adapter": "storagely_html",
        "sitemap_url": "https://www.rightmovestorage.com/sitemap.xml",
        "minimum_facilities": 1, "minimum_url_score": 80
      }]}
    ''')
    cohort, _ = load_config(config, Path("independent_operators.json"))
    assert cohort[0][1]["adapter"] == "storagely_html"


def test_explicit_sold_out_store_is_a_complete_checkpoint():
    assert complete_checkpoint_record({
        "url": "https://example.com/store", "address": "1 Main St", "state": "TX",
        "zip": "75001", "inventory_status": "sold_out", "units": [],
    })
