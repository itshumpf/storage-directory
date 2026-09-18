import json

import daily_scraper as scraper


def test_attribute_checkpoint_round_trip_and_sku_safe_restore(tmp_path, monkeypatch):
    monkeypatch.setattr(scraper, "CHECKPOINT_DIR", str(tmp_path))
    monkeypatch.setattr(scraper.sys, "argv", ["daily_scraper.py"])
    source = {
        "store_id": "101", "rating": 4.7, "reviews": 12,
        "units": [
            {"sku": "same", "attrs": "Climate Controlled", "price_min": 40, "price_max": 60},
            {"sku": "old", "attrs": "Drive Up", "price_min": 80, "price_max": 90},
        ],
    }
    results = {"101": scraper._capture_attribute_result(source)}
    scraper._save_attribute_checkpoint(results, {"101"})

    loaded, completed = scraper._load_attribute_checkpoint()
    fresh = [{"store_id": "101", "units": [{"sku": "same"}, {"sku": "new"}]}]
    assert scraper._restore_attribute_results(fresh, loaded) == 1
    assert completed == {"101"}
    assert fresh[0]["rating"] == 4.7
    assert fresh[0]["units"][0]["attrs"] == "Climate Controlled"
    assert "attrs" not in fresh[0]["units"][1]


def test_atomic_json_replaces_destination(tmp_path):
    destination = tmp_path / "output.json"
    destination.write_text('{"old": true}', encoding="utf-8")
    scraper._atomic_json(str(destination), {"new": True})
    assert json.loads(destination.read_text(encoding="utf-8")) == {"new": True}
    assert not (tmp_path / "output.json.tmp").exists()
