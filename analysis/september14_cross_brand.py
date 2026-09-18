#!/usr/bin/env python3
"""Compare the 2026-09-14 Public Storage pricing reset with peer operators."""
from __future__ import annotations

import json
import statistics
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
BRANDS = ("publicstorage", "cubesmart", "storagesense", "uhaul",
          "storagemart", "smartstop", "independent")


def load(brand: str, day: str) -> list[dict] | None:
    path = ROOT / "history" / brand / f"{day}.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def unit_map(stores: list[dict]) -> dict[tuple[str, str], tuple[dict, dict]]:
    return {
        (str(store.get("store_id")), str(unit.get("sku"))): (store, unit)
        for store in stores for unit in store.get("units", []) if unit.get("sku")
    }


def median(values):
    return statistics.median(values) if values else None


def store_tens(stores: list[dict]) -> list[float]:
    result = []
    for store in stores:
        prices = [float(unit["price"]) for unit in store.get("units", [])
                  if unit.get("available") and unit.get("price")
                  and unit.get("size") == "10x10"]
        if prices:
            result.append(min(prices))
    return result


def analyze(brand: str, before: list[dict], after: list[dict]) -> dict:
    old, new = unit_map(before), unit_map(after)
    common = set(old) & set(new)
    down = same = up = promo_changed = both = price_only = promo_only = 0
    changed_ratios = []
    old_prices, new_prices = [], []
    for key in common:
        old_unit, new_unit = old[key][1], new[key][1]
        old_price, new_price = old_unit.get("price"), new_unit.get("price")
        price_changed = (old_price is not None and new_price is not None
                         and float(old_price) != float(new_price))
        promotion_changed = (
            str(old_unit.get("promo") or ""), str(old_unit.get("promo2") or "")
        ) != (
            str(new_unit.get("promo") or ""), str(new_unit.get("promo2") or "")
        )
        promo_changed += promotion_changed
        both += price_changed and promotion_changed
        price_only += price_changed and not promotion_changed
        promo_only += promotion_changed and not price_changed
        if old_price is None or new_price is None:
            continue
        old_price, new_price = float(old_price), float(new_price)
        old_prices.append(old_price)
        new_prices.append(new_price)
        down += new_price < old_price
        same += new_price == old_price
        up += new_price > old_price
        if price_changed and old_price:
            changed_ratios.append(new_price / old_price)
    return {
        "brand": brand,
        "stores": [len(before), len(after)],
        "unit_groups": [sum(len(s.get("units", [])) for s in before),
                        sum(len(s.get("units", [])) for s in after)],
        "matched_skus": len(common),
        "price": {"down": down, "same": same, "up": up},
        "promotion": {"changed": promo_changed, "both": both,
                      "price_only": price_only, "promotion_only": promo_only},
        "median_10x10": [median(store_tens(before)), median(store_tens(after))],
        "matched_price_median": [median(old_prices), median(new_prices)],
        "median_changed_price_ratio": median(changed_ratios),
    }


def main() -> None:
    results = []
    for brand in BRANDS:
        before, after = load(brand, "2026-09-13"), load(brand, "2026-09-14")
        if before is not None and after is not None:
            results.append(analyze(brand, before, after))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
