"""Identity and deduplication helpers for independent self-storage facilities."""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit


SUFFIXES = {
    "avenue": "ave", "boulevard": "blvd", "circle": "cir", "court": "ct", "drive": "dr",
    "highway": "hwy", "lane": "ln", "parkway": "pkwy", "place": "pl", "road": "rd",
    "street": "st", "terrace": "ter", "trail": "trl",
}
DIRECTIONS = {"north": "n", "south": "s", "east": "e", "west": "w",
              "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw"}


def _words(value) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


def normalized_address(record: dict) -> str:
    words = [SUFFIXES.get(word, DIRECTIONS.get(word, word)) for word in _words(record.get("address"))]
    city = "".join(_words(record.get("city")))
    state = "".join(_words(record.get("state")))
    zip5 = re.sub(r"\D", "", str(record.get("zip") or ""))[:5]
    return "|".join(("".join(words), city, state, zip5))


def facility_match_key(record: dict) -> tuple[str, str]:
    """Prefer a normalized physical address; coordinates are a conservative fallback."""
    address = normalized_address(record)
    mailing_only = bool(re.search(r"\b(?:p\.?\s*o\.?|post office)\s+box\b",
                                  str(record.get("address") or ""), re.I))
    if (record.get("address") and not mailing_only and record.get("state")
            and (record.get("zip") or record.get("city"))):
        return "address", address
    try:
        lat, lng = float(record["lat"]), float(record["lng"])
        return "geo", f"{lat:.4f},{lng:.4f}"
    except (KeyError, TypeError, ValueError):
        raise ValueError("facility needs a usable address or coordinates for deduplication")


def deduplicate_facilities(records: list[dict]) -> list[dict]:
    """Merge aliases for the same physical property without discarding provenance."""
    groups = defaultdict(list)
    for record in records:
        groups[facility_match_key(record)].append(record)
    merged = []
    for key, values in sorted(groups.items()):
        primary = dict(values[0])
        primary["facility_key"] = f"{key[0]}:{key[1]}"
        primary["aliases"] = sorted({str(v.get("name") or "").strip() for v in values if v.get("name")})
        primary["operator_ids"] = sorted({str(v.get("operator_id") or "").strip()
                                           for v in values if v.get("operator_id")})
        primary["platforms"] = sorted({str(v.get("platform") or "").strip()
                                       for v in values if v.get("platform")})
        primary["source_urls"] = sorted({str(v.get("url") or "").strip() for v in values if v.get("url")})
        merged.append(primary)
    return merged


def validate_operator_registry(path: str | Path) -> dict:
    """Fail closed when a candidate registry could accidentally enable collection."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    operators = data.get("operators")
    if not isinstance(operators, list):
        raise ValueError("operator registry must contain an operators list")
    ids, domains = set(), set()
    for operator in operators:
        operator_id = str(operator.get("operator_id") or "").strip()
        domain = str(operator.get("domain") or "").lower().strip().removeprefix("www.")
        url_host = (urlsplit(str(operator.get("representative_url") or "")).hostname or "").lower()
        if not operator_id or not domain or not url_host:
            raise ValueError("each operator needs operator_id, domain, and representative_url")
        if url_host.removeprefix("www.") != domain:
            raise ValueError(f"representative URL host does not match {domain}")
        if operator_id in ids or domain in domains:
            raise ValueError(f"duplicate operator id or domain: {operator_id} / {domain}")
        if operator.get("status") not in {"candidate", "probe_ready", "policy_pending", "policy_hold"}:
            raise ValueError(f"unreviewed operator has unsafe status: {operator_id}")
        if operator.get("enabled") is not False:
            raise ValueError(f"candidate must be disabled: {operator_id}")
        if operator.get("status") == "candidate" and (
                operator.get("robots_reviewed") is not False
                or operator.get("terms_reviewed") is not False):
            raise ValueError(f"candidate incorrectly marked reviewed: {operator_id}")
        if operator.get("status") == "probe_ready":
            if operator.get("robots_reviewed") is not True or operator.get("terms_reviewed") is not False:
                raise ValueError(f"probe-ready candidate has inconsistent review flags: {operator_id}")
            if operator.get("robots_status") != "public_paths_allowed":
                raise ValueError(f"probe-ready candidate lacks a robots decision: {operator_id}")
        if operator.get("status") == "policy_hold" and operator.get("terms_reviewed") is True:
            if not operator.get("terms_url") or not operator.get("policy_reason"):
                raise ValueError(f"reviewed policy hold lacks documentation: {operator_id}")
        if operator.get("status") == "policy_pending":
            if operator.get("terms_status") != "not_found" or not operator.get("policy_reason"):
                raise ValueError(f"policy pending candidate lacks documentation: {operator_id}")
        if not operator.get("platform_hint") or not operator.get("discovery_basis"):
            raise ValueError(f"candidate lacks discovery evidence: {operator_id}")
        ids.add(operator_id)
        domains.add(domain)
    enabled = sum(operator.get("enabled") is True for operator in operators)
    if data.get("collection_policy", {}).get("enabled_count") != enabled:
        raise ValueError("collection policy enabled_count does not match operator records")
    return data
