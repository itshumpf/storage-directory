"""Import a friendly text inbox into the fail-closed independent registry."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from independent_registry import validate_operator_registry


INBOX = Path("independent_candidates.txt")
REGISTRY = Path("independent_operators.json")


def _slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")


def _display_name(host: str) -> str:
    stem = host.removeprefix("www.").split(".")[0]
    return re.sub(r"[-_]+", " ", stem).title()


def parse_inbox(text: str) -> list[dict]:
    rows, seen = [], set()
    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line:
            name, raw_url = (part.strip() for part in line.split("|", 1))
        else:
            name, raw_url = "", line
        parsed = urlsplit(raw_url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise ValueError(f"line {number}: expected an http(s) operator URL")
        host = parsed.hostname.lower().removeprefix("www.")
        if host in seen:
            continue
        seen.add(host)
        clean_url = urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "/", "", ""))
        rows.append({"name": name or _display_name(host), "domain": host,
                     "representative_url": clean_url})
    return rows


def merge_candidates(registry: dict, candidates: list[dict]) -> tuple[dict, list[dict]]:
    existing_domains = {row["domain"] for row in registry["operators"]}
    existing_ids = {row["operator_id"] for row in registry["operators"]}
    added = []
    for candidate in candidates:
        if candidate["domain"] in existing_domains:
            continue
        base = _slug(candidate["name"]) or _slug(candidate["domain"])
        operator_id, suffix = base, 2
        while operator_id in existing_ids:
            operator_id, suffix = f"{base}_{suffix}", suffix + 1
        row = {
            "operator_id": operator_id,
            **candidate,
            "platform_hint": "unknown",
            "discovery_basis": "Manual text intake; not yet classified.",
            "evidence_type": "manual_url",
            "status": "candidate",
            "robots_reviewed": False,
            "terms_reviewed": False,
            "enabled": False,
        }
        registry["operators"].append(row)
        added.append(row)
        existing_domains.add(candidate["domain"])
        existing_ids.add(operator_id)
    return registry, added


def save_atomic(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def run(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inbox", type=Path, default=INBOX)
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--apply", action="store_true", help="write new disabled candidates to the registry")
    args = parser.parse_args(argv)
    registry = validate_operator_registry(args.registry)
    candidates = parse_inbox(args.inbox.read_text(encoding="utf-8"))
    registry, added = merge_candidates(registry, candidates)
    print(json.dumps({"inbox_entries": len(candidates), "new_candidates": len(added),
                      "operators": [{"operator_id": row["operator_id"], "name": row["name"],
                                     "url": row["representative_url"]} for row in added],
                      "applied": args.apply}, indent=2))
    if args.apply and added:
        save_atomic(args.registry, registry)
        validate_operator_registry(args.registry)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
