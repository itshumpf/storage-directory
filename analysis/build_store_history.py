"""
build_store_history.py — Generate store-history.json: a lazily-fetched
sidecar carrying every currently-tracked store's price-over-time series, for
the directory's per-store price-history popup.

Run after update_history.py, from the repo root:
    python analysis/build_store_history.py

Reads the combined daily series (via build_trends.load_history, for the dense
cheapest-10x10 + all-sizes-median series) and history/combined/store-sizes-*.csv
(for the per-size series), every operator, scoped to the stores present in
all_locations.json — falling back to the legacy Public Storage files and
enriched_locations.json on a clone without pipeline output. Pure stdlib.

Shape (compact, keys short on purpose — this file is fetched once per
directory visit, not per store):
    {
      "d":  [dates for the dense series, shared axis],
      "sd": [dates for the per-size series, shared axis],
      "v":  [size vocabulary, shared column order for "sz" rows],
      "m": {
        "<store_id>": [
          [[ten, med], ...]   // aligned to "d", null where the store wasn't
                              // logged that day
          [[p0..p9], ...]     // aligned to "sd", one row per date, one
                              // column per size in "v", null where absent
        ]
      }
    }
"""
import csv
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_markets import SIZE_VOCAB  # noqa: E402
from build_trends import load_history  # noqa: E402

OUT = "store-history.json"
STORE_SIZE_CSV = re.compile(r"^store-sizes-\d{4}-\d{2}\.csv$")


def load_store_size_history():
    """date -> store_id -> {size: price} from history/store-sizes-YYYY-MM.csv."""
    out = {}
    # RETARGETED 2026-09-05: the combined, brand-tagged series when the pipeline
    # has produced it (every operator), else the legacy Public Storage files.
    combined = sorted((Path("history") / "combined").glob("store-sizes-????-??.csv"))
    files = combined or [p for p in sorted(Path("history").glob("store-sizes-*.csv")) if STORE_SIZE_CSV.match(p.name)]
    for p in files:
        with open(p, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if not row.get("price"):
                    continue
                d = row["date"]
                out.setdefault(d, {}).setdefault(row["store_id"], {})[row["size"]] = float(row["price"])
    return dict(sorted(out.items()))


def main():
    src = Path("all_locations.json") if Path("all_locations.json").exists() else Path("enriched_locations.json")
    if not src.exists():
        sys.exit(f"{src} not found — run this from the repo root after a scrape.")
    current_ids = []
    seen = set()
    for s in json.loads(src.read_text(encoding="utf-8")):
        sid = str(s.get("store_id", ""))
        if sid and sid not in seen:
            seen.add(sid)
            current_ids.append(sid)

    snaps = load_history()
    dates = list(snaps)

    size_hist = load_store_size_history()
    size_dates = list(size_hist)

    out = {}
    for sid in current_ids:
        dense = []
        any_dense = False
        for d in dates:
            row = snaps.get(d, {}).get(sid)
            if row:
                any_dense = True
                dense.append([row["ten"], row["med"]])
            else:
                dense.append([None, None])

        sparse = []
        any_sparse = False
        for d in size_dates:
            prices = size_hist.get(d, {}).get(sid)
            if prices:
                any_sparse = True
                sparse.append([prices.get(sz) for sz in SIZE_VOCAB])
            else:
                sparse.append([None] * len(SIZE_VOCAB))

        if any_dense or any_sparse:
            out[sid] = [dense, sparse]

    payload = {"d": dates, "sd": size_dates, "v": SIZE_VOCAB, "m": out}
    Path(OUT).write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT} ({len(out):,} stores, {len(dates)} dense dates, {len(size_dates)} per-size dates)")


if __name__ == "__main__":
    main()
