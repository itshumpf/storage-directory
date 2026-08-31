#!/usr/bin/env python3
"""build_parquet.py — Convert history/*.csv into a queryable Parquet corpus.

    python analysis/build_parquet.py              # incremental, skips existing
    python analysis/build_parquet.py --rebuild    # rewrite everything

Writes history/parquet/<table>/<YYYY-MM-DD>.parquet — one immutable file per
table per day. Then:

    duckdb> SELECT * FROM read_parquet('history/parquet/rate_changes/*.parquet')

WHY DAILY PARTITIONS AND NOT ONE FILE
-------------------------------------
A single growing file — .csv, .parquet or .duckdb — is rewritten on every
append, so git stores a complete new copy each day. Twelve months of that is
365 full copies of an ever-larger binary.

Daily partitions are written once and never touched again, so git only ever
adds a small new file. Measured on the real corpus 2026-08-30:

    history/ as CSV      71.67 MB
    history/ as Parquet   7.61 MB     9.4x smaller
    rate_changes.csv     38.44 MB  ->  3.83 MB   (10.0x)

Projected to twelve months of rate changes: ~307 MB of CSV against ~31 MB of
Parquet. The CSV path meets GitHub's 100 MB per-file limit around month four.

The .duckdb file is deliberately NOT an artifact here. DuckDB reads the
partitions directly through read_parquet(), so the database is derived in
seconds and never has to be stored, backed up or committed.

THE CARRIAGE RETURN, AND WHY THE SOURCE CSV IS LEFT ALONE
---------------------------------------------------------
history/rate_changes.csv has mixed line endings: CRLF on 2026-07-11 through
2026-07-19, LF from 2026-07-20 on. 75,437 of 545,272 rows are affected.

The CR lands inside the final field, so `brand` holds two values that render
identically — 469,835 "publicstorage" and 75,437 "publicstorage\\r". Any
GROUP BY brand splits the log in two without saying so. That column exists
precisely so a second operator can enter the dataset, so it has to be right.

This script strips the CR on the way into Parquet and does NOT rewrite the
source CSV. The line ending is the only surviving record of which rows were
written by a Windows local run and which by the Linux CI runner, and raw
files should not be edited to fix a problem that belongs at the boundary. The
provenance is preserved explicitly instead, in a `wrote_with` column, which is
strictly more useful than a byte nobody can see.

DuckDB's CSV sniffer refuses these files outright because of the mixed
endings; strict_mode=false is required and is set below.
"""
import argparse
import hashlib
import os
import re
import sys
from pathlib import Path

HISTORY = Path("history")
OUT = HISTORY / "parquet"

# Files whose rows carry a date column and are partitioned by it.
DATED = {
    "rate_changes.csv": "date",
    "store-sizes-2026-*.csv": "date",
    "sizes-2026-*.csv": "date",
    "2026-*.csv": "date",
}
# Small files with no useful date partition — copied whole.
WHOLE = ("pipeline.csv", "psa-stock.csv")

CR_BOUNDARY_NOTE = (
    "CRLF rows are 2026-07-11..2026-07-19 (local Windows runs); "
    "LF rows are 2026-07-20 onward (GitHub Actions ubuntu runner)."
)


def table_name(stem):
    """history/2026-08.csv and history/2026-07.csv are the same table.

    The monthly stem carries no subject at all — '2026-08' is a date, not a
    name — so it is mapped explicitly rather than by stripping, which would
    leave an empty string and silently produce one table per month.
    """
    if re.fullmatch(r"20\d\d-\d\d", stem):
        return "stores_daily"
    return re.sub(r"-20\d\d-\d\d$", "", stem)


def varchar_columns(con, rel):
    """Column names DuckDB types as VARCHAR, so trim() applies only to them."""
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{rel}', strict_mode=false, "
        f"header=true)").fetchall()
    return [r[0] for r in rows if str(r[1]).upper().startswith("VARCHAR")]


def line_ending_dates(path):
    """date -> 'crlf' | 'lf' | 'mixed', read from the raw bytes."""
    seen = {}
    with open(path, "rb") as f:
        f.readline()                                   # header
        for raw in f:
            if not raw.strip():
                continue
            d = raw.split(b",", 1)[0].decode("utf-8", "replace")
            kind = "crlf" if raw.endswith(b"\r\n") else "lf"
            if seen.get(d, kind) != kind:
                kind = "mixed"
            seen[d] = kind
    return seen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rebuild", action="store_true",
                    help="rewrite partitions that already exist")
    ap.add_argument("--history", default=str(HISTORY))
    args = ap.parse_args()

    hist = Path(args.history)
    out = hist / "parquet"
    try:
        import duckdb
    except ImportError:
        sys.exit("duckdb is not installed:  pip install duckdb")

    con = duckdb.connect()
    written = skipped = 0
    total_csv = total_pq = 0

    patterns = list(DATED.items())
    seen_files = set()

    for pattern, datecol in patterns:
        for src in sorted(hist.glob(pattern)):
            if src.name in WHOLE or src in seen_files:
                continue
            seen_files.add(src)
            tdir = out / table_name(src.stem)
            tdir.mkdir(parents=True, exist_ok=True)

            endings = line_ending_dates(src)
            # strict_mode=false: required, the mixed endings defeat the sniffer.
            # Types are inferred rather than forced to VARCHAR — storing numbers
            # as text costs about 2.6x of the compression.
            rel = src.as_posix()
            # trim() only the text columns; this is what removes the stray CR
            # from whichever field happens to be last on a CRLF row.
            repl = ", ".join(f'trim("{c}") AS "{c}"' for c in
                             varchar_columns(con, rel))
            repl = f" REPLACE ({repl})" if repl else ""
            dates = [r[0] for r in con.execute(
                f"SELECT DISTINCT trim(CAST({datecol} AS VARCHAR)) FROM "
                f"read_csv('{rel}', strict_mode=false, header=true) "
                f"WHERE {datecol} IS NOT NULL ORDER BY 1").fetchall()]

            for d in dates:
                dest = tdir / f"{d}.parquet"
                if dest.exists() and not args.rebuild:
                    skipped += 1
                    continue
                we = endings.get(str(d), "lf")
                con.execute(f"""
                    COPY (
                      SELECT *{repl}, '{we}' AS wrote_with
                      FROM read_csv('{rel}', strict_mode=false, header=true)
                      WHERE trim(CAST({datecol} AS VARCHAR)) = '{d}'
                    ) TO '{dest.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """)
                written += 1

            total_csv += src.stat().st_size
            total_pq += sum(p.stat().st_size for p in tdir.glob("*.parquet"))
            print(f"  {src.name:28s} {len(dates):3d} day partitions -> {tdir}/")

    for name in WHOLE:
        src = hist / name
        if not src.exists():
            continue
        tdir = out / src.stem
        tdir.mkdir(parents=True, exist_ok=True)
        dest = tdir / "all.parquet"
        if dest.exists() and not args.rebuild:
            skipped += 1
            continue
        con.execute(f"""COPY (SELECT * FROM read_csv('{src.as_posix()}',
                      strict_mode=false, header=true, all_varchar=true))
                      TO '{dest.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
        written += 1
        print(f"  {name:28s} whole file -> {dest}")

    print(f"\n{written} partitions written, {skipped} already present")
    if total_csv and total_pq:
        print(f"source CSV {total_csv/1e6:.2f} MB -> parquet {total_pq/1e6:.2f} MB "
              f"({total_csv/total_pq:.1f}x)")
    print(f"\nnote: {CR_BOUNDARY_NOTE}")
    print("      every row carries wrote_with = 'crlf' | 'lf' recording which.")
    print("\nquery it:")
    print(f"  SELECT * FROM read_parquet('{(out/'rate_changes'/'*.parquet').as_posix()}')")


if __name__ == "__main__":
    main()
