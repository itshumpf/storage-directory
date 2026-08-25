#!/usr/bin/env python3
"""backfill_brand.py — Add the operator column to history/rate_changes.csv.

Run once, from the repo root:

    python analysis/backfill_brand.py --dry-run     # report only, writes nothing
    python analysis/backfill_brand.py               # writes

WHY THIS EXISTS
---------------
Every row in the rate log was produced by one operator, so until now the
operator did not need naming. A second chain (Extra Space) is about to write
into the same file. The moment it does, "no brand column" stops meaning "these
are all the same chain" and starts meaning nothing at all -- and the two cannot
be separated afterwards, because store_id ranges are not guaranteed disjoint.

So the column is added BEFORE the first foreign row lands, and existing rows
are stamped explicitly rather than left to a "blank means the old one"
convention. A blank field is indistinguishable from a tag that failed to
write; an explicit value is not.

HOW IT WRITES
-------------
By appending ",<brand>" to each line as text, NOT by parsing and re-emitting
CSV. Round-tripping through the csv module can legitimately change the quoting
of fields it did not touch (a promo string containing a comma or a quote), and
that would show up as a whole-file diff in which the real change is invisible.
Appending to the raw line leaves every existing byte alone.

LINE ENDINGS ARE MIXED HERE, AND THAT IS CORRECT
------------------------------------------------
As of 2026-08-24 this log holds 75,437 CRLF rows (the July rows) and 419,506
LF rows. That is the documented design, not damage: .gitattributes marks these
files `-text` precisely so committed bytes are never converted, and the writers
emit LF for new rows. See the comment block at the top of .gitattributes.

So each line's ending is detected and preserved individually -- the tag is
inserted before any trailing CR, never after it. An earlier version of this
script refused outright on seeing CRLF, which would have been the same mistake
as treating any anomaly as a defect before reading the record that explains it.

The one case that genuinely cannot be handled by line-append is a quoted field
containing an embedded newline, which naive splitting would tear in half. That
is checked for, and the script refuses if it finds one.

VERIFY AFTERWARDS
-----------------
    git diff --stat        one file changed
    git diff | head        header gains ",brand"; rows gain ",publicstorage"

Line count must be identical before and after. The script asserts this itself
and restores the original if it is not.
"""
import argparse
import csv
import os
import shutil
import sys
from pathlib import Path

LOG = Path("history/rate_changes.csv")
OLD_HEADER = ["date", "store_id", "site_number", "size", "sku",
              "field", "old", "new"]
NEW_HEADER = OLD_HEADER + ["brand"]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log", type=Path, default=LOG)
    ap.add_argument("--brand", default="publicstorage",
                    help="operator tag to stamp on existing rows")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change; write nothing")
    args = ap.parse_args()

    path = args.log
    if not path.exists():
        print(f"no rate log at {path}", file=sys.stderr)
        return 1

    raw = path.read_bytes()
    text = raw.decode("utf-8")

    lines = text.split("\n")
    trailing_blank = lines and lines[-1] == ""
    if trailing_blank:
        lines = lines[:-1]

    if not lines:
        print("REFUSING: file is empty.", file=sys.stderr)
        return 1

    def split_cr(line):
        """(payload, ending) -- the CR, if any, belongs to the ending."""
        return (line[:-1], "\r") if line.endswith("\r") else (line, "")

    header = next(csv.reader([split_cr(lines[0])[0]]))
    if header == NEW_HEADER:
        print(f"{path} already has the brand column -- nothing to do.")
        return 0
    if header != OLD_HEADER:
        print(f"REFUSING: unexpected header.\n  found:    {header}\n"
              f"  expected: {OLD_HEADER}", file=sys.stderr)
        return 1

    # A quoted field containing a newline would have been split above, so every
    # data line must parse on its own into exactly len(OLD_HEADER) fields.
    bad = []
    n_crlf = 0
    for i, line in enumerate(lines[1:], start=2):
        payload, ending = split_cr(line)
        if ending:
            n_crlf += 1
        try:
            fields = next(csv.reader([payload]))
        except csv.Error as e:
            bad.append((i, f"unparseable: {e}"))
            continue
        if len(fields) != len(OLD_HEADER):
            bad.append((i, f"{len(fields)} fields, expected {len(OLD_HEADER)}"))
    if bad:
        print(f"REFUSING: {len(bad):,} line(s) do not parse as single rows. "
              f"An embedded newline inside a quoted field would do this, and "
              f"appending text per line would corrupt them.", file=sys.stderr)
        for i, why in bad[:10]:
            print(f"  line {i}: {why}", file=sys.stderr)
        return 1

    n_rows = len(lines) - 1
    print(f"log      : {path}")
    print(f"rows     : {n_rows:,}")
    print(f"header   : {' -> '.join([','.join(OLD_HEADER), ','.join(NEW_HEADER)])}")
    print(f"stamping : {args.brand!r} on all {n_rows:,} existing rows")
    print(f"endings  : {n_crlf:,} CRLF / {n_rows - n_crlf:,} LF, each preserved")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 0

    # Insert before the CR so the tag lands inside the row, not after its ending.
    out = [split_cr(lines[0])[0] + ",brand" + split_cr(lines[0])[1]]
    for ln in lines[1:]:
        payload, ending = split_cr(ln)
        out.append(payload + "," + args.brand + ending)
    new_text = "\n".join(out) + ("\n" if trailing_blank else "")

    backup = path.with_suffix(path.suffix + ".prebrand")
    shutil.copy2(path, backup)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(new_text, encoding="utf-8", newline="")
    os.replace(tmp, path)

    # --- verify, and roll back if the shape changed ------------------------
    check = path.read_text(encoding="utf-8").split("\n")
    if check and check[-1] == "":
        check = check[:-1]
    ok = len(check) == len(lines)
    if ok:
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            ok = next(r) == NEW_HEADER and all(
                len(row) == len(NEW_HEADER) and row[-1] == args.brand for row in r)
    if not ok:
        shutil.copy2(backup, path)
        print("VERIFY FAILED -- original restored from "
              f"{backup.name}. Nothing changed.", file=sys.stderr)
        return 1

    print(f"\nwrote {n_rows:,} rows, line count unchanged.")
    print(f"backup: {backup}  (delete once `git diff --stat` looks right)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
