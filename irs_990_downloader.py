"""Discover and download IRS Form 990 bulk XML archives.

The IRS page changes as new monthly archives are published, so links are
discovered from the official download page instead of being hard-coded.
Downloads use ``.part`` files and HTTP Range requests when the server supports
them, allowing an interrupted run to be restarted safely.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PAGE_URL = "https://www.irs.gov/charities-non-profits/form-990-series-downloads"
ALLOWED_HOST = "apps.irs.gov"
LINK_RE = re.compile(r"/pub/epostcard/990/xml/(?P<year>\d{4})/", re.I)
CHUNK_SIZE = 1024 * 1024


@dataclass(frozen=True)
class Download:
    year: int
    url: str

    @property
    def filename(self) -> str:
        return Path(urlparse(self.url).path).name


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.links.append(value)


def discover_downloads(html: str, years: set[int]) -> list[Download]:
    """Return unique IRS ZIP/CSV links for the requested publication years."""
    found: dict[str, Download] = {}
    parser = LinkParser()
    parser.feed(html)
    for href in parser.links:
        url = urljoin(PAGE_URL, href)
        parsed = urlparse(url)
        match = LINK_RE.search(parsed.path)
        if parsed.hostname != ALLOWED_HOST or not match:
            continue
        year = int(match.group("year"))
        if year not in years or Path(parsed.path).suffix.lower() not in {".zip", ".csv"}:
            continue
        found[url] = Download(year, url)
    return sorted(found.values(), key=lambda item: (item.year, item.filename))


def fetch_catalog(timeout: int) -> str:
    request = Request(PAGE_URL, headers={"User-Agent": "irs-990-bulk-downloader/1.0 (public IRS data)"})
    with urlopen(request, timeout=timeout) as response:
        return response.read().decode(response.headers.get_content_charset() or "utf-8")


def validate_file(path: Path) -> None:
    if path.suffix.lower() != ".zip":
        if path.stat().st_size == 0:
            raise ValueError("empty file")
        return
    with zipfile.ZipFile(path) as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise ValueError(f"corrupt ZIP member: {bad_member}")


def download_one(
    item: Download,
    output: Path,
    timeout: int,
    retries: int,
    verify: bool,
) -> dict[str, object]:
    year_dir = output / str(item.year)
    year_dir.mkdir(parents=True, exist_ok=True)
    destination = year_dir / item.filename
    partial = destination.with_suffix(destination.suffix + ".part")

    if destination.exists():
        if verify:
            validate_file(destination)
        return {"file": str(destination), "status": "existing", "bytes": destination.stat().st_size}

    for attempt in range(retries + 1):
        try:
            offset = partial.stat().st_size if partial.exists() else 0
            headers = {"User-Agent": "irs-990-bulk-downloader/1.0 (public IRS data)"}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            request = Request(item.url, headers=headers)
            with urlopen(request, timeout=timeout) as response:
                status = response.getcode()
                if offset and status == 206:
                    mode = "ab"
                elif status == 200:
                    mode = "wb"
                else:
                    raise HTTPError(item.url, status, f"unexpected HTTP status {status}", response.headers, None)
                with partial.open(mode) as handle:
                    while chunk := response.read(CHUNK_SIZE):
                        if chunk:
                            handle.write(chunk)
            if verify:
                validate_file(partial)
            os.replace(partial, destination)
            return {"file": str(destination), "status": "downloaded", "bytes": destination.stat().st_size}
        except (OSError, HTTPError, URLError, zipfile.BadZipFile, ValueError):
            if attempt == retries:
                raise
            time.sleep(min(2**attempt, 30))
    raise AssertionError("unreachable")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", nargs="+", type=int, default=[2024, 2025, 2026])
    parser.add_argument("--output", type=Path, default=Path("data/irs-990"))
    parser.add_argument("--workers", type=int, default=3, help="simultaneous downloads")
    parser.add_argument("--timeout", type=int, default=120, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true", help="list links without downloading")
    parser.add_argument("--no-verify", action="store_true", help="skip ZIP integrity checks")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    years = set(args.years)
    if not years or any(year < 2016 or year > 2100 for year in years):
        raise SystemExit("Years must be between 2016 and 2100")
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")

    downloads = discover_downloads(fetch_catalog(args.timeout), years)
    if not downloads:
        raise SystemExit("No matching IRS files were found")

    print(f"Found {len(downloads)} files for {', '.join(map(str, sorted(years)))}")
    if args.dry_run:
        for item in downloads:
            print(item.url)
        return 0

    args.output.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {
        "source": PAGE_URL,
        "years": sorted(years),
        "files": [asdict(item) | {"filename": item.filename} for item in downloads],
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    failed = False
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        future_map = {
            pool.submit(download_one, item, args.output, args.timeout, args.retries, not args.no_verify): item
            for item in downloads
        }
        for future in concurrent.futures.as_completed(future_map):
            item = future_map[future]
            try:
                result = future.result()
                mib = int(result["bytes"]) / (1024 * 1024)
                print(f"[{result['status']}] {item.year}/{item.filename} ({mib:.1f} MiB)")
            except Exception as exc:
                failed = True
                print(f"[failed] {item.url}: {exc}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
