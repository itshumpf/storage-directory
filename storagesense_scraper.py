"""Conservative, resumable collector for Storage Sense's public rates.

This is a deliberately slow *daily snapshot* collector. It checks robots.txt
on every run, spaces every HTTP request (including the inventory POST),
persists cooldowns, and leaves a run report that says exactly what was and was
not refreshed.

CHANGES 2026-09-02
------------------
History, and three fixes about the run being able to describe its own health.

**Dated snapshots and a change log.** ``history/storagesense/YYYY-MM-DD.json``
records the facilities actually re-read on that date — not the whole rolling
file, which would assert that several hundred facilities were observed when
forty-five were. ``history/storagesense_rate_changes.csv`` logs price, street
price and promo changes, diffed against each facility's **own previous
observation** rather than against yesterday, with ``days_since_previous`` on
every row because that interval is variable by design.

1. **The refresh cycle is now checked against the catalog rather than assumed.**
   A daily budget and a refresh window only close if
   ``budget * (refresh_hours / 24) >= len(catalog)``. If they do not, the
   oldest-first queue grows every day and some facilities silently never come
   round — a shortfall that is invisible in the output because every record
   present still looks fine. The arithmetic is now computed, recorded on the
   report, and warned about in words. See ``_coverage``.

2. **A failure while fetching robots or the catalog exits cleanly.** It used to
   escape ``run()`` as a traceback after the report was written, so the caller
   saw a crash rather than the ``STOPPED:`` line and exit code 2 that every
   other stop path produces. A stop is a stop regardless of which exception
   type carried it.

3. **The report records the plan, not just the outcome.** ``due_total`` says how
   many facilities were eligible, against ``planned_facilities`` for how many
   the budget allowed. Those two being different is the thing that reveals a
   growing backlog, and previously only the second was written down.
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

from storagesense_parser import BRAND as BRAND_NAME, parse_catalog_html, parse_facility_html

CATALOG_URL = "https://www.storagesense.com/locations/"
ROBOTS_URL = "https://www.storagesense.com/robots.txt"
DEFAULT_OUTPUT = Path("history/storagesense_locations.json")
DEFAULT_STATE = Path("history/storagesense_state.json")
DEFAULT_REPORT = Path("history/storagesense_last_run.json")
SNAPSHOT_DIR = Path("history/storagesense")
CHANGE_LOG = Path("history/storagesense_rate_changes.csv")
CHANGE_HEADER = ["date", "brand", "store_id", "site_number", "size", "sku",
                 "field", "old", "new", "days_since_previous"]
AVAIL_LOG = Path("history/storagesense_availability.csv")
AVAIL_HEADER = ["date", "brand", "store_id", "site_number", "size", "sku",
                "transition", "last_price", "days_since_previous"]
MIN_FACILITIES = 250
DEFAULT_DELAY_SECONDS = 5.0
# Zero means every currently listed facility. Daily price history needs a
# complete same-day observation; politeness comes from pacing, not from
# carrying a facility's price into tomorrow.
DEFAULT_DAILY_BUDGET = 0
DEFAULT_REFRESH_HOURS = 0
DEFAULT_COOLDOWN_HOURS = 24
MAX_CONSECUTIVE_ERRORS = 3
USER_AGENT = "FindStorageResearch/1.0 (public advertised storage rates; contact: braeden@thekeenas.com)"


class CrawlStopped(RuntimeError):
    """A deliberate stop condition: do not retry by starting another run."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    return (value or _utc_now()).isoformat()


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _load_json(path: Path, default: object) -> object:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _save_atomic(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def _retry_after_seconds(value: str | None, now: datetime | None = None) -> int:
    """Parse Retry-After; an absent or bad value gets a conservative day."""
    if not value:
        return DEFAULT_COOLDOWN_HOURS * 3600
    try:
        return max(1, int(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(1, int((retry_at - (now or _utc_now())).total_seconds()))
        except (TypeError, ValueError, IndexError):
            return DEFAULT_COOLDOWN_HOURS * 3600


def _coverage(catalog_size: int, budget: int, refresh_hours: int) -> dict[str, object]:
    """Can this budget actually refresh this catalog inside the refresh window?

    A rolling collector has two numbers that have to agree and never did here:
    how many facilities exist, and how many the budget can visit before they
    are all due again. If capacity is short, `_select_due` returns a full
    budget every day forever, the backlog grows, and the *output stays
    plausible the whole time* — every record in it is real, some are just
    increasingly stale, and nothing says which.

    This is the same defect shape as a scraper whose regex stops matching:
    the failure is in what is absent, so nothing raises.
    """
    if budget <= 0:
        return {
            "catalog_facilities": catalog_size,
            "daily_budget": "all",
            "refresh_window_days": 1.0,
            "refreshes_per_window": catalog_size,
            "days_for_one_full_pass": 1.0,
            "cycle_closes": True,
        }
    cycle_days = refresh_hours / 24.0
    capacity = budget * cycle_days
    days_for_one_pass = (catalog_size / budget) if budget else float("inf")
    covered = capacity >= catalog_size
    out: dict[str, object] = {
        "catalog_facilities": catalog_size,
        "daily_budget": budget,
        "refresh_window_days": round(cycle_days, 2),
        "refreshes_per_window": round(capacity, 1),
        "days_for_one_full_pass": round(days_for_one_pass, 1),
        "cycle_closes": covered,
    }
    if not covered:
        shortfall = catalog_size - capacity
        out["warning"] = (
            f"{catalog_size} facilities cannot all be refreshed every "
            f"{cycle_days:.0f} days at {budget}/day — capacity is "
            f"{capacity:.0f}, short by {shortfall:.0f}. The oldest-first queue "
            f"will grow and some facilities will never come round. Raise "
            f"--budget to at least {int(catalog_size / cycle_days) + 1}, or "
            f"raise --refresh-hours to at least "
            f"{int(catalog_size / budget * 24) + 1}."
        )
    return out


def _write_snapshot(records: list[dict], day: str) -> tuple[Path, int]:
    """Record what was actually observed today, and only that.

    A rolling collector cannot write the daily snapshot the rest of this
    project writes. Public Storage and CubeSmart re-read every store every
    night, so a dated file of the whole estate is an honest statement about
    that date. Here only ~45 of several hundred facilities are refreshed, so
    dumping the whole rolling file each day would assert that four hundred
    facilities were observed when forty-five were — the same class of claim as
    a scraper reporting a clean run over data it never fetched.

    So the snapshot holds the facilities re-read on that date, nothing else.
    The union across dates reconstructs the history; no single file overstates
    what was seen.

    A second run on the same day MERGES rather than replaces, because the first
    run's facilities were genuinely observed on that date and dropping them
    would lose real observations.
    """
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SNAPSHOT_DIR / f"{day}.json"
    merged: dict[str, dict] = {}
    prior = _load_json(path, [])
    if isinstance(prior, list):
        for row in prior:
            if isinstance(row, dict) and row.get("site_number"):
                merged[str(row["site_number"])] = row
    for row in records:
        merged[str(row["site_number"])] = row
    _save_atomic(path, list(merged.values()))
    return path, len(merged)


def _unit_changes(previous: dict, current: dict, day: str) -> list[list[object]]:
    """Diff one facility against its OWN previous observation, not yesterday.

    The interval between two observations of the same facility is variable by
    design — a seven-day refresh window means most gaps are a week, a backlog
    makes them longer, and a re-run makes them zero. `days_since_previous` is
    therefore on every row.

    Without it every consumer silently assumes one day, which is the same
    mistake `analysis/update_rate_log.py` made until 2026-09-01: it read the
    baseline file's mtime, `shutil.copy` had reset that mtime to now, and every
    run reported a baseline age of 0.0 days regardless of the real gap.

    A unit with no price is skipped rather than logged as a change. Storage
    Sense reports no price for a rented unit, so `120 -> None -> 120` is a unit
    being occupied and released, not two repricings.
    """
    seen = _parse_time(previous.get("last_checked_at"))
    now = _parse_time(current.get("last_checked_at"))
    gap = round((now - seen).total_seconds() / 86400.0, 2) if seen and now else ""

    def priced(record: dict) -> dict[str, dict]:
        return {u["sku"]: u for u in record.get("units", [])
                if u.get("sku") and isinstance(u.get("price"), (int, float)) and u["price"] > 0}

    before, after = priced(previous), priced(current)
    rows: list[list[object]] = []
    for sku, unit in after.items():
        old = before.get(sku)
        if old is None:
            continue                        # first priced sighting is not a change
        for field in ("price", "street_price", "promo"):
            was, now_value = old.get(field), unit.get(field)
            if was != now_value:
                rows.append([day, current.get("brand", BRAND_NAME),
                             current.get("store_id", ""), current.get("site_number", ""),
                             unit.get("size", ""), sku, field, was, now_value, gap])
    return rows


def _availability_changes(previous: dict, current: dict, day: str) -> list[list[object]]:
    """Log when a size class starts or stops being bookable online.

    WHAT THIS IS NOT
    ----------------
    This is **not occupancy**, and must never be published as occupancy.

    Storage Sense advertises per *size class*, not per physical unit, and
    nowhere states how many units a class contains. A 5x10 going unbookable
    means the last one went — but the last of one and the last of forty look
    identical here, so no unit count and no occupancy percentage can be
    derived from it. What can be derived is the share of size classes a
    facility currently cannot sell, which is a real and comparable number as
    long as it is called that.

    Two more reasons the gap between this and occupancy is not small:

    - **Operators withhold inventory from the web on purpose.** A size can
      vanish because revenue management pulled it, not because it rented.
      Web availability is a marketing surface, not an inventory feed.
    - **The refresh window is a week**, so anything that rents and re-lists
      inside that window is invisible. Every transition here is "changed at
      some point in the last `days_since_previous` days".

    Three transitions are logged, all named for what was observed rather than
    for what it might mean: `listed` (a SKU that was not on the page before),
    `unpriced` (still listed, no bookable price), and `delisted` (gone from the
    page entirely). Whether Storage Sense removes a sold-out size or keeps it
    without a price is **not yet known** — the first two runs of this log
    settle it, which is why both are recorded separately instead of collapsed.
    """
    seen = _parse_time(previous.get("last_checked_at"))
    now = _parse_time(current.get("last_checked_at"))
    gap = round((now - seen).total_seconds() / 86400.0, 2) if seen and now else ""

    def by_sku(record: dict) -> dict[str, dict]:
        return {u["sku"]: u for u in record.get("units", []) if u.get("sku")}

    def bookable(unit: dict) -> bool:
        price = unit.get("price")
        return isinstance(price, (int, float)) and price > 0

    before, after = by_sku(previous), by_sku(current)
    if not before:
        return []                           # first sighting establishes, not changes

    brand = current.get("brand", BRAND_NAME)
    store_id = current.get("store_id", "")
    site = current.get("site_number", "")
    rows: list[list[object]] = []

    for sku, unit in after.items():
        old = before.get(sku)
        if old is None:
            rows.append([day, brand, store_id, site, unit.get("size", ""), sku,
                         "listed", unit.get("price", ""), gap])
        elif bookable(old) and not bookable(unit):
            rows.append([day, brand, store_id, site, unit.get("size", ""), sku,
                         "unpriced", old.get("price", ""), gap])
        elif bookable(unit) and not bookable(old):
            rows.append([day, brand, store_id, site, unit.get("size", ""), sku,
                         "repriced_from_unpriced", unit.get("price", ""), gap])

    for sku, old in before.items():
        if sku not in after:
            rows.append([day, brand, store_id, site, old.get("size", ""), sku,
                         "delisted", old.get("price", ""), gap])
    return rows


def _append_rows(path: Path, header: list[str], rows: list[list[object]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        if new_file:
            writer.writerow(header)
        writer.writerows(rows)


def _append_changes(rows: list[list[object]]) -> None:
    _append_rows(CHANGE_LOG, CHANGE_HEADER, rows)


class PoliteSession:
    """One-process request pacing with a durable 429/403 cooldown."""

    def __init__(self, state_path: Path, delay: float) -> None:
        if delay < 1:
            raise ValueError("delay must be at least one second")
        self.state_path = state_path
        self.state = _load_json(state_path, {})
        if not isinstance(self.state, dict):
            self.state = {}
        self.delay = delay
        self._last_request_started = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"})

    def _save_state(self) -> None:
        _save_atomic(self.state_path, self.state)

    def _check_cooldown(self) -> None:
        cooldown = _parse_time(self.state.get("cooldown_until"))
        if cooldown and cooldown > _utc_now():
            raise CrawlStopped(f"Persistent cooldown active until {cooldown.isoformat()}")

    def _pace(self) -> None:
        wait = self.delay - (time.monotonic() - self._last_request_started)
        if wait > 0:
            time.sleep(wait)
        self._last_request_started = time.monotonic()

    def _record_cooldown(self, response: requests.Response) -> None:
        until = _utc_now() + timedelta(seconds=_retry_after_seconds(response.headers.get("Retry-After")))
        self.state["cooldown_until"] = _iso(until)
        self.state["cooldown_reason"] = f"HTTP {response.status_code} from {response.url}"
        self._save_state()

    def request(self, method: str, url: str, **kwargs: object) -> requests.Response:
        self._check_cooldown()
        # Callers may give slow page loads a timeout explicitly.  Set the
        # conservative default here rather than passing timeout twice.
        kwargs.setdefault("timeout", 60)
        for attempt in range(3):
            self._pace()
            try:
                response = self.session.request(method, url, **kwargs)
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(30 * (2**attempt))
                continue
            if response.status_code in {403, 429}:
                self._record_cooldown(response)
                raise CrawlStopped(f"Storage Sense returned HTTP {response.status_code}; cooldown recorded and run stopped")
            if response.status_code >= 500 and attempt < 2:
                time.sleep(30 * (2**attempt))
                continue
            response.raise_for_status()
            return response
        raise RuntimeError("unreachable")

    def get(self, url: str, **kwargs: object) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: object) -> requests.Response:
        return self.request("POST", url, **kwargs)


def _get(session: object, url: str) -> str:
    response = session.get(url, timeout=60)  # type: ignore[attr-defined]
    response.raise_for_status()
    return response.text


def _robots_rules(text: str, user_agent: str) -> tuple[list[tuple[str, bool]], float | None]:
    """Return the most-specific applicable Allow/Disallow rules.

    ``urllib.robotparser`` incorrectly rejects Storage Sense's explicit Allow
    exception beneath its broader /wp-admin/ Disallow in the Python runtime
    used here.  This small parser applies the standard longest-path rule (with
    Allow winning ties) instead of silently treating a permitted endpoint as
    prohibited.
    """
    groups: list[tuple[list[str], list[tuple[str, bool]], float | None]] = []
    agents: list[str] = []
    rules: list[tuple[str, bool]] = []
    crawl_delay: float | None = None
    for raw_line in text.splitlines() + [""]:
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            if agents:
                groups.append((agents, rules, crawl_delay))
            agents, rules, crawl_delay = [], [], None
            continue
        if ":" not in line:
            continue
        field, value = (part.strip() for part in line.split(":", 1))
        field = field.lower()
        if field == "user-agent":
            if rules:
                groups.append((agents, rules, crawl_delay))
                agents, rules, crawl_delay = [], [], None
            agents.append(value.lower())
        elif field in {"allow", "disallow"} and agents and value:
            rules.append((value, field == "allow"))
        elif field == "crawl-delay" and agents:
            try:
                crawl_delay = float(value)
            except ValueError:
                pass
    user_agent = user_agent.lower()
    specific = [group for group in groups if any(agent != "*" and agent in user_agent for agent in group[0])]
    matched = specific or [group for group in groups if "*" in group[0]]
    return ([(path, allow) for _, group_rules, _ in matched for path, allow in group_rules],
            next((delay for _, _, delay in matched if delay is not None), None))


def _robots_can_fetch(text: str, user_agent: str, url: str) -> tuple[bool, float | None]:
    rules, crawl_delay = _robots_rules(text, user_agent)
    path = urlsplit(url).path or "/"
    matches = [(len(rule_path), allowed) for rule_path, allowed in rules if path.startswith(rule_path)]
    if not matches:
        return True, crawl_delay
    longest = max(length for length, _ in matches)
    # An Allow rule wins if it ties a Disallow rule at the most-specific path.
    return any(allowed for length, allowed in matches if length == longest), crawl_delay


def check_robots(session: object) -> dict[str, object]:
    """Fetch and enforce current robots rules for both public endpoints."""
    text = _get(session, ROBOTS_URL)
    required = (CATALOG_URL, urljoin(CATALOG_URL, "/wp-admin/admin-ajax.php"))
    permissions = [_robots_can_fetch(text, USER_AGENT, url) for url in required]
    blocked = [url for url, (allowed, _) in zip(required, permissions) if not allowed]
    if blocked:
        raise CrawlStopped(f"robots.txt disallows required public endpoint(s): {', '.join(blocked)}")
    return {"robots_url": ROBOTS_URL, "checked_at": _iso(), "crawl_delay": permissions[0][1]}


def fetch_rendered_facility(session: object, url: str) -> str:
    """Fetch the public Candee unit fragment a normal browser loads for a facility."""
    shell = _get(session, url)
    soup = BeautifulSoup(shell, "html.parser")
    setup = next((s.get_text() for s in soup.find_all("script") if "candee_ajax_load_template" in (s.get_text() or "")), None)
    if not setup:
        raise KeyError("Facility shell has no public Candee unit-template request")
    encoded = re.search(r"atob\('([^']+)'\)", setup)
    theme = re.search(r"'theme'\s*:\s*'([^']+)'", setup)
    ajax_url = re.search(r'var ajaxurl\s*=\s*"([^"]+)"', shell)
    if not encoded or not theme or not ajax_url:
        raise ValueError("Candee unit-template request changed shape")
    try:
        query_vars = json.loads(base64.b64decode(encoded.group(1)))
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Could not decode public Candee query_vars") from exc
    form = {"action": "candee_ajax_load_template", "current_url": url, "ajax_data[theme]": theme.group(1)}
    for key, value in query_vars.items():
        form[f"query_vars[{key}]"] = value
    response = session.post(ajax_url.group(1).replace("\\/", "/"), data=form, timeout=60)  # type: ignore[attr-defined]
    response.raise_for_status()
    if "unitsTable" not in response.text or "ItemList" not in response.text:
        raise ValueError("Public Candee unit response did not contain unit cards and JSON-LD")
    return response.text


def fetch_catalog(session: object) -> list[dict]:
    rows = parse_catalog_html(_get(session, CATALOG_URL))
    if len(rows) < MIN_FACILITIES:
        raise ValueError(f"Catalog returned only {len(rows)} facilities; expected at least {MIN_FACILITIES}")
    return rows


def _select_due(catalog: list[dict], existing: dict[str, dict], budget: int,
                refresh_hours: int, now: datetime) -> tuple[list[dict], int]:
    """Oldest-first slice of what is due, plus how many were due in total.

    Returning the total matters: ``len(selected)`` alone cannot distinguish
    "everything due was refreshed" from "the budget capped a backlog", and
    those are different states of the collector.
    """
    cutoff = now - timedelta(hours=refresh_hours)

    def last_checked(store: dict) -> datetime:
        value = _parse_time(existing.get(store["site_number"], {}).get("last_checked_at"))
        return value or datetime.min.replace(tzinfo=timezone.utc)

    due = [store for store in catalog if last_checked(store) <= cutoff]
    ordered = sorted(due, key=lambda store: (last_checked(store), store["site_number"]))
    return (ordered if budget <= 0 else ordered[:budget]), len(due)


def run(output: Path, state_path: Path, report_path: Path, delay: float,
        budget: int = DEFAULT_DAILY_BUDGET, refresh_hours: int = DEFAULT_REFRESH_HOURS,
        limit: int = 0) -> dict[str, object]:
    """Run one full daily snapshot and always write a machine-readable report."""
    started = _utc_now()
    old_rows = _load_json(output, [])
    existing = {str(row.get("site_number")): row for row in old_rows if isinstance(row, dict)} if isinstance(old_rows, list) else {}
    report: dict[str, object] = {"started_at": _iso(started), "policy": {
        "min_seconds_between_requests": delay, "daily_facility_budget": budget, "refresh_hours": refresh_hours},
        "succeeded": [], "failures": []}
    session = PoliteSession(state_path, delay)
    try:
        robots = check_robots(session)
        report["robots"] = robots
        declared = robots.get("crawl_delay")
        if isinstance(declared, (int, float)) and declared > session.delay:
            session.delay = float(declared)
            report["policy"] = {**report["policy"], "min_seconds_between_requests": session.delay}
        catalog = fetch_catalog(session)
        report["catalog_facilities"] = len(catalog)

        # Does the schedule actually cover the catalog? Recorded every run so a
        # shortfall is visible in the report rather than inferred later from
        # stale timestamps.
        coverage = _coverage(len(catalog), budget, refresh_hours)
        report["coverage"] = coverage
        if not coverage["cycle_closes"]:
            print(f"WARNING: {coverage['warning']}")

        if limit:
            todo, due_total = catalog[:limit], len(catalog)
        else:
            todo, due_total = _select_due(catalog, existing, budget, refresh_hours, started)
        report["planned_facilities"] = len(todo)
        report["due_total"] = due_total
        if due_total > len(todo):
            report["backlog"] = due_total - len(todo)
            print(f"NOTE: {due_total} facilities are due, budget allows {len(todo)}; "
                  f"{due_total - len(todo)} carried to the next run")

        day = started.date().isoformat()
        observed: list[dict] = []
        changes: list[list[object]] = []
        availability: list[list[object]] = []

        consecutive_errors = 0
        for index, store in enumerate(todo, 1):
            site = store["site_number"]
            try:
                record = parse_facility_html(fetch_rendered_facility(session, store["url"]), store)
                record["last_checked_at"] = _iso()
                # Diff against this facility's own previous observation BEFORE
                # it is replaced — the prior record is only in hand here.
                prior = existing.get(site)
                if prior:
                    changes.extend(_unit_changes(prior, record, day))
                    availability.extend(_availability_changes(prior, record, day))
                existing[site] = record
                observed.append(record)
                report["succeeded"].append(site)  # type: ignore[index]
                consecutive_errors = 0
                print(f"[{index}/{len(todo)}] {site} {store['city']}, {store['state']}: {len(record['units'])} unit classes")
            except CrawlStopped as exc:
                report["stopped"] = str(exc)
                break
            except Exception as exc:
                consecutive_errors += 1
                report["failures"].append({"site_number": site, "error": str(exc)})  # type: ignore[index]
                print(f"[{index}/{len(todo)}] {site}: {exc}; retained prior record if present")
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    report["stopped"] = f"Stopped after {MAX_CONSECUTIVE_ERRORS} consecutive unexpected errors"
                    break
        # Written even on a stop: the facilities read before the stop were
        # genuinely observed, and discarding them to keep the bookkeeping tidy
        # would throw away real data to report a cleaner failure.
        _save_atomic(output, list(existing.values()))
        if observed:
            snapshot_path, snapshot_count = _write_snapshot(observed, day)
            report["snapshot"] = {"path": str(snapshot_path),
                                  "observed_this_run": len(observed),
                                  "facilities_in_file": snapshot_count}
        _append_changes(changes)
        _append_rows(AVAIL_LOG, AVAIL_HEADER, availability)
        report["changes_logged"] = len(changes)
        report["availability_transitions"] = len(availability)
        if availability:
            kinds: dict[str, int] = {}
            for row in availability:
                kinds[row[6]] = kinds.get(row[6], 0) + 1  # type: ignore[index]
            summary = ", ".join(f"{count} {kind}" for kind, count in sorted(kinds.items()))
            print(f"  availability: {summary} -> {AVAIL_LOG}")
        if changes:
            print(f"  {len(changes)} price/promo changes -> {CHANGE_LOG}")
        elif observed:
            # A measured zero, said out loud, so it can never later be mistaken
            # for a run that did not diff.
            print(f"  0 changes across {len(observed)} facilities re-read "
                  f"(measured, not missing)")
    except CrawlStopped as exc:
        report["stopped"] = str(exc)
    except Exception as exc:
        # A robots or catalog failure is still a stop, and the caller should see
        # the same STOPPED line and exit code as every other stop rather than a
        # traceback. The type is kept so the report says what actually broke.
        report["stopped"] = f"{type(exc).__name__}: {exc}"
        report["stopped_before_any_facility"] = True
    finally:
        report["finished_at"] = _iso()
        report["stored_facilities"] = len(existing)
        _save_atomic(report_path, report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--budget", type=int, default=DEFAULT_DAILY_BUDGET,
                        help="Maximum facilities; 0 means every facility (the daily default)")
    parser.add_argument("--refresh-hours", type=int, default=DEFAULT_REFRESH_HOURS)
    parser.add_argument("--limit", type=int, default=0, help="Test-only override: fetch this many facilities")
    args = parser.parse_args()
    outcome = run(args.out, args.state, args.report, args.delay, args.budget, args.refresh_hours, args.limit)
    if outcome.get("stopped"):
        print(f"STOPPED: {outcome['stopped']}")
        raise SystemExit(2)
