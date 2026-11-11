"""Capture every public regional market order into one compressed SQLite per region."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import email.utils
import json
import math
import os
import random
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import zstandard


ESI = "https://esi.evetech.net/latest"
USER_AGENT = "Lena-EOU-Market-Orders/0.1 (+https://github.com/lena-moreau/lena-eou)"
COMPATIBILITY_DATE = "2020-01-01"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
WORKER_COUNT = 4
SNAPSHOT_RETRY_DELAY_SECONDS = 2.5
FINAL_SNAPSHOT_RETRY_DELAY_SECONDS = 300.0
MAX_SNAPSHOT_ATTEMPTS = 4
GITHUB_FILE_LIMIT_BYTES = 100_000_000
INSERT_BATCH_SIZE = 5000
ROOT = Path(__file__).resolve().parents[1]
REGION_DB = ROOT / "data" / "esi" / "uni" / "regions.db"
OUTPUT_DIR = ROOT / "data" / "esi" / "mkt"
HEADERS_PATH = OUTPUT_DIR / "headers.json"


class CaptureError(RuntimeError):
    """The full regional market snapshot could not be safely collected."""


class SnapshotError(CaptureError):
    """Page metadata changed during one region's capture."""

    def __init__(
        self,
        message: str,
        retry_at: dt.datetime | None = None,
        unchanged: bool = False,
    ):
        super().__init__(message)
        self.retry_at = retry_at
        self.unchanged = unchanged


@dataclass(frozen=True)
class PageResult:
    orders: list[dict[str, Any]]
    headers: dict[str, str]
    page_count: int
    received_at: dt.datetime
    status_code: int


@dataclass(frozen=True)
class RegionSeed:
    region_id: int
    page_count: int
    expected_last_modified_gmt: str | None = None
    has_existing_capture: bool = False
    not_before_utc: str | None = None
    existing_order_count: int = 0


@dataclass
class RegionResult:
    region_id: int
    last_modified: str
    last_modified_gmt: str
    last_page: int
    order_count: int
    compressed_path: Path
    next_request_at_utc: str


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def github_escape(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def report_github_error(title: str, message: str) -> None:
    """Make a failure visible as both a log line and an Actions annotation."""
    safe_title = github_escape(title).replace(":", "%3A").replace(",", "%2C")
    print(f"::error title={safe_title}::{github_escape(message)}", flush=True)


def parse_http_date(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def format_http_date_gmt(value: dt.datetime) -> str:
    return email.utils.format_datetime(value.astimezone(dt.timezone.utc).replace(microsecond=0), usegmt=True)


def last_modified_gmt_from_iso(value: str) -> str | None:
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return format_http_date_gmt(parsed)


def retry_delay(headers: Any, attempt: int, status: int | None = None) -> float:
    retry_after = headers.get("Retry-After") if headers else None
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            retry_at = parse_http_date(retry_after)
            if retry_at:
                return max(0.0, (retry_at - utc_now()).total_seconds())
    if status == 420 and headers:
        try:
            return max(1.0, float(headers.get("X-ESI-Error-Limit-Reset", "1")))
        except (TypeError, ValueError):
            pass
    return min(30.0, 2**attempt + random.random())


def lower_headers(headers: Any) -> dict[str, str]:
    return {key.lower(): value for key, value in headers.items()}


def regions_from_database(path: Path = REGION_DB) -> list[int]:
    if not path.is_file():
        raise CaptureError(f"No existe {path.relative_to(ROOT)}; ejecuta primero el workflow de regiones ESI.")
    try:
        with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='regions'"
            ).fetchone()
            if not table:
                raise CaptureError(f"{path.relative_to(ROOT)} no contiene la tabla regions.")
            ids = [row[0] for row in connection.execute("SELECT region_id FROM regions ORDER BY region_id")]
    except sqlite3.Error as exc:
        raise CaptureError(f"No se pudo leer {path.relative_to(ROOT)}: {exc}") from exc
    if not ids or any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in ids):
        raise CaptureError("La base de regiones contiene IDs inválidos o está vacía.")
    if len(ids) != len(set(ids)):
        raise CaptureError("La base de regiones contiene IDs duplicados.")
    return ids


def orders_url(region_id: int, page: int) -> str:
    query = urllib.parse.urlencode({"order_type": "all", "page": page})
    return f"{ESI}/markets/{region_id}/orders/?{query}"


def fetch_page(region_id: int, page: int) -> PageResult:
    request = urllib.request.Request(
        orders_url(region_id, page),
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "X-Compatibility-Date": COMPATIBILITY_DATE,
        },
    )
    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                received_at = dt.datetime.now(dt.timezone.utc)
                headers = lower_headers(response.headers)
                try:
                    payload = json.loads(response.read())
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    raise CaptureError(f"Región {region_id}, página {page}: ESI devolvió JSON inválido") from exc
                if not isinstance(payload, list) or any(not isinstance(order, dict) for order in payload):
                    raise CaptureError(f"Región {region_id}, página {page}: respuesta JSON no válida")
                try:
                    page_count = int(headers["x-pages"])
                except (KeyError, ValueError) as exc:
                    raise CaptureError(f"Región {region_id}, página {page}: X-Pages inválido o ausente") from exc
                if page_count < 1 or page > page_count:
                    raise SnapshotError(f"Región {region_id}: página {page} fuera del X-Pages={page_count}")
                return PageResult(payload, headers, page_count, received_at, response.status)
        except urllib.error.HTTPError as exc:
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"Región {region_id}, página {page}: HTTP {exc.code}: {detail}") from exc
            delay = retry_delay(exc.headers, attempt, exc.code)
            rate_remaining = exc.headers.get("X-ESI-Error-Limit-Remain", "unknown")
            rate_reset = exc.headers.get("X-ESI-Error-Limit-Reset", "unknown")
            retry_after = exc.headers.get("Retry-After", "not provided")
            message = (
                f"Región {region_id}, página {page}: HTTP {exc.code}; "
                f"reintento {attempt + 1}/{MAX_RETRIES}, espera {delay:.1f}s; "
                f"ESI error limit remaining={rate_remaining}, reset={rate_reset}s, "
                f"Retry-After={retry_after}."
            )
            print(f"::warning::{github_escape(message)}", flush=True)
            print(f"  {message}", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Región {region_id}, página {page}: error de red: {exc}") from exc
            delay = min(30.0, 2**attempt + random.random())
            message = (
                f"Región {region_id}, página {page}: error de red ({exc}); "
                f"reintento {attempt + 1}/{MAX_RETRIES}, espera {delay:.1f}s."
            )
            print(f"::warning::{github_escape(message)}", flush=True)
            print(f"  {message}", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def read_compressed_capture(path: Path, region_id: int, temporary_dir: Path) -> tuple[str, int, str, int] | None:
    plain_path = temporary_dir / f"cached-{region_id}.db"
    try:
        with path.open("rb") as source, plain_path.open("wb") as output:
            zstandard.ZstdDecompressor().copy_stream(source, output)
        with sqlite3.connect(f"{plain_path.resolve().as_uri()}?mode=ro", uri=True) as connection:
            capture = connection.execute(
                "SELECT region_id, last_modified, order_count FROM capture"
            ).fetchone()
            cache = connection.execute(
                "SELECT next_request_at_utc, page_count FROM esi_cache WHERE region_id = ?", (region_id,)
            ).fetchone()
        if not capture or not cache or capture[0] != region_id:
            return None
        return capture[1], capture[2], cache[0], cache[1]
    except (OSError, sqlite3.Error, zstandard.ZstdError):
        return None
    finally:
        plain_path.unlink(missing_ok=True)


def plan_region_work(
    region_ids: list[int], temporary_dir: Path, force: bool
) -> tuple[dict[int, RegionSeed], list[RegionResult], dict[str, Any]]:
    try:
        previous = json.loads(HEADERS_PATH.read_text(encoding="utf-8"))
        previous_regions = previous.get("regions", {}) if isinstance(previous, dict) else {}
    except (OSError, json.JSONDecodeError):
        previous_regions = {}
    if not isinstance(previous_regions, dict):
        previous_regions = {}

    seeds: dict[int, RegionSeed] = {}
    cached_results: list[RegionResult] = []
    now = utc_now()
    for region_id in region_ids:
        cached = previous_regions.get(str(region_id)) if isinstance(previous_regions, dict) else None
        compressed_path = OUTPUT_DIR / f"{region_id}.db.zst"
        state = None
        expires_at = None
        try:
            state = read_compressed_capture(compressed_path, region_id, temporary_dir) if compressed_path.is_file() else None
            if state:
                last_modified, order_count, next_request_text, page_count = state
                expires_at = dt.datetime.fromisoformat(next_request_text.replace("Z", "+00:00"))
                cache_matches_headers = (
                    isinstance(cached, dict)
                    and cached.get("last_modified") == last_modified
                    and cached.get("last_page") == page_count
                )
                if (
                    cache_matches_headers
                    and expires_at > now
                    and not force
                ):
                    raw_last_modified = cached.get("last_modified_gmt")
                    if not isinstance(raw_last_modified, str):
                        raw_last_modified = last_modified_gmt_from_iso(last_modified)
                    if raw_last_modified is None:
                        raise ValueError(f"Last-Modified inválido en headers.json para región {region_id}")
                    cached_results.append(
                        RegionResult(
                            region_id,
                            last_modified,
                            raw_last_modified,
                            page_count,
                            order_count,
                            compressed_path,
                            iso_utc(expires_at),
                        )
                    )
                    print(
                        f"Región {region_id}: reutilizo captura hasta {iso_utc(expires_at)} según Expires de ESI.",
                        flush=True,
                    )
                    continue
        except (KeyError, TypeError, ValueError):
            state = None
            expires_at = None
        estimated_pages = cached.get("last_page") if isinstance(cached, dict) else None
        if isinstance(estimated_pages, bool) or not isinstance(estimated_pages, int) or estimated_pages < 1:
            estimated_pages = 1
            estimate_source = "sin entrada válida en headers.json; se usa 1"
        else:
            estimate_source = "headers.json"
        expected_last_modified_gmt = cached.get("last_modified_gmt") if isinstance(cached, dict) else None
        if not isinstance(expected_last_modified_gmt, str) and isinstance(cached, dict):
            previous_last_modified = cached.get("last_modified")
            if isinstance(previous_last_modified, str):
                expected_last_modified_gmt = last_modified_gmt_from_iso(previous_last_modified)
        if not isinstance(expected_last_modified_gmt, str):
            expected_last_modified_gmt = None
        seeds[region_id] = RegionSeed(
            region_id,
            estimated_pages,
            expected_last_modified_gmt,
            has_existing_capture=state is not None,
            not_before_utc=iso_utc(expires_at) if expires_at and expires_at > now else None,
            existing_order_count=state[1] if state else 0,
        )
        print(
            f"Región {region_id}: estimación de {estimated_pages} páginas ({estimate_source}); "
            "la página 1 se pedirá al iniciar su captura.",
            flush=True,
        )

    return seeds, cached_results, previous_regions if isinstance(previous_regions, dict) else {}


def assign_regions(seeds: dict[int, RegionSeed]) -> list[list[RegionSeed]]:
    """Reserve the largest region for worker 1; LPT-balance the rest over workers 2–4."""
    assignments: list[list[RegionSeed]] = [[] for _ in range(WORKER_COUNT)]
    loads = [0] * WORKER_COUNT
    ordered = sorted(seeds.values(), key=lambda item: (-item.page_count, item.region_id))
    if not ordered:
        return assignments

    largest = ordered[0]
    assignments[0].append(largest)
    loads[0] = largest.page_count
    print(
        f"Asignación fija: worker 1 recibe solo la región {largest.region_id} "
        f"({largest.page_count} páginas estimadas).",
        flush=True,
    )

    for seed in ordered[1:]:
        worker = min(range(1, WORKER_COUNT), key=lambda index: (loads[index], index))
        assignments[worker].append(seed)
        loads[worker] += seed.page_count
    for worker, (regions, pages) in enumerate(zip(assignments, loads), start=1):
        region_order = ", ".join(f"{seed.region_id}(~{seed.page_count})" for seed in regions) or "ninguna"
        print(
            f"Worker {worker}: {len(regions)} regiones, {pages} páginas estimadas para reparto; "
            f"orden por estimación: {region_order}.",
            flush=True,
        )
    return assignments


def int_field(order: dict[str, Any], field: str, region_id: int) -> int:
    value = order.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CaptureError(f"Región {region_id}: {field} inválido en orden {order.get('order_id')!r}")
    return value


def normalize_order(order: dict[str, Any], region_id: int) -> tuple[Any, ...]:
    order_id = int_field(order, "order_id", region_id)
    if order_id <= 0:
        raise CaptureError(f"Región {region_id}: order_id debe ser positivo")
    integer_fields = ("type_id", "location_id", "system_id", "volume_remain", "volume_total", "min_volume", "duration")
    values = {field: int_field(order, field, region_id) for field in integer_fields}
    if any(value < 0 for value in values.values()):
        raise CaptureError(f"Región {region_id}: valor negativo en orden {order_id}")
    is_buy_order = order.get("is_buy_order")
    if not isinstance(is_buy_order, bool):
        raise CaptureError(f"Región {region_id}: is_buy_order inválido en orden {order_id}")
    price = order.get("price")
    if isinstance(price, bool) or not isinstance(price, (int, float)) or not math.isfinite(price) or price <= 0:
        raise CaptureError(f"Región {region_id}: price inválido en orden {order_id}")
    issued_text = order.get("issued")
    if not isinstance(issued_text, str):
        raise CaptureError(f"Región {region_id}: issued inválido en orden {order_id}")
    try:
        issued = dt.datetime.fromisoformat(issued_text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CaptureError(f"Región {region_id}: issued inválido en orden {order_id}") from exc
    if issued.tzinfo is None:
        raise CaptureError(f"Región {region_id}: issued sin zona horaria en orden {order_id}")
    order_range = order.get("range")
    if not isinstance(order_range, str):
        raise CaptureError(f"Región {region_id}: range inválido en orden {order_id}")
    return (
        order_id,
        values["type_id"],
        values["location_id"],
        values["system_id"],
        float(price),
        values["volume_remain"],
        values["volume_total"],
        values["min_volume"],
        int(is_buy_order),
        issued_text,
        values["duration"],
        order_range,
    )


def snapshot_retry_at(pages: list[PageResult]) -> dt.datetime:
    """Return the latest ESI page expiry, with a five-minute conservative fallback."""
    expiries = [parse_http_date(page.headers.get("expires")) for page in pages]
    if expiries and all(expiry is not None for expiry in expiries):
        return max(expiry for expiry in expiries if expiry is not None) + dt.timedelta(seconds=1)
    latest_response = max((page.received_at for page in pages), default=dt.datetime.now(dt.timezone.utc))
    return latest_response + dt.timedelta(seconds=FINAL_SNAPSHOT_RETRY_DELAY_SECONDS)


def region_cache_deadline(pages: list[PageResult]) -> dt.datetime:
    expiries = [parse_http_date(page.headers.get("expires")) for page in pages]
    if all(value is not None for value in expiries):
        return max(value for value in expiries if value is not None)
    latest_response = max((page.received_at for page in pages), default=utc_now())
    return latest_response + dt.timedelta(seconds=3600)


def fetch_region_orders(
    seed: RegionSeed,
    force: bool,
) -> tuple[dict[int, tuple[Any, ...]], str, str, str, int]:
    first_page = fetch_page(seed.region_id, 1)
    page_count = first_page.page_count
    print(
        f"Región {seed.region_id}: página 1 recibida; X-Pages real de ESI={page_count}.",
        flush=True,
    )

    captured_pages = [first_page]
    first_last_modified_gmt = first_page.headers.get("last-modified")
    baseline_date = parse_http_date(first_last_modified_gmt)
    if baseline_date is None:
        diagnostic_header_names = (
            "last-modified",
            "x-pages",
            "expires",
            "etag",
            "date",
            "cache-control",
            "content-type",
            "x-compatibility-date",
            "x-esi-cache-status",
            "x-esi-request-id",
        )
        response_headers = {
            name: first_page.headers.get(name) for name in diagnostic_header_names
        }
        raise SnapshotError(
            f"Región {seed.region_id}: página 1, HTTP {first_page.status_code}; "
            f"Last-Modified esperado (captura previa)="
            f"{json.dumps(seed.expected_last_modified_gmt, ensure_ascii=False)}, "
            f"recibido={json.dumps(first_last_modified_gmt, ensure_ascii=False)}; "
            f"cabeceras relevantes={json.dumps(response_headers, ensure_ascii=False, sort_keys=True)}",
            snapshot_retry_at(captured_pages),
        )
    baseline_modified = iso_utc(baseline_date)
    previous_date = parse_http_date(seed.expected_last_modified_gmt)
    if (
        not force
        and seed.has_existing_capture
        and previous_date is not None
        and iso_utc(previous_date) == baseline_modified
    ):
        raise SnapshotError(
            f"Región {seed.region_id}: Last-Modified de página 1 coincide con el snapshot previo; "
            f"esperado={json.dumps(seed.expected_last_modified_gmt, ensure_ascii=False)}, "
            f"recibido={json.dumps(first_last_modified_gmt, ensure_ascii=False)}",
            snapshot_retry_at(captured_pages),
            unchanged=True,
        )
    orders_by_id: dict[int, tuple[Any, ...]] = {}

    def consume(page_number: int, page: PageResult) -> None:
        observed_pages = [*captured_pages, page]
        if page.page_count != page_count:
            raise SnapshotError(
                f"Región {seed.region_id}, página {page_number}, HTTP {page.status_code}: "
                f"X-Pages esperado={page_count}, "
                f"recibido={page.page_count}",
                snapshot_retry_at(observed_pages),
            )
        modified_date = parse_http_date(page.headers.get("last-modified"))
        modified = iso_utc(modified_date) if modified_date else None
        if modified != baseline_modified:
            raise SnapshotError(
                f"Región {seed.region_id}, página {page_number}, HTTP {page.status_code}: "
                f"Last-Modified esperado="
                f"{json.dumps(first_last_modified_gmt, ensure_ascii=False)}, "
                f"recibido={json.dumps(page.headers.get('last-modified'), ensure_ascii=False)} "
                f"(normalizado esperado={baseline_modified}, recibido={modified or 'null'})",
                snapshot_retry_at(observed_pages),
            )
        captured_pages.append(page)
        for raw_order in page.orders:
            row = normalize_order(raw_order, seed.region_id)
            order_id = row[0]
            previous = orders_by_id.get(order_id)
            if previous is not None and previous != row:
                raise SnapshotError(
                    f"Región {seed.region_id}: order_id {order_id} aparece con valores distintos entre páginas",
                    snapshot_retry_at(captured_pages),
                )
            orders_by_id[order_id] = row

    consume(1, first_page)
    for page_number in range(2, page_count + 1):
        page = fetch_page(seed.region_id, page_number)
        consume(page_number, page)
        if page_number % 50 == 0 or page_number == page_count:
            print(f"  Región {seed.region_id}: {page_number}/{page_count} páginas; {len(orders_by_id):,} órdenes únicas.", flush=True)
    last_page_modified_gmt = captured_pages[-1].headers.get("last-modified")
    last_page_modified = parse_http_date(last_page_modified_gmt)
    if last_page_modified is None:
        raise SnapshotError(
            f"Región {seed.region_id}: última página sin Last-Modified válido",
            snapshot_retry_at(captured_pages),
        )
    next_request_at = region_cache_deadline(captured_pages)
    return (
        orders_by_id,
        iso_utc(last_page_modified),
        last_page_modified_gmt,
        iso_utc(next_request_at),
        page_count,
    )


def write_sqlite(
    region_id: int,
    records: dict[int, tuple[Any, ...]],
    last_modified: str,
    next_request_at_utc: str,
    page_count: int,
    path: Path,
) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA page_size=4096")
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=MEMORY")
        connection.execute(
            """CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                type_id INTEGER NOT NULL,
                location_id INTEGER NOT NULL,
                system_id INTEGER NOT NULL,
                price REAL NOT NULL,
                volume_remain INTEGER NOT NULL,
                volume_total INTEGER NOT NULL,
                min_volume INTEGER NOT NULL,
                is_buy_order INTEGER NOT NULL CHECK(is_buy_order IN (0, 1)),
                issued TEXT NOT NULL,
                duration INTEGER NOT NULL,
                "range" TEXT NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE capture (
                region_id INTEGER PRIMARY KEY CHECK(region_id > 0),
                last_modified TEXT NOT NULL,
                order_count INTEGER NOT NULL CHECK(order_count >= 0)
            )"""
        )
        connection.execute(
            """CREATE TABLE esi_cache (
                region_id INTEGER PRIMARY KEY REFERENCES capture(region_id),
                next_request_at_utc TEXT NOT NULL,
                page_count INTEGER NOT NULL CHECK(page_count > 0)
            )"""
        )
        batch: list[tuple[Any, ...]] = []
        for order_id in sorted(records):
            batch.append(records[order_id])
            if len(batch) == INSERT_BATCH_SIZE:
                connection.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
                batch.clear()
        if batch:
            connection.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch)
        connection.execute(
            "INSERT INTO capture VALUES (?, ?, ?)", (region_id, last_modified, len(records))
        )
        connection.execute(
            "INSERT INTO esi_cache VALUES (?, ?, ?)", (region_id, next_request_at_utc, page_count)
        )
        connection.commit()
        connection.execute("VACUUM")
        if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
            raise CaptureError(f"Falló integrity_check para región {region_id}")
    finally:
        connection.close()


def make_region_database(
    region_id: int,
    records: dict[int, tuple[Any, ...]],
    last_modified: str,
    next_request_at_utc: str,
    page_count: int,
    staging_dir: Path,
) -> Path:
    plain_path = staging_dir / f"{region_id}.db"
    compressed_path = staging_dir / f"{region_id}.db.zst"
    write_sqlite(region_id, records, last_modified, next_request_at_utc, page_count, plain_path)
    with plain_path.open("rb") as source, compressed_path.open("wb") as output:
        zstandard.ZstdCompressor(level=22, write_checksum=True, write_content_size=True).copy_stream(source, output)
    plain_path.unlink()
    size = compressed_path.stat().st_size
    if size >= GITHUB_FILE_LIMIT_BYTES:
        compressed_path.unlink(missing_ok=True)
        raise CaptureError(
            f"Región {region_id}: la base comprimida ocupa {size:,} bytes y supera el límite de GitHub"
        )
    return compressed_path


def capture_region(seed: RegionSeed, staging_dir: Path, force: bool) -> RegionResult | None:
    if seed.not_before_utc:
        not_before = dt.datetime.fromisoformat(seed.not_before_utc.replace("Z", "+00:00"))
        initial_wait = max(0.0, (not_before - dt.datetime.now(dt.timezone.utc)).total_seconds())
        if initial_wait > 0:
            print(
                f"Región {seed.region_id}: espera {initial_wait:.1f}s hasta Expires antes de solicitar ESI.",
                flush=True,
            )
            time.sleep(initial_wait)
    for attempt in range(1, MAX_SNAPSHOT_ATTEMPTS + 1):
        try:
            print(
                f"Región {seed.region_id}: inicio de captura, intento {attempt}/{MAX_SNAPSHOT_ATTEMPTS}.",
                flush=True,
            )
            records, last_modified, last_modified_gmt, next_request_at, actual_pages = fetch_region_orders(
                seed, force
            )
            compressed = make_region_database(
                seed.region_id, records, last_modified, next_request_at, actual_pages, staging_dir
            )
            print(
                f"Región {seed.region_id}: {actual_pages} páginas, {len(records):,} órdenes; "
                f"{compressed.stat().st_size:,} bytes comprimidos.",
                flush=True,
            )
            return RegionResult(
                seed.region_id,
                last_modified,
                last_modified_gmt,
                actual_pages,
                len(records),
                compressed,
                next_request_at,
            )
        except SnapshotError as exc:
            if attempt >= MAX_SNAPSHOT_ATTEMPTS:
                if exc.unchanged:
                    print(
                        f"Región {seed.region_id}: sin cambios tras el intento final; "
                        "se conserva la DB y headers.json.",
                        flush=True,
                    )
                    return None
                raise
            if attempt == MAX_SNAPSHOT_ATTEMPTS - 1:
                retry_at = exc.retry_at
                delay = (
                    max(0.0, (retry_at - dt.datetime.now(dt.timezone.utc)).total_seconds())
                    if retry_at
                    else FINAL_SNAPSHOT_RETRY_DELAY_SECONDS
                )
                wait_reason = "espera hasta Expires antes del último intento"
            else:
                delay = SNAPSHOT_RETRY_DELAY_SECONDS
                wait_reason = "reintento rápido"
            message = (
                f"Región {seed.region_id}: snapshot inválido ({exc}); "
                f"reinicio {attempt + 1}/{MAX_SNAPSHOT_ATTEMPTS} en {delay:.1f}s ({wait_reason})."
            )
            print(message, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def run_worker(worker_index: int, seeds: list[RegionSeed], staging_dir: Path, force: bool):
    results: list[RegionResult] = []
    errors: list[tuple[int, str]] = []
    unchanged_regions: list[int] = []
    for index, seed in enumerate(seeds, start=1):
        print(f"Worker {worker_index}: región {index}/{len(seeds)} (ID {seed.region_id}).", flush=True)
        try:
            result = capture_region(seed, staging_dir, force)
            if result is not None:
                results.append(result)
            else:
                unchanged_regions.append(seed.region_id)
        except Exception as exc:
            errors.append((seed.region_id, str(exc)))
            message = f"Worker {worker_index}, región {seed.region_id}: {type(exc).__name__}: {exc}"
            report_github_error(f"Captura fallida en región {seed.region_id}", message)
            print(message, file=sys.stderr, flush=True)
    return results, errors, unchanged_regions


def write_headers_json(
    path: Path,
    previous_regions: dict[str, Any],
    successful_results: list[RegionResult],
    region_ids: list[int],
) -> None:
    allowed_ids = {str(region_id) for region_id in region_ids}
    regions = {
        key: value
        for key, value in previous_regions.items()
        if key in allowed_ids and isinstance(value, dict)
    }
    for result in successful_results:
        regions[str(result.region_id)] = {
            "last_modified": result.last_modified,
            "last_modified_gmt": result.last_modified_gmt,
            "last_page": result.last_page,
        }
    document = {"regions": regions}
    with path.open("w", encoding="utf-8", newline="\n") as output:
        json.dump(document, output, ensure_ascii=False, sort_keys=True, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def publish(
    staged_files: list[Path],
    staged_headers: Path,
    region_ids: list[int],
    remove_obsolete: bool,
) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for staged in staged_files:
        final_path = OUTPUT_DIR / staged.name
        os.replace(staged, final_path)
    if remove_obsolete:
        expected = {f"{region_id}.db.zst" for region_id in region_ids}
        for existing in OUTPUT_DIR.glob("*.db.zst"):
            if existing.name not in expected:
                existing.unlink()
    os.replace(staged_headers, HEADERS_PATH)


def main(force: bool = False) -> int:
    try:
        print(f"Modo force: {'activado' if force else 'desactivado'}.", flush=True)
        region_ids = regions_from_database()
        print(
            f"Planificación: usando last_page de headers.json para estimar páginas de "
            f"{len(region_ids)} regiones; sin peticiones ESI previas.",
            flush=True,
        )
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix=".market-orders-capture-", dir=ROOT) as temp_name:
            staging_dir = Path(temp_name)
            seeds, cached_results, previous_regions = plan_region_work(region_ids, staging_dir, force)
            if not seeds:
                print(
                    "Todas las regiones tienen una captura local vigente; no se hacen peticiones ESI. "
                    "headers.json se mantiene sin cambios porque no se escribió ninguna DB.",
                    flush=True,
                )
                return 0
            assignments = assign_regions(seeds)
            new_results: list[RegionResult] = []
            failures: list[tuple[int, str]] = []
            unchanged_regions: list[int] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT, thread_name_prefix="esi-region") as pool:
                futures = {
                    pool.submit(run_worker, index, seeds_for_worker, staging_dir, force): index
                    for index, seeds_for_worker in enumerate(assignments, start=1)
                }
                for future in concurrent.futures.as_completed(futures):
                    worker_results, worker_errors, worker_unchanged = future.result()
                    new_results.extend(worker_results)
                    failures.extend(worker_errors)
                    unchanged_regions.extend(worker_unchanged)

            results = new_results + cached_results
            headers_temp = staging_dir / "headers.json"
            write_headers_json(headers_temp, previous_regions, new_results, region_ids)
            publish(
                [result.compressed_path for result in new_results],
                headers_temp,
                region_ids,
                remove_obsolete=not failures,
            )

        unchanged_by_id = {seed.region_id: seed for seed in seeds.values()}
        total_pages = sum(result.last_page for result in results) + sum(
            unchanged_by_id[region_id].page_count for region_id in unchanged_regions
        )
        total_orders = sum(result.order_count for result in results) + sum(
            unchanged_by_id[region_id].existing_order_count for region_id in unchanged_regions
        )
        print(
            f"Publicadas {len(new_results)} regiones nuevas/actualizadas; {len(failures)} regiones fallidas, "
            f"{len(unchanged_regions)} sin cambios, "
            f"{total_pages:,} páginas y {total_orders:,} órdenes conocidas; "
            f"JSON de headers en {HEADERS_PATH.relative_to(ROOT)}.",
            flush=True,
        )
        if failures:
            summary = "; ".join(f"{region}: {message}" for region, message in failures[:10])
            message = f"{len(failures)} regiones no se capturaron. Fallos: {summary}"
            report_github_error("La captura terminó con regiones fallidas", message)
            print(message, file=sys.stderr, flush=True)
            return 1
        return 0
    except (CaptureError, OSError, sqlite3.Error, ValueError, zstandard.ZstdError) as exc:
        report_github_error("Captura de órdenes fallida", str(exc))
        print(f"Captura de órdenes fallida: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    force_capture = os.environ.get("FORCE_CAPTURE", "false").strip().lower() in {"1", "true", "yes", "on"}
    raise SystemExit(main(force=force_capture))
