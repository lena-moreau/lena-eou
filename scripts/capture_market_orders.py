"""Capture every public regional market order into one compressed SQLite per region."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import email.utils
import json
import math
import os
import random
import shutil
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
COMPATIBILITY_DATE = "2026-11-10"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
WORKER_COUNT = 4
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


@dataclass(frozen=True)
class PageResult:
    orders: list[dict[str, Any]]
    headers: dict[str, str]
    page_count: int


@dataclass(frozen=True)
class RegionSeed:
    region_id: int
    page_count: int
    first_page: PageResult


@dataclass
class RegionResult:
    region_id: int
    page_count: int
    order_count: int
    compressed_path: Path
    headers_by_page: dict[str, dict[str, str]]
    captured_at_utc: str


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def iso_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


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
                return PageResult(payload, headers, page_count)
        except urllib.error.HTTPError as exc:
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"Región {region_id}, página {page}: HTTP {exc.code}: {detail}") from exc
            delay = retry_delay(exc.headers, attempt, exc.code)
            print(f"  Región {region_id}, página {page}: HTTP {exc.code}; espera {delay:.1f}s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Región {region_id}, página {page}: error de red: {exc}") from exc
            delay = min(30.0, 2**attempt + random.random())
            print(f"  Región {region_id}, página {page}: error de red; espera {delay:.1f}s", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def discover_first_pages(region_ids: list[int]) -> tuple[dict[int, RegionSeed], list[RegionResult]]:
    try:
        previous = json.loads(HEADERS_PATH.read_text(encoding="utf-8"))
        previous_regions = previous.get("regions", {}) if isinstance(previous, dict) else {}
    except (OSError, json.JSONDecodeError):
        previous_regions = {}

    seeds: dict[int, RegionSeed] = {}
    cached_results: list[RegionResult] = []
    to_fetch: list[int] = []
    now = utc_now()
    for region_id in region_ids:
        cached = previous_regions.get(str(region_id)) if isinstance(previous_regions, dict) else None
        compressed_path = OUTPUT_DIR / f"{region_id}.db.zst"
        try:
            cached_pages = int(cached["pages"])
            cached_orders = int(cached["unique_orders"])
            cached_headers = cached["headers_by_page"]
            if not isinstance(cached_headers, dict) or cached_pages <= 0:
                raise ValueError("headers cache format invalid")
            if any(not isinstance(cached_headers.get(str(page)), dict) for page in range(1, cached_pages + 1)):
                raise ValueError("per-page headers missing")
            expiry_values = [
                parse_http_date(cached_headers[str(page)].get("expires"))
                for page in range(1, cached_pages + 1)
            ]
            captured_at = cached["captured_at_utc"]
            usable = (
                compressed_path.is_file()
                and cached_orders >= 0
                and isinstance(captured_at, str)
                and all(expiry is not None for expiry in expiry_values)
                and max(expiry_values) > now
            )
        except (KeyError, TypeError, ValueError):
            usable = False
        if usable:
            cached_results.append(
                RegionResult(region_id, cached_pages, cached_orders, compressed_path, cached_headers, captured_at)
            )
            print(f"Región {region_id}: se reutiliza la captura; todas sus páginas siguen en caché ESI.", flush=True)
        else:
            to_fetch.append(region_id)

    failures: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT, thread_name_prefix="esi-page1") as pool:
        futures = {pool.submit(fetch_page, region_id, 1): region_id for region_id in to_fetch}
        for future in concurrent.futures.as_completed(futures):
            region_id = futures[future]
            try:
                first_page = future.result()
                seeds[region_id] = RegionSeed(region_id, first_page.page_count, first_page)
                print(f"Página 1/ {first_page.page_count} descubierta para región {region_id}.", flush=True)
            except Exception as exc:
                failures.append(f"{region_id}: {exc}")
    if failures:
        raise CaptureError("No se pudo obtener página 1 de todas las regiones: " + "; ".join(failures[:10]))
    return seeds, cached_results


def assign_regions(seeds: dict[int, RegionSeed]) -> list[list[RegionSeed]]:
    """Greedy longest-processing-time assignment; each region stays in one worker."""
    assignments: list[list[RegionSeed]] = [[] for _ in range(WORKER_COUNT)]
    loads = [0] * WORKER_COUNT
    for seed in sorted(seeds.values(), key=lambda item: (-item.page_count, item.region_id)):
        worker = min(range(WORKER_COUNT), key=lambda index: (loads[index], index))
        assignments[worker].append(seed)
        loads[worker] += seed.page_count
    for worker, (regions, pages) in enumerate(zip(assignments, loads), start=1):
        print(f"Worker {worker}: {len(regions)} regiones, {pages} páginas asignadas.", flush=True)
    return assignments


def int_field(order: dict[str, Any], field: str, region_id: int) -> int:
    value = order.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CaptureError(f"Región {region_id}: {field} inválido en orden {order.get('order_id')!r}")
    return value


def encode_range(value: Any, region_id: int, order_id: int) -> int:
    named = {"station": 0, "solarsystem": 1, "region": 2}
    if value in named:
        return named[value]
    if not isinstance(value, str):
        raise CaptureError(f"Región {region_id}: range inválido en orden {order_id}")
    try:
        distance = int(value)
    except ValueError as exc:
        raise CaptureError(f"Región {region_id}: range desconocido en orden {order_id}") from exc
    if str(distance) != value or not 0 <= distance <= 32764:
        raise CaptureError(f"Región {region_id}: range fuera de rango en orden {order_id}")
    return distance + 3


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
        int(issued.timestamp()),
        values["duration"],
        encode_range(order.get("range"), region_id, order_id),
    )


def fetch_region_orders(seed: RegionSeed) -> tuple[dict[int, tuple[Any, ...]], dict[str, dict[str, str]], int]:
    page_count = seed.page_count
    baseline_modified = seed.first_page.headers.get("last-modified")
    first_expiry = parse_http_date(seed.first_page.headers.get("expires"))
    first_page = seed.first_page
    if first_expiry and first_expiry <= utc_now() + dt.timedelta(seconds=2):
        first_page = fetch_page(seed.region_id, 1)
        page_count = first_page.page_count
        baseline_modified = first_page.headers.get("last-modified")

    pages: dict[str, dict[str, str]] = {"1": first_page.headers}
    orders_by_id: dict[int, tuple[Any, ...]] = {}

    def consume(page_number: int, page: PageResult) -> None:
        if page.page_count != page_count:
            raise SnapshotError(
                f"Región {seed.region_id}: X-Pages pasó de {page_count} a {page.page_count} en página {page_number}"
            )
        modified = page.headers.get("last-modified")
        if baseline_modified and modified != baseline_modified:
            raise SnapshotError(f"Región {seed.region_id}: Last-Modified cambió en página {page_number}")
        pages[str(page_number)] = page.headers
        for raw_order in page.orders:
            row = normalize_order(raw_order, seed.region_id)
            order_id = row[0]
            previous = orders_by_id.get(order_id)
            if previous is not None and previous != row:
                raise SnapshotError(
                    f"Región {seed.region_id}: order_id {order_id} aparece con valores distintos entre páginas"
                )
            orders_by_id[order_id] = row

    consume(1, first_page)
    for page_number in range(2, page_count + 1):
        page = fetch_page(seed.region_id, page_number)
        consume(page_number, page)
        if page_number % 50 == 0 or page_number == page_count:
            print(f"  Región {seed.region_id}: {page_number}/{page_count} páginas; {len(orders_by_id):,} órdenes únicas.", flush=True)
    return orders_by_id, pages, page_count


def write_sqlite(
    region_id: int,
    records: dict[int, tuple[Any, ...]],
    page_count: int,
    captured_at: str,
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
                issued INTEGER NOT NULL,
                duration INTEGER NOT NULL,
                range_code INTEGER NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE capture (
                region_id INTEGER NOT NULL,
                captured_at_utc TEXT NOT NULL,
                page_count INTEGER NOT NULL,
                order_count INTEGER NOT NULL
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
            "INSERT INTO capture VALUES (?, ?, ?, ?)", (region_id, captured_at, page_count, len(records))
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
    page_count: int,
    captured_at: str,
    staging_dir: Path,
) -> Path:
    plain_path = staging_dir / f"{region_id}.db"
    compressed_path = staging_dir / f"{region_id}.db.zst"
    write_sqlite(region_id, records, page_count, captured_at, plain_path)
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


def capture_region(seed: RegionSeed, captured_at: str, staging_dir: Path) -> RegionResult:
    for attempt in range(1, 4):
        try:
            print(f"Región {seed.region_id}: captura en su worker (intento {attempt}/3).", flush=True)
            records, headers_by_page, actual_pages = fetch_region_orders(seed)
            compressed = make_region_database(seed.region_id, records, actual_pages, captured_at, staging_dir)
            print(
                f"Región {seed.region_id}: {actual_pages} páginas, {len(records):,} órdenes; "
                f"{compressed.stat().st_size:,} bytes comprimidos.",
                flush=True,
            )
            return RegionResult(seed.region_id, actual_pages, len(records), compressed, headers_by_page, captured_at)
        except SnapshotError:
            if attempt >= 3:
                raise
            delay = 2 + random.random() * 2
            print(f"Región {seed.region_id}: snapshot cambió; se reintentará en {delay:.1f}s.", flush=True)
            time.sleep(delay)
            refreshed = fetch_page(seed.region_id, 1)
            seed = RegionSeed(seed.region_id, refreshed.page_count, refreshed)
    raise AssertionError("unreachable")


def run_worker(worker_index: int, seeds: list[RegionSeed], captured_at: str, staging_dir: Path):
    results: list[RegionResult] = []
    errors: list[tuple[int, str]] = []
    for index, seed in enumerate(seeds, start=1):
        print(f"Worker {worker_index}: región {index}/{len(seeds)} (ID {seed.region_id}).", flush=True)
        try:
            results.append(capture_region(seed, captured_at, staging_dir))
        except Exception as exc:
            errors.append((seed.region_id, str(exc)))
            print(f"Worker {worker_index}: fallo región {seed.region_id}: {exc}", file=sys.stderr, flush=True)
    return results, errors


def write_headers_json(path: Path, results: list[RegionResult], captured_at: str) -> None:
    document = {
        "captured_at_utc": captured_at,
        "workers": WORKER_COUNT,
        "regions": {
            str(result.region_id): {
                "pages": result.page_count,
                "unique_orders": result.order_count,
                "captured_at_utc": result.captured_at_utc,
                "headers_by_page": result.headers_by_page,
            }
            for result in sorted(results, key=lambda item: item.region_id)
        },
    }
    with path.open("w", encoding="utf-8", newline="\n") as output:
        json.dump(document, output, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def publish(staged_files: list[Path], staged_headers: Path, region_ids: list[int]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for staged in staged_files:
        final_path = OUTPUT_DIR / staged.name
        os.replace(staged, final_path)
    expected = {f"{region_id}.db.zst" for region_id in region_ids}
    for existing in OUTPUT_DIR.glob("*.db.zst"):
        if existing.name not in expected:
            existing.unlink()
    os.replace(staged_headers, HEADERS_PATH)


def main() -> int:
    try:
        region_ids = regions_from_database()
        print(f"Descubrimiento: leyendo la primera página de {len(region_ids)} regiones con {WORKER_COUNT} conexiones como máximo.", flush=True)
        seeds, cached_results = discover_first_pages(region_ids)
        if not seeds:
            print("Todas las regiones tienen una captura local y ESI mantiene vigentes sus cachés; no se hacen peticiones ni se reescriben archivos.", flush=True)
            return 0
        assignments = assign_regions(seeds)
        captured_at = iso_utc(utc_now())
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(prefix=".market-orders-capture-", dir=OUTPUT_DIR) as temp_name:
            staging_dir = Path(temp_name)
            new_results: list[RegionResult] = []
            failures: list[tuple[int, str]] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT, thread_name_prefix="esi-region") as pool:
                futures = {
                    pool.submit(run_worker, index, seeds_for_worker, captured_at, staging_dir): index
                    for index, seeds_for_worker in enumerate(assignments, start=1)
                }
                for future in concurrent.futures.as_completed(futures):
                    worker_results, worker_errors = future.result()
                    new_results.extend(worker_results)
                    failures.extend(worker_errors)

            if failures:
                summary = "; ".join(f"{region}: {message}" for region, message in failures[:10])
                raise CaptureError(
                    f"No se publica ningún archivo: fallaron {len(failures)} regiones. {summary}"
                )
            results = new_results + cached_results
            if len(results) != len(region_ids):
                raise CaptureError(f"Captura incompleta: {len(results)} de {len(region_ids)} regiones")

            headers_temp = staging_dir / "headers.json"
            write_headers_json(headers_temp, results, captured_at)
            publish([result.compressed_path for result in new_results], headers_temp, region_ids)

        total_pages = sum(result.page_count for result in results)
        total_orders = sum(result.order_count for result in results)
        print(
            f"Publicado: {len(results)} regiones, {total_pages:,} páginas y {total_orders:,} órdenes; "
            f"JSON de headers en {HEADERS_PATH.relative_to(ROOT)}.",
            flush=True,
        )
        return 0
    except (CaptureError, OSError, sqlite3.Error, ValueError, zstandard.ZstdError) as exc:
        print(f"Captura de órdenes fallida: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
