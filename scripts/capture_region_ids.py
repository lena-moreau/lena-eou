"""Capture ESI region IDs into a compact SQLite snapshot."""

from __future__ import annotations

import datetime as dt
import email.utils
import json
import random
import re
import shutil
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from contextlib import closing
from pathlib import Path
from typing import Any


URL = "https://esi.evetech.net/latest/universe/regions/"
USER_AGENT = "Lena-EOU-Region-Capture/0.1 (+https://github.com/lena-moreau/lena-eou)"
COMPATIBILITY_DATE = "2026-11-10"
TIMEOUT_SECONDS = 60
MAX_RETRIES = 5
FALLBACK_CACHE_SECONDS = 3600
ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = ROOT / "data" / "esi" / "uni" / "regions.db"


class CaptureError(RuntimeError):
    """Raised when the ESI region list cannot be safely captured."""


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
        return parsed.astimezone(dt.timezone.utc).replace(microsecond=0)
    except (TypeError, ValueError, OverflowError):
        return None


def cache_deadline(headers: dict[str, str], received_at: dt.datetime) -> dt.datetime:
    deadlines: list[dt.datetime] = []
    expires = parse_http_date(headers.get("expires"))
    if expires:
        deadlines.append(expires)
    match = re.search(r"(?:^|,)\s*max-age\s*=\s*(\d+)", headers.get("cache-control", ""), re.I)
    if match:
        server_date = parse_http_date(headers.get("date")) or received_at
        deadlines.append(server_date + dt.timedelta(seconds=int(match.group(1))))
    return max(deadlines) if deadlines else received_at + dt.timedelta(seconds=FALLBACK_CACHE_SECONDS)


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


def fetch_regions(etag: str | None) -> tuple[list[int] | None, dict[str, str], bool, dt.datetime]:
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
        "X-Compatibility-Date": COMPATIBILITY_DATE,
    }
    if etag:
        headers["If-None-Match"] = etag
    request = urllib.request.Request(URL, headers=headers)

    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                received_at = utc_now()
                response_headers = lower_headers(response.headers)
                try:
                    payload = json.loads(response.read())
                except (ValueError, UnicodeDecodeError) as exc:
                    raise CaptureError("ESI devolvió JSON inválido para la lista de regiones") from exc
                if not isinstance(payload, list):
                    raise CaptureError("ESI devolvió una respuesta que no es una lista de regiones")
                return payload, response_headers, False, received_at
        except urllib.error.HTTPError as exc:
            received_at = utc_now()
            response_headers = lower_headers(exc.headers)
            if exc.code == 304:
                return None, response_headers, True, received_at
            retryable = exc.code in (420, 429) or 500 <= exc.code <= 599
            if not retryable or attempt >= MAX_RETRIES:
                detail = exc.read(512).decode("utf-8", errors="replace").strip()
                raise CaptureError(f"ESI respondió HTTP {exc.code}: {detail}") from exc
            delay = retry_delay(exc.headers, attempt, exc.code)
            print(f"ESI respondió HTTP {exc.code}; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
        except (TimeoutError, OSError, urllib.error.URLError) as exc:
            if attempt >= MAX_RETRIES:
                raise CaptureError(f"Error de red al consultar ESI: {exc}") from exc
            delay = min(30.0, 2**attempt + random.random())
            print(f"Error de red con ESI; reintento en {delay:.1f} s", file=sys.stderr, flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def validate_region_ids(payload: list[Any]) -> list[int]:
    region_ids: list[int] = []
    seen: set[int] = set()
    for item in payload:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise CaptureError(f"ID de región inválido: {item!r}")
        if item in seen:
            raise CaptureError(f"ESI repitió el ID de región {item}")
        region_ids.append(item)
        seen.add(item)
    if not region_ids:
        raise CaptureError("ESI devolvió una lista de regiones vacía")
    return sorted(region_ids)


def read_metadata(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata'"
            ).fetchone()
            return dict(connection.execute("SELECT key, value FROM metadata")) if exists else {}
    except sqlite3.Error as exc:
        print(f"No se pudo leer la caché local ({exc}); se consultará ESI sin ETag.", file=sys.stderr, flush=True)
        return {}


def stored_regions_are_valid(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as connection:
            check = connection.execute("PRAGMA integrity_check").fetchone()
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='regions'"
            ).fetchone()
            count = connection.execute("SELECT COUNT(*) FROM regions").fetchone()[0] if table else 0
            return check == ("ok",) and count > 0
    except sqlite3.Error:
        return False


def _write_database(
    temporary: Path,
    region_ids: list[int] | None,
    metadata: dict[str, str],
) -> None:
    connection = sqlite3.connect(temporary)
    try:
        connection.execute("PRAGMA page_size = 4096")
        connection.execute("PRAGMA journal_mode = OFF")
        connection.execute("PRAGMA synchronous = OFF")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS regions (region_id INTEGER NOT NULL PRIMARY KEY CHECK(region_id > 0)) WITHOUT ROWID"
        )
        connection.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID")
        if region_ids is not None:
            connection.execute("DELETE FROM regions")
            connection.executemany("INSERT INTO regions(region_id) VALUES (?)", ((value,) for value in region_ids))
        connection.execute("DELETE FROM metadata")
        connection.executemany("INSERT INTO metadata(key, value) VALUES (?, ?)", sorted(metadata.items()))
        connection.commit()
        connection.execute("VACUUM")
        check = connection.execute("PRAGMA integrity_check").fetchone()
        if check != ("ok",):
            raise CaptureError(f"Falló integrity_check en la base: {check}")
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def persist(
    headers: dict[str, str],
    received_at: dt.datetime,
    etag: str,
    last_modified: str | None,
    region_ids: list[int] | None,
) -> None:
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cache_until = cache_deadline(headers, received_at)
    metadata = {
        "etag": etag,
        "last_modified": last_modified or "",
        "cache_control": headers.get("cache-control", ""),
        "expires": headers.get("expires", ""),
        "next_request_at_utc": iso_utc(cache_until),
        "checked_at_utc": iso_utc(received_at),
    }
    temporary = DATABASE_PATH.with_name(f".{DATABASE_PATH.name}.tmp")
    temporary.unlink(missing_ok=True)
    if DATABASE_PATH.is_file() and stored_regions_are_valid(DATABASE_PATH):
        shutil.copy2(DATABASE_PATH, temporary)
    try:
        _write_database(temporary, region_ids, metadata)
        temporary.replace(DATABASE_PATH)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def process(now: dt.datetime | None = None) -> None:
    state = read_metadata(DATABASE_PATH)
    database_valid = stored_regions_are_valid(DATABASE_PATH)
    if state and database_valid:
        deadline_text = state.get("next_request_at_utc")
        try:
            deadline = dt.datetime.fromisoformat(deadline_text.replace("Z", "+00:00")) if deadline_text else None
        except ValueError:
            deadline = None
        if deadline and deadline.tzinfo and (now or utc_now()) < deadline:
            print(f"Caché ESI vigente hasta {deadline_text}; no se consulta el endpoint.", flush=True)
            return

    previous_etag = state.get("etag") if state and database_valid else None
    print("Consultando ESI /universe/regions/" + (" con If-None-Match." if previous_etag else "."), flush=True)
    payload, headers, not_modified, received_at = fetch_regions(previous_etag)
    etag = headers.get("etag") or previous_etag
    if not etag:
        raise CaptureError("ESI no devolvió ETag; no se actualiza la base")

    if not_modified:
        if not state:
            raise CaptureError("ESI respondió 304 sin una base local previa")
        persist(headers, received_at, etag, headers.get("last-modified") or state.get("last_modified"), None)
        print(f"ETag sin cambios; no se agregaron IDs, caché vigente hasta {iso_utc(cache_deadline(headers, received_at))}.", flush=True)
        return

    if not headers.get("etag"):
        raise CaptureError("ESI no devolvió ETag en la respuesta 200; no se actualiza la base")
    ids = validate_region_ids(payload or [])
    changed = etag != previous_etag or not stored_regions_are_valid(DATABASE_PATH)
    persist(headers, received_at, etag, headers.get("last-modified"), ids if changed else None)
    if changed:
        print(f"Capturados {len(ids)} IDs de regiones en {DATABASE_PATH.relative_to(ROOT)}.", flush=True)
    else:
        print("ESI devolvió el mismo ETag; se conserva la lista actual sin duplicar datos.", flush=True)


def main() -> int:
    try:
        process()
    except (CaptureError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"Captura de regiones fallida: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
