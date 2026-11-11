"""Listen to R2Z2, fetch killmails from ESI, and publish compressed SQLite checkpoints."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import email.utils
import gzip
import heapq
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "esi" / "killmails"
STATE_PATH = OUTPUT_DIR / "state.db.zlib"
R2Z2_BASE = "https://r2z2.zkillboard.com/ephemeral"
ESI_BASE = "https://esi.evetech.net/latest"
USER_AGENT = "Lena-EOU-KillmailCapture/0.1 (+https://github.com/lena-moreau/lena-eou)"
COMPATIBILITY_DATE = "2020-01-01"
REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_DURATION_SECONDS = 1800
DEFAULT_CHECKPOINT_SECONDS = 300
R2Z2_IDLE_POLL_SECONDS = 60
R2Z2_MIN_REQUEST_INTERVAL_SECONDS = 0.1  # 10 requests/s, below the published 15/s ceiling.
ESI_DEFAULT_REQUEST_INTERVAL_SECONDS = 1.0
ESI_404_RETRY_DELAYS_SECONDS = (60.0, 300.0, 900.0)


class CaptureError(RuntimeError):
    """The run could not safely capture or publish killmail data."""


@dataclass(frozen=True)
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes


@dataclass(frozen=True)
class PendingKillmail:
    sequence_id: int
    killmail_id: int | None
    killmail_hash: str | None
    killmail_time: str | None
    payload_json: str
    attempts: int
    next_attempt_at: float


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso_utc(value: dt.datetime | None = None) -> str:
    value = value or utc_now()
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def github_escape(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def log_error(title: str, message: str) -> None:
    print(f"::error title={github_escape(title)}::{github_escape(message)}", flush=True)
    print(message, file=sys.stderr, flush=True)


def log_warning(message: str) -> None:
    print(f"::warning::{github_escape(message)}", flush=True)
    print(f"  {message}", file=sys.stderr, flush=True)


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def parse_iso_time(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return iso_utc(parsed)


def parse_http_date(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def retry_after_seconds(headers: dict[str, str], default: float) -> float:
    value = headers.get("retry-after")
    if value:
        try:
            return max(1.0, float(value))
        except ValueError:
            retry_at = parse_http_date(value)
            if retry_at:
                return max(1.0, (retry_at - utc_now()).total_seconds())
    return default


def request_json(url: str, headers: dict[str, str]) -> HttpResult:
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            response_headers = {key.lower(): value for key, value in response.headers.items()}
            body = response.read()
            if response_headers.get("content-encoding", "").lower() == "gzip":
                body = gzip.decompress(body)
            return HttpResult(response.status, response_headers, body)
    except urllib.error.HTTPError as exc:
        response_headers = {key.lower(): value for key, value in exc.headers.items()}
        body = exc.read(2048)
        if response_headers.get("content-encoding", "").lower() == "gzip":
            try:
                body = gzip.decompress(body)
            except OSError:
                pass
        return HttpResult(exc.code, response_headers, body)


def parse_json_body(result: HttpResult, service: str) -> Any:
    try:
        return json.loads(result.body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CaptureError(f"{service} devolvió JSON inválido (HTTP {result.status}).") from exc


def compress_zlib(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    compressor = zlib.compressobj(level=9)
    try:
        with source.open("rb") as input_file, temporary.open("wb") as output_file:
            while chunk := input_file.read(1024 * 1024):
                output_file.write(compressor.compress(chunk))
            output_file.write(compressor.flush())
            output_file.flush()
            os.fsync(output_file.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def decompress_zlib(source: Path, destination: Path) -> None:
    decompressor = zlib.decompressobj()
    with source.open("rb") as input_file, destination.open("wb") as output_file:
        while chunk := input_file.read(1024 * 1024):
            output_file.write(decompressor.decompress(chunk))
        output_file.write(decompressor.flush())


def initialize_state_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(
            """CREATE TABLE IF NOT EXISTS capture_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                next_sequence INTEGER,
                updated_at_utc TEXT NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS pending (
                sequence_id INTEGER PRIMARY KEY,
                killmail_id INTEGER,
                killmail_hash TEXT,
                killmail_time TEXT,
                payload_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                received_at_utc TEXT NOT NULL
            )"""
        )
        connection.execute(
            """CREATE TABLE IF NOT EXISTS seen (
                killmail_id INTEGER NOT NULL,
                killmail_hash TEXT NOT NULL,
                sink TEXT NOT NULL CHECK(sink IN ('esi', 'r2z2')),
                sequence_id INTEGER NOT NULL,
                captured_at_utc TEXT NOT NULL,
                PRIMARY KEY (killmail_id, killmail_hash)
            ) WITHOUT ROWID"""
        )
        connection.execute(
            "INSERT OR IGNORE INTO capture_state VALUES (1, NULL, ?)", (iso_utc(),)
        )
        connection.commit()
    finally:
        connection.close()


class CaptureStore:
    def __init__(self, database_path: Path, lock: threading.RLock, condition: threading.Condition):
        self.path = database_path
        self.lock = lock
        self.condition = condition
        self.connection = sqlite3.connect(database_path, check_same_thread=False, timeout=30)
        self.connection.execute("PRAGMA busy_timeout=30000")
        self.connection.execute("PRAGMA synchronous=FULL")

    def get_next_sequence(self) -> int | None:
        with self.lock:
            value = self.connection.execute(
                "SELECT next_sequence FROM capture_state WHERE singleton = 1"
            ).fetchone()[0]
            return int(value) if value is not None else None

    def initialize_sequence(self, sequence: int) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE capture_state SET next_sequence = ?, updated_at_utc = ? WHERE singleton = 1",
                (sequence, iso_utc()),
            )
            self.connection.commit()
            self.condition.notify_all()

    def enqueue_r2z2(self, sequence_id: int, payload: dict[str, Any]) -> bool:
        esi_payload = payload.get("esi")
        if not isinstance(esi_payload, dict):
            esi_payload = {}
        zkb = payload.get("zkb") if isinstance(payload.get("zkb"), dict) else {}
        raw_id = payload.get("killmail_id", esi_payload.get("killmail_id"))
        try:
            killmail_id = int(raw_id) if raw_id is not None else None
        except (TypeError, ValueError):
            killmail_id = None
        killmail_hash = payload.get("hash") or zkb.get("hash")
        if not isinstance(killmail_hash, str) or not killmail_hash:
            killmail_hash = None
        killmail_time = parse_iso_time(esi_payload.get("killmail_time"))
        inserted = False
        with self.lock:
            if killmail_id is not None and killmail_hash:
                seen = self.connection.execute(
                    "SELECT 1 FROM seen WHERE killmail_id = ? AND killmail_hash = ?",
                    (killmail_id, killmail_hash),
                ).fetchone()
                pending_duplicate = self.connection.execute(
                    "SELECT 1 FROM pending WHERE killmail_id = ? AND killmail_hash = ?",
                    (killmail_id, killmail_hash),
                ).fetchone()
                if not seen and not pending_duplicate:
                    cursor = self.connection.execute(
                        """INSERT OR IGNORE INTO pending
                           (sequence_id, killmail_id, killmail_hash, killmail_time,
                            payload_json, attempts, next_attempt_at, received_at_utc)
                           VALUES (?, ?, ?, ?, ?, 0, 0, ?)""",
                        (sequence_id, killmail_id, killmail_hash, killmail_time,
                         compact_json(payload), iso_utc()),
                    )
                    inserted = cursor.rowcount == 1
            else:
                # Preserve malformed/unaddressable messages in the fallback DB at checkpoint.
                cursor = self.connection.execute(
                    """INSERT OR IGNORE INTO pending
                       (sequence_id, killmail_id, killmail_hash, killmail_time,
                        payload_json, attempts, next_attempt_at, received_at_utc)
                       VALUES (?, NULL, NULL, ?, ?, 0, 0, ?)""",
                    (sequence_id, killmail_time, compact_json(payload), iso_utc()),
                )
                inserted = cursor.rowcount == 1
            self.connection.execute(
                "UPDATE capture_state SET next_sequence = ?, updated_at_utc = ? WHERE singleton = 1",
                (sequence_id + 1, iso_utc()),
            )
            self.connection.commit()
            self.condition.notify_all()
        return inserted

    def next_due_pending(self) -> PendingKillmail | None:
        with self.lock:
            row = self.connection.execute(
                """SELECT sequence_id, killmail_id, killmail_hash, killmail_time,
                          payload_json, attempts, next_attempt_at
                   FROM pending WHERE next_attempt_at <= ?
                   ORDER BY killmail_time DESC, sequence_id ASC LIMIT 1""",
                (time.time(),),
            ).fetchone()
        return PendingKillmail(*row) if row else None

    def seconds_until_next_pending(self) -> float | None:
        with self.lock:
            row = self.connection.execute(
                "SELECT MIN(next_attempt_at) FROM pending"
            ).fetchone()
        if not row or row[0] is None:
            return None
        return max(0.0, float(row[0]) - time.time())

    def pending_count(self) -> int:
        with self.lock:
            return int(self.connection.execute("SELECT COUNT(*) FROM pending").fetchone()[0])

    def retry_pending(self, item: PendingKillmail, delay: float) -> None:
        with self.lock:
            self.connection.execute(
                "UPDATE pending SET attempts = attempts + 1, next_attempt_at = ? WHERE sequence_id = ?",
                (time.time() + delay, item.sequence_id),
            )
            self.connection.execute(
                "UPDATE capture_state SET updated_at_utc = ? WHERE singleton = 1", (iso_utc(),)
            )
            self.connection.commit()
            self.condition.notify_all()

    def retry_count(self, item: PendingKillmail) -> int:
        with self.lock:
            row = self.connection.execute(
                "SELECT attempts FROM pending WHERE sequence_id = ?", (item.sequence_id,)
            ).fetchone()
        return int(row[0]) if row else item.attempts

    def mark_complete(self, item: PendingKillmail, sink: str) -> None:
        with self.lock:
            if item.killmail_id is not None and item.killmail_hash:
                self.connection.execute(
                    """INSERT OR IGNORE INTO seen
                       (killmail_id, killmail_hash, sink, sequence_id, captured_at_utc)
                       VALUES (?, ?, ?, ?, ?)""",
                    (item.killmail_id, item.killmail_hash, sink, item.sequence_id, iso_utc()),
                )
            self.connection.execute("DELETE FROM pending WHERE sequence_id = ?", (item.sequence_id,))
            self.connection.execute(
                "UPDATE capture_state SET updated_at_utc = ? WHERE singleton = 1", (iso_utc(),)
            )
            self.connection.commit()
            self.condition.notify_all()

    def save_compressed_state(self, output_path: Path) -> None:
        with self.lock:
            self.connection.commit()
            compress_zlib(self.path, output_path)

    def close(self) -> None:
        with self.lock:
            self.connection.commit()
            self.connection.close()


class PartWriter:
    def __init__(self, work_dir: Path, shared_lock: threading.RLock):
        self.work_dir = work_dir
        self.lock = shared_lock
        self.esi_path = work_dir / "active-esi.db"
        self.r2_path = work_dir / "active-r2z2.db"
        self.esi = self._open(self.esi_path)
        self.r2 = self._open(self.r2_path)
        self._create_schema(self.esi)
        self._create_schema(self.r2)
        self.esi_count = 0
        self.r2_count = 0

    @staticmethod
    def _open(path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(path, check_same_thread=False, timeout=30)
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA temp_store=MEMORY")
        return connection

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS killmails (
                sequence_id INTEGER PRIMARY KEY,
                killmail_id INTEGER,
                killmail_hash TEXT,
                killmail_time TEXT,
                captured_at_utc TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )"""
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_killmails_time ON killmails(killmail_time DESC)"
        )

    def write_esi(self, item: PendingKillmail, payload: dict[str, Any]) -> None:
        with self.lock:
            self.esi.execute(
                "INSERT OR IGNORE INTO killmails VALUES (?, ?, ?, ?, ?, ?)",
                (item.sequence_id, item.killmail_id, item.killmail_hash, item.killmail_time,
                 iso_utc(), compact_json(payload)),
            )
            self.esi.commit()
            self.esi_count += 1

    def write_r2z2(self, item: PendingKillmail) -> None:
        with self.lock:
            self.r2.execute(
                "INSERT OR IGNORE INTO killmails VALUES (?, ?, ?, ?, ?, ?)",
                (item.sequence_id, item.killmail_id, item.killmail_hash, item.killmail_time,
                 iso_utc(), item.payload_json),
            )
            self.r2.commit()
            self.r2_count += 1

    def _seal(self, connection: sqlite3.Connection, path: Path, prefix: str, stamp: str) -> Path | None:
        connection.commit()
        count = int(connection.execute("SELECT COUNT(*) FROM killmails").fetchone()[0])
        if count == 0:
            connection.close()
            path.unlink(missing_ok=True)
            return None
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity != ("ok",):
            raise CaptureError(f"SQLite integrity_check falló para lote {prefix}: {integrity!r}")
        first_sequence, last_sequence = connection.execute(
            "SELECT MIN(sequence_id), MAX(sequence_id) FROM killmails"
        ).fetchone()
        connection.close()
        final_path = OUTPUT_DIR / prefix / f"{stamp}-{first_sequence}-{last_sequence}.db.zlib"
        compress_zlib(path, final_path)
        path.unlink(missing_ok=True)
        return final_path

    def rotate(self, stamp: str) -> list[Path]:
        with self.lock:
            esi_path, r2_path = self.esi_path, self.r2_path
            old_esi, old_r2 = self.esi, self.r2
            self.esi = None  # type: ignore[assignment]
            self.r2 = None  # type: ignore[assignment]
            esi_part = r2_part = None
            try:
                esi_part = self._seal(old_esi, esi_path, "esi", stamp)
                r2_part = self._seal(old_r2, r2_path, "r2z2", stamp)
            finally:
                self.esi = self._open(self.esi_path)
                self.r2 = self._open(self.r2_path)
                self._create_schema(self.esi)
                self._create_schema(self.r2)
                self.esi_count = 0
                self.r2_count = 0
        return [path for path in (esi_part, r2_part) if path is not None]

    def close(self) -> None:
        with self.lock:
            for connection in (self.esi, self.r2):
                if connection:
                    connection.commit()
                    connection.close()


class EsiRateLimiter:
    def __init__(self):
        self.interval = ESI_DEFAULT_REQUEST_INTERVAL_SECONDS
        self.next_request_at = 0.0

    def wait(self, stop: threading.Event) -> bool:
        delay = max(0.0, self.next_request_at - time.monotonic())
        return not stop.wait(delay)

    def record(self, headers: dict[str, str]) -> None:
        value = headers.get("x-ratelimit-limit", "")
        try:
            amount_text, window_text = value.split("/", 1)
            amount = float(amount_text)
            unit = window_text[-1].lower()
            count = float(window_text[:-1])
            seconds = count * {"s": 1, "m": 60, "h": 3600}.get(unit, 0)
            if amount > 0 and seconds > 0:
                # ESI charges two tokens per successful GET. Target half of the advertised bucket rate.
                self.interval = max(ESI_DEFAULT_REQUEST_INTERVAL_SECONDS, 4.0 * seconds / amount)
        except (ValueError, TypeError):
            pass
        self.next_request_at = time.monotonic() + self.interval

    def defer(self, seconds: float) -> None:
        self.next_request_at = max(self.next_request_at, time.monotonic() + max(1.0, seconds))


class CaptureRun:
    def __init__(self, duration_seconds: int, checkpoint_seconds: int):
        self.duration_seconds = duration_seconds
        self.checkpoint_seconds = checkpoint_seconds
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.stop = threading.Event()
        self.stats = Counter()
        self.stats_lock = threading.Lock()
        self.worker_errors: list[str] = []
        self.worker_errors_lock = threading.Lock()
        self.rate_limiter = EsiRateLimiter()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        self.temp_context = tempfile.TemporaryDirectory(prefix="r2z2-capture-")
        self.work_dir = Path(self.temp_context.name)
        self.state_path = self.work_dir / "state.db"
        if STATE_PATH.is_file():
            decompress_zlib(STATE_PATH, self.state_path)
        initialize_state_database(self.state_path)
        self.store = CaptureStore(self.state_path, self.lock, self.condition)
        self.writer = PartWriter(self.work_dir, self.lock)

    def count(self, key: str, value: int = 1) -> None:
        with self.stats_lock:
            self.stats[key] += value

    def record_worker_error(self, worker: str, exc: BaseException) -> None:
        message = f"Worker {worker}: {type(exc).__name__}: {exc}"
        with self.worker_errors_lock:
            self.worker_errors.append(message)
        log_error(f"Error en worker {worker}", message)
        self.stop.set()
        with self.condition:
            self.condition.notify_all()

    def initialize_cursor(self) -> None:
        if self.store.get_next_sequence() is not None:
            return
        result = request_json(
            f"{R2Z2_BASE}/sequence.json",
            {"Accept": "application/json", "User-Agent": USER_AGENT, "Accept-Encoding": "gzip"},
        )
        if result.status != 200:
            message = f"R2Z2 sequence.json respondió HTTP {result.status}; cabeceras={result.headers}."
            log_error("No se pudo iniciar el cursor R2Z2", message)
            raise CaptureError(message)
        payload = parse_json_body(result, "R2Z2 sequence.json")
        sequence = payload.get("sequence") if isinstance(payload, dict) else None
        if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence <= 0:
            raise CaptureError(f"R2Z2 sequence.json no contiene una secuencia positiva: {payload!r}")
        self.store.initialize_sequence(sequence)
        print(f"R2Z2: cursor inicial obtenido de sequence.json: {sequence}.", flush=True)

    def r2z2_worker(self) -> None:
        last_request_at = 0.0
        try:
            while not self.stop.is_set():
                sequence = self.store.get_next_sequence()
                if sequence is None:
                    self.initialize_cursor()
                    continue
                wait_for_interval = R2Z2_MIN_REQUEST_INTERVAL_SECONDS - (time.monotonic() - last_request_at)
                if wait_for_interval > 0 and self.stop.wait(wait_for_interval):
                    break
                url = f"{R2Z2_BASE}/{sequence}.json"
                try:
                    result = request_json(
                        url,
                        {"Accept": "application/json", "User-Agent": USER_AGENT, "Accept-Encoding": "gzip"},
                    )
                except (OSError, TimeoutError, urllib.error.URLError) as exc:
                    self.count("r2z2_network_errors")
                    log_warning(f"R2Z2 secuencia {sequence}: error de red {type(exc).__name__}: {exc}; reintento en 60 s.")
                    self.stop.wait(R2Z2_IDLE_POLL_SECONDS)
                    continue
                last_request_at = time.monotonic()
                self.count(f"r2z2_http_{result.status}")
                if result.status == 200:
                    try:
                        payload = parse_json_body(result, f"R2Z2 secuencia {sequence}")
                    except CaptureError as exc:
                        log_error(f"R2Z2 secuencia {sequence}: JSON inválido", str(exc))
                        self.stop.wait(R2Z2_IDLE_POLL_SECONDS)
                        continue
                    if not isinstance(payload, dict) or payload.get("sequence_id") != sequence:
                        log_error(
                            f"R2Z2 secuencia {sequence}: identificador inconsistente",
                            f"La respuesta no confirma sequence_id={sequence}: {compact_json(payload)[:1000]}",
                        )
                        self.stop.wait(R2Z2_IDLE_POLL_SECONDS)
                        continue
                    inserted = self.store.enqueue_r2z2(sequence, payload)
                    if inserted:
                        self.count("r2z2_enqueued")
                    else:
                        self.count("r2z2_duplicates")
                    if sequence % 100 == 0:
                        print(
                            f"R2Z2: cursor {sequence + 1}; pendientes {self.store.pending_count()}.",
                            flush=True,
                        )
                    continue
                if result.status == 404:
                    self.count("r2z2_head_404")
                    print(
                        f"R2Z2: secuencia {sequence} devuelve 404; feed al día, siguiente consulta en "
                        f"{R2Z2_IDLE_POLL_SECONDS}s.", flush=True,
                    )
                    self.stop.wait(R2Z2_IDLE_POLL_SECONDS)
                    continue
                delay = retry_after_seconds(result.headers, R2Z2_IDLE_POLL_SECONDS)
                message = (
                    f"R2Z2 secuencia {sequence}: HTTP {result.status}; espera {delay:.1f}s; "
                    f"cabeceras={compact_json(result.headers)}; cuerpo={result.body[:512].decode('utf-8', errors='replace')}"
                )
                log_warning(message)
                self.stop.wait(delay)
        except BaseException as exc:
            self.record_worker_error("R2Z2", exc)

    def _headers_diagnostic(self, headers: dict[str, str]) -> str:
        names = (
            "retry-after", "x-esi-error-limit-remain", "x-esi-error-limit-reset",
            "x-ratelimit-group", "x-ratelimit-limit", "x-ratelimit-remaining",
            "x-ratelimit-used", "x-esi-cache-status", "etag", "expires", "date",
        )
        return compact_json({name: headers.get(name) for name in names})

    def _fallback(self, item: PendingKillmail, reason: str) -> None:
        with self.lock:
            self.writer.write_r2z2(item)
            self.store.mark_complete(item, "r2z2")
        self.count("r2z2_fallback")
        log_warning(
            f"Killmail {item.killmail_id} secuencia {item.sequence_id}: se conserva en R2Z2 "
            f"porque no se guardó en ESI ({reason})."
        )

    def _retry(self, item: PendingKillmail, delay: float, reason: str) -> None:
        self.store.retry_pending(item, delay)
        self.count("esi_retries")
        log_warning(
            f"ESI killmail {item.killmail_id} hash={item.killmail_hash} secuencia={item.sequence_id}: "
            f"{reason}; reintento en {delay:.1f}s; intento persistido={item.attempts + 1}."
        )

    def _fetch_esi(self, item: PendingKillmail) -> None:
        if item.killmail_id is None or not item.killmail_hash:
            self._fallback(item, "R2Z2 no proporcionó killmail_id/hash válidos")
            return
        if not self.rate_limiter.wait(self.stop):
            self.store.retry_pending(item, 1.0)
            return
        url = f"{ESI_BASE}/killmails/{item.killmail_id}/{urllib.parse.quote(item.killmail_hash, safe='')}/"
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "X-Compatibility-Date": COMPATIBILITY_DATE,
        }
        try:
            result = request_json(url, headers)
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            self.count("esi_network_errors")
            self.rate_limiter.defer(30.0)
            self._retry(item, 30.0, f"error de red {type(exc).__name__}: {exc}")
            return
        self.count(f"esi_http_{result.status}")
        self.rate_limiter.record(result.headers)
        if result.status == 200:
            try:
                payload = parse_json_body(result, "ESI killmail")
                returned_id = payload.get("killmail_id") if isinstance(payload, dict) else None
                if not isinstance(payload, dict) or returned_id != item.killmail_id:
                    raise CaptureError(
                        f"ESI devolvió killmail_id={returned_id!r}, esperado={item.killmail_id}."
                    )
            except CaptureError as exc:
                attempts = item.attempts + 1
                if attempts >= 3:
                    self._fallback(item, f"respuesta 200 no válida tras {attempts} intentos: {exc}")
                else:
                    self._retry(item, (2.5, 5.0, 10.0)[attempts - 1], f"HTTP 200 con respuesta inválida: {exc}")
                return
            with self.lock:
                self.writer.write_esi(item, payload)
                self.store.mark_complete(item, "esi")
            self.count("esi_success")
            if self.stats["esi_success"] % 100 == 0:
                print(
                    f"ESI: {self.stats['esi_success']} killmails capturados; "
                    f"cola pendiente {self.store.pending_count()}.", flush=True,
                )
            return

        diagnostic = self._headers_diagnostic(result.headers)
        body = result.body[:512].decode("utf-8", errors="replace").strip()
        message = (
            f"ESI HTTP {result.status} para killmail {item.killmail_id} hash={item.killmail_hash} "
            f"secuencia={item.sequence_id}; cabeceras={diagnostic}; cuerpo={body!r}"
        )
        if result.status == 404:
            attempts = item.attempts + 1
            if attempts >= len(ESI_404_RETRY_DELAYS_SECONDS) + 1:
                self._fallback(item, f"404 persistente tras {attempts} solicitudes ESI")
            else:
                delay = ESI_404_RETRY_DELAYS_SECONDS[attempts - 1]
                self._retry(item, delay, message)
            return
        if result.status == 420:
            try:
                reset_delay = float(result.headers.get("x-esi-error-limit-reset", "60"))
            except (TypeError, ValueError):
                reset_delay = 60.0
            delay = max(1.0, reset_delay)
            self.rate_limiter.defer(delay)
            self._retry(item, delay, message)
            return
        if result.status == 429:
            delay = retry_after_seconds(result.headers, 60.0)
            self.rate_limiter.defer(delay)
            self._retry(item, delay, message)
            return
        if 500 <= result.status <= 599:
            delay = min(300.0, 2.0 ** min(item.attempts + 1, 8))
            self._retry(item, delay, message)
            return
        if 400 <= result.status <= 499:
            self._fallback(item, f"error HTTP permanente {result.status}; {message}")
            return
        self._retry(item, 60.0, message)

    def esi_worker(self) -> None:
        try:
            while not self.stop.is_set():
                item = self.store.next_due_pending()
                if item is None:
                    wait_for = self.store.seconds_until_next_pending()
                    timeout = min(30.0, wait_for) if wait_for is not None else 30.0
                    with self.condition:
                        self.condition.wait(timeout=max(0.1, timeout))
                    continue
                self._fetch_esi(item)
        except BaseException as exc:
            self.record_worker_error("ESI", exc)

    def _checkpoint_stamp(self) -> str:
        return utc_now().strftime("%Y%m%dT%H%M%SZ")

    def checkpoint(self, initial: bool = False) -> None:
        stamp = self._checkpoint_stamp()
        with self.lock:
            self.store.connection.commit()
            parts = self.writer.rotate(stamp)
            compress_zlib(self.state_path, STATE_PATH)
        if parts:
            print(
                "Checkpoint SQLite: " + ", ".join(
                    f"{path.relative_to(ROOT)} ({path.stat().st_size:,} bytes)" for path in parts
                ), flush=True,
            )
        elif not initial:
            print(f"Checkpoint {stamp}: sin nuevas filas; cursor R2Z2={self.store.get_next_sequence()}.", flush=True)
        self.publish_checkpoint(stamp)
        with self.stats_lock:
            stats_summary = ", ".join(f"{key}={value}" for key, value in sorted(self.stats.items())) or "sin respuestas"
        print(f"Checkpoint {stamp}: ESI/R2Z2 {stats_summary}; pendientes={self.store.pending_count()}.", flush=True)

    def publish_checkpoint(self, stamp: str) -> None:
        relative_dir = OUTPUT_DIR.relative_to(ROOT).as_posix()
        subprocess.run(["git", "add", "-A", relative_dir], cwd=ROOT, check=True)
        diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT)
        if diff.returncode == 0:
            return
        if diff.returncode != 1:
            raise CaptureError("git diff --cached falló al detectar cambios")
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=ROOT, check=True)
        subprocess.run(
            ["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"],
            cwd=ROOT, check=True,
        )
        subprocess.run(
            ["git", "commit", "-m", f"Capture killmail checkpoint {stamp}"], cwd=ROOT, check=True
        )
        for attempt in range(1, 6):
            subprocess.run(["git", "fetch", "origin", "main"], cwd=ROOT, check=True)
            rebase = subprocess.run(["git", "rebase", "origin/main"], cwd=ROOT)
            if rebase.returncode != 0:
                subprocess.run(["git", "rebase", "--abort"], cwd=ROOT)
                if attempt == 5:
                    raise CaptureError("No se pudo rebasar el checkpoint de killmails sobre origin/main.")
                time.sleep(attempt * 2)
                continue
            push = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=ROOT)
            if push.returncode == 0:
                print(f"Checkpoint {stamp}: publicado en main.", flush=True)
                return
            if attempt == 5:
                raise CaptureError("No se pudo publicar el checkpoint de killmails tras cinco intentos.")
            time.sleep(attempt * 2)

    def run(self) -> int:
        self.initialize_cursor()
        # Persist the first cursor before consuming the feed, so an interrupted first run resumes safely.
        self.checkpoint(initial=True)
        print(
            f"Captura iniciada durante {self.duration_seconds}s; R2Z2 poll={R2Z2_IDLE_POLL_SECONDS}s; "
            f"checkpoint={self.checkpoint_seconds}s; cursor={self.store.get_next_sequence()}.",
            flush=True,
        )
        listener = threading.Thread(target=self.r2z2_worker, name="r2z2-listener", daemon=True)
        esi = threading.Thread(target=self.esi_worker, name="esi-killmail-fetcher", daemon=True)
        listener.start()
        esi.start()
        deadline = time.monotonic() + self.duration_seconds
        next_checkpoint = time.monotonic() + self.checkpoint_seconds
        checkpoint_errors = 0
        try:
            while time.monotonic() < deadline and not self.stop.is_set():
                remaining = min(deadline, next_checkpoint) - time.monotonic()
                if remaining > 0:
                    self.stop.wait(remaining)
                if self.stop.is_set():
                    break
                now = time.monotonic()
                if now >= next_checkpoint:
                    try:
                        self.checkpoint()
                    except Exception as exc:
                        checkpoint_errors += 1
                        log_error("No se pudo publicar el checkpoint", f"{type(exc).__name__}: {exc}")
                    next_checkpoint = time.monotonic() + self.checkpoint_seconds
        finally:
            self.stop.set()
            with self.condition:
                self.condition.notify_all()
            listener.join(timeout=REQUEST_TIMEOUT_SECONDS + 5)
            esi.join(timeout=REQUEST_TIMEOUT_SECONDS + 5)
            if listener.is_alive() or esi.is_alive():
                checkpoint_errors += 1
                log_error("Workers sin cerrar", "Un worker de captura no terminó tras el timeout HTTP.")
            try:
                self.checkpoint()
            except Exception as exc:
                checkpoint_errors += 1
                log_error("Falló el checkpoint final", f"{type(exc).__name__}: {exc}")
            with self.worker_errors_lock:
                worker_errors = list(self.worker_errors)
            print(
                f"Fin de captura: cursor={self.store.get_next_sequence()}, "
                f"pendientes={self.store.pending_count()}, errores_worker={len(worker_errors)}, "
                f"errores_checkpoint={checkpoint_errors}.", flush=True,
            )
            self.writer.close()
            self.store.close()
            self.temp_context.cleanup()
        return 1 if checkpoint_errors or worker_errors else 0


def main() -> int:
    try:
        duration_seconds = int(os.environ.get("CAPTURE_DURATION_SECONDS", DEFAULT_DURATION_SECONDS))
        checkpoint_seconds = int(os.environ.get("CHECKPOINT_SECONDS", DEFAULT_CHECKPOINT_SECONDS))
        if duration_seconds < 60 or checkpoint_seconds < 30 or checkpoint_seconds >= duration_seconds:
            raise CaptureError("CAPTURE_DURATION_SECONDS/CHECKPOINT_SECONDS tienen valores fuera de rango.")
        return CaptureRun(duration_seconds, checkpoint_seconds).run()
    except (CaptureError, OSError, sqlite3.Error, ValueError, zlib.error) as exc:
        log_error("Captura R2Z2/ESI fallida", f"{type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
