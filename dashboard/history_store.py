"""SQLite-backed chart history for the dashboard."""

from __future__ import annotations

import math
import sqlite3
import threading
import time
from pathlib import Path
from time import time as _wall_time


MAX_HISTORY_SECONDS = 7 * 24 * 60 * 60
# Table, series key, catalog. The primary key serves both reads and cleanup.
HISTORY_TABLES = (
    ("node_queue_history", "node_id", "history_nodes"),
    ("node_traffic_history", "service_id", "history_services"),
)


class HistoryStore:
    """Persist raw samples and return time-spanning chart data.

    Samples expire after seven days. The database is capped at ``max_bytes``.
    Once the main file plus WAL/SHM exceeds that footprint, the oldest samples
    in each series are removed until the compacted database is below a target
    watermark. Recent history is retained.
    """

    def __init__(self, path: str | Path, max_bytes: int = 50 * 1024 * 1024):
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        database = Path(path)
        database.parent.mkdir(parents=True, exist_ok=True)
        self._path = database
        self._max_bytes = max_bytes
        self._lock = threading.Lock()
        self._last_size_check = 0.0  # time.monotonic() of the last rolling check
        self._last_retention_check = 0.0
        self._connection = sqlite3.connect(database, check_same_thread=False)
        try:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._initialize_schema()
        except Exception:
            self._connection.close()
            raise
        self._last_retention_check = time.monotonic()

    def _initialize_schema(self) -> None:
        with self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS history_nodes (
                    node_id INTEGER PRIMARY KEY,
                    node_key TEXT NOT NULL UNIQUE
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS history_services (
                    service_id INTEGER PRIMARY KEY,
                    node_id INTEGER NOT NULL REFERENCES history_nodes(node_id),
                    service TEXT NOT NULL,
                    UNIQUE (node_id, service)
                )
                """
            )
            # One B-tree per sample table, no rowid copy or secondary indexes.
            # Series catalogs let cleanup seek each primary-key timestamp range.
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS node_queue_history (
                    node_id INTEGER NOT NULL REFERENCES history_nodes(node_id),
                    timestamp REAL NOT NULL,
                    control_queue_delay_us INTEGER NOT NULL,
                    transfer_tcp_queue_delay_us INTEGER NOT NULL,
                    transfer_udp_queue_delay_us INTEGER NOT NULL,
                    PRIMARY KEY (node_id, timestamp)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS node_traffic_history (
                    service_id INTEGER NOT NULL REFERENCES history_services(service_id),
                    timestamp REAL NOT NULL,
                    rx_bytes_per_second INTEGER NOT NULL,
                    tx_bytes_per_second INTEGER NOT NULL,
                    PRIMARY KEY (service_id, timestamp)
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ip_location_cache (
                    ip TEXT PRIMARY KEY,
                    country_code TEXT NOT NULL,
                    location_code TEXT NOT NULL,
                    city TEXT NOT NULL,
                    region TEXT NOT NULL
                ) WITHOUT ROWID
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS unlocode_country_cache (
                    country_code TEXT PRIMARY KEY,
                    csv_data TEXT NOT NULL
                ) WITHOUT ROWID
                """
            )
            self._expire_history(_wall_time())

    def _node_id(self, node_key: str, *, create: bool = False) -> int | None:
        row = self._connection.execute(
            "SELECT node_id FROM history_nodes WHERE node_key = ?", (node_key,)
        ).fetchone()
        if row is not None:
            return row[0]
        if create:
            return self._connection.execute(
                "INSERT INTO history_nodes (node_key) VALUES (?)", (node_key,)
            ).lastrowid
        return None

    def _service_id(self, node_id: int, service: str) -> int:
        row = self._connection.execute(
            "SELECT service_id FROM history_services WHERE node_id = ? AND service = ?",
            (node_id, service),
        ).fetchone()
        if row is not None:
            return row[0]
        return self._connection.execute(
            "INSERT INTO history_services (node_id, service) VALUES (?, ?)", (node_id, service)
        ).lastrowid

    def _expire_history(self, now: float) -> None:
        cutoff = now - MAX_HISTORY_SECONDS
        for table, key, catalog in HISTORY_TABLES:
            self._connection.execute(
                f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM {catalog}) "
                "AND timestamp < ?", (cutoff,),
            )

    def _maybe_expire_history(self) -> None:
        now = time.monotonic()
        if now - self._last_retention_check >= 60:
            self._expire_history(_wall_time())
            self._last_retention_check = now

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def load_ip_locations(self) -> dict[str, tuple[str, str, str, str]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT ip, country_code, location_code, city, region "
                "FROM ip_location_cache"
            ).fetchall()
        return {
            ip: (country, location, city, region)
            for ip, country, location, city, region in rows
        }

    def save_ip_location(
        self,
        ip: str,
        country_code: str,
        location_code: str,
        city: str,
        region: str,
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO ip_location_cache VALUES (?, ?, ?, ?, ?)",
                (ip, country_code, location_code, city, region),
            )

    def load_unlocode_country(self, country_code: str) -> str | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT csv_data FROM unlocode_country_cache WHERE country_code = ?",
                (country_code,),
            ).fetchone()
        return None if row is None else row[0]

    def save_unlocode_country(self, country_code: str, csv_data: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO unlocode_country_cache VALUES (?, ?)",
                (country_code, csv_data),
            )

    def _maybe_roll_if_too_large(self) -> None:
        """Remove oldest samples and compact when the DB exceeds its cap.

        Runs at most once per minute. It targets 80% of the configured limit so
        normal appends do not immediately trigger another compaction. Each
        series retains its latest sample until it expires after seven days.
        """
        now = time.monotonic()
        if now - self._last_size_check < 60.0:
            return
        self._last_size_check = now
        footprint = self._database_footprint()
        if footprint <= self._max_bytes:
            return

        target = max(1, int(self._max_bytes * 0.8))
        for _ in range(8):
            fraction = min(0.75, max(0.10, 1.0 - target / footprint))
            with self._connection:
                deleted = sum(
                    self._delete_oldest_fraction(table, key, fraction)
                    for table, key, _ in HISTORY_TABLES
                )
            if deleted == 0:
                break

            self._compact()
            footprint = self._database_footprint()
            if footprint <= target:
                break

    def _compact(self) -> None:
        # VACUUM in WAL mode writes the compacted image through the WAL.
        # Truncate both before and after it so footprint measures allocated
        # disk rather than an old or newly generated WAL segment.
        self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self._connection.execute("VACUUM")
        self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _delete_oldest_fraction(self, table: str, key: str, fraction: float) -> int:
        # Grouping follows primary-key order; each cutoff/delete then seeks a
        # single series. Keep its latest point even if other series are newer.
        series = self._connection.execute(
            f"SELECT {key}, COUNT(*) FROM {table} GROUP BY {key} HAVING COUNT(*) > 1"
        ).fetchall()
        deleted = 0
        for series_id, count in series:
            delete_count = min(count - 1, max(1, math.ceil(count * fraction)))
            cutoff = self._connection.execute(
                f"SELECT timestamp FROM {table} WHERE {key} = ? "
                "ORDER BY timestamp LIMIT 1 OFFSET ?", (series_id, delete_count - 1),
            ).fetchone()[0]
            deleted += self._connection.execute(
                f"DELETE FROM {table} WHERE {key} = ? AND timestamp <= ?", (series_id, cutoff),
            ).rowcount
        return deleted

    def _database_footprint(self) -> int:
        """Total on-disk size of the SQLite main file plus its WAL/SHM sidecars."""
        total = 0
        for suffix in ("", "-wal", "-shm"):
            sidecar = self._path.with_name(self._path.name + suffix)
            try:
                total += sidecar.stat().st_size
            except OSError:
                pass
        return total

    def append_queue(
        self,
        node_key: str,
        timestamp: float,
        control_queue_delay_us: int,
        transfer_tcp_queue_delay_us: int,
        transfer_udp_queue_delay_us: int,
    ) -> None:
        if timestamp < _wall_time() - MAX_HISTORY_SECONDS:
            return
        with self._lock, self._connection:
            self._maybe_roll_if_too_large()
            self._maybe_expire_history()
            self._connection.execute(
                "INSERT OR REPLACE INTO node_queue_history VALUES (?, ?, ?, ?, ?)",
                (
                    self._node_id(node_key, create=True),
                    timestamp,
                    control_queue_delay_us,
                    transfer_tcp_queue_delay_us,
                    transfer_udp_queue_delay_us,
                ),
            )

    def append_traffic(
        self, node_key: str, timestamp: float, services: list[tuple[str, int, int]]
    ) -> None:
        if not services:
            return
        if timestamp < _wall_time() - MAX_HISTORY_SECONDS:
            return
        with self._lock, self._connection:
            self._maybe_roll_if_too_large()
            self._maybe_expire_history()
            node_id = self._node_id(node_key, create=True)
            rows = [
                (self._service_id(node_id, service), timestamp, rx_rate, tx_rate)
                for service, rx_rate, tx_rate in services
            ]
            self._connection.executemany(
                "INSERT OR REPLACE INTO node_traffic_history VALUES (?, ?, ?, ?)", rows
            )

    def queue_history(
        self, node_key: str, since: float | None, max_points: int = 2000, *, until: float | None = None
    ) -> list[tuple[float, int, int, int]]:
        since = max(since if since is not None else -math.inf, _wall_time() - MAX_HISTORY_SECONDS)
        with self._lock, self._connection:
            self._maybe_expire_history()
            node_id = self._node_id(node_key)
            if node_id is None:
                return []
            conditions = "node_id = ? AND timestamp >= ?"
            params: tuple = (node_id, since)
            if until is not None:
                conditions += " AND timestamp <= ?"
                params += (until,)
            count, first, last = self._connection.execute(
                "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM node_queue_history "
                f"WHERE {conditions}",
                params,
            ).fetchone()
            if not count:
                return []
            if count <= max_points or first == last:
                return self._connection.execute(
                    "SELECT timestamp, control_queue_delay_us, transfer_tcp_queue_delay_us, "
                    "transfer_udp_queue_delay_us FROM node_queue_history "
                    f"WHERE {conditions} ORDER BY timestamp",
                    params,
                ).fetchall()
            bucket = (last - first) / (max_points - 1)
            return self._connection.execute(
                "SELECT MIN(timestamp), MAX(control_queue_delay_us), "
                "MAX(transfer_tcp_queue_delay_us), MAX(transfer_udp_queue_delay_us) "
                f"FROM node_queue_history WHERE {conditions} "
                "GROUP BY CAST((timestamp - ?) / ? AS INTEGER) ORDER BY 1",
                (*params, first, bucket),
            ).fetchall()

    def traffic_history(
        self, node_key: str, since: float | None, services: list[str], max_points: int = 2000, *, until: float | None = None
    ) -> dict[str, list[tuple[float, int, int]]]:
        since = max(since if since is not None else -math.inf, _wall_time() - MAX_HISTORY_SECONDS)
        result: dict[str, list[tuple[float, int, int]]] = {}
        with self._lock, self._connection:
            self._maybe_expire_history()
            node_id = self._node_id(node_key)
            if node_id is None:
                return {service: [] for service in sorted(set(services))}
            service_conditions = "h.service_id = s.service_id AND h.timestamp >= ?"
            service_params: tuple = (node_id, since)
            if until is not None:
                service_conditions += " AND h.timestamp <= ?"
                service_params += (until,)
            stored_services = dict(self._connection.execute(
                "SELECT s.service, s.service_id FROM history_services s WHERE s.node_id = ? "
                "AND EXISTS (SELECT 1 FROM node_traffic_history h "
                f"WHERE {service_conditions})",
                service_params,
            ).fetchall())
            names = sorted(set(services).union(stored_services))
            for service in names:
                if service not in stored_services:
                    result[service] = []
                    continue
                conditions = "service_id = ? AND timestamp >= ?"
                params: tuple = (stored_services[service], since)
                if until is not None:
                    conditions += " AND timestamp <= ?"
                    params += (until,)
                count, first, last = self._connection.execute(
                    "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM node_traffic_history "
                    f"WHERE {conditions}",
                    params,
                ).fetchone()
                if not count:
                    result[service] = []
                    continue
                if count <= max_points or first == last:
                    rows = self._connection.execute(
                        "SELECT timestamp, rx_bytes_per_second, tx_bytes_per_second "
                        f"FROM node_traffic_history WHERE {conditions} ORDER BY timestamp",
                        params,
                    ).fetchall()
                else:
                    bucket = (last - first) / (max_points - 1)
                    rows = self._connection.execute(
                        "SELECT MIN(timestamp), CAST(AVG(rx_bytes_per_second) AS INTEGER), "
                        "CAST(AVG(tx_bytes_per_second) AS INTEGER) FROM node_traffic_history "
                        f"WHERE {conditions} "
                        "GROUP BY CAST((timestamp - ?) / ? AS INTEGER) ORDER BY 1",
                        (*params, first, bucket),
                    ).fetchall()
                result[service] = rows
        return result
