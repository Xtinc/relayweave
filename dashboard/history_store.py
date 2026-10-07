"""SQLite-backed chart history for the dashboard."""

from __future__ import annotations

import math
import sqlite3
import threading
import time
from pathlib import Path


class HistoryStore:
    """Persist raw samples and return time-spanning chart data.

    The database is capped at ``max_bytes``. Once the main file plus WAL/SHM
    exceeds that footprint, the oldest timestamp groups are removed until the
    compacted database is below a target watermark. Recent history is retained.
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
        self._connection = sqlite3.connect(database, check_same_thread=False)
        with self._connection:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            # Old single-node tables are not part of the current schema. Keeping
            # them would consume the rolling size budget without being queried.
            self._connection.execute("DROP TABLE IF EXISTS load_history")
            self._connection.execute("DROP TABLE IF EXISTS traffic_history")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS node_queue_history (
                    node_key TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    control_queue_delay_us INTEGER NOT NULL,
                    transfer_tcp_queue_delay_us INTEGER NOT NULL,
                    transfer_udp_queue_delay_us INTEGER NOT NULL,
                    PRIMARY KEY (node_key, timestamp)
                )
                """
            )
            if self._connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='node_load_history'"
            ).fetchone():
                self._connection.execute(
                    "INSERT OR IGNORE INTO node_queue_history "
                    "SELECT node_key, timestamp, control_queue_delay_us, "
                    "transfer_tcp_queue_delay_us, transfer_udp_queue_delay_us "
                    "FROM node_load_history"
                )
                self._connection.execute("DROP TABLE node_load_history")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS node_traffic_history (
                    node_key TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    service TEXT NOT NULL,
                    rx_bytes_per_second INTEGER NOT NULL,
                    tx_bytes_per_second INTEGER NOT NULL,
                    PRIMARY KEY (node_key, timestamp, service)
                )
                """
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS node_traffic_history_service_time "
                "ON node_traffic_history(node_key, service, timestamp)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS node_queue_history_time ON node_queue_history(timestamp)"
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS node_traffic_history_time "
                "ON node_traffic_history(timestamp)"
            )
            location_columns = {
                row[1]
                for row in self._connection.execute(
                    "PRAGMA table_info(ip_location_cache)"
                )
            }
            expected_location_columns = {
                "ip",
                "country_code",
                "location_code",
                "city",
                "region",
            }
            if location_columns and location_columns != expected_location_columns:
                self._connection.execute("DROP TABLE ip_location_cache")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ip_location_cache (
                    ip TEXT PRIMARY KEY,
                    country_code TEXT NOT NULL,
                    location_code TEXT NOT NULL,
                    city TEXT NOT NULL,
                    region TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS unlocode_country_cache (
                    country_code TEXT PRIMARY KEY,
                    csv_data TEXT NOT NULL
                )
                """
            )

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
        normal appends do not immediately trigger another compaction.
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
                deleted = self._delete_oldest_fraction("node_queue_history", fraction)
                deleted += self._delete_oldest_fraction("node_traffic_history", fraction)
            if deleted == 0:
                break

            # VACUUM in WAL mode writes the compacted image through the WAL.
            # Truncate both before and after it so footprint measures allocated
            # disk rather than an old or newly generated WAL segment.
            self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._connection.execute("VACUUM")
            self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            footprint = self._database_footprint()
            if footprint <= target:
                break

    def _delete_oldest_fraction(self, table: str, fraction: float) -> int:
        timestamps = self._connection.execute(
            f"SELECT COUNT(DISTINCT timestamp) FROM {table}"
        ).fetchone()[0]
        if timestamps <= 1:
            return 0
        delete_count = min(timestamps - 1, max(1, math.ceil(timestamps * fraction)))
        cutoff = self._connection.execute(
            f"SELECT DISTINCT timestamp FROM {table} ORDER BY timestamp LIMIT 1 OFFSET ?",
            (delete_count - 1,),
        ).fetchone()[0]
        cursor = self._connection.execute(f"DELETE FROM {table} WHERE timestamp <= ?", (cutoff,))
        return cursor.rowcount

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
        with self._lock, self._connection:
            self._maybe_roll_if_too_large()
            self._connection.execute(
                "INSERT OR REPLACE INTO node_queue_history VALUES (?, ?, ?, ?, ?)",
                (
                    node_key,
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
        with self._lock, self._connection:
            self._maybe_roll_if_too_large()
            self._connection.executemany(
                "INSERT OR REPLACE INTO node_traffic_history VALUES (?, ?, ?, ?, ?)",
                (
                    (node_key, timestamp, service, rx_rate, tx_rate)
                    for service, rx_rate, tx_rate in services
                ),
            )

    def queue_history(
        self, node_key: str, since: float | None, max_points: int = 2000
    ) -> list[tuple[float, int, int, int]]:
        conditions = "node_key = ?"
        params: tuple = (node_key,)
        if since is not None:
            conditions += " AND timestamp >= ?"
            params += (since,)
        with self._lock:
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
        self, node_key: str, since: float | None, services: list[str], max_points: int = 2000
    ) -> dict[str, list[tuple[float, int, int]]]:
        result: dict[str, list[tuple[float, int, int]]] = {}
        with self._lock:
            service_conditions = "node_key = ?"
            service_params: tuple = (node_key,)
            if since is not None:
                service_conditions += " AND timestamp >= ?"
                service_params += (since,)
            stored_services = self._connection.execute(
                "SELECT DISTINCT service FROM node_traffic_history "
                f"WHERE {service_conditions}",
                service_params,
            ).fetchall()
            names = sorted(set(services).union(row[0] for row in stored_services))
            for service in names:
                conditions = "node_key = ? AND service = ?"
                params: tuple = (node_key, service)
                if since is not None:
                    conditions += " AND timestamp >= ?"
                    params += (since,)
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
