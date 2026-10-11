"""HistoryStore retention tests that require only the Python standard library."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from history_store import HistoryStore, MAX_HISTORY_SECONDS


class HistoryStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        clock = patch("history_store._wall_time", return_value=6000.0)
        self.clock = clock.start()
        self.addCleanup(clock.stop)

    def test_seven_day_retention_expires_both_tables_and_preserves_caches(self) -> None:
        self.clock.return_value = MAX_HISTORY_SECONDS + 100
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "history.sqlite3"
            store = HistoryStore(database)
            store.save_ip_location("1.1.1.1", "AU", "SYD", "Sydney", "NSW")
            for timestamp in (99, 100, 101):
                store.append_queue("node-a", timestamp, 1, 2, 3)
                store.append_traffic("node-a", timestamp, [("ssh", 4, 5)])
            self.assertEqual([row[0] for row in store.queue_history("node-a", None)], [100, 101])
            self.assertEqual([row[0] for row in store.traffic_history("node-a", None, ["ssh"])["ssh"]], [100, 101])
            # Advance time without an append: reads must still hide expired rows.
            self.clock.return_value += 1
            self.assertEqual([row[0] for row in store.queue_history("node-a", None)], [101])
            self.assertEqual([row[0] for row in store.traffic_history("node-a", None, ["ssh"])["ssh"]], [101])
            store._last_retention_check = 0
            store.queue_history("node-a", None)
            for table in ("node_queue_history", "node_traffic_history"):
                self.assertEqual(store._connection.execute(f"SELECT COUNT(*) FROM {table} WHERE timestamp < 101").fetchone()[0], 0)
            store.append_queue("node-b", self.clock.return_value, 1, 2, 3)
            for table in ("node_queue_history", "node_traffic_history"):
                self.assertGreaterEqual(store._connection.execute(f"SELECT MIN(timestamp) FROM {table}").fetchone()[0], 101)
            store.close()
            self.clock.return_value += 1
            store = HistoryStore(database)
            try:
                self.assertEqual(store.queue_history("node-a", None), [])
                self.assertEqual(store.traffic_history("node-a", None, ["ssh"])["ssh"], [])
                self.assertIn("1.1.1.1", store.load_ip_locations())
                for table in ("node_queue_history", "node_traffic_history"):
                    self.assertEqual(store._connection.execute(f"SELECT COUNT(*) FROM {table} WHERE timestamp < 102").fetchone()[0], 0)
            finally:
                store.close()

    def test_zoom_bounds_are_applied_before_downsampling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            try:
                for timestamp in range(100):
                    store.append_queue("node-a", timestamp, timestamp, 2, 3)
                    store.append_traffic("node-a", timestamp, [("ssh", timestamp, 5)])
                queue = store.queue_history("node-a", 10, max_points=3, until=12)
                traffic = store.traffic_history("node-a", 10, ["ssh"], max_points=3, until=12)["ssh"]
                self.assertEqual([row[0] for row in queue], [10, 11, 12])
                self.assertEqual([row[0] for row in traffic], [10, 11, 12])
            finally:
                store.close()

    def test_load_history_is_migrated_to_queue_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "history.sqlite3"
            connection = sqlite3.connect(database)
            with connection:
                connection.execute(
                    "CREATE TABLE node_load_history ("
                    "node_key TEXT, timestamp REAL, control_queue_delay_us INTEGER, "
                    "transfer_tcp_queue_delay_us INTEGER, transfer_udp_queue_delay_us INTEGER, "
                    "rtt_ms REAL)"
                )
                connection.execute(
                    "INSERT INTO node_load_history VALUES ('node-a', 10, 1, 2, 3, 4)"
                )
            connection.close()

            store = HistoryStore(database)
            try:
                self.assertEqual(store.queue_history("node-a", None), [(10.0, 1, 2, 3)])
                self.assertIsNone(
                    store._connection.execute(
                        "SELECT 1 FROM sqlite_master WHERE name='node_load_history'"
                    ).fetchone()
                )
            finally:
                store.close()

    def test_old_location_cache_is_replaced(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "history.sqlite3"
            connection = sqlite3.connect(database)
            with connection:
                connection.execute(
                    "CREATE TABLE ip_location_cache (ip TEXT PRIMARY KEY, location TEXT NOT NULL)"
                )
                connection.execute(
                    "INSERT INTO ip_location_cache VALUES ('8.8.8.8', '🇺🇸 MV')"
                )
            connection.close()

            store = HistoryStore(database)
            try:
                self.assertEqual(store.load_ip_locations(), {})
                columns = {
                    row[1]
                    for row in store._connection.execute(
                        "PRAGMA table_info(ip_location_cache)"
                    )
                }
                self.assertEqual(
                    columns,
                    {"ip", "country_code", "location_code", "city", "region"},
                )
            finally:
                store.close()

    def test_same_timestamp_is_isolated_by_node(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            try:
                store.append_queue("node-a", 10.0, 1, 2, 3)
                store.append_queue("node-b", 10.0, 5, 6, 7)
                store.append_traffic("node-a", 10.0, [("ssh", 11, 12)])
                store.append_traffic("node-b", 10.0, [("ssh", 21, 22)])
                self.assertEqual(store.queue_history("node-a", None), [(10.0, 1, 2, 3)])
                self.assertEqual(store.queue_history("node-b", None), [(10.0, 5, 6, 7)])
                self.assertEqual(
                    store.traffic_history("node-a", None, ["ssh"])["ssh"][0][1:], (11, 12)
                )
                self.assertEqual(
                    store.traffic_history("node-b", None, ["ssh"])["ssh"][0][1:], (21, 22)
                )
            finally:
                store.close()

    def test_downsampling_preserves_earliest_and_latest_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            try:
                for timestamp in range(10):
                    store.append_queue("node-a", float(timestamp), timestamp, timestamp, timestamp)
                    store.append_traffic(
                        "node-a", float(timestamp), [("ssh", timestamp, timestamp)]
                    )

                load = store.queue_history("node-a", None, max_points=3)
                traffic = store.traffic_history(
                    "node-a", None, ["ssh"], max_points=3
                )["ssh"]
                self.assertEqual((load[0][0], load[-1][0]), (0.0, 9.0))
                self.assertEqual((traffic[0][0], traffic[-1][0]), (0.0, 9.0))
            finally:
                store.close()

    def test_size_limit_discards_oldest_samples_and_retains_recent_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3", max_bytes=1 << 30)
            try:
                rows = [
                    ("node-a", float(index), index, index, index)
                    for index in range(6000)
                ]
                traffic = [
                    ("node-a", float(index), "ssh", index, index) for index in range(6000)
                ]
                with store._connection:
                    store._connection.executemany(
                        "INSERT INTO node_queue_history VALUES (?, ?, ?, ?, ?)", rows
                    )
                    store._connection.executemany(
                        "INSERT INTO node_traffic_history VALUES (?, ?, ?, ?, ?)", traffic
                    )
                before = store._database_footprint()
                store._max_bytes = before * 3 // 4
                store._last_size_check = 0.0

                store.append_queue("node-a", 6000.0, 1, 2, 3)

                self.assertLessEqual(store._database_footprint(), store._max_bytes)
                load_range = store._connection.execute(
                    "SELECT MIN(timestamp), MAX(timestamp) FROM node_queue_history"
                ).fetchone()
                traffic_range = store._connection.execute(
                    "SELECT MIN(timestamp), MAX(timestamp) FROM node_traffic_history"
                ).fetchone()
                self.assertGreater(load_range[0], 0.0)
                self.assertEqual(load_range[1], 6000.0)
                self.assertGreater(traffic_range[0], 0.0)
                self.assertEqual(traffic_range[1], 5999.0)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
