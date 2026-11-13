"""HistoryStore query and retention tests using the standard library."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from history_store import HISTORY_TABLES, HistoryStore, MAX_HISTORY_SECONDS



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

    def test_current_database_preserves_samples_and_caches_across_restarts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "history.sqlite3"
            store = HistoryStore(database)
            store.append_queue("node-a", 10.25, 0, 2, 3)
            store.append_queue("node-a", 11.25, 4, 5, 6)
            store.append_queue("node-b", 10.25, 7, 8, 9)
            store.append_traffic("node-a", 10.25, [("ssh", 0, 12)])
            store.append_traffic("node-a", 11.25, [("ssh", 13, 14)])
            store.append_traffic("node-b", 10.25, [("ssh", 21, 22), ("已下线服务", 31, 32)])
            store.save_ip_location("1.1.1.1", "AU", "SYD", "Sydney", "NSW")
            store.save_unlocode_country("AU", "cached country data")
            store.close()
            store = HistoryStore(database)
            try:
                self.assertEqual(store.queue_history("node-a", None), [(10.25, 0, 2, 3), (11.25, 4, 5, 6)])
                self.assertEqual(store.queue_history("node-b", None), [(10.25, 7, 8, 9)])
                self.assertEqual(store.traffic_history("node-a", None, ["missing"]), {
                    "missing": [], "ssh": [(10.25, 0, 12), (11.25, 13, 14)],
                })
                self.assertEqual(store.traffic_history("node-b", 10, [], until=11), {
                    "ssh": [(10.25, 21, 22)], "已下线服务": [(10.25, 31, 32)],
                })
                self.assertEqual(store.load_ip_locations(), {"1.1.1.1": ("AU", "SYD", "Sydney", "NSW")})
                self.assertEqual(store.load_unlocode_country("AU"), "cached country data")
                store.append_queue("node-a", 10.25, 41, 42, 43)
                store.append_traffic("node-a", 10.25, [("ssh", 51, 52), ("new", 61, 62)])
                store.append_queue("node-c", 12.25, 71, 72, 73)
                store.append_traffic("node-c", 12.25, [("ssh", 81, 82)])
                self.assertEqual(store.queue_history("node-a", None), [(10.25, 41, 42, 43), (11.25, 4, 5, 6)])
                self.assertEqual(store.traffic_history("node-a", None, []), {
                    "new": [(10.25, 61, 62)], "ssh": [(10.25, 51, 52), (11.25, 13, 14)],
                })
                self.assertEqual(store.traffic_history("node-b", None, ["ssh"])["ssh"], [(10.25, 21, 22)])
                self.assertEqual(store.queue_history("node-c", None), [(12.25, 71, 72, 73)])
                self.assertEqual(store.traffic_history("node-c", None, []), {"ssh": [(12.25, 81, 82)]})
                self.assertEqual(store._connection.execute("PRAGMA foreign_key_check").fetchall(), [])
                self.assertEqual(store._connection.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
            finally:
                store.close()

    def test_primary_storage_serves_both_queries_and_expiration_without_secondary_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            try:
                store.append_queue("node-a", 10.25, 1, 2, 3)
                store.append_traffic("node-a", 10.25, [("ssh", 4, 5)])
                indexes = store._connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' "
                    "AND tbl_name IN ('node_queue_history', 'node_traffic_history') ORDER BY name"
                ).fetchall()
                self.assertEqual(indexes, [])
                for table, key, catalog in HISTORY_TABLES:
                    with self.subTest(table=table):
                        plan = store._connection.execute(
                            f"EXPLAIN QUERY PLAN SELECT * FROM {table} "
                            f"WHERE {key} = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp",
                            (1, 0, 100),
                        ).fetchall()
                        self.assertTrue(any("USING PRIMARY KEY" in row[3] for row in plan), plan)
                        self.assertFalse(any("TEMP B-TREE" in row[3] for row in plan), plan)
                        plan = store._connection.execute(
                            f"EXPLAIN QUERY PLAN DELETE FROM {table} "
                            f"WHERE {key} IN (SELECT {key} FROM {catalog}) AND timestamp < ?", (10,),
                        ).fetchall()
                        self.assertTrue(any("USING PRIMARY KEY" in row[3] for row in plan), plan)
            finally:
                store.close()

    def test_failed_append_rolls_back_new_names_and_samples(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3")
            try:
                with self.assertRaises(sqlite3.IntegrityError):
                    store.append_traffic("new-node", 10.25, [("good", 1, 2), ("invalid", None, 3)])
                self.assertEqual(store._connection.execute("SELECT * FROM history_nodes").fetchall(), [])
                self.assertEqual(store._connection.execute("SELECT * FROM history_services").fetchall(), [])
                self.assertEqual(store.traffic_history("new-node", None, ["good"]), {"good": []})
                store.append_traffic("new-node", 10.25, [("good", 4, 5)])
                self.assertEqual(store.traffic_history("new-node", None, []), {"good": [(10.25, 4, 5)]})
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
                self.assertEqual([row[1] for row in load], [4, 8, 9])
                self.assertEqual([row[1] for row in traffic], [2, 6, 9])
            finally:
                store.close()

    def test_size_limit_discards_oldest_samples_and_retains_recent_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3", max_bytes=1 << 30)
            try:
                # Seed many samples without thousands of independent commits.
                with store._connection:
                    node_id = store._node_id("node-a", create=True)
                    service_id = store._service_id(node_id, "ssh")
                rows = [
                    (node_id, float(index), index, index, index)
                    for index in range(6000)
                ]
                traffic = [
                    (service_id, float(index), index, index) for index in range(6000)
                ]
                with store._connection:
                    store._connection.executemany(
                        "INSERT INTO node_queue_history VALUES (?, ?, ?, ?, ?)", rows
                    )
                    store._connection.executemany(
                        "INSERT INTO node_traffic_history VALUES (?, ?, ?, ?)", traffic
                    )
                # An inactive series' latest sample must also survive trimming,
                # even though it is older than the active series' latest point.
                for timestamp in (5.25, 6.25):
                    store.append_queue("offline-node", timestamp, 7, 8, 9)
                    store.append_traffic("offline-node", timestamp, [("ssh", 10, 11)])
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
                self.assertEqual(store.queue_history("offline-node", None), [(6.25, 7, 8, 9)])
                self.assertEqual(store.traffic_history("offline-node", None, []), {"ssh": [(6.25, 10, 11)]})
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
