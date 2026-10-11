"""Focused tests for single-connection cluster dashboard behavior."""

from __future__ import annotations

import gzip
import json
import math
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import dashboard as dashboard_module
from dashboard import (
    MAX_HISTORY_RANGE_SECONDS,
    _format_endpoint,
    _history_after,
    _history_cursor,
    _history_since,
    _load_agent_config,
    _node_snapshot,
)
from history_store import HistoryStore
from flow_map import FlowCollection
from proxy_client import ProxyControlClient
from proxy_protocol import CtrlMessage, HEADER_LENGTH, from_cbor, pack_frame


class _Socket:
    def __init__(self, reply: CtrlMessage | None = None):
        self.incoming = bytearray(pack_frame(reply)) if reply else bytearray()
        self.outgoing = bytearray()
        self.timeout = None

    def settimeout(self, timeout) -> None:
        self.timeout = timeout

    def sendall(self, data: bytes) -> None:
        self.outgoing.extend(data)

    def recv(self, length: int) -> bytes:
        result = bytes(self.incoming[:length])
        del self.incoming[:length]
        return result


def _status(
    request_id: int,
    node_id: str,
    uptime_ms: int = 1234,
) -> CtrlMessage:
    return CtrlMessage(
        "server.status.reported",
        {
            "request_id": request_id,
            "node_id": node_id,
            "uptime_ms": uptime_ms,
            "relays": [],
            "services": [
                {
                    "service": "ssh",
                    "protocol": "tcp",
                    "rx_bytes": 10,
                    "tx_bytes": 20,
                    "rx_bytes_per_second": 30,
                    "tx_bytes_per_second": 40,
                    "accessors": {
                        "192.0.2.10:49152": 2,
                        "192.0.2.11:49153": 1,
                    },
                }
            ],
        },
    )


def _request_id(data: bytes) -> int:
    length = int.from_bytes(data[:HEADER_LENGTH], "big")
    request = from_cbor(data[HEADER_LENGTH : HEADER_LENGTH + length])
    assert request.command == "server.cluster" and request.params
    return request.params["request_id"]


def _messages(data: bytes) -> list[CtrlMessage]:
    messages = []
    offset = 0
    while offset < len(data):
        length = int.from_bytes(data[offset : offset + HEADER_LENGTH], "big")
        start = offset + HEADER_LENGTH
        messages.append(from_cbor(data[start : start + length]))
        offset = start + length
    return messages


def _topology(
    request_id: int,
    *,
    version: int = 7,
) -> CtrlMessage:
    return CtrlMessage(
        "topology.snapshot",
        {
            "request_id": request_id,
            "epoch": 11,
            "snapshot_version": version,
            "created_age_ms": 25,
            "nodes": [
                {
                    "node_id": identity,
                    "address": "127.0.0.1",
                    "report_age_ms": 100,
                    "control_queue_delay_us": 1,
                    "transfer_tcp_queue_delay_us": 2,
                    "transfer_udp_queue_delay_us": 3,
                } for identity in ("node-a", "node-b")
            ],
            "links": [
                {
                    "source": source,
                    "destination": destination,
                    "rtt_ms": 12.5,
                    "jitter_ms": 1.25,
                    "loss_rate": 0.05,
                    "transmitted": 20,
                    "received": 19,
                    "completed": 20,
                    "age_ms": 250,
                    "quality": {"cost": 25.0, "confidence": 0.8, "usable": True},
                } for source, destination in (("node-a", "node-b"), ("node-b", "node-a"))
            ],
        },
    )


def _without_accessors(message: CtrlMessage) -> CtrlMessage:
    assert message.params
    message.params["services"][0].pop("accessors")
    return message


class DashboardConfigTest(unittest.TestCase):
    def test_only_entry_server_is_loaded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "client.json"
            path.write_text(
                json.dumps(
                    {
                        "server": {"host": "entry.example", "port": 18443},
                        "certificate": {
                            "ca_file": "ca.pem",
                            "certificate_chain": "client.pem",
                            "private_key": "client.key",
                        },
                    }
                ),
                encoding="utf-8",
            )
            config = _load_agent_config(path)
            self.assertEqual(config["host"], "entry.example")
            self.assertEqual(config["port"], 18443)
            self.assertNotIn("expected_node_id", config)

    def test_endpoint_formatting_is_unambiguous(self) -> None:
        self.assertEqual(_format_endpoint("192.0.2.1", 443), "192.0.2.1:443")
        self.assertEqual(_format_endpoint("2001:db8::1", 443), "[2001:db8::1]:443")

    def test_history_range_rejects_invalid_boundaries(self) -> None:
        with patch.object(dashboard_module, "request", SimpleNamespace(args={"queue_range": "all"})):
            self.assertIsNone(_history_since("queue_range", 1000.0))
        with patch.object(dashboard_module, "request", SimpleNamespace(args={"queue_range": "300"})):
            self.assertEqual(_history_since("queue_range", 1000.0), 700.0)

        def reject(_status: int, description: str):
            raise ValueError(description)

        invalid = ("nan", "inf", "-inf", "0", str(MAX_HISTORY_RANGE_SECONDS + 1))
        for value in invalid:
            with (
                self.subTest(value=value),
                patch.object(dashboard_module, "request", SimpleNamespace(args={"queue_range": value})),
                patch.object(dashboard_module, "abort", reject),
                self.assertRaisesRegex(ValueError, "finite number"),
            ):
                _history_since("queue_range", 1000.0)

    def test_incremental_cursor_is_strict_and_tracks_latest_sample(self) -> None:
        node = SimpleNamespace(
            service_traffic_updated=102.0,
        )
        snap = SimpleNamespace(
            nodes={"node-a": node}, topology=SimpleNamespace(received_at=101.0)
        )
        self.assertEqual(_history_cursor(snap, 100.0), 102.0)
        self.assertGreater(math.nextafter(102.0, math.inf), 102.0)

        with patch.object(dashboard_module, "request", SimpleNamespace(args={"history_after": "102"})):
            self.assertEqual(_history_after(), 102.0)

    def test_incremental_snapshot_queries_strictly_after_cursor(self) -> None:
        calls = []
        snap = SimpleNamespace(
            nodes={},
            connected=True,
            connecting=False,
            entry_node_id="entry",
            peer="127.0.0.1:18443",
            last_error="",
            topology=None,
            flow_map=FlowCollection().snapshot(True),
        )
        client = SimpleNamespace(snapshot=lambda load, traffic: calls.append((load, traffic)) or snap)
        with patch.object(dashboard_module, "_client", client):
            response = dashboard_module.app.test_client().get(
                "/api/snapshot?queue_range=300&bandwidth_range=300&history_after=100"
            )
        self.assertEqual(response.status_code, 200)
        self.assertGreater(calls[0][0], 100.0)
        self.assertGreater(calls[0][1], 100.0)
        self.assertEqual(response.json["history_mode"], "delta")
        self.assertEqual(response.json["history_cursor"], 100.0)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_dashboard_page_is_gzipped_when_browser_accepts_it(self) -> None:
        client = dashboard_module.app.test_client()
        plain = client.get("/")
        compressed = client.get("/", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(compressed.headers["Content-Encoding"], "gzip")
        self.assertIn("Accept-Encoding", compressed.headers["Vary"])
        self.assertEqual(gzip.decompress(compressed.data), plain.data)
        self.assertLess(len(compressed.data), len(plain.data))
        disabled = client.get("/", headers={"Accept-Encoding": "gzip;q=0"})
        self.assertNotIn("Content-Encoding", disabled.headers)

    def test_invalid_flow_filters_are_rejected_before_snapshot(self) -> None:
        with patch.object(dashboard_module, "_client") as collector:
            http = dashboard_module.app.test_client()
            for query in ("flow_protocol=http", "flow_service=" + "a" * 65):
                with self.subTest(query=query):
                    self.assertEqual(http.get("/api/snapshot?" + query).status_code, 400)
            collector.snapshot.assert_not_called()

    def test_snapshot_includes_accessor_locations(self) -> None:
        traffic = SimpleNamespace(
            service="ssh",
            protocol="tcp",
            rx_bytes=1,
            tx_bytes=2,
            rx_bytes_per_second=3,
            tx_bytes_per_second=4,
            accessors={"8.8.8.8:53": 1},
        )
        node = SimpleNamespace(
            node_id="node-a",
            connected=True,
            last_error="",
            uptime_ms=1000,
            service_traffic={"ssh": traffic},
            service_traffic_updated=100.0,
            traffic_history={},
            queue_history=[],
        )
        location = {"label": "🇺🇸 MTV", "title": "Mountain View，California"}
        locations = SimpleNamespace(location_for_endpoint=lambda endpoint: location)
        with patch.object(dashboard_module, "_location_cache", locations):
            result = _node_snapshot(node, 101.0)
        self.assertEqual(
            result["service_traffic"][0]["accessor_locations"],
            {"8.8.8.8:53": location},
        )
        self.assertNotIn("queue", result)
        self.assertNotIn("peer", result)


class ClusterPollingTest(unittest.TestCase):
    def make_client(self, directory: str) -> ProxyControlClient:
        store = HistoryStore(Path(directory) / "history.sqlite3")
        self.addCleanup(store.close)
        return ProxyControlClient(
            host="127.0.0.1",
            port=18443,
            ca_file="unused",
            cert_file="unused",
            key_file="unused",
            history_store=store,
        )

    def test_identity_handshake_precedes_cluster_queries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket(CtrlMessage("server.identified", {"node_id": "node-a"}))
            try:
                self.assertEqual(client._identify_server(sock), "node-a")
                length = int.from_bytes(sock.outgoing[:HEADER_LENGTH], "big")
                request = from_cbor(bytes(sock.outgoing[HEADER_LENGTH : HEADER_LENGTH + length]))
                self.assertEqual(request, CtrlMessage("server.identify", {}))
                self.assertIsNone(sock.timeout)
            finally:
                client.stop()
                client._history_store.close()

    def test_one_round_accepts_multiple_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                request_id = _request_id(sock.outgoing)
                client._handle_message(sock, _status(request_id, "node-a"))
                client._handle_message(sock, _status(request_id, "node-b"))
                snapshot = client.snapshot(None, None)
                self.assertEqual(set(snapshot.nodes), {"node-a", "node-b"})
                self.assertTrue(all(node.connected for node in snapshot.nodes.values()))
                self.assertEqual(snapshot.nodes["node-a"].service_traffic["ssh"].rx_bytes, 10)
                self.assertEqual(
                    snapshot.nodes["node-a"].service_traffic["ssh"].accessors,
                    {"192.0.2.10:49152": 2, "192.0.2.11:49153": 1},
                )
                self.assertEqual(len(snapshot.nodes["node-a"].queue_history), 0)
            finally:
                client.stop()
                client._history_store.close()

    def test_flow_round_can_complete_after_newer_node_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                first = _request_id(sock.outgoing)
                client._handle_message(sock, _topology(first))
                head = _status(first, "node-a")
                head.params["relays"] = [dict(mode="multi", role="ingress", epoch=11,
                    flow_id=2**64-1, service="ssh", protocol="tcp", agent_peer="192.0.2.1:99",
                    path=["node-a", "node-b"])]
                client._handle_message(sock, head)
                sock.outgoing.clear()
                client._poll_once(sock)
                newer = _request_id(sock.outgoing)
                client._handle_message(sock, _status(newer, "node-b", 999))
                tail = _status(first, "node-b", 123)
                tail.params["relays"] = [dict(mode="multi", role="egress", epoch=11,
                    flow_id=2**64-1, service="ssh", protocol="tcp", agent_peer="[2001:db8::1]:55")]
                client._handle_message(sock, tail)
                snap = client.snapshot(None, None)
                self.assertEqual(snap.nodes["node-b"].uptime_ms, 999)
                self.assertEqual(snap.flow_map["flow_count"], 1)
                self.assertEqual(snap.flow_map["routes"][0]["businesses"][0]["flow_id"], str(2**64-1))
                client._handle_message(sock, tail)
                self.assertEqual(client.snapshot(None, None).flow_map["flow_count"], 1)
                client._handle_message(sock, _status(newer, "node-a"))
                self.assertEqual(client.snapshot(None, None).flow_map["flow_count"], 0)
                client._set(connected=False)
                self.assertEqual(client.snapshot(None, None).flow_map["state"], "disconnected")
            finally:
                client.stop()
                client._history_store.close()

    def test_status_requires_relays_without_committing_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._poll_once(sock)
                message = _status(_request_id(sock.outgoing), "node-a")
                del message.params["relays"]
                with self.assertRaises(ValueError):
                    client._handle_message(sock, message)
                self.assertEqual(client.snapshot(None, None).nodes, {})
            finally:
                client.stop()
                client._history_store.close()

    def test_poll_collects_a_complete_topology_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                messages = _messages(bytes(sock.outgoing))
                self.assertEqual(
                    [message.command for message in messages],
                    ["server.cluster", "topology.query"],
                )
                topology_request = messages[1].params["request_id"]
                client._handle_message(sock, _status(topology_request, "stale-node"))
                self.assertIn("stale-node", client.snapshot(None, None).nodes)
                client._handle_message(sock, _topology(topology_request))
                topology = client.snapshot(None, None).topology
                self.assertIsNotNone(topology)
                self.assertEqual(topology.version, 7)
                self.assertEqual({node.node_id for node in topology.nodes}, {"node-a", "node-b"})
                self.assertEqual(
                    {(link.source, link.destination) for link in topology.links},
                    {("node-a", "node-b"), ("node-b", "node-a")},
                )
                self.assertEqual(
                    client._history_store.queue_history("node-a", None)[0][1:],
                    (1, 2, 3),
                )
                self.assertNotIn("stale-node", client.snapshot(None, None).nodes)
                client._mark_session_connected("new-entry")
                self.assertIsNone(client.snapshot(None, None).topology)
            finally:
                client.stop()
                client._history_store.close()

    def test_topology_age_includes_request_and_assembly_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                with patch("proxy_client.time.monotonic", return_value=100.0):
                    client._poll_once(sock)
                request = _request_id(sock.outgoing)
                message = _topology(request)
                message.params["created_age_ms"] = 0
                for node in message.params["nodes"]:
                    node["report_age_ms"] = 14000
                with patch("proxy_client.time.monotonic", return_value=102.0):
                    client._handle_message(sock, message)
                topology = client.snapshot(None, None).topology
                rendered = dashboard_module._topology_snapshot(topology, 102.0, True)
                self.assertEqual(rendered["age_s"], 2.0)
                self.assertFalse(rendered["stale"])
                self.assertTrue(all(not link["usable"] for link in rendered["links"]))
                self.assertTrue(all(node["control_queue_delay_us"] is None for node in rendered["nodes"]))
                self.assertAlmostEqual(rendered["links"][0]["quality_score"], 100 * math.exp(-0.25))
                self.assertFalse(dashboard_module._topology_snapshot(topology, 100.0, False)["links"][0]["usable"])
                self.assertTrue(dashboard_module._topology_snapshot(topology, 115.0, True)["stale"])
            finally:
                client.stop()
                client._history_store.close()

    def test_expired_topology_request_or_snapshot_is_not_published(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                for created_age, finish in ((0, 110.0), (14000, 101.0)):
                    sock.outgoing.clear()
                    with patch("proxy_client.time.monotonic", return_value=100.0):
                        client._poll_once(sock)
                    request = _request_id(sock.outgoing)
                    message = _topology(request)
                    message.params["created_age_ms"] = created_age
                    with patch("proxy_client.time.monotonic", return_value=finish):
                        client._handle_message(sock, message)
                    self.assertIsNone(client.snapshot(None, None).topology)
            finally:
                client.stop()
                client._history_store.close()

    def test_topology_delayed_old_epoch_cannot_replace_new_epoch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._poll_once(sock)
                older = _request_id(sock.outgoing)
                sock.outgoing.clear()
                client._poll_once(sock)
                newer = _request_id(sock.outgoing)
                message = _topology(newer)
                message.params["epoch"] = 22
                client._handle_message(sock, message)
                client._handle_message(sock, _topology(older, version=999))
                self.assertEqual(client.snapshot(None, None).topology.epoch, 22)
                self.assertEqual(client.snapshot(None, None).topology.last_request_id, newer)
            finally:
                client.stop()
                client._history_store.close()

    def test_same_version_reply_advances_epoch_watermark(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                requests = []
                for _ in range(3):
                    sock.outgoing.clear()
                    client._poll_once(sock)
                    requests.append(_request_id(sock.outgoing))
                for request in (requests[0], requests[2]):
                    message = _topology(request)
                    message.params["epoch"] = 22
                    client._handle_message(sock, message)
                client._handle_message(sock, _topology(requests[1], version=999))
                topology = client.snapshot(None, None).topology
                self.assertEqual(topology.epoch, 22)
                self.assertEqual(topology.last_request_id, requests[2])
            finally:
                client.stop()
                client._history_store.close()

    def test_quality_validation_aborts_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                for quality in (
                    {"cost": None, "confidence": 0.5, "usable": True},
                    {"cost": 10.0, "confidence": 2.0, "usable": True},
                    {"cost": 10.0, "confidence": 0.5, "usable": True, "score": 90.0},
                ):
                    sock.outgoing.clear()
                    client._poll_once(sock)
                    request = _request_id(sock.outgoing)
                    message = _topology(request)
                    message.params["links"][0]["quality"] = quality
                    with self.assertLogs("proxy_client", level="WARNING"):
                        client._handle_message(sock, message)
                    client._handle_message(sock, _topology(request))
                    self.assertIsNone(client.snapshot(None, None).topology)
            finally:
                client.stop()
                client._history_store.close()

    def test_topology_rejects_links_to_unknown_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                request = _messages(bytes(sock.outgoing))[1].params["request_id"]
                message = _topology(request)
                message.params["links"][0]["destination"] = "missing-node"
                with self.assertRaisesRegex(ValueError, "invalid node"):
                    client._record_topology(message.params)
            finally:
                client.stop()
                client._history_store.close()

    def test_topology_member_history_is_available_before_service_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                request_id = _request_id(sock.outgoing)
                with patch("proxy_client.time.time", return_value=90.0):
                    client._handle_message(sock, _status(request_id, "node-a"))
                with patch("proxy_client.time.time", return_value=100.0):
                    client._handle_message(
                        sock, _topology(request_id)
                    )

                http = dashboard_module.app.test_client()
                with (
                    patch.object(dashboard_module, "_client", client),
                    patch("dashboard.time.time", return_value=101.0),
                ):
                    response = http.get("/api/snapshot")
                self.assertEqual(response.status_code, 200)
                data = response.json
                nodes = {node["node_id"]: node for node in data["nodes"]}
                self.assertEqual(set(nodes), {"node-a", "node-b"})
                pending = nodes["node-b"]
                self.assertIsNone(pending["uptime_s"])
                self.assertEqual(pending["service_traffic"], [])
                self.assertFalse(pending["connected"])
                self.assertEqual(pending["history"], [{"t": 100.0, "cqd": 1, "tcp_qd": 2, "udp_qd": 3}])
                self.assertEqual((data["connected_nodes"], data["total_nodes"]), (1, 2))
                self.assertEqual(data["history_cursor"], 100.0)

                with patch("proxy_client.time.time", return_value=110.0):
                    client._handle_message(sock, _status(request_id, "node-b"))
                with (
                    patch.object(dashboard_module, "_client", client),
                    patch("dashboard.time.time", return_value=111.0),
                ):
                    data = http.get("/api/snapshot?history_after=100").json
                node = next(node for node in data["nodes"] if node["node_id"] == "node-b")
                self.assertEqual(node["uptime_s"], 1.234)
                self.assertEqual(node["service_traffic"][0]["service"], "ssh")
                self.assertTrue(node["connected"])
                self.assertEqual(node["history"], [])
                self.assertEqual(len(client.snapshot(None, None).nodes["node-b"].queue_history), 1)
                self.assertEqual((data["connected_nodes"], data["total_nodes"]), (2, 2))

                remaining = _topology(request_id, version=8)
                remaining.params["links"] = []
                remaining.params["nodes"] = remaining.params["nodes"][:1]
                client._handle_message(sock, remaining)
                self.assertEqual(set(client.snapshot(None, None).nodes), {"node-a"})
            finally:
                client.stop()
                client._history_store.close()

    def test_snapshot_reads_persisted_samples_before_client_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                client._handle_message(
                    sock,
                    _status(_request_id(sock.outgoing), "node-a"),
                )
                client._history_store.append_queue("node-a", 100.0, 11, 12, 13)
                client._history_store.append_traffic("node-a", 100.0, [("ssh", 15, 16)])

                snapshot = client.snapshot(None, None).nodes["node-a"]
                self.assertEqual(snapshot.queue_history[0].timestamp, 100.0)
                self.assertEqual(snapshot.traffic_history["ssh"][0].timestamp, 100.0)
            finally:
                client.stop()
                client._history_store.close()

    def test_status_requires_service_accessors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                request_id = _request_id(sock.outgoing)
                with self.assertRaisesRegex(ValueError, "service accessors must be an object"):
                    client._handle_message(
                        sock, _without_accessors(_status(request_id, "node-a"))
                    )
                self.assertNotIn("node-a", client.snapshot(None, None).nodes)
            finally:
                client.stop()
                client._history_store.close()

    def test_fragmented_status_is_published_only_after_assembly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                request_id = _request_id(sock.outgoing)
                message = _status(request_id, "node-a")
                message.params["services"][0]["accessors"] = {"x" * 150000: 1}
                frames = pack_frame(message)
                length = int.from_bytes(frames[:HEADER_LENGTH], "big")
                buf = bytearray(frames[:HEADER_LENGTH + length])
                self.assertIsNone(client._try_read_one(buf))
                self.assertNotIn("node-a", client.snapshot(None, None).nodes)
                buf.extend(frames[HEADER_LENGTH + length:])
                complete = client._try_read_one(buf)
                self.assertIsNotNone(complete)
                client._handle_message(sock, complete)
                snapshot = client.snapshot(None, None)
                self.assertEqual(snapshot.nodes["node-a"].service_traffic["ssh"].accessors, {"x" * 150000: 1})
                self.assertEqual(len(snapshot.nodes["node-a"].traffic_history["ssh"]), 1)
            finally:
                client.stop()
                client._history_store.close()

    def test_duplicate_and_older_reports_are_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                first = _request_id(sock.outgoing)
                client._handle_message(sock, _status(first, "node-a", 1))
                sock.outgoing.clear()
                client._poll_once(sock)
                second = _request_id(sock.outgoing)
                client._handle_message(sock, _status(second, "node-a", 2))
                client._handle_message(sock, _status(second, "node-a", 99))
                client._handle_message(sock, _status(first, "node-a", 98))
                snapshot = client.snapshot(None, None)
                self.assertEqual(snapshot.nodes["node-a"].uptime_ms, 2)
            finally:
                client.stop()
                client._history_store.close()

    def test_recent_late_report_is_accepted_until_a_newer_report_exists(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                first = _request_id(sock.outgoing)
                for _ in range(2):
                    sock.outgoing.clear()
                    client._poll_once(sock)
                client._handle_message(sock, _status(first, "node-a", 7))
                snapshot = client.snapshot(None, None)
                self.assertTrue(snapshot.nodes["node-a"].connected)
                self.assertEqual(snapshot.nodes["node-a"].uptime_ms, 7)
            finally:
                client.stop()
                client._history_store.close()

    def test_three_missed_rounds_mark_node_offline_until_next_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(directory)
            sock = _Socket()
            try:
                client._set(connected=True)
                client._poll_once(sock)
                client._handle_message(sock, _status(_request_id(sock.outgoing), "node-a"))
                for _ in range(2):
                    sock.outgoing.clear()
                    client._poll_once(sock)
                self.assertTrue(client.snapshot(None, None).nodes["node-a"].connected)
                sock.outgoing.clear()
                client._poll_once(sock)
                latest = _request_id(sock.outgoing)
                self.assertFalse(client.snapshot(None, None).nodes["node-a"].connected)
                client._handle_message(sock, _status(latest, "node-a"))
                self.assertTrue(client.snapshot(None, None).nodes["node-a"].connected)
                client._set(connected=False, last_error="entry disconnected")
                self.assertFalse(client.snapshot(None, None).nodes["node-a"].connected)
                client._mark_session_connected("entry")
                self.assertFalse(client.snapshot(None, None).nodes["node-a"].connected)
                sock.outgoing.clear()
                client._poll_once(sock)
                client._handle_message(sock, _status(_request_id(sock.outgoing), "node-a"))
                self.assertTrue(client.snapshot(None, None).nodes["node-a"].connected)
            finally:
                client.stop()
                client._history_store.close()


if __name__ == "__main__":
    unittest.main()
