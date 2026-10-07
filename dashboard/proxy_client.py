"""TLS control-channel client for the proxy server.

Establishes the same channel the C++ RelayAgent uses (agent/src/relay_agent.cpp):
TCP -> mutual-TLS handshake -> length-prefixed CBOR control messages, with
server-driven `ping`/client `pong` heartbeat. Periodically requests service
status from every cluster node and the Master's topology snapshot, while
retaining persistent queue and traffic history for the dashboard.

Runs on a background thread so the Flask dashboard can be served from the
main thread. Auto-reconnects with exponential backoff, mirroring RelayAgent.
"""

from __future__ import annotations

import logging
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field, replace

from history_store import HistoryStore
from proxy_protocol import (
    CtrlMessage,
    HEADER_LENGTH,
    MAX_PAYLOAD_LENGTH,
    QUEUE_DELAY_UNSET,
    MessageReceiver,
    pack_frame,
)


@dataclass
class QueueSample:
    """One queue-delay measurement from a topology snapshot."""

    timestamp: float
    control_queue_delay_us: int
    transfer_tcp_queue_delay_us: int
    transfer_udp_queue_delay_us: int


@dataclass
class ServiceTraffic:
    """Latest per-service traffic snapshot from server.status.reported.

    Mirrors RegistryMgr::traffic_report() in node/src/registry_mgr.cpp:
    rx_bytes / tx_bytes are cumulative bytes since registration (uint64,
    saturating); rx_bytes_per_second / tx_bytes_per_second are an EMA in
    bytes/second sampled once per second on the server.
    """

    service: str
    protocol: str  # "tcp", "tls", or "udp"
    rx_bytes: int
    tx_bytes: int
    rx_bytes_per_second: int
    tx_bytes_per_second: int
    accessors: dict[str, int]


@dataclass
class _TrafficPoint:
    """A single service's traffic at one timestamp (a traffic-history entry)."""

    timestamp: float
    rx_bytes_per_second: int
    tx_bytes_per_second: int


@dataclass(frozen=True)
class TopologyNode:
    node_id: str
    address: str
    report_age_ms: int | None
    control_queue_delay_us: int | None
    transfer_tcp_queue_delay_us: int | None
    transfer_udp_queue_delay_us: int | None


@dataclass(frozen=True)
class TopologyLink:
    source: str
    destination: str
    rtt_ms: float | None
    jitter_ms: float | None
    loss_rate: float
    transmitted: int
    received: int
    completed: int
    age_ms: int | None
    quality_cost: float | None
    confidence: float
    usable: bool


@dataclass(frozen=True)
class TopologySnapshot:
    epoch: int
    version: int
    last_request_id: int
    created_at: float  # Conservative monotonic origin, including request and assembly time.
    received_at: float  # Wall clock for persistent history and HTTP delta cursors.
    nodes: tuple[TopologyNode, ...]
    links: tuple[TopologyLink, ...]


@dataclass
class NodeSnapshot:
    """Latest report and history for one cluster node."""

    node_id: str
    uptime_ms: int | None = None
    service_traffic: dict[str, ServiceTraffic] = field(default_factory=dict)
    service_traffic_updated: float = 0.0
    last_round: int = 0
    connected: bool = False
    last_error: str = ""
    queue_history: list[QueueSample] = field(default_factory=list)
    traffic_history: dict[str, list[_TrafficPoint]] = field(default_factory=dict)


@dataclass
class Snapshot:
    """Latest cluster view observed through one control connection."""

    entry_node_id: str = ""
    connected: bool = False
    connecting: bool = False
    last_error: str = ""
    peer: str = ""
    nodes: dict[str, NodeSnapshot] = field(default_factory=dict)
    topology: TopologySnapshot | None = None


class ProxyControlClient:
    """Thread-safe client that talks to the proxy control channel."""

    def __init__(
        self,
        host: str,
        port: int,
        ca_file: str,
        cert_file: str,
        key_file: str,
        server_name: str | None = None,
        poll_interval: float = 2.0,
        connect_timeout: float = 5.0,
        handshake_timeout: float = 5.0,
        history_db: str = "dashboard.sqlite3",
        max_history_bytes: int = 50 * 1024 * 1024,
        history_store: HistoryStore | None = None,
    ):
        self.host = host
        self.port = port
        self.ca_file = ca_file
        self.cert_file = cert_file
        self.key_file = key_file
        # An empty server_name falls back to the connection host. For an IP
        # address, Python verifies the IP SAN without sending a DNS SNI name.
        self.server_name = host if server_name is None else server_name
        self.poll_interval = poll_interval
        self.connect_timeout = connect_timeout
        self.handshake_timeout = handshake_timeout

        self._lock = threading.Lock()
        self._snapshot = Snapshot()
        self._nodes: dict[str, NodeSnapshot] = {}
        self._owns_history_store = history_store is None
        self._history_store = history_store or HistoryStore(history_db, max_bytes=max_history_bytes)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._socket: ssl.SSLSocket | None = None
        self._next_request_id = 1
        self._round_number = 0
        self._rounds: dict[int, tuple[int, float]] = {}
        self._discarded_topology: set[int] = set()
        self._receiver = MessageReceiver()
        self._member_ids: set[str] | None = None
        # Serializes writes: the reader thread answers pings while the poller
        # thread issues cluster queries; both share one SSL socket.
        self._send_lock = threading.Lock()

    # ------------------------------------------------------------------ public

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="proxy-control", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._close_socket()
        if self._thread:
            self._thread.join()
        if self._owns_history_store:
            self._history_store.close()

    def snapshot(self, queue_since: float | None, traffic_since: float | None) -> Snapshot:
        """Return a copy of the current cluster snapshot for the UI."""
        with self._lock:
            topology = self._snapshot.topology
            member_ids = (
                (node.node_id for node in topology.nodes)
                if topology is not None
                else self._nodes
            )
            nodes = {}
            for node_id in member_ids:
                report = self._nodes.get(node_id)
                node = (
                    NodeSnapshot(node_id=node_id)
                    if report is None
                    else replace(report, service_traffic=dict(report.service_traffic))
                )
                node.connected = (
                    report is not None
                    and self._snapshot.connected
                    and self._round_number - report.last_round < 3
                )
                node.last_error = (
                    ""
                    if node.connected
                    else self._snapshot.last_error
                    or ("no service report" if report is None else "no report for three collection rounds")
                )
                nodes[node_id] = node
            snap = Snapshot(
                entry_node_id=self._snapshot.entry_node_id,
                connected=self._snapshot.connected,
                connecting=self._snapshot.connecting,
                last_error=self._snapshot.last_error,
                peer=self._snapshot.peer,
                nodes=nodes,
                topology=topology,
            )
        for node in snap.nodes.values():
            node.queue_history = [
                QueueSample(*row)
                for row in self._history_store.queue_history(node.node_id, queue_since)
            ]
            traffic_rows = self._history_store.traffic_history(
                node.node_id, traffic_since, sorted(node.service_traffic)
            )
            node.traffic_history = {
                service: [
                    _TrafficPoint(
                        timestamp=row[0],
                        rx_bytes_per_second=row[1],
                        tx_bytes_per_second=row[2],
                    )
                    for row in rows
                ]
                for service, rows in traffic_rows.items()
            }
        return snap

    # --------------------------------------------------------------- internals

    def _allocate_request_id(self) -> int:
        rid = self._next_request_id
        self._next_request_id += 1
        if self._next_request_id == 0:
            self._next_request_id = 1
        return rid if rid != 0 else self._allocate_request_id()

    def _set(self, **kwargs) -> None:
        with self._lock:
            for key, value in kwargs.items():
                setattr(self._snapshot, key, value)

    def _mark_session_connected(self, node_id: str) -> None:
        with self._lock:
            # A new control session must not make reports from the old session
            # look current. Keep discovered nodes, but require a fresh report
            # before each becomes online again.
            self._round_number += 3
            self._rounds.clear()
            self._discarded_topology.clear()
            self._receiver = MessageReceiver()
            self._member_ids = None
            self._snapshot.topology = None
            self._snapshot.entry_node_id = node_id
            self._snapshot.connected = True
            self._snapshot.connecting = False
            self._snapshot.last_error = ""
            self._snapshot.peer = (
                f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"
            )

    def _build_ssl_context(self) -> ssl.SSLContext:
        # Mirror agent/src/main.cpp / relay_agent.cpp: verify the server
        # against ca_file and present a client certificate (the server runs
        # verify_peer | verify_fail_if_no_peer_cert).
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(self.ca_file)
        ctx.load_cert_chain(certfile=self.cert_file, keyfile=self.key_file)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        # Match the server's option set: no SSLv2/3/TLS1.0/1.1.
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        return ctx

    def _connect(self) -> ssl.SSLSocket:
        ctx = self._build_ssl_context()
        raw = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        raw.settimeout(self.handshake_timeout)
        sock = None
        try:
            sock = ctx.wrap_socket(raw, server_hostname=self.server_name or self.host)
        except Exception:
            (sock or raw).close()
            raise
        return sock

    def _identify_server(self, sock: ssl.SSLSocket) -> str:
        """Perform the current control-channel identity handshake.

        RelayAgent does this before treating a connection as usable. The reply
        intentionally has no request_id, so it must be consumed before the
        normal reader/pending-request machinery starts.
        """
        deadline = time.monotonic() + self.handshake_timeout
        self._send(sock, CtrlMessage("server.identify", {}))
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("server.identify timed out")
            sock.settimeout(remaining)
            message = self._read_message(sock)
            if message.command == "ping":
                self._send(sock, CtrlMessage("pong"))
                continue
            if message.command != "server.identified" or not message.params:
                raise ValueError(f"Expected server.identified, got {message.command}")
            node_id = message.params.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                raise ValueError("server.identified did not contain a valid node_id")
            sock.settimeout(None)
            return node_id

    def _read_message(self, sock: ssl.SSLSocket) -> CtrlMessage:
        while True:
            header = self._recv_exact(sock, HEADER_LENGTH)
            length = int.from_bytes(header, "big")
            if not (1 <= length <= MAX_PAYLOAD_LENGTH):
                raise ValueError(f"Invalid payload length in frame: {length}")
            message = self._receiver.receive(self._recv_exact(sock, length))
            if message is not None:
                return message

    @staticmethod
    def _recv_exact(sock: ssl.SSLSocket, length: int) -> bytes:
        result = bytearray()
        while len(result) < length:
            chunk = sock.recv(length - len(result))
            if not chunk:
                raise EOFError("server closed the control channel")
            result.extend(chunk)
        return bytes(result)

    def _close_socket(self) -> None:
        with self._lock:
            sock = self._socket
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def _run(self) -> None:
        backoff = 0.5
        max_backoff = 10.0
        while not self._stop.is_set():
            sock = None
            try:
                self._set(connecting=True, last_error="")
                sock = self._connect()
                with self._lock:
                    self._socket = sock
                if self._stop.is_set():
                    break
                node_id = self._identify_server(sock)
                if self._stop.is_set():
                    break
                self._mark_session_connected(node_id)
                backoff = 0.5
                self._session_loop(sock)
            except Exception as exc:  # noqa: BLE001
                if self._stop.is_set():
                    break
                self._set(connected=False, connecting=False, last_error=str(exc))
            finally:
                if sock is not None:
                    self._close_socket()
                    with self._lock:
                        if self._socket is sock:
                            self._socket = None
            if self._stop.is_set():
                break
            self._set(connected=False, connecting=False)
            if self._stop.wait(backoff):
                break
            backoff = min(backoff * 2, max_backoff)

    def _session_loop(self, sock: ssl.SSLSocket) -> None:
        """Run a reader thread + periodic poller over one established session.

        The reader thread drains frames continuously (so server pings are
        answered immediately and status reports are accepted); the poller
        sends one non-blocking server.cluster request on each cadence.
        """
        session_dead = threading.Event()
        sock.settimeout(None)  # blocking recv; reader thread owns it
        reader = threading.Thread(
            target=self._reader_loop, args=(sock, session_dead), name="proxy-reader", daemon=True
        )
        reader.start()
        next_poll = time.monotonic()
        try:
            while not self._stop.is_set() and not session_dead.is_set():
                now = time.monotonic()
                if now >= next_poll:
                    self._poll_once(sock)
                    next_poll = now + self.poll_interval
                # Sleep in small slices so stop/session death wake us promptly.
                self._stop.wait(timeout=0.25)
                if session_dead.is_set():
                    raise EOFError("reader detected peer closed the control channel")
        finally:
            self._close_socket()
            reader.join()
            with self._lock:
                if self._socket is sock:
                    self._socket = None

    def _reader_loop(self, sock: ssl.SSLSocket, session_dead: threading.Event) -> None:
        buf = bytearray()
        try:
            while not self._stop.is_set():
                try:
                    chunk = sock.recv(4096)
                except (ssl.SSLWantReadError, socket.timeout):
                    continue
                except OSError:
                    break
                if not chunk:
                    break  # peer closed
                buf.extend(chunk)
                while True:
                    msg = self._try_read_one(buf)
                    if msg is None:
                        break
                    try:
                        self._handle_message(sock, msg)
                    except OSError:
                        # send failed -> socket is dead.
                        session_dead.set()
                        return
        except Exception:  # noqa: BLE001
            pass
        finally:
            session_dead.set()

    def _try_read_one(self, buf: bytearray) -> CtrlMessage | None:
        """Consume available frames and return one complete logical message, if ready."""
        while len(buf) >= HEADER_LENGTH:
            length = int.from_bytes(buf[:HEADER_LENGTH], "big")
            if not (1 <= length <= MAX_PAYLOAD_LENGTH):
                raise ValueError(f"Invalid payload length in frame: {length}")
            if len(buf) < HEADER_LENGTH + length:
                return None
            payload = bytes(buf[HEADER_LENGTH : HEADER_LENGTH + length])
            del buf[: HEADER_LENGTH + length]
            message = self._receiver.receive(payload)
            if message is not None:
                return message
        return None

    def _handle_message(self, sock: ssl.SSLSocket, msg: CtrlMessage) -> None:
        if msg.command == "ping":
            self._send(sock, CtrlMessage("pong"))
            return
        if msg.command == "pong":
            return
        if msg.command == "server.status.reported" and msg.params:
            self._record_status(msg.params)
        elif msg.command == "topology.snapshot" and msg.params:
            try:
                self._record_topology(msg.params)
            except ValueError as error:
                logging.getLogger(__name__).warning("Discarding topology response: %s", error)

    def _send(self, sock: ssl.SSLSocket, message: CtrlMessage) -> None:
        frame = pack_frame(message)
        with self._send_lock:
            sock.sendall(frame)

    def _poll_once(self, sock: ssl.SSLSocket) -> None:
        request_id = self._allocate_request_id()
        with self._lock:
            self._round_number += 1
            round_number = self._round_number
            self._rounds[request_id] = (round_number, time.monotonic())
            self._rounds = {
                rid: value for rid, value in self._rounds.items() if round_number - value[0] < 3
            }
            retained_requests = self._rounds.keys()
            self._discarded_topology.intersection_update(retained_requests)
        self._send(sock, CtrlMessage("server.cluster", {"request_id": request_id}))
        self._send(sock, CtrlMessage("topology.query", {"request_id": request_id}))

    @staticmethod
    def _unsigned(params: dict, name: str, maximum: int | None = None) -> int:
        value = params.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be an unsigned integer")
        if maximum is not None and value > maximum:
            raise ValueError(f"{name} is out of range")
        return value

    def _record_status(self, params: dict) -> None:
        request_id = self._unsigned(params, "request_id")
        node_id = params.get("node_id")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("server.status.reported.node_id must be a non-empty string")
        uptime_ms = self._unsigned(params, "uptime_ms")

        raw_services = params.get("services")
        if not isinstance(raw_services, list):
            raise ValueError("server.status.reported.services must be an array")
        by_service: dict[str, ServiceTraffic] = {}
        for entry in raw_services:
            if not isinstance(entry, dict):
                raise ValueError("server.status.reported.services entries must be objects")
            name = entry.get("service")
            protocol = entry.get("protocol")
            if not isinstance(name, str) or not name:
                raise ValueError("server.status.reported service name must be a non-empty string")
            if protocol not in {"tcp", "tls", "udp"}:
                raise ValueError("server.status.reported service protocol is invalid")
            if name in by_service:
                raise ValueError("server.status.reported repeats a service")
            raw_accessors = entry.get("accessors")
            if not isinstance(raw_accessors, dict):
                raise ValueError("server.status.reported service accessors must be an object")
            for client, connections in raw_accessors.items():
                if not isinstance(client, str) or not client:
                    raise ValueError(
                        "server.status.reported service accessor client must be a non-empty string"
                    )
                if (
                    isinstance(connections, bool)
                    or not isinstance(connections, int)
                    or connections <= 0
                ):
                    raise ValueError(
                        "server.status.reported service accessor connections must be positive"
                    )
            by_service[name] = ServiceTraffic(
                service=name,
                protocol=protocol,
                rx_bytes=self._unsigned(entry, "rx_bytes"),
                tx_bytes=self._unsigned(entry, "tx_bytes"),
                rx_bytes_per_second=self._unsigned(entry, "rx_bytes_per_second"),
                tx_bytes_per_second=self._unsigned(entry, "tx_bytes_per_second"),
                accessors=raw_accessors,
            )

        with self._lock:
            round_info = self._rounds.get(request_id)
            current = self._nodes.get(node_id)
            if (
                round_info is None
                or (self._member_ids is not None and node_id not in self._member_ids)
                or (current is not None and current.last_round >= round_info[0])
            ):
                return
            round_number = round_info[0]

        timestamp = time.time()
        self._history_store.append_traffic(
            node_id,
            timestamp,
            [
                (name, traffic.rx_bytes_per_second, traffic.tx_bytes_per_second)
                for name, traffic in by_service.items()
            ],
        )
        with self._lock:
            current = self._nodes.get(node_id)
            if current is not None and current.last_round >= round_number:
                return
            self._nodes[node_id] = NodeSnapshot(
                node_id=node_id,
                uptime_ms=uptime_ms,
                service_traffic=by_service,
                service_traffic_updated=timestamp,
                last_round=round_number,
            )

    @staticmethod
    def _nullable_unsigned(params: dict, name: str, maximum: int | None = None) -> int | None:
        value = params.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"topology.snapshot.{name} must be null or an unsigned integer")
        if maximum is not None and value > maximum:
            raise ValueError(f"topology.snapshot.{name} is out of range")
        return value

    @staticmethod
    def _nullable_number(params: dict, name: str) -> float | None:
        value = params.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"topology.snapshot.{name} must be null or a number")
        value = float(value)
        if not 0 <= value < float("inf"):
            raise ValueError(f"topology.snapshot.{name} is out of range")
        return value

    def _record_topology(self, params: dict) -> None:
        request_id = self._unsigned(params, "request_id")
        with self._lock:
            if request_id not in self._rounds or request_id in self._discarded_topology:
                return
        try:
            self._record_topology_snapshot(params)
        except ValueError:
            with self._lock:
                self._discarded_topology.add(request_id)  # Terminate this topology request.
            raise

    def _record_topology_snapshot(self, params: dict) -> None:
        request_id = self._unsigned(params, "request_id")
        epoch = self._unsigned(params, "epoch")
        version = self._unsigned(params, "snapshot_version")
        created_age_ms = self._unsigned(params, "created_age_ms")
        if epoch == 0 or version == 0 or created_age_ms >= 15000:
            raise ValueError("topology.snapshot contains invalid version or age")

        raw_nodes = params.get("nodes")
        raw_links = params.get("links")
        if not isinstance(raw_nodes, list) or not isinstance(raw_links, list):
            raise ValueError("topology.snapshot nodes and links must be arrays")

        nodes: list[TopologyNode] = []
        for entry in raw_nodes:
            if not isinstance(entry, dict):
                raise ValueError("topology.snapshot node must be an object")
            node_id = entry.get("node_id")
            address = entry.get("address")
            if not isinstance(node_id, str) or not node_id or not isinstance(address, str) or not address:
                raise ValueError("topology.snapshot node identity is invalid")
            nodes.append(
                TopologyNode(
                    node_id=node_id,
                    address=address,
                    report_age_ms=self._nullable_unsigned(entry, "report_age_ms"),
                    control_queue_delay_us=self._nullable_unsigned(
                        entry, "control_queue_delay_us", QUEUE_DELAY_UNSET
                    ),
                    transfer_tcp_queue_delay_us=self._nullable_unsigned(
                        entry, "transfer_tcp_queue_delay_us", QUEUE_DELAY_UNSET
                    ),
                    transfer_udp_queue_delay_us=self._nullable_unsigned(
                        entry, "transfer_udp_queue_delay_us", QUEUE_DELAY_UNSET
                    ),
                )
            )

        links: list[TopologyLink] = []
        for entry in raw_links:
            if not isinstance(entry, dict):
                raise ValueError("topology.snapshot link must be an object")
            source = entry.get("source")
            destination = entry.get("destination")
            if not isinstance(source, str) or not source or not isinstance(destination, str) or not destination:
                raise ValueError("topology.snapshot link identity is invalid")
            loss_rate = self._nullable_number(entry, "loss_rate")
            if loss_rate is None or loss_rate > 1:
                raise ValueError("topology.snapshot.loss_rate is out of range")
            quality = entry.get("quality")
            if not isinstance(quality, dict) or set(quality) != {"cost", "confidence", "usable"}:
                raise ValueError("topology.snapshot.quality is invalid")
            cost = self._nullable_number(quality, "cost")
            confidence = self._nullable_number(quality, "confidence")
            usable = quality["usable"]
            if confidence is None or confidence > 1 or not isinstance(usable, bool):
                raise ValueError("topology.snapshot.quality confidence/usable is invalid")
            if (cost is None and usable) or (cost is not None and cost > 1e9):
                raise ValueError("topology.snapshot.quality cost is invalid")
            links.append(
                TopologyLink(
                    source=source,
                    destination=destination,
                    rtt_ms=self._nullable_number(entry, "rtt_ms"),
                    jitter_ms=self._nullable_number(entry, "jitter_ms"),
                    loss_rate=loss_rate,
                    transmitted=self._unsigned(entry, "transmitted"),
                    received=self._unsigned(entry, "received"),
                    completed=self._unsigned(entry, "completed"),
                    age_ms=self._nullable_unsigned(entry, "age_ms"),
                    quality_cost=cost,
                    confidence=confidence,
                    usable=usable,
                )
            )

        with self._lock:
            round_info = self._rounds.get(request_id)
            if round_info is None:
                return
            elapsed = max(0.0, time.monotonic() - round_info[1])
            if elapsed >= 10.0 or elapsed + created_age_ms / 1000.0 >= 15.0:
                self._discarded_topology.add(request_id)
                return
            if len(nodes) + len(links) > 65536:
                raise ValueError("topology.snapshot exceeds the entry limit")
            if len(nodes) > 1024 or len({node.node_id for node in nodes}) != len(nodes):
                raise ValueError("topology.snapshot repeats a node")
            if len({(link.source, link.destination) for link in links}) != len(links):
                raise ValueError("topology.snapshot repeats a directed link")
            node_ids = {node.node_id for node in nodes}
            if any(
                link.source == link.destination
                or link.source not in node_ids
                or link.destination not in node_ids
                for link in links
            ):
                raise ValueError("topology.snapshot link references an invalid node")
            current = self._snapshot.topology
            if current is not None:
                if current.epoch == epoch and current.version >= version:
                    self._snapshot.topology = replace(current, last_request_id=max(request_id, current.last_request_id))
                    return
                if current.epoch != epoch and request_id <= current.last_request_id:
                    return
            received_at = time.time()
            member_ids = {node.node_id for node in nodes}
            self._nodes = {
                node_id: node for node_id, node in self._nodes.items() if node_id in member_ids
            }
            self._member_ids = member_ids
            origin = self._rounds[request_id][1] - created_age_ms / 1000.0
            self._snapshot.topology = TopologySnapshot(
                epoch=epoch,
                version=version,
                last_request_id=max(request_id, current.last_request_id if current else 0),
                created_at=origin,
                received_at=received_at,
                nodes=tuple(nodes),
                links=tuple(links),
            )

        for node in nodes:
            queues = (
                node.control_queue_delay_us,
                node.transfer_tcp_queue_delay_us,
                node.transfer_udp_queue_delay_us,
            )
            elapsed = max(0.0, time.monotonic() - origin)
            if (node.report_age_ms is not None and node.report_age_ms / 1000.0 + elapsed < 15.0
                and all(value is not None for value in queues)):
                self._history_store.append_queue(node.node_id, received_at, *queues)
