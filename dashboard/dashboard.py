"""Flask dashboard server for the proxy control-channel client.

Serves a single-page cluster dashboard that periodically fetches /api/snapshot
and renders Node health, per-node charts, and cluster-wide services.
One ProxyControlClient runs on a background thread; Waitress serves
HTTP requests in a single process, listening only on 127.0.0.1.

Usage:
    python dashboard.py dashboard.example.json
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
import gzip
import json
import math
import signal
import sys
import time
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request

# Keep sibling modules importable regardless of the current working directory.
DASHBOARD_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(DASHBOARD_DIR))

from history_store import HistoryStore  # noqa: E402
from ip_location import IpLocationCache  # noqa: E402
from proxy_client import ProxyControlClient  # noqa: E402
from service_runtime import RequestDrain  # noqa: E402

app = Flask(__name__, template_folder="templates", static_folder=None)
_client: ProxyControlClient | None = None
_location_cache: IpLocationCache | None = None
MAX_HISTORY_RANGE_SECONDS = 7 * 24 * 60 * 60
GZIP_MINIMUM_SIZE = 512
GZIP_MIMETYPES = {
    "application/json",
    "application/javascript",
    "image/svg+xml",
    "text/css",
    "text/html",
    "text/javascript",
}


@app.after_request
def gzip_response(response):
    """Compress browser-facing text responses without another dependency."""
    response.vary.add("Accept-Encoding")
    if (
        request.method == "HEAD"
        or response.status_code < 200
        or response.status_code in (204, 304)
        or response.direct_passthrough
        or response.headers.get("Content-Encoding")
        or response.mimetype not in GZIP_MIMETYPES
        or request.accept_encodings["gzip"] <= 0
    ):
        return response

    data = response.get_data()
    if len(data) < GZIP_MINIMUM_SIZE:
        return response
    compressed = gzip.compress(data, compresslevel=5, mtime=0)
    if len(compressed) >= len(data):
        return response
    response.set_data(compressed)
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = str(len(compressed))
    return response


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/snapshot")
def api_snapshot():
    assert _client
    now = time.time()
    history_after = _history_after()
    queue_since = _history_since("queue_range", now)
    traffic_since = _history_since("bandwidth_range", now)
    if history_after is not None:
        # HistoryStore uses an inclusive lower bound. Move to the next
        # representable float so an incremental request is strictly newer
        # than the last sample already held by the browser.
        incremental_since = math.nextafter(history_after, math.inf)
        queue_since = (
            max(queue_since, incremental_since)
            if queue_since is not None
            else incremental_since
        )
        traffic_since = (
            max(traffic_since, incremental_since)
            if traffic_since is not None
            else incremental_since
        )
    snap = _client.snapshot(queue_since, traffic_since)
    nodes = [_node_snapshot(node, now) for node in snap.nodes.values()]
    nodes.sort(key=lambda node: node["node_id"])
    connected_nodes, total_nodes = _cluster_counts(snap)
    response = jsonify(
        {
            "now": now,
            "history_mode": "delta" if history_after is not None else "full",
            "history_cursor": _history_cursor(snap, history_after),
            "connected_nodes": connected_nodes,
            "total_nodes": total_nodes,
            "nodes": nodes,
            "entry_connected": snap.connected,
            "entry_connecting": snap.connecting,
            "entry_node_id": snap.entry_node_id,
            "entry_peer": snap.peer,
            "entry_error": snap.last_error,
            "topology": _topology_snapshot(snap.topology, time.monotonic(), snap.connected),
        }
    )
    response.headers["Cache-Control"] = "no-store"
    return response


def _node_snapshot(snap, now: float) -> dict:
    history = [
        {
            "t": sample.timestamp,
            "cqd": sample.control_queue_delay_us,
            "tcp_qd": sample.transfer_tcp_queue_delay_us,
            "udp_qd": sample.transfer_udp_queue_delay_us,
        }
        for sample in snap.queue_history
    ]
    service_traffic = sorted(
        (
            {
                "service": tr.service,
                "protocol": tr.protocol,
                "rx_bytes": tr.rx_bytes,
                "tx_bytes": tr.tx_bytes,
                "rx_bytes_per_second": tr.rx_bytes_per_second,
                "tx_bytes_per_second": tr.tx_bytes_per_second,
                "accessors": tr.accessors,
                "accessor_locations": {
                    endpoint: (
                        _location_cache.location_for_endpoint(endpoint)
                        if _location_cache is not None
                        else {"label": "🌐 ---", "title": "Location lookup pending"}
                    )
                    for endpoint in tr.accessors
                },
            }
            for tr in snap.service_traffic.values()
        ),
        key=lambda service: (service["service"], service["protocol"]),
    )
    traffic_history = {
        name: [
            {
                "t": pt.timestamp,
                "rx_bps": pt.rx_bytes_per_second,
                "tx_bps": pt.tx_bytes_per_second,
            }
            for pt in points
        ]
        for name, points in snap.traffic_history.items()
    }
    report_age = (
        now - snap.service_traffic_updated if snap.service_traffic_updated else None
    )
    return {
        "key": snap.node_id,
        "node_id": snap.node_id,
        "connected": snap.connected,
        "connecting": False,
        "last_error": snap.last_error,
        "uptime_s": None if snap.uptime_ms is None else snap.uptime_ms / 1000.0,
        "report_age_s": report_age,
        "service_traffic": service_traffic,
        "service_traffic_updated": snap.service_traffic_updated,
        "service_traffic_age_s": report_age,
        "traffic_history": traffic_history,
        "history": history,
    }


def _topology_snapshot(snap, now: float, connected: bool) -> dict | None:
    if snap is None:
        return None
    age_s = max(0.0, now - snap.created_at)
    stale = not connected or age_s >= 15.0
    reporting_nodes = {
        node.node_id for node in snap.nodes
        if node.report_age_ms is not None and node.report_age_ms / 1000.0 + age_s < 15.0
    }
    return {
        "epoch": snap.epoch,
        "version": snap.version,
        "age_s": age_s,
        "stale": stale,
        "nodes": [
            {
                "node_id": node.node_id,
                "address": node.address,
                "report_age_s": (
                    None
                    if node.report_age_ms is None
                    else node.report_age_ms / 1000.0 + age_s
                ),
                "control_queue_delay_us": node.control_queue_delay_us if node.node_id in reporting_nodes and not stale else None,
                "transfer_tcp_queue_delay_us": node.transfer_tcp_queue_delay_us if node.node_id in reporting_nodes and not stale else None,
                "transfer_udp_queue_delay_us": node.transfer_udp_queue_delay_us if node.node_id in reporting_nodes and not stale else None,
            }
            for node in snap.nodes
        ],
        "links": [
            {
                "source": link.source,
                "destination": link.destination,
                "rtt_ms": link.rtt_ms,
                "jitter_ms": link.jitter_ms,
                "loss_rate": link.loss_rate,
                "transmitted": link.transmitted,
                "received": link.received,
                "completed": link.completed,
                "quality_cost": link.quality_cost,
                "quality_score": None if link.quality_cost is None else 100.0 * math.exp(-link.quality_cost / 100.0),
                "confidence": link.confidence,
                "usable": (
                    link.usable and not stale and link.source in reporting_nodes
                    and link.age_ms is not None and link.age_ms / 1000.0 + age_s < 45.0
                ),
                "age_s": None if link.age_ms is None else link.age_ms / 1000.0 + age_s,
            }
            for link in snap.links
        ],
    }


def _format_endpoint(address: str, port: int) -> str:
    return f"[{address}]:{port}" if ":" in address else f"{address}:{port}"


@app.route("/api/health")
def api_health():
    assert _client
    now = time.time()
    snap = _client.snapshot(now, now)
    connected, total = _cluster_counts(snap)
    return jsonify(
        {
            "ok": snap.connected and connected == total,
            "connected_nodes": connected,
            "total_nodes": total,
        }
    )


def _cluster_counts(snap) -> tuple[int, int]:
    return sum(node.connected for node in snap.nodes.values()), len(snap.nodes)


def _history_since(parameter: str, now: float) -> float | None:
    value = request.args.get(parameter, "300")
    if value == "all":
        return None
    try:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds <= 0 or seconds > MAX_HISTORY_RANGE_SECONDS:
            raise ValueError
    except (OverflowError, ValueError):
        abort(400, description=f"{parameter} must be a finite number in (0, 604800] or 'all'")
    return now - seconds


def _history_after() -> float | None:
    value = request.args.get("history_after")
    if value is None:
        return None
    try:
        timestamp = float(value)
        if not math.isfinite(timestamp) or timestamp < 0:
            raise ValueError
    except (OverflowError, ValueError):
        abort(400, description="history_after must be a finite non-negative timestamp")
    return timestamp


def _history_cursor(snap, previous: float | None) -> float | None:
    """Return the newest sample visible in this coherent metadata snapshot."""
    timestamps = [] if previous is None else [previous]
    if snap.topology is not None:
        timestamps.append(snap.topology.received_at)
    for node in snap.nodes.values():
        if node.service_traffic_updated:
            timestamps.append(node.service_traffic_updated)
    return max(timestamps) if timestamps else None


def _load_agent_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as stream:
        root = json.load(stream)
    server = root["server"]
    certificate = root["certificate"]
    channel = root.get("channel", {})

    def certificate_file(name: str) -> str:
        value = Path(certificate[name])
        return str(value if value.is_absolute() else path.parent / value)

    return {
        "host": server["host"],
        "port": int(server["port"]),
        "ca_file": certificate_file("ca_file"),
        "cert_file": certificate_file("certificate_chain"),
        "key_file": certificate_file("private_key"),
        "server_name": certificate.get("server_name", server["host"]),
        "connect_timeout": float(server.get("connect_timeout_ms", 5000)) / 1000.0,
        "handshake_timeout": float(channel.get("handshake_timeout_ms", 5000)) / 1000.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Proxy cluster dashboard")
    parser.add_argument(
        "config", type=Path, help="dashboard entry-node JSON (Agent server/certificate format is also accepted)"
    )
    parser.add_argument(
        "--poll-interval", type=float, default=2.0, help="seconds between dashboard queries"
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DASHBOARD_DIR / "dashboard.sqlite3",
        help="SQLite history database",
    )
    parser.add_argument(
        "--max-db-bytes",
        type=int,
        default=50 * 1024 * 1024,
        help="history DB size cap in bytes; oldest samples roll off when exceeded (default: 50 MiB)",
    )
    parser.add_argument("--http-port", type=int, default=5000, help="dashboard HTTP port")
    args = parser.parse_args()
    if not math.isfinite(args.poll_interval) or not 0 < args.poll_interval <= 3600:
        parser.error("--poll-interval must be a finite number in (0, 3600]")
    if not 1 <= args.http_port <= 65535:
        parser.error("--http-port must be in [1, 65535]")
    if args.max_db_bytes <= 0:
        parser.error("--max-db-bytes must be positive")
    config_path = args.config.resolve()
    try:
        config = _load_agent_config(config_path)
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        parser.error(f"failed to load {config_path}: {error}")

    global _client, _location_cache
    previous_sigterm = signal.signal(signal.SIGTERM, _request_shutdown)
    try:
        with ExitStack() as cleanup:
            history_store = HistoryStore(args.database.resolve(), max_bytes=args.max_db_bytes)
            cleanup.callback(history_store.close)
            _location_cache = IpLocationCache(history_store)
            cleanup.callback(_location_cache.close)
            _client = ProxyControlClient(
                poll_interval=args.poll_interval,
                history_store=history_store,
                **config,
            )
            cleanup.callback(_client.stop)
            requests = RequestDrain(app)
            cleanup.callback(requests.close)
            _client.start()
            tls_name = _client.server_name or _client.host
            print(f"[dashboard] connecting to {_format_endpoint(_client.host, _client.port)} (TLS name={tls_name})",
                  flush=True)
            print(f"[dashboard] serving http://127.0.0.1:{args.http_port}/", flush=True)
            try:
                from waitress import serve
                serve(requests, host="127.0.0.1", port=args.http_port)
            except KeyboardInterrupt:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
    return 0


def _request_shutdown(_signum, _frame) -> None:
    # Waitress treats KeyboardInterrupt as a request to stop its worker pool.
    # Ignore another SIGTERM while collectors and SQLite finish shutting down.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise KeyboardInterrupt


if __name__ == "__main__":
    raise SystemExit(main())
