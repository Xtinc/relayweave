"""Real Node/control/data sockets -> Python collector -> Dashboard API.

Set RELAYWEAVE_NODE_BINARY to the built Node executable to enable these tests.
Three Nodes use distinct loopback addresses and the existing test certificates.
"""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time
import unittest

import dashboard as service
from history_store import HistoryStore
from proxy_client import ProxyControlClient
from proxy_protocol import CtrlMessage, MessageReceiver, pack_frame

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "test" / "data"
NODE_BINARY = os.environ.get("RELAYWEAVE_NODE_BINARY", "")


def client_context():
    context = ssl.create_default_context(cafile=str(DATA / "tls_channel_test_ca.pem"))
    context.load_cert_chain(str(DATA / "tls_channel_test_client.pem"), str(DATA / "tls_channel_test_client.key"))
    return context


def read_exact(sock, length):
    result = bytearray()
    while len(result) < length:
        chunk = sock.recv(length - len(result))
        if not chunk:
            raise EOFError("control/data socket closed")
        result.extend(chunk)
    return bytes(result)


class Control:
    def __init__(self, context, address, port):
        raw = socket.create_connection((address, port), timeout=5)
        self.socket = context.wrap_socket(raw, server_hostname="localhost")
        self.receiver = MessageReceiver()

    def send(self, command, params=None):
        self.socket.sendall(pack_frame(CtrlMessage(command, params)))

    def receive(self, expected):
        while True:
            length = int.from_bytes(read_exact(self.socket, 4), "big")
            message = self.receiver.receive(read_exact(self.socket, length))
            if message is None:
                continue
            if message.command == "ping":
                self.send("pong")
                continue
            if message.command != expected:
                raise AssertionError(f"Expected {expected}, got {message}")
            return message.params

    def close(self):
        self.socket.close()


@unittest.skipUnless(NODE_BINARY, "set RELAYWEAVE_NODE_BINARY to run live Flow tests")
class LiveFlowTest(unittest.TestCase):
    def setUp(self):
        self.resources = ExitStack()
        self.addCleanup(self.resources.close)
        directory = Path(self.resources.enter_context(tempfile.TemporaryDirectory()))
        self.context = client_context()
        self.configs = {}
        self.controls = {}
        reservations = []

        def port(kind=socket.SOCK_STREAM):
            reservation = socket.socket(type=kind)
            reservation.bind(("127.0.0.1", 0))
            reservations.append(reservation)
            return reservation.getsockname()[1]

        cluster_port, link_tcp, link_udp = port(), port(), port(socket.SOCK_DGRAM)
        for index, node in enumerate(("a", "b", "c")):
            config = json.loads((ROOT / "node" / "node.example.json").read_text())
            address = f"127.0.0.{index+1}"
            config["cluster"].update(role="master" if index == 0 else "slave", node_id=node,
                                     address="127.0.0.1", control_port=cluster_port,
                                     tcp_port=link_tcp, udp_port=link_udp)
            config["control"].update(address=address, advertise_address=address, port=port())
            for protocol, section in (("tcp", "tcp"), ("tls", "tls"), ("udp", "udp")):
                config[section].update(address=address, port=port(socket.SOCK_DGRAM if protocol == "udp" else socket.SOCK_STREAM))
                config[section]["rx_bytes_per_second"] = 0
                config[section]["tx_bytes_per_second"] = 0
            config["certificate"] = {
                "server_ca_file": str(DATA / "tls_channel_test_ca.pem"),
                "ca_file": str(DATA / "tls_channel_test_client_ca.pem"),
                "certificate_chain": str(DATA / "tls_channel_test_server.pem"),
                "private_key": str(DATA / "tls_channel_test_server.key"),
            }
            self.configs[node] = config
        for reservation in reservations:
            reservation.close()
        for node, config in self.configs.items():
            path = directory / f"{node}.json"
            path.write_text(json.dumps(config))
            log = self.resources.enter_context((directory / f"{node}.log").open("w+"))
            process = subprocess.Popen([NODE_BINARY, str(path)], stdout=log, stderr=subprocess.STDOUT)

            def stop(process=process):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            self.resources.callback(stop)
            deadline = time.monotonic() + 8
            while True:
                try:
                    with socket.create_connection((config["control"]["address"], config["control"]["port"]), timeout=.2):
                        break
                except OSError:
                    if process.poll() is not None or time.monotonic() >= deadline:
                        log.flush()
                        log.seek(0)
                        self.fail("Node startup failed: " + log.read())
                    time.sleep(.05)
        store = HistoryStore(directory / "history.sqlite3")
        self.resources.callback(store.close)
        self.collector = ProxyControlClient(
            host="127.0.0.1", port=self.configs["a"]["control"]["port"],
            ca_file=str(DATA / "tls_channel_test_ca.pem"), cert_file=str(DATA / "tls_channel_test_client.pem"),
            key_file=str(DATA / "tls_channel_test_client.key"), history_store=store,
            server_name="localhost", poll_interval=.15,
        )
        self.collector.start()
        self.resources.callback(self.collector.stop)
        previous = service._client
        service._client = self.collector
        self.resources.callback(setattr, service, "_client", previous)
        self.http = service.app.test_client()
        self.wait_map(lambda view: self.collector.snapshot(0, 0).topology is not None
                      and len(self.collector.snapshot(0, 0).topology.nodes) == 3 and view["state"] == "ready")
        for node in ("a", "c"):
            for role in ("consumer", "producer"):
                config = self.configs[node]["control"]
                control = Control(self.context, config["address"], config["port"])
                self.controls[node, role] = control
                self.resources.callback(control.close)
                if role == "producer":
                    for protocol in ("tcp", "tls", "udp"):
                        control.send("service.register", {"request_id": 1, "service": "demo-" + protocol, "protocol": protocol})
                        control.receive("service.ok")
        self.request = 100

    def wait_map(self, condition, query=""):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            response = self.http.get("/api/snapshot?queue_range=1&bandwidth_range=1" + query)
            self.assertEqual(response.status_code, 200)
            view = response.json["flow_map"]
            if condition(view):
                return view
            time.sleep(.05)
        self.fail("Flow snapshot did not converge: " + repr(view))

    def open_business(self, protocol, path, service_name=None):
        consumer = self.controls[path[0], "consumer"]
        producer = self.controls[path[-1], "producer"]
        request = self.request
        self.request += 1
        params = {"request_id": request, "service": service_name or "demo-" + protocol, "protocol": protocol}
        if len(path) > 1:
            params.update(path=path, epoch=self.collector.snapshot(0, 0).topology.epoch)
        consumer.send("relay.open", params)
        first, last = consumer.receive("relay.opened"), producer.receive("relay.offer")
        # Complete a collection round while neither data endpoint has attached.
        # This covers the Ready barrier for every protocol and business mode.
        previous_round = self.http.get("/api/snapshot").json["flow_map"]["round"]
        establishing = self.wait_map(lambda item: item["state"] == "ready" and item["round"] != previous_round)
        identity_field = "flow_id" if len(path) > 1 else "uuid"
        for route in establishing["routes"]:
            for business in route["businesses"]:
                if identity_field in business and (len(path) > 1 or business["node"] == path[0]):
                    self.assertNotEqual(business[identity_field], str(first[identity_field]))
        endpoints = []
        for node, endpoint, role in ((path[0], first, 2), (path[-1], last, 1)):
            address = self.configs[node]["control"]["address"]
            if protocol == "udp":
                stream = socket.socket(type=socket.SOCK_DGRAM)
                stream.settimeout(5)
                stream.connect((address, endpoint["data_port"]))
            else:
                stream = socket.create_connection((address, endpoint["data_port"]), timeout=5)
                if protocol == "tls":
                    stream = self.context.wrap_socket(stream, server_hostname="localhost")
            self.resources.callback(stream.close)
            stream.sendall(pack_frame(CtrlMessage("relay.attach", {"role": role, "uuid": endpoint["uuid"], "ticket": endpoint["ticket"]})))
            endpoints.append(stream)
        consumer.receive("relay.ready")
        producer.receive("relay.ready")
        payload = b"live-flow-map"
        if protocol == "udp":
            endpoints[0].send(first["session_id"].to_bytes(8, "big") + payload)
            self.assertEqual(endpoints[1].recv(4096), last["session_id"].to_bytes(8, "big") + payload)
        else:
            endpoints[0].sendall(payload)
            self.assertEqual(read_exact(endpoints[1], len(payload)), payload)

        def close():
            consumer.send("relay.cancel", {"request_id": request, "uuid": first["uuid"], "reason": "test complete"})
            if len(path) > 1 or protocol == "udp":
                consumer.receive("relay.closed")
                producer.receive("relay.closed")
            for stream in endpoints:
                stream.close()
        return close

    def test_all_protocols_single_and_multi_publish_and_clear(self):
        for protocol in ("tcp", "tls", "udp"):
            for path in (["a"], ["a", "b", "c"]):
                with self.subTest(protocol=protocol, path=path):
                    close = self.open_business(protocol, path)
                    view = self.wait_map(lambda item: item["flow_count"] == 1 and item["state"] == "ready")
                    self.assertEqual(view["routes"][0]["path"], path)
                    self.assertEqual(view["routes"][0]["protocol"], protocol)
                    self.assertEqual(view["routes"][0]["consumer"]["node"], path[0])
                    self.assertEqual(view["routes"][0]["producer"]["node"], path[-1])
                    close()
                    self.wait_map(lambda item: item["flow_count"] == 0 and item["state"] == "ready")

    def test_same_route_connections_merge_and_filter_in_python(self):
        close_one = self.open_business("tcp", ["a", "b", "c"])
        close_two = self.open_business("tcp", ["a", "b", "c"])
        close_three = self.open_business("tls", ["a"])
        view = self.wait_map(lambda item: item["flow_count"] == 3)
        self.assertEqual(view["route_count"], 2)
        filtered = self.wait_map(lambda item: item["flow_count"] == 3, "&flow_protocol=tcp")
        self.assertEqual(filtered["visible_route_count"], 1)
        self.assertEqual(filtered["routes"][0]["count"], 2)
        close_one()
        self.wait_map(lambda item: item["flow_count"] == 2)
        close_two()
        close_three()
        self.wait_map(lambda item: item["flow_count"] == 0)

    def test_branch_and_reverse_paths_share_actual_node_stations(self):
        producer = self.controls["a", "producer"]
        producer.send("service.register", {"request_id": 1, "service": "reverse-tcp", "protocol": "tcp"})
        producer.receive("service.ok")
        close_direct = self.open_business("tcp", ["a", "c"])
        close_branch = self.open_business("udp", ["a", "b", "c"])
        close_reverse = self.open_business("tcp", ["c", "b", "a"], "reverse-tcp")
        view = self.wait_map(lambda item: item["flow_count"] == 3)
        self.assertEqual({tuple(route["path"]) for route in view["routes"]},
                         {("a", "c"), ("a", "b", "c"), ("c", "b", "a")})
        self.assertEqual(view["svg"].count('class="flow-node"'), 3)
        close_direct()
        close_branch()
        close_reverse()
        self.wait_map(lambda item: item["flow_count"] == 0)


if __name__ == "__main__":
    unittest.main()
