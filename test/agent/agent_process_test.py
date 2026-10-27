"""Exercise the Agent executable's blocking signal shutdown and failed-start cleanup."""

import contextlib
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time


def reserve(stack):
    sock = stack.enter_context(socket.socket())
    sock.bind(("127.0.0.1", 0))
    return sock.getsockname()[1]


def run(binary, data, directory, stop_signal=None):
    with contextlib.ExitStack() as reservations:
        server_port, forward_port, occupied_port = [reserve(reservations) for _ in range(3)]
    config = {
        "server": {"host": "127.0.0.1", "port": server_port, "connect_timeout_ms": 200},
        "certificate": {
            "ca_file": str(data / "tls_channel_test_ca.pem"),
            "certificate_chain": str(data / "tls_channel_test_client.pem"),
            "private_key": str(data / "tls_channel_test_client.key"),
        },
        "forwards": [{"service": "echo", "listen_address": "127.0.0.1",
                      "listen_port": forward_port, "protocol": "tcp"}],
    }
    with socket.socket() as occupied:
        if stop_signal is None:
            occupied.bind(("127.0.0.1", occupied_port))
            occupied.listen()
            config["forwards"].append({"service": "blocked", "listen_address": "127.0.0.1",
                                       "listen_port": occupied_port, "protocol": "tcp"})
        path = directory / "agent.json"
        path.write_text(json.dumps(config))
        process = subprocess.Popen([binary, str(path)], stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True)
        try:
            if stop_signal is not None:
                deadline = time.monotonic() + 5
                while True:
                    try:
                        with socket.create_connection(("127.0.0.1", forward_port), timeout=0.1):
                            break
                    except OSError:
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError("Agent did not open its local listener")
                        time.sleep(0.01)
                process.send_signal(stop_signal)
            output, _ = process.communicate(timeout=5)
            expected = 0 if stop_signal is not None else 1
            if process.returncode != expected or "Agent stopped" not in output:
                raise RuntimeError(f"Agent did not join its workers cleanly ({process.returncode}):\n{output}")
            if stop_signal is None and "Address already in use" not in output:
                raise RuntimeError(f"Failed startup lost its original error:\n{output}")
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", forward_port))
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        for stop_signal in (signal.SIGTERM, signal.SIGINT, None):
            run(sys.argv[1], Path(sys.argv[2]).resolve(), Path(directory), stop_signal)
    print("[PASS] Agent blocking signal shutdown, listener release and failed-start worker cleanup")
