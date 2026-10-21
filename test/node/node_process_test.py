"""Exercise the real Node executable's signal and failed-start thread cleanup."""

import contextlib
import json
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time


def reserve(stack, kind):
    sock = stack.enter_context(socket.socket(socket.AF_INET, kind))
    sock.bind(("127.0.0.1", 0))
    return sock


def configuration(data, reservations):
    config = json.loads((data / "node_config_modern.json").read_text())
    for key, filename in config["certificate"].items():
        config["certificate"][key] = str(data / filename)
    for section in ("control", "tcp", "tls"):
        config[section]["port"] = reserve(reservations, socket.SOCK_STREAM).getsockname()[1]
    config["udp"]["port"] = reserve(reservations, socket.SOCK_DGRAM).getsockname()[1]
    config["control"]["advertise_address"] = "127.0.0.1"
    config["cluster"]["address"] = "127.0.0.1"
    for key, kind in (("control_port", socket.SOCK_STREAM), ("tcp_port", socket.SOCK_STREAM),
                      ("udp_port", socket.SOCK_DGRAM)):
        config["cluster"][key] = reserve(reservations, kind).getsockname()[1]
    config["tcp"]["setup_timeout_ms"] = 300
    config["tls"]["setup_timeout_ms"] = 300
    return config


def rebind(config):
    for section, key, kind in (("control", "port", socket.SOCK_STREAM),
                               ("tcp", "port", socket.SOCK_STREAM),
                               ("tls", "port", socket.SOCK_STREAM),
                               ("udp", "port", socket.SOCK_DGRAM),
                               ("cluster", "tcp_port", socket.SOCK_STREAM),
                               ("cluster", "udp_port", socket.SOCK_DGRAM)):
        with socket.socket(socket.AF_INET, kind) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", config[section][key]))


def run(binary, data, directory, fail_start):
    with contextlib.ExitStack() as reservations:
        config = configuration(data, reservations)
    occupied = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with occupied:
        if fail_start:
            occupied.bind(("127.0.0.1", config["cluster"]["control_port"]))
            occupied.listen()
        path = directory / "node.json"
        path.write_text(json.dumps(config))
        process = subprocess.Popen([binary, str(path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            if not fail_start:
                deadline = time.monotonic() + 5
                while True:
                    try:
                        with socket.create_connection(("127.0.0.1", config["control"]["port"]), timeout=0.1):
                            break
                    except OSError:
                        if process.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError("Node did not open its control listener")
                        time.sleep(0.01)
                # Keep unbound TCP and TLS setup sockets alive while shutdown drains their deadlines.
                with contextlib.ExitStack() as pending:
                    for section in ("tcp", "tls"):
                        pending.enter_context(socket.create_connection(("127.0.0.1", config[section]["port"])))
                    process.send_signal(signal.SIGTERM)
                    output, _ = process.communicate(timeout=5)
            else:
                output, _ = process.communicate(timeout=5)
            expected = 1 if fail_start else 0
            if process.returncode != expected or "Node stopped" not in output:
                raise RuntimeError(f"Node did not join its workers cleanly ({process.returncode}):\n{output}")
            if fail_start and "Address already in use" not in output:
                raise RuntimeError(f"Failed startup lost its original error:\n{output}")
            rebind(config)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        for fail_start in (False, True):
            run(sys.argv[1], Path(sys.argv[2]).resolve(), Path(directory), fail_start)
    print("[PASS] Node signal shutdown, pending setup and failed-start process cleanup")
