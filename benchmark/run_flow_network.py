#!/usr/bin/env python3
"""Run FlowProtocol with per-connection netem in a disposable user/network namespace."""
import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time


def command(args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def wait_file(path, process, timeout=10):
    deadline = time.monotonic() + timeout
    while not path.exists():
        if process.poll() is not None:
            raise RuntimeError(f"benchmark exited with {process.returncode}; see benchmark.log")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"waiting for {path.name}")
        time.sleep(0.01)


def arguments():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--benchmark", type=Path, default=Path("build-flow-perf/benchmark/benchmark_flow"))
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--flows", type=int, choices=range(1, 101), default=100, metavar="1..100")
    p.add_argument("--nodes", type=int, choices=range(2, 9), default=3, metavar="2..8")
    p.add_argument("--rates-mbps", default="1000,100,500")
    p.add_argument("--rtt-ms", type=float, default=100)
    p.add_argument("--phase-seconds", type=int, default=5)
    p.add_argument("--warmup-seconds", type=int, default=2)
    p.add_argument("--chunk-bytes", type=int, default=32768)
    p.add_argument("--pause-flow-ms", type=int, default=0)
    p.add_argument("--duplex", type=int, choices=(0, 1), default=0)
    p.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    args = p.parse_args()
    args.benchmark = args.benchmark.resolve()
    args.output = args.output.resolve()
    rates = [int(item) for item in args.rates_mbps.split(",")]
    if not rates or any(not 1 <= r <= 10000 for r in rates):
        p.error("network rates must be 1..10000 Mbps")
    if not 0 <= args.rtt_ms <= 1000:
        p.error("RTT must be 0..1000 ms")
    return args, rates


def main():
    args, rates = arguments()
    args.output.mkdir(parents=True, exist_ok=True)
    if not args.inside:
        # The outer process never touches a host interface or qdisc.
        invocation = ["unshare", "--user", "--map-root-user", "--net", sys.executable,
                      str(Path(__file__).resolve()), *sys.argv[1:], "--inside"]
        environment = dict(os.environ)
        environment["FLOW_BENCHMARK_PARENT_NETNS"] = os.readlink("/proc/self/ns/net")
        completed = subprocess.run(invocation, env=environment)
        if completed.returncode:
            (args.output / "runner_failure.json").write_text(json.dumps({
                "status": "failed_or_unavailable", "returncode": completed.returncode,
                "invocation": invocation}, indent=2) + "\n")
        return completed.returncode

    host_ns = os.environ.get("FLOW_BENCHMARK_PARENT_NETNS")
    own_ns = os.readlink("/proc/self/ns/net")
    if not host_ns or host_ns == own_ns:
        raise RuntimeError("refusing to modify the host network namespace")
    command(["ip", "link", "set", "lo", "up"])
    # Avoid giant loopback segments; retain normal kernel offloads and record the MTU.
    command(["ip", "link", "set", "lo", "mtu", "1500"])
    command(["ip", "link", "add", "ifb0", "type", "ifb"])
    command(["ip", "link", "set", "ifb0", "mtu", "1500", "up"])
    control = args.output / "network-control"
    control.mkdir(exist_ok=True)
    for name in ("listeners.json", "network_ready", "ready.json", "start"):
        (control / name).unlink(missing_ok=True)
    (args.output / "metadata.json").unlink(missing_ok=True)
    invocation = [str(args.benchmark), "--output", str(args.output), "--network-control", str(control),
                  "--flows", str(args.flows), "--nodes", str(args.nodes), "--rates-mbps", args.rates_mbps,
                  "--warmup-seconds", str(args.warmup_seconds), "--phase-seconds", str(args.phase_seconds),
                  "--chunk-bytes", str(args.chunk_bytes), "--pause-flow-ms", str(args.pause_flow_ms),
                  "--duplex", str(args.duplex)]
    changes = []
    process = None
    try:
        with (args.output / "benchmark.log").open("w") as log:
            process = subprocess.Popen(invocation, stdout=log, stderr=subprocess.STDOUT)
            wait_file(control / "listeners.json", process)
            listeners = json.loads((control / "listeners.json").read_text())
            directions = []
            for listener in listeners:
                directions += [{"dst": listener["address"], "dport": listener["port"]},
                               {"src": listener["address"], "sport": listener["port"]}]
            command(["tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:", "htb", "default", "1"])
            command(["tc", "class", "add", "dev", "lo", "parent", "1:", "classid", "1:1", "htb",
                     "rate", "10000mbit", "ceil", "10000mbit", "quantum", "1514"])
            command(["tc", "qdisc", "add", "dev", "ifb0", "root", "handle", "2:", "prio", "bands",
                     str(len(directions) + 1), "priomap", *["0"] * 16])

            def shape(rate, operation):
                began = time.monotonic_ns()
                for index in range(len(directions)):
                    # Change the rate of existing backlog; netem supplies delay only.
                    command(["tc", "class", operation, "dev", "lo", "parent", "1:", "classid", f"1:{index + 2}",
                             "htb", "rate", f"{rate}mbit", "ceil", f"{rate}mbit", "burst", "64kb",
                             "cburst", "64kb", "quantum", "1514"])
                    if operation == "add":
                        # Receive ingress avoids sender-side TCP Small Queues artefacts.
                        command(["tc", "qdisc", "add", "dev", "ifb0", "parent", f"2:{index + 2}",
                                 "handle", f"{index + 10}:", "netem", "limit", "20000", "delay", f"{args.rtt_ms / 2}ms"])
                changes.append({"rate_mbps": rate, "began_monotonic_ns": began,
                                "applied_monotonic_ns": time.monotonic_ns()})

            shape(rates[0], "add")
            for index, link in enumerate(directions):
                for device, handle in (("lo", "1"), ("ifb0", "2")):
                    filter_command = ["tc", "filter", "add", "dev", device, "parent", handle + ":", "protocol", "ip",
                                      "prio", str(index + 1), "u32"]
                    for key, value in link.items():
                        match = [str(value), "0xffff"] if key.endswith("port") else [value + "/32"]
                        filter_command += ["match", "ip", key, *match]
                    command([*filter_command, "flowid", f"{handle}:{index + 2}"])
            command(["tc", "qdisc", "add", "dev", "lo", "handle", "ffff:", "ingress"])
            command(["tc", "filter", "add", "dev", "lo", "parent", "ffff:", "protocol", "ip", "prio", "1",
                     "u32", "match", "ip", "protocol", "6", "0xff", "action", "mirred", "egress", "redirect", "dev", "ifb0"])
            # Handshake and BBR/CUBIC start with the intended RTT, not a loopback history.
            (control / "network_ready").touch()
            wait_file(control / "ready.json", process)
            connections = json.loads((control / "ready.json").read_text())["connections"]
            (control / "start").touch()
            wait_file(args.output / "metadata.json", process)
            metadata = json.loads((args.output / "metadata.json").read_text())
            start = metadata["start_monotonic_ns"] / 1e9
            for phase, rate in enumerate(rates[1:], 1):
                deadline = start + args.warmup_seconds + phase * args.phase_seconds
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("benchmark exited before rate change")
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
                with (args.output / "tcp-states.txt").open("a") as stream:
                    stream.write(f"PHASE {phase - 1} END monotonic_ns={time.monotonic_ns()}\n")
                    stream.write(command(["ss", "-tinm"]))
                shape(rate, "change")
            deadline = start + args.warmup_seconds + len(rates) * args.phase_seconds - 0.2
            while time.monotonic() < deadline and process.poll() is None:
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            with (args.output / "tcp-states.txt").open("a") as stream:
                stream.write(f"PHASE {len(rates) - 1} END monotonic_ns={time.monotonic_ns()}\n")
                stream.write(command(["ss", "-tinm"]))
            timeout = args.warmup_seconds + args.phase_seconds + 20
            status = process.wait(timeout=timeout)
            statistics = "LO EGRESS / INGRESS\n" + command(["tc", "-s", "qdisc", "show", "dev", "lo"])
            statistics += "IFB RECEIVE DELAY\n" + command(["tc", "-s", "qdisc", "show", "dev", "ifb0"])
            (args.output / "qdisc.txt").write_text(statistics)
            (args.output / "classes.txt").write_text(command(["tc", "-s", "class", "show", "dev", "lo"]))
            tcp_settings = {}
            for key in ("tcp_rmem", "tcp_wmem", "tcp_congestion_control", "tcp_moderate_rcvbuf"):
                tcp_settings[key] = Path("/proc/sys/net/ipv4", key).read_text().strip()
            report = {"status": "passed" if status == 0 else "failed", "kernel": platform.platform(),
                      "tc_version": command(["tc", "-V"]).strip(), "namespace": own_ns,
                      "per_hop_rtt_ms": args.rtt_ms, "loopback_mtu": 1500,
                      "rate_scope": "each direction of each adjacent TCP connection, including TCP ACK and Flow CREDIT",
                      "netem_queue_limit_packets": 20000, "connections": connections, "changes": changes,
                      "shaper": "HTB egress bandwidth, netem on IFB receive ingress", "htb_burst_bytes": 65536,
                      "shaping_before_link_establishment": True,
                      "tcp_settings": tcp_settings, "invocation": invocation}
            (args.output / "network.json").write_text(json.dumps(report, indent=2) + "\n")
            print(f"Network result: {args.output} (exit {status})")
            return status
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        # Namespace teardown also removes all qdiscs, including after interruption.


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, TimeoutError, subprocess.CalledProcessError) as error:
        print(f"[FAIL] {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr, file=sys.stderr)
        sys.exit(1)
