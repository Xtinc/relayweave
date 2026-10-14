#!/usr/bin/env python3
"""Run bounded component benchmarks and preserve measurements, logs and host information."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

try:
    import resource
except ImportError:
    resource = None


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, default=Path("build-flow-perf"))
    parser.add_argument("--output", type=Path, help="Default: <build-dir>/benchmark/results/<UTC timestamp>")
    parser.add_argument("--cases", nargs="+", choices=("common", "routing", "node", "relay", "dual_index_map", "udp_node", "flow"),
                        default=["common", "routing", "node", "relay", "dual_index_map", "udp_node", "flow"])
    parser.add_argument("--quick", action="store_true", help="One repeat, smaller workloads, short Flow/UDP runs")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="Maximum runtime of each executable")
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 600:
        parser.error("timeout must be in [1, 600]")
    args.build_dir = args.build_dir.resolve()
    args.output = (args.output or args.build_dir / "benchmark/results" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")).resolve()
    return args


def usage():
    return resource.getrusage(resource.RUSAGE_CHILDREN) if resource else None


def main():
    args = arguments()
    args.output.mkdir(parents=True, exist_ok=False)  # Never overwrite recorded measurements.
    environment = {"started_utc": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
                   "machine": platform.machine(), "processor": platform.processor(), "python": sys.version,
                   "build_dir": str(args.build_dir), "quick": args.quick, "runs": []}
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        (args.output / "cpuinfo.txt").write_text(cpuinfo.read_text())
    cache = args.build_dir / "CMakeCache.txt"
    if cache.exists():
        environment["cmake"] = [line for line in cache.read_text().splitlines()
                                if line.startswith(("CMAKE_BUILD_TYPE:", "CMAKE_CXX_COMPILER:", "CMAKE_CXX_FLAGS",
                                                    "ENABLE_IO_URING:", "BUILD_TESTING:", "BUILD_BENCHMARKS:"))]
    source = Path(__file__).resolve().parent.parent
    environment["benchmark_sha256"] = {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
                                       for path in sorted(source.joinpath("benchmark").iterdir())
                                       if path.suffix in (".cpp", ".h", ".py")}
    git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=source, text=True, capture_output=True)
    if git.returncode == 0:
        environment["git_head"] = git.stdout.strip()
        state = subprocess.run(["git", "status", "--short"], cwd=source, text=True, capture_output=True)
        environment["git_status"] = state.stdout
    rows = []
    failed = False
    for case in args.cases:
        executable = args.build_dir / "benchmark" / ("benchmark_" + case)
        command = [str(executable)]
        if case == "flow":
            command += ["--flows", "100", "--nodes", "3", "--warmup-seconds", "0" if args.quick else "1",
                        "--phase-seconds", "1" if args.quick else "3", "--rates-mbps", "1000,100,500,1000",
                        "--output", str(args.output / "flow/sink-steps")]
        elif case == "udp_node":
            command += ["--duration-seconds", "1" if args.quick else "3", "--drain-ms", "200"]
        else:
            command += ["--repeats", "1" if args.quick else "3", "--output", str(args.output / (case + ".json"))]
            if args.quick:
                count = {"common": 10000, "routing": 2, "node": 10000, "relay": 64, "dual_index_map": 10000}[case]
                command += ["--iterations", str(count)]
        before = usage()
        started = time.monotonic()
        run = {"case": case, "command": command, "started_utc": datetime.now(timezone.utc).isoformat()}
        log = args.output / (case + ".log")
        print(f"Running {case} -> {log}", flush=True)
        try:
            with log.open("w") as output:
                result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=args.timeout_seconds, cwd=source)
            run["returncode"] = result.returncode
        except subprocess.TimeoutExpired:
            run["error"] = "timeout"
            run["returncode"] = -1
        except OSError as error:
            run["error"] = str(error)
            run["returncode"] = -1
        run["seconds"] = time.monotonic() - started
        after = usage()
        if after:
            run["cpu_seconds"] = after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime
            # ru_maxrss is a high-water mark across children; label it as suite peak, not per-case RSS.
            run["suite_peak_rss_mib"] = after.ru_maxrss / (1024 * 1024 if sys.platform == "darwin" else 1024)
        failed |= run["returncode"] != 0
        environment["runs"].append(run)
        measurement = args.output / (case + ".json")
        if run["returncode"] == 0 and measurement.exists():
            data = json.loads(measurement.read_text())
            grouped = {}
            for sample in data["samples"]:
                grouped.setdefault(sample["case"], []).append(sample)
            for name, samples in grouped.items():
                rows.append({"target": case, "case": name, "repeats": len(samples),
                             "median_ns_per_operation": statistics.median(s["ns_per_operation"] for s in samples),
                             "median_operations_per_second": statistics.median(s["operations_per_second"] for s in samples),
                             "median_payload_mbps": statistics.median(s["payload_mbps"] for s in samples),
                             "median_cpu_seconds": statistics.median(s["cpu_seconds"] for s in samples),
                             "min_seconds": min(s["seconds"] for s in samples), "max_seconds": max(s["seconds"] for s in samples)})
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2, ensure_ascii=False) + "\n")
    if rows:
        with (args.output / "components.csv").open("w", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    lines = ["# 组件性能记录", "", f"开始时间（UTC）：{environment['started_utc']}",
             f"运行环境：{environment['platform']}，{environment['machine']}",
             f"模式：{'短时验证，单次采样' if args.quick else '组件重复三次'}", "",
             "| 用例 | 状态 | 总耗时 s | 进程 CPU s | 累计子进程峰值 RSS MiB |", "|---|---|---:|---:|---:|"]
    for run in environment["runs"]:
        lines.append(f"| {run['case']} | {'通过' if run['returncode'] == 0 else '失败'} | {run['seconds']:.3f} | "
                     f"{run.get('cpu_seconds', 0):.3f} | {run.get('suite_peak_rss_mib', 0):.2f} |")
    lines += ["", "RSS 为当前套件已退出子进程的累计峰值；原始命令、日志、构建与主机信息见 environment.json。",
              "组件指标包含用例内的结果校验开销；吞吐为业务载荷，缓存/队列/计数的操作速率不换算为网络带宽。", "",
              "| 组件用例 | 次数 | 中位耗时 ns/操作 | 中位操作/秒 | 中位载荷 Mbps |", "|---|---:|---:|---:|---:|"]
    for row in rows:
        lines.append(f"| {row['case']} | {row['repeats']} | {row['median_ns_per_operation']:.1f} | "
                     f"{row['median_operations_per_second']:.0f} | {row['median_payload_mbps']:.2f} |")
    lines += ["", "逐次原始值见各组件 JSON，汇总见 components.csv；UDP Node 的包速率、丢包与重复包见 udp_node.log。",
              "通过表示执行与数据/状态校验成功，实测性能不使用跨机器统一通过阈值。"]
    if "flow" in args.cases and (args.output / "flow/sink-steps/summary.json").exists():
        flow_report = Path(__file__).with_name("flow_report.py")
        result = subprocess.run([sys.executable, str(flow_report), str(args.output / "flow")], cwd=source)
        failed |= result.returncode != 0
        lines += ["", "Flow 的阶段吞吐、收敛、信用与缓存记录见 [Flow 报告](flow/报告.md)。"]
    (args.output / "报告.md").write_text("\n".join(lines) + "\n")
    print(f"Saved report: {args.output / '报告.md'}", flush=True)
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
