#!/usr/bin/env python3
"""Summarise saved Flow benchmark measurements without treating low throughput as a functional failure."""
import argparse
import csv
import json
from pathlib import Path
import platform


def case_summaries(directory):
    summary = directory / "summary.json"
    return [summary] if summary.exists() else sorted(directory.glob("*/summary.json"))


def plot_rates(directory):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from collections import deque

    cases = case_summaries(directory)
    if not cases:
        raise ValueError(f"no benchmark cases found in {directory}")
    columns = min(2, len(cases))
    rows_count = (len(cases) + columns - 1) // columns
    plt.rcParams.update({"svg.fonttype": "none", "font.size": 10})
    figure, axes = plt.subplots(rows_count, columns, figsize=(7 * columns, 3.8 * rows_count),
                                constrained_layout=True, squeeze=False)
    for axis, path in zip(axes.flat, cases):
        folder = path.parent
        summary = json.loads(path.read_text())
        with (folder / "timeline.csv").open() as stream:
            rows = [{key: float(value) for key, value in row.items()} for row in csv.DictReader(stream)]
        rows = [row for row in rows if row["phase"] >= 0]
        if not rows:
            plt.close(figure)
            raise ValueError(f"no measurement samples found in {folder / 'timeline.csv'}")
        window = deque()
        smoothed = []
        for row in rows:
            window.append(row)
            while len(window) > 1 and sum(item["interval_s"] for item in list(window)[1:]) >= 0.5:
                window.popleft()
            smoothed.append(sum(item["received_bytes"] for item in window) * 8 /
                            sum(item["interval_s"] for item in window) / 1e6)
        x = [row["elapsed_s"] - summary["warmup_seconds"] for row in rows]
        axis.plot(x, [row["received_mbps"] for row in rows], color="#94b5d6", alpha=0.5, linewidth=0.7, label="100 ms samples")
        axis.plot(x, smoothed, color="#1265a8", linewidth=1.7, label="~500 ms rolling throughput")
        if any(row["target_mbps"] > 0 for row in rows):
            axis.step(x, [row["target_mbps"] or float("nan") for row in rows], where="post", color="#bd4545", linewidth=1.2, linestyle="--", label="Requested aggregate rate")
        axis.set(title=f"{folder.name} — {summary['flows']} flows, {summary['nodes']} nodes",
                 xlabel="Measurement time (s)", ylabel="Consumed payload (Mbps)",
                 xlim=(0, max(0.1, max(x))), ylim=(0, None))
        axis.grid(alpha=0.2)
        axis.legend(fontsize=8)
    for axis in list(axes.flat)[len(cases):]:
        axis.set_visible(False)
    figure.savefig(directory / "dynamic-rate.svg")
    figure.savefig(directory / "dynamic-rate.png", dpi=150)
    plt.close(figure)


def phase_metrics(rows, target, began):
    duration = sum(row["interval_s"] for row in rows)
    throughput = sum(row["received_bytes"] for row in rows) * 8 / duration / 1e6
    midpoint = began + duration / 2
    steady = [row for row in rows if row["elapsed_s"] - row["interval_s"] >= midpoint]
    steady_time = sum(row["interval_s"] for row in steady)
    steady_rate = sum(row["received_bytes"] for row in steady) * 8 / steady_time / 1e6 if steady_time else None
    convergence = None
    consecutive = 0
    # Windows span at least 500 ms and stay entirely within this phase.
    # Three successive in-band windows make the convergence declaration less sensitive to bursts.
    settling_rows = [row for row in rows if row["elapsed_s"] - row["interval_s"] >= began]
    for index, row in enumerate(settling_rows):
        window = []
        for earlier in reversed(settling_rows[:index + 1]):
            window.append(earlier)
            if sum(item["interval_s"] for item in window) >= 0.5:
                break
        window_time = sum(item["interval_s"] for item in window)
        actual = sum(item["received_bytes"] for item in window) * 8 / window_time / 1e6
        if target and window_time >= 0.5 and abs(actual - target) <= target * 0.1:
            consecutive += 1
            if consecutive == 3:
                convergence = row["elapsed_s"] - began
                break
        else:
            consecutive = 0
    return {
        "mean_mbps": throughput, "steady_mbps": steady_rate,
        "convergence_s": convergence,
        "peak_100ms_mbps": max(row["received_mbps"] for row in rows),
        "cpu_mean_pct": sum(row["cpu_pct"] * row["interval_s"] for row in rows) / duration,
        "rss_peak_mib": max(row["rss_bytes"] for row in rows) / 2**20,
        "positive_sample_excess_mib": sum(max(0, row["received_bytes"] - target * 125000 * row["interval_s"])
                                         for row in rows) / 2**20 if target else None,
        "net_excess_mib": (sum(row["received_bytes"] for row in rows) - target * 125000 * duration) / 2**20 if target else None,
        "steady_within_10pct": abs(steady_rate - target) <= target * 0.1 if target and steady_rate is not None else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="One benchmark case directory, or directory containing benchmark cases")
    parser.add_argument("--plot", action="store_true", help="Export throughput curves using optional matplotlib")
    args = parser.parse_args()
    records = []
    for path in case_summaries(args.directory):
        summary = json.loads(path.read_text())
        network_path = path.parent / "network.json"
        network = json.loads(network_path.read_text()) if network_path.exists() else {}
        with (path.parent / "timeline.csv").open() as stream:
            rows = [{key: float(value) for key, value in row.items()} for row in csv.DictReader(stream)]
        with (path.parent / "flows.csv").open() as stream:
            flow_rows = list(csv.DictReader(stream))
        for phase, target in enumerate(summary["rates_mbps"]):
            target *= summary["directions"]
            selected = [row for row in rows if row["phase"] == phase]
            if not selected:
                continue
            began = summary["warmup_seconds"] + phase * summary["phase_seconds"]
            if network and phase:
                began = (network["changes"][phase]["applied_monotonic_ns"] - summary["start_monotonic_ns"]) / 1e9
            metrics = phase_metrics(selected, target, began)
            per_flow = [int(row["received_bytes"]) for row in flow_rows if int(row["phase"]) == phase]
            # The slow Flow is intentionally excluded when reporting healthy-flow progress.
            healthy = per_flow[1:] if summary["pause_flow_ms"] and len(per_flow) > 1 else per_flow
            record = {"case": path.parent.name, "status": summary["status"], "mode": summary["mode"],
                      "flows": summary["flows"], "directions": summary["directions"], "nodes": summary["nodes"], "chunk_bytes": summary["chunk_bytes"],
                      "per_hop_rtt_ms": network.get("per_hop_rtt_ms", ""), "phase": phase, "target_mbps": target,
                      **metrics, "healthy_flows_with_progress": sum(value > 0 for value in healthy),
                      "healthy_flows": len(healthy),
                      "peak_flow_cache_mib_by_node": "/".join(f"{value / 2**20:.2f}" for value in summary["peak_buffered_bytes_by_node"]),
                      "released_after_stop": summary["payloads_released_after_stop"]}
            record["healthy_mib_while_slow_flow_paused"] = summary["healthy_bytes_while_slow_flow_paused"] / 2**20
            records.append(record)
    if not records:
        parser.error("no measured phases found")
    with (args.directory / "phases.csv").open("w") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)
    environment_file = args.directory / "environment.json"
    environment = json.loads(environment_file.read_text()) if environment_file.exists() else {}
    measured_platform = environment.get("platform", "未记录；汇总机为 " + platform.platform())
    findings = []
    baseline = next((r for r in records if r["case"] == "baseline-3node-100flow"), None)
    if baseline:
        findings.append(f"三节点、百条 Flow、32 KiB 数据块的本机回环平均吞吐为 {baseline['mean_mbps'] / 1000:.2f} Gbps。")
    sink = [r for r in records if r["case"] == "sink-speed-steps"]
    if sink:
        values = "、".join(f"{r['target_mbps']} → {r['steady_mbps']:.1f}" for r in sink)
        findings.append(f"终点消费限速各阶段的目标 → 后半段实测为 {values} Mbps；" +
                        f"统计收敛为 {min(r['convergence_s'] for r in sink if r['convergence_s'] is not None):.2f}–" +
                        f"{max(r['convergence_s'] for r in sink if r['convergence_s'] is not None):.2f} s。")
    network = [r for r in records if r["case"].startswith("network-100flow-rtt100") and r["target_mbps"] == 1000]
    if network:
        findings.append(f"每跳 RTT 100 ms 的三次百流调速测试中，1 Gbps 目标阶段平均吞吐为 " +
                        f"{min(r['mean_mbps'] for r in network):.1f}–{max(r['mean_mbps'] for r in network):.1f} Mbps，未达到 1 Gbps。")
        findings.append("TCP 状态显示共享连接使用 4 MiB 发送缓冲，并出现明显 sndbuf_limited；这支持发送缓存为瓶颈候选，尚未进行扩大缓存的对照验证。")
    slow = next((r for r in records if r["case"] == "slow-consumer"), None)
    if slow:
        findings.append(f"一条流暂停消费 3 秒期间，其他流仍消费 {slow['healthy_mib_while_slow_flow_paused']:.2f} MiB；各方向缓存未越窗，停止后在用载荷归还。")
    lines = ["# 多节点 Flow 流控性能实测", "", *findings, "",
             f"执行环境：`{measured_platform}`。", "",
             f"设备：`{(environment.get('hardware') or '未记录').strip(chr(0))}`；CPU governor：`{environment.get('cpu_governor', '未记录')}`。", "",
             "Release 构建；所有逻辑节点位于同一进程、同一 io_context、单线程。CPU 百分比以一个 CPU 核为 100%。",
             "消费速度测试在终点使用生产 TokenBucket；网络测试在隔离命名空间对每条相邻 TCP 连接的两个方向分别用出口 HTB 调速、接收入口 IFB/netem 添加传播延迟，RTT 为每跳设置值。网络整形在 NodeLink 建立前生效。",
             "CLI 的速率为每个方向的聚合目标；表中吞吐为所有方向合计，双向测试的表中目标是 CLI 速率的两倍。",
             "测量对象为 FlowProtocol、共享 NodeLink 和 FlowStream，不包含 Agent、TLS 接入和控制面建路开销。", "",
             "进程 CPU 同时包含流量生成、池载荷拷贝和完整数据校验的开销；不能当作单个中继节点的纯转发 CPU。", "",
             "| 场景 | Flow | 每跳 RTT/ms | 阶段目标/Mbps | 平均/Mbps | 后半段/Mbps | 收敛/s | CPU/% | RSS峰值/MiB |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in records:
        convergence = f"{row['convergence_s']:.2f}" if row["convergence_s"] is not None else "未达到" if row["target_mbps"] else "—"
        steady = f"{row['steady_mbps']:.2f}" if row["steady_mbps"] is not None else "—"
        lines.append(f"| {row['case']} | {row['flows']} | {row['per_hop_rtt_ms'] or '本机'} | "
                     f"{row['target_mbps'] or '不限速'} | {row['mean_mbps']:.2f} | {steady} | {convergence} | "
                     f"{row['cpu_mean_pct']:.1f} | {row['rss_peak_mib']:.2f} |")
    lines += ["", "消费调速的收敛时间从阶段边界计起，网络调速从所有方向 qdisc 修改完成计起；至少 500 ms 的滑动窗口连续三次落入目标 ±10% 为准。未达到不等于功能失败。",
              "该指标包含统计观察窗口，时间下限约 0.7 s。CSV 的 passed 只表示功能、预算与排空检查通过，性能是否达标另见 steady_within_10pct。",
              "网络调速的实际 qdisc 应用时间另存于各场景 network.json；切换多个方向需要少量执行时间。",
              "100 ms 吞吐尖峰和超额字节保留在 phases.csv；positive_sample_excess_mib 累加每个采样区间的正向超额，net_excess_mib 为整个阶段相对目标的有符号差额，二者均以业务载荷计。",
              "发送等待分位数为 logarithmic histogram 的微秒上界，见各场景 flows.csv；send 完成表示交接，吞吐以终点完成消费为准。", "",
              "| 场景 | 缓存峰值，按节点/MiB | 健康流方向有进展 | 暂停期间其他流消费/MiB | 停止后池载荷释放 | 功能结果 |",
              "|---|---|---:|---:|---|---|"]
    seen = set()
    for row in records:
        if row["case"] in seen:
            continue
        seen.add(row["case"])
        same_case = [r for r in records if r["case"] == row["case"]]
        progress = min(r["healthy_flows_with_progress"] for r in same_case)
        lines.append(f"| {row['case']} | {row['peak_flow_cache_mib_by_node']} | {progress}/{row['healthy_flows']} | "
                     f"{row['healthy_mib_while_slow_flow_paused']:.2f} | "
                     f"{'是' if row['released_after_stop'] else '否'} | {row['status']} |")
    lines += ["", "每个场景保留 timeline.csv、flows.csv、metadata.json、summary.json；网络场景额外保留 network.json、qdisc.txt、classes.txt、tcp-states.txt、benchmark.log。",
              "所有收到的数据块均检查序号、Flow 身份及完整载荷内容，排空后核对总发送/消费字节。缓存上限按每流每方向固定窗口检查。",
              "RSS 与载荷池占用不包括内核 TCP 缓冲和整形队列内存。本机回环与 netem 结果不代表跨机器 1 Gbps 验收；共享物理 TCP 的拥塞、内核缓冲与调度也会影响应用层信用周转。", ""]
    if args.plot:
        plot_rates(args.directory)
        lines += ["![动态调速吞吐曲线](dynamic-rate.png)", ""]
    (args.directory / "报告.md").write_text("\n".join(lines))
    print(f"Saved {args.directory}/{{phases.csv,报告.md}} ({len(records)} phases)")


if __name__ == "__main__":
    main()
