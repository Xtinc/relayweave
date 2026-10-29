# 关键通道 benchmark

只保留五个性能研究入口，每个例子一个 `.cpp`。测量生产数据通道，避免用容器、路由算法、控制面或代理连接测试稀释重点。benchmark 手动运行，与功能 CTest 分开，不设置跨机器通用性能门槛。

| 文件 / 可执行目标 | 测量问题 | 关键指标 |
|---|---|---|
| `tcp_relay_benchmark.cpp` / `benchmark_tcp_relay` | TCP relay 相对直连的转发成本；1400 B / 32 KiB 块 | 双向业务吞吐、块/秒、CPU 时间；直连使用相同生成与校验逻辑 |
| `tls_relay_benchmark.cpp` / `benchmark_tls_relay` | TLS 1.2 / 1.3 的 TLS↔TLS 与 TCP↔TLS relay 成本 | 双向业务吞吐、CPU 时间；握手单独计时，记录协商版本和密码套件 |
| `udp_relay_benchmark.cpp` / `benchmark_udp_relay` | UDP relay 的往返成本与持续双向发包能力；1400 B 业务报文 | RTT p50/p95/p99、发包/转发 pps、业务吞吐、各方向接收量、丢包率、乱序数 |
| `flow_benchmark.cpp` / `benchmark_flow` | Flow 复用与多跳转发的开销 | 终点吞吐、每流发送等待 p50/p95/p99、CPU、RSS、各节点缓存与载荷池占用 |
| `congestion_benchmark.cpp` / `benchmark_congestion` | 带宽降低、恢复与单流暂停时的信用流控及背压能力 | 稳态吞吐、收敛时间、超额字节、公平性、健康流进展、缓存上限与排空 |

`benchmark.h` 统一输出；`relay_workload.h` 复用 socket 建立、收发与完整性校验；`flow_workload.h` 复用真实 FlowProtocol/NodeLink 拓扑、采样与资源检查。实验入口负责定义各自的负载和默认参数。

## 构建和运行

```sh
cmake -S . -B build-flow-perf -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=ON
cmake --build build-flow-perf --target benchmarks --parallel 2

# 五个入口的短时正确性验证，不用于性能结论。
python3 benchmark/run_benchmarks.py --build-dir build-flow-perf --quick

# Relay 各重复三次；Flow 预热 2 秒，吞吐测量 5 秒；调速各阶段 5 秒。
# 拥塞入口另运行一次单流暂停实验（暂停 3 秒，测量 8 秒）。
python3 benchmark/run_benchmarks.py --build-dir build-flow-perf

# 只运行指定通道。
python3 benchmark/run_benchmarks.py --build-dir build-flow-perf --cases tcp_relay tls_relay
```

runner 为每次运行创建独立目录，默认位于 `<build-dir>/benchmark/results/<UTC 时间戳>/`，记录环境、构建参数、源码摘要、Git 状态、实际命令和日志。`relays.csv` 保存 Relay 各项中位数，`phases.csv` 保存 Flow/拥塞各阶段统计，`报告.md` 与 `flow-report.md` 便于查阅。失败或超时返回非零，原始记录仍保留。`--output DIRECTORY` 可指定新的运行目录，`--timeout-seconds` 默认 120 秒。

三个 Relay 入口均支持 `--iterations N --repeats N --output FILE.json`。TCP/TLS 的 iterations 表示每个方向的业务块数，UDP 表示往返次数和饱和实验每方向的发包数；JSON 的 operations 分别为完成的方向块数、握手数、往返数或成功转发报文数。默认 TCP/TLS 为 4096，UDP 为 65536；默认重复三次。

## 多跳 Flow

```sh
# A → B → C，100 条 Flow 复用两条真实 TCP NodeLink，不限速。
build-flow-perf/benchmark/benchmark_flow --nodes 3 --flows 100 \
  --warmup-seconds 2 --phase-seconds 5 --output build-flow-perf/benchmark/results/flow-3nodes

# 对照：分别修改 --nodes 2/3/5，--flows 1/10/100，--chunk-bytes 4096/32768。
# 每次只改一个变量并使用新的输出目录；--duplex 1 测量双向传输。
```

支持 2–8 个节点、1–100 条 Flow、16–65467 B 数据块、1–120 秒测量阶段。`--nodes` 是端点在内的节点数，跳数为 nodes−1。吞吐按终点完成消费的字节计算，预热和排空不计入阶段吞吐。

## 带宽拥塞与背压

```sh
# 终点消费带宽：1 Gbps → 100 Mbps → 500 Mbps → 1 Gbps。
build-flow-perf/benchmark/benchmark_congestion --nodes 3 --flows 100 \
  --rates-mbps 1000,100,500,1000 --phase-seconds 5 \
  --output build-flow-perf/benchmark/results/congestion-steps

# 单条流暂停，其余流继续；观察背压隔离及恢复。
build-flow-perf/benchmark/benchmark_congestion --flows 100 --rates-mbps 0 \
  --pause-flow-ms 3000 --phase-seconds 8 \
  --output build-flow-perf/benchmark/results/congestion-slow-flow

# 真实网络瓶颈：每跳 RTT 100 ms，按阶段改变每条 NodeLink 两方向的带宽。
python3 benchmark/run_congestion_network.py \
  --benchmark build-flow-perf/benchmark/benchmark_congestion \
  --nodes 3 --flows 100 --rtt-ms 100 --rates-mbps 1000,100,500,1000 \
  --warmup-seconds 5 --phase-seconds 5 \
  --output build-flow-perf/benchmark/results/congestion-network

# 单个实验目录或包含多个实验的父目录均可；--plot 额外需要 matplotlib。
python3 benchmark/flow_report.py build-flow-perf/benchmark/results/congestion-steps --plot
```

终点消费实验使用生产 TokenBucket，按每流等分目标速率，观察应用层信用周转；它模拟下游消费速度，不代表物理链路拥塞。真实网络实验在一次性 user/network namespace 内用 HTB 调带宽、IFB/netem 加接收延迟，建链前即应用整形，保存实际切换时间、TCP 状态及 qdisc 统计。要求 Linux、`unshare`、`ip`、`tc`、`ss`、用户命名空间与 IFB 支持；不可用时明确失败，不操作宿主网卡。它测量 Flow 信用控制与内核 TCP 拥塞控制共同作用的结果。

CLI 的 Mbps 是每方向聚合目标，0 表示不限速；双向实验的汇总吞吐和目标均为两个方向之和。每个实验输出 `timeline.csv`（100 ms 采样）、`flows.csv`（每流每阶段字节数和等待分位数）、`metadata.json`、`summary.json`。

排空预算为 10 秒加上预计传输时间的两倍，预计时间按未消费载荷（含每流一个可能尚未完成交接的数据块）和末阶段速率计算。网络模式取两个方向中较大的积压；消费限速模式按积压最多的流及其等分速率计算；不限速模式保留 10 秒。`drain.json` 在排空开始时发布预算，网络 runner 使用同一预算等待退出；`summary.json` 记录积压上界、超时预算和实际排空耗时。

收敛定义为至少 500 ms 的滑动窗口连续三次进入目标 ±10%，包含约 0.7 秒的统计观察下限。后半阶段吞吐用于稳态比较；未达到目标保留原始结果，不等同于功能失败。`phases.csv` 另存正向超额字节、阶段净超额字节、Jain 公平指数（1 表示等份）及健康流最低/最高吞吐，`flows.csv` 的等待分位数为直方图微秒上界。

流控的数学模型、固定容量参数、节点 20 ms 批次与各场景扫描结论统一见
[RelayWeave 逐跳信用流控机制](../docs/RelayWeave设计.md#54-逐跳信用流控机制-hop-by-hop-credit-flow-control)。
精确期限、即时刷新及多 Flow 批次隔离由 `lnk_flow_control` 回归验证；
本目录的普通 Flow、拥塞与网络基准用于测量吞吐、等待和实际内存。

## 测量边界

- TCP/TLS 使用真实本机 socket，socket 建立、证书加载不计入转发时间；TLS 握手单列。业务吞吐排除 TLS/UDP 帧头。
- UDP 往返只保留一个在途报文，不能视为饱和吞吐。饱和实验无逐包确认，独立并发发收，发完后留 200 ms 排空；吞吐和转发 pps 的计时包含排空，短跑会低估持续吞吐。丢包作为测量值记录，内容错误或重复序号使实验失败。
- 每个入口都校验完整业务载荷、序号及方向；TCP/Flow 额外检查结束位置和排空字节数。Flow 采样检查每方向信用窗口；排空任务并释放端点和协议持有者后，通过一次执行器交接让末尾完成回调退出，再以弱引用检查池最终销毁，结果写入 `pool_resources_released_after_cleanup`。运行中的标准池允许缓存历史高水位，不在 stop 时调用 `release()`。
- Flow 时间线保留逻辑窗口占用、RSS、CPU 和吞吐，不依赖生产池的私有字节统计。汇总脚本兼容旧结果的 `payloads_released_after_stop` 字段；该旧字段只证明在用载荷已归还，与新结果的池最终销毁检查含义不同。
- 当前使用单进程、单 io_context、单线程。CPU 包含生成、复制与校验开销；TCP 直连对照用于识别这些开销。RSS 不含内核 socket/整形队列内存。
- Flow 实验不包含 Agent、控制面建路或 TLS 接入。本机回环和 netem 数据用于比较通道成本，跨机器性能需在目标环境另测。
