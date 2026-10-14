# 性能基准

性能基准与功能 CTest 分开，手动运行；不以机器性能阈值判定功能测试是否通过。

| 分组 / 目标 | 组件与用例 | 规模与指标 |
|---|---|---|
| 基础组件 `benchmark_common` | PayloadPool/PooledBuffer 复用与复制；NodeLink/UDP 帧头；控制消息 CBOR 编解码与分片重组；TokenBucket 准入、丢弃、预留和协程等待；TrafficCounter；LRUCache；channel 池载荷交接；AsyncEvent 广播 | 1400/4096/32768/65467 字节块、100 个预热池块、100/10000 缓存项、1/4 个计数线程、1/100 个等待者；ns/操作、操作/秒、复制 Mbps、限速实测速率、上游分配次数 |
| 索引 `benchmark_dual_index_map` | 生产侵入式双索引与标准容器参考实现的插入、查找、更新次索引、删除 | 默认 200000 项，各阶段单独计时，相同校验和 |
| 路由 `benchmark_routing` | RouteGraph 构建与分层路径搜索；LinkQuality 更新与评估；AgentRouting 接收快照与计算候选路径；Topology 快照校验 | 32/128/512/1024 节点，4/8 层、2 个入口、指数跨度有向边；100 条链路质量、5% 模拟丢失；耗时、操作/秒、可达节点数 |
| Node `benchmark_node` | RegistryMgr 服务/会话查找、服务列表、流量采样与报告、会话退出并重建其服务 | 100 个会话、100/10000 个服务；查找速率、报告耗时、会话重建耗时 |
| 传输 `benchmark_relay` | TCP 双向复制；TLS 1.2/1.3 握手和 TLS↔TLS、TCP↔TLS 复制；UDP 双向报文；TLSChannel 控制消息；HTTP CONNECT/SOCKS5 代理隧道与连接反复建立 | 真实本机 socket；1400/32768 字节流块、1400 字节 UDP；业务吞吐、握手/报文往返耗时、控制消息/秒、连接/秒 |
| 多节点 `benchmark_flow` | FlowProtocol/NodeLink 的 TCP 每流信用、多路复用、中继、池载荷转移、动态调速、背压、多流隔离和双向传输 | 1/10/100 Flow、2–8 节点；吞吐、调速收敛、等待分布、CPU、RSS、信用与缓存占用 |
| UDP Node `benchmark_udp_node` | mTLS 控制接入、Registry/DatagramMgr 业务链路、真实 UDP Relay 饱和或定速发送 | 发包率、转发率、丢包率、重复包、内容校验；默认 1400 字节报文 |

## 构建与产物目录

```console
cmake -S . -B build-flow-perf -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=ON
cmake --build build-flow-perf --target benchmarks --parallel 2

# 短时验证；每个组件运行一次，Flow 各调速阶段 1 秒、UDP Node 1 秒。
python3 benchmark/run_benchmarks.py --build-dir build-flow-perf --quick

# 组件各运行三次，Flow 各阶段 3 秒、UDP Node 3 秒。
python3 benchmark/run_benchmarks.py --build-dir build-flow-perf

# 只运行选定组件；单个目标默认有 120 秒超时。
python3 benchmark/run_benchmarks.py --cases common routing node --quick
```

CSV、日志、曲线、运行参数和实测报告保存到构建目录下的 `benchmark/results/`，不进入 Git。
`benchmark_flow` 的默认输出为该目录下的 `flow/`；多场景运行通过 `--output` 指定不同子目录。
`benchmark/results/` 等源码目录内的本地输出目录也被忽略。
统一 runner 为每次运行建立独立时间戳目录，保存 `environment.json`、各目标日志、逐次 JSON、
中位数 `components.csv` 和 `报告.md`。记录主机、构建参数、Git 版本、本地修改状态、基准源码 SHA-256、实际命令、
进程 CPU 时间与套件累计子进程峰值 RSS；异常和超时也写入记录并以非零状态退出。
已存在的运行目录不能覆盖；需要再次运行时使用新目录。
多配置生成器的可执行文件可能位于 `Debug/` 或 `Release/` 子目录。

## 组件用例

基础组件、索引、路由、Node 和传输目标使用相同参数：
`--iterations N`、`--repeats N`、`--output FILE.json`。输出默认位于构建目录的
`benchmark/results/<组件名>.json`。按案例记录总耗时、CPU 时间、ns/操作、操作/秒、载荷 Mbps 和校验和。

```console
build-flow-perf/benchmark/benchmark_common --iterations 100000 --repeats 3
build-flow-perf/benchmark/benchmark_dual_index_map --iterations 200000 --repeats 3
build-flow-perf/benchmark/benchmark_routing --iterations 10 --repeats 3
build-flow-perf/benchmark/benchmark_node --iterations 100000 --repeats 3
build-flow-perf/benchmark/benchmark_relay --iterations 256 --repeats 3
build-flow-perf/benchmark/benchmark_udp_node --duration-seconds 3 --rate-pps 50000
```

`iterations` 在基础组件中表示基本操作次数，控制消息按体积分摊次数，限速等待每档最多 128 块，
广播轮数为基本次数的千分之一。Node 的服务列表/报告次数为基本次数的千分之一，会话重建为百分之一。
索引用例表示表项数；路由表示每种拓扑的查询次数，质量样本数为其千倍；传输表示每方向的数据块数、
UDP 往返次数、控制请求数或代理连接重建次数。JSON 中的 `operations` 是每个用例实际使用的分母。

池块在计时前预热，复用和复制要求测量期间上游申请为零；队列交接保持池块所有权并校验序号。
复制、帧和网络用例校验数据、顺序与结束位置；计数与注册表用例核对结果和状态。
耗时包含该用例内的校验成本，不能把微基准的操作速率直接当作系统吞吐。
独立组件测量不设置跨机器通用的性能通过阈值；非零退出代表校验、执行或资源条件失败。

TCP/TLS 复制并行传输两个方向，载荷吞吐记录两方向总和；TLS 握手单独计时，证书加载和 socket 建立在计时外。
UDP 复制用例一次只保留一个待响应报文，测量双向往返成本；饱和吞吐和丢包由 UDP Node 基准测量。
UDP 没有信用流控，不引入 TCP 的等待或确认机制。代理用例使用 HTTP CONNECT 与 SOCKS5 IPv4，
大块往返和短连接重建分别计时，均包含连接握手，不覆盖 HTTP 普通请求重写吞吐。

路由拓扑为固定的有向图，边跨越 1/2/4/8 等节点间隔，构建、路径搜索、快照接收、校验与候选计算分开计时。
Agent 候选计算在计时前注入已有测试夹具的测量状态，不打开原始 ICMP socket；DNS、ICMP 的实际网络耗时不计入算法耗时。
注册表借用未启动的 TLS 会话对象，单独测量索引和报告成本；真实控制连接由传输和 UDP Node 用例覆盖。

## 多节点 Flow 性能与调速记录

```console
# 100 条 TCP Flow 共用 A → B → C 的两条相邻 NodeLink。
# 终点消费速率按每流均分，使用生产 TokenBucket，分别降速、提速。
build-flow-perf/benchmark/benchmark_flow --flows 100 --nodes 3 \
  --rates-mbps 1000,100,500,1000 --phase-seconds 5 --output build-flow-perf/benchmark/results/sink-steps

# 一条流暂停消费，其余流持续传输；窗口与载荷释放必须正确。
build-flow-perf/benchmark/benchmark_flow --flows 100 --rates-mbps 1000 \
  --pause-flow-ms 3000 --phase-seconds 8 --output build-flow-perf/benchmark/results/slow-flow

# 真实 TCP 连接：每跳 RTT 100 ms，各方向带宽 1 Gbps → 100 Mbps → 500 Mbps。
# runner 创建一次性 user/network namespace，出口 HTB 调速、接收入口 IFB/netem 模拟延迟。
python3 benchmark/run_flow_network.py --flows 100 --nodes 3 --rtt-ms 100 \
  --rates-mbps 1000,100,500 --warmup-seconds 5 --phase-seconds 5 --output build-flow-perf/benchmark/results/network-steps

python3 benchmark/flow_report.py build-flow-perf/benchmark/results
# 可选绘图，需要 matplotlib。
python3 benchmark/flow_report.py build-flow-perf/benchmark/results --plot

# 统一 runner 结果中的 Flow，或单个场景目录，也可直接绘图。
python3 benchmark/flow_report.py build-flow-perf/benchmark/results/components-final/flow --plot
python3 benchmark/flow_report.py build-flow-perf/benchmark/results/components-final/flow/sink-steps --plot
```

可通过 `--flows 1/10/100`、`--nodes 2/3`、`--chunk-bytes 4096/16384/32768` 比较复用与数据块开销，
或使用 `--phase-seconds 120` 测量长期运行。网络 runner 要求 Linux、`ip`、`tc`、`ss`、允许用户命名空间及
命名空间内的网络管理权限及 IFB 支持；失败会保存错误记录，不退回宿主网卡整形或伪造 RTT。

每个场景保存 100 ms 采样 `timeline.csv`、每流阶段统计 `flows.csv`、运行参数 `metadata.json` 和结果
`summary.json`。网络场景增加实际调速时间、TCP 参数与 `qdisc` 统计。汇总生成 `phases.csv` 和 `报告.md`，
记录稳态吞吐、降速/恢复收敛、发送等待、CPU、RSS、各节点缓存与池载荷占用。
收敛判据为至少 500 ms 的滑动窗口连续三次进入目标 ±10%；未达到目标仍保留测量结果。
绘图按实际场景目录生成子图，不依赖目录名称；输出 PNG 和 SVG，报告使用 PNG 预览。

吞吐以终点完成消费为准；逐块检查完整数据与顺序，排空后核对字节总数。采样检查缓存窗口上限，
停止后检查在用载荷全部归还。内存池保留空闲块是正常行为，RSS 不要求回到启动值。
测试中的消费限速变化仅模拟下游速度，不增加生产限速器的动态配置接口；`bandwidth_mbps` 是节点容量统计参数。
`--duplex 1` 同时传输两个方向；CLI 速率为每方向聚合值，CSV 吞吐和目标记录两个方向的总和。
测量使用单进程、单执行器，不包含完整 Agent/Node 控制与接入链路；netem 结果须与真实跨机器吞吐验收区分。
网络整形在 NodeLink 建立前配置，握手即使用预期 RTT，避免内核拥塞控制保留本机低延迟历史。
