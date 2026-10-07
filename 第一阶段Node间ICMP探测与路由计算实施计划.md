# 第一阶段：Node 间 ICMP 探测与路由计算实施计划

## 1. 本阶段目标

在不改变现有 Agent、服务注册和 Relay 数据路径的前提下，为 RelayNode 增加一个独立的路由管理器：

```text
调用方提供待探测 Node 列表
    → ICMPv4 Echo 探测
    → RTT/抖动/丢包/新鲜度统计
    → 有向拓扑快照
    → 受最大层数约束的路由计算
    → 主备候选和诊断输出
```

路由计算输入是 `source_node_id` 和 `destination_node_id`。将来接入业务时，`destination_node_id` 是 Producer 固定注册的服务 Node。

## 2. 简化后的设计结论

第一阶段只引入一个有状态类，路由算法使用同一命名空间下的自由函数：

| 类 | 是否拥有状态 | 职责 |
|---|---|---|
| `routing::Mgr` | 是 | 拥有线程、I/O、timer、ICMP socket、探测目标、在途探测、指标、拓扑快照和集群汇聚状态 |
| `routing::find_routes()` | 否 | 对一个不可变图快照执行受限 Dijkstra 和备路计算 |

不新增 `NodeDirectory`、`NodeProbeManager`、`MetricStore`、`TopologyStore` 或 `IcmpPacketCodec` 类。

- ICMP 头、IPv4 头和 payload 编解码使用 `routing_mgr.cpp` 匿名 namespace 中的无状态函数；
- 每个目标的窗口样本和指标作为 `routing::Mgr` 的私有 struct；
- 远端指标、快照序号和当前快照直接由 `routing::Mgr` 拥有；
- 只有当一整块逻辑已经复杂到无法独立理解、测试或复用时，才再抽象新类。

路由算法保留独立源文件，是因为它需要与穷举结果对拍，且不应依赖 ICMP 或 Asio I/O；它没有状态，因此不再为它建立空壳类。

## 3. 范围边界

### 3.1 本阶段实现

- Linux 下基于 Asio 的 ICMPv4 Echo Request/Reply；
- `routing::Mgr` 独立线程和完整生命周期；
- 由公开函数接口在启动前一次性设置待探测 Node 目标集；
- 单 Node 进程共享一个 ICMP raw socket；
- 多目标错峰调度、超时、回包匹配和停止清理；
- RTT、EWMA RTT、抖动、滑动丢包窗口、样本数和新鲜度；
- 各 Node 向 master 上报自己的出边指标；
- master 生成不可变拓扑快照；
- 基于 `(node_id, used_layers)` 的受限 Dijkstra；
- 主路候选和备用候选计算；
- 路由决策日志、快照导出和离线重放测试。

### 3.2 本阶段不实现

- 不修改 Producer 的固定 Node 注册模型；
- 不修改 RelayAgent 和 Forwarder；
- 不创建 Node—Node 业务数据通道；
- 不下发数据转发表；
- 不将计算结果用于现有 Relay；
- 不做 Agent—Node 探测；
- 不做 ICMPv6，但公共值类型不应绑死 IPv4；
- 不做带宽主动探测；
- 不自动发现待探测目标，目标集的来源由上层决定；
- 不把 ICMP 可达等同于未来 TCP/TLS/UDP 业务通道一定可用。

## 4. `routing::Mgr` 设计

### 4.1 所有权和执行域

`routing::Mgr` 自己拥有执行域：

```text
routing::Mgr
  ├─ asio::io_context
  ├─ executor_work_guard
  ├─ std::jthread
  ├─ asio::ip::icmp::socket
  ├─ asio::steady_timer
  ├─ targets / in-flight probes / local metrics
  ├─ remote metrics / current snapshot
  └─ routing::find_routes()（无状态，查询时调用）
```

`routing::Mgr` 不使用 PImpl；上述资源、状态和私有辅助结构都由类本身直接持有。当前没有稳定 ABI 或隐藏第三方依赖的需求，额外的 `Impl` 只会增加一次间接访问和代码跳转。

运行期可变状态只在自有 `io_context` 线程上访问。探测目标在 `start()` 前完成校验和设置，启动后作为不可变输入使用；其他公开异步方法只传递值对象，不与 RelayNode 共享容器。

这种方式将 ICMP raw socket、定时任务和路由计算与现有三个 executor 隔离：

```text
control_io        现有控制会话、Registry、ClusterMgr
transfer_tcp_io   现有 TCP/TLS 数据面
transfer_udp_io   现有 UDP 数据面
routing::Mgr      自有线程，不由 main.cpp 额外管理 routing_io
```

### 4.2 公开接口

接口方向如下，具体 handler 类型在编码时以尽量小的方式定义：

```cpp
namespace routing
{
struct Target { std::string id; asio::ip::address address; };

class Mgr {
public:
    explicit Mgr(std::string local_id);
    void set_targets(std::vector<Target> targets);
    void start();
    void stop();
    std::future<std::shared_ptr<const Snapshot>> async_snapshot();
    std::future<Result> async_route(Request request);
};
}
```

不暴露 socket、timer、在途 probe 或可变指标的引用。

### 4.3 启动前目标设置语义

`set_targets()` 设置本次 `routing::Mgr` 生命周期使用的完整目标集：

- 只能在构造完成且尚未启动时成功调用一次；
- 输入拒绝空集、空 ID、本 Node ID、保留 ID、非 IPv4 地址、重复 ID 和重复地址；
- 目标数量不得超过类内固定上限；
- 设置成功后目标集不可变，`start()` 只读取该目标集并为目标错峰安排首次探测；
- 未设置目标就调用 `start()`，第二次设置目标，或启动后设置目标，都以明确的 `logic_error` 拒绝；
- 本阶段不提供运行期 `add/remove/update/replace` 接口。

生命周期固定为：

```text
Created --set_targets()--> Ready --start()--> Running --stop()--> Stopped
```

目标发生变化时，由上层停止并销毁当前 manager，再用新目标构造和启动新实例；首版不支持同一实例 restart，以避免引入状态迁移和并发更新语义。

本阶段不规定目标集来自静态配置、集群成员发现还是上层控制器。上层必须先准备完整目标列表，再依次调用 `set_targets()` 和 `start()`。

### 4.4 启动与停止

`start()` 流程：

1. 验证状态为 Ready，目标已经设置；
2. 创建自有 `std::jthread`，向自有 executor 投递 ICMP 初始化任务；
3. 将状态切换为 Running 并立即返回，不等待 raw socket 初始化结果；
4. 工作线程打开 ICMPv4 raw socket，成功后启动唯一接收链和唯一调度链；
5. socket 打开、配置或其他探测初始化失败时，关闭 socket 并将全部本地边发布为 Expired；
6. 探测不可用不反向导致 Node 启动失败，快照查询、路由查询和 `stop()` 仍然可用。

`start()` 的返回只表示 manager 的执行线程已经启动，不表示网络探测可用。探测能力通过边状态表达：初始化尚未完成时为 Warming，初始化失败后为 Expired。未设置目标、重复启动等调用契约错误仍同步抛出。

`stop()` 流程：

1. 幂等判断已停止状态；
2. post 取消 timer、关闭 socket、清空各目标的在途 probe；
3. 等待接收和调度异步链退出；
4. 释放 work guard，停止 `io_context`；
5. join 自有线程；
6. 析构函数作为最后保障调用 `stop()`，但正常生命周期必须显式停止。

`start()` 和 `stop()` 不得在 manager 自有线程内同步调用。

## 5. 固定探测参数

首版不将探测周期、窗口和权重暴露为配置。它们作为 `routing::Mgr` 的 `private static constexpr` 常量：

```cpp
static constexpr auto probe_interval = std::chrono::seconds(10);
static constexpr auto probe_timeout = std::chrono::seconds(3);
static constexpr std::size_t window_size = 12;
static constexpr std::size_t min_successes = 3;
static constexpr auto fresh_age = std::chrono::seconds(25);
static constexpr auto expire_age = std::chrono::seconds(45);
static constexpr double ewma_alpha = 0.2;
static constexpr std::size_t max_targets = 64;
static constexpr std::size_t max_layers = 3;
static constexpr double relay_penalty = 2.0;
static constexpr auto report_interval = std::chrono::seconds(10);
```

理由：

- 第一阶段只用于观测和影子计算，10 秒周期不会频繁产生 ICMP；
- 固定参数减少首版配置面、校验分支和组合测试；
- 真实数据不足时，对外暴露大量参数只会提前固化尚未证明的策略；
- 后续只有已经出现明确调优需求的参数，才逐项转为配置。

各目标的首次探测在一个周期内均匀错开。后续探测加不超过 ±10% 的随机 jitter，避免多个 Node 同步发包。同一目标最多有一个未完成 probe。

`routing.enabled` 可以保留为唯一个部署开关，它不属于探测策略参数：

```json
{
  "routing": {
    "enabled": false
  }
}
```

`routing` 对象缺失时等价于关闭。不在配置中放入目标列表和任何周期、阈值或权重。

## 6. ICMP 实现

### 6.1 现代化原则

参考文件：

- `vendor/asio-1.38.2/src/examples/cpp11/icmp/icmp_header.hpp`
- `vendor/asio-1.38.2/src/examples/cpp11/icmp/ipv4_header.hpp`
- `vendor/asio-1.38.2/src/examples/cpp11/icmp/ping.cpp`

只参考 wire format 和 Asio ICMP socket 用法，不直接复制示例类：

- 使用固定大小 `std::array<std::uint8_t, N>` 和 `std::span`；
- 使用显式大端编解码，不使用 `istream/ostream`解析网络包；
- checksum 和包解析是 `routing_mgr.cpp` 中的无状态私有辅助函数；
- 一条持续异步接收链和一条统一调度链，不为每个目标创建一套 socket/timer 对象。

### 6.2 ICMP header 和 payload

ICMP header 使用标准 8 字节格式。`identifier` 在 manager 启动时随机生成一个非零 16 位值，不直接使用 PID；`sequence` 为 16 位递增值，允许回绕。

payload 使用固定二进制格式：

```text
magic[4] | version[1] | reserved[3] | instance_id[8] | probe_id[8]
```

- `instance_id`：manager 每次启动随机生成，拒绝上次进程实例的迟到回包；
- `probe_id`：64 位递增 ID，与目标自身保存的在途 probe 一起匹配；
实际 RTT 只以在途 probe 中的 `steady_clock::time_point` 计算，不在 payload 中重复携带时间戳。

### 6.3 回包匹配

只有同时满足以下条件才记为成功样本：

1. IPv4 头长和包长合法；
2. ICMP 是 Echo Reply；
3. identifier 等于本 manager 随机值；
4. payload magic、version 和 instance ID 正确；
5. probe ID 仍等于某个目标的在途 probe ID；
6. 来源 IPv4 地址等于该目标地址；
7. 未超过固定 `probe_timeout`；
8. 该 probe ID 没有成功记录过。

重复、迟到、不匹配和畸形回包直接忽略，不更新边指标。raw socket 可能收到主机上的其他 ICMP 包，过滤必须是正常快路径。

### 6.4 调度简化

`routing::Mgr` 只使用一个调度 timer：

- 每个 target 记录 `next_probe_at`；
- 每个目标的可选 `Probe` 记录 `expires_at`；
- timer 始终等待最早的下次发送或在途 probe 超时；
- 回包和 stop 通过 cancel timer 唤醒调度器重新计算截止时间；
- 同一目标有在途 probe 时不发第二个 probe。

这样不为每个目标创建 timer，也不需要额外调度类。

### 6.5 ICMP 能力边界

ICMP Echo Reply 由对端内核回复，不是 RelayNode 应用层的鉴权响应：

- 期望 IP、identifier、sequence 和随机 cookie 能排除误包和盲目伪造；
- ICMP checksum 只检测损坏，不认证对端 Node 进程；
- 在 payload 中放 HMAC 也只是被对端内核原样回显，不能证明 RelayNode 进程存活；
- ICMP 可被防火墙限速，也可能使用与业务流量不同的 QoS；
- 本阶段的结果只是影子路由依据，后续数据通道仍需要 mTLS 和端到端验证。

ICMP 观测值是由本 Node 发起的 RTT，不是精确单向延迟。`A→B` 可以与 `B→A` 不同，但两者都包含往返路径。代码不应将它命名为 `one_way_delay`。

## 7. `routing::Mgr` 内部数据

仅对外暴露跨模块传递的值类型。以下结构作为 `routing::Mgr` 私有实现细节：

```text
Peer {
  Target target,
  EdgeState state,
  deque<bool> samples,
  optional<Probe> probe,
  next_probe_at,
  last_reply_at,
  latest_rtt_ms,
  rtt_ms,
  jitter_ms,
  recovery_samples
}
```

边状态：

```text
Warming --成功样本达到 min_successes---------------> Fresh
Fresh   --age > fresh_age----------------------------> Aging
Aging   --收到有效样本-----------------------> Fresh
Aging   --age >= expire_age--------------------------> Expired
Expired --重新累积 min_successes---------------------> Fresh
```

- Warming 和 Expired 边不进入可用图；
- Expired 后的恢复样本重新计数；
- Aging 边可用，但路由权重随 age 增加；
- 丢包率从固定大小成功/失败样本窗口计算；
- 新指标导出为不可变 `EdgeMetric` 值对象，而不暴露 `TargetState`。

master 汇聚远端指标时，`routing::Mgr` 直接拥有：

```text
unordered_map<SourceNodeId, RemoteReportState> remote_reports;
shared_ptr<const TopologySnapshot> current_snapshot;
```

不为这两个容器额外建立 store 类。

## 8. 集群汇聚边界

`routing::Mgr` 不直接依赖 `ClusterMgr`：

```text
routing::Mgr 产生本地 MetricReport
    → report_handler 被 post 到 control executor
    → RelayNode 使用 ClusterMgr 发送 routing.metrics.report

RelayNode 收到 routing.metrics.report
    → 使用 ClusterRoom 认证的 source
    → routing::Mgr::submit_remote_metrics(...)（后续汇聚接口）
```

原则：

- 上报使用独立 `routing.*` 命名空间；
- master 使用 ClusterRoom 写入的 `source`，不信任 payload 自报的源 Node ID；
- 每个报告带单调 `report_seq`，重复或乱序报告被忽略；
- 集群断开时本地继续以有界窗口探测，不积压历史报告；
- 重连后只上报最新完整状态；
- 发送队列满时丢弃本次指标报告，不阻塞现有控制面；
- 跨 executor 只传值对象副本。

探测目标列表不由 `routing::Mgr` 从 ClusterMgr 内部容器拉取。上层在启动前组装完整的 `routing::Target[]`，依次调用 `set_targets()` 和 `start()`；第一版集成测试也直接通过该接口注入固定目标。运行期集群成员变化不自动修改探测目标，如需应用新目标则重建 manager。

## 9. 拓扑快照

`routing::Mgr` 从本地和远端的最新指标构造：

```text
TopologySnapshot {
  snapshot_id,
  created_at,
  nodes[],
  usable_edges[],
  excluded_edges[]
}
```

边使用具名值类型表达，不使用 `std::pair<std::string, std::string>`：

```cpp
namespace routing
{
struct Edge { std::string from; std::string to; };
}
```

快照使用扁平 `std::vector<routing::Metric>`，便于复制、序列化和离线重放；`routing::find_routes()` 计算时构造 `source → outgoing edges` 邻接表。Node 图是稀疏图，不采用空间复杂度为 `O(V²)` 的邻接矩阵。标准容器继续负责存储，但不再用匿名 `pair/tuple` 表达领域概念。

- 快照一旦发布不再修改；
- 一次路由计算只读一个快照；
- 低频指标更新后直接重建快照，首版不额外实现 debounce 类；
- 只保留当前快照；离线分析由显式导出的 JSON 文件完成，不在内存中维护快照历史库；
- 快照序列化不在 ICMP 回包处理路径内同步写磁盘。

这里不单独抽象 `TopologyStore`，因为首版只有“最新报告集合 → 当前快照”的单一流程。

## 10. 路由算法

### 10.1 输入输出

```text
Request {
  source,
  destination,
  max_layers
}

Result {
  snapshot_id,
  optional<Path> primary,
  optional<Path> backup
}

Path {
  nodes[],
  cost
}
```

`Request.max_layers` 首版由调用方提供，但必须在 `1..routing::max_layers` 之间。

### 10.2 边权重

所有项转换为毫秒等效代价：

```text
W(e) = wrtt  × EWMA_RTT_ms
     + wj    × EWMA_Jitter_ms
     + wloss × [-ln(1-LossRate)]
     + Page
     + Pconfidence
     + Prelay
```

- Warming/Expired 边直接排除；
- 丢包率超过固定硬阈值的边直接排除；
- Aging 边按 age 增加惩罚；
- 样本少、波动大的边增加可信度惩罚；
- 每增加一个中间 Node 增加固定中继惩罚；
- 直达边存在时必须与多跳路使用同一套评分。

边权重中的权重和硬阈值同样先使用 `static constexpr`，不增加配置表面。

### 10.3 算法约束

- 优先队列元素保存累计代价和当前简单路径；
- 起点 Node 计为第 1 层；
- 每进入一个 Node，`used_layers + 1`；
- 只保留 `used_layers <= max_layers` 的状态；
- 路径必须精确结束于 `destination_node_id`；
- 路径无环；
- 同分时按 Node ID 序列字典序选择，保证可重放；
- 代价必须为有限非负值。

### 10.4 备用路径

1. 先计算主路；
2. 排除主路的中间 Node，并排除与主路完全相同的路径后重算；
3. 当前 `max_layers == 3`，不同候选最多各有一个中间 Node，因此不同中间 Node 已同时保证节点和链路不相交；
4. 不存在上述候选时，`backup` 为空，不保留当前约束下永远不可达的部分重叠退化分支；
5. 将来若提高 `max_layers`，再根据实际需求增加链路不相交和部分重叠策略。

## 11. Linux 权限

ICMP raw socket 需要 root 或 `CAP_NET_RAW`。不让 RelayNode 以 root 运行，systemd 服务增加：

```ini
CapabilityBoundingSet=CAP_NET_RAW
AmbientCapabilities=CAP_NET_RAW
NoNewPrivileges=true
```

- `routing.enabled=false`：不构造/启动 `routing::Mgr`，不打开 raw socket；
- `routing.enabled=true` 但无权限：Node 继续运行，manager 将边标记为 Expired，路由查询不会使用这些边；诊断信息应提示缺少 `CAP_NET_RAW`；
- 不建议用户将整个服务改为 root；
- 生产化时可评估将 raw socket 拆入最小权限 helper 进程，但不在第一阶段提前引入 IPC 和额外进程。

## 12. 拟议代码结构

```text
route/inc/icmp.h
route/inc/route_graph.h

route/src/icmp.cpp
route/src/route_graph.cpp
```

| 文件 | 内容 |
|---|---|
| `icmp.*` | 管理多地址 ICMP 探测协程及链路指标缓存 |
| `route_graph.*` | 构造分层带权有向图，并按调用方给定的最大节点数计算最短路径 |

当前 `route` 保持独立，不接入 `node`，不增加 manager/store/codec 空壳类。上层后续只负责把 ICMP 指标转换为 `RouteGraph::Link` 并读取计算结果。

## 13. 开发顺序

### 任务 1：值类型和纯路由算法

- 定义 `routing::Target/Edge/Metric/Snapshot/Request/Result`；
- 使用手工快照实现受限 Dijkstra；
- 实现硬约束、边权重和确定性 tie-break；
- 实现主路和备路计算；
- 用穷举搜索对拍随机小图。

### 任务 2：`routing::Mgr` 骨架和生命周期

- 实现自有 `io_context`、work guard 和 `std::jthread`；
- 实现 start/stop、重复调用，以及探测初始化失败时的降级和资源收敛；
- 实现 `set_targets()` 的校验、一次性设置和状态约束；
- 实现异步路由查询和快照查询；
- 先使用注入样本测试线程隔离，不等待 ICMP I/O。

### 任务 3：ICMP 编解码和异步探测

- 在 `routing_mgr.cpp` 中实现无状态 packet helper；
- 打开唯一 ICMPv4 raw socket；
- 实现唯一接收协程和唯一调度协程；
- 实现目标错峰、回包匹配、超时、重复/迟到拒绝；
- 实现固定窗口、EWMA、抖动和边状态转换；
- 无 `CAP_NET_RAW` 时降级为 Expired 边，并提供可操作的诊断信息。

### 任务 4：集群汇聚和快照

- 定义 `routing.metrics.report` 内部控制消息；
- RelayNode 对 `routing.*` 消息做最小分流，其他消息继续进入 `ControlRouter`；
- slave 低频上报最新完整边状态；
- master 通过 `submit_remote_metrics()` 汇聚；
- 实现当前不可变快照和 JSON 导出；
- 在 `start()` 前用 `set_targets()` 为多 Node 集成测试注入固定目标集。

### 任务 5：集成、打包和回归

- Node 配置只增加默认关闭的 `routing.enabled`；
- RelayNode 启用时持有 `routing::Mgr`，但不管理它的内部线程；
- systemd 增加 `CAP_NET_RAW` 最小权限；
- 更新 Node 示例配置、设计和故障排查文档；
- 运行现有全部 CTest；
- 对比路由功能关闭/开启时的现有 Relay 延迟和容量基准。

## 14. 测试计划

### 14.1 无特权单元测试

默认 CI 不依赖 `CAP_NET_RAW`：

- ICMP checksum 已知向量；
- IPv4/ICMP/payload 合法与畸形包解析；
- identifier、sequence、instance/probe ID 匹配；
- 未设置目标就 start、合法设置、空集和重复目标拒绝；
- 第二次设置、启动后设置和同一实例 restart 拒绝；
- 超时、重复和迟到回包处理；
- EWMA、抖动、滑动丢包窗口和边状态；
- `routing::Mgr` 重复 start/stop、探测初始化失败和停止中有在途 probe 时的资源收敛；
- 受限 Dijkstra 的直达最优、多跳最优、层数约束、无路和同分 tie-break；
- 主备路节点不相交，以及不存在独立备路时返回空；
- 固定快照导出后重放结果一致。

packet helper 保持在 `.cpp` 私有范围，不为了测试而把实现细节提升为公共抽象。

### 14.2 集成测试

- 使用可注入样本验证 3～5 Node 汇聚，不需要 raw socket；
- 模拟指标消息乱序、重复、丢失、slave 重连和 master 重启；
- 验证一条边从 Fresh 到 Aging、Expired 再恢复；
- 验证路由计算不修改 Registry、Relay 或 Agent 状态；
- 验证 manager 自有线程的启停不阻塞现有 executor。

### 14.3 特权 ICMP 烟雾测试

单独提供不默认注册为 CTest 的真实 ICMP 烟雾测试：

- 仅在检测到 `CAP_NET_RAW` 或 root 时运行；
- 先对 loopback 验证发送、回包匹配和停止；
- 再在 network namespace/veth 构成的两 Node 网络上测试；
- 使用 `tc netem` 注入延迟和丢包；
- 无特权必须明确报告 skip；manager 本身保持 Running，但边必须为 Expired，不能伪装成探测通过。

### 14.4 现有功能回归

- `routing` 字段缺失；
- `routing.enabled=false`；
- `routing.enabled=true` 但未提供目标或目标集为空，Node 启动明确失败；
- `routing.enabled=true` 但集群暂时不可用；
- 启用探测前后 TCP/TLS/UDP Relay 的功能和生命周期；
- ICMP 初始化失败不阻塞 Node 启动，且停止期间全部资源均被回收。

## 15. 验收标准

### 15.1 功能正确性

- 调用方可在启动前一次性设置完整待探测目标，启动后目标保持不变；
- 3～5 Node 的有向观测拓扑可稳定生成；
- 可计算任意已知源 Node 到目标 Node 的主备候选；
- 输出路径无环、不超过 `Lmax`、通过硬约束并结束于目标 Node；
- 直达边存在时正常参评；
- 相同快照和参数得到相同结果；
- 断链后相关边在固定 `expire_age` 内消失，恢复后经足够样本再进图。

### 15.2 简洁性

- 首版只有 `routing::Mgr` 一个主要新类，路由计算是自由函数；
- `routing::Mgr` 自己管理线程，RelayNode 只调用 start/stop 和数据接口；
- 探测目标只有一个启动前一次性设置接口；
- 探测参数是类内 `static constexpr`，配置仅保留功能开关；
- 只使用一个 raw socket、一个接收协程和一个调度 timer；
- 不存在仅包装一个容器或函数调用的 manager/store/codec 层。

### 15.3 隔离性

- 功能默认关闭；
- 关闭时不创建 manager 线程、不打开 raw socket、不发送指标报告；
- 开启时不修改现有 Relay 选路和数据面状态；
- 路由计算错误、无路和过期只影响诊断输出；
- 探测和计算不导致现有 Relay P99 延迟或成功率明显退化。

### 15.4 生命周期

- `start()` 不等待 ICMP 初始化；初始化失败后 manager 仍可查询和停止，边为 Expired；
- 线程创建或任务投递失败时不遗留线程、socket 或 work guard；
- stop 后不遗留 socket、timer、在途 probe 或未结束异步操作；
- 未配置目标、重复设置、启动后设置和 restart 都被确定性拒绝；
- manager 的 stop/join 不依赖 control/TCP/UDP executor 继续运行。

## 16. 可合并版本拆分

### 版本 1A：本地闭环

- 跨模块值类型；
- `routing::find_routes()` 及算法对拍测试；
- `routing::Mgr` 自有线程和生命周期；
- `set_targets()` 启动前一次性目标设置；
- ICMP 编解码、raw socket、调度、指标窗口和边状态；
- 注入快照的路由查询；
- loopback ICMP 可选烟雾测试。

### 版本 1B：多 Node 汇聚闭环

- `routing.metrics.report` 和 RelayNode 最小分流；
- master 远端指标汇聚和当前快照；
- 3～5 Node 主备路由计算；
- JSON 快照导出与离线重放；
- systemd `CAP_NET_RAW`、文档和完整回归；
- 影子路由分析报告。

1A 和 1B 全部完成后，才认为第一阶段交付完成。此时系统仍只计算路由，不执行路由。
