# Node 共享数据通道与按路径转发实施计划

## 范围与现状

第一阶段已完成 master 协调的 Node 间共享 TCP/UDP 数据通道和基本分帧收发。
第二阶段已完成 NodeFlow 建立、提交及 Node 间逐跳转发，并使用内部数据验证。
第三阶段已实现 Agent 最佳路径提交、首 Node 定位及 TCP/TLS/UDP 首末接入，实施与验证记录见文末。
service.lookup/located 保持服务发现原义，多跳使用 node.lookup/located 查询首 Node；
Agent 沿用现有控制连接建立方式连接首 Node 后提交最佳路径。
第四阶段已补齐首末 socket↔NodeFlow 桥接和真实业务；首端确认两端接入和绑定后，首末各自激活并通过已有 relay.ready 开始业务，第三、第四阶段整体上线。
不增加阶段专用接口、临时协议或上线开关。
本文阶段编号针对共享数据通道与路径执行，区别于历史探测/自适应路由预研的编号。

## 端到端职责重构：分步实施计划（2026-10-09）

本节为当前已完成的重构方案，优先于下文第三、第四阶段的历史结构描述。
已完成的业务功能和 NodeLink/NodeFlow 是回归基线；重构完成后整体使用，不发布中间步骤，
不添加临时协议、兼容开关、业务 prepare 或额外 master 协调。

### 完整流程与三个生命周期

```mermaid
flowchart LR
    F[Agent forward 本地监听] --> A[请求方 AgentRelay]
    A --> H[首 Node RelaySession]
    H --> D{单节点或多节点}
    D -->|单节点本地复制| P[服务方 AgentRelay]
    D -->|已有 NodeFlow| T[末 Node RelaySession]
    T --> P
    P --> S[目标服务 socket]
```

forward 拥有配置和本地监听，持续存在；AgentRelay 拥有一次业务接入和复制资源；
RelaySession 拥有一次 Node 本地业务实例、所选控制器和数据资源句柄。
TCP/TLS 每个已接受的应用连接创建一个 AgentRelay；UDP 每个 forward 保留当前业务实例，
应用来源和重试状态归 forward。中间 Node 只有第二阶段的 NodeFlow，不创建 Agent 业务实例。

建立顺序固定为：服务发现 → 最佳路径选择 → 获取/复用入口控制连接 → relay.open →
Node 资源建立 → opened/offer → 请求方接入、服务方连接目标并接入 → ready → 双向传输。
单节点与多节点在路径确定以后选择各自控制器；协议 I/O 的差异由数据资源处理。
没有可用计算路径或最佳路径只有一个 Node 时，使用服务位置走单节点接入，保留现有回退规则。

service.lookup/located 只返回服务所在 Node 的身份和位置，不因发现服务就连接服务所在 Node。
请求方用 Node 身份计算路径；单节点入口就是服务 Node，多节点入口是路径首 Node。
需要时才通过已有主控制连接发送 node.lookup/located 查询入口位置，并复用现有控制连接表。
服务方继续使用已注册服务的控制连接。活动业务保留入口控制连接引用，退出后释放；
主控制连接独立存活。有效 LRU 只取最佳路径并附当前 epoch，不复用上一次业务的票据或 Flow。
默认 max_nodes 仍为 4，上限为 8，包含首尾 Node、不包含 Agent。
服务发现沿用主控制连接；当前不设计主控断线后的服务定位，不增加位置缓存或备用发现通道。
主控制恢复后重新注册和发现。单节点入口地址不可达时清除该次发现结果并重新查询；
多节点首 Node 定位失败不删除仍有效的服务所在 Node 位置。

服务离线时，UDP 只保留 Agent 本地 forward，不建立 Node 等待实例、不分配端点或 Flow。
此时本地报文丢弃；服务上线后重新选择路径和连接入口，再申请新的业务实例。
单节点和多节点使用同一规则。TCP/TLS 当前无法建立的应用连接关闭，监听继续存在。
活动业务失效时先结束当前 AgentRelay；UDP 后续恢复同样从服务发现和路径选择开始。

### 类的职责与执行域

| 类 | 拥有和负责 | 不承担的职责 |
|---|---|---|
| RelayAgent | control_io：控制连接表、注册/发现、服务位置、选路及入口引用 | 应用 socket 复制、Node 业务协调 |
| NodeConnection | control_io：一条控制连接的识别、消息、心跳和重连 | 服务路径选择和数据接入 |
| AgentRouting / 路径 LRU | control_io：拓扑、探测、最佳路径 | 控制连接和业务资源 |
| Forwarder | transfer_io：forward 和 AgentRelay 容器、事件分派、关闭排空 | 拓扑计算、Node 事务 |
| StreamForward / DatagramForward | transfer_io：长期监听；UDP 来源、当前实例及重试 | Node 资源、服务等待实例 |
| AgentRelay | transfer_io：一次请求方/服务方接入、目标连接、ready 等待、业务复制和清理 | 全局发现、另一端协调 |
| RelayNode | control_io：注册查询、RelaySession 容器、请求/集群消息分派及停止排空 | socket 复制、master 业务状态 |
| RelaySession | control_io：直接拥有 Single 或 Multi 控制器、端点句柄；跨执行域调用数据操作 | 第二份业务状态机、共享监听、NodeLink 建路 |
| ControlRouterSingle | control_io：本 Node 两个 Agent 的接入、ready、建立超时、取消与通知 | 全局查询/容器、数据复制和 UDP 等待服务 |
| ControlRouterMulti | control_io：首末 attached/ready/finished/close、Flow 失效、预算与取消 | 全局消息查表/创建、master 业务协调 |
| StreamPipeline / DatagramMgr | transfer_tcp_io / transfer_udp_io：监听、票据校验、本地配对或端点、实际 I/O、限速计数 | Agent 控制引用、业务阶段、服务等待和控制通知 |
| NodeLinkMgr / LnkChannel | 原执行域：第二阶段既有建路、共享通道和逐跳传输 | Agent 接入与业务生命周期 |

RelaySession 通过 std::variant 直接持有 ControlRouterSingle 或 ControlRouterMulti，
不增加第三个控制器、公共控制器基类、
Impl 或回调注册框架。两个控制器具有相同层次、执行域和外部作用；差别仅在本地配对与首末协调。

### 实施步骤与检查标准

每一步单独整理、构建和执行相应回归，通过以后继续下一步；这里的拆分只是开发顺序，
不引入阶段专用运行流程。开始前保存完整工作区基线，只撤回本次重构，不覆盖此前第三、第四阶段改动。

| 步骤 | 状态 | 修改内容 | 完成标准 |
|---|---|---|---|
| 1. 名称和边界 | 已完成 | RelayPipeline 改名 RelaySession，同步源码、构建清单和当前结构文档；此步保留现有运行行为 | 无旧类型/文件引用；Node 和多跳回归通过；明确单节点统一由第 4 步落实 |
| 2. 发现与入口 | 已完成 | 服务位置与控制连接分开；按目的 Node 计算路径，然后取得入口连接；删除仅为发现目的而创建的尾 Node 连接 | 单节点只连接服务 Node；多节点请求方只取得首 Node；路径缓存、断开/epoch、Agent 集群和多跳回归通过 |
| 3. Agent 业务统一 | 已完成 | 以 AgentRelay 合并 StreamRelay / DatagramRelay / PathRelay 的接入和生命周期；保留协议 I/O 与角色差异 | opened/offer/ready/error/closed 共用实例分派；UDP 仅本地等待服务，上线后重新选路；真实双 Agent 全链路通过 |
| 4. Node 单节点统一 | 已完成 | RelaySession 同层拥有 Single/Multi；Single 协调移到 control_io；数据管理器只留下本地配对/端点 API 和 I/O | 删除数据域控制器、控制弱引用、服务等待定时器和 attach_service；缺服务直接失败；取消/建立超时/限速统计/单节点回归通过 |
| 5. Node 全局与实例分离 | 已完成 | open/reject/cancel、peer 查找/创建移到 RelayNode；控制器只操作自身实例；统一实例容器和停止排空 | Single/Multi 不再静态操作 Node 全局表；无重复阶段/请求/流身份；正常 FIN 与异常清理及分叉隔离通过 |
| 6. 整理与验证 | 已完成 | 删除替代代码和无用成员，核对所有权与跨域参数，更新配置说明、架构和本实施文档 | Debug/Release 全目标及完整 CTest；说明跳过项和实测限制；git diff --check 与文档链接检查通过 |

第 3、4 步共同落实 UDP 离线规则：Agent 不再提交无服务请求，Node 也不保留旧的等待服务流程。
第 4 步删除 udp.service_wait_timeout_ms，使用 udp.setup_timeout_ms 限制实际接入，
默认 10 秒，与 TCP/TLS 一致；测试、配置样例和部署文档同步更新，不保留旧字段兼容分支。
多节点建立继续使用请求剩余预算。建立超时只覆盖建立阶段，业务运行期间不加租约或到期扫描。

数据管理器对单节点提供本地配对操作，对多节点提供本地端点操作；均明确执行
install/wait/bind/activate/run/close。RelaySession 只在跨域边界调用这些操作，
业务阶段、超时和控制通知只有控制器一份；数据侧仅保留票据、接入事件和实际 I/O 状态。
Flow 失效监视必须由 Multi 实例拥有，并随业务退出取消、排空，不能成为 RelayNode 的另一套业务生命周期。

### 必须保留的正确性条件

- 本地监听与单次业务分开；业务失败或服务离线不销毁 forward。
- 接入票据、角色和 UDP 来源检查继续保留；单节点 UDP 原载荷范围、多跳 0..4096 字节继续保留。
- 首末控制只通过既有集群通道交流；master 继续只负责第二阶段 Flow 建路和消息转交。
- Multi 的 attached/ready 保证可开始传输，finished 保证最后 DATA/FIN 排空，不因简化而提前关闭 Flow。
- 正常结束不提前取消 Agent 数据读取；异常保留原始原因。跨执行域任务取消后必须排空，再释放资源。
- UDP 共享 socket 保留唯一发送链及有界队列；服务计数只在末端或单节点统计一次。
- 节点停止先取消并排空业务实例，再停止 NodeLinkMgr；共享 NodeLink 与其他 Flow 不受单实例关闭影响。

### 执行记录

- 第 1 步已完成：源码类型与文件统一为 RelaySession / relay_session.*；实例引用和容器同步为 relay_ / multi_sessions_。
  源码、CMake、README 和设计文档引用已同步；旧类型/文件名仅保留在本节名称迁移说明中。
  与 build/architecture-before 的 Node 基线逐文件比对，确认 7 个源码/构建文件只有名称变化，没有业务行为改动。
- 第 1 步验证：Release 全目标构建通过，无 C++ 编译警告；relay_integration、udp_relay_integration、
  udp_session_routing、relay_paths 四项回归全部通过，耗时 25.45 秒。
  git diff --check 和相关文档 41 个本地链接检查通过。
  日志：build/architecture-step1-build.log、build/architecture-step1-tests.log。
  本步未运行 Debug、全量 CTest、多机或吞吐测试；完整验证安排在第 6 步。
- 第 2、3 步已合并完成：服务发现只保存目的 Node 身份及地址，选路后取得实际入口。
  多节点请求方不因发现就连接尾 Node，入口连接在多个业务间复用，最后一个业务退出后释放。
  合并实施使连接引用直接归 AgentRelay，不增加旧三种实例的临时连接管理逻辑。
- AgentRelay 统一请求方与服务方的 opened/offer/ready/error/closed 分派、接入预算、数据复制和清理。
  Forwarder 只拥有监听、来源及重试状态、业务容器；删除 StreamRelay/DatagramRelay/PathRelay 和 forwarder_paths.cpp。
  单节点同一 Agent 的两种角色均接收 ready；多节点 TCP/TLS 仍等待 FIN 排空再释放入口。
  UDP 服务离线时 Agent 只保留本地 forward，发现服务后每个新实例重新选路。
- 服务发现仍经主控；不设计主控断线定位，不增加位置缓存或备用通道。
  接入地址不可达时单节点重新发现，多节点首 Node 失败不清除有效目的位置；迟到发现回复按 request_id 丢弃。
- 第 2、3 步验证：Release 全目标构建通过，无 C++ 编译警告；完整 CTest 26 项通过、probe_integration 跳过，耗时 161.89 秒。
  真实多跳 TCP/TLS/UDP、TCP/TLS 半关闭、同一 Agent 单节点双角色、UDP 最大载荷及来源检查、
  首 Node 连接复用与最后引用释放、有效 LRU/epoch、主控在线的从节点恢复、服务迁移和迟到定位回复均通过。
  git diff --check 及相关文档 41 个本地链接检查通过；Debug 和跨机器验证仍安排在第 6 步。
  日志：build/architecture-step2-agent-build.log、build/architecture-step2-discovery-tests.log、build/architecture-step2-agent-tests.log。
- 第 4、5 步已合并完成：Node 所有业务由 RelaySession 持有 Single/Multi variant，两个控制器均在 control_io。
  RelayNode 在 relay_node_relays.cpp 创建、查找和分派实例，统一 relay_sessions_ 容器、取消和停止排空；
  删除控制器静态全局入口、独立多跳任务计数和 Node 全局 Flow watcher。
  Multi 的 Flow 监视由本实例的结构化子任务拥有，任一分支结束后取消并排空另一分支，不延长实例生命周期。
- 数据 manager 只管理本地配对与多节点端点；业务通过 install/wait/bind/activate/run/close 操作数据资源。
  数据域不再保存 ControlRouterSingle、控制会话弱引用、服务名/请求身份、业务建立 timer 或业务控制通知。
  保留原票据与来源校验、单节点配对复制、多节点 FIN 桥接、协议容量及限速统计。
- 删除 Node UDP WaitingForProducer、attach_service 和服务等待 timer；无服务的 open 立即失败，不分配资源。
  udp.service_wait_timeout_ms 已删除，配置、样例和测试使用 udp.setup_timeout_ms，默认 10 秒；无旧字段兼容分支。
  单节点建立失败通知请求方 error 和已获 offer 的服务方 closed，避免留下额外 ready 等待；
  活动单节点 TCP/TLS 仍通过数据 socket 结束，多节点正常结束仍等待 FIN 排空。
- 第 4、5 步验证：Release 全目标构建通过，无 C++ 编译警告；relay_integration、udp_relay_integration、
  udp_session_routing、agent_reconnect、agent_cluster、proxy_relay_integration、relay_paths 七项全部通过，耗时 39.32 秒。
  新规则验证包括离线不占 Node 容量、注册不复活旧请求、半接入超时、迟到票据丢弃、ready 后无运行到期，
  以及首末控制断开保留原始原因和分叉 Flow 隔离。日志：build/architecture-step4-build.log、build/architecture-step4-tests.log。
- 第 6 步已完成：Debug/Release 全目标构建通过，无 C++ 编译警告；完整 CTest 均为 26 项通过、
  probe_integration 因缺少 Windows 管理员权限或 Linux CAP_NET_RAW 跳过。
  Debug 耗时 162.77 秒，Release 耗时 158.30 秒。
  日志：build/architecture-final-debug-build.log、build/architecture-final-debug-tests.log、
  build/architecture-step4-build.log、build/architecture-final-release-tests.log。
- 所有权和冗余审查完成：控制器直接拥有业务状态，RelaySession 只保存所选控制器和数据句柄；
  Node 容器拥有并排空实例，Multi 子任务随实例取消、排空后才释放端点和 Flow。
  对比 build/architecture-step4-before，Node 源码由 7350 行减少至 6840 行；
  本轮未修改 Agent、协议及第二阶段 NodeLinkMgr / LnkChannel / ClusterMgr / Topology 的实现。
  README、设计文档、配置升级说明和本计划已同步；git diff --check、54 个本地文档链接及配置样例检查通过。
  本次未执行跨机器或吞吐测试；下文第三、第四阶段的测试结果是本次重构前的基线。

### 完整流程简化复查（2026-10-09）

沿 forward → AgentRelay → RelaySession → Single/Multi → 数据管理器复查：

- 路径计算只访问 control_io 内的路由和有效 LRU，改为同步函数；定位和建连等待继续由 select_relay 协程管理。
- RelaySession 的 variant 与数据 API 收为内部实现。控制器明确传入接入角色、accessor、epoch/flow_id，
  不再由 RelaySession 读取控制器私有成员。Multi 只保存使用中的 epoch/flow_id，
  不保留建路结果中未使用的 stage/reason，也不跨域复制整份 FlowResult。
- 数据端点用实际 stream / UDP source 表达已接入，删除重复 connected 标记；
  UDP 配对只在绑定了统计资源后激活，逐包转发不重复检查该已成立条件。
- 服务离线只向每个受影响的 AgentRelay 分派一次取消，forward 继续管理监听及重试。
  单节点同一 Agent 的双方通过一条 ready 通知统一分派，删除重复通知。
- 单节点在 attach 完成后、绑定前确认控制参与方仍有效；正常复制返回不覆盖已记录的取消原因。
  票据、来源、建立期限、跨域排空及 Multi 的 FIN/finished 规则保留。

Debug/Release 全目标构建通过，无 C++ 编译警告；完整 CTest 均为 26 项通过、probe_integration 因权限跳过，
分别耗时 163.66 / 157.72 秒。新增断言确认活动单节点 UDP 的两端关闭通知保留取消原因；
真实单/多节点 TCP/TLS/UDP、同一 Agent 双角色、FIN 排空、限速统计、共享隔离、恢复与路径缓存均通过。
日志：build/architecture-review-debug-build.log、build/architecture-review-debug-tests.log、
build/architecture-review-release-build.log、build/architecture-review-release-tests.log。
git diff --check 与 54 个本地文档链接检查通过。本轮未修改协议和第二阶段 NodeLink/NodeFlow 实现；
未新增缓存、控制器基类、回调层、握手或 master 业务流程，未执行跨机器或吞吐基准。

## 命名约定

| 名称 | 含义 |
|---|---|
| NodeLink | 相邻 Node 对之间的物理 TCP/UDP 通道 |
| NodeFlow | 跨完整路径的双向逻辑流；保留原始领域概念 |
| NodeLinkMgr / nodelink_mgr.* | control_io 中统一管理 NodeLink 建连和 NodeFlow 路径事务的类 / 文件 |
| FlowFrame | NodeFlow 的 DATA/FIN/RESET 载荷帧 |
| LnkChannel / lnk_channel.* | 数据域内直接拥有 NodeLink 和 NodeFlow 状态、监听、读写与逐跳分派的类 / 文件 |
| LnkFrameHeader / LnkFrType | 数据 socket 上的二进制头和类型，区别于 CtrlMessage/WireMessage 控制帧 |
| ControlSession | 原有 Agent 控制连接会话，session_id 保持原义，与 flow_id 无关 |
| RelaySession | Node 本地的一次业务中继实例；区别于长期共享的 StreamPipeline / DatagramMgr |

外部转发接口统一使用 async_open/send/receive/close_flow；控制命令使用平级 flow.*，身份字段为 flow_id。
业务数据只通过 NodeFlow 收发接口；第一阶段的 LinkData/Diagnostic 测试通道已删除。
Node 配置与成员标识（NodeConfig、RelayNode、node_id）保留 Node 前缀。NodeLink 与 NodeFlow 都是核心概念，保留 Node 前缀；辅助帧、结果类型和通道实现继续使用简写。
Topology::Link / RouteGraph::Link 表示质量报告中的链路或路由图边，与 NodeLink 数据通道区别命名。

## 第一阶段：共享物理通道（已完成）

### 配置

`cluster` 必须包含以下字段，master/slave 都填写相同的数据端口：

```json
"cluster": {
  "role": "master",
  "node_id": "master-1",
  "address": "0.0.0.0",
  "control_port": 18447,
  "tcp_port": 18448,
  "udp_port": 18448
}
```

- `control_port` 用于原有 mTLS 集群控制连接；slave 的 address/control_port 指向 master。
- `tcp_port/udp_port` 是独立明文 Node 数据监听，不复用 Agent 数据监听端口。
- TCP bind 沿用 `tcp.address`，UDP bind 沿用 `udp.address`。
- 对端地址只读取现有 `topology.members`，不新增地址注册接口。
- 不接受旧 `cluster.port`、可选 `node_links` 或缺失/越界端口。
- 同一 TCP 监听命名空间不得冲突，同一 UDP 监听命名空间不得冲突；TCP/UDP 可以使用相同数字。
- 接收 prepare 时验证两个数据端口与本节点一致，不一致返回 prepare 错误。
- 部署脚本通过 `relayweave-node --check-cluster-config FILE` 使用同一解析器检查端口；该检查不要求目标证书已经安装。

### 执行域与所有权

RelayNode 显式接收第四个单线程 `cluster_data_io`。
`NodeLinkMgr` 协调状态属于 `control_io`，由 RelayNode 直接拥有，直接持有 LnkChannel；
`protocol/inc/lnk_channel.h`、`protocol/src/lnk_channel.cpp` 和 `protocol/src/lnk_channel_flows.cpp`
中的 LnkChannel 直接持有 TCP/UDP socket、解析/发送队列和逻辑流分派状态，全部属于数据执行域。
LnkChannel 只依赖 protocol 组件、Asio 和标准库，不依赖 node 的拓扑或协调器；
NodeLinkMgr 留在 node，负责物理链路协调和完整路径事务，再向 LnkChannel 下发执行参数。
帧类型与编解码位于 protocol/message。物理读写和逻辑流分派共用一个监控计时器，没有回调注册层。
跨域使用参数副本、post 和 co_spawn 完成事件，不共享可变 NodeLink 状态。
启动时同步绑定监听，任何监听失败直接回滚；随后 activate 将任务启动投递到数据执行域，
任务计数的增加与减少都在该域完成。UDP NodeLink 不创建 TCP socket 或 TCP 发送队列。
activate 启动 accept、UDP 接收、UDP 发送、monitor 四条独立任务链，复用统一 spawn 计数与退出通知；
TCP 在握手后按连接启动发送链并继续读取，不为每个数据包启动协程。

公共 `frame_io.h` 提供精确首帧读取和完整 datagram 长度校验；endpoint 直接使用 operator==/!= 比较。
Agent–Node 的 Pipeline/DatagramMgr 接入也调用这些函数，保留原有配对和限速行为。
Node 接入握手复用 CtrlMessage/WireMessage，普通数据使用 LnkFrameHeader，UDP 复用 DatagramHeader，不使用 TLS、TokenBucket、服务流量统计或双 socket 复制函数。

### LnkChannel 内部概念

`lnk_channel.h` 在 `lnk` 命名空间中声明独立的 NodeLink、NodeFlow、Frame 与 Datagram，
不再将领域对象嵌套在 LnkChannel 类中，也不新增文件。
LnkChannel 是单线程数据域的容器，直接管理 `links_` 和 `flows_` 两张表及共享 I/O、内存池、通知与任务生命周期。
NodeFlow 与 NodeLink 通过身份关联：一个 NodeFlow 在本节点最多引用 previous/next 两条邻接 Link，
一条 NodeLink 可以承载多个 Flow。完整路径的选择和事务协调属于 NodeLinkMgr。

| 概念 | 职责与状态 |
|---|---|
| NodeFlow | 一条逻辑流在本节点的状态：epoch、transport、previous/next、Prepared/Active/Closed、双向 FIN、准备期限、接收队列及占用字节 |
| NodeLink | 与一个邻居之间的物理通道：身份、接入握手、Ready/Closed、建立期限、PING/PONG；TCP 持有 socket 和发送队列，UDP 使用 endpoint 与通道共享 socket |
| Frame | 结构化 LnkFrameHeader 与独占载荷；分派、TCP 发送与终点接收使用同一种表示 |
| Datagram | UDP 发送项，由 Frame 和目标 link_id 组成，进入共享 udp_writes_ 队列，不持有 NodeLink |
| PooledBuffer | 字节块所有权与载荷视图；移动、切片只转移所有权或调整视图，存储来自通道拥有的 PMR 池 |

`FrameQueue` 是保存 Frame 的 Asio channel 类型别名，TCP 发送和终点接收复用此队列类型。
公开接口使用独立拥有载荷的 FlowFrame，在进入和离开数据域时转换为内部 Frame。
中继路径为 `Frame → TCP 写出` 或 `Frame → Datagram → UDP 写出`，载荷仅移动所有权；写出前在协程帧内编码固定帧头。
UDP 出队时按 link_id 与帧头 epoch 验证目标仍存在；目标 endpoint 复制到发送协程局部变量，
不跨 co_await 保存 NodeLink 引用或表迭代器。发送错误时重新查表并校验身份后才关闭 Link，
正常 UDP 排队和发送无需增减 NodeLink 的共享引用计数。
业务帧由 header.kind 判定，不另存 business 标记。终点 RESET 直接关闭 Flow 并以原因唤醒接收者，
无需进入随即被取消的接收队列，也不受接收队列容量影响；中间节点仍按原方向转发 RESET 再关闭本地 Flow。

共享支撑状态由 LnkChannel 持有：`events_` 向控制域报告结果；`monitor_timer_` 调度 Link 心跳、
Flow 准备和 retired_ 去重记录的到期；`buffered_bytes_` 汇总终点接收缓存，`udp_pending_data_`
统计共享 UDP 发送队列中的业务项；`tasks_` 与 `tasks_done_` 在停止时等待所有数据任务退出。
`acceptor_`、`accepting_` 和 `udp_socket_` 管理接入及共享 UDP I/O。
PMR 池声明在持有缓冲的队列之前，使缓冲先于池销毁。

NodeLink/NodeFlow 的 shared_ptr 用于表项删除与异步任务收尾之间的保活，不代表对象可以活得比通道更久。
解析、连接、TCP 读写协程按值持有 NodeLink；close 的局部副本保留对象直到删除表项及通知完成。
receive_flow 在协程帧中同时保留 NodeFlow 和 LnkChannel，关闭后恢复读取状态和原因，再释放对象及池化载荷。
同步辅助函数的 const shared_ptr& 不增加引用计数；fail_flow 最后调用 close_flow，之后不再访问可能失效的参数引用。
retired_ 仅保存关闭 ID 和回收期限，不保留 NodeFlow。单线程避免并行访问，不能替代跨挂起点的对象保活。

容量、帧开销、池参数和超时常量只放在两个实现文件中，头文件保留类型与运行状态。
monitor 提前推进迭代器后直接清理到期 Link，无需临时收集过期项。

### 校验边界

| 入口或阶段 | 保留的校验 |
|---|---|
| NodeLinkMgr 控制入口 | 参数类型与非零身份、授权、路径形状及本节点成员资格、重复 prepare 的内容一致性 |
| LnkChannel prepare | 数据域容量、关闭身份去重、邻接 Link 的 Ready/epoch/peer/transport；这些状态可能在跨域投递后变化 |
| 本地 send_flow | Flow 存在且 Active、epoch、端点注入方向；FlowFrame 格式、双向 FIN 状态与 UDP 只允许 DATA，在载荷分配前确认 |
| TCP/UDP 网络入口 | 二进制帧格式与长度、Link epoch；UDP 来源 endpoint 与接入凭据、TCP 接入身份 |
| incoming_flow | Flow 存在且 Active、预期入边与方向、FIN 状态和 UDP 帧类型 |
| 内部 deliver/enqueue | 发送队列和终点接收缓存容量；内部帧长度、Link 存活与 Ready 只保留 Debug 断言 |
| 异步恢复与 UDP 出队 | TCP/解析任务恢复后的 Closed 状态、接收等待后的 Flow 状态、UDP 目标 ID/epoch；防止等待期间关闭或换代 |

FlowFrame::validate 返回 `bool noexcept`，格式错误直接得到 Invalid；准备失败与 UDP 接入容量失败直接报告状态，
不再主动抛异常后在同一函数中接住。真实 I/O 和网络解码失败仍通过相应入口处理。
控制参数不在数据域重复验证。停止会在首次等待前清空 Flow/Link 表，查表入口无需重复检查 stopping_。
Flow 安装时确认邻接 Link 与其 epoch 一致，运行期间 Link 身份、epoch 和 transport 不变；
关闭 Link 同步删除关联 Flow。因此无等待的内部分派无需再次检查出边 Ready、重复比较 Flow epoch，
或在 UDP 出队校验 ID/epoch 后重复判定 transport。异步等待后的生命周期校验保留。

### 身份与协调

每次尝试的身份是 `master epoch + fresh NodeLink ID`，匹配凭据通过已认证集群控制通道下发。
准备命令同时带有排序后的 left/right、transport、对端成员地址和统一数据端口。
业务控制命令采用平级 `link.*`；`cluster.*` 仍保留给原有集群握手。

| 命令 | 方向 | 作用 |
|---|---|---|
| link.prepare / prepared | master ↔ 两端 | 安装本次身份、凭据、对端和准备期限；UDP 在 prepared 前完成一次解析和 endpoint 固定 |
| link.connect | master → 两端 | 收齐两个 prepared 后，各端执行固定角色 |
| link.ready | 两端 → master | 本地双向接入完成；master 收齐两个 ready 后返回成功 |
| link.error | 端点 → master | 携带失败阶段与原始原因，使外层 ensure 返回错误 |
| link.close / closed | master ↔ 两端 | 清理本次身份；关闭整条物理通道 |
| link.status | master ↔ 端点 | 查询 Ready 通道的两端状态；response 区分请求/回复，request_id 关联本轮查询 |
| link.attach / attached | 数据 socket 上 | 按 epoch、NodeLink ID、Node ID、匹配凭据完成接入 |

内部 `RelayNode::async_ensure_link(left, right, transport)` 在 master 协调执行域运行。
同一无向 Node 对、同一 transport 只保存一个请求：Ready 直接返回现有 ID；建立中的请求合并等待。
TCP/UDP 分别建立和复用。master 自身也可以作为端点。

`RelayNode::async_link_status(id)` 由 master 查询现有 Ready 通道的两端状态，
返回查询结果与按 Node ID 索引的 ready 标志，不把状态查询当作重建触发器。
并发查询合并，每轮使用独立 request_id；回复同时验证当前 epoch、NodeLink ID、Node 对、
transport 和认证 source，只接收本轮两端的回复。查询最多等待 5 秒，不重发；
关闭、成员失效或停止会取消查询并返回原因。回复不携带匹配凭据。

### NodeLink 状态

NodeLink 使用 `Preparing / Resolving / Prepared / WaitingForPeer / Connecting / Attaching / Ready / Closed`
状态枚举；只保留 `attached`、`acknowledged` 两个独立握手标志，以支持 UDP 握手乱序。
connect 在启动协程前离开 Prepared，重复请求不会启动新任务。控制通知的 stage 从状态映射，
心跳超时显式报告 keepalive，不在对象内保存阶段字符串。

| 路径 | 状态转换 |
|---|---|
| TCP 发起端 | Preparing → Prepared → Resolving → Connecting → Attaching → Ready |
| TCP 接收端 | Preparing → Prepared → WaitingForPeer → Attaching → Ready；对端接入早到时可直接从 Prepared 进入 Attaching |
| UDP | Preparing → Resolving → Prepared → Attaching → Ready |
| 关闭 | 任意存活状态 → Closed；异步任务恢复后不能覆盖 Closed |

UDP 在 Prepared 期间可以确认对端 Attach 并记录 attached，不跳过本地 connect。
在 Attaching 期间收齐两个握手标志才进入 Ready，收到确认和收到对端 Attach 的顺序不受限制。

### TCP

1. 对 Node ID 排序，left 固定为发起方，right 为接收方。
2. 双方 prepared 后发送 connect；发起方只解析一次，并只选择第一个结果执行一次目标 socket connect。
3. 发起方发送 attach，接收方验证身份和凭据后发送 attached。
4. 每条连接只保留一个读链、一个写链。接入完成后的数据通过独立的有界写队列发送。
5. 失败不换方向、不遍历解析结果、不自动重连。

### UDP

1. 使用固定本地监听 socket；每个 NodeLink 的对端 IP 和端口在 prepared 前固定。
2. 收齐两个 prepared 后，各端仅发送一次 attach；收到有效对端 attach 时发送 attached。
3. 本端必须收到有效对端 attach，且本端 attach 得到 attached 响应，才上报 ready。
4. 每个 UDP 包以八字节 DatagramHeader(NodeLink ID) 开头，随后是 LnkFrameHeader 和帧体；只有接入帧体包含 WireMessage。
5. 错误 endpoint、凭据、epoch 或旧 NodeLink ID 被丢弃，不修改状态。丢包导致超时，返回外层。

### 基本收发与边界

共享通道使用固定二进制帧承载 PING/PONG 和 NodeFlow 的 DATA/FIN/RESET。
DATA 为最多 4096 字节的原始二进制 payload，业务身份为 epoch + flow_id；测试序号只放在测试 payload 中。
TCP 的 NodeLink/对端由已确认 socket 绑定，UDP 额外校验 NodeLink ID 和固定 endpoint。
业务收发统一使用 async_send_flow/async_receive_flow，不再提供绕过 NodeFlow 的诊断收发接口。
UDP 不在本层提供可靠投递、排序或业务重传。

接入 CBOR 沿用 WireMessage 上限；UDP 接入帧体也遵守 4096 字节二进制帧体上限。
TCP 每连接发送队列 100 帧，UDP 共享发送队列 100 包；各 NodeFlow 使用独立的有界接收队列。
队列边界限制内存，不做速率等待。没有服务限速或中间节点服务统计。

### 期限、失效与停止

- 单次 master 建立总期限 10 秒；端点准备期限同为 10 秒，作为远端清理兜底。
- 错误返回阶段与原因，取消 socket/resolver、释放临时状态，向两端发 close。
- 再次由外层调用 ensure 才创建新 ID 和凭据，不延续失败尝试。
- Ready 后每 5 秒 ping，20 秒没有匹配 pong 则报失效；不触发重建。
- 成员离线或 master epoch 变化清理相应通道与建立请求。
- stop 先拒绝新申请并完成等待者，再关闭数据监听、连接、队列、resolver、timer，等待全部数据任务退出。
- 数据任务退出后才停止集群控制连接，最后主程序退出并 join 数据线程。

## 第二阶段：独立的 NodeFlow 与逐跳转发（已实现）

### 目标与范围

保留第一阶段的共享通道：每对 Node 复用一条双向 TCP，UDP 使用固定 socket。
master 显式提交路径，一条逻辑流对应一条固定路径；中间 Node 按逻辑流和方向分派数据。
首末 Node 使用内部测试接口注入/接收，验证 A→B→C、A→B→D 共用 A–B 通道。

本阶段实现 Node 数据面的最小闭环，独立于 Agent 服务注册、业务 socket、限速与统计。
Agent 路径提交和真实 socket 桥接在第三、第四阶段接入。
NodeLinkMgr、LnkChannel、flow.* 命令和内部逻辑流接口已接入 RelayNode。
不在本阶段加入每流信用、公平调度、连接池、存量换路或完整的业务背压。

### 模块边界与执行域

| 模块 | 执行域 | 直接职责 |
|---|---|---|
| NodeLinkMgr | control_io | 成员/epoch 校验、相邻 Node 建连与复用、全路径准备/提交/关闭、回复关联及控制失效清理 |
| LnkChannel（protocol） | cluster_data_io | socket、首帧身份、物理读写链、队列、保活及本地逻辑流分派 |
| 首末端桥接（后续） | 对应业务/数据执行域 | 将 Agent 业务 socket 绑定到逻辑流，转换数据、半关闭、取消和可写等待 |

RelayNode 与 Pipeline/DatagramMgr 同层直接拥有 NodeLinkMgr；NodeLinkMgr 在内部协调相邻建连和完整路径事务。
LnkChannel 收帧后直接查本地逻辑流表并选择出边，
不把逻辑流路径表、业务限速或 Agent 对象放进 NodeLink。

NodeLinkMgr 直接持有 LnkChannel；跨域传递参数副本。
LnkChannel 的 receive_event 提供有界控制通知，由 NodeLinkMgr 的一个 control_io 协程接收并处理；通知循环由管理器启动、停止并等待。
通知上限 4096；超限关闭通知流并带原始原因报错，控制域终止该数据模块，不自动重建。
每一帧的查表、暂存和转发留在 cluster_data_io，不往返 control_io。
首末端通过逻辑流接口传递字节与事件，不访问内部 NodeLink/socket，也不持有中间 Node 的业务状态。
既有单节点业务仍使用当前 Pipeline/DatagramMgr；多跳桥接调用独立逻辑流模块。

此处分模块对应不同状态和执行域，不建立通用基类、插件工厂、PImpl 或多层包装。
不预先创建空的信用控制器、调度器或连接池类。

### 步骤 1：统一逻辑流身份与接口

一条 NodeFlow 使用唯一 flow_id，身份为 master epoch + flow_id；一条 NodeFlow 对应一条双向逻辑流和固定路径。
不额外分配 Route ID 或 route_version。
路径建立时固定，重新建路使用新的 flow_id。不同逻辑流即使路径相同也各有独立表项，
相邻 NodeLink 按 Node 对和 transport 复用。

RelayNode 提供以下内部接口：

| 能力 | 输入/输出与语义 |
|---|---|
| async_open_flow(path, transport) | 运行中的 master 或普通首 Node 输入有序 Node 路径和 TCP/UDP；使用当前 epoch，返回身份及建立阶段/原因。普通首 Node 通过通用 flow.open/opened 请求复用 master 原有事务，不携带 Agent 业务信息 |
| async_send_flow(frame) | 输入身份、forward/reverse、DATA/FIN/RESET 和 payload；不能由调用者指定任意出边 |
| async_receive_flow(epoch, id) | 按逻辑流接收 DATA/FIN；RESET 或失效以异常唤醒等待者，避免多个逻辑流竞争同一个接收队列 |
| async_close_flow(epoch, id) | 幂等关闭并保留共享 NodeLink。master 等待全路径 closed，最多 10 秒；普通首 Node 发送 flow.close.request，由 master 执行相同关闭事务，调用方等待自己的本地 Flow 关闭，最多 10 秒。失联/停止唤醒等待者 |

发送接口直接返回 FlowSendStatus，区分 queued、capacity_exceeded、closed、invalid。
接口接管输入帧，被拒绝的帧自动释放。queued 只表示完整帧已进入本地队列，不能要求调用方重发已入队数据；
capacity_exceeded 表示帧未入队。
capacity_exceeded 会关闭受影响逻辑流并报告原因，不表示可以等待后重试；未来可在相同发送边界加入异步可写等待。

### 步骤 2：最小逻辑流帧与统一入口

接入握手复用 CtrlMessage/WireMessage；逻辑流数据使用 LnkFrameHeader + 原始帧体。
TCP 通过已确认 socket 绑定 NodeLink 和对端；UDP 通过 DatagramHeader(NodeLink ID) 和固定 endpoint 校验入边，每帧校验 epoch。
逻辑流层只要求 flow_id、direction、DATA/FIN/RESET 类型以及对应 payload/原因。
DATA 为最多 4096 字节的原始二进制 payload；测试序号放在测试 payload 中，不成为必需业务协议字段。
Node 逻辑流标识独立于 Agent 的现有 UDP session 标识；UDP 外层 DatagramHeader 仍标识相邻 NodeLink，
不能用它代替跨整条路径的 flow_id，也不能直接透传 Agent datagram 头作为 Node 头。
FIN 表示一个逻辑方向结束，RESET 表示该逻辑流失败，均不关闭承载其他逻辑流的物理 TCP。
UDP DATA 保留 datagram 边界，不提供可靠投递、排序或重传；UDP Flow 不接受 FIN/RESET，
关闭通过逻辑流控制流程完成。

forward 沿提交路径，reverse 沿原路径返回，与 TCP 发起方 Node ID 排序无关。
当前静态路径先验证无重复 Node/无环，每段严格验证预期入边和邻居，不增加 hop_limit 或换路版本状态。
转发保持逻辑流身份、方向、帧类型和内容；UDP 将外层 NodeLink ID 替换为出边身份。

物理解析完成后只有一个逻辑流帧分派入口，直接调用同数据域的 LnkChannel。
DATA/FIN/RESET 直接以内部 Frame 分派；PING/PONG 由通道处理，FlowFrame 仅用于公开收发边界。
每个逻辑流只消费自己的 receive_flow 队列，不提供所有流竞争的诊断接收队列。

### 步骤 3：显式路径安装与生命周期

master 校验当前 epoch、成员在线、地址快照、路径包含 2..8 个 Node 且无环。
每条路径最多 8 个 Node，prepare/commit/close 确认各使用一个 8 位掩码；此限制不约束集群总节点数。
第二阶段不要求终点注册服务。所有相邻边使用现有 async_ensure_link，可以并行准备；
任意边失败则返回外层，不自动换路径、重试或重连。

| 控制命令 | 完成条件 |
|---|---|
| flow.prepare / prepared | 数据域确认相邻 NodeLink Ready 并安装临时逻辑流表项后回复 |
| flow.commit / committed | 数据域将表项切为可转发后回复 |
| flow.error | 回报逻辑流身份、失败阶段和原因 |
| flow.close / closed | 清理该逻辑流的数据状态与等待者后回复 |

收齐全部 prepared 才 commit，收齐全部 committed 才向调用者返回 Ready 并允许首末端注入。
控制消息验证认证 source、逻辑流身份、路径参与者和本轮 request_id，重复确认不能推进两次。
重复命令参数相同则幂等，冲突则拒绝；每次外层重建分配新的逻辑流身份。

相邻 NodeLink 准备沿用单次 10 秒期限；随后逻辑流 prepare/commit 共用一次 10 秒总期限。
各端使用 ttl_ms 表示下发的准备剩余时长，以本地单调时钟检查未提交表项的准备超时；commit 后不再检查运行期限。
部分失败向全部涉及 Node 发 close，包括已提交节点；不取消其他请求正在使用的共享 ensure，
也不为回滚单个逻辑流关闭 Ready NodeLink。

提交后的 Flow 由控制连接和路径有效性管理，不需要周期续期。
与 master 的控制连接断开或心跳超时，各节点清理全部本地 Flow 与端点授权；重连不能复用旧 Flow。
epoch/路径成员失效时清理相关 Flow；NodeLink 断链时清理使用该 Link 的 Flow；业务结束使用显式 close。
NodeLink 与集群 mTLS 控制连接分别使用现有心跳，健康控制连接下遗失的单个 Flow 不再有独立过期兜底。
集群控制发送队列溢出沿用原处理：上报本地 ClusterError，清理本地协调状态，不主动断开全部集群控制连接。
该条件下 close 可能无法送达，远端已提交 Flow 不保证立即回收；其后仍依赖显式关闭、控制断开或路径失效清理。
关闭/过期身份保留 10 秒的有界去重记录，覆盖本轮 prepare/commit 期限；期间迟到 prepare 不能复活它，
commit 始终不能创建表项。控制消息使用有序的现有 mTLS 通道，外层重建使用新身份。
NodeLink PING/PONG 仅判断物理连接，不混入业务 DATA。
PING 间隔为 5 秒，20 秒未收到新的有效 PONG 则关闭 Link。`last_ack_ping` 记录已确认的最大序号，
允许确认较早发出但尚未确认的 PING；重复、倒退或超出已发送范围的 PONG 不延长存活期限。
LnkChannel 使用一个定时器等待最近的建立、心跳、Flow 准备或关闭身份回收期限，不再每 100ms 扫描。
新增更早期限时重新唤醒并计算；没有待检查对象时无限期等待，stop 取消等待并回收任务。

LnkChannel 的内部载荷使用 `lnk_channel.h` 中独占、可移动的 PooledBuffer，存储来自同一数据执行器上的
`std::pmr::unsynchronized_pool_resource`，不添加锁或 strand。缓冲保存原始分配大小及载荷视图，
移动、偏移和排队不复制载荷，析构时以原地址和大小归还池；新分配的字节不做初始化写入。
所有权由带 PMR 释放器的 `std::unique_ptr` 管理，载荷视图使用 `std::span`，切片只调整视图。
移动时清空源视图，数据访问直接读取视图，不在每次访问时检查存储是否存在。
TCP 直接读入池化缓冲；UDP 直接接收到最大合法报文大小加 1 字节的池化缓冲，验证后移除头部视图并转交发送队列，
不再从公共接收数组复制载荷。多出的 1 字节用于识别超长报文，避免截断后的报文被当成完整帧。
空载荷不分配字节块，UDP 心跳和 bootstrap 可复用当前接收块。
公开 FlowFrame 继续独立拥有 vector/string，在发送和接收接口边界复制一次，池化缓冲不跨执行器泄漏。
等待 receive_flow 时保持 LnkChannel 存活，池声明在持有缓冲的队列之前，所有缓冲先于池销毁。
端点 8MiB 缓存预算按缓冲原始分配大小加帧开销计费，避免 UDP 小载荷仍持有完整接收块却只按有效字节计费。
池可向上游按块申请并缓存内存，不承诺零上游分配或新的总内存硬上限；现有队列容量限制保留。

### 步骤 4：逐跳分派与结束事件

LnkChannel 的本地表项保存逻辑流身份、transport、前后邻居/NodeLink、
临时或已提交状态、两个方向的结束状态、期限与容量占用，不保存业务 service 或 Agent socket。
按身份查表，检查提交状态、方向和预期入边；只在首节点注入 forward，在末节点注入 reverse。
中间 Node 转发，对应终点交付内部接收接口；错误逻辑流帧不能使共享 NodeLink 因协议语义错误关闭。

| 逻辑流 | B 的 forward 映射 | B 的 reverse 映射 |
|---|---|---|
| S1：A→B→C | A–B → B–C | B–C → A–B |
| S2：A→B→D | A–B → B–D | B–D → A–B |

TCP 同一逻辑流、同一方向的 DATA 和 FIN 保持顺序：FIN 不能被优先发送到此前 DATA 前面。
FIN 后拒绝该方向的新 DATA 和重复 FIN，另一方向仍可继续；FIN 后仍允许 RESET。
即使两个方向都收到 FIN，也不自动删除 Flow，最终由显式 close 或失效流程释放。
RESET 释放该逻辑流并以失败原因唤醒终点等待者。
此处先验证逻辑事件传播，真实 socket 的 shutdown_send/排空在第四阶段由首末桥接实现。

### 步骤 5：容量、故障与停止

- LnkChannel 保持每条 TCP 一个读链、一个写链；发送队列有界，并为通道保活帧保留少量容量。
- LnkChannel 的活动表项加关闭去重记录最多 1000 条，每逻辑流终点队列最多 16 帧，
  全模块终点暂存最多 8 MiB（缓冲原始分配大小加每帧 128 字节）。中间转发直接入物理队列，
  TCP 每 NodeLink 和 UDP 全局队列保留 4 个名额给控制帧，DATA/FIN/RESET 的准入上限为 96。
  容量不足返回 capacity_exceeded/逻辑流错误，不无限增加 detached 协程或隐藏缓冲。
- 逻辑流拥塞、逻辑关闭和畸形逻辑流帧不走 fail(link)。最小版不承诺慢逻辑流完全不影响其他逻辑流的延迟，
  也不把拥塞失败处理当作信用控制或业务背压。
- 物理 NodeLink 断开时，依赖它的逻辑流全部失败；控制连接失效、成员离线/地址变化和 epoch 改变同样清理。
  已入物理队列或网络的旧帧由终点逻辑流身份/状态验证拒绝，不重新建路。
- stop 先拒绝逻辑流请求并取消控制事务，再清理转发表/队列/等待者，
  最后按现有顺序排空通道数据任务、停止集群控制连接、join 数据线程。
  LnkChannel 使用 AsyncEvent 等待全部数据任务退出，停止过程屏蔽调用方取消，支持并发和重复停止；
  activate 只启动一组后台任务，停止后不能再次激活。NodeFlow 使用 Prepared/Active/Closed 表达生命周期。
- 中间 Node 不使用 TokenBucket、服务限速、业务流量统计，也不依赖 Agent 的重连策略。

### 步骤 6：后续机制放置的位置

下列是设计边界，不是本阶段待实现类或占位协议。

| 后续机制 | 放置位置 | 对原有业务的影响范围 |
|---|---|---|
| 每流信用 | LnkChannel 的逻辑流/方向状态与发送准入 | 首末端通过发送结果/可写事件协作；不进入 Pipeline 的限速器 |
| 公平调度 | 同一 NodeLink 写链之前的逻辑流队列选择 | 修改逻辑流发送选取，不改变路径选择、Agent 首帧或 socket 所有权 |
| 连接池/多通道 | NodeLinkMgr 建连策略与 LnkChannel 连接选择 | 逻辑流层通过邻居/transport 解析当前通道；业务端不持有具体 NodeLink/socket |
| 存量换路 | NodeLinkMgr 的路径协调与 LnkChannel 的分段表 | 首末端保持逻辑流；届时增加路径代次、排空/确认和新旧段切换协议 |
| 背压 | 终点实际消费 → 逻辑流可写/信用 → 首末端暂停或恢复 socket 读取 | 桥接使用逻辑流接口，不直接观察 TCP 发送队列；中间暂存仍有硬上限 |

分段表当前固定记录 NodeLink ID，prepare 直接检查 LnkChannel 中的相邻 NodeLink 状态、节点、transport 和 epoch；
发送使用已安装 NodeLink，并检查其 Ready 状态，不能在发送每帧时随意更换连接而破坏顺序。
池化时可扩展该解析点并在逻辑流安装时固定所选连接，不把某条永久 socket 的引用暴露给首末业务。
发送准入、逻辑流分派和物理写链分别是信用、调度和池化的修改位置；最小版可用直接方法实现。

信用与背压是内存/消费能力控制，和按字节每秒的服务限速不同。
queued、socket 写完成和终点已消费是三个不同事件，未来信用不能在解析到帧时就无条件返还。
真实 TCP 接入前需确定持续流量下的可写等待与失败传播；不要求先实现公平调度、连接池或存量换路。

保持模块边界能减少业务改动，不代表存量换路无需协议升级。
换路时的序号、路径代次、旧数据排空和顺序保证需单独设计与验证，当前不提前增加字段或空状态。

### 步骤 7：增量、代码落点与验收

| 增量 | 交付与验收 |
|---|---|
| 2.1 帧与本地逻辑流表 | 逻辑流身份/方向、解析分类、模块与执行域边界；原始通道测试继续通过 |
| 2.2 全路径安装 | 相邻 ensure 复用、prepare/commit/close、建立期限和错误关联 |
| 2.3 TCP 逐跳 | 双向 DATA/FIN/RESET、分叉共用 A–B、顺序与逻辑流隔离 |
| 2.4 UDP 逐跳 | 固定 socket 分派、回程与 datagram 边界，无可靠投递/重传 |
| 2.5 清理与回归 | 容量、部分提交失败、断链、离线、epoch 变化和停止排空 |

| 文件/模块 | 修改内容 |
|---|---|
| protocol/inc/message.h、protocol/src/message.cpp | flow.* 控制命令、LnkFrameHeader 固定头编解码及 FlowFrame 校验 |
| protocol/inc/async_event.h | 一次性广播完成事件；单个等待者取消不影响共享事务 |
| protocol/inc/lnk_channel.h、protocol/src/lnk_channel.cpp、protocol/src/lnk_channel_flows.cpp | 共享 TCP/UDP、直接逻辑流分派、容量及统一监控 |
| node/inc/nodelink_mgr.h、node/src/nodelink_mgr.cpp | control_io 内的建连、复用、路径与逻辑流事务、通知及停止管理 |
| node/inc/relay_node.h、node/src/relay_node.cpp | 直接所有权、内部逻辑流接口、控制分发及停止编排 |
| protocol/CMakeLists.txt、node/CMakeLists.txt、test/CMakeLists.txt | 按模块归属编译与注册测试 |
| test/lnk_frame_test.cpp、test/lnk_channel_flows_test.cpp、test/node_flows_test.cpp | protocol 独立链接、数据面故障边界和真实 Node 路径测试 |

测试在不启动 Agent、不注册服务、不建立 Agent 业务连接的情况下完成 Node 逻辑流闭环。
至少覆盖 master 在路径之外/首末/中间、Node ID 排序与路径方向相反、TCP/UDP 分叉和回程、
交错发送的逻辑流隔离、DATA/FIN 顺序、半关闭方向状态、RESET、单逻辑流关闭后的其他逻辑流继续传输、
重复命令、旧身份/错入边/错方向、容量及保活、部分提交、准备超时、空闲已提交流、成员/控制失效及停止排空。

运行新增逻辑流测试、第一阶段 node_links 和现有集群、TCP/TLS/UDP Relay、Agent 生命周期相关回归，
完成 Debug/Release 构建；记录已知基线问题与未执行平台验证。

验收结果：显式路径上的 Node 逻辑流可独立建立、双向逐跳传帧和关闭；共享边不混流，
Node 数据面无需 Agent/服务对象即可运行和测试，后续机制修改在明确边界内进行。

## 第三阶段：Agent 路径提交与首末接入准备

### 目标与阶段边界

本节为第三阶段已确认设计，同时覆盖 TCP、TLS、UDP；本轮实施记录见文末。
复用前两阶段 NodeFlow，第三阶段完成控制与接入，第四阶段完成桥接；最终流程如下：

```text
Agent 通过已有主控制连接发现服务终点、获取拓扑
                         ↓
              读取有效缓存或计算最佳路径
                         ↓
       ┌─────────────────┴─────────────────┐
  失败、不可达或单节点                  多节点路径
       ↓                                  ↓
 原有单节点中继                    经主控制连接查询首 Node
                                          ↓
                                  node.located 返回首 Node
                                          ↓
                                  建立或复用首 Node 控制连接
                                          ↓
                                  向首 Node 提交最佳路径
                                          ↓
                                  首 Node 申请已有 NodeFlow
                                          ↓
                                  首 Node 通过集群消息通知末 Node
                                          ↓
                                  首末通过各自控制连接通知接入
                                          ↓
                                  首末 Agent 分别接入
                                          ↓
                                  安装首末数据桥接（第四阶段）
                                          ↓
                                  relay.ready 后业务转发
```

首 Node 已有就绪控制连接时跳过位置查询，直接复用。第三、第四阶段仅划分实现和测试工作，完成后整体上线。
直接实现最终使用的多跳建立流程，不增加 prepare_only 模式、专用准备 API、试验开关或阶段成功通知。
多跳只提交最佳路径，建立失败返回原请求者，由外层决定是否重新选路；内部不自动尝试其他候选或回退直连。

### 与原有单节点中继的关系

Agent–Node 控制与接入方式沿用原架构：单节点时两端连接同一个服务 Node，多跳时两端分别连接路径首尾 Node。

| 行为 | 原有单节点中继 | 多跳中继 |
|---|---|---|
| 请求方控制连接 | 服务所在 Node | 最佳路径首 Node |
| 服务方控制连接 | 服务注册所在 Node | 不变，即路径末 Node |
| relay.open / opened | 请求方与该 Node 通信 | 请求方与首 Node 通信 |
| relay.offer / reject | 服务方与该 Node 通信 | 服务方与末 Node 通信 |
| 数据接入 | 各自控制连接主机与回复的数据端口，使用 ticket 和 attach | 同样的接入方式，只是分别接入首尾 Node |

不为多跳另建一套 Agent–Node 控制通道协议。新增的是最佳路径提交、Node 间协调及 Flow 绑定，
多跳建连只保留最终业务所需的就绪等待，不改变原有单节点数据复制流程。
跨 Node 时不能直接复用要求 producer/consumer 都在本地的 Relay 配对状态；复用接入基础能力，
首尾各自保存一个本地端点，中间 Node 只保存已有 NodeFlow 转发表项。

### 路径计算与 LRU 缓存

- routing.max_nodes 统计完整路径中的真实 Node，包含首尾，不包含 Agent；A→B→C 计为 3，单节点路径计为 1。
- Agent 配置解析、AgentRouting 构造校验和计算上限统一为 8，默认保持 4，即最多首尾加两个中间 Node。
  原有 9..16 配置明确拒绝，同步修改错误提示、配置说明和测试；AgentConfig 默认值和示例配置保持 4。
- calculate_service_paths() 返回最佳路径；内部保留现有候选计算、排序及诊断日志，提交消息不携带候选集合或成本。
- 保留现有 LRU 结构及 TTL。有效缓存命中后直接取最佳路径，提交时读取当前 epoch，不在缓存中增加 epoch 字段。
- epoch 变化时清空缓存，保留现有拓扑失效及服务终点变化时的清理；普通质量快照更新不强制清空有效缓存。
- 计算失败、不可达或结果只有一个 Node 时，沿用现有单节点中继，不安装 NodeFlow。

### 服务发现与首 Node 定位

Agent 启动时通过配置的 server.host/port 建立主控制连接，用它注册服务、发现服务和获取拓扑。
service.lookup/located 仅用于原有服务发现，始终返回服务所在 Node，不增加入口定位分支。
多跳首 Node 定位使用独立的 node.lookup/located，复用已有的集群转交、回复关联和连接建立方式。
两种位置回复的 address/port 均为 Agent 控制地址和端口，Agent 继续调用 ensure_connection() 建立或复用连接。

路径计算必须先知道服务终点。首次 service.located 取得服务 Node，保留现有服务关联后计算或读取最佳路径。
已有有效服务终点信息时不重复发现：

| 场景 | 查询与后续行为 |
|---|---|
| 原有服务发现 | service.lookup/located 返回服务 Node，保持原流程 |
| 计算失败、不可达或单节点 | 使用服务 Node 位置取得入口连接，AgentRelay 发送原有单节点 relay.open |
| 多节点，首 Node 尚无就绪控制连接 | node.lookup 指定最佳路径首 Node，node.located 返回其控制端点；建立或复用连接后提交路径 |
| 多节点，首 Node 已有就绪控制连接 | 跳过定位，直接复用该连接并提交路径 |

node.lookup 通过已有主控制连接发送，只带本次 request_id 和目标 node_id，不携带服务名或完整路径。
接收 Node 将它定向单播给指定首 Node；首 Node 使用自身 advertise_address/control.port 回复，
沿用现有 session_id/request_id 集群回复关联。主控制 Node 即为首 Node 时本地回复。
指定 Node 只需是当前成员，不要求注册目标服务；不存在时返回 node.error，无回复受业务建立期限限制。
该查询不重新计算路径、不安装 Flow、不广播搜索其他入口，定位失败也不误删服务终点记录。

service.located 保存 service_locations_ 中的服务 Node 位置，不建立尾 Node 控制连接；
node.located 只完成本次多跳业务的入口连接等待，
不写回服务终点关联，不把首 Node 当作路由目的地，也不复制一份服务注册表。
不同查询和 Relay 建立使用各自请求 ID；定位回复须匹配预期首 Node，迟到回复不能覆盖更新后的路径或连接。

请求方通过首 Node 控制连接提交多跳 relay.open，服务方继续复用在末 Node 注册服务的控制连接。
relay.opened/offer 分别由首末 Node 发送，数据主机继续取各自控制连接主机，数据端口和 ticket 由回复给出。
主控制 Node 可以在业务路径之外；不是首 Node 时只负责发现/拓扑请求，是首 Node 时直接复用主控制连接。
不扩展拓扑控制端口字段、不新增地址注册服务，也不增加 relay.opened/offer 的数据主机字段。
首 Node 定位或控制连接失败返回原请求者，不在内部重新选路或回退直连。

### 最小协议增量与身份

| 消息 | 多跳增量与语义 |
|---|---|
| service.lookup / located | 保持原有服务发现语义和字段 |
| node.lookup（新增） | request_id、node_id，用于查询已选首 Node 的控制端点 |
| node.located（新增） | request_id、node_id、address、port，返回指定 Node 的控制端点 |
| node.error（新增） | request_id、node_id、reason，返回本次节点定位失败 |
| relay.open | 在原有 service、protocol、request_id 上增加 path、epoch 和剩余预算 budget_ms；只携带最佳路径 |
| relay.offer / opened | 增加 epoch、flow_id 和剩余建立预算 budget_ms；沿用本地 uuid、data_port、ticket，UDP 保留 session_id；不回传路径，数据主机取控制连接主机 |
| relay.attach | 保持 role、uuid、ticket 格式；RelaySession 关联本地端点与 Flow，接入帧不增加 Flow 身份 |
| relay.ready | 复用现有通知；全路径已提交、两端 attach 成功且首末桥接可用后才发送，允许业务转发 |
| relay.error / closed | 建立失败返回关联请求及失败 stage、原始 reason；就绪后失效通知 closed |

Node 间业务协调使用 relay.peer.open / attached / close，由首尾各自中继实例持有的 ControlRouterMulti 处理。
首 Node 本地按实际 consumer ControlSession 与 request_id 关联请求，首尾通信仅使用 (epoch, flow_id)；
不把请求方会话或请求 ID 发给末端或 master。集群来源使用认证控制通道给出的 source，不信任 Agent 自报身份。
master 不保存 Agent 业务事务；flow.open / opened / close.request 只补齐普通首 Node 请求第二阶段建路/关闭的入口，
参数只有 Flow 身份、有序 Node 路径和传输协议，不携带服务、Agent 会话、票据或 attach 状态。
Flow 身份继续为 (epoch, flow_id)，首末分别分配本地 uuid、ticket 和 UDP session_id，不要求两端相同。
ticket 只下发对应 Agent，未知或过期身份不能通过 attach 创建新端点。

### 多跳建立顺序与首尾职责

1. Agent 已发现服务终点，读取有效缓存或计算最佳路径。多节点时复用首 Node 就绪控制连接，
   否则经主控制连接发送 node.lookup，并根据 node.located 建立或复用首 Node 连接。
2. 请求方向首 Node 提交服务、协议、请求 ID、最佳路径和当前 epoch；首 Node 创建 RelaySession，
   该实例直接持有 ControlRouterMulti、请求方真实控制会话及本地数据端点句柄。
3. 首 Node 通过 NodeLinkMgr::open_flow() 申请 Flow；普通 Node 的远程申请由 NodeLinkMgr 内部处理，
   master 只执行第二阶段已有建路事务。TCP/TLS 使用 TCP NodeFlow，UDP 使用 UDP NodeFlow。
4. Flow 提交后，首 Node 使用 relay.peer.open 通知末 Node，包含 Flow 身份、服务、协议和剩余预算；
   末 Node 校验既有本地 Flow 元数据中的首尾身份，复用服务注册表查找 producer 会话，创建自己的 RelaySession。
5. 首末各自安装本地接入端点，并立即通过各自控制连接发送 relay.opened / relay.offer。
   各自生成本地 uuid、ticket 和数据端口，不增加预留或安装屏障；任一端失败由首尾直接通知并回滚。
6. 两端 Agent 沿用控制连接主机及回复的数据端口发送 attach。首末各自把端点绑定到现有 NodeFlow；
   末端 attach 和绑定完成后发送 relay.peer.attached，首 Node 汇总两端状态。
7. 首端激活本地端点并发送 relay.peer.ready；末端收到后激活本地端点。
   首末分别通知自己的 Agent relay.ready，再开始 socket↔NodeFlow 桥接；接入本身不能提前启动业务。
8. 失败、取消或控制断开时，双方立即清理各自端点，通过 relay.peer.close 通知对方；首 Node 发起关闭本 Flow，
   共享 NodeLink 保留。关闭幂等；首端取消后的迟到建路结果不能复活请求。

2..8 节点、无重复节点及成员资格等路径检查复用 NodeLinkMgr；首尾仅补充当前 epoch、真实会话与 Flow 端点身份检查。
不重新计算路由、不比较成本、不增加 ACL 系统。master 既不下发 Agent 端点绑定，也不等待 attach 或协调业务 ready。
集群单播仍可经 master 转交，这只是既有通信路由；master 恰好位于首尾时按普通端点角色参与。

### 代码与执行域边界

| 入口或模块 | 修改边界 |
|---|---|
| Agent 路径计算后 | 单节点或计算失败调用原流程，多节点定位/连接首 Node 后调用独立多跳建立函数 |
| service.lookup/located | 原有服务发现保持不变 |
| node.lookup/located/error | 独立的节点定位处理，复用集群转交、回复关联和 ensure_connection，不修改服务关联 |
| RelayNode 分派 relay.open/reject/cancel | 创建或查找 RelaySession，再交给该实例的 Single/Multi 控制器；共用业务容器和停止排空 |
| ControlRouterSingle | control_io 上推进本地双方接入、建立期限、ready、取消及通知；数据复制仍归所属数据 manager |
| RelaySession / ControlRouterMulti | 每个多跳实例直接持有控制对象与本地数据句柄，首尾通过集群消息直接协调；master 无业务状态 |
| RelayNode / RegistryMgr | Node 直接持有唯一注册表，处理公共注册、发现、拓扑和状态查询；删除原 ControlRouter 及重复配置、依赖和包装入口 |
| Agent 收到 relay.offer/opened | 无 Flow 身份调用原处理，有 Flow 身份调用独立多跳接入处理 |
| Pipeline / DatagramMgr | 复用监听和 attach 解析，按 uuid 所属端点表分派；多跳使用独立端点状态和处理方法 |
| NodeLinkMgr / LnkChannel | 复用现有路径事务与数据面，不加入 Agent 业务逐帧处理 |

复用 TLS 握手、编解码及 ticket 基础校验，不向原有 Relay 对象或复制循环加入多跳模式分支。
不新增通用协调框架、回调注册层或第二套路径管理器。
公共控制状态、RelaySession、ControlRouterSingle/Multi 和 NodeLinkMgr 路径状态属于 control_io；
socket、UDP endpoint 保持原有传输执行域归属；
跨域传递参数副本并用 post/co_spawn 下发操作和返回完成结果，不共享可变端点状态。

### 业务就绪、失效与停止

RelayNode 创建 RelaySession 时按所选路径构造 Single/Multi variant，创建后类型固定；
同一 Node 的共享数据管理器可并发承载本地配对和多节点端点。
多跳建立由首端协程顺序推进，只保存本实例请求与本地端点状态，不增加 master 业务表或通用协调框架。
不新增业务 Prepared 终态或 relay.prepared 通知；Agent–Node 继续使用已有 relay.ready 表达业务可转发。

就绪等待是最终转发的一部分：必须确认 NodeFlow 已提交、两端 attach 成功、首末桥接可用，才允许发送业务数据。
第三阶段实现路径与接入，第四阶段补齐桥接和最终就绪推进；不为未完成的增量建立单独产品模式或专用入口。

- TCP/TLS 多跳接入完成后等待 relay.ready，收到后进入第四阶段的 socket↔NodeFlow 桥接，不调用单节点双 socket 复制函数。
- UDP 沿用已有 relay.ready 等待语义；就绪前不转发业务包，不增加业务缓存。
- 原有单节点就绪与复制流程保持原样；多跳使用独立状态和处理方法，复用控制连接、握手与帧解析。

第二阶段已有的 flow.prepare/commit 继续保留：它们负责各 Node 安装、确认并提交路径表项，失败时回滚全路径，
属于最终产品的建路事务，与本轮删除的阶段专用业务准备通知无关。

整次建立沿用 Agent 建立超时预算，包含首 Node 定位、控制连接等待、建路和首末接入，阶段切换不重新计时；
跨 Node 下发剩余时长，各 Node 保留已有建路、接入及关闭超时。建立事务和本地端点占用现有容量预算，等待及通知有界。

失败、取消、超时、服务失效、控制断开、Flow 失效及停止时，唤醒请求等待者，释放首末 socket、ticket、uuid、
UDP binding，并通过既有接口关闭本 Flow，保留共享 NodeLink。consumer 控制连接所属节点为首 Node，
其离线或控制会话断开清理所属业务事务。沿用主控制连接断开时的拓扑失效及连接重连处理，
主连接不作为额外的 Flow 身份或业务回复跳点。
多跳末端服务失效时，按 service/protocol 和路径末 Node 关联真实服务终点进行失效处理，不能把首 Node 当作服务节点。

同会话同请求 ID 的一致重复请求关联已有事务，参数不一致返回冲突；关闭幂等，重复 attach 不替换已有端点。
取消后的迟到建立成功立即回收，不取消其他调用者共享的 NodeLink 建立请求。
定位请求及连接等待受同一建立事务取消约束，取消后的迟到回复不能重新建立业务。
等待期间 epoch 变化或服务终点改变时取消该建立事务，由外层重新选择；不能把已失效的待提交路径换上新 epoch 继续建路。
首 Node 控制连接被待建立或活动 Relay 引用期间不得被 release_unused_connections 回收，
复用同连接的并发事务各自释放引用，原有服务/主连接引用继续有效。

业务就绪后不增加运行期租约，通过显式取消及现有失效机制释放。
NodeLinkMgr 增加基于现有关闭完成事件的 Flow 终止等待接口，供业务事务感知断链，不新增轮询。
停止先拒绝新事务、取消等待并清理端点，再排空 Flow 和后台任务，最后关闭集群控制连接。

### 实施增量与验收

| 增量 | 交付 |
|---|---|
| 3.1 最佳路径与定位 | 默认 4、上限 8、最佳路径与有效缓存复用；独立 node.lookup/located 定位首 Node，复用控制连接建立 |
| 3.2 多跳控制 | 实例持有 ControlRouterSingle/Multi；首 Node 管理请求并与末端直接通信，NodeLinkMgr 提供普通首端 Flow 申请 |
| 3.3 首末接入 | 独立端点及 Agent 接入处理、复用各自控制连接主机与数据端口；与第四阶段桥接及 relay.ready 衔接 |
| 3.4 清理回归 | 取消竞态、超时、断链、控制失效、停止排空及文档同步 |

测试至少覆盖：

- max_nodes 包含首尾、不包含 Agent；省略配置默认 4、显式配置生效、8 节点支持，超过 8 的配置拒绝。
- 有效缓存命中携带当前 epoch，epoch 变化清缓存。
- service.lookup/located 保持原有行为；node.lookup/located 返回指定首 Node，未注册目标服务也能定位，两种请求不混淆。
- 主控制 Node 在路径外/首末/中间；首 Node 连接已存在时复用，不存在时通过定位回复建立，不重复建连。
- 首 Node 定位不覆盖服务终点或重新计算到首 Node 的路径；错请求 ID、迟到回复、并发定位及取消正确关联。
- 首 Node 不存在时定位明确失败，不广播或改路；定位失败不误删服务终点，末端服务失效不误认首 Node。
- 首末数据主机沿用各自控制连接，控制端口与数据端口不同、首末数据端口不同的情况下，实际接入目标正确。
- 单节点及计算失败沿用原中继；只提交最佳路径，多跳失败准确返回而不内部换路。
- TCP/TLS/UDP 均完成首末接入；最终缺少任一端或桥接未就绪时不能 relay.ready，提前业务数据不被转发。
- 不存在阶段专用准备 API、业务 Prepared 终态、relay.prepared 消息或临时上线开关。
- A→B→C、A→B→D 共享 A–B，master 在路径外/首末/中间；身份隔离，单 Flow 关闭不影响其他 Flow。
- 服务失效、旧 epoch、错 ticket、重复 attach、UDP 错 endpoint、重复请求、超时和取消竞态。
- 首 Node consumer 控制断开、producer 控制断开、NodeLink 断链及停止完整清理并唤醒等待者；
  首 Node 连接不被空闲回收，并发事务关闭不提前回收共享控制连接。

路径测试使用现有显式 ingress 输入能力构造确定性候选，不依赖 ICMP 权限或实际 RTT。
完成 Debug/Release 构建，运行新增测试及既有 Agent、集群、TCP/TLS/UDP Relay、NodeLink/NodeFlow 回归，
分别记录失败与权限跳过项。第三阶段验收内部路径及首末端点绑定、失败传播和清理，不新增测试专用生产入口。
第三、第四阶段完成后联合验收真实业务、就绪等待及单节点回归，再整体上线。

## 第四阶段：真实业务转发与回归

把 Agent–Node socket 绑定到已准备的 Node 逻辑流，形成“本地 socket ↔ 逻辑流”的读写桥接。
沿用第三阶段的独立 node.lookup/located 首 Node 定位流程，首末数据主机继续取各自控制连接主机；
桥接安装完成后再发送多跳 relay.ready，允许业务 DATA。
TCP/TLS 业务补齐接入确认、DATA/FIN/RESET、socket 半关闭和取消。
公平调度、连接池和存量换路暂不考虑实施，首末桥接通过逻辑流接口协作。
TLS 继续用于现有 Agent–Node 接入，Node 间传输使用共享明文 TCP；UDP 接入保留 session 和源 endpoint。

首末使用原有速率配置：TCP/TLS 等待令牌，UDP 超限逐包丢弃；服务流量与 accessor 只在服务末端统计，避免跨跳重复计数。
中间 Node 只做校验和分派。DATA 直接在传输执行域与 cluster_data_io 间传递，不经过 control_io。
TCP/TLS 双向排空后，首末交换 relay.peer.finished，再关闭 Flow；Agent 正常结束等待 Node 完成通知，收到正常关闭也继续排空数据 socket。
多跳 UDP 保留独立本地 session 和固定源地址，载荷最多 4096 字节，空报文有效；超长报文丢弃且保留会话，不增加分片协议。
本地 UDP 接收队列与共享 socket 写队列各最多 16 包，队列满时丢弃当前报文；NodeFlow 发送超出既有容量终止本 Flow，不增加信用或调度层。
验证真实应用、分叉回程、多服务、多流、半关闭、局部断链和事务回滚，与第三阶段一起完成最终就绪流程后整体上线。
不维护阶段专用模式、临时协议、独立准备入口或多跳上线开关。

以下保留各轮历史实现和验证记录，旧名称、平台及测试数量只对应当轮。
当前结构以正文为准，当前 Linux 审查结果见文末。

## 第一阶段验证

新增 node_links 测试及原有配置测试覆盖必填/旧字段/端口冲突、共享收发、流隔离、
重复与并发 ensure、master 端点、连接/DNS 错误、UDP 接入丢失、错误凭据与旧 ID、
成员离线及停止中的任务排空。运行现有集群、TCP/TLS/UDP Relay 和 Agent 生命周期回归，
分别完成 Debug/Release 构建与 CTest。

> 以下为历史实施记录，旧名称和租约行为表示当时版本；当前设计见上方正文及末尾取消运行期租约的记录。

## 实现与验证记录（2026-10-07）

实现入口：

- `node/inc/node_links.h`：Link 结果和建连协调状态；数据声明位于 node_channel，帧编解码位于 protocol/message。
- `node/src/node_links.cpp`：master ensure、并发合并、成员/epoch 校验、控制命令和清理。
- `node/src/node_channel.cpp`：单独数据域上的 TCP/UDP 首帧、读写链、固定 endpoint、保活及任务排空。
- `protocol/inc/frame_io.h`：由原 Pipeline/DatagramMgr 和新数据通道共同使用的帧解析。
- `test/node_links_test.cpp`：真实本地 socket 和三 Node 集成、故障及生命周期测试。

| 验证 | 结果 |
|---|---|
| Debug / Release 全目标构建 | 均成功 |
| 新增 node_links | Debug 44.30 秒通过；Release 44.29 秒通过 |
| 新增通道与现有集群、TCP/TLS/UDP Relay、Agent 生命周期/重连/集群、Proxy Relay 指定回归 | 最后补齐启动隔离边界后，两种构建均 9/9 通过 |
| 全量 CTest | 两种构建均为 18/21 通过；三项基线失败见下 |
| 必填端口、旧 port 拒绝、端口范围/冲突 | relay_integration 中通过 |
| 部署端口预检入口 | Debug/Release 的 --check-cluster-config 均通过示例配置 |
| 部署脚本 | bash -n 语法通过；未执行 Linux 安装或 systemd 部署 |
| Python Dashboard smoke | 本机无 Python，未运行；fixture 已补必填数据端口 |

在同一 Windows 主机，用未修改 HEAD `66df1ffd9f9c872a7f3406e3ac4994e35537c63f`
隔离构建并复测后，三项相同失败均可复现：

- agent_path_cache：30 秒超时。
- proxy：已占用 HTTP 监听端口仍能绑定的断言失败。
- probe_integration：具备原始 socket 权限时崩溃；受限权限下按现有规则跳过。

因此本次新增及指定传输/生命周期回归通过，全量测试尚未全绿；这些旧测试问题未在本次共享通道改动中扩展修复。
Debug/Release 全量日志分别位于 `build/node-links-final-debug-tests.log` 和
`build/node-links-final-release-tests.log`，基线日志位于 `build/node-links-baseline-build/Testing/Temporary/LastTest.log`。
最后启动边界回归日志为 `build/node-links-startup-debug-tests.log` / `build/node-links-startup-release-tests.log`。

第一阶段提供共享通道和带 flow_id 的基本收发。第二阶段已安装 B 上的会话映射，
验证 A→B→C / A→B→D 的 Node 测试数据逐跳转发；Agent 路径提交和真实业务在第三、第四阶段接入。

## 设计检查后的修正（2026-10-07）

- activate 不再在调用线程修改数据任务计数；停止后的延迟启动不会重新创建后台任务。
- link.status 请求与回复分别处理，补齐两端查询、请求关联、并发合并和等待释放。
- 移除 Pipeline 中只转调公共首帧解析的 read_relay_attach 及其未使用的 transport 参数。
- 数据监听 endpoint 改为启动局部变量；只保留配置端口和 bind 地址。
- TCP socket/每连接发送队列仅在 TCP Link 中构造，保留 UDP 全局 socket/发送队列。
- 建立请求索引、端点授权状态、握手标记和任务计数仍有各自用途，保留。

本轮 Debug/Release 全目标构建成功；新增通道及现有集群、TCP/TLS/UDP Relay、
Agent 生命周期/重连/集群和 Proxy Relay 指定回归均 9/9 通过。
node_links 分别用时 44.47 / 44.53 秒，包含 slave 回复、master 端点、TCP/UDP 并发状态查询、
关闭时取消查询及停止后的延迟启动检查。状态查询的身份校验和 5 秒超时由代码复核，
本轮未单独注入状态查询丢包；已有数据通道故障测试仍验证建连与保活超时。
本轮未重跑上表全量 CTest 的三项已复现旧失败。日志：
`build/node-links-review-debug-tests.log` / `build/node-links-review-release-tests.log`。

## 第二阶段初版实现与验证（2026-10-07，结构已由下节修正）

调整已落地：物理通道及会话帧编解码位于 protocol/node_channel，原 node 数据实现文件已移除。
NodeLinks 保留建连协调；NodeSessions 在控制域协调路径，NodeSessionForwarder 在数据域逐跳分派。
本阶段没有引入 Agent 对象、业务限速、服务统计、PImpl、信用/调度/连接池占位类。

新增测试的实际覆盖范围：

- node_channel_protocol 仅链接 protocol，检查独立启动/停止、监听释放和 DATA/FIN/RESET 编解码；
  拒绝缺失参数、错方向、混合 flow/session 身份、超长 payload 和非法 FIN。
- node_session_forwarder 使用独立协议通道和数据模块，验证错方向/旧 epoch/未知会话拒绝，
  已关闭身份不能由 prepare/commit/refresh 复活，FIN 后 RESET、准入满时关闭受影响 Flow，
  准备超时、无控制续期的 20 秒租约清理、等待者取消，以及共享 Link 保活与新会话继续使用。
- node_sessions 在五个真实 Node、四个独立执行域上验证 TCP/UDP 的 A→B→C 与 A→B→D，
  交错双向数据、反向路径、并发申请、共享边复用、方向结束和错误原因传播、慢会话容量失败，
  master 位于首/末/中间、超过租约时长的正常续期、断链后的会话隔离、成员重启端口不一致，
  外层新身份重建及停止中的申请。

本机冷 UDP 接入测试曾观察到首包没有进入监听，单次尝试按约定返回超时。
测试外层显式重新 ensure 并校验新 Link ID，随后在 slave 中间节点完成 UDP 分叉；
实现中未增加接入包重发或自动重建。本轮没有改动主机防火墙规则。

部分 commit 确认丢失、独立丢弃 session.close/closed，以及运行中的 epoch 切换
尚未逐项做网络故障注入；对应期限/身份分支已代码复核，准备过期和停止续期的清理已直接测试。
Linux 实机、多机防火墙与 UDP 丢包/乱序环境未验证；当前 UDP 仍不承诺可靠性与顺序。
Agent 路径提交与真实 socket 桥接继续按第三、第四阶段实施。

本轮 Debug/Release 全目标构建成功。两种配置的新增及指定回归均为 14/14 通过，
包含 node_channel_protocol、node_session_forwarder、node_sessions、node_links、cluster_integration、
tls_channel_mtls、relay_protocol、TCP/TLS/UDP Relay、Agent 生命周期/重连/集群及 Proxy Relay。
node_sessions 分别用时 33.22 / 33.00 秒，node_session_forwarder 为 20.88 / 20.92 秒。
日志：build/node-sessions-final-debug-tests.log、build/node-sessions-final-release-tests.log。
本轮未重跑第一阶段记录的三项已复现基线失败；没有将指定回归通过表述为全量 CTest 全绿。

## 按 README 约束修正结构（2026-10-07）

检查结论：物理通道与会话逐跳转发共用数据执行域和生命周期，通过 FrameEvent/Event 回调及注册接口拆开，增加了两套启动、停止和监控。这个拆分没有形成可独立使用的协议通道。当前合并为 node 内的 NodeLinkChannels，protocol 只保留纯帧编解码。

| 模块 | 当前职责与状态归属 |
|---|---|
| protocol/node_frame | NodeSessionFrame 编解码和字段校验，无 socket、路由表或 Node 依赖 |
| NodeLinks | control_io 上的 Node 对建连事务、合并请求、授权和成员校验 |
| NodeSessions | control_io 上的整条路径 prepare/commit/close、确认和租约续期 |
| NodeLinkChannels | cluster_data_io 上的 TCP/UDP 读写、物理 Link 表、会话表、逐跳分派和统一期限监控 |
| RelayNode | 直接持有协调模块，消费控制通知并编排关闭与任务等待 |

已删除 NodeSessionForwarder、模块回调类型、回调注册/解绑接口，以及独立的会话 monitor/start/stop。数据实现使用 node_channel.cpp 和 node_channel_sessions.cpp 两个源文件，属于同一个类。收帧直接查会话表、选择出边，不绕行回调或额外转发对象。发送校验直接检查帧，避免只为校验构造 JSON 消息；逐帧分派不再复制 Link 元数据。

跨执行域只传参数副本与控制结果。数据模块提供有界 receive_event 协程接口，RelayNode 的单个 control_io 消费协程取得通知后直接调用 NodeLinks/NodeSessions。该队列最多 4096 条，仅承载 link/session 控制状态，不承载业务帧；容量耗尽报告原始错误并停止数据模块，不静默丢失状态或触发自动重建。

保留的状态各有用途：控制表表示全路径事务/授权，数据表表示本地 socket、相邻边及接收状态，不能跨执行域共享可变对象；Link 与 Session 具有不同身份和关闭范围；失效身份短期保留防止迟到命令复活会话。shared_ptr 用于仍被读写协程、接收等待者或事务确认引用的状态，移除查找表项后仍需安全完成取消。

这轮结构以当前共享连接、固定路径和有界队列需求为准，未加入信用、公平调度、连接池或换路占位层。后续可在发送准入、会话表及物理写链处扩展。Asio 的任务完成处理仍用于记录异常、计数和唤醒停止等待，不作为模块间可注册的业务回调。内部协程注明所属执行域；头文件使用防卫式声明，无 PImpl、strand 或新锁。

关闭顺序为停止路径申请与续期、清理会话、关闭并等待数据任务、等待控制通知消费退出，再停止集群控制连接。控制通知队列溢出、准备过期、租约过期及在途停止均纳入验证。

另移除 ensure_control/status_control/open_control 三个只负责重复切换执行域的包装。外部调用在 RelayNode 入口绑定 control_io，NodeLinks/NodeSessions 内部按执行域约束直接等待；相邻 Link 并发建立仍使用独立协程链。

本轮 Debug/Release 全目标构建均成功。新增 node_frame_protocol、node_channel_sessions、node_sessions、node_links，以及集群、TLS 通道、Relay 协议、TCP/TLS/UDP Relay、Agent 生命周期/重连/集群和 Proxy Relay 指定回归，两种配置均 14/14 通过。Debug 总用时 137.83 秒，Release 132.99 秒；路径测试分别 33.50 / 33.13 秒，数据模块测试 21.43 / 20.92 秒。node_links 新增控制通知队列溢出的错误传播检查。

日志为 build/node-data-review-debug-tests.log 和 build/node-data-review-release-tests.log。git diff --check 通过；本轮未重跑前述三项已复现基线失败，不代表全量 CTest 全绿。Linux 实机与多机网络尚未验证；Agent 路径提交、真实 socket 桥接及持续流量背压仍属于后续阶段。

## Node 数据面二进制格式（2026-10-07）

NodeFrameHeader、NodeFrameKind 和 NodeSessionFrame 合并到 protocol/message.h/.cpp，删除 node_frame.h/.cpp 及 to_msg/from_msg 转换。控制协调的 link.*、session.* 消息保持 CBOR。Node 数据 socket 使用以下固定帧头，整数按大端逐字段编解码，不传输 C++ struct 的内存布局。

| 偏移 | 字节数 | 字段 |
|---|---|---|
| 0 | 1 | magic=0x4e |
| 1 | 1 | version=1 |
| 2 | 1 | 类型：DATA=1、FIN=2、RESET=3、PING=4、PONG=5、诊断 DATA=6、ATTACH=7、ATTACHED=8 |
| 3 | 1 | flags：bit0 为 reverse，其余位必须为零 |
| 4 | 4 | body_length |
| 8 | 8 | master epoch |
| 16 | 8 | 会话 ID；诊断帧为 flow_id；接入与保活必须为零 |
| 24 | 8 | 保活 sequence 或诊断 message_id；其他帧必须为零 |

DATA 的帧体为原始字节，最多 4096 字节；FIN 无帧体；RESET 的帧体为最多 512 字节的原因。业务帧仍按本地会话表校验 epoch、方向与入边，未知或已关闭身份不能创建新会话。保活帧无帧体；诊断帧保留第一阶段 flow_id/message_id 收发接口。

TCP 的 link.attach/attached 首帧继续复用 WireMessage/CBOR，双方匹配凭据及 data_version=1 后切换到上述二进制格式；后续不再接受接入帧。对端及 Link 身份由已确认 socket 绑定，数据帧不重复传 node 字符串或凭据。

UDP 每个 datagram 均为 8 字节 Link ID + 32 字节帧头 + 帧体，精确校验整体长度、epoch 和固定 endpoint。ATTACH/ATTACHED 的帧体为原有 WireMessage/CBOR；普通数据与保活不解析 CBOR。仍各发送一次接入包，不增加重传、重连或可靠投递。

发送队列直接拥有固定头和原始载荷，TCP 使用两个 buffer，UDP 使用三个 buffer，一次发送完整帧/datagram。中间节点只解析头并移动载荷到出边队列，首末接收队列也移动 DATA 载荷；不为每跳生成 JSON 对象、编码 CBOR 或拼接整帧副本。接收仍需要独立缓冲区，RESET 原因仍有小量字符串复制，不宣称系统级零拷贝。拒绝准入时自动释放未发送载荷，继续保持原有队列容量、保活、租约、FIN/RESET 与停止语义。

数据线格式已变更，Node 需要统一升级；没有旧数据格式兼容分支。本次未接入 Agent 业务桥接或增加流控/调度层。

本轮验证：Debug/Release 全目标构建成功，二进制协议及新增/指定回归均 14/14 通过，总用时分别 136.64 / 131.09 秒。node_frame_protocol 检查固定字节序、版本、类型、标志、长度与字段组合；node_links 检查旧 epoch、错凭据/身份/版本、UDP 长度不匹配、完整 4096 字节 TCP 数据及非法 TCP 长度提前失败；node_sessions 在 TCP/UDP 分叉路径验证双向完整载荷和空 DATA。既有容量拒绝、FIN/RESET、保活、租约、停止清理及 Agent/Relay 回归继续通过。

日志：build/node-binary-debug-tests.log、build/node-binary-release-tests.log。git diff --check 通过。本轮未重跑已记录的三项基线失败，未做吞吐基准或 Linux 多机验证；这些功能测试不用于宣称确定的吞吐提升。

## 命名统一与冗余清理（2026-10-07）

保留 NodeFlow 的原始领域含义：数据域表项使用 LnkChannel::NodeFlow，控制域路径协调器使用 NodeFlows。
不引起歧义的辅助类型统一为 FlowFrame、FlowResult、FlowSendStatus，以及 LinkResult、LinkStatus、LinkData。
物理通道协调器为 Links，数据实现为 LnkChannel；对应文件为 links.*、node_flows.*、lnk_channel.* 和 lnk_channel_flows.cpp。
二进制协议公共类型为 LnkFrameHeader/LnkFrType，仍放在 protocol/message，固定头布局和版本保持一致。

原 Node 逻辑流 session.* 命令统一为 flow.*，参数 session_id 改为 flow_id；原有 ControlSession 和 Agent UDP session_id 保留。
这次控制命令更名需要所有 Node 同步升级，不保留旧转发命令或接口别名。
内部入口使用 async_ensure_link、async_link_status、async_send_link、async_receive_link、async_close_link，
以及 async_open_flow、async_send_flow、async_receive_flow、async_close_flow，
新测试名称为 links、node_flows、lnk_channel_flows、lnk_frame，CMake、调用方与当前文档已同步。

已删除仅为内部校验复制元数据的 LinkInfo/info；prepare_flow 直接验证同数据域内的 Link 状态与身份，
对外只提供 link_ready 状态查询。NodeFlows 不再额外传入、保存可能与通道不一致的数据执行器，跨域任务使用 channel.executor()。
Link 失效与租约过期时直接遍历并删除受影响 NodeFlow，保留共享所有权直至当前操作结束，移除两组临时 vector。
没有加入兼容包装、回调注册、额外调度层或每帧 JSON 转换。

本轮验证：Debug/Release 全目标构建通过，四个更名测试及指定集群、TCP/TLS/UDP Relay、Agent 生命周期/重连/集群与 Proxy Relay 回归均 14/14 通过。总用时分别 136.35 / 137.55 秒；node_flows 为 33.53 / 32.91 秒，lnk_channel_flows 为 21.50 / 21.39 秒，links 为 44.58 / 44.22 秒。分叉路径、共享复用、拥塞隔离、断链、租约与停止排空继续通过。

日志：build/lnk-naming-debug-tests.log、build/lnk-naming-release-tests.log。git diff --check 通过；当前源码及构建定义无旧转发类名、文件名或 session.* 命令。未重跑前述三项基线失败，未进行 Linux 多机或吞吐验证。

## 保留 NodeLink 核心名称（2026-10-07）

核心物理通道对象恢复为 LnkChannel::NodeLink，控制域协调器恢复为 NodeLinks，与 NodeFlow/NodeFlows 对应。
协调模块为 node/inc/node_links.h、node/src/node_links.cpp；测试为 test/node_links_test.cpp，CTest 名称为 node_links。
RelayNode 和 NodeFlows 通过 node_links_ 直接持有或引用协调器；数据域表仍由 LnkChannel 直接拥有。

LinkResult、LinkStatus、LinkData 保持辅助类型简写，FlowFrame、LnkFrameHeader/LnkFrType 和 lnk_channel.* 保持当前命名。
link_id、links 路径参数、link.* / flow.* 控制命令、现有内部异步接口及二进制数据布局均保持原义。
本轮只调整 C++ 核心类型、协调模块/测试名称和文档，不增加兼容别名、封装层或额外状态。
前述命名记录中的 Link/Links 表示上一轮名称，当前命名以本文开头的约定及本节为准。

本轮验证：Debug/Release 全目标构建通过；node_links、lnk_frame、node_flows、lnk_channel_flows 均 4/4 通过，总用时分别 99.93 / 99.11 秒。覆盖共享 TCP/UDP、双向及分叉转发、隔离、期限与停止清理。日志为 build/nodelink-naming-debug-tests.log、build/nodelink-naming-release-tests.log；git diff --check 通过。

## 按业务边界清理数据热路径（2026-10-07）

生产数据只通过 NodeFlow 接口注入和接收。删除公开 send_frame、link_ready、LinkData、Diagnostic 类型及独立诊断接收队列，
同步删除 RelayNode 的 async_send_link/async_receive_link。错误帧测试改为真实 TCP peer 写入，正常双向多流测试改用 NodeFlow。
协议类型 6 保留为空缺，不重排 DATA/FIN/RESET/PING/PONG/ATTACH/ATTACHED 的已有数字；32 字节布局及版本保持不变。

本地 send_flow 只调用一次 FlowFrame::validate；每一跳网络接收只通过 LnkFrameHeader::decode 校验一次格式。
TCP 在分配帧体前验证头部，保持消费完整帧后丢弃旧 epoch 的流边界；UDP 验证 Link/endpoint、epoch 与 datagram 精确长度。
process 只分派，不重复入口校验。分派仍验证 NodeFlow 身份、活动状态、预期入边、方向、FIN 状态及 UDP 只允许 DATA。

LnkFrameHeader::encode 是 noexcept 纯编码，不再次验证已确认的帧；enqueue 的长度一致性是内部 Debug 断言，
Release 不承担重复格式检查。UDP 本地接入 CBOR 的大小在建连/响应阶段检查，队列容量和关闭/停止状态仍在实际排队边界执行。
没有增加 checked/unchecked API、标记包装类或新的校验层。

NodeLink 的 id/epoch/transport 和 NodeFlow 的 epoch/transport 在创建时转换成不可变字段；
JSON 参数只作为不可变控制通知模板和 prepare 校验信息，逐帧转发不查 JSON。
deliver 单次查出边后直接编码入队；UDP 接收使用单次 find，UDP 发送队列直接持有 NodeLink，
出队后检查 closed，避免再次查表及重复保存 endpoint/id。关闭后在途 TCP 读取不再进入分派。

本轮验证：Debug/Release 全目标构建通过。14 项指定回归中，首次运行各 13 项通过；新增停止测试等待了后停止的对端，提前收到正常的断链错误，导致错误原因断言失败。测试改为等待先停止的本地端后，lnk_channel_flows 在 Debug/Release 单独复跑通过，分别用时 21.15 / 21.47 秒；生产停止逻辑未为测试改动。至此 14 项相关测试均已通过。覆盖二进制格式、实际 TCP 错误入边/未知流/旧 epoch/迟到数据/非法长度、TCP/UDP 双向多流、共享及分叉转发、容量与租约、停止清理，以及集群、TCP/TLS/UDP Relay 和 Agent 回归。

日志：build/node-hotpath-debug-tests.log、build/node-hotpath-release-tests.log 记录首次回归；build/node-hotpath-stop-debug-tests.log、build/node-hotpath-stop-release-tests.log 记录停止测试复跑。git diff --check 通过。未重跑此前记录的三项基线失败，未做吞吐/CPU 基准或 Linux 多机验证，不宣称已测得性能提升。

## 统一 NodeLinkMgr（2026-10-07）

node_links.* 与 node_flows.* 合并为 nodelink_mgr.h/.cpp，删除两个旧协调器。RelayNode 直接持有 NodeLinkMgr，与 Pipeline/DatagramMgr 同层；管理器直接持有 LnkChannel，不通过旧类包装或回调注册。NodeLink/NodeFlow 核心概念及 link.*/flow.* 命令、二进制格式和 RelayNode 外部入口保持原义。

NodeLinkMgr 在 control_io 管理 LinkRequest、FlowRequest、端点授权、状态查询、路径租约和回复关联，合并执行器、集群/拓扑依赖及 running 状态。数据通知循环与续期任务移入管理器，取消 RelayNode 的通知任务和等待标记；关闭先停止新申请、结束请求并排空 LnkChannel，再等待续期与通知任务退出，之后 RelayNode 关闭集群控制连接。通知流失效时终止数据模块，不等待通知协程自身、不自动重建。

LnkChannel 继续在 cluster_data_io 独立管理 socket、NodeLink/NodeFlow 数据状态、收发、分派及容量，NodeLinkMgr 不进入逐帧热路径。成员变化、控制失败和停止处理直接遍历/删除受影响请求，不再复制临时请求集合。两阶段同步监听绑定/回滚与 activate 保留，失败次数、期限、共享复用及租约行为保持原样。历史记录中的 NodeLinks/NodeFlows 表示合并前实现，当前以本节及正文为准。

本轮验证：Debug/Release 全目标构建通过，14 项相关回归均 14/14 通过，总用时分别 134.46 / 136.63 秒。覆盖共享 TCP/UDP 与并发复用、master 端点、分叉/回程、流隔离、期限/租约、成员变化、断链及停止中的在途操作，以及现有集群、TCP/TLS/UDP Relay、Agent 生命周期/重连/集群与 Proxy Relay。日志为 build/nodelink-mgr-debug-tests.log、build/nodelink-mgr-release-tests.log；git diff --check 通过。未重跑此前记录的三项基线失败，未做 Linux 多机或吞吐基准验证。

## NodeLinkMgr 合并后清理（2026-10-07）

移除仅供统一通知循环调用的 link_event/flow_event 两个旧分派函数。receive_events 在 control_io 中统一检查 running、查端点、上报并清理身份；LinkReady 的本地状态更新和 Link 凭据剔除继续保留。Link 通知直接移动原 CtrlMessage，不再复制参数 JSON。控制消息的运行状态检查统一放在 handle 入口，handle_link/handle_flow 保留各自的授权、身份及状态机校验。

Link 请求索引改为排序后的 Node ID 对和 RelayProtocol 组成的类型化 tuple，去掉 JSON 数组 dump 作为查找键；控制消息格式保持原样。路径校验辅助函数明确命名 valid_flow，避免合并后与 Link 成员校验混淆。

审核未发现可以删除的剩余管理器成员：Link/Flow 授权表属于 control_io，LnkChannel 数据状态属于 cluster_data_io，不能因内容相似直接合并；请求确认集合、并发等待者、关闭事务、租约和任务退出标记分别承担实际生命周期职责。start/activate/rollback 保证监听失败时的同步回滚；shutdown/stop 分离用于通知任务异常时避免等待自身。未增加测试专用生产接口或改变数据热路径。

本轮验证：Debug/Release 全目标构建通过；14 项相关回归均 14/14 通过，总用时分别 135.46 / 132.33 秒。日志为 build/nodelink-mgr-cleanup-debug-tests.log、build/nodelink-mgr-cleanup-release-tests.log；git diff --check 通过。复用/并发/反向 Node 对、状态查询、双向多流、分叉路径、期限、租约、离线及停止清理和既有 Agent/Relay 行为继续通过。未增加测试专用生产代码，未重跑此前记录的三项基线失败，未做 Linux 多机或吞吐基准。

## 取消运行期 Flow 租约（2026-10-07）

已删除 FlowRefresh 命令、refresh_flow、NodeLinkMgr 的续期协程、扫描定时器、refreshed 时间和续期任务退出标记。
所有 committed 后直接返回 Ready；LnkChannel 只检查未提交表项的 prepare_deadline，提交后没有运行期过期扫描或定期控制消息。
建立阶段和关闭确认仍各有超时，关闭身份的 10 秒去重记录及 NodeLink 心跳保留。

集群 Connector 在已加入的控制连接接收断开或心跳失败时，先标记断开并上报 ClusterError，再进入重连。
NodeLinkMgr 结束当前 Flow 请求和关闭等待，先清空端点授权再投递本地 Flow 清理，避免清理通知发往失效控制连接。
成员/epoch 变化、NodeLink 断链、显式 close 和停止继续清理对应 Flow；控制连接恢复后重新申请 Flow。

现有数据通道场景由运行期到期改为空闲已提交 Flow 保持有效、控制失效清理并唤醒接收者；路径场景保留长时间空闲后传输。
本轮 node_links、node_flows、lnk_channel_flows、lnk_frame、cluster_integration 五个 Debug 目标编译通过，静态搜索无运行期租约代码残留，git diff --check 通过；未运行测试。

## 最终整理与验证（2026-10-08）

### 当前状态与表的职责

| 成员/类型 | 保留原因 |
|---|---|
| LinkRequest / link_requests_ | master 按排序后的节点对与 transport 合并并发建连、复用 Ready Link；双端准备/就绪各占两位 |
| StatusQuery | 一轮双端状态查询的结果、唯一超时及完成事件；两个字符串记录已回复节点，两个 bool 记录 Ready 状态 |
| link_endpoints_ | 本节点收到的 Link 安装授权及回复关联，区别于 master 的全局建连请求 |
| FlowRequest / flow_requests_ | master 的路径参数、建立结果及 prepare/commit 确认；运行期间保留身份以便错误和关闭关联 |
| flow_endpoints_ | 本节点 Flow 安装授权及回复关联，不跨执行域共享数据表项 |
| closing_flows_ | 已退出活动表但仍等待全路径 close 确认的事务；与活动表分开以保持直接的状态边界 |
| completed_event / released_event | 分别广播建立结果和关闭确认，避免完成建立时误唤醒关闭等待者 |
| FlowRequest::timeout | 建立与关闭阶段复用的唯一计时器；准备剩余时长直接取 expiry，不重复保存 deadline |
| done_ / events_running_ | 等待通知消费任务退出，保证管理器销毁前任务已经结束 |

没有把上述表改为 DualIndexMap：它们不是同一个对象的两个唯一键索引，直接合并会混淆控制授权、master 协调及关闭阶段。
LnkChannel 中的 NodeLink/NodeFlow 表属于 cluster_data_io，包含本地 socket、相邻边及接收队列，继续由数据域独立拥有。

### 简化与修正

- protocol 新增最小 AsyncEvent，仅提供一次性 wait/notify_all。通知后到达的等待者立即成功；单个等待者取消返回 false，
  不取消共享事务。所有操作属于同一执行域，事件必须活得比等待协程久，不提供重置、超时或跨线程同步接口。
- Flow 路径限定为 2..8 节点，prepared/committed/closed 各用 uint8_t；测试覆盖第 8 位和 9 节点提前拒绝。
  Link 双端确认同样使用位掩码，LinkStatus 使用固定字符串/bool，不再分配 map。
- 状态查询使用独立完成事件和单个超时；取消一个调用者只返回其本地取消结果，其他调用者继续等待同轮查询。
- master 收到 LinkClosed 后移除 Ready 复用缓存；Flow 在等待并发 ensure 时被取消，立即结束其路径事务，
  其他使用者的共享建连不受影响。
- 已删除全部运行期 Flow 续租代码；保留建立/关闭超时、未提交 prepare_deadline、关闭身份去重及现有心跳。
  新回归验证已提交 Flow 长时间空闲后仍可传输，以及 master 停止后远端接收等待者被唤醒并报告失效。
- README 增加每个普通变量单独声明的约束；本次 33 个改动 C++ 文件已检查，普通变量均分别声明。
- 按用户选择保留集群发送队列溢出处理，不增加全局断开策略；远端旧 Flow 的回收限制见正文生命周期说明。

### 最终验证结果

Debug/Release 全目标构建通过；两种配置的示例配置预检命令均成功。
全量 CTest 各 22/25 通过，总用时分别 170.04 / 164.21 秒。
node_links、node_flows、lnk_channel_flows、lnk_frame、async_event 五项核心测试均通过；
集群、TCP/TLS/UDP Relay、Agent 生命周期/重连/集群和 Proxy Relay 回归通过。

三项已有失败仍存在：agent_path_cache 超时 30 秒，proxy 的 Windows 已占用 HTTP 端口断言失败，
probe_integration 在 Debug 崩溃、Release 本次报实际发送计数断言失败。
同一主机的未修改 HEAD 基线已记录这三项失败；本轮没有修改其测试及 route/proxy 实现，
也没有将全量结果描述为全绿。基线证据见前述第一阶段验证记录。

日志为 build/final-review-debug-tests.log 与 build/final-review-release-tests.log。
完整暂存内容的 git diff --check 通过。未做 Linux 多机、部署脚本运行、Dashboard Python 或性能基准验证。

## 当前 Linux 数据通道审查（2026-10-08）

本轮审查全部 Git 改动及未跟踪的新文件，包括 protocol 文件迁移、公开发送返回值、非抛异常帧校验、
单定时器调度、NodeLink 状态机、NodeFlow 生命周期、PMR 载荷及相关测试，未发现需要继续修改的生产代码问题。
保留现有 State 加两个握手 bool、统一 Frame、独占缓冲和同步 shared_ptr 引用接口，没有增加额外抽象层。

已同步本文和《RelayWeave 设计》的模块归属、状态转换、所有权、最近期限唤醒、有效 PONG、
内存转移、发送拒绝语义、容量计费、测试名称及源码索引。历史记录中的旧名称和结果不代表当前实现。
NodeLink/NodeFlow 从表中删除后由在途协程保活，通道和池覆盖这些任务的收尾；UDP 排队不持有 NodeLink。
公开 FlowFrame 不引用内部池，FIN 不自动释放 Flow，UDP Flow 只接受 DATA。

验证环境为 Linux ARM64、GCC 14.2、C++20、Debug。`cmake --build build --parallel 1` 全目标成功，无编译警告。
首次 CTest 在受限沙箱内因禁止创建 socket 出现初始化失败；允许本地 socket 后重跑完整 26 项：
25 项通过、0 项失败、1 项跳过，总用时 77.91 秒。
跳过项为 probe_integration，程序明确报告缺少 Linux CAP_NET_RAW；普通 TCP/UDP、Agent、Proxy、
NodeLink/NodeFlow、二进制帧、池化缓冲和路由测试均通过。

git diff --check、修改及新增 C++ 文件的行尾空白检查、相关文档本地链接检查通过。
本轮日志为 `/tmp/relayweave-final-review-build.log` 和 `/tmp/relayweave-final-review-tests-unsandboxed.log`；
沙箱首次运行记录在 `/tmp/relayweave-final-review-tests.log`。
本轮未重新执行 Release、ICMP 实测或吞吐/CPU 基准，不将池复用单元测试视为端到端性能提升的证明。


## 第三阶段实施与简化审查（2026-10-09）

本节保留第三阶段结束时的结构与验证记录；第四阶段在这些边界上补齐业务桥接，最新实现见文末。
第三阶段只完成控制与首末接入，当时不发送多跳 relay.ready；第四阶段补齐绑定、激活和正常结束排空。

### 第三阶段代码分工

| 代码 | 职责与所属执行域 |
|---|---|
| agent/src/relay_agent.cpp | control_io 上返回最佳路径，处理有效 LRU、epoch 失效、首 Node 定位、控制连接复用与引用保留；默认 4、上限 8 个 Node，包含首尾、不含 Agent |
| agent/src/agent_relay.cpp | 当前统一单/多节点 Agent 接入及生命周期；第三阶段原 forwarder_paths.cpp 和 direct 函数已在职责重构第 3 步替换 |
| node/src/relay_node_control.cpp | RelayNode 的公共注册、服务发现、节点定位、拓扑及状态查询，直接使用 Node 唯一 RegistryMgr；原 handle_cluster_lookup/location/status_query/status_report 保留在此文件 |
| node/src/control_router_single.cpp | control_io 上的单节点双方接入、建立期限、ready、取消与通知，不做全局查表或数据复制 |
| node/src/control_router_multi.cpp | control_io 上的首末 attached/ready/finished/close、实例预算、Flow 监视及失败清理；全局分派移到 Node |
| node/src/relay_session.cpp | 单/多节点实例直接持有 Single/Multi variant、本地数据句柄和唯一取消信号，跨域等待 install/wait/bind/activate/bridge/close |
| node/src/relay_node_relays.cpp | Node 的全局创建、请求/peer 查找和分派、统一实例容器、取消与停止排空 |
| node/src/pipeline_mgr.cpp、datagram_mgr.cpp | 数据域的本地配对和多节点端点、attach 校验及实际 I/O；无控制引用、服务等待或业务通知 |
| node/src/nodelink_requests.cpp | 普通首 Node 远程调用第二阶段已有 Flow 建路事务，参数仅含 Flow 身份、路径和传输协议，不携带 Agent 或服务业务状态 |

Node 不再持有公共 ControlRouter 或 ControlRouterConfig。公共查询直接进入 RelayNode，
全局业务入口进入 RelayNode，实例选择 ControlRouterSingle/Multi；没有第三个控制器、第二份注册表或额外业务协调层。
master 只承担原有 Flow 建路和集群转交，首尾通过 relay.peer.open/attached/close 直接协调。

### 第三阶段删除的冗余

- 第三阶段删除 RelayEndpoint 当时未使用的 epoch、flow_id 和与端点表键重复的 uuid。
  第四阶段只为实际数据收发绑定 epoch、flow_id；uuid 仍不重复保存。
- UDP session_id 仅存在于 DatagramMgr 的端点，TCP/TLS 端点不携带无用途的 UDP 字段。
- Agent 多跳接入在同一 transfer_io 中直接 co_await attach_path_relay，删除额外 co_spawn 和外层重复超时。
  DNS、connect、TLS 和 attach I/O 仍受剩余建立预算限制，ready 等待使用同一个绝对截止时间。
- 重复请求直接复用已有通知函数，删除仅调用一次的 replay 包装；清理无用途的包含、友元和函数末尾 return。
- 文档合并此前已被替代的第三阶段实施记录，删除旧 master 业务事务和旧文件入口的描述，保留一份当前流程说明。

### 保留的必要流程与状态

首末各自只有一个 RelaySession，由其 ControlRouterMulti 保存本次请求、Flow 身份、对端和建立截止时间；
数据管理器仅保存本地 attach 校验、socket/UDP 地址及接入完成事件。中间 Node 不增加 Agent 业务对象。
首端仅在 open_flow 成功且 epoch 一致后安装端点；末端先核对本地已提交 Flow 的首尾身份与协议。
这些建立检查位于拥有 Flow 元数据的控制层；第四阶段数据端点绑定业务帧所需身份，不重复执行路径校验。

取消信号、Flow 关闭事件等待和 Node 任务计数用于及时终止接入并排空后台任务，不能用删除这些状态来简化生命周期。
停止顺序为取消多跳实例、关闭本地端点与本 Flow、排空多跳任务，再停止 NodeLinkMgr 和集群控制；共享 NodeLink 保留。
跨执行域 co_spawn 保留；同域 co_spawn 仅在独立失效监视或需要给整个子操作施加超时时使用。

没有新增预留、安装或业务 prepare 屏障，没有阶段专用通知、试验开关或多候选重试流程。
已有 flow.prepare/commit 是第二阶段全路径安装事务，继续复用；第三阶段只补齐首末 Agent 接入及必要的失效清理。

### 验证

此前控制入口重构后的 Debug 与 Release 完整回归均为 26 项通过、0 失败，1 项 ICMP 权限跳过。
本轮精简后 Debug 全目标构建通过，无编译警告；完整 CTest 共 27 项，26 项通过、0 失败，
probe_integration 因 ICMP 权限跳过，总用时 166.48 秒。
覆盖公共查询、单节点 TCP/TLS/UDP、NodeLink/Flow、路径缓存与 Agent 生命周期、多跳首末接入、
同一监听上的单节点/多跳并存、重复请求/attach、错票据、取消、控制断开、分叉隔离、断链与停止排空。
构建与回归日志分别为 build/phase3-review-build.log 和 build/phase3-review-tests.log。
本轮未重跑 Release；源码差异空白检查与相关文档 41 个本地链接检查通过。

## 第四阶段实施与审查（2026-10-09）

### 最终业务流程

沿用第三阶段的路径选择、node.lookup/located 和首尾控制连接，不改变服务发现。
单节点和多节点均由 RelaySession 管理，分别直接持有 ControlRouterSingle 和 ControlRouterMulti。
master 仅执行第二阶段建路及既有集群消息转交，中间 Node 不持有 Agent 业务状态。

多跳建立顺序为 Flow 提交 → 首末端点安装 → opened/offer → Agent attach → 本地 Flow 绑定。
末端绑定后发送 relay.peer.attached；首端激活并发送 relay.peer.ready，末端随后激活。
首末分别发送已有 relay.ready，Agent 才开始业务复制。激活只用于切换本地端点的可转发状态，
没有新增业务 prepare、预留事务、安装确认屏障或阶段专用接口。

TCP/TLS 读取本地 socket 形成 DATA；读到 EOF 发送 FIN，收到 FIN 只关闭 socket 的发送方向。
TLS 专用接入流使用底层 TCP 半关闭，EOF/stream_truncated 表示该业务方向结束，另一方向继续排空。
两端各自双向结束后交换 relay.peer.finished，再关闭 Flow；正常关闭通知不会提前取消 Agent 数据读取。
Agent 在正常复制完成后等待 Node 完成通知再释放首 Node 控制连接引用，避免取消消息抢在最后 DATA/FIN 前关闭路径。
I/O 错误通过 RESET 与既有 peer.close/Flow close 收敛；显式取消不额外发送掩盖原始原因的 RESET。
双向传输协程统一在本执行域等待、取消和排空，保留最先发生的原始错误。

UDP 沿用原有监听、session 头和固定来源；本地 session 去头后直接作为 NodeFlow DATA，返回时写入接收端本地 session 头。
多跳载荷为 0..4096 字节；超长、错误来源和未就绪报文丢弃，不关闭会话、不增加分片。
复用原 bindings_ 索引分派单节点与多节点会话，删除重复扫描端点表的 session 分配检查。
本地接收队列和共享 socket 写队列各最多 16 包，满时丢弃当前报文；NodeFlow 发送仍遵守第二阶段的有界容量，容量不足关闭该 Flow。
单节点与多节点 UDP 通过同一条写链发送，等待者取消后跳过未发送报文；队列独立拥有报文字节，避免重用接收缓冲或跨流并发写 socket。

首末继续使用现有传输速率配置；TCP/TLS 等待令牌，UDP 超限逐包丢弃。
服务统计只在末端累计成功转发的 payload，rx 为服务向请求方，tx 为请求方向服务，不重复计算帧头或跨跳字节。
accessor 来自请求方真实控制会话，建立时加入、退出时移除。中间 Node 不做服务限速或统计。

### 代码边界与精简

| 代码 | 第四阶段职责 |
|---|---|
| node/src/relay_endpoint.cpp | 最小公共端点绑定、跨域收发和限速/计数；只为真实业务帧保存 epoch/flow_id，不重复保存 uuid 或路径 |
| node/src/pipeline_mgr.cpp | TCP/TLS 独立端点的双向桥接、FIN 半关闭和失败 RESET；单节点复制流程保持原样 |
| node/src/datagram_mgr.cpp | UDP 独立端点队列、共享单写链、session/来源校验、NodeFlow 桥接及退出清理 |
| node/src/relay_session.cpp | 通过已有数据管理器执行 install/wait/bind/activate/bridge/close，直接拥有本地句柄及自己的控制器 |
| node/src/control_router_multi.cpp | 首尾 attached/ready/finished/close 推进，建立预算与运行期分离，取消后等待桥接退出再释放端点和 Flow |
| agent/src/agent_relay.cpp | 同一传输执行域顺序执行接入与业务复制；多跳 TCP/TLS 双向排空，UDP 复用本地监听与来源；原 forwarder_paths.cpp 已替换 |
| protocol/src/xfr_channel.cpp | 多跳 Agent 半关闭复制及双向任务的原始错误传播，不改变单节点复制函数 |

业务 payload 直接在 transfer_io 与 cluster_data_io 间传递，control_io 只处理控制及完成结果。
RelayNode 停止时先取消并排空多跳任务，再停止 NodeLinkMgr；原有共享 NodeLink 和其他 Flow 的生命周期保持独立。
没有新增抽象控制器基类、业务总管、回调注册层、公平调度、连接池或存量换路。

### 验证范围

relay_paths 使用五个真实本地 Node、真实请求方/服务方 Agent 和应用 socket。
测试通过编译期友元填入现有有效 LRU，固定多跳路径，避免依赖 ICMP；不增加生产协议、配置开关或运行时测试入口。
多地址测试证书仅供该测试使用，覆盖 127.0.0.1..5，复用现有测试私钥，生产 TLS 主机名校验保持原样。
覆盖 TCP/TLS 多帧双向数据、应用半关闭后回包、超过建立预算仍可传输、两个方向的 socket RESET、
并发 UDP 会话、session 改写/来源/空报文/载荷上限、服务限速和精确字节统计，以及活动分叉流隔离、局部断链、取消和停止排空。
同一监听上的单节点并存、错票据、重复请求/attach、服务缺失和目标连接失败继续回归。

### 最终验证结果

Linux / GCC 14 / Asio 1.38.2 下，Debug 与 Release 全目标构建成功，均无 C++ 编译警告。
最终 Debug CTest 共 27 项：26 项通过、0 失败、1 项 probe_integration 因 ICMP 权限跳过，用时 161.89 秒。
最终 Release CTest 同为 26 项通过、0 失败、1 项相同权限跳过，用时 157.68 秒。
多跳集成 relay_paths 在两种构建中均通过，包含真实双 Agent 全链路业务及上述故障、限速、统计和生命周期检查。

构建日志：build/phase4-build.log、build/phase4-release-build.log。
回归日志：build/phase4-debug-tests.log、build/phase4-release-tests.log；独立多跳验证见 build/phase4-paths-tests.log。
git diff --check、修改文本的尾部空白检查及 41 个本地文档链接检查通过。
本轮为本机多 Node / 多执行域集成验证，未进行多机网络或持续吞吐测试。

### 第四阶段业务完整后的再次复查

按实际入口、接入、传输和关闭流程重新核对成员及函数调用，进一步精简如下：

- ControlRouterMulti 首末共用一次对端状态等待和一次本地激活。首端等待 attached，末端等待 ready；
  两个状态的含义仍明确保留，去掉重复等待、激活和检查分支，不增加公共状态机或回调层。
- 单节点 TCP/TLS、UDP Relay 删除仅转调 ControlRouterSingle::belongs_to 的包装函数，直接调用所属控制器。
- NodeLinkMgr 原 endpoint_ready 仅有末端调用，改为 egress_ready，删除没有使用场景的 ingress 参数和首端判断分支。
- Agent UDP 本地接收去掉临时 lambda 和重复查表，将来源 IP 检查放到单节点/多节点分支之前。
  修正多节点分支绕过原来源检查的问题，保留原有同 IP 更换端口后更新回包地址的行为。

保留的状态均有实际用途：ticket/role 用于接入校验，connected/active 分别表示已接入和允许转发，
epoch/flow_id 用于真实业务帧，peer_finished 用于正常 FIN 排空，取消信号和任务计数用于停止排空，
首 Node 控制连接引用用于业务期间保持连接。没有新增占位成员或预留接口。
UDP 共享写链及有界队列继续保留，保证多个 Flow 共用一个监听 socket 时只有一条发送链并持有待发送字节。

新增真实双 Agent UDP 回归：拒绝不同本地来源 IP 的报文、允许原 IP 使用新端口、回包发往更新后的端点。
仅加入来源拒绝断言时，修正前的 Release relay_paths 已失败并准确复现问题，日志见 build/phase4-review-red-test.log。

本次复查后的 Release 全目标构建成功，无 C++ 编译警告；完整 CTest 为 26 项通过、0 失败、
1 项 probe_integration 因 ICMP 权限跳过，用时 157.52 秒。
relay_paths 通过，用时 6.53 秒，包含新增来源与端口更新断言。
构建与回归日志分别为 build/phase4-review-release-build.log、build/phase4-review-release-tests.log。
git diff --check、41 个修改文本的尾部空白检查以及本次检查的 51 个本地文档链接均通过。
