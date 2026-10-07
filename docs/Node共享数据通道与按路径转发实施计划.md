# Node 共享数据通道与按路径转发实施计划

## 范围与现状

Agent 已在 Stream 建连前计算推荐路径，但业务仍直接连接服务所在 Node。
第一阶段已完成 master 协调的 Node 间共享 TCP/UDP 数据通道和基本分帧收发。
第二阶段已实现 Node 间逐跳转发，由 master 显式指定路径并使用内部测试数据验证；
第三、第四阶段再接入 Agent 计算路径、首末 socket 和真实业务。
本文阶段编号针对共享数据通道与路径执行，区别于历史探测/自适应路由预研的编号。

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
`node/inc/lnk_channel.h`、`node/src/lnk_channel.cpp` 和 `node/src/lnk_channel_flows.cpp`
中的 LnkChannel 直接持有 TCP/UDP socket、解析/发送队列和逻辑流分派状态，全部属于数据执行域。
帧类型与编解码位于 protocol/message。物理读写和逻辑流分派共用一个监控计时器，没有回调注册层。
跨域使用参数副本、post 和 co_spawn 完成事件，不共享可变 NodeLink 状态。
启动时同步绑定监听，任何监听失败直接回滚；随后 activate 将任务启动投递到数据执行域，
任务计数的增加与减少都在该域完成。UDP NodeLink 不创建 TCP socket 或 TCP 发送队列。

公共 `frame_io.h` 提供精确首帧读取和完整 datagram 长度校验；endpoint 直接使用 operator==/!= 比较。
Agent–Node 的 Pipeline/DatagramMgr 接入也调用这些函数，保留原有配对和限速行为。
Node 接入握手复用 CtrlMessage/WireMessage，普通数据使用 LnkFrameHeader，UDP 复用 DatagramHeader，不使用 TLS、TokenBucket、服务流量统计或双 socket 复制函数。

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

## 第二阶段：独立的 NodeFlow与逐跳转发（已实现）

### 目标与范围

保留第一阶段的共享通道：每对 Node 复用一条双向 TCP，UDP 使用固定 socket。
master 显式提交路径，一条逻辑逻辑流对应一条固定路径；中间 Node 按逻辑流和方向分派数据。
首末 Node 使用内部测试接口注入/接收，验证 A→B→C、A→B→D 共用 A–B 通道。

本阶段实现 Node 数据面的最小闭环，独立于 Agent 服务注册、业务 socket、限速与统计。
Agent 路径提交和真实 socket 桥接在第三、第四阶段接入。
NodeLinkMgr、LnkChannel、flow.* 命令和内部逻辑流接口已接入 RelayNode。
不在本阶段加入每流信用、公平调度、连接池、存量换路或完整的业务背压。

### 模块边界与执行域

| 模块 | 执行域 | 直接职责 |
|---|---|---|
| NodeLinkMgr | control_io | 成员/epoch 校验、相邻 Node 建连与复用、全路径准备/提交/关闭、回复关联及控制失效清理 |
| LnkChannel（node） | cluster_data_io | socket、首帧身份、物理读写链、队列、保活及本地逻辑流分派 |
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
| async_open_flow(path, transport) | 运行中的 master 输入有序 Node 路径和 TCP/UDP；使用当前 epoch，返回身份及建立阶段/原因 |
| async_send_flow(frame) | 输入身份、forward/reverse、DATA/FIN/RESET 和 payload；不能由调用者指定任意出边 |
| async_receive_flow(epoch, id) | 按逻辑流接收 DATA/FIN；RESET 或失效以异常唤醒等待者，避免多个逻辑流竞争同一个接收队列 |
| async_close_flow(epoch, id) | master 幂等关闭，等待全路径 closed，最多 10 秒；释放逻辑流状态并保留共享 NodeLink，失联/停止向等待者返回错误 |

发送结果明确区分 queued、would_block、closed、invalid。
queued 只表示入本地队列；would_block 必须未入队，且保留或返还 payload 给调用方，
不能部分接收一个帧，也不能要求调用方重发已经成功入队的数据。
本阶段遇到容量不足使受影响逻辑流失败并报告原因；未来可在相同发送边界加入异步可写等待。

### 步骤 2：最小逻辑流帧与统一入口

接入握手复用 CtrlMessage/WireMessage；逻辑流数据使用 LnkFrameHeader + 原始帧体。
TCP 通过已确认 socket 绑定 NodeLink 和对端；UDP 通过 DatagramHeader(NodeLink ID) 和固定 endpoint 校验入边，每帧校验 epoch。
逻辑流层只要求 flow_id、direction、DATA/FIN/RESET 类型以及对应 payload/原因。
DATA 为最多 4096 字节的原始二进制 payload；测试序号放在测试 payload 中，不成为必需业务协议字段。
Node 逻辑流标识独立于 Agent 的现有 UDP session 标识；UDP 外层 DatagramHeader 仍标识相邻 NodeLink，
不能用它代替跨整条路径的 flow_id，也不能直接透传 Agent datagram 头作为 Node 头。
FIN 表示一个逻辑方向结束，RESET 表示该逻辑流失败，均不关闭承载其他逻辑流的物理 TCP。
UDP DATA 保留 datagram 边界，不提供可靠投递、排序或重传；UDP 关闭通过逻辑流控制流程完成，
不依赖 UDP FIN/RESET 一定送达。

forward 沿提交路径，reverse 沿原路径返回，与 TCP 发起方 Node ID 排序无关。
当前静态路径先验证无重复 Node/无环，每段严格验证预期入边和邻居，不增加 hop_limit 或换路版本状态。
转发保持逻辑流身份、方向、帧类型和内容；UDP 将外层 NodeLink ID 替换为出边身份。

物理解析完成后只有一个逻辑流帧分派入口，直接调用同数据域的 LnkChannel。
DATA/FIN/RESET 直接进入 FlowFrame 分派；PING/PONG 由通道处理。
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
FIN 后拒绝该方向的新 DATA，另一方向仍可继续；FIN 后仍允许 RESET。
RESET 释放该逻辑流并以失败原因唤醒终点等待者。
此处先验证逻辑事件传播，真实 socket 的 shutdown_send/排空在第四阶段由首末桥接实现。

### 步骤 5：容量、故障与停止

- LnkChannel 保持每条 TCP 一个读链、一个写链；发送队列有界，并为通道保活帧保留少量容量。
- LnkChannel 的活动表项加关闭去重记录最多 1000 条，每逻辑流终点队列最多 16 帧，
  全模块终点暂存最多 8 MiB（payload/reason 字节加每帧固定开销）。中间转发直接入物理队列，
  TCP 每 NodeLink 和 UDP 全局队列保留 4 个名额给控制帧，DATA/FIN/RESET 的准入上限为 96。
  容量不足返回 would_block/逻辑流错误，不无限增加 detached 协程或隐藏缓冲。
- 逻辑流拥塞、逻辑关闭和畸形逻辑流帧不走 fail(link)。最小版不承诺慢逻辑流完全不影响其他逻辑流的延迟，
  也不把拥塞失败处理当作信用控制或业务背压。
- 物理 NodeLink 断开时，依赖它的逻辑流全部失败；控制连接失效、成员离线/地址变化和 epoch 改变同样清理。
  已入物理队列或网络的旧帧由终点逻辑流身份/状态验证拒绝，不重新建路。
- stop 先拒绝逻辑流请求并取消控制事务，再清理转发表/队列/等待者，
  最后按现有顺序排空通道数据任务、停止集群控制连接、join 数据线程。
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
| node/inc/lnk_channel.h、node/src/lnk_channel.cpp、node/src/lnk_channel_flows.cpp | 共享 TCP/UDP、直接逻辑流分派、容量及统一监控 |
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

第二阶段验收后，Agent 路径计算返回选中的完整路径，并将路径、拓扑 epoch、目标服务/协议和请求信息
发给入口 Node，再转给 master。入口需要建立或复用 Agent 控制连接，不能继续固定使用服务 Node 的连接。
master 除第二阶段的路径校验外，还验证服务所在 Node、请求者控制逻辑流及访问关系，并调用既有建路入口。
计算结果只有一个 Node 时沿用该 Node 的现有单节点 Relay，无需安装跨 Node 路由。

补充首末端接入准备和 ticket/逻辑流身份绑定：请求方 Agent 接入首节点，服务方 Agent 接入末节点。
复用现有 relay.offer/opened/attach 流程与解析，在 Node 路由就绪并且首末接入完成后才允许业务 DATA。
Agent 路径计算、建路失败和服务失效的结果回到原请求者，由外层决定是否重新选路。
本阶段先完成控制与接入闭环，后续真实流式转发使用第四阶段的业务语义。

## 第四阶段：真实业务转发与回归

把 Agent–Node socket 绑定到已准备的 Node 逻辑流，形成“本地 socket ↔ 逻辑流”的读写桥接。
TCP/TLS 业务补齐接入确认、DATA/FIN/RESET、socket 半关闭、取消及必要背压，
不能将第二阶段有界队列的拥塞失败处理直接当作真实流式业务的流控。
公平调度、连接池和存量换路按后续单独任务实施，首末桥接通过逻辑流接口协作。
TLS 继续用于现有 Agent–Node 接入，Node 间传输使用共享明文 TCP；UDP 接入保留 session 和源 endpoint。

保留首末端原有服务限速和流量统计；中间 Node 只做校验和分派。
验证真实应用、分叉回程、多服务、多流、半关闭、局部断链和事务回滚，再让新连接实际采用计算路径。

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
  已关闭身份不能由 prepare/commit/refresh 复活，FIN 后 RESET、准入满时完整返还 payload，
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

发送队列直接拥有固定头和原始载荷，TCP 使用两个 buffer，UDP 使用三个 buffer，一次发送完整帧/datagram。中间节点只解析头并移动载荷到出边队列，首末接收队列也移动 DATA 载荷；不为每跳生成 JSON 对象、编码 CBOR 或拼接整帧副本。接收仍需要独立缓冲区，RESET 原因仍有小量字符串复制，不宣称系统级零拷贝。拒绝准入时完整返还未发送载荷，继续保持原有队列容量、保活、租约、FIN/RESET 与停止语义。

数据线格式已变更，Node 需要统一升级；没有旧数据格式兼容分支。本次未接入 Agent 业务桥接或增加流控/调度层。

本轮验证：Debug/Release 全目标构建成功，二进制协议及新增/指定回归均 14/14 通过，总用时分别 136.64 / 131.09 秒。node_frame_protocol 检查固定字节序、版本、类型、标志、长度与字段组合；node_links 检查旧 epoch、错凭据/身份/版本、UDP 长度不匹配、完整 4096 字节 TCP 数据及非法 TCP 长度提前失败；node_sessions 在 TCP/UDP 分叉路径验证双向完整载荷和空 DATA。既有容量拒绝、FIN/RESET、保活、租约、停止清理及 Agent/Relay 回归继续通过。

日志：build/node-binary-debug-tests.log、build/node-binary-release-tests.log。git diff --check 通过。本轮未重跑已记录的三项基线失败，未做吞吐基准或 Linux 多机验证；这些功能测试不用于宣称确定的吞吐提升。

## 命名统一与冗余清理（2026-10-07）

保留 NodeFlow 的原始领域含义：数据域表项使用 LnkChannel::NodeFlow，控制域路径协调器使用 NodeFlows。
不引起歧义的辅助类型统一为 FlowFrame、FlowResult、FlowSendResult、FlowSendStatus，以及 LinkResult、LinkStatus、LinkData。
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
