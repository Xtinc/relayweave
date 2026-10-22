# RelayWeave

本文是项目的编码风格约束，适用于贡献者和 AI 编码工具。新增与修改代码应遵循本文，异步代码参考
`protocol/src/xfr_channel.cpp` 和 `protocol/src/tls_channel.cpp` 的既有风格。

系统、Agent、链路质量与推荐路径见 [RelayWeave 设计](docs/RelayWeave设计.md)；
Node 的部件结构、完整业务流程、配置与协议集中见 [RelayNode 设计](docs/RelayNode设计.md)，
执行域见其中的[部件结构与执行域](docs/RelayNode设计.md#1-部件结构与执行域)，
内存分配、协程数量及性能复核见[资源模型与验证](docs/RelayNode设计.md#8-资源模型与验证)。
测试分组与运行入口见 [测试目录](test/README.md)。
RelayNode 直接持有 node/nodelink_mgr，统一协调 NodeLink 建连与 NodeFlow 路径事务，层次与 Pipeline/DatagramMgr 一致。
NodeLinkMgr 直接持有 protocol/FlowProtocol；协议服务持有 lnk_channel 的邻接 TCP/UDP 传输并统一管理 Flow。
部件通过 FlowStream/FlowDatagram 收发，层次与接口见 [设计文档中的协议边界](docs/RelayNode设计.md#flowprotocol端点与本地逻辑流状态)。
protocol/message 定义 Node 固定二进制帧，业务载荷不经过 JSON/CBOR。
控制通知使用有界协程收发接口，数据域直接逐跳分派，不通过模块回调注册。
Flow 提交后不做周期续租；控制连接断开、epoch/路径成员失效、NodeLink 断开或显式关闭时清理。
超时仅用于建立阶段和关闭确认，NodeLink 与集群控制连接使用各自的心跳检测失联。
NodeSession 按所选路径直接持有 ControlRouterSingle 或 ControlRouterMulti，两者都在 control_io 协调业务；多跳首尾通过集群单播通信。
RelayNode 直接持有唯一 RegistryMgr 和 NodeSession 容器，负责全局业务创建、查找分派和停止排空；控制器只推进自己的实例。
NodeControl 借用注册表、拓扑和业务元数据，统一处理公共注册、查询与集群响应回送；NodeMetrics 拥有队列探针、流量采样和运行指标。
两者统一声明和实现在 `node_control.h/.cpp` 中，保持各自的所有权和执行域。
重复请求、peer 身份匹配、控制断线与 epoch 失效判断通过 NodeSession 交给控制器，RelayNode 不读取控制器内部状态。
master 只保留 NodeLinkMgr 原有建路职责，不保存中继业务状态。
Agent 支持最佳路径提交、首 Node 定位及首末接入；首末 socket 直接桥接 NodeFlow，双方绑定后发送 relay.ready。
Agent 服务发现只保存目的 Node 位置，选路后才取得实际入口控制连接；发现继续使用主控，不提供主控离线定位。
Forwarder 拥有长期本地监听和业务容器，AgentSession 统一一次 TCP/TLS/UDP 接入与复制；UDP 服务离线时只保留本地 forward。
单跳和多跳 TCP/TLS 均保留双向半关闭，UDP 单节点用户载荷上限为 65499 字节、多节点为 65467 字节；服务限速和统计保留在端点，
共享通道、路径事务、数据热路径和生命周期统一见 [RelayNode 设计](docs/RelayNode设计.md)。
多节点 TCP/TLS 使用逐跳双向信用窗口，拥塞时等待；UDP 满队列丢包而不关闭健康 Flow。
固定窗口、预算和协议升级边界见 [共享数据面容量与信用算法](docs/RelayNode设计.md#5-逐跳信用流控与容量)；
TCP 共享发送队列按流标签交错插入，算法、接口与实现比较见 [FairQueue](docs/FairQueue.md)。
部署见 [打包与安装](docs/RelayWeave打包与安装.md)、[证书制作与部署](docs/证书制作与部署.md)
和 [管理面板部署说明](dashboard/README.md)。

## C++ / Asio 编码约束

- 类与结构体说明、对外接口统一使用 Doxygen 风格的英文注释（`/** ... */`），以 `@brief` 概括职责或行为。
  类说明应交代所有权、依赖生命周期和执行域；有参数的对外接口必须逐个使用 `@param` 说明参数含义、单位、有效范围或所有权，
  有返回值的对外接口必须使用 `@return` 说明结果含义及失败或空值情况，协程说明等待完成后的结果。
  私有接口及实现内部类的接口可使用简短的 `@brief`，无需逐项列出参数和返回值；保留必要的执行域、生命周期与取消约束。
  按需使用 `@pre`、`@throws` 说明调用前提、异常、取消与排空语义，模板参数使用 `@tparam`；不逐字段或逐语句复述代码。
  注释集中在声明处，实现在 `.cpp` 内的局部类也遵循同一规则；覆盖的接口可使用 `@copydoc` 复用基类契约。
  移除零散的行内、语句旁及分段说明注释；必要的设计原因和执行器约束归入对应类或接口的 Doxygen 注释，
  跨模块流程、容量推导和历史验证记录按职责归入设计文档：Node 相关内容由 `docs/RelayNode设计.md` 维护，
  系统、Agent 与通用约束由 `docs/RelayWeave设计.md` 维护，避免多处维护同一说明。
- 每条普通变量声明只声明一个变量，并独占一行，适用于局部变量、成员变量和全局变量。
  即使类型相同，也不得使用逗号连写，例如 `bool left_ready = false, right_ready = false;` 应拆成两行独立声明。
- 头文件避免重复包含应使用防卫式声明，不使用 `#pragma once`。
- 模块间尽量避免使用回调函数或函数包装器解耦合。对于生命周期具有明确所属权的对象，优先直接持有或引用；
- 异步控制流使用 C++20 协程，优先使用 `asio::awaitable`、`co_await` 和 Asio 协程组合，避免回调链。
- 谨慎使用 `asio::co_spawn` 启动协程链。项目可控的内部协程调用应在注释中说明所需执行器，并直接
  `co_await`；需要独立协程链或跨执行器边界时才使用 `co_spawn`。
- 不使用 Impl/PImpl 模式。类必须声明并直接持有依赖和状态，不通过不透明的 `Impl`、`Implementation`、
  `Private` 或类似的堆分配实现对象隐藏它们。唯一例外是 ABI 边界，且必须获得明确的项目级批准。
- 异步失败需要以异常传播时使用 `asio::use_awaitable`。协程需要检查预期错误时，定义并使用
  `asio::as_tuple(asio::use_awaitable)`，通过结构化绑定解包结果。不使用 `asio::redirect_error`。
- 简单操作超时优先使用 `asio::cancel_after(..., asio::use_awaitable)`。只有定时器同时承担共享状态、
  协议截止时间或唤醒信号时，才使用显式定时器。
- 仅由单个协程使用的状态可以放在该协程帧中。其他协程或回调无需查找、访问的定时器、缓冲区、端点、
  临时消息、截止时间和结果可以使用局部变量。
- 被其他协程、回调、查找表或独立异步操作反复引用的状态，必须由类或其所属容器直接持有。不得将协程帧中
  对象的指针或引用发布出去供后续查找。优先使用 `std::map<Key, Session>` 等按值持有对象的容器；
  只有确实共享所有权时才使用智能指针。
- 保持所有权和生命周期清晰。分离运行的协程必须由存活时间更长的对象拥有，关闭时能够取消，异常出口能够
  妥善处理。关闭操作必须幂等，并在销毁共享资源前等待所有未完成协程结束。
- 受执行器约束的状态只能在其所属执行器上访问。`co_await asio::dispatch(..., asio::use_awaitable)`
  不会改变当前协程的关联执行器，不得用它代替将协程链绑定到所需执行器。已由单线程 `io_context`
  管理的状态不得额外添加 strand 或锁。
- 将取消作为状态机的一部分，区分预期取消、超时、EOF 和真实 I/O 失败，不得静默合并为同一种结果。
- 主流程使用抛出异常的操作；预期且需要分支判断的结果，以及析构函数或 `noexcept` 函数中的尽力清理，
  使用 `asio::error_code` 重载。
- 协程循环保持线性，便于局部阅读。使用提前 `co_return`、结构化绑定和显式状态检查；只有所有权或对象大小
  确实需要时才引入堆分配和间接访问。
- 控制流语句的主体必须使用花括号，包括单条语句，以免后续修改异步路径时产生歧义。

## 日志规范

- **INF**：记录监听、控制连接、服务注册与发现、业务连接建立与断开、停止等关键进展。仅查看 INF 应能大致
  还原业务流程；Relay 建立记录包含协议、角色、服务和会话标识，控制连接与代理连接保留必要端点。
- **DEB**：补充连接尝试、TLS 协商、请求关联、路由候选、关闭原因等诊断细节。每条记录应提供额外信息，
  避免将 INF 的内容换个说法再打印。候选路由的汇总、排名、路径与成本属于 DEB。
- **ERR**：记录真实失败，包含失败操作或阶段、原始错误原因，以及可用于关联业务的服务、端点、请求或会话
  标识。异步任务的异常出口不得省略原始异常；预期取消、正常 EOF 和正常关闭不作为 ERR。
- 日志以简短文字描述事件，以 ASCII 符号辅助表达关系。`[+]` 表示连接已经建立，`[x]` 表示连接已断开，
  `->` 表示目标或路径，`<->` 表示双向连接。错误由 `[ERR]` 前缀区分，不添加感叹号错误标记。
  监听、服务注册和进程停止等事件直接使用文字，不借用连接建立或断开标记。
- 使用项目日志宏，保留 `[INF]`、`[DEB]`、`[ERR]` 和函数、行号前缀；每条记录独立成行并及时刷新输出。
  路由候选逐条记录，例如 `Route #1 cost=109.285 service=web/tls: agent -> node-a -> node-b *`，
  不输出脱离级别与上下文的多行列表。`cost` 放在排名之后，固定小数点后三位；推荐候选仅在行末加 `*`，
  其他候选不加标记。`*` 表示推荐候选，不表示该业务连接已采用此路径。
- 同一连接的建立和断开使用 `[+]`、`[x]` 配对，不再重复写 `connected`、`closed`；
  仅建立成功后才记录对应的断开。
  正常结束、异常退出和停止清理均应覆盖，避免遗漏或重复。两条记录的公共字段顺序与值保持一致。
  Agent Relay 固定按“协议、角色、`service`、`uuid`”排列，断开原因 `reason` 放在最后；
  Node Relay 按“协议、`service`、`uuid`”排列；控制连接按节点或会话标识、`peer` 排列；
  代理连接按协议、`peer`、`target` 排列。
- Relay 通过 `service`、`uuid` 关联业务，正常流程不附加 `server`、客户端临时源端口或转发连接地址，
  也不为省略的详情另加一条 DEB 重复记录。连接入口可能只是跳板，不能用它表示服务所在节点或完整路径；
  路径展示以路由计算结果为准。控制连接的 `peer` 表示对端，代理连接的 `peer`、`target` 分别表示客户端与
  业务目标。失败诊断仍保留失败阶段、相关 I/O 端点与原始错误。
- 使用简短事件名和必要的 `key=value` 字段。复用已有的 `id`、`host`、端点、请求 ID、Relay ID 等关联信息，
  不为日志新增冗余字段、结构体、统计状态或复杂调用链。独立记录可重复必要的关联标识，避免重复同一份详情。
- 同一次事件的相同信息只记录一次。异常已向调用方传播时，由能提供完整业务上下文的一层记录 ERR；自行处理
  或分离运行的任务必须记录自身错误。INF 可记录连接断开的概况，完整失败原因集中在 ERR。
  高频事件应控制打印频率，路由缓存命中不重复打印相同候选列表。
- 连接日志记录实际端点，路由日志记录计算结果，不添加临时实现阶段的模式标签。

格式示例：

```text
[DEB] ... TLS handshake complete role=client peer=192.0.2.10:18443 version=TLSv1.3 cipher=TLS_AES_256_GCM_SHA384
[INF] ... Control [+] node=node-a peer=192.0.2.10:18443
[DEB] ... Routes service=web/tls -> node-b candidates=1 max_nodes=4
[DEB] ... Route #1 cost=109.285 service=web/tls: agent -> node-a -> node-b *
[INF] ... Relay [+] tls consumer service=web uuid=659
[INF] ... Relay [x] tls consumer service=web uuid=659 reason=I/O ended
[ERR] ... Relay open failed tls service=web request_id=42 uuid=0 reason=service unavailable
```
