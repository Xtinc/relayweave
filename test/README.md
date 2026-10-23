# 测试目录

测试按被验证的组件分组；需要完整 Agent、Node 或 Proxy 链路的场景归入 `integration/`。
测试夹具、私有状态访问和参考实现只存在于测试目录，不给业务类增加测试接口。
Agent 路由测试通过夹具检查私有候选计算；生产接口只提供目的节点选路，不为测试保留探测模式或全目的候选查询。

| 目录 | 内容 | CTest 标签 |
|---|---|---|
| `common/` | 双索引容器、LRU、缓冲池、AsyncEvent、令牌桶、流量计数 | `common`、`unit` |
| `protocol/` | 控制/二进制编解码、mTLS、FlowProtocol/NodeLink、Stream/Datagram 端点、TCP/TLS/UDP 复制 | `protocol`，按用例区分 `unit` / `integration` |
| `route/` | 链路质量、路径算法、DNS 探测生命周期、真实 ICMP | `routing`，真实 ICMP 另标 `icmp` |
| `agent/` | Agent/Session/Forwarder 生命周期、控制连接池、路由组装和缓存 | `agent`，路由相关另标 `routing` |
| `node/` | 配置/统计、启动停止、集群控制、共享 NodeLink、Flow、UDP session 路由、拓扑 | `node`，集群相关另标 `cluster` |
| `proxy/` | HTTP、CONNECT、SOCKS5、容量、超时和监听回滚 | `proxy`、`integration` |
| `integration/` | 单/多节点 TCP/TLS/UDP、恢复、迁移及 Proxy 完整链路 | `end_to_end`、`integration` |
| `deployment/` | Deb 控制脚本换行与执行、能力校验、证书校验与配置 dry-run | `deployment`、`integration` |
| `tools/` | ICMP/TLS 手动诊断、真实 Dashboard 服务 smoke | 手动运行，不注册 CTest |
| `support/` | 公共夹具、资源定位和容器对拍参考实现 | 不单独运行 |
| `data/` | 测试证书和配置样本 | 只供测试使用 |

`unit` 用例不要求网络权限；使用真实 socket、跨执行域异步交互或进程的用例标为
`integration`。组件标签与运行类型可以组合筛选。Dashboard 的 Python 测试保留在
`dashboard/`，与该模块的 Python 导入和依赖环境一起运行。

## 构建与运行

```console
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build --config Debug --parallel 1
ctest --test-dir build -C Debug --output-on-failure

# 仅纯单元测试 / 某个组件 / 完整业务链路
ctest --test-dir build -C Debug -L unit --output-on-failure
ctest --test-dir build -C Debug -L protocol --output-on-failure
ctest --test-dir build -C Debug -L end_to_end --output-on-failure

# 普通路由测试和需要原始 socket 权限的 ICMP 分开运行
ctest --test-dir build -C Debug -L routing -LE icmp --output-on-failure
ctest --test-dir build -C Debug -L icmp --output-on-failure

# 部署测试可独立运行；Linux 有 Python3 时也自动加入 CTest
python3 -m unittest discover -s test/deployment -p '*_test.py' -v

# Dashboard 的环境须安装 dashboard/requirements.txt
python3 -m unittest discover -s dashboard -p 'test_*.py'
python3 test/tools/dashboard_service_smoke.py --build-dir build --two-nodes
```

每个自动测试都有超时。`probe_integration` 只在原始 socket 权限不足时返回 77 并跳过，
其他失败仍报错；跳过不能替代具有 CAP_NET_RAW 权限的验证。
证书和配置统一从源码的 `test/data/` 读取，移动测试执行文件无需复制一份资源；
路由 HTML 报告等运行产物写入构建目录。

手动诊断工具位于构建目录中对应的分组：

```console
build/test/tools/test_icmp 127.0.0.1 4
build/test/tools/test_tls_channel_peer server 18443
build/test/tools/test_tls_channel_peer client localhost 18443
```

多配置生成器的可执行文件可能位于分组下的 `Debug/` 或 `Release/` 子目录；CTest 自动定位它们。
性能基准的构建、运行和产物目录见 [benchmark/README.md](../benchmark/README.md)。

## 回归边界与整理原则

- Agent 连接池（`node_connection`）：真实 mTLS 后仍等待节点识别；就绪连接不创建等待或分配请求编号、Node/端点复用、身份冲突、活动引用防回收、60 秒空闲边界、停止期间复用旧索引、任务完成后重建、Agent 停止排空，以及连接句柄晚于 Agent 释放。
- Agent 停止（`agent_lifecycle`）：所有者线程顺序调用阻塞 stop，等待控制侧与业务侧排空；完成后重复调用直接返回；排空失败保持 Stopping、保留元数据，解决失败后可再次调用完成排空，最终释放 Agent。
- Agent 进程（`agent_process`）：真实可执行文件处理 SIGTERM/SIGINT 后阻塞停止并释放监听；启动失败时仍排空、join worker 并返回原异常。
- Agent 入口（`agent_session`）：回复同时关联 primary 来源、request_id、node_id、原截止时间和取消状态；Session 本地取消保留入口预算，整体停止立即唤醒并排空。


- 公共控制（`cluster_integration`）：本地/远端发现、状态与拓扑查询、认证来源与路由字段清理、错误 requester 和关闭会话的迟到响应；重启后重复验证。
- 运行指标（`node_metrics`）：启动前的无效队列值和运行时间、三个执行域采样、控制探针同时采样服务流量、转发字节计入可用率、分域取消后回调释放且不重调度；`node_flows` 继续验证真实转发计数。
- 关闭（`node_shutdown`）：运行故障不污染后续正常关闭结果、关闭返回 false 后仍排空其余模块、并发/重复结果一致、无法排空的异常保持 Stopping 并可重试、取消及启动失败清理。
- 进程（`node_process`，Unix/Python）：真实 SIGTERM、未完成 TCP/TLS setup、启动失败退出码与端口重绑，验证线程 join 收尾。

- TCP 复制：两侧读/写失败和限速等待中的对向失败必须排空；正常 EOF 保留反向回传，校验原始异常和实际流量。
- UDP 复制：零长度、4097 字节及最大合法载荷双向保持原样；短头、错误 session、超限包丢弃后仍能处理后续包；两侧 I/O 失败都能排空。
- Flow 流控（`lnk_flow_control`）：按本地 TCP 准入、数量上限、UDP 丢包、信用批次、真实 wire 和三跳恢复归类。
  批次检查直接驱动真实端点的 `poll(now)`，覆盖初始授信、20 ms 精确期限、跨 Flow/方向共享、阈值/耗尽即时刷新、待发合并与关闭，不依赖 sleep。
  满 DATA 窗口仍能排入唯一 FIN；数量场景覆盖 TCP、UDP、混合各 1000 Flow、第 1001 个拒绝和退休身份到期后复用；固定并发重复建立、传输和关闭时记录 RSS 趋势。三跳停读保持有界，恢复保持 DATA/FIN 顺序，健康流继续推进。
- 业务任务准入（`node_relay_capacity`）：暂停数据执行域时，按 TCP/TLS/UDP 各自的 `max_relays` 限制建立中及关闭未排空任务，排空后恢复名额。
  同时覆盖端点安装前的重复请求、参数冲突、peer 身份匹配、关闭中消息消费，以及控制断线、epoch 和集群失败的实例取消范围。
  `relay_paths` 验证额度饱和后新请求被拒绝，已有请求仍重发完整且一致的端点与票据，冲突仍返回原校验错误。
- 缓冲池（`pooled_buffer`）：独占移动与切片、稳定信用计费、资源跨线程生命周期；两个线程分别预热后，20,000 次取块归还不增加上游分配。
  分配计数仅在该测试可执行文件中拦截全局分配；实际分配失败通过测试侧资源验证，不向生产池增加故障接口。UDP 拥塞通过拒绝准入和健康 Flow 继续收发验证。
- 端点交付（`lnk_flow_receive`）：保持调用方执行器及载荷地址；socket 写完成通知后才归还信用；关闭、取消唤醒挂起交接并排空任务，迟到通知不复活 Flow，业务缓冲可晚于协议销毁。
  单向 FIN 保留反向传输；FIN 后对向错误保留真实原因，接收失败取消信用等待中的发送者。
- 共享 Link（`lnk_channel_flows`、`node_links`）：并发响应、FIN/RESET、准备期限、Flow 隔离、物理 writer 失败取消 reader 并清理依赖 Flow。
  跨流交错后按被测 Flow 的接收计数等待积压到达；Link 关闭检查本队列排空、相邻队列关联帧删除，以及健康 Flow 的帧和信用保留。
- 公平队列（`fair_queue`）：三种候选与生产版本的独立模型对拍、穷举/随机轨迹、有限状态进展、只可移动数据、哈希冲突及 Frame 所有权；`lnk_flow_control` 的 wire 场景验证真实写出交错和关闭隔离。
- 多节点 UDP：65467 字节载荷在共享 Link 和多跳路径中双向完整转发；65468 字节在接入时丢弃；最大长度的非法帧不会妨碍后续合法帧；各处队列满立即丢包，不创建 stream 状态或消费确认。
- 单节点 TLS 和多节点 TCP/TLS：请求半关闭后，服务方仍能完整返回超过两个最大 DATA 载荷的连续字节流，并正常传递反向 EOF。
- TLS 1.2/1.3：DATA/FIN 业务半关闭、反向排空、仅载荷流量计数；未收到业务 FIN 的异常截断及非法长度必须失败并排空对向。
- 限速与计数：快路径不能重复扣费，失败试扣不能吃掉令牌，预留债务必须累积，空包与无限速不等待；并发计数/采样不丢字节，计数溢出饱和。
- 控制与生命周期：并发/重复停止、取消后排空、超时、旧 request/epoch、断线恢复、服务迁移及对象释放继续保留独立回归。
- Relay 数量准入同时验证入口/出口共用多跳额度、关闭未排空仍占用额度、排空后恢复，以及多跳数量上限不限制单跳准入；协议额度仍统计两类业务。
- 保留旧配置拒绝用例：`node_config_legacy.json` 验证旧字段不能绕过当前配置校验。生成的配置放在临时目录，先验证有效基线能通过，再逐项检查非法字段。
- UDP 集成测试删除旧 `PROXY_UDP_9000_PPS` 分支，容量测量由独立 Node 基准承担；复制层回归从业务集成用例移入协议组。
- 路径缓存测试删除与业务行为无关的日志等级/措辞断言；编解码测试不再混入限速和计数测试。

新增测试应验证可观察的业务结果、边界或真实故障，优先使用协议对端和公开接口。
必要的状态构造或检查放在 `support/` 或测试翻译单元中；不为测试增加业务友元、开关或诊断接口。

公平队列的手动 Release 容器对比见 [FairQueue](../docs/FairQueue.md)。`fair_queue_compare` 与
`fair_queue_memory` 分离计时和分配计数，不加入 CTest 或既有五个通道 benchmark。
