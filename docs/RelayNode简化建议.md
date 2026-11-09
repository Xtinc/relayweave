# RelayNode 流程与结构简化建议（代码核查修订版）

核查日期：2026-11-09。本文对照当前 `node/`、相关 `protocol/` 实现、测试源码、[README](../README.md) 与 [设计文档](RelayWeave设计.md) 重新评估原建议，保留原编号便于追踪。本文记录重构建议及后续实施状态；F6 已按本版方案修改，其余条目仍为建议，验证结果见第 7 节。

**结论：优先把状态归属、输入转换和清理路径写清楚。** 原文识别出了真实的重复与维护负担，但“减少行数、减少接口、减少状态枚举”不能直接等同于流程更简单。部分方案会增加隐藏分派、无效状态组合，或删除必要的完成确认与认证校验，不能照原方案实施。

## 1. 判断标准与核查边界

按以下顺序判断一项重构是否成立：

1. 读一条业务流程时，能否直接看出谁持有状态、在哪个执行域操作、何时完成、失败后谁清理。
2. 能否减少同一事实的重复存储、重复字段转换及隐式约定；新增的索引、参数、虚函数和同步规则是否更少。
3. 能否保持协议、身份校验、半关闭、原始期限、错误首因、关闭幂等和排空语义。
4. 最后才考虑代码量、分派复杂度和分配次数。性能优化须单独说明收益，不能以其代替流程简化依据。

收益按上述目标分为高、中、低；可行性考虑改动范围、已有验证入口及语义变化。排序先处理所有权约束问题，再按收益与可行性综合安排，不把所有建议都列为待办。

核查范围为 `node/inc/*.h`、`node/src/*.cpp`，并追踪 `AsyncEvent`、`TLSChannel`、`FlowProtocol`、`DualIndexMap` 与相关测试。核查基线的 Node 源码共 **8246 行**，原文这一度量成立；后续实施会改变行数。`DualIndexMap`、其测试及参考实现另有 **1027 行**，不能把删除测试的行数当作业务流程简化收益。原文各项净减少行数没有实现或测量支撑，本版撤销这些预测，也不叠加彼此重叠项目的收益。

保留设计文档 2.1、3.3、3.4、8.1 及附录 C 的执行域、所有权和停机约束。附录 D.2.1 的索引优化仍是未完成项；D.4 的 pending socket 认领观察与准入/排空计数仍须分别处理，不并入接口收敛后视为已经解决。

## 2. 按收益和可行性排序的实施建议

下表只列有明确实施价值的范围。P0 可先做，P1 需要独立验证，P2 是有触发条件才做的后续项；表内顺序即建议推进顺序。

| 顺序 | 原编号与修订范围 | 对流程清晰度的收益 | 可行性 | 批次 | 结论 |
|---:|---|---|---|---|---|
| 1 | F6：`PendingOpen` 由管理器容器直接持有 | 高：消除对协程帧地址的登记，状态归属明确 | 高 | P0（已实施） | 按值持有，保留已提交与已接受的分别判断 |
| 2 | D1：拓扑链路公共字段编解码集中 | 中：字段和范围约束各有明确入口 | 高 | P0 | 保留 report/snapshot 的上下文差异及边界校验 |
| 3 | D4：明确 JSON → Params → Spec 的单向转换 | 中：接收校验与数据域投影分开阅读 | 高 | P0 | 不虚构 Params/Spec 双向等价关系 |
| 4 | D6：UDP 配置解析提取为完整函数 | 中：每个配置段就地完成解析与校验 | 高 | P0 | 保持 UDP 特有 burst 校验 |
| 5 | D7：提取 TLS context 的共同配置步骤 | 低至中：共同初始化步骤集中维护 | 高 | P0 | 显式传入不同 CA，保留上下文用途 |
| 6 | S9 的小范围：命名重复 Link 容量常量 | 低：明确硬编码上限的含义 | 高 | P0 | 新配置项、统一期限另行评估 |
| 7 | S4：用业务专用普通容器替代通用双索引模板 | 高：可删除仅为一个使用者存在的通用机制 | 中 | P1 | 必须补齐按 owner 清理与异常回滚，详见 3.2 |
| 8 | S1：局部消除本地/远端配对实现重复 | 中：缩短重复准备和清理代码 | 中 | P1 | 优先保留类型明确的入口，不先造 `PairHandle` |
| 9 | F1 的计数部分：按 TCP/TLS、UDP、多跳维护准入计数 | 中：容量规则集中；另有性能收益 | 中 | P1 | 完成排空才归还名额，不能与数据域计数混用 |
| 10 | S5：仅提取重复的报文字段构造 | 低至中：减少字段维护点 | 高（限小范围） | P2 | 不上移差异状态，不新增逐阶段虚钩子 |
| 11 | F10：控制域采样职责显式化 | 低：不再由 timer 地址暗示职责 | 中 | P2 | 不增加跨域回调或额外计时器；否则保留现状 |
| 12 | F13：加入失败后有界发送错误并断开 | 中：去掉“再读一帧”推进收尾的约定 | 低至中 | P2 | 需先解决发送完成保证，不能直接 send 后 disconnect |
| 13 | F1 + F2 的限缩版：索引选候选，会话继续校验与处理 | 中：路由直接；高并发性能收益可能较高 | 中至低 | P2 | 基于分派开销测量再实施，不删除认证与业务判定 |

S4 虽然结构收益较高，但比字段整理多出容器一致性与删除语义的验证成本，因此放在 P1。索引会新增维护规则，优先级低于直接消除隐式生命周期与重复字段的工作。

## 3. 主要建议的正确实施边界

### 3.1 F6：修复 PendingOpen 的状态归属，优先于共享指针化

**实施状态：2026-11-09 已完成，14 项 Node 回归全部通过，详见第 7 节。** 以下保留问题依据及实施边界。

**核查结果：问题成立，原方案须修正。** 原实现的 `NodeLinkMgr::open_flow()` 在协程局部创建 `PendingOpen request(control_)`，将 `&request` 放入 `pending_opens_`；成员变化、控制失败和远端结果处理随后按 ID 查找它。原实现依靠同域处理及 `ScopeGuard` 在局部对象析构前撤销登记，不能仅据此宣称已有悬空访问；但这种方式违反 README 中“不得将协程帧中对象的指针或引用发布出去供后续查找”的明确约束。

现使用 `std::map<std::uint64_t, PendingOpen>`，通过 `try_emplace(id, control_)` 原位构造。`open_flow()` 借用容器内记录，等待结束后由同一清理范围移除；通知处理只查找并更新该记录。`PendingOpen` 内的 `AsyncEvent` 不可移动，不应构造临时值后再移入容器。

必须保持“已登记 → 发送请求 → 等待结果 → 判断接受 → 统一撤销及必要的远端关闭”的归属关系，并覆盖发送失败和取消。只要等待协程仍借用记录，其他失败处理就只能填写结果并唤醒，不能提前删除该记录；管理器也必须活到等待退出。

原文建议的 `shared_ptr` 没有证明存在容器之外的独立存活需求，不作为默认选择。`accepted` 也不能直接以“结果成功”替换：远端成功结果可能已写入，但本端等待仍因取消而退出，此时尚未接受结果，仍需要请求远端清理。保留显式接受标志，或让接受/移交动作本身表达这一事实。

依据：[`nodelink_mgr.h`](../node/inc/nodelink_mgr.h) 的 `PendingOpen`、`pending_opens_`；[`nodelink_mgr.cpp`](../node/src/nodelink_mgr.cpp) 的 `open_flow`、`handle_flow_request`、`fail_waiters`。验证重点是取消/超时、成功与取消竞争、发送失败、迟到回复，以及未接受成功结果后的远端释放。

### 3.2 S4：普通容器确实可行，但原文漏掉了按 owner 删除

**核查结果：唯一生产使用者判断成立，原替换方案不完整。** 全仓生产代码只有 `RegistryMgr::services_` 使用 `DualIndexMap`。现有模板同时提供按服务查找、按会话计数和按会话删除；原文的 `services_ + service_counts_` 只能直接解决前两项，无法定位一个会话名下的全部服务，删除并不会自动变为“单索引操作”。两张普通 map 也仍需维护一致性，不能断言天然更安全。

以流程简单为目标，优先考虑最小实现：

- 服务表按名称保存 `{owner, protocol, traffic}`；控制会话表继续保存会话。
- 注册时扫描服务表统计 owner 名下数量，断开时 `erase_if(owner == id)`；查询从服务条目直接取得 owner。
- 当前配置解析将全局服务数限制为 256，低频注册/断开采用有界扫描有合理性。服务输出若使用有序 map，可直接按顺序遍历，不再先收集名称、排序后逐项回查。

如果实测要求按 owner 高效删除，则在控制会话记录中持有其服务名集合，集合大小同时作为计数，不再另设计数 map。插入服务与更新 owner 集合必须处于同一回滚范围。这个方案是性能与额外一致性规则之间的选择，不应在未测量时默认增加。

保持同 owner、同协议注册幂等，跨 owner/协议冲突拒绝，限额准确，断开删除全部服务，活动 pair 仍持有其 `ServiceTraffic`。替换后才删除无使用者模板、专用对拍测试、参考实现及 `test/common/CMakeLists.txt` 注册项，并同步设计文档 2.3.1 与附录 B；不能只删头文件。

依据：[`registry_mgr.cpp`](../node/src/registry_mgr.cpp) 的 `register_service`、`remove`、`find_service`、`traffic_report`。模板与测试可删的 1027 行只代表维护范围缩小，不承诺业务代码净减少同等数量。

### 3.3 D1 / D4 / D6 / D7：优先收敛真实重复，保留上下文

**D1。** 重复不仅有上报、快照输出和快照校验，还包括 `update_report()` 的解析。可集中链路公共九字段的解析与编码；快照另有 `source` 字段。report 中 destination 的成员资格、非自身、去重检查继续留在 report 入口；age 随接收时间增长与报告过期继续留在快照构建侧。`Topology::Link` 是私有类型，使用私有成员/可访问的局部辅助实现即可，不为几个自由函数扩大公共接口。编解码不是同一操作，不必把两者塞入一个字段反射框架。

**D4。** 当前 `LinkParams`、`FlowParams` 已有 `to_params()`，Flow 另有 `identity_params()`。真正需要整理的是接收侧解析与向数据域构造 Spec 的投影。`FlowSpec` 只持有本节点的 previous/next 等邻接信息，无法反向还原完整 path、addresses 和事务身份；`LinkSpec` 也不是完整双端控制记录。可按实际调用提取 `parse_*_params` 和 `make_*_spec`，显式带入本节点、TTL 等上下文，保留认证来源、端口、成员、epoch、幂等冲突和容量校验，不建立虚假的双向转换层。当前接收侧 `FlowParams` 聚合初始化为八个字段，原文“九字段”不准确。

**D6。** 提取 `parse_datagram_node_config()`，使白名单、address/port、容量、setup timeout、traffic 和 UDP 特有的非零 rate 必须配正 burst 校验在同一函数内完成。不要为了 TCP/UDP 对称而建立带大量模式参数的通用解析器。`ControlNodeConfig::max_services = 256` 是默认值，`NodeConfig::maximum_services = 256` 是合法上限，二者当前相等不表示语义相同，不应强行合并。

**D7。** 共同部分是 options、证书链、私钥、私钥匹配检查及加载 CA。Agent context 使用 `ca_file`，集群使用 `server_ca_file`；前者当前构造为 `tls_server`，后者为 `tls`，也并非完全相同。优先提取一个对既有 context 应用共同设置的辅助函数，显式传入证书、私钥与 CA，保留两个 context 的角色与双 CA 边界。不要只为减少两段初始化另造 context 管理类。

依据：[`topology.cpp`](../node/src/topology.cpp)、[`nodelink_mgr.cpp`](../node/src/nodelink_mgr.cpp)、[`node_config.cpp`](../node/src/node_config.cpp)、[`main.cpp`](../node/src/main.cpp)、[`relay_node.cpp`](../node/src/relay_node.cpp)。

### 3.4 S1 / S5：合并重复代码，避免重新制造模式框架

**S1：部分成立。** `RelayTransport` 确有五组本地/远端入口，但 prepare 和 run 差异具有业务含义：本地等待两端、远端只接一端并绑定 Flow；UDP 本地 run 等待关闭，远端 run 从 Flow 收包；计费与 accessor 生命周期也不同。原文“实现者各只有一处分支”的承诺不能由增加 handle 自动实现。

先保留 `install_local_pair()` 返回 `AttachPair`、`install_remote_pair(role)` 返回 `AttachInfo` 的类型约束，在各管理器内提取重复的容量检查、查找移除与释放逻辑；小到只有几行的包装可以保留。只有统一后的调用确实更易读时，才进一步合并后续资源操作。

原 `PairHandle {uuid, kind, credentials}` 同时保存模式和可独立选择的 variant，会产生 kind 与凭据类型不一致的组合；每个阶段又只需 uuid/模式，携带整份凭据增加重复状态。不能以“接口十个变五个”作为采用它的依据。必须保留安装回滚、UDP 两来源绑定后才唤醒、跨 Flow 域绑定、激活屏障，以及晚到资源关闭。

**S5：仅字段构造适合合并。** 三组通知并非逐字相同。Single 的 ready 对同 Agent 双角色去重；Single 在 Ready 后的 TCP/TLS 正常收尾不发 closed；Multi 的关闭包含 stage、epoch、path、Flow 和可用端点信息，并有不同通知门槛。清理顺序也不同：Single 先停止数据任务再通知，Multi 先汇合业务/Flow 监视，再通知本端与 peer、停止数据任务、由 ingress 释放 Flow。

因此继续让两个具体类拥有各自 Agent 与强类型 endpoint，并各自完整表达 run/收尾。只提取确实重复的普通字段构造函数；不把状态上移为 `vector<AgentPeer> + AttachResult`，不为三个短函数引入 `append_identity()`、`open_command()` 虚钩子。

依据：[`node_common.h`](../node/inc/node_common.h) 的 `RelayTransport`；[`pipeline_mgr.cpp`](../node/src/pipeline_mgr.cpp)、[`datagram_mgr.cpp`](../node/src/datagram_mgr.cpp)；[`node_session.cpp`](../node/src/node_session.cpp) 的两组通知与 run。

### 3.5 F1 / F2：区分路由、准入和业务校验

**扫描事实成立，原索引方案不成立。** `RelayNode` 有八处显式 `for (relay_sessions_)`，状态上报另有一处；准入另用 `count_if`。多跳 open 重试查找与新建准入可能涉及三次遍历，单跳 open 当前不走重复请求查找，不能概括为每次 open 都三次扫描。停机、epoch 失效、状态聚合等遍历本来就是全体操作，不必消除。

先独立评估准入计数：stream（TCP/TLS 共用）、datagram 和 multi 三项足够，total 可由拥有容器取得。插入失败回滚和根任务完成出口集中维护；关闭中仍占额。数据管理器自己的容量复核保护不同执行域资源，不能一并删除。

后续若分派扫描成为实测瓶颈，可建立以下**候选索引**：

| 消息 | 候选键 | 仍由会话确认的内容 |
|---|---|---|
| 多跳 `relay.open` 重试、按请求取消 | 原始 `AgentPeer` 身份 + `request_id` 的候选集合 | 模式、请求所属角色、字段冲突、关闭状态、通知重发 |
| `relay.attach`、按 UUID 取消/拒绝 | Node 全局 `uuid` | 原始 Agent、角色、ticket、连接编号、期限和重放 |
| `relay.peer.*` | `epoch` + `flow_id` | 认证 source 是否为该会话 peer、命令阶段与幂等 |
| 控制断线 / `relay.detach` | 可暂保留遍历；确有必要时按 Agent 建一对多索引 | 同 Agent 双角色与正确取消范围 |

不能单独用 `request_id` 做 Node 全局键，不同 Agent 可使用同一编号；远端身份应按 primary Node + 原始 session ID 区分，本地身份按 weak owner 区分，不能用易变化的 accessor 地址替代。`std::map` 查找是 `O(log N)`；只有采用哈希表才可称平均 `O(1)`。

即使加上 Agent 身份，也不能默认所有模式下请求键唯一：单跳 open 当前没有按 request_id 去重。只有证明具体模式的唯一性后才可存单指针，否则要保留候选集合或暂不为该路径建索引。同一报文同时携带 request_id 与 uuid 时，现有会话按两者之一匹配；索引不能自行改变匹配优先级和取消范围。

索引安装时点也不同：request 创建时可用，UUID 要等 Installed，ingress Flow 身份要等 open_flow 成功。它们不能全部在 `start_relay()` 一次插入。优先明确每个身份的发布点、回滚点和最终撤销点，不为登记索引增加独立协程/回调框架；根任务退出前仍保留拥有容器与准入名额，迟到消息不得命中释放对象。

F2 只允许把“找候选”放在 RelayNode。README 明确要求重复请求、peer 身份匹配、控制失效与 epoch 判断由会话处理。命中索引不能代替鉴权；直接删除 `handle*()` 并改成无参数 `on_reopen()` 会丢失重复请求冲突校验。Single 不支持 open 重试而 Multi 支持是当前行为，统一它属于业务变更，不能顺手加入内部重构。

依据：[`relay_node.cpp`](../node/src/relay_node.cpp) 的 `start_relay`、`handle_relay`、`handle_relay_peer`；[`node_session.cpp`](../node/src/node_session.cpp) 的 `handle*`；设计附录 D.2.1。

### 3.6 S9 / F10 / F13：小调整也要核对真实语义

**S9。** `shared_links_` 与 `local_links_` 的上限确实各写了一次 `1000`，可以命名为同一明确的 Link 容量常量，放在实际拥有约束的层。`control.max_connections` 同时限制多跳端点数也属实，但新增 `cluster.max_paths` 是配置和容量策略变更；复用 TCP 上限还会牵涉 UDP，不能作为等价整理。Link 建立、Flow 建立、关闭确认和 Agent 总建立期限是不同预算，默认恰好相同不能改成“同名同义”。新增可调项应另行定义含义、默认兼容性与配置文档。

**F10。** 地址比较确实隐式选择控制采样职责，但简单拆出 `on_control_sample()` 并不会自动消除选择分支。若修改，应让控制探针入口明确承担服务采样与带宽采样，TCP/UDP 入口只测队列延迟；公共延迟计算可用普通函数复用。不能让传输域访问 `RegistryMgr`，也不为去掉一行比较增加运行期回调、模式状态或第二套调度流程。收益小，随指标代码改动再做。

**F13。** 非法/重复 node_id 后确有“发 ClusterError，再 async_receive(join_timeout_)”的收尾约定。但 `TLSChannel::send()` 是 post 入队，`async_disconnect()` 可能先改变关闭状态并取消 writer，**不能保证错误报文已发送**。原文“直接 disconnect 后返回”会影响现有客户端收到 ClusterError 的行为，不能列作无条件低风险项。

若要改，应先提供并使用有期限的发送完成语义，再进入统一 disconnect 收尾；同时验证不回应、继续发帧和正常断开的对端都能有界退出。若这要求为单个分支大幅扩展通用 channel，保留当前有限期等待并明确其目的更简单。当前 `join_timeout_` 来自握手配置，也不能固定称为 10 秒。

依据：[`node_control.cpp`](../node/src/node_control.cpp) 的 `schedule_queue_probe`；[`cluster_mgr.cpp`](../node/src/cluster_mgr.cpp) 的 `ClusterSession::reader`；[`tls_channel.cpp`](../protocol/src/tls_channel.cpp) 的 `send`、`disconnect`、`run`、`wloop`。`cluster_integration` 已有重复/保留/超长 node_id 必须收到 ClusterError 的断言。

## 4. 原方案应撤回或暂缓的部分

### 4.1 S2 / F4：保留必要的事件与寿命语义

`AttachResult` 的 monostate 用于无凭据的阶段事件。改为 `optional<variant<AttachInfo, AttachPair>>` 仍然有“无值/单端/双端”三种语义状态，没有自动变简单；拆成两个 installed 接口又会增加 API。没有独立证据说明当前表示造成维护负担，S2 不单独实施。

`NodeSession::Progress` 表示历史业务里程碑；`SessionTransfer::State` 表示 Created/Running/Stopping/Stopped 的任务寿命；Event 是阶段通知。设计附录 C.1 已明确三者不重复。一个 stopped bool 无法同时表达“从未启动、运行中、已请求停止、真实退出”，F4 的删状态方案撤回。

Finished 与 Stopped 也不能合并：正常 Finished 后任务仍等待停止，pair 继续存在以支持 peer Finished 协调；Stopped 由原生完成处理器发出，涵盖取消早于进入 run 的路径。`proceed_` 正常只发送两次放行，但数据任务在 Finished 后还会第三次等待，以保留资源直到取消，不能只按两次等待改写。

当前 `AsyncEvent` 是单执行器的一次性锁存事件，`proceed_` 是跨 control/transfer 执行域的 `concurrent_channel`，不能直接替换。若通过跨域 post 包装事件，需要重新证明顺序、取消、对象寿命与早到通知，反而增加机制。

`installed()`/`wait()` 使用 assert 校验事件是事实；如需 release 下显式诊断，可独立做小范围内部不变量检查，但这不是外部协议错误，也不构成取消阶段握手的理由。原文把 `assert(sent)` 归在 `proceed_` 路径不准确，实际在 `publish()` 检查 events 发送。

### 4.2 S3 / S6 / S7：封装目的可以保留，不以新增层次为目标

- **S3：低收益，随修改整理。** AgentPeer 已有本地、远端两种构造函数；实际相等关系是远端 node/session 二元组或本地 weak owner，accessor 不参与相等比较。可用 `is_remote()` 命名 `!node.empty()`，但增加另一个存储枚举会重复模式状态；只增加 `kind()` 也不能在编译期阻止错误访问 local。需要更强约束时，应先限制成员任意写入，再评估类型拆分，不把 enum 当成完整解决方案。
- **S6：不单独实施。** 会话直接借用所属 RelayNode 与其生命周期一致。原 `NodeServices` 聚合仍暴露可变 manager，又宣称不暴露可变句柄；仅声明 send_relay 成员也不能自动完成向 Node 的转发。为删除 friend 增加依赖包、转发器或函数包装器，不必然让 run 更清楚。确需收窄依赖时直接传真正使用的引用与小值，避免成为新的服务定位层；不把 ClusterMgr 的既有 friend 混入会话重构。
- **S7：原解释不成立。** 本地流量对象在 prepare 绑定，并非 activate 时才声称绑定；远端 stream/source 与 Flow 要在 install 之后才具备，不能把激活前就绪校验移到 install。当前本地 accessor 随 active 到 close，远端 accessor 由 run 的 guard 管理，统一 activate 还会改变统计窗口。枚举替换 bool 若需要额外保留 closed，并未自动减少状态；维持现状，仅在具体路径确有重复时局部提取。

### 4.3 S8 / D5：撤回不等价的 ID 合并与派生

**S8。** `generate_random_id()` 只保证密码学随机且非零，不保证“当前不重复”；碰撞检查由调用方按各自命名空间负责。`RelayIdAllocator` 则在 Node 内跨 TCP/UDP 保留活动 UUID 并释放，两者职责不同。随机函数已是统一入口；ticket、Link token 是认证凭据，不能退化为顺序 ID。把它们塞入带策略的通用生成器会模糊安全属性、引入依赖与锁，收益不足。保留随机凭据与 UUID 分配器的分工，只改善命名/注释即可。

**D5。** `uuid` 与 `uuid ^ mask` 不能保证不同 relay 的 UDP session 不冲突：若另一 relay UUID 恰为 `uuid ^ mask`，会与前一 relay 的一端相同；`uuid == mask` 时还会产生零。一个包含完整 64 位 UUID 和角色的空间也不能无条件一一映射到非零 64 位 session 空间，需要额外限制或分配协议。

即使保留随机 session，将 bindings 改为 `session_id → weak_ptr<Pair>` 仍不能直接替代现有功能：本地 pair 需要区分 Producer/Consumer，attach 按 UUID 查找，关闭按 UUID 撤销两个 session；weak 引用还会增加过期处理。当前 bindings 明确保存 `{relay_uuid, side}`，职责清楚。保留独立 session 分配与索引，任何修改均须另行证明碰撞、回收、源地址和角色隔离。

### 4.4 F3 / F5 / F9：必要协议边界不能按表面相似合并

**F3：撤回“peer 消息与数据面重复”的结论。** FIN 表示一个方向发送结束，不证明对端已经把接收数据写入 Agent socket；接收 FIN、完成本地双向转发、收到对端 Finished、释放整条 Flow 是不同完成点。PeerClose 可在数据接入或绑定之前传递阶段与原因，Flow Reset 不能无条件覆盖。UDP 也没有 TCP 的 FIN/信用语义。PeerReady 则协调首末端激活和对 Agent 的 ready 宣告，逐跳 credit 不能替代。保留五条 peer 消息；减少消息若确有性能动机，作为独立协议设计验证半关闭、尾部排空、早期失败与对端退出，不列入本次简化待办。

**F5：撤回通用 AckTransaction。** SharedLink 是可被多路径复用的两端链路记录，MasterPath 是拥有独立建立/提交/关闭与释放确认的路径事务。SharedLink 当前没有 MasterPath 的 `reply_to`，Link 的 stage 还是诊断字符串，不能照原表格视为同一个阶段机。失败共享 Link 会影响多个正在 Linking 的路径，取消单条路径却不能取消共享 Link；两者抽象差异远大于命令和位宽。

prepared 与 ready 分别确认本端授权准备和实际链路建立；Master 当前要求双方 Ready。接收端 attach 发生在数据域，不等价于 Master 已收到其 ready。不得顺带删掉接收端确认。普通位运算或参数构造可局部复用，不提取包含 fail、timeout、reply_to 的通用生命周期框架。

**F9：撤回降低网络输入校验的建议。** `validate_server_status()` 校验的是通过集群回送的远端节点报告；`validate_snapshot()` 也用于接收 Master 快照。它们在发送节点产生，不能据此视作接收节点的内部可信对象。mTLS/source 认证和长度上限不保证字段类型、值域、路径和角色语义正确，本地结构化类型也不会跨网络自动保留。

可结合 D1 提取具体解析/校验函数以减少 schema 漂移；保留外部输入的字段、范围及身份约束。不要为减少校验代码建立通用字段生成框架，也不要将其直接降为数组形状检查。

### 4.5 F7 / F8 / D3 / F12：已有直接流程，不急于改成框架

**F7。** 当前 group/subgroup + switch 已把 Link/Flow、Peer、拓扑与查询分开，少量命令改成 handler 表会增加间接跳转与 allowed_states 的维护。原文还把停止期规则写反：`state == Running || command != FlowOpen` 表示停止期间**拒绝 FlowOpen**、继续处理其他 Link/Flow 清理消息。若整理，只提取清楚命名的准入谓词或相邻分支，不要求表驱动。

**F8 / D3。** NodeControl 已有 `broadcast_client_query`、`attach_cluster_reply_route`、`prepare_client_reply` 三个集中路由辅助函数。三类业务形成六个集群 handler，不是六对。ClusterRoom 的 source 来自认证连接并覆盖来报字段；NodeControl 负责 requester/session 回送，Relay 代转还需原始 Agent 身份与 accessor。这些边界不可一并改为无上下文的 `with_route/take_route`。

保留各层既有集中处理点，只有发现具体散落/重复字段时再提取局部函数；请求关联字段 request_id 也不必从所有业务载荷中强行剥离。线上信封改版不是内部简化范围。

**F12。** LocalParticipant 使本机与远端共用参与者、定向投递和成员表逻辑；删除后要给成员列表、身份唯一性、容量、停机条件及本机投递增加特例。当前 `publish_members()` 定向本机后广播其他节点，正对应设计 3.2 的已完成广播收敛。[`CtrlMessage`](../protocol/inc/message.h) 按值持有 `optional<njson>`，第一次投递复制消息，接收侧修改不会反向修改原载荷；复用原变量改写 target 不构成共享载荷错误。可用明确的本机消息/广播消息变量提高局部可读性，但不绕过 ClusterMgr 的接收路径，不删除 LocalParticipant。

### 4.6 F11 / D2：性能或异步接口演进应有单独依据

**F11：暂缓。** `RelayNode::stop()` 明确要求由执行器外的所有者线程调用、worker 保持运行；这些 future.get 是既有同步停机边界，并非工作线程中的任意阻塞。`setup_tasks_.wait()` 是 C++ atomic 的阻塞等待，原文称其为“自旋”等待不准确。

把内部排空整理成 stop_all 协程可以讨论，但仅把三次 get 收成一次不证明流程更简单。若确需异步 stop 接口，各域清理必须由绑定到对应执行器的协程执行，不能用 `dispatch` 假装切换协程执行器，也不能让 control_io 阻塞等待 TCP setup。普通 AsyncEvent 不能直接跨线程通知；异步排空需明确同域计数、零计数早到与完成通知。保留控制会话/业务 → NodeLink/Topology/Cluster → TCP/UDP 停止顺序及异常后 Stopping 可重试语义，成本较高，暂不作为简化优先项。

**D2：暂缓，定位为性能优化。** 报告到达、成员变化和定时上报确会触发快照重建；但当前 snapshot 是带 version、created_at 的物化视图。按查询即时生成或仅 dirty 缓存会影响版本递增、created_age_ms、report_age、链路 age 及 15 秒报告过期的观察时点，不能直接承诺语义不变。

若剖析表明 JSON 重建分配显著，可先固定快照捕获内容、版本与捕获时间，只延迟序列化；仍会增加缓存失效规则和结构化快照状态。须与直接重建的简单性比较后再决定，不为尚未测量的收益引入 dirty 状态。

## 5. 原编号逐项裁决索引

| 编号 | 裁决 | 后续去向 |
|---|---|---|
| S1 | 部分成立，撤回优先引入 PairHandle 的方案 | P1 局部复用，见 3.4 |
| S2 | 表示变化不消除语义状态 | 保留，见 4.1 |
| S3 | 隐式判别属实，enum 不解决类型安全 | 随修改改善命名，见 4.2 |
| S4 | 唯一使用者属实，原两 map 方案漏 owner 清理 | P1 完整替换，见 3.2 |
| S5 | 有共同字段，通知与收尾规则不同 | P2 仅字段构造，见 3.4 |
| S6 | 删除 friend 本身不构成流程收益 | 不单列重构，见 4.2 |
| S7 | 就绪校验与统计寿命不可前移/合并 | 保留，见 4.2 |
| S8 | 随机凭据与保留式 UUID 非同一职责 | 撤回，见 4.3 |
| S9 | 容量常量可命名，配置/期限语义需区分 | P0 仅常量，见 3.6 |
| F1 | 准入与分派扫描属实，键/时点/复杂度有误 | P1 计数，P2 索引，见 3.5 |
| F2 | 候选查找可上移，业务与身份判定不可删除 | 与索引一起限缩实施，见 3.5 |
| F3 | FIN、Finished、Close、Ready 语义不同 | 撤回，见 4.4 |
| F4 | 业务进度、任务寿命和阶段事件不可混同 | 撤回状态/通道合并，见 4.1 |
| F5 | Link 与 Path 不构成同一事务机 | 撤回公共框架与删确认，见 4.4 |
| F6 | 协程帧地址发布问题已改为容器按值持有 | P0 已实施，见 3.1、7 |
| F7 | 表驱动收益不足，停止期规则原文写反 | 保留直接分派，见 4.5 |
| F8 | 已有路由辅助函数，新增封装收益有限 | 与 D3 合并为局部维护，见 4.5 |
| F9 | 被校验的是远端网络输入 | 撤回校验退化，见 4.4 |
| F10 | 隐式职责选择属实，但拆函数不自动消除分支 | P2 小范围处理，见 3.6 |
| F11 | 外部线程同步停机合理，atomic.wait 不是自旋 | 暂缓异步接口演进，见 4.6 |
| F12 | 本地参与者统一语义有价值 | 保留，只改善变量命名，见 4.5 |
| F13 | 再收一帧可改善，直接断开会破坏错误发送保证 | P2 有条件实施，见 3.6 |
| D1 | 重复成立，需涵盖 report 解析及上下文差异 | P0，见 3.3 |
| D2 | 分配优化可能成立，快照语义不天然等价 | 暂缓并先测量，见 4.6 |
| D3 | 信封可局部集中，但已有职责边界 | 与 F8 合并；协议改版移出范围，见 4.5 |
| D4 | 转换重复成立，Params/Spec 不是双向等价类型 | P0 单向转换，见 3.3 |
| D5 | UUID 异或派生不保证跨会话唯一 | 撤回，见 4.3 |
| D6 | UDP 解析可集中，默认值与上限非重复语义 | P0，见 3.3 |
| D7 | 初始化共同步骤成立，context 角色与 CA 不同 | P0，见 3.3 |

## 6. 实施与验证安排

以下保留各建议的验证要求；F6 的实际验证单独记在第 7 节，其余条目未因本次修复而实施。测试源码提供验收入口，不能据此承诺所有新增边界已覆盖。

| 改动范围 | 现有验证入口 | 需重点补足/确认的可观察行为 |
|---|---|---|
| F6 | `node_flows` | 成功与取消竞争、未接受结果补发关闭、记录撤销后迟到回复 |
| D1 / D4 | `topology`、`node_links`、`node_flows` | 缺失/越界/空值、错误来源、字段冲突、TTL、身份关联 |
| D6 / D7 / S9 常量 | `node_config`、`node_lifecycle`、`cluster_integration`、`node_process` | UDP 限速校验、双 CA 与密钥失败、启动失败清理；相关项按改动选择 |
| S4 | `node_status`、`node_metrics`、`cluster_integration` | 服务幂等/冲突、两级限额、owner 批量清理、注册插入失败回滚、旧 traffic 存活 |
| S1 / S5 | `relay_pair`、`session_transfer`、`udp_session_routing`、`relay_paths` | 同 Agent 双角色、激活前取消、安装回滚、晚到 Flow 回收、通知字段与清理顺序 |
| F1 / F2 | `node_relay_capacity`、`relay_paths`、`cluster_integration` | 不同 Agent 相同 request_id、单跳重复编号与双键匹配、远端代转身份、关闭中占额、各索引发布/撤销、错角色/票据/peer |
| F10 / F13 | `node_metrics`、`cluster_integration`、`node_process` | 分域采样与释放；错误先发出、恶意对端有界退出、停机时仍排空 |
| 若未来推进 F11 / D2 | `node_shutdown`、`node_process`、`session_transfer` / `topology` | 取消先于 run、停机异常可重试、worker join / 版本时间与过期语义 |

每项单独提交，P0 不夹带配置策略或协议变更。P1 的容器、配对、计数分别实施并验证，避免同时改动状态归属和消息分派。P2 先满足表中的触发条件，再形成具体方案；不以本建议文档代替相关协议/设计变更记录。

可按已配置的构建目录运行：

```bash
cmake --build build --parallel 1
ctest --test-dir build -L node --output-on-failure
ctest --test-dir build -R '^relay_paths$' --output-on-failure
```

`cluster_integration`、`node_process` 已属于 `node` 标签，无需为“额外覆盖”重复运行同一批测试；多配置生成器按 [测试说明](../test/README.md) 加 `-C Debug`。只有改动涉及端到端 TCP/TLS/UDP 行为或出现新问题，才进一步选择 `end_to_end` 回归与相应故障用例。性能项目另记录同条件下的分派耗时或分配数据，不将测试通过当作性能收益证明。

## 7. F6 实施记录（2026-11-09）

`pending_opens_` 已改为按值持有 `PendingOpen`，发送前原位登记，并由同一退出清理范围撤销。发送失败不补发远端关闭；已提交但未接受的结果仍请求关闭，保留独立 `accepted` 标志。通知、成员变化和控制失败仅更新结果并唤醒，不移除仍被等待协程借用的记录。等待返回后、接受结果前显式检查取消状态，避免取消与成功通知竞争时误接受结果。主设计文档中的所有权描述已同步更新。

`node_flows` 新增取消后成功回复先于等待恢复到达、远端拒绝、发送失败的回归，并确认记录保留到等待结束、远端关闭使用原 epoch/flow_id、迟到回复不能重建已退出记录。

验证：复用 `/tmp/relayweave-nodelink-merge-build` 的 Debug 构建（`OPENSSL_USE_STATIC_LIBS=OFF`），完成 Node 可执行文件及全部 Node 测试目标的构建。执行 `ctest --test-dir /tmp/relayweave-nodelink-merge-build -L node --output-on-failure`，**14/14 通过，耗时 122.55 秒**，包括 `node_flows`、`node_links`、`node_shutdown`、`cluster_integration` 和 `node_process`。`git diff --check` 通过。
