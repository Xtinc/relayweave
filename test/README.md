# 测试目录

测试按被验证的组件分组；需要完整 Agent、Node 或 Proxy 链路的场景归入 `integration/`。
测试夹具、私有状态访问和参考实现只存在于测试目录，不给业务类增加测试接口。

| 目录 | 内容 | CTest 标签 |
|---|---|---|
| `common/` | 双索引容器、LRU、缓冲池、AsyncEvent、令牌桶、流量计数 | `common`、`unit` |
| `protocol/` | 控制/二进制编解码、mTLS、LnkChannel、TCP/UDP 复制 | `protocol`，按用例区分 `unit` / `integration` |
| `route/` | 链路质量、路径算法、DNS 探测生命周期、真实 ICMP | `routing`，真实 ICMP 另标 `icmp` |
| `agent/` | Agent/Session/Forwarder 生命周期、路由组装和缓存 | `agent`，路由相关另标 `routing` |
| `node/` | 配置/统计、启动停止、集群控制、共享 NodeLink、Flow、UDP session 路由、拓扑 | `node`，集群相关另标 `cluster` |
| `proxy/` | HTTP、CONNECT、SOCKS5、容量、超时和监听回滚 | `proxy`、`integration` |
| `integration/` | 单/多节点 TCP/TLS/UDP、恢复、迁移及 Proxy 完整链路 | `end_to_end`、`integration` |
| `deployment/` | 安装脚本、能力校验、证书校验与配置 dry-run | `deployment`、`integration` |
| `benchmark/` | UDP Node 吞吐、双索引容器性能比较 | 手动运行，不注册 CTest |
| `tools/` | ICMP/TLS 手动诊断、真实 Dashboard 服务 smoke | 手动运行，不注册 CTest |
| `support/` | 公共夹具、资源定位和容器对拍参考实现 | 不单独运行 |
| `data/` | 测试证书和配置样本 | 只供测试使用 |

`unit` 用例不要求网络权限；使用真实 socket、跨执行域异步交互或进程的用例标为
`integration`。组件标签与运行类型可以组合筛选。Dashboard 的 Python 测试保留在
`dashboard/`，与该模块的 Python 导入和依赖环境一起运行。

## 构建与运行

```console
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build --parallel 1
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

手动工具和性能基准位于构建目录中对应的分组：

```console
build/test/tools/test_icmp 127.0.0.1 4
build/test/tools/test_tls_channel_peer server 18443
build/test/tools/test_tls_channel_peer client localhost 18443
build/test/benchmark/benchmark_dual_index_map
build/test/benchmark/benchmark_udp_node --help
```

多配置生成器的可执行文件可能位于分组下的 `Debug/` 或 `Release/` 子目录；CTest 自动定位它们。
基准只报告吞吐、丢包或耗时，不用机器性能阈值决定功能测试是否通过。

## 回归边界与整理原则

- TCP 复制：两侧读/写失败和限速等待中的对向失败必须排空；正常 EOF 保留反向回传，校验原始异常和实际流量。
- UDP 复制：零长度、4097 字节及最大合法载荷双向保持原样；短头、错误 session、超限包丢弃后仍能处理后续包；两侧 I/O 失败都能排空。
- 多节点 UDP：65467 字节载荷在共享 Link 和多跳路径中双向完整转发；65468 字节在接入时丢弃；最大长度的非法帧不会妨碍后续合法帧。
- 多节点 TCP/TLS：小请求半关闭后，服务方仍能完整返回超过两个最大 DATA 载荷的连续字节流，并正常传递反向 EOF。
- 限速与计数：快路径不能重复扣费，失败试扣不能吃掉令牌，预留债务必须累积，空包与无限速不等待；并发计数/采样不丢字节，计数溢出饱和。
- 控制与生命周期：并发/重复停止、取消后排空、超时、旧 request/epoch、断线恢复、服务迁移及对象释放继续保留独立回归。
- 保留旧配置拒绝用例：`node_config_legacy.json` 验证旧字段不能绕过当前配置校验。生成的配置放在临时目录，先验证有效基线能通过，再逐项检查非法字段。
- UDP 集成测试删除旧 `PROXY_UDP_9000_PPS` 分支，容量测量由独立 Node 基准承担；复制层回归从业务集成用例移入协议组。
- 路径缓存测试删除与业务行为无关的日志等级/措辞断言；编解码测试不再混入限速和计数测试。

新增测试应验证可观察的业务结果、边界或真实故障，优先使用协议对端和公开接口。
必要的状态构造或检查放在 `support/` 或测试翻译单元中；不为测试增加业务友元、开关或诊断接口。
