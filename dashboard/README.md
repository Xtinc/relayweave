# Proxy Cluster Dashboard

A Python control-channel client + web dashboard for the C++ TLS proxy cluster
(`node/src/relay_node.cpp`). It establishes the same mutual-TLS control
channel the C++ `RelayAgent` uses, identifies its entry node with
`server.identify`, periodically requests `server.cluster` and
`topology.query`, and renders the reports returned by the cluster in a
browser dashboard.

## Files

| File | Purpose |
|------|---------|
| `proxy_protocol.py` | Wire protocol: 4-byte big-endian length header + CBOR payload (`cbor2`). Mirrors `protocol/inc/message.h` (max frame payload 64 KiB, max logical message 16 MiB, max command length 32). |
| `proxy_client.py`   | One background-thread mTLS client: handshake, heartbeat, periodic service-status and topology queries, history capture, and auto-reconnect. |
| `history_store.py`  | SQLite persistence and time-range/downsampled chart queries. |
| `ip_location.py`    | Background public-IP geolocation, UNECE UN/LOCODE matching, and persistent caches. |
| `dashboard.py`      | Single-process Waitress/Flask service: serves the page and `/api/snapshot`. |
| `templates/dashboard.html` | Single-page dashboard: service traffic table, Node health metrics, queue-delay and per-service bandwidth charts. |

Browser responses negotiate gzip through the standard `Accept-Encoding`
header.  The first `/api/snapshot` request returns the selected history
window and a `history_cursor`; later requests send that cursor as
`history_after` and receive only newer chart samples.  Changing either chart
range resets the cursor and performs a new full-window request.

## Protocol summary (from the C++ source)

- **Transport**: TCP → mutual TLS. The server runs
  `verify_peer | verify_fail_if_no_peer_cert`, so the client **must** present a
  certificate signed by the server's `client-ca.pem`. The client verifies the
  server against `server-ca.pem`, and verifies its identity against
  `server.host`. SNI is controlled separately by `certificate.server_name`.
- **Frame**: `struct { uint32_be length; uint8_t payload[length]; }`, length in
  `[1, 64 KiB]`.
- **Payload**: CBOR object `{"command": str, "params": obj?}` (nlohmann::json
  `to_cbor`).
- **Heartbeat**: the server sends `ping` every `heartbeat_interval_ms` (5s) and
  drops the channel after `heartbeat_timeout_ms` (20s) of silence. The client
  replies `pong` to each `ping`.
- **Commands used here**:
  - `server.identify {}` → `server.identified {node_id}`. This handshake is
    completed before cluster polling starts.
  - `server.cluster {request_id}` → one complete `server.status.reported` message per node with
    `{request_id, node_id, uptime_ms, services:[...]}`. `proxy_protocol.MessageReceiver`
    transparently assembles large messages before they reach the client. Single services
    and accessor maps can span frames. Node membership, advertised addresses, and queue
    delays are supplied only by `topology.snapshot`.
  - Each service entry contains `{service, protocol, rx_bytes, tx_bytes,
    rx_bytes_per_second, tx_bytes_per_second, accessors}`. `accessors` maps
    active consumer control peers to their relay connection counts; this field
    is required by the dashboard protocol. `rx_bytes` /
    `tx_bytes` are cumulative bytes since service registration (saturating);
    `rx_bytes_per_second` / `tx_bytes_per_second` are a 1 s EMA in **bytes/s**
    (the dashboard renders them as bits/s). RX is producer/service → consumer,
    TX is consumer → producer. The dashboard shows each service's live RX/TX
    rate and cumulative bytes in the service table, and plots the selected
    service's RX/TX history in the realtime bandwidth chart. The service table
    renders each service and its current accessors as a tree.
  - `topology.query {request_id}` is accepted by any Node and routed to the
    Cluster Master. The Master returns one complete `topology.snapshot` message
    containing one coherent `epoch`, `snapshot_version`, `created_age_ms`,
    Node queue delays, and directed ICMP link summaries with `quality:{cost,confidence,usable}`. The protocol layer assembles
    the message before the Dashboard replaces its current topology data.

Large messages use consecutive CBOR fragment envelopes
`{"__fragment": [page_index, page_count, bytes]}` with a maximum 65472-byte chunk.
The TCP receiver uses one ordered assembly buffer. A final page with missing
predecessors discards the message while leaving the connection usable; no
reordering cache, message IDs, or assembly timers are needed. Node, Agent, and
Dashboard must be upgraded together; the former business pagination fields are removed.

## Topology collection and page content

The Cluster Master publishes the current member list. Every Node probes the
other advertised IPv4 endpoints once per second, maintains RTT, jitter, and
loss EMA locally, and reports only that summary plus the existing control,
TCP, and UDP executor queue delays every five seconds. The Master replaces a
complete versioned snapshot after membership or report changes; raw ICMP
samples are not centralized.

The page starts with cluster status, followed by the selected Node's five
health metrics and responsive queue/bandwidth charts, then registered services. It does not display Agent
routes, service routes, the Node-to-Node graph, or the directed-link table. Agent
route calculations, per-ingress candidate costs, and connection establishment
are recorded in Agent logs. Topology collection still supplies
current membership and queue values; the API retains link quality data. The score
in that data is derived from cost as `100 * exp(-cost / 100)` and is not transmitted by Nodes.
Snapshot and source-report ages must remain below 15 seconds, and link age below
45 seconds; disconnects invalidate current availability while preserving historical scores.
Age includes query and page-assembly time and uses a monotonic clock. This topology is observational only and is
not used to route Relay traffic.

ICMP probing by Nodes and Agents requires `CAP_NET_RAW`. Their packaged services
retain it in both AmbientCapabilities and CapabilityBoundingSet, and installation
and provisioning check the effective capability. The Dashboard itself needs no raw
socket capability. Quality and route design is documented in
[RelayWeave 设计](../docs/RelayWeave设计.md#77-链路质量与推荐路径).

## 独立服务部署

看板可运行在任意一台部署了 Node 和 Agent 的服务器上。服务端 Agent 发布看板的本地 HTTP 端口，客户端 Agent 建立本地 forward：

```text
浏览器 http://127.0.0.1:15000/
  → 客户端 Agent（dashboard / tls）
  → RelayWeave Node
  → 服务端 Agent
  → Dashboard 127.0.0.1:5000
```

Dashboard 本身用独立的 mTLS 控制连接从配置的入口 Node 采集整个集群；这条采集连接和 Agent 转发网页的连接各自独立。多个浏览器共享同一个采集进程和 SQLite 历史库，客户端不需要运行 Python。客户端 Agent 必须配置下面的 `forwards`，仅安装 Agent 不会自动开放看板入口。

### 1. 安装看板服务

构建项目的 Debian 包：

```bash
scripts/build_deb.sh
sudo apt install ./build-package/packages/relayweave-dashboard_*_all.deb
```

也可在已经配置并构建的目录中只输出 Dashboard 包：

```bash
cpack --config build/CPackConfig.cmake -G DEB -D CPACK_COMPONENTS_ALL=dashboard
```

此包安装 `relayweave-dashboard.service`、专用系统用户 `relayweave-dashboard` 和 Python 代码，运行时使用系统的 `/usr/bin/python3` 和发行版提供的 Flask、cbor2、Waitress，不绑定构建机器上的 Python 版本；不安装或修改 Node/Agent 配置。首次安装会启用开机启动，但不会立即启动；先完成配置和证书准备。升级会重启已经运行的看板服务。

| 内容 | 路径 |
|---|---|
| 命令入口 | `/usr/bin/relayweave-dashboard` |
| 采集配置 | `/etc/relayweave/dashboard.json` |
| 看板独立证书目录 | `/etc/relayweave/dashboard-certs/` |
| 程序与模板 | `/usr/lib/relayweave/dashboard/` |
| 持久化历史库 | `/var/lib/relayweave-dashboard/history.sqlite3` |
| 部署工具 | `/usr/sbin/relayweave-provision-dashboard` |

准备自己的看板配置文件，例如 `/secure/relayweave/config/dashboard.json`，参考 [dashboard.example.json](dashboard.example.json)。`server.host` / `port` 指向入口 Node；可以连接同机 `127.0.0.1:18443`，也可以连接集群内其他 Node。`certificate.server_name` 必须匹配 Node 服务端证书中的 DNS/IP SAN：连接回环地址而证书只包含公网地址时，填写证书中的实际公网地址或域名，不能保留示例 `203.0.113.10`。

采集连接和 TLS 握手超时均默认为 5 秒，示例省略对应字段。证书用于采集连接的 mTLS 认证，即使连接同机 Node 也需要保留。

Dashboard 复用与 Node、Agent 相同的部署脚本。证书输入目录采用 Agent 的命名：`server-ca.pem`、`client-ca.pem`、`shared-client.pem`、`shared-client.key`。其中 `client-ca.pem` 仅用于部署前验证客户端证书，不会安装。可使用现有 Agent 证书目录，或按相同命名准备专用客户端证书。

先校验，再安装配置和证书并启动：

```bash
relayweave-provision-dashboard \
  --cert-dir /secure/relayweave/agent \
  --config /secure/relayweave/config/dashboard.json --dry-run
sudo relayweave-provision-dashboard \
  --cert-dir /secure/relayweave/agent \
  --config /secure/relayweave/config/dashboard.json --start
curl http://127.0.0.1:5000/api/health
```

工具检查客户端证书信任链和私钥匹配，将配置原样安装到 `/etc/relayweave/dashboard.json`，将证书安装到独立的 `/etc/relayweave/dashboard-certs/`。配置中的证书路径须与示例一致，使用 `shared-client.pem` / `shared-client.key`；工具不会重写 JSON。目录为 `root:relayweave-dashboard`、`0750`，私钥和配置为 `0640`，看板用户可以读取，无需修改 Agent 的证书权限。

覆盖前备份到 `/var/backups/relayweave/dashboard-<UTC时间>-<pid>/`。不带启停参数时仅安装文件；`--start` 启用并启动服务，`--restart` 仅重启已运行的服务。源码中可用 `bash scripts/provision_relayweave.sh dashboard ...` 调用相同流程。`--dry-run` 不修改文件或服务，也不检查安装后的权限和采集连接。

服务始终使用单进程 Waitress，HTTP 监听地址固定为 `127.0.0.1`，默认端口 `5000`。不需要开放服务器的公网 5000 端口。`/api/health` 的 HTTP 200 表示 HTTP 服务可用，JSON 的 `ok` 才表示集群采集正常；入口 Node 暂时离线时，页面继续可用，采集客户端会自动重连。

### 2. 服务端 Agent 发布看板

将下面一项加入服务器现有 `/etc/relayweave/agent.json` 的 `services` 数组，保留已有服务、证书和其他配置：

```json
{"name": "dashboard", "target_host": "127.0.0.1", "target_port": 5000, "protocol": "tls"}
```

然后重启服务端 Agent：

```bash
sudo systemctl restart relayweave-agent
```

Dashboard、Agent 和 Node 分别由自己的 systemd unit 管理，不绑定彼此的启停。发布端 Agent 必须与 Dashboard 位于同一主机，才能连接上述回环地址。集群中使用唯一的看板服务名；已有同名服务时改名并同步修改客户端 forward。

### 3. 客户端 Agent 建立浏览器入口

将下面一项加入客户端 Agent 的 `forwards` 数组，并重启客户端 Agent：

```json
{"service": "dashboard", "listen_address": "127.0.0.1", "listen_port": 15000, "protocol": "tls"}
```

客户端可连接同一集群内其他 Node，沿用现有集群服务发现。浏览器访问 **http://127.0.0.1:15000/**，不需要修改浏览器的 HTTP/SOCKS 代理设置。这里的 `tls` 加密的是 Agent 与 Node 之间的数据通道；本地浏览器和服务端回环连接仍是 HTTP/TCP。

### 4. 运维与历史迁移

```bash
systemctl status relayweave-dashboard
journalctl -u relayweave-dashboard -f
sudo systemctl restart relayweave-dashboard
```

与 Node、Agent 一致，配置文件路径必须作为命令行参数传入。CMake 负责将示例安装为 `/etc/relayweave/dashboard.json`，systemd 显式传入配置和持久化历史库路径，不由程序猜测安装环境：

```bash
relayweave-dashboard /etc/relayweave/dashboard.json --database /var/lib/relayweave-dashboard/history.sqlite3
```

服务接收 SIGTERM 后停止 HTTP 服务，等待已接纳的 Flask 页面/API 处理结束，再结束采集和位置查询线程并关闭 SQLite。页面/API 均一次性生成响应，不使用流式数据库响应；重启时底层 HTTP 连接可能关闭，浏览器后续请求会重新连接。数据库位于 systemd `StateDirectory`，重启、升级和卸载软件包均不主动删除历史记录。修改监听端口、采样间隔或历史容量时使用 `sudo systemctl edit relayweave-dashboard` 覆盖 `ExecStart`，再重启；端口变化时同步修改发布端 Agent 的目标端口。

迁移旧的本地历史库时，先正常退出旧看板并停止新服务；将数据库及存在的 `-wal` / `-shm` 文件作为同一组复制，设置所有者为 `relayweave-dashboard:relayweave-dashboard`，再启动服务。不要在旧进程仍写入时只复制主数据库文件。

### 从源码运行

使用系统提供的 `python3` 创建虚拟环境并安装依赖，无需指定某个固定的 Python 小版本：

```bash
python3 -m venv /tmp/relayweave-dashboard-venv
/tmp/relayweave-dashboard-venv/bin/pip install -r dashboard/requirements.txt
/tmp/relayweave-dashboard-venv/bin/python dashboard/dashboard.py /path/to/dashboard.json \
  --database /path/to/state/history.sqlite3
```

也兼容原来的 Agent JSON：只读取 `server`、`certificate`、`channel` 中的采集连接参数，忽略 `services` 和 `forwards`。未传入 `--database` 时，数据库默认位于程序目录下的 `dashboard.sqlite3`；systemd 服务通过该参数使用上述固定状态目录。`--http-port` 默认 5000，`--poll-interval` 默认 2 秒，`--max-db-bytes` 默认 50 MiB。HTTP 地址固定为 `127.0.0.1`，不再支持 `--bind` 和 `--debug`；已有自定义启动命令应删除这两个参数。

### Python 版本与依赖

Dashboard 打包时仅复制 Python 源码，不探测或绑定构建机的 Python 解释器，也不要求部署端与构建机的 Python 版本一致。Debian 安装使用系统 `python3` 和发行版配套的依赖；源码运行使用所选虚拟环境中的解释器。

“不绑定固定版本”不代表兼容所有 Python 版本：解释器仍须满足程序和第三方依赖的要求。当前 Debian 包声明的最低版本为 Python 3.10，这是最低安装条件，并非指定必须使用 Python 3.10；源码安装时，pip 会在 `requirements.txt` 的范围内选择兼容版本。

### 验证

在已安装 `requirements.txt` 依赖的 Python 环境中：

```bash
python -m unittest discover -s dashboard -p 'test_*.py'
python test/dashboard_service_smoke.py --build-dir build
python test/dashboard_service_smoke.py --build-dir build --two-nodes
```

冒烟脚本启动真实 Node、发布端 Agent、消费端 Agent 和 Dashboard，验证 TCP/TLS 转发的页面、快照接口、SIGTERM 与重启后的历史保留；配置、端口和数据库均位于临时目录。`--two-nodes` 通过 Slave 入口采集 Master 汇总，并验证入口停止与恢复；具备 CAP_NET_RAW 时还验证两条有向质量链路和 Agent 推荐路径日志，否则明确记录 ICMP 验证跳过。

## Dashboard design

The header selector chooses the node shown by the metric tiles and charts.
The registered-services table combines services from all discovered nodes.
Five health metrics follow the table; queue-delay and per-service bandwidth
charts share a row on wide screens and stack on narrow screens.
Queue history is sampled from the Master's immutable
topology snapshots rather than duplicated `server.cluster` fields.
The backend returns every topology member with its queue history even before a
service report arrives. Uptime is unknown and the service list is empty until
that report is received; the frontend uses this complete Node view directly.

The dashboard uses a categorical palette, thin 2px lines, 8px end-markers,
hairline gridlines, a crosshair tooltip, a legend for ≥2 series, a table
view, and light/dark/auto themes. The realtime bandwidth chart uses palette
slots 5 (magenta = RX) and 6 (green = TX); a
per-service dropdown picks which service's history to plot, and the same
RX/TX color pairing carries to the service table's rate columns so identity
stays consistent across the table and the chart.
