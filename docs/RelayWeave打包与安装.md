# RelayWeave 打包与安装

本文集中说明 RelayWeave 的 CMake/CPack Debian 打包、版本和架构命名、文件布局、systemd 服务生命周期、安装、升级、卸载和排错。系统原理见 [RelayWeave 设计](RelayWeave设计.md)，证书生成和信任关系见 [证书制作与部署](证书制作与部署.md)。

## 1. 包与版本

项目通过 CPack 的 DEB generator 从一次构建生成四个独立组件包：

| 包 | 程序 | 用途 |
|---|---|---|
| `relayweave-node` | `relayweave-node` | 集群节点、服务发现和 TCP/TLS/UDP 数据中继 |
| `relayweave-agent` | `relayweave-agent` | 发布本地服务并建立本地 forward |
| `relayweave-proxy` | `relayweave-proxy` | HTTP、HTTPS CONNECT 和 SOCKS5 应用层代理 |
| `relayweave-dashboard` | `relayweave-dashboard` | 独立集群看板，经服务端 Agent 发布给客户端浏览器 |

包文件使用 Debian 标准名称：

```text
relayweave-node_<upstream-version>-<debian-release>_<architecture>.deb
relayweave-agent_<upstream-version>-<debian-release>_<architecture>.deb
relayweave-proxy_<upstream-version>-<debian-release>_<architecture>.deb
relayweave-dashboard_<upstream-version>-<debian-release>_all.deb
```

例如：

```text
relayweave-node_1.0.0-1_arm64.deb
relayweave-agent_1.0.0-1_arm64.deb
relayweave-proxy_1.0.0-1_arm64.deb
```

版本来源只有两处：

- upstream version 在顶层 [`CMakeLists.txt`](../CMakeLists.txt) 的 `project(relayweave VERSION ...)` 中维护；
- Debian revision 使用 CMake cache 变量 `RELAYWEAVE_PACKAGE_RELEASE`，默认值为 `1`。

应用输出的版本、CMake project version 和包的 upstream version 来自同一 `PROJECT_VERSION`。只修改 Debian 构建或维护脚本而应用版本不变时，可以递增 revision，例如从 `1.0.0-1` 变为 `1.0.0-2`。

Dashboard 是纯 Python 包，架构固定为 `all`。构建时只复制源码，不绑定构建机的 Python 版本；运行时使用系统 `python3` 及发行版提供的 Flask、cbor2 和 Waitress。安装包中的 Python 最低版本约束（当前为 `>= 3.10`）不是固定版本要求，部署端无需与构建机使用相同的 Python 版本。其他包的架构由 CPack/DPKG 根据当前构建目标自动填写，例如 `amd64`、`arm64` 或 `armhf`。包文件名始终包含架构；交叉编译时必须确保工具链目标与 Debian architecture 一致。

## 2. 构建环境

最低要求：

- CMake 3.21；
- 支持 C++20 的编译器；
- OpenSSL headers 和 libraries；
- pthread；
- Debian 打包工具，包括 `dpkg-shlibdeps`；
- Bash，用于项目包装脚本。

Debian/Ubuntu 可以安装：

```console
sudo apt update
sudo apt install build-essential cmake libssl-dev dpkg-dev
```

Linux 上 GCC 的 `libstdc++` 和 `libgcc` 被静态链接，glibc、OpenSSL 等系统库仍按动态依赖处理。CPack 启用 `dpkg-shlibdeps` 自动计算三个包的共享库依赖，并显式依赖 `systemd`。

## 3. 生成 Deb 包

从仓库根目录执行：

```console
scripts/build_deb.sh
```

脚本默认：

- 使用 `build-package/`；
- 配置 `Release`；
- 关闭测试目标；
- 使用单作业编译，降低小内存设备的峰值占用；
- 把结果写入 `build-package/packages/`。

指定构建目录、Debian revision 或并行数：

```console
RELAYWEAVE_PACKAGE_RELEASE=2 scripts/build_deb.sh /tmp/relayweave-package
RELAYWEAVE_BUILD_JOBS=2 scripts/build_deb.sh
```

`RELAYWEAVE_BUILD_JOBS` 必须是正整数。在内存有限的 ARM 设备上建议保持默认值 `1`。

等价的手工命令是：

```console
cmake -S . -B build-package \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DRELAYWEAVE_PACKAGE_RELEASE=1
cmake --build build-package --parallel 1
cpack --config build-package/CPackConfig.cmake -G DEB
```

构建完成后可在安装前检查元数据和内容：

```console
dpkg-deb --info build-package/packages/relayweave-agent_*.deb
dpkg-deb --contents build-package/packages/relayweave-agent_*.deb
dpkg-deb --info build-package/packages/relayweave-node_*.deb
dpkg-deb --contents build-package/packages/relayweave-node_*.deb
dpkg-deb --info build-package/packages/relayweave-proxy_*.deb
dpkg-deb --contents build-package/packages/relayweave-proxy_*.deb
```

## 4. 安装内容

| 内容 | Agent 包 | Node 包 | Proxy 包 |
|---|---|---|---|
| 程序 | `/usr/bin/relayweave-agent` | `/usr/bin/relayweave-node` | `/usr/bin/relayweave-proxy` |
| 配置 | `/etc/relayweave/agent.json` | `/etc/relayweave/node.json` | `/etc/relayweave/proxy.json` |
| systemd unit | `/lib/systemd/system/relayweave-agent.service` | `/lib/systemd/system/relayweave-node.service` | `/lib/systemd/system/relayweave-proxy.service` |
| 证书目录 | `/etc/relayweave/certs/` | `/etc/relayweave/certs/` | 不需要 |
| 部署工具 | `/usr/sbin/relayweave-provision-agent` | `/usr/sbin/relayweave-provision-node` | `/usr/sbin/relayweave-provision-proxy` |

四个组件包均在 `/usr/share/doc/relayweave-<role>/` 安装设计、打包与安装、证书部署文档；Dashboard
另包含其 README。route 模块静态链接到 Node 和 Agent，不单独打包；测试、诊断程序及测试证书不安装。

配置文件分别由 `agent/agent.example.json`、`node/node.example.json` 和 `proxy/proxy.example.json` 安装而来，并通过 Debian `conffiles` 机制管理。升级时 dpkg 保留管理员修改；如果新包也修改了模板，dpkg 按标准 conffile 规则决定是否询问替换。

Node 和 Agent 包只创建证书目录，不包含证书或私钥；Proxy 不需要证书目录。不同主机需要部署的具体文件见 [证书制作与部署](证书制作与部署.md)。手工复制或由部署工具安装的证书不属于包文件，卸载或 purge 脚本不会主动删除它们。

## 5. 首次安装

使用 `apt` 安装本地包，让它同时解析共享库依赖：

```console
sudo apt install ./build-package/packages/relayweave-node_1.0.0-1_arm64.deb
sudo apt install ./build-package/packages/relayweave-agent_1.0.0-1_arm64.deb
sudo apt install ./build-package/packages/relayweave-proxy_1.0.0-1_arm64.deb
```

只安装当前主机需要的角色；应用代理通常与发布该代理服务的 Agent 安装在同一台远端主机。

首次安装执行以下动作：

1. 安装程序、示例配置和 systemd unit；Node/Agent 另创建证书目录；
2. 执行 `systemctl daemon-reload`；
3. enable 对应服务，使其在以后开机时启动；
4. 不立即启动服务，避免使用尚未修改的示例地址和缺失证书启动。

随后准备主机专用配置和由证书生成脚本输出的部署目录。先执行只读验证：

```console
relayweave-provision-node \
  --cert-dir /secure/relayweave/nodes/master-1 \
  --config /secure/relayweave/config/master-1.json \
  --verify-host proxy.example.com \
  --dry-run

relayweave-provision-agent \
  --cert-dir /secure/relayweave/agent \
  --config /secure/relayweave/config/agent.json \
  --dry-run

relayweave-provision-proxy \
  --config /secure/relayweave/config/proxy.json \
  --dry-run
```

验证通过后安装文件并首次启动，只执行当前主机实际安装的角色：

```console
sudo relayweave-provision-node \
  --cert-dir /secure/relayweave/nodes/master-1 \
  --config /secure/relayweave/config/master-1.json \
  --verify-host proxy.example.com \
  --start

sudo relayweave-provision-agent \
  --cert-dir /secure/relayweave/agent \
  --config /secure/relayweave/config/agent.json \
  --start

sudo relayweave-provision-proxy \
  --config /secure/relayweave/config/proxy.json \
  --start
```

Dashboard 也使用同一工具：`relayweave-provision-dashboard --cert-dir DIR --config FILE [--dry-run] [--start|--restart]`。证书输入与 Agent 相同，安装到独立的 `/etc/relayweave/dashboard-certs/`，私钥设置为看板专用组可读；详见 [Dashboard 部署说明](../dashboard/README.md#独立服务部署)。

工具按固定路径安装配置和证书，覆盖前把已有文件保存到 `/var/backups/relayweave/<role>-<UTC时间>-<pid>/`。Proxy 不使用证书，因此其部署命令不接受 `--cert-dir`。默认不改变服务状态；`--start` 执行 `systemctl enable --now`，`--restart` 执行 `systemctl try-restart`。Node 的 `--verify-host` 可以重复指定，Agent、Proxy 和 Dashboard 不接受该选项。

检查状态和日志：

```console
systemctl status relayweave-node.service
journalctl -u relayweave-node.service -e
systemctl status relayweave-agent.service
journalctl -u relayweave-agent.service -e
systemctl status relayweave-proxy.service
journalctl -u relayweave-proxy.service -e
```

## 6. 升级

用 `apt install` 安装更高版本或更高 Debian revision 的本地包：

```console
sudo apt install ./relayweave-node_1.0.0-2_arm64.deb
sudo apt install ./relayweave-agent_1.0.0-2_arm64.deb
sudo apt install ./relayweave-proxy_1.0.0-2_arm64.deb
```

升级行为：

- 配置继续按 conffile 规则保留；
- 新程序安装完成后执行 `systemctl daemon-reload`；
- 升级前正在运行的服务会重启并加载新程序和配置；
- 原本停止的服务不会因为升级被意外启动；
- 三个组件是独立包，可以分别升级。

链路质量协议使用 `quality: {cost, confidence, usable}`，Node、Agent、Dashboard 应统一升级，不支持
混用旧质量格式。Agent 的 routing 配置默认启用且只接受可选 `max_nodes`（默认 4，范围 1–16）；
旧配置中的 `routing.enabled` 必须删除，dpkg 保留的 conffile 不会自动完成这一修改。

升级后检查：

```console
dpkg-query -W 'relayweave-*'
systemctl --no-pager --full status relayweave-node.service relayweave-agent.service relayweave-proxy.service
```

回退时可以重新安装保留的旧包，但必须确认当前配置仍兼容旧程序。

配置或证书与软件同时更新时，推荐先用部署工具验证并写入新文件但不操作服务，再安装新 Deb。正在运行的服务会由包升级流程重启一次并加载全部新内容：

```console
sudo relayweave-provision-node --cert-dir /secure/relayweave/nodes/master-1 \
  --config /secure/relayweave/config/master-1.json --verify-host proxy.example.com
sudo apt install ./relayweave-node_1.1.0-1_arm64.deb
```

只轮换配置或证书、不升级软件时使用 `--restart`。原文件备份用于人工回退，工具不会自动恢复旧版本。

## 7. 服务脚本生命周期

| dpkg 阶段 | 行为 |
|---|---|
| 首次 `postinst configure` | reload systemd，enable 服务，不启动 |
| 升级 `postinst configure <old-version>` | 仅当服务当前 active 时 restart |
| `prerm remove/deconfigure` | disable 并停止服务 |
| `postrm remove/purge/...` | reload systemd 并清除 failed 状态 |

unit 使用 `Restart=on-failure` 和 5 秒重启间隔，并启用 `NoNewPrivileges=true`、`PrivateTmp=true`。Node 和 Agent 使用 systemd 系统服务的默认身份运行，AmbientCapabilities 和 CapabilityBoundingSet 均保留 `CAP_NET_RAW` 与 `CAP_NET_BIND_SERVICE`，用于原始 ICMP 和低端口监听。两者的安装脚本与部署工具检查 ICMP 能力，包含 systemd override 的实际生效配置；升级会 reload unit，并重启原本运行的服务。

Proxy 不需要特权端口或证书文件，使用 `DynamicUser=true`，并额外启用只读系统目录、隐藏 home 和私有设备等限制。Dashboard 本身不打开原始 socket；发布它的 Agent 仍需要上述探测权限。

## 8. 卸载与清理

删除程序但保留 conffile：

```console
sudo apt remove relayweave-agent relayweave-node relayweave-proxy
```

同时删除包管理的配置文件：

```console
sudo apt purge relayweave-agent relayweave-node relayweave-proxy
```

`remove` 和 `purge` 都会停止并 disable 对应服务。手工放入 `/etc/relayweave/certs/` 的证书和私钥不会被维护脚本删除；确认不再需要后应由管理员单独归档或安全删除。

## 9. 常见问题

### 包名没有架构

正常 CPack 输出使用 `DEB-DEFAULT` 文件名，必然包含 Debian architecture。确认使用项目生成的 `CPackConfig.cmake`，不要直接手写不带架构的输出文件名。

### 安装时报告依赖缺失

优先使用 `apt install ./package.deb`，不要只使用 `dpkg -i`。如果构建机没有 `dpkg-shlibdeps`，重新安装 `dpkg-dev` 后清理或重新配置打包目录。

### 首次安装后服务没有运行

这是预期行为。首次安装只 enable，不启动。配置和证书准备完成后，使用部署工具的 `--start`，或执行 `systemctl start`。

### 升级后服务没有启动

维护脚本只重启升级前处于 active 状态的服务。使用 `systemctl start` 明确启动原本停止的服务。

### 服务反复重启

先检查 JSON 路径、证书文件、私钥权限、监听地址和端口占用：

```console
journalctl -u relayweave-node.service -n 100 --no-pager
journalctl -u relayweave-agent.service -n 100 --no-pager
journalctl -u relayweave-proxy.service -n 100 --no-pager
```

证书错误的进一步检查见 [证书制作与部署](证书制作与部署.md)。

### 部署工具提示缺少 `client-ca.pem`

Agent 运行时不需要 Client CA，但部署工具需要它验证 `shared-client.pem`。应传入证书生成脚本创建的 `agent/` 部署目录；工具验证后不会把 `client-ca.pem` 安装到 Agent。

### Node 或 Agent 提示缺少 `CAP_NET_RAW`

检查安装的 unit 和本机 override 是否保留 ICMP 能力：

```console
systemctl cat relayweave-agent.service
systemctl show relayweave-agent.service -p AmbientCapabilities -p CapabilityBoundingSet
systemctl cat relayweave-node.service
systemctl show relayweave-node.service -p AmbientCapabilities -p CapabilityBoundingSet
```

安装或部署检查失败时先修正 unit/override，再执行 `systemctl daemon-reload` 和重新部署。
手动启动二进制也需要原始 socket 权限；权限不足时质量探测记录错误并重试，原有业务直连仍可工作。

## 10. 实现位置

- [`cmake/Packaging.cmake`](../cmake/Packaging.cmake)：CPack 组件、包名、依赖、版本和控制脚本；
- [`scripts/build_deb.sh`](../scripts/build_deb.sh)：Release 配置、单作业构建和 CPack 调用；
- [`scripts/provision_relayweave.sh`](../scripts/provision_relayweave.sh)：Node/Agent 证书验证、三类配置的备份部署和显式服务操作；
- Agent、Node 与 Proxy 各自的 `CMakeLists.txt`：组件安装布局；
- `packaging/agent`、`packaging/node`、`packaging/proxy`：systemd unit、conffiles 和 maintainer scripts。


## Dashboard 独立服务

Dashboard 通过 `relayweave-dashboard` 包独立安装，由专用用户运行。配置位于 `/etc/relayweave/dashboard.json`，证书位于 `/etc/relayweave/dashboard-certs/`，历史数据库位于 `/var/lib/relayweave-dashboard/history.sqlite3`。首次安装启用 unit 但不立即启动；准备证书并编辑配置后执行 `sudo systemctl enable --now relayweave-dashboard`。包升级只重启已运行的看板服务，卸载不主动清理历史库。

服务默认监听服务器的 `127.0.0.1:5000`。由同机 Agent 的 `services` 发布 `dashboard`，客户端 Agent 用 `forwards` 监听 `127.0.0.1:15000`，浏览器即可访问 `http://127.0.0.1:15000/`。Node 无需新增功能或配置字段，客户端无需安装 Python。详细的证书权限、发布/消费配置和历史迁移步骤见 [Dashboard 部署说明](../dashboard/README.md#独立服务部署)。
