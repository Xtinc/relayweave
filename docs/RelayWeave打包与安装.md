# RelayWeave 打包与安装

端到端 TLS 编译需要支持 RPK 的 OpenSSL **3.2 或更高版本**，默认静态链接，程序运行时不依赖系统 libssl/libcrypto；可通过 `OPENSSL_USE_STATIC_LIBS=OFF` 改用动态库。此次数据接入协议要求所有 Node/Agent 统一升级，不兼容旧版。Node 删除独立 `tls` 数据配置，TCP/TLS 共用原 `tcp` 端口、容量和限速；升级前删除 Node 配置中的 `tls` 段。Agent 凭据不变，双方对应 service/forward 的 `protocol: "tls"` 即启用端到端加密。


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
- Debian revision 使用 CMake cache 变量 `RELAYWEAVE_PACKAGE_RELEASE`，默认值为 `3`。

应用输出的版本、CMake project version 和包的 upstream version 来自同一 `PROJECT_VERSION`。只修改 Debian 构建或维护脚本而应用版本不变时，可以递增 revision，例如从 `1.0.0-1` 变为 `1.0.0-2`。

Dashboard 是纯 Python 包，架构固定为 `all`。构建时只复制源码，不绑定构建机的 Python 版本；运行时使用系统 `python3` 及发行版提供的 Flask、cbor2 和 Waitress。安装包中的 Python 最低版本约束（当前为 `>= 3.10`）不是固定版本要求，部署端无需与构建机使用相同的 Python 版本。其他包的架构由 CPack/DPKG 根据当前构建目标自动填写，例如 `amd64`、`arm64` 或 `armhf`。包文件名始终包含架构；交叉编译时必须确保工具链目标与 Debian architecture 一致。

## 2. 构建环境

最低要求：

- CMake 3.24；
- 支持 C++20 的编译器；
- 静态模式需要 `vendor/openssl-<版本>/` 源码目录中的 OpenSSL 3.2+ 稳定版本，以及 C 编译器和 Perl 5；Unix/Linux/macOS 使用 Make，Windows MSVC 使用 NMake、原生 Windows Perl（如 Strawberry Perl）和 NASM；
- 动态模式需要系统提供 OpenSSL 3.2+ 开发头文件和共享库，无需 vendor OpenSSL 源码及其构建工具；
- Unix 上需要 pthread；
- 生成 Deb 包需要 Debian 打包工具，包括 `dpkg-shlibdeps`；
- Bash，用于项目的 Linux 包装脚本。

Debian/Ubuntu 可以安装：

```console
sudo apt update
sudo apt install build-essential cmake perl dpkg-dev
```

Asio 1.38.2 和 JSON 3.12.0 优先使用本地 `vendor/asio-1.38.2` 和 `vendor/json-3.12.0`，缺失时由 FetchContent 下载源码压缩包并解压到构建目录。也可通过 `FETCHCONTENT_SOURCE_DIR_ASIO` 和 `FETCHCONTENT_SOURCE_DIR_NLOHMANN_JSON` 指定已有源码目录。

OpenSSL 来源由 `OPENSSL_USE_STATIC_LIBS` 决定：

- `ON`（默认）：从 vendor 源码构建静态库，选取版本最高、至少为 3.2 的稳定版本；缺失、版本过旧或构建失败时直接报错，不回退系统库，也不使用 `OPENSSL_ROOT_DIR` 或缓存的系统库路径。支持 Unix 和 Windows MSVC 本机构建，不支持交叉编译。
- `OFF`：直接使用系统 OpenSSL 共享库，通过 `find_package(OpenSSL 3.2 REQUIRED COMPONENTS SSL Crypto)` 检查头文件和库；跳过 vendor 源码及其构建工具检查，不构建自有动态库。系统版本不足时配置失败。可用 `-DOPENSSL_ROOT_DIR=/path/to/openssl` 指定已安装的 OpenSSL 前缀。Debian/Ubuntu 动态构建还需安装版本满足要求的 `libssl-dev`。

OpenSSL 源码没有 `CMakeLists.txt`，项目通过 CMake `ExternalProject_Add` 在构建阶段执行 Perl `Configure`、Make 和 `install_sw`。直接使用 `vendor/openssl-3.5.9/` 中已有的源码，运行：

```console
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 2
```

源码目录保持干净，不能包含已经在源码树内构建的 OpenSSL 配置。编译和安装产物位于 `build/_deps/openssl-3.5.9-static/`，不会覆盖系统 OpenSSL，也不需要 sudo 或在线下载；项目不处理源码压缩包。Unix 上 OpenSSL 的并行数默认是 `1`，可在配置时通过 `-DRELAYWEAVE_OPENSSL_BUILD_JOBS=2` 调整；它独立于外层 CMake 的并行数，Windows NMake 使用串行构建。vendor 构建关闭测试、命令行程序、动态模块和压缩支持，保留项目所需的 TLS/RPK 功能。

Windows x64 在 Visual Studio 2022 的 **Developer PowerShell / x64 Native Tools Command Prompt** 中运行，先将原生 Windows Perl 和 NASM 的目录加入当前终端的 `PATH`：

```powershell
cmake -S . -B build-win -G "Visual Studio 17 2022" -A x64
cmake --build build-win --config Release --parallel 4
ctest --test-dir build-win -C Release --output-on-failure
```

配置和构建均须使用对应架构的 VS 开发环境，使 `cl`、`nmake`、Windows SDK 头文件和库可用；Git/MSYS 的 Perl 不能替代原生 Windows Perl。CMake 根据目标架构选择 `VC-WIN64A`、`VC-WIN32` 或 `VC-WIN64-ARM`，使用 NMake 编译，补齐 OpenSSL 静态链接所需的 Windows 系统库。Debug/Release 等配置的构建目录及安装目录分别位于 `build/<配置>/` 和 `install/<配置>/`，均在上述 OpenSSL 产物根目录内；单配置生成器未指定构建类型时，OpenSSL 使用 Release 目录。OpenSSL 的运行库选项跟随 `CMAKE_MSVC_RUNTIME_LIBRARY`，未设置时默认 Release 使用 `/MD`，Debug 使用 `/MDd`；如需静态 CRT，可按 CMake 标准设置该变量，显式设为空时不追加运行库选项。Windows 本机构建不生成 Deb 包。

vendor 静态构建固定包含 `no-zlib no-zstd no-brotli`，不查找或链接这些压缩库，也不需要安装它们的开发包。动态模式链接系统 libssl/libcrypto，压缩库依赖由系统共享库处理，项目无需显式链接。部署动态版本时，运行环境必须提供匹配的 OpenSSL 3.2+ 共享库。RPK 接口从 OpenSSL 3.2 开始提供，最低版本要求为 3.2。

在已有构建目录切换 OpenSSL 链接方式时，直接重新配置即可：

```console
cmake -S . -B build -DOPENSSL_USE_STATIC_LIBS=OFF
```

动态模式配置时会重新查找系统 OpenSSL 头文件和库，避免沿用旧缓存的静态库路径；切换方式无需手动清理缓存。Deb 中的 `openssl` 命令行工具供证书部署脚本使用，与程序链接的 libssl/libcrypto 分开，不设置相同的版本要求。

Linux 上 GCC 的 `libstdc++` 和 `libgcc` 也被静态链接，glibc 等其他系统库仍按动态依赖处理。CPack 启用 `dpkg-shlibdeps` 自动计算三个包的共享库依赖，并显式依赖 `systemd`。Agent/Node 包保留 `openssl` 命令行工具依赖，供证书部署脚本使用；默认静态模式无需部署 OpenSSL 3.2+ 共享库。

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
  -DRELAYWEAVE_PACKAGE_RELEASE=2
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

Git 对 `packaging/` 文件统一要求 LF 换行；CMake 还会把 `conffiles`、`postinst`、
`prerm`、`postrm` 和 Dashboard 启动脚本生成到构建目录并统一为 LF，CPack 使用这些副本。
这样即使源码来自 CRLF 检出或压缩包，维护脚本的解释器路径仍是有效的 `/bin/sh`。
`1.0.4-2` 是包含该修复的包修订号；复用旧构建目录时需显式指定
`-DRELAYWEAVE_PACKAGE_RELEASE=2`，以覆盖缓存中的旧值。

## 4. 安装内容

| 内容 | Agent 包 | Node 包 | Proxy 包 |
|---|---|---|---|
| 程序 | `/usr/bin/relayweave-agent` | `/usr/bin/relayweave-node` | `/usr/bin/relayweave-proxy` |
| 配置 | `/etc/relayweave/agent.json` | `/etc/relayweave/node.json` | `/etc/relayweave/proxy.json` |
| systemd unit | `/lib/systemd/system/relayweave-agent.service` | `/lib/systemd/system/relayweave-node.service` | `/lib/systemd/system/relayweave-proxy.service` |
| 证书目录 | `/etc/relayweave/certs/` | `/etc/relayweave/certs/` | 不需要 |
| 部署工具 | `/usr/sbin/relayweave-provision-agent` | `/usr/sbin/relayweave-provision-node` | `/usr/sbin/relayweave-provision-proxy` |

四个组件包只安装运行文件和部署工具，不安装仓库中的文档、README 或 requirements.txt。文档从源码仓库查阅。route 模块静态链接到 Node 和 Agent，不单独打包；测试、诊断程序及测试证书不安装。

Node 的 UDP 建立超时使用 `udp.setup_timeout_ms`，默认 10000 毫秒；旧 `udp.service_wait_timeout_ms` 已删除，升级已有配置时需要移除旧字段。服务离线仅在 Agent 本地等待，不分配 Node 资源。

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

随后编辑当前角色的 `/etc/relayweave/node.json` 或 `/etc/relayweave/agent.json`，将签发端生成的本机证书压缩包传到目标机器。完整流程是：安装 Deb → 编辑配置 → 安装证书 → 手工启动服务。

```console
# 无参数进入证书安装/更新/卸载菜单，只运行当前主机所需角色。
sudo relayweave-provision-node
sudo relayweave-provision-agent
```

命令方式同样支持首次安装和更新：

```console
relayweave-provision-node --install-certs ./node-master-1.tar.gz --dry-run
sudo relayweave-provision-node --install-certs ./node-master-1.tar.gz
sudo systemctl start relayweave-node

relayweave-provision-agent --install-certs ./client.tar.gz --dry-run
sudo relayweave-provision-agent --install-certs ./client.tar.gz
sudo systemctl start relayweave-agent
```

更新后需要加载新证书时手工执行 `systemctl restart relayweave-node` 或 `relayweave-agent`。证书流程不调用 systemd，不覆盖配置；`--remove-certs` 备份并卸载当前角色证书，也不改变服务状态。Node 只安装四个证书文件，Agent 只安装三个；具体内容和生成方法见 [证书制作与部署](证书制作与部署.md)。Node/Agent 同机且共用证书目录时，卸载一方保留另一方使用的 Server CA。`--cert-dir DEST` 可指定目标目录，配置中的路径须与之对应。

覆盖或卸载前备份到 `/var/backups/relayweave/<role>-<UTC时间>-<随机后缀>/`；写入失败时尝试恢复。`--dry-run` 验证文件但不修改目标或服务，无需 root。

原有完整部署接口继续保留：

```console
sudo relayweave-provision-node \
  --cert-dir /secure/relayweave/node-certificates \
  --config /secure/relayweave/config/master-1.json \
  --verify-host proxy.example.com --start

sudo relayweave-provision-agent \
  --cert-dir /secure/relayweave/client-certificates \
  --config /secure/relayweave/config/agent.json --start

sudo relayweave-provision-proxy --config /secure/relayweave/config/proxy.json --start
```

这里的 Node/Agent 证书目录来自对应压缩包的解压结果；Agent 若额外提供 `client-ca.pem` 则做签发链验证，但不安装该文件。完整部署接口默认不改变服务状态；只有显式的 `--start` 执行 `systemctl enable --now`，`--restart` 执行 `systemctl try-restart`。`--verify-host` 仅适用于 Node，可重复指定。

Proxy 和 Dashboard 的接口保持原样。Proxy 不需要证书；Dashboard 仍使用 `relayweave-provision-dashboard --cert-dir DIR --config FILE [--dry-run] [--start|--restart]`，输入需包含 Client CA 公钥证书以验证客户端签发链，安装到独立的 `/etc/relayweave/dashboard-certs/`；详见 [Dashboard 部署说明](../dashboard/README.md#独立服务部署)。

检查状态和日志：

```console
systemctl status relayweave-node.service
journalctl -u relayweave-node.service -e
systemctl status relayweave-agent.service
journalctl -u relayweave-agent.service -e
systemctl status relayweave-proxy.service
journalctl -u relayweave-proxy.service -e
```

### 5.1 TCP 拥塞控制和收发缓冲

在源码目录运行 `scripts/configure_tcp.py`，可持久化主机的 TCP 拥塞算法和收发缓冲上限。默认选择 CUBIC，TCP 发送、接收自动调优上限各为 32 MiB，保留原有最小值和初始值，并启用接收缓冲自动调优。通用 socket 收发缓冲上限至少提高到对应值；这些是按需使用的上限，不是每连接预分配内存。

```console
# 只预览，无需 root，不修改配置或运行参数。
python3 scripts/configure_tcp.py --dry-run

# 写入持久配置并立即加载、核对结果。
sudo python3 scripts/configure_tcp.py

# 自定义算法及发送、接收上限，单位 MiB。
sudo python3 scripts/configure_tcp.py --congestion-control cubic \
  --send-buffer-mib 32 --receive-buffer-mib 64
```

算法必须已出现在 `sysctl net.ipv4.tcp_available_congestion_control` 中。脚本写入 `/etc/sysctl.d/99-zz-relayweave-tcp.conf`，在标准 sysctl.d 加载顺序中晚于常见的 `99-bbr.conf`，不改变 qdisc。若另有更晚加载的自定义配置或服务修改相同参数，需要同步处理其覆盖关系。

每次应用前，将本脚本配置文件的原内容和相关运行参数备份到 `/var/backups/relayweave/tcp/<时间戳>-<随机后缀>/snapshot.json`。加载或读回验证失败时自动回滚；手动恢复时使用脚本打印的备份目录，恢复操作本身也会先备份当前状态：

```console
sudo python3 scripts/configure_tcp.py --restore /var/backups/relayweave/tcp/<备份目录>
```

更换默认拥塞算法后，重新启动 benchmark 或 Node 进程以重建监听 socket 和 TCP 连接；已有连接不会自动切换算法。应用可自行设置 socket 参数，覆盖系统默认值。该脚本由管理员手动执行，安装包不自动修改系统网络参数。

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
- 升级或重复安装不重启服务，已有进程继续运行；
- 原本停止的服务不会因为升级被意外启动；
- 保留原有开机启动设置，不重新 enable 或 disable 服务；
- 四个组件是独立包，可以分别升级。

软件文件在安装时更新。需要加载新程序和配置时，手动执行 `sudo systemctl restart relayweave-<role>`；也可以等下次开机启动服务，前提是对应服务保持 enabled。`systemctl daemon-reload` 只重新读取 unit 定义，不会重启已有进程。这一策略控制包安装脚本的启停行为，不提供整套文件延迟替换机制。

链路质量协议使用 `quality: {cost, confidence, usable}`，Node、Agent、Dashboard 应统一升级，不支持
混用旧质量格式。Agent 的 routing 配置默认启用且只接受可选 `max_nodes`（默认 4，范围 1–8）；
旧配置中的 `routing.enabled` 必须删除，dpkg 保留的 conffile 不会自动完成这一修改。

Node 配置需要将旧 `cluster.port` 改为必填 `cluster.control_port`，并增加必填的
`cluster.tcp_port/udp_port`。所有 Node 的两个数据端口一致，不能与各自普通数据监听冲突。
新的部署工具使用同版本 `relayweave-node --check-cluster-config FILE` 验证这些字段。
Node 升级时先停止服务、安装新包、迁移配置，再使用部署工具 `--start`；保留的 conffile 不会自动迁移。

### 6.1 协议与配置迁移边界

软件包可以分别安装，但跨进程协议变更仍须按下表协调部署；包安装完成也不表示运行中的进程已加载新程序。

| 变更 | 协调升级范围 | 迁移与检查 |
|---|---|---|
| 删除节点定位命令与 `service.located.port` | 使用这些旧字段的 Agent 与 Node | 旧 Agent 若强制读取 port，不能继续配套使用；没有旧命令兼容分支。 |
| Flow 事务精简 | Master 与全部 Slave | Prepare 传完整授权，后续只传事务身份及诊断；须同步升级，不能以 Agent 仍可连接作为集群兼容证明。 |
| 端到端 TLS 与控制绑定 | 全部 Node 与 Agent | 删除 Node 的 `tls` 段，统一使用 `tcp` 端口、容量和限速；流式接入改为读取 32 字节编号后经控制 mTLS 提交 attach。旧数据 TLS、数据 socket 票据和 `tls_framing` 协商均已移除，无明文回退。 |
| 质量格式与集群端口 | Node、Agent、Dashboard 及对应配置 | 按本节迁移 quality、routing 和 cluster 端口字段；conffile 保留不自动转换内容。 |

端到端 TLS 的两端继续使用同一套共享客户端证书与私钥，只需将对应 service/forward 的 protocol 设为 tls。无需给 Producer 另签 serverAuth 证书；若轮换了共享私钥，互联双方必须同时更新，详见 [端到端公钥与凭据轮换](证书制作与部署.md#26-端到端公钥与凭据轮换)。

OpenSSL 部署以第 2 节当前链接方式为准：默认静态链接时从 vendor 中已有的 OpenSSL 3.2+ 源码构建，运行机无需同版本共享库；选择动态链接时构建机和运行机须提供匹配的系统库。历史改造记录中的运行库依赖不替代当前打包规则。

多跳准备失败的诊断 stage 已由早期 attach/bridge 收敛为 prepare；按旧 stage 字面值分类的日志工具需同步调整。会话内部的控制器删除、数据任务合并和所有权调整没有另加兼容协议。

### 6.2 升级核验与回退

升级后检查：

```console
dpkg-query -W 'relayweave-*'
systemctl --no-pager --full status relayweave-node.service relayweave-agent.service relayweave-proxy.service
```

回退时可以重新安装保留的旧包，但必须确认当前配置仍兼容旧程序。

配置或证书与软件同时更新时，推荐先用部署工具验证并写入新文件但不操作服务，再安装新 Deb，最后在合适的时机手动重启服务以加载全部新内容：

```console
sudo relayweave-provision-node --cert-dir /secure/relayweave/nodes/master-1 \
  --config /secure/relayweave/config/master-1.json --verify-host proxy.example.com
sudo apt install ./relayweave-node_1.1.0-1_arm64.deb
sudo systemctl restart relayweave-node
```

只轮换配置或证书、不升级软件时使用 `--restart`。原文件备份用于人工回退，工具不会自动恢复旧版本。

## 7. 服务脚本生命周期

| dpkg 阶段 | 行为 |
|---|---|
| 首次 `postinst configure` | reload systemd，enable 服务，不启动 |
| 升级或重复安装 `postinst configure <old-version>` | reload systemd，不启动或重启服务，保留开机启动设置 |
| `prerm remove/deconfigure` | disable 并停止服务 |
| `postrm remove/purge/...` | reload systemd 并清除 failed 状态 |

unit 使用 `Restart=on-failure` 和 5 秒重启间隔，并启用 `NoNewPrivileges=true`、`PrivateTmp=true`。Node 和 Agent 使用 systemd 系统服务的默认身份运行，AmbientCapabilities 和 CapabilityBoundingSet 均保留 `CAP_NET_RAW` 与 `CAP_NET_BIND_SERVICE`，用于原始 ICMP 和低端口监听。两者的安装脚本与部署工具检查 ICMP 能力，包含 systemd override 的实际生效配置；升级会 reload unit，但不会重启原本运行的服务。

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

### dpkg 无法执行 postinst，报告 No such file or directory

这个错误也可能表示脚本首行指定的解释器不存在。先检查脚本和 `/bin/sh`：

```console
ls -l /var/lib/dpkg/info/relayweave-node.postinst /bin/sh
head -n 1 /var/lib/dpkg/info/relayweave-node.postinst | cat -v
head -n 1 /var/lib/dpkg/info/relayweave-agent.postinst | cat -v
```

如果首行显示 `#!/bin/sh^M`，说明包内脚本是 CRLF；内核会尝试执行不存在的 `/bin/sh\r`。
确认后可修正已安装角色的维护脚本，再完成挂起的配置：

```bash
for package in relayweave-node relayweave-agent relayweave-proxy relayweave-dashboard; do
    for hook in postinst prerm postrm; do
        script=/var/lib/dpkg/info/$package.$hook
        [ -f "$script" ] || continue
        sudo sed -i 's/\r$//' "$script"
        sudo chmod 755 "$script"
    done
done
sudo dpkg --configure -a
```

修正 `prerm` 和 `postrm` 是为了让后续升级和卸载也能执行。此操作不会替换 JSON 配置；
配置阶段会按原有规则重启当前 active 的服务。如果脚本确实缺失或 `/bin/sh` 损坏，
应分别重新解包修复版软件包或修复系统解释器。

随后使用修复版包，例如：

```console
sudo apt install ./relayweave-node_1.0.4-2_amd64.deb ./relayweave-agent_1.0.4-2_amd64.deb
```

再次 `apt install` 同版本的 `1.0.4-1` 会跳过解包，仅重试原来的 `postinst`，所以不会
自动修复。若重新生成的包仍沿用同一版本号，需要 `apt install --reinstall ./package.deb`。
`_apt` 无法访问 `/root` 包文件的 Notice 与维护脚本执行失败无关。

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

Dashboard 通过 `relayweave-dashboard` 包独立安装，由专用用户运行。配置位于 `/etc/relayweave/dashboard.json`，证书位于 `/etc/relayweave/dashboard-certs/`，历史数据库位于 `/var/lib/relayweave-dashboard/history.sqlite3`。首次安装启用 unit 但不立即启动；准备证书并编辑配置后执行 `sudo systemctl enable --now relayweave-dashboard`。包升级或重复安装不启动或重启看板服务，保留开机启动设置；手动重启或下次开机启动时加载新程序。卸载不主动清理历史库。

服务默认监听服务器的 `127.0.0.1:5000`。由同机 Agent 的 `services` 发布 `dashboard`，客户端 Agent 用 `forwards` 监听 `127.0.0.1:15000`，浏览器即可访问 `http://127.0.0.1:15000/`。Node 无需新增功能或配置字段，客户端无需安装 Python。详细的证书权限、发布/消费配置和历史迁移步骤见 [Dashboard 部署说明](../dashboard/README.md#独立服务部署)。
