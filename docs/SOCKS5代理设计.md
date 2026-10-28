# SOCKS5 代理

`relayweave-proxy` 的 SOCKS5 入口支持无认证协商、TCP CONNECT 和 UDP ASSOCIATE。
TCP 和 UDP 的目标地址均可为 IPv4、IPv6 或域名；域名在代理出口解析。
BIND、用户名密码及 GSSAPI 认证尚未实现，不将当前实现称为完整的 SOCKS5 标准实现。
协议依据为 [RFC 1928](https://www.rfc-editor.org/rfc/rfc1928.txt)。

## UDP 关联与所有权

客户端在 SOCKS5 TCP 连接上完成协商并发送 UDP ASSOCIATE。
请求中的地址和端口描述客户端的 UDP 来源，不是业务目标；不知道来源时可以填写全零。
代理回复一个独立 UDP 接收地址和动态端口，客户端随后向该端点发送 SOCKS5 UDP 报文。

`ProxyServer` 持有活动 `ProxySession`；UDP Session 按值持有唯一 `Socks5UdpAssociation`。
关联借用所属 Session 的 TCP socket，直接持有客户端接收 socket、IPv4/IPv6 出口 socket、
UDP resolver 和两个有界 LRU 缓存。所有状态都在 Proxy 的单线程执行器访问，不增加锁或 strand。
IPv6 出口不可用时仍提供 IPv4 UDP 转发。

`run()` 使用独立的 TCP 生命周期、客户端收包、IPv4/IPv6 目标收包协程链。
任何链结束后取消并等待其他链完成，Session 才能被移除；TCP EOF 和 Proxy stop 都关闭 UDP socket
并取消解析任务。TCP 半关闭写方向同样结束关联。UDP 空闲不会单独解除仍存活的 TCP 关联。

## UDP 报文与隔离

请求和回复的 UDP 封装为：

```text
RSV(2) | FRAG(1) | ATYP(1) | 地址(可变) | 端口(2) | 载荷
```

- 只接受 `RSV=0`、`FRAG=0`。不实现可选的 SOCKS5 应用层分片重组，非零 FRAG 丢弃。
- 支持 IPv4、IPv6、非空 ASCII 域名，拒绝截断头、非法地址类型及业务目标端口零。
- UDP 来源 IP 必须等于 TCP 对端。请求给出非零来源端口时必须匹配；未知端口在第一个头部有效的
  客户端报文上固定，同一关联不接受其他来源端口抢占。
- 客户端与目标使用不同 socket 收包。出口只向本关联近期请求过的目标发送回复，防止未请求的来源注入。
- 每个关联拥有独立的入口端口和出口 socket，多个客户端及同源 IP 的不同关联互相隔离。
- 回复保留目标实际来源 IP 和端口；不把代理入口地址当成目标来源。
- 单个 SOCKS5 UDP 报文上限为 65507 字节，IPv4 头开销为 10 字节，IPv6 为 22 字节；
  载荷和头部合计超限时丢弃。支持空 UDP 载荷。
- 解析失败、目标不可达或报文不合法时丢弃当前报文；合法后续报文仍可使用关联。

## 容量与配置

兼容原有配置。`proxy` 下新增两个可选字段：

| 字段 | 默认值 | 作用 |
|---|---|---|
| `udp_max_targets` | `256` | 每个关联的域名解析缓存和目标授权缓存各自的容量，范围 1..4096 |
| `udp_target_timeout_ms` | `60000` | 两个缓存的 TTL；只在报文事件中检查和淘汰，不启动周期定时器 |

目标授权在客户端发送时刷新 TTL，收到回复不会延长授权。解析缓存命中只更新 LRU 顺序，
不延长 DNS 缓存 TTL；该 TTL 为本地缓存策略，不读取 DNS 响应中的 TTL。
超过容量时淘汰最近最少使用条目。每个 UDP 关联占用一个 `max_connections` 名额，
与 HTTP、TCP CONNECT 共用容量限制。解析超时沿用 `connect_timeout_ms`。

## 使用方式

启动 `relayweave-proxy proxy.json` 后，客户端连接配置中的 SOCKS5 TCP 入口，选择无认证方法并发送
UDP ASSOCIATE。客户端读取成功回复中的 BND.ADDR/BND.PORT，再直接向该 UDP 端点发送带目标地址的报文。
客户端须保持 TCP 控制连接打开；服务端返回的 UDP 地址和端口须能从客户端访问。

回复中的动态端口由每个关联独立绑定，结束关联后释放。数据收发遵循标准 SOCKS5 UDP 格式，
不提供 UDP-over-TCP 等私有扩展。

## 验证

`socks5_udp` 测试通过真实 TCP/UDP socket 验证 IPv4/IPv6 入口、跨地址族转发、域名、空载荷和最大报文，
以及非法头、分片丢弃、来源端口固定、多关联隔离、回复来源检查、缓存容量/过期、TCP 关闭和 Proxy stop。
`proxy` 保留 HTTP、CONNECT、SOCKS5 TCP、超时和停止回归；原先拒绝 UDP ASSOCIATE 的断言改为拒绝 BIND。
