# hy-client

Hysteria v2 代理管理 Client —— 常驻进程，部署于每个代理节点，负责管理本地 Hysteria v2 代理、统计上报流量、接收 Server 指令踢用户下线。

**特点：**
- 不直连 Redis，所有数据读写通过 Server HTTP API 完成
- 使用 Go 1.22+ 标准库（net/http），零第三方 HTTP 客户端依赖
- 本地缓存采用纯 JSON 文件，无 SQLite
- systemd + 静态二进制部署

---

## 架构

```
                        ┌─────────────────────────────┐
                        │        hy-client             │
                        │  (每个代理节点常驻进程)        │
                        │                               │
  ┌───────────────────► │  ┌─────────┐  ┌───────────┐  │
  │ Hysteria v2 本地 API │  │Reporter │  │  Kicker   │  │
  │ GET  /traffic       │  │10s 周期  │  │15s 周期   │  │
  │ GET  /online        │  │差值计算  │  │踢人 + ACK │  │
  │ POST /kick          │  └────┬─────┘  └────┬─────┘  │
  └─────────────────────┘       │             │        │
                                │  ┌─────────┐ │        │
                                │  │Heartbeat│ │        │
                                │  │30s 周期 │ │        │
                                │  └────┬────┘ │        │
                                │       │       │        │
                                │  ┌────▼────┐ │        │
                                │  │ 缓存重放 │ │        │
                                │  │(30s 周期)│ │        │
                                │  └─────────┘ │        │
                                └──────┬────────┘        │
                                       │ HTTP (HMAC签名)  │
                            ┌──────────▼───────────────┐│
                            │  Server HTTP API          ││
                            │  POST /api/v1/traffic/report
                            │  GET  /api/v1/kick/list
                            │  POST /api/v1/kick/ack
                            │  POST /api/v1/node/heartbeat
                            └───────────────────────────┘
```

---

## 安装

### 快速安装

```bash
# 编译
cd hy-client
go build -o hy-client .

# 部署二进制和配置
cp hy-client /usr/local/bin/
mkdir -p /etc/hy-client /var/lib/hy-client
cp config.yaml /etc/hy-client/config.yaml
cp hy-client.service /etc/systemd/system/

# 赋权
chmod +x /usr/local/bin/hy-client
chmod 644 /etc/systemd/system/hy-client.service

# systemd 托管
systemctl daemon-reload
systemctl enable --now hy-client
```

### 手动启动（调试）

```bash
# 直接运行（foreground）
./hy-client -config /etc/hy-client/config.yaml

# 指定日志等级
./hy-client -config /etc/hy-client/config.yaml
```

---

## 配置 (`/etc/hy-client/config.yaml`)

```yaml
node_id: node-01                  # 节点唯一标识
server_url: http://127.0.0.1:8080 # Server HTTP 根地址
server_secret: "your-hmac-secret" # HMAC-SHA256 密钥（必须与 Server 一致）

hysteria:
  api_url: http://127.0.0.1:9999 # 本地 Hysteria v2 API 地址
  secret: "your-hysteria-secret" # Hysteria API Authorization secret

intervals:
  traffic_report: 10s   # 流量采集与上报周期
  kick_check: 15s       # 踢人列表检查周期
  heartbeat: 30s        # 心跳上报周期

cache:
  path: /var/lib/hy-client/cache.json # 本地补报缓存文件
  max_entries: 10000                  # 缓存最大条目数

log:
  level: info  # debug | info | warn | error
```

---

## 命令行参数

```
-config string   配置文件路径 (默认: /etc/hy-client/config.yaml)
-version         打印版本并退出
```

---

## 信号处理

| 信号      | 行为                                       |
|-----------|--------------------------------------------|
| SIGTERM   | 停止所有协程 → 缓存落盘 → 退出             |
| SIGINT    | 同 SIGTERM                                 |
| SIGHUP    | 重新加载配置（不重启进程，更新 intervals 等） |

**注意：** `kill -9` 无法捕获。不过本地缓存文件采用原子写（写 .tmp → fsync → rename），不会损坏。

---

## Server 接口契约（Client 视角）

### 1. 流量上报

**请求：** `POST /api/v1/traffic/report`

```json
{
  "node_id": "node-01",
  "timestamp": 1740000000,
  "users": [
    {"user_id": "u1", "upload": 1024, "download": 2048}
  ]
}
```

**响应：**

```json
{"ok": true, "quota_exceeded": []}
```

### 2. 踢人列表

**请求：** `GET /api/v1/kick/list?node_id=node-01`

**响应：**

```json
{"kick": [{"user_id": "u1", "reason": "quota_exceeded"}]}
```

`reason` 取值：`quota_exceeded` / `admin_kick` / `expired`

### 3. 踢人确认

**请求：** `POST /api/v1/kick/ack`

```json
{"node_id": "node-01", "user_ids": ["u1"]}
```

### 4. 心跳

**请求：** `POST /api/v1/node/heartbeat`

```json
{"node_id": "node-01", "version": "1.0.0", "online_users": 5}
```

响应 `{ok, interval}` 中的 `interval` 为 Server 建议的上报间隔（当前版本暂不实现动态调整）。

---

## Hysteria v2 本地 API（必需）

Client 依赖本地 Hysteria v2 进程暴露以下 HTTP API（默认 `http://127.0.0.1:9999`），所有请求需带 `Authorization: <secret>` 头：

| 方法   | 路径      | 说明                                       |
|--------|-----------|--------------------------------------------|
| GET    | /traffic  | 返回累计流量 `{userid: {tx, rx}, ...}`    |
| GET    | /online   | 返回在线用户 `{userid: device_count, ...}` |
| POST   | /kick     | Body: `["uid1","uid2"]`，断开指定用户连接   |

**注意：** `/kick` 只是断连，客户端会自动重连。真正阻止用户需 Server 侧认证后端配合。

---

## 关键边界情况

### 计数器重置（Hysteria 重启）

当 `cur.tx < last.tx` 或 `cur.rx < last.rx` 时，视为 Hysteria 重启或清零，直接上报当前值 `cur`，而非增量 0。

### 网络中断后的补报

后台协程每 30 秒读取本地缓存 `cache.json`，按时间顺序重放未上报的流量条目，成功后删除。重试采用指数退避（初始 1s，上限 60s，带 jitter）。

### 缓存原子写入

```
写 .tmp 文件 → fsync → os.Rename(覆盖 cache.json)
```

`kill -9` 可能留下 `.tmp` 文件残留，但 `cache.json` 始终是完整的 JSON。

### 时钟漂移

签名的 timestamp 使用 Client 本地时间。Server 应验证 timestamp 在合理窗口内（如 ±5 分钟），超出则拒绝。

### /kick 后用户重连

Client 只负责调用 `/kick` + ACK。若用户立即重连，下一个 15s 周期会再次踢——形成闭环。真正的阻止依赖 Server 侧认证后端。

---

## 日志

JSON 格式输出到 stdout（systemd 托管时自动进入 journal）：

```bash
journalctl -u hy-client -f
```

---

## 许可

MIT
