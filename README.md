# Metabase MCP Server

让 AI 助手（Claude、Cursor 等）直接操作你的 Metabase —— 查询数据库、执行 SQL、管理看板和问题卡片。

---

## 快速开始

### 方式一：Docker（推荐）

```bash
docker run -d \
  -p 8000:8000 \
  -e METABASE_URL=https://metabase.example.com \
  -e METABASE_API_KEY=mb_xxxx \
  --name metabase-mcp \
  docker.cnb.cool/debug.icu/mcp/metabase:latest
```

然后在你的 MCP 客户端填入连接地址即可（见下方「连接配置」）。

### 方式二：下载二进制

从 [Releases](../../releases) 下载对应平台的二进制文件，直接运行：

```bash
# HTTP 模式（供远程客户端连接，默认）
METABASE_URL=https://metabase.example.com \
METABASE_API_KEY=mb_xxxx \
./metabase-mcp --transport http

# stdio 模式（供 Claude Desktop / Claude Code 等本地客户端使用）
METABASE_URL=https://metabase.example.com \
METABASE_API_KEY=mb_xxxx \
./metabase-mcp --transport stdio
```

---

## 连接配置

### Claude Code（stdio 模式）

使用 `claude mcp add` 命令一键注册，支持项目级和全局两种作用域。

**使用 `METABASE_CREDENTIALS`（推荐，避免明文）**

先生成 base64url 编码的凭据：

```bash
# API Key
echo -n 'apikey:mb_xxxx' | base64 | tr '+/' '-_' | tr -d '='

# 邮箱 + 密码
echo -n 'alice@example.com:password' | base64 | tr '+/' '-_' | tr -d '='
```

再注册 MCP server：

```bash
# 注册到当前项目（配置保存在项目 .mcp.json）
claude mcp add metabase \
  -e METABASE_URL=https://metabase.example.com \
  -e METABASE_CREDENTIALS=YXBpa2V5Om1iX3h4eHg \
  -- /path/to/metabase-mcp --transport stdio

# 注册为全局可用（所有项目共享）
claude mcp add metabase --scope user \
  -e METABASE_URL=https://metabase.example.com \
  -e METABASE_CREDENTIALS=YXBpa2V5Om1iX3h4eHg \
  -- /path/to/metabase-mcp --transport stdio
```

**使用明文凭据**

```bash
# API Key
claude mcp add metabase \
  -e METABASE_URL=https://metabase.example.com \
  -e METABASE_API_KEY=mb_xxxx \
  -- /path/to/metabase-mcp --transport stdio

# 邮箱 + 密码
claude mcp add metabase \
  -e METABASE_URL=https://metabase.example.com \
  -e METABASE_USER_EMAIL=alice@example.com \
  -e METABASE_PASSWORD=your-password \
  -- /path/to/metabase-mcp --transport stdio
```

注册后验证是否生效：

```bash
claude mcp list
claude mcp get metabase
```

### Claude Code（HTTP 模式，连接远程服务器）

```bash
# 使用 API Key
claude mcp add metabase --transport http \
  "http://apikey:mb_xxxx@your-server:8000/mcp"

# 使用邮箱 + 密码（@ 需编码为 %40）
claude mcp add metabase --transport http \
  "http://alice%40example.com:password@your-server:8000/mcp"
```

### Claude Desktop（stdio 模式）

编辑 `claude_desktop_config.json`：

```json
{
  "mcpServers": {
    "metabase": {
      "command": "/path/to/metabase-mcp",
      "args": ["--transport", "stdio"],
      "env": {
        "METABASE_URL": "https://metabase.example.com",
        "METABASE_CREDENTIALS": "YXBpa2V5Om1iX3h4eHg"
      }
    }
  }
}
```

### Claude Desktop / Cursor（HTTP 模式，连接远程服务器）

在客户端填入连接 URL，凭据直接写在 URL 里：

```
# 使用 API Key（推荐）
http://apikey:mb_xxxx@your-server:8000/mcp

# 使用邮箱 + 密码（@ 需编码为 %40）
http://alice%40example.com:password@your-server:8000/mcp
```

> **多用户场景**：一个服务端实例可以同时服务多个用户，每个人在 URL 里填自己的凭据即可，互不干扰。

### 其他凭据传递方式

如果客户端不支持在 URL 中携带认证信息，还可以用：

**HTTP 请求头**

```
X-Metabase-Api-Key: mb_xxxx

# 或者邮箱 + 密码：
X-Metabase-Email: alice@example.com
X-Metabase-Password: your-password

# 可选：覆盖服务端默认的 Metabase 地址
X-Metabase-Url: https://other-metabase.example.com
```

**URL 路径编码**（适用于只能填写普通 URL 的客户端）

```bash
# 生成凭据 token
echo -n 'alice@example.com:password' | base64 | tr '+/' '-_' | tr -d '='
# → YWxpY2VAZXhhbXBsZS5jb206cGFzc3dvcmQ

# 连接 URL
http://your-server:8000/YWxpY2VAZXhhbXBsZS5jb206cGFzc3dvcmQ/mcp
```

---

## 可用工具

连接后，AI 助手可以使用以下工具：

### 数据库探索

| 工具 | 功能 |
|------|------|
| `list_databases` | 列出所有已接入的数据库 |
| `list_tables` | 列出指定数据库中的所有数据表 |
| `get_table_fields` | 查看某张表的字段/列定义 |

### 数据查询

| 工具 | 功能 |
|------|------|
| `execute_query` | 对指定数据库执行原生 SQL 查询 |

### 问题卡片（Saved Questions）

| 工具 | 功能 |
|------|------|
| `list_cards` | 列出所有已保存的问题 |
| `get_card` | 查看某个问题的详情 |
| `execute_card` | 运行已保存的问题并返回结果 |
| `create_card` | 创建新的问题卡片 |

### 集合（Collections）

| 工具 | 功能 |
|------|------|
| `list_collections` | 列出所有集合 |
| `get_collection_items` | 浏览集合内的内容 |
| `create_collection` | 新建集合 |

### 看板（Dashboards）

| 工具 | 功能 |
|------|------|
| `list_dashboards` | 列出所有看板 |
| `get_dashboard` | 查看看板详情（含卡片列表） |
| `create_dashboard` | 新建看板 |
| `update_dashboard` | 修改看板信息 |
| `add_card_to_dashboard` | 将问题卡片添加到看板 |
| `update_dashboard_card` | 调整看板中卡片的位置/尺寸 |
| `remove_card_from_dashboard` | 从看板移除卡片 |

---

## 配置项

所有配置通过环境变量设置，支持 `.env` 文件（参考 `.env.example`）。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `METABASE_URL` | — | **必填**。Metabase 实例地址，如 `https://metabase.example.com` |
| `METABASE_CREDENTIALS` | — | base64url 编码的凭据（推荐）。格式见下方说明，优先级高于下面三项 |
| `METABASE_API_KEY` | — | 明文 API Key |
| `METABASE_USER_EMAIL` | — | 明文邮箱（与 `METABASE_PASSWORD` 配合使用） |
| `METABASE_PASSWORD` | — | 明文密码 |
| `METABASE_HTTP_PROXY` | — | 访问 Metabase 时使用的 HTTP 代理地址（留空则不走代理，且忽略系统 `HTTP_PROXY` 环境变量） |
| `TRANSPORT` | `http` | 传输模式：`http` 或 `stdio` |
| `HOST` | `0.0.0.0` | HTTP 监听地址 |
| `PORT` | `8000` | HTTP 监听端口 |
| `MCP_PATH` | `/mcp` | MCP 端点路径 |
| `CLIENT_CACHE_TTL` | `3600` | 每个用户连接的缓存时长（秒） |

### METABASE_CREDENTIALS 格式

对 `apikey:<key>` 或 `<email>:<password>` 做 base64url 编码（无填充）：

```bash
# API Key
echo -n 'apikey:mb_xxxx'            | base64 | tr '+/' '-_' | tr -d '='

# 邮箱 + 密码
echo -n 'alice@example.com:password' | base64 | tr '+/' '-_' | tr -d '='
```

---

## Docker Compose 示例

```yaml
services:
  metabase-mcp:
    image: docker.cnb.cool/debug.icu/mcp/metabase:latest
    ports:
      - "8000:8000"
    environment:
      METABASE_URL: https://metabase.example.com
      METABASE_CREDENTIALS: YXBpa2V5Om1iX3h4eHg
    restart: unless-stopped
```
