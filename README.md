# Metabase MCP Server

A **Model Context Protocol (MCP) server** for [Metabase](https://www.metabase.com/) that exposes database, query, card, collection, and dashboard operations as MCP tools.

Key additions over the reference implementation:

| Feature | Detail |
|---------|--------|
| **HTTP transport** | Uses FastMCP `streamable-http` — accessible via a plain URL |
| **Multi-user support** | Credentials are passed **per-connection** in the URL or headers; a single running server serves many users simultaneously |
| **Connection caching** | `MetabaseClient` instances are cached per credential set (configurable TTL) |
| **Session auto-refresh** | 401 responses trigger a transparent re-login |
| **Docker-ready** | Multi-stage `Dockerfile` with non-root user and health-check |

---

## Quick Start

### 1 — Clone & configure

```bash
cp .env.example .env
# Edit .env: set METABASE_URL and default credentials
```

### 2 — Run directly

```bash
# with uv
uv run python server.py

# or plain Python (after pip install -e .)
metabase-mcp
```

### 3 — Run with Docker

```bash
docker build -t metabase-mcp .

docker run -d \
  -p 8000:8000 \
  -e METABASE_URL=https://metabase.example.com \
  -e METABASE_API_KEY=mb_xxxx \
  --name metabase-mcp \
  metabase-mcp
```

---

## Connecting MCP clients

The server listens on **`http://<HOST>:<PORT><MCP_PATH>`** (default: `http://0.0.0.0:8000/mcp`).

### Method 1 — HTTP Basic Auth in the URL *(recommended)*

This is what happens automatically when you use `http://user:pass@host/path` URLs.

```
# Email + password
http://alice%40example.com:s3cr3t@mcp-server:8000/mcp

# Metabase API key  (use literal "apikey" as the username)
http://apikey:mb_abc123@mcp-server:8000/mcp
```

> `@` in the email must be URL-encoded as `%40`.

### Method 2 — URL path encoding

Encode `email:password` (or `apikey:<key>`) as **base64url**, then prefix the path:

```bash
# Generate the token
python3 -c "import base64; print(base64.urlsafe_b64encode(b'alice@example.com:s3cr3t').decode().rstrip('='))"
# → YWxpY2VAZXhhbXBsZS5jb206czNjcjN0

# Connection URL
http://mcp-server:8000/YWxpY2VAZXhhbXBsZS5jb206czNjcjN0/mcp
```

### Method 3 — Custom HTTP headers

```
X-Metabase-Email: alice@example.com
X-Metabase-Password: s3cr3t

# or for API key:
X-Metabase-Api-Key: mb_abc123

# Optional: override the Metabase instance URL per-request
X-Metabase-Url: https://other-metabase.example.com
```

### Claude Desktop config example

```json
{
  "mcpServers": {
    "metabase": {
      "url": "http://alice%40example.com:s3cr3t@mcp-server:8000/mcp"
    }
  }
}
```

---

## Available Tools

### Database
| Tool | Description |
|------|-------------|
| `list_databases` | List all configured Metabase databases |
| `list_tables` | List tables in a database (formatted Markdown) |
| `get_table_fields` | Get column/field metadata for a table |

### Query
| Tool | Description |
|------|-------------|
| `execute_query` | Run a native SQL query |

### Cards (Saved Questions)
| Tool | Description |
|------|-------------|
| `list_cards` | List all saved questions |
| `get_card` | Get a single card's details |
| `execute_card` | Run a saved question and return results |
| `create_card` | Create a new saved question |

### Collections
| Tool | Description |
|------|-------------|
| `list_collections` | List all collections |
| `get_collection_items` | Browse items inside a collection |
| `create_collection` | Create a new collection |

### Dashboards
| Tool | Description |
|------|-------------|
| `list_dashboards` | List all dashboards |
| `get_dashboard` | Get a dashboard with its cards |

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `METABASE_URL` | **yes** | — | Target Metabase instance URL |
| `METABASE_API_KEY` | no | — | Default API key (fallback when no per-request creds) |
| `METABASE_USER_EMAIL` | no | — | Default email (fallback) |
| `METABASE_PASSWORD` | no | — | Default password (fallback) |
| `HOST` | no | `0.0.0.0` | Server bind address |
| `PORT` | no | `8000` | Server bind port |
| `MCP_PATH` | no | `/mcp` | MCP endpoint path |
| `CLIENT_CACHE_TTL` | no | `3600` | Seconds to cache a user's HTTP client |

---

## Architecture

```
MCP Client (Claude / Cursor / …)
        │  HTTP POST/GET  http://user:pass@server:8000/mcp
        ▼
CredentialsMiddleware  (pure ASGI)
  ├── Extracts creds from Basic Auth / URL path / custom headers
  ├── Stores them in a contextvar  (_request_creds)
  └── Rewrites path if creds were embedded in it
        │
        ▼
FastMCP  (streamable-http transport)
  └── MCP tool handlers
        └── _get_client()  → looks up contextvar → returns cached MetabaseClient
                                                          │
                                                          ▼
                                                   Metabase REST API
```

---

## docker-compose example

```yaml
services:
  metabase-mcp:
    build: .
    ports:
      - "8000:8000"
    environment:
      METABASE_URL: https://metabase.example.com
      # Server-level default credentials (optional)
      METABASE_API_KEY: mb_default_key
    restart: unless-stopped
```
