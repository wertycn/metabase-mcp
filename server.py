#!/usr/bin/env python3
"""
Metabase MCP Server — HTTP Transport with Multi-User Support

Credential passing (per-connection, choose one):

  1. HTTP Basic Auth  (most standard — what "http://user:pass@host/" translates to)
       Email + password : http://user%40example.com:password@server:8000/mcp
       API key          : http://apikey:<mb_api_key>@server:8000/mcp

  2. URL-path encoding  (for clients that send raw URLs without auth support)
       Email + password : http://server:8000/<base64url(email:password)>/mcp
       API key          : http://server:8000/<base64url(apikey:<key>)>/mcp

  3. Custom HTTP headers  (for programmatic / proxy use)
       X-Metabase-Email, X-Metabase-Password  — or —  X-Metabase-Api-Key
       (Optional) X-Metabase-Url to override the server-level METABASE_URL

Environment variables (server-level defaults):
  METABASE_URL          — target Metabase instance URL (required)
  METABASE_USER_EMAIL   — default email for unauthenticated requests
  METABASE_PASSWORD     — default password for unauthenticated requests
  METABASE_API_KEY      — default API key for unauthenticated requests
  HOST                  — bind address (default: 0.0.0.0)
  PORT                  — bind port   (default: 8000)
  MCP_PATH              — MCP endpoint path (default: /mcp)
  CLIENT_CACHE_TTL      — seconds to cache MetabaseClient (default: 3600)
"""

from __future__ import annotations

import base64
import contextvars
import hashlib
import logging
import os
import time
from typing import Any

import httpx
import uvicorn
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger("metabase-mcp")

# ---------------------------------------------------------------------------
# Server-level configuration  (env vars / defaults)
# ---------------------------------------------------------------------------
METABASE_URL: str = os.getenv("METABASE_URL", "").rstrip("/")
DEFAULT_EMAIL: str = os.getenv("METABASE_USER_EMAIL", "")
DEFAULT_PASSWORD: str = os.getenv("METABASE_PASSWORD", "")
DEFAULT_API_KEY: str = os.getenv("METABASE_API_KEY", "")

HOST: str = os.getenv("HOST", "0.0.0.0")
PORT: int = int(os.getenv("PORT", "8000"))
MCP_PATH: str = os.getenv("MCP_PATH", "/mcp")
CLIENT_CACHE_TTL: int = int(os.getenv("CLIENT_CACHE_TTL", "3600"))

# ---------------------------------------------------------------------------
# Per-request context variable
# ---------------------------------------------------------------------------
# Holds a dict with keys: metabase_url, email, password, api_key
_request_creds: contextvars.ContextVar[dict[str, str] | None] = contextvars.ContextVar(
    "request_creds", default=None
)


# ---------------------------------------------------------------------------
# Metabase HTTP client
# ---------------------------------------------------------------------------

# {credential_hash: (MetabaseClient, last_used_monotonic)}
_client_cache: dict[str, tuple[MetabaseClient, float]] = {}


class MetabaseClient:
    """Async HTTP client for the Metabase REST API."""

    def __init__(
        self,
        base_url: str,
        email: str = "",
        password: str = "",
        api_key: str = "",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self.api_key = api_key
        self.session_token: str | None = None
        self._http = httpx.AsyncClient(timeout=30.0)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    async def _login(self) -> None:
        """Obtain a Metabase session token via email / password."""
        resp = await self._http.post(
            f"{self.base_url}/api/session",
            json={"username": self.email, "password": self.password},
            headers={"Content-Type": "application/json"},
        )
        if resp.status_code != 200:
            raise ToolError(
                f"Metabase authentication failed ({resp.status_code}): {resp.text}"
            )
        self.session_token = resp.json()["id"]
        logger.info("Acquired Metabase session token for %s", self.email)

    async def _auth_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-KEY"] = self.api_key
        else:
            if not self.session_token:
                await self._login()
            headers["X-Metabase-Session"] = self.session_token  # type: ignore[assignment]
        return headers

    # ------------------------------------------------------------------
    # API request
    # ------------------------------------------------------------------

    async def request(
        self, method: str, path: str, *, retry_auth: bool = True, **kwargs: Any
    ) -> Any:
        url = f"{self.base_url}/api{path}"
        headers = await self._auth_headers()
        resp = await self._http.request(method=method, url=url, headers=headers, **kwargs)

        if resp.status_code == 401 and retry_auth and not self.api_key:
            # Session may have expired — refresh once
            self.session_token = None
            await self._login()
            headers = await self._auth_headers()
            resp = await self._http.request(
                method=method, url=url, headers=headers, **kwargs
            )

        if not resp.is_success:
            raise ToolError(
                f"Metabase API {method} {path} failed "
                f"({resp.status_code}): {resp.text}"
            )
        return resp.json()

    async def close(self) -> None:
        await self._http.aclose()


# ---------------------------------------------------------------------------
# Client cache helpers
# ---------------------------------------------------------------------------

def _cred_key(creds: dict[str, str]) -> str:
    raw = (
        f"{creds['metabase_url']}|"
        f"{creds.get('email', '')}|"
        f"{creds.get('password', '')}|"
        f"{creds.get('api_key', '')}"
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def _get_client() -> MetabaseClient:
    """
    Return a cached MetabaseClient for the credentials attached to the
    current request.  Falls back to server-level defaults if no per-request
    credentials were provided.
    """
    creds = _request_creds.get()
    if creds is None:
        creds = {
            "metabase_url": METABASE_URL,
            "email": DEFAULT_EMAIL,
            "password": DEFAULT_PASSWORD,
            "api_key": DEFAULT_API_KEY,
        }

    if not creds.get("metabase_url"):
        raise ToolError(
            "No Metabase URL configured. "
            "Set the METABASE_URL environment variable on the server, or pass the "
            "X-Metabase-Url header with every request."
        )

    if not creds.get("api_key") and not creds.get("email"):
        raise ToolError(
            "No Metabase credentials provided. "
            "Connect using http://email:password@server/mcp or "
            "http://apikey:<key>@server/mcp, or set server-level defaults via "
            "METABASE_USER_EMAIL/METABASE_PASSWORD or METABASE_API_KEY."
        )

    key = _cred_key(creds)
    now = time.monotonic()

    if key in _client_cache:
        client, last_used = _client_cache[key]
        if now - last_used < CLIENT_CACHE_TTL:
            _client_cache[key] = (client, now)
            return client
        # Expired entry — drop it (old httpx client will be GC'd)
        del _client_cache[key]

    client = MetabaseClient(
        base_url=creds["metabase_url"],
        email=creds.get("email", ""),
        password=creds.get("password", ""),
        api_key=creds.get("api_key", ""),
    )
    _client_cache[key] = (client, now)
    return client


# ---------------------------------------------------------------------------
# Credential extraction helpers
# ---------------------------------------------------------------------------

def _decode_basic_auth(authorization: str) -> tuple[str, str] | None:
    """Parse 'Authorization: Basic <b64>' → (username, password) or None."""
    if not authorization.startswith("Basic "):
        return None
    try:
        decoded = base64.b64decode(authorization[6:]).decode("utf-8")
        username, sep, password = decoded.partition(":")
        return (username, password) if sep else None
    except Exception:
        return None


def _decode_path_creds(segment: str) -> tuple[str, str] | None:
    """
    Try to decode a URL-path segment as base64url(username:password).
    Returns (username, password) or None.
    """
    try:
        # Restore padding
        pad = (4 - len(segment) % 4) % 4
        decoded = base64.urlsafe_b64decode(segment + "=" * pad).decode("utf-8")
        username, sep, password = decoded.partition(":")
        return (username, password) if sep and username else None
    except Exception:
        return None


def _build_creds(
    username: str, password: str, mb_url: str
) -> dict[str, str]:
    """Map (username, password) → credential dict, handling apikey convention."""
    if username.lower() == "apikey":
        return {"metabase_url": mb_url, "email": "", "password": "", "api_key": password}
    return {"metabase_url": mb_url, "email": username, "password": password, "api_key": ""}


def _extract_credentials(
    raw_headers: list[tuple[bytes, bytes]], path: str
) -> tuple[dict[str, str] | None, str]:
    """
    Extract credentials from the request and return (creds_dict, clean_path).
    clean_path has any credential prefix stripped so FastMCP sees /mcp.

    Priority:
      1. Custom headers  (X-Metabase-*)
      2. HTTP Basic Auth (Authorization: Basic …)
      3. URL path prefix (/{base64url_creds}/mcp)
    """
    headers: dict[bytes, bytes] = {k.lower(): v for k, v in raw_headers}
    mb_url = (
        headers.get(b"x-metabase-url", b"").decode().strip() or METABASE_URL
    )
    clean_path = path  # may be rewritten below

    # ---- 1. Custom headers ------------------------------------------------
    api_key = headers.get(b"x-metabase-api-key", b"").decode().strip()
    email = headers.get(b"x-metabase-email", b"").decode().strip()
    password = headers.get(b"x-metabase-password", b"").decode().strip()

    if api_key:
        return (
            {"metabase_url": mb_url, "email": "", "password": "", "api_key": api_key},
            clean_path,
        )
    if email and password:
        return (
            {"metabase_url": mb_url, "email": email, "password": password, "api_key": ""},
            clean_path,
        )

    # ---- 2. HTTP Basic Auth -----------------------------------------------
    auth_header = headers.get(b"authorization", b"").decode()
    if auth_header:
        parsed = _decode_basic_auth(auth_header)
        if parsed:
            return _build_creds(*parsed, mb_url), clean_path

    # ---- 3. URL path prefix  /{base64creds}/mcp ---------------------------
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2:
        candidate = parts[0]
        parsed = _decode_path_creds(candidate)
        if parsed:
            # Strip the credential segment from the path seen by FastMCP
            clean_path = "/" + "/".join(parts[1:])
            return _build_creds(*parsed, mb_url), clean_path

    return None, clean_path


# ---------------------------------------------------------------------------
# Pure-ASGI credential middleware
# ---------------------------------------------------------------------------

class CredentialsMiddleware:
    """
    Pure ASGI middleware (no BaseHTTPMiddleware overhead) that:
      - Extracts per-request Metabase credentials from the request.
      - Rewrites the URL path when credentials were embedded in it.
      - Stores credentials in a contextvar so MCP tools can access them.

    Works correctly with streaming / SSE responses because it does NOT
    use BaseHTTPMiddleware (which would break context propagation on streams).
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope["type"] in ("http", "websocket"):
            raw_headers: list[tuple[bytes, bytes]] = scope.get("headers", [])
            path: str = scope.get("path", "")

            creds, clean_path = _extract_credentials(raw_headers, path)

            if clean_path != path:
                scope = {**scope, "path": clean_path, "raw_path": clean_path.encode()}

            token = _request_creds.set(creds)
            try:
                await self.app(scope, receive, send)
            finally:
                _request_creds.reset(token)
        else:
            await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# FastMCP server definition
# ---------------------------------------------------------------------------

mcp = FastMCP(name="metabase-mcp", instructions=(
    "This server provides tools to interact with a Metabase instance. "
    "Use it to query databases, manage saved questions, dashboards, and collections."
))


# ==========================================================================
# Database tools
# ==========================================================================

@mcp.tool
async def list_databases(ctx: Context) -> dict[str, Any]:
    """List all databases configured in Metabase."""
    await ctx.info("Fetching databases from Metabase")
    return await _get_client().request("GET", "/database")


@mcp.tool
async def list_tables(database_id: int, ctx: Context) -> str:
    """
    List all tables in a specific Metabase database.

    Args:
        database_id: The numeric ID of the database.

    Returns:
        Formatted Markdown table with table metadata.
    """
    await ctx.info(f"Fetching tables for database {database_id}")
    result = await _get_client().request("GET", f"/database/{database_id}/metadata")
    tables: list[dict] = result.get("tables", [])
    tables.sort(key=lambda t: t.get("display_name") or "")

    out = f"# Tables in Database {database_id}\n\n**Total:** {len(tables)}\n\n"
    if not tables:
        return out + "*No tables found.*\n"

    out += "| Table ID | Display Name | Description | Entity Type |\n"
    out += "|----------|--------------|-------------|-------------|\n"
    for t in tables:
        name = (t.get("display_name") or "N/A").replace("|", "\\|")
        desc = (t.get("description") or "—").replace("|", "\\|")
        out += f"| {t.get('id', 'N/A')} | {name} | {desc} | {t.get('entity_type', 'N/A')} |\n"
    return out


@mcp.tool
async def get_table_fields(
    table_id: int,
    ctx: Context,
    limit: int = 50,
) -> dict[str, Any]:
    """
    Get field/column metadata for a specific table.

    Args:
        table_id: The numeric ID of the table.
        limit: Maximum fields to return (default 50; 0 = unlimited).
    """
    await ctx.info(f"Fetching fields for table {table_id}")
    result = await _get_client().request("GET", f"/table/{table_id}/query_metadata")
    fields: list = result.get("fields", [])
    if limit > 0 and len(fields) > limit:
        result["fields"] = fields[:limit]
        result["_truncated"] = True
        result["_total_fields"] = len(fields)
        result["_limit_applied"] = limit
        await ctx.info(f"Truncated {len(fields)} fields to {limit}")
    return result


# ==========================================================================
# Query tools
# ==========================================================================

@mcp.tool
async def execute_query(
    database_id: int,
    query: str,
    ctx: Context,
    native_parameters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Execute a native SQL query against a Metabase database.

    Args:
        database_id: The numeric ID of the database to query.
        query: The SQL query string.
        native_parameters: Optional list of Metabase native query parameters.
    """
    await ctx.info(f"Executing SQL query on database {database_id}")
    payload: dict[str, Any] = {
        "database": database_id,
        "type": "native",
        "native": {"query": query},
    }
    if native_parameters:
        payload["native"]["parameters"] = native_parameters

    result = await _get_client().request("POST", "/dataset", json=payload)
    row_count = len(result.get("data", {}).get("rows", []))
    await ctx.info(f"Query returned {row_count} rows")
    return result


# ==========================================================================
# Card (saved question) tools
# ==========================================================================

@mcp.tool
async def list_cards(ctx: Context) -> list[dict[str, Any]]:
    """List all saved questions/cards in Metabase."""
    await ctx.info("Fetching saved cards")
    return await _get_client().request("GET", "/card")


@mcp.tool
async def get_card(card_id: int, ctx: Context) -> dict[str, Any]:
    """
    Get details of a specific saved question/card.

    Args:
        card_id: The numeric ID of the card.
    """
    await ctx.info(f"Fetching card {card_id}")
    return await _get_client().request("GET", f"/card/{card_id}")


@mcp.tool
async def execute_card(
    card_id: int,
    ctx: Context,
    parameters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Execute a saved Metabase question/card and return results.

    Args:
        card_id: The numeric ID of the card.
        parameters: Optional list of dashboard filter parameters.
    """
    await ctx.info(f"Executing card {card_id}")
    payload: dict[str, Any] = {}
    if parameters:
        payload["parameters"] = parameters

    result = await _get_client().request("POST", f"/card/{card_id}/query", json=payload)
    row_count = len(result.get("data", {}).get("rows", []))
    await ctx.info(f"Card {card_id} returned {row_count} rows")
    return result


@mcp.tool
async def create_card(
    name: str,
    database_id: int,
    query: str,
    ctx: Context,
    description: str | None = None,
    collection_id: int | None = None,
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Create a new saved question/card in Metabase.

    Args:
        name: Card display name.
        database_id: ID of the database the query targets.
        query: SQL query for the card.
        description: Optional description.
        collection_id: Optional collection to place the card in.
        visualization_settings: Optional visualization configuration dict.
    """
    await ctx.info(f"Creating card '{name}' in database {database_id}")
    payload: dict[str, Any] = {
        "name": name,
        "database_id": database_id,
        "dataset_query": {
            "database": database_id,
            "type": "native",
            "native": {"query": query},
        },
        "display": "table",
        "visualization_settings": visualization_settings or {},
    }
    if description:
        payload["description"] = description
    if collection_id is not None:
        payload["collection_id"] = collection_id

    result = await _get_client().request("POST", "/card", json=payload)
    await ctx.info(f"Created card ID {result.get('id')}")
    return result


# ==========================================================================
# Collection tools
# ==========================================================================

@mcp.tool
async def list_collections(ctx: Context) -> list[dict[str, Any]]:
    """List all collections in Metabase."""
    await ctx.info("Fetching collections")
    return await _get_client().request("GET", "/collection")


@mcp.tool
async def get_collection_items(
    collection_id: str,
    ctx: Context,
    model: str | None = None,
) -> dict[str, Any]:
    """
    Get items within a Metabase collection.

    Args:
        collection_id: Collection ID, or "root" for the root collection.
        model: Optional filter — one of 'card', 'dashboard', 'collection'.
    """
    await ctx.info(f"Fetching items for collection '{collection_id}'")
    params: dict[str, str] = {}
    if model:
        params["model"] = model
    return await _get_client().request(
        "GET", f"/collection/{collection_id}/items", params=params
    )


@mcp.tool
async def create_collection(
    name: str,
    ctx: Context,
    description: str | None = None,
    parent_id: int | None = None,
) -> dict[str, Any]:
    """
    Create a new collection in Metabase.

    Args:
        name: Collection display name.
        description: Optional description.
        parent_id: Optional numeric ID of the parent collection.
    """
    await ctx.info(f"Creating collection '{name}'")
    payload: dict[str, Any] = {"name": name}
    if description:
        payload["description"] = description
    if parent_id is not None:
        payload["parent_id"] = parent_id

    result = await _get_client().request("POST", "/collection", json=payload)
    await ctx.info(f"Created collection ID {result.get('id')}")
    return result


# ==========================================================================
# Dashboard tools
# ==========================================================================

@mcp.tool
async def list_dashboards(ctx: Context) -> list[dict[str, Any]]:
    """List all dashboards in Metabase."""
    await ctx.info("Fetching dashboards")
    return await _get_client().request("GET", "/dashboard")


@mcp.tool
async def get_dashboard(dashboard_id: int, ctx: Context) -> dict[str, Any]:
    """
    Get details of a specific dashboard, including its cards.

    Args:
        dashboard_id: The numeric ID of the dashboard.
    """
    await ctx.info(f"Fetching dashboard {dashboard_id}")
    return await _get_client().request("GET", f"/dashboard/{dashboard_id}")


@mcp.tool
async def create_dashboard(
    name: str,
    ctx: Context,
    description: str | None = None,
    collection_id: int | None = None,
    parameters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Create a new dashboard in Metabase.

    Args:
        name: Dashboard display name.
        description: Optional description.
        collection_id: Optional collection to place the dashboard in.
        parameters: Optional list of dashboard filter parameter definitions.
    """
    await ctx.info(f"Creating dashboard '{name}'")
    payload: dict[str, Any] = {"name": name}
    if description:
        payload["description"] = description
    if collection_id is not None:
        payload["collection_id"] = collection_id
    if parameters:
        payload["parameters"] = parameters

    result = await _get_client().request("POST", "/dashboard", json=payload)
    await ctx.info(f"Created dashboard ID {result.get('id')}")
    return result


@mcp.tool
async def update_dashboard(
    dashboard_id: int,
    ctx: Context,
    name: str | None = None,
    description: str | None = None,
    collection_id: int | None = None,
    parameters: list[dict[str, Any]] | None = None,
    archived: bool | None = None,
) -> dict[str, Any]:
    """
    Update an existing dashboard's metadata.

    Args:
        dashboard_id: The numeric ID of the dashboard.
        name: New display name.
        description: New description.
        collection_id: Move dashboard to this collection.
        parameters: Replace dashboard filter parameter definitions.
        archived: True to archive (move to Trash), False to restore.
    """
    await ctx.info(f"Updating dashboard {dashboard_id}")
    payload: dict[str, Any] = {}
    if name is not None:
        payload["name"] = name
    if description is not None:
        payload["description"] = description
    if collection_id is not None:
        payload["collection_id"] = collection_id
    if parameters is not None:
        payload["parameters"] = parameters
    if archived is not None:
        payload["archived"] = archived

    result = await _get_client().request("PUT", f"/dashboard/{dashboard_id}", json=payload)
    await ctx.info(f"Updated dashboard {dashboard_id}")
    return result


@mcp.tool
async def add_card_to_dashboard(
    dashboard_id: int,
    card_id: int,
    ctx: Context,
    row: int = 0,
    col: int = 0,
    size_x: int = 12,
    size_y: int = 8,
    parameter_mappings: list[dict[str, Any]] | None = None,
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Add a saved question/card to a dashboard at a specified grid position.

    The dashboard grid is 24 columns wide. Cards are placed using row/col
    coordinates and sized with size_x (width) / size_y (height) in grid units.

    Args:
        dashboard_id: The numeric ID of the target dashboard.
        card_id: The numeric ID of the saved question to add.
        row: Grid row position (0-based).
        col: Grid column position (0-based, grid is 24 cols wide).
        size_x: Card width in grid units (default 12 = half width).
        size_y: Card height in grid units (default 8).
        parameter_mappings: Optional dashboard filter → card parameter mappings.
        visualization_settings: Optional visualization overrides for this placement.
    """
    await ctx.info(f"Adding card {card_id} to dashboard {dashboard_id}")
    payload: dict[str, Any] = {
        "cardId": card_id,
        "row": row,
        "col": col,
        "size_x": size_x,
        "size_y": size_y,
        "parameter_mappings": parameter_mappings or [],
        "visualization_settings": visualization_settings or {},
    }

    result = await _get_client().request(
        "POST", f"/dashboard/{dashboard_id}/cards", json=payload
    )
    await ctx.info(f"Added card {card_id} as dashcard ID {result.get('id')}")
    return result


@mcp.tool
async def update_dashboard_card(
    dashboard_id: int,
    dashcard_id: int,
    ctx: Context,
    row: int | None = None,
    col: int | None = None,
    size_x: int | None = None,
    size_y: int | None = None,
    parameter_mappings: list[dict[str, Any]] | None = None,
    visualization_settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Move, resize, or reconfigure a card already placed on a dashboard.

    The dashcard_id is the placement ID (returned by add_card_to_dashboard
    or visible in get_dashboard response as dashcards[].id), not the card ID.

    Args:
        dashboard_id: The numeric ID of the dashboard.
        dashcard_id: The numeric ID of the dashcard placement to update.
        row: New grid row position.
        col: New grid column position.
        size_x: New width in grid units.
        size_y: New height in grid units.
        parameter_mappings: Updated filter parameter mappings.
        visualization_settings: Updated visualization settings.
    """
    await ctx.info(f"Updating dashcard {dashcard_id} on dashboard {dashboard_id}")

    # Fetch current state so callers only need to pass changed fields
    dashboard = await _get_client().request("GET", f"/dashboard/{dashboard_id}")
    current = next(
        (dc for dc in dashboard.get("dashcards", []) if dc["id"] == dashcard_id),
        None,
    )
    if current is None:
        raise ToolError(f"Dashcard {dashcard_id} not found on dashboard {dashboard_id}")

    payload: dict[str, Any] = {
        "id": dashcard_id,
        "row": row if row is not None else current["row"],
        "col": col if col is not None else current["col"],
        "size_x": size_x if size_x is not None else current["size_x"],
        "size_y": size_y if size_y is not None else current["size_y"],
        "parameter_mappings": (
            parameter_mappings
            if parameter_mappings is not None
            else current.get("parameter_mappings", [])
        ),
        "visualization_settings": (
            visualization_settings
            if visualization_settings is not None
            else current.get("visualization_settings", {})
        ),
    }

    result = await _get_client().request(
        "PUT", f"/dashboard/{dashboard_id}/dashcard/{dashcard_id}", json=payload
    )
    await ctx.info(f"Updated dashcard {dashcard_id}")
    return result


@mcp.tool
async def remove_card_from_dashboard(
    dashboard_id: int,
    dashcard_id: int,
    ctx: Context,
) -> dict[str, Any]:
    """
    Remove a card placement from a dashboard.

    Args:
        dashboard_id: The numeric ID of the dashboard.
        dashcard_id: The numeric ID of the dashcard placement to remove
                     (from add_card_to_dashboard or get_dashboard response).
    """
    await ctx.info(f"Removing dashcard {dashcard_id} from dashboard {dashboard_id}")
    result = await _get_client().request(
        "DELETE", f"/dashboard/{dashboard_id}/dashcard/{dashcard_id}"
    )
    await ctx.info(f"Removed dashcard {dashcard_id} from dashboard {dashboard_id}")
    return result or {}


# ---------------------------------------------------------------------------
# ASGI app factory
# ---------------------------------------------------------------------------

def build_app() -> Any:
    """
    Build the production ASGI application:
      CredentialsMiddleware → FastMCP streamable-HTTP app
    """
    mcp_asgi = mcp.http_app(path=MCP_PATH, transport="streamable-http")
    return CredentialsMiddleware(mcp_asgi)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Metabase MCP server starting")
    if METABASE_URL:
        logger.info("  Metabase URL : %s", METABASE_URL)
    else:
        logger.warning(
            "  METABASE_URL is not set — clients MUST supply X-Metabase-Url header"
        )
    logger.info("  Listen       : http://%s:%d%s", HOST, PORT, MCP_PATH)
    logger.info(
        "  Connect URL  : http://<email>:<password>@%s:%d%s", HOST, PORT, MCP_PATH
    )
    logger.info(
        "  API key URL  : http://apikey:<api_key>@%s:%d%s", HOST, PORT, MCP_PATH
    )

    app = build_app()
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    main()
