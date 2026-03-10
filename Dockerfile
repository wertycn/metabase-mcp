# ─── build stage ────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Install uv (fast Python package manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy only dependency declaration first (better layer caching)
COPY pyproject.toml ./

# Install dependencies into a virtual-environment inside /app/.venv
RUN uv venv .venv && \
    uv pip install --python .venv/bin/python --no-cache \
        fastmcp>=2.12.0 \
        httpx>=0.28.0 \
        "uvicorn[standard]>=0.34.0" \
        python-dotenv>=1.0.1

# ─── runtime stage ───────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy the virtual-environment from the build stage
COPY --from=builder /app/.venv .venv

# Copy application source
COPY server.py ./

# Activate venv for all subsequent RUN / CMD commands
ENV PATH="/app/.venv/bin:$PATH"

# ── Runtime configuration (override with -e / --env-file) ─────────────────
#
# Required:
ENV METABASE_URL=""
#
# Authentication — provide either API key OR email+password
ENV METABASE_API_KEY=""
ENV METABASE_USER_EMAIL=""
ENV METABASE_PASSWORD=""
#
# Server bind settings
ENV HOST="0.0.0.0"
ENV PORT="8000"
ENV MCP_PATH="/mcp"
#
# MetabaseClient cache TTL in seconds (default 1 h)
ENV CLIENT_CACHE_TTL="3600"

EXPOSE 8000

# Drop to a non-root user for security
RUN adduser --disabled-password --gecos "" mcpuser
USER mcpuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${PORT}/health')" || exit 1

CMD ["python", "server.py"]
