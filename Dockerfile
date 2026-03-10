# Runtime-only image — binary is built in CI (CNB) and copied into context before docker build.
# Expected: ./metabase-mcp  (linux/amd64 binary, copied from dist/ in CI)
FROM alpine:3.21

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY metabase-mcp .

# ── Runtime configuration (override with -e / --env-file) ─────────────────────
ENV METABASE_URL=""
ENV METABASE_API_KEY=""
ENV METABASE_USER_EMAIL=""
ENV METABASE_PASSWORD=""
# HTTP proxy for outbound Metabase API requests.
# If not set, HTTP_PROXY / HTTPS_PROXY env vars are explicitly IGNORED.
ENV METABASE_HTTP_PROXY=""
ENV TRANSPORT="http"
ENV HOST="0.0.0.0"
ENV PORT="8000"
ENV MCP_PATH="/mcp"
ENV CLIENT_CACHE_TTL="3600"

EXPOSE 8000

RUN adduser -D -H mcpuser
USER mcpuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- "http://localhost:${PORT}/health" >/dev/null 2>&1 || exit 1

CMD ["./metabase-mcp", "--transport", "http"]
