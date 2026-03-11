package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/mark3labs/mcp-go/server"
)

// version is set at build time via -ldflags="-X main.version=x.y.z".
var version = "dev"

func main() {
	transport := flag.String(
		"transport",
		getEnv("TRANSPORT", "http"),
		"Transport mode: stdio or http",
	)
	flag.Parse()

	cfg := loadConfig()
	initSessionCache()
	initCache(cfg.ClientCacheTTL)

	slog.Info("Starting Metabase MCP server",
		"transport", *transport,
		"metabase_url", cfg.MetabaseURL,
	)
	if cfg.MetabaseHTTPProxy != "" {
		slog.Info("Using HTTP proxy for Metabase requests", "proxy", cfg.MetabaseHTTPProxy)
	} else {
		slog.Info("No HTTP proxy — env vars (HTTP_PROXY/HTTPS_PROXY) are ignored")
	}

	s := buildServer(cfg)

	switch *transport {
	case "stdio":
		slog.Info("Running in stdio mode")
		if err := server.ServeStdio(s); err != nil {
			slog.Error("stdio server error", "err", err)
			os.Exit(1)
		}

	case "http":
		addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
		slog.Info("Running in HTTP (streamable-http) mode",
			"addr", addr,
			"path", cfg.MCPPath,
		)
		slog.Info("Connect URL",
			"email+pass", fmt.Sprintf("http://<email>:<password>@%s:%d%s", cfg.Host, cfg.Port, cfg.MCPPath),
			"api_key", fmt.Sprintf("http://apikey:<key>@%s:%d%s", cfg.Host, cfg.Port, cfg.MCPPath),
		)
		if err := runHTTP(s, cfg, addr); err != nil {
			slog.Error("HTTP server error", "err", err)
			os.Exit(1)
		}

	default:
		fmt.Fprintf(os.Stderr, "unknown transport: %q (use 'stdio' or 'http')\n", *transport)
		os.Exit(1)
	}
}

// runHTTP starts the HTTP server with credential middleware wrapping the MCP handler.
func runHTTP(s *server.MCPServer, cfg *Config, addr string) error {
	mcpHandler := server.NewStreamableHTTPServer(s,
		server.WithEndpointPath(cfg.MCPPath),
	)

	// credMiddleware extracts per-request Metabase credentials and injects them
	// into the request context before the MCP handler processes the request.
	// It also rewrites the URL path when credentials were embedded as a prefix
	// (e.g. /{base64creds}/mcp → /mcp).
	handler := &credMiddleware{next: mcpHandler, cfg: cfg}

	srv := &http.Server{
		Addr:    addr,
		Handler: handler,
	}
	return srv.ListenAndServe()
}
