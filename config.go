package main

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

// Config holds all server-level configuration loaded from environment variables.
type Config struct {
	// Metabase server defaults
	MetabaseURL     string
	DefaultEmail    string
	DefaultPassword string
	DefaultAPIKey   string

	// HTTP proxy for outbound Metabase API requests.
	// If empty, no proxy is used — env vars (HTTP_PROXY, HTTPS_PROXY) are ignored.
	// If non-empty, this specific proxy URL is used.
	MetabaseHTTPProxy string

	// HTTP server settings (used in http transport mode)
	Host    string
	Port    int
	MCPPath string

	// MetabaseClient cache TTL in seconds
	ClientCacheTTL int
}

func loadConfig() *Config {
	port, _ := strconv.Atoi(getEnv("PORT", "8000"))
	ttl, _ := strconv.Atoi(getEnv("CLIENT_CACHE_TTL", "3600"))

	cfg := &Config{
		MetabaseURL:       strings.TrimRight(getEnv("METABASE_URL", ""), "/"),
		DefaultEmail:      getEnv("METABASE_USER_EMAIL", ""),
		DefaultPassword:   getEnv("METABASE_PASSWORD", ""),
		DefaultAPIKey:     getEnv("METABASE_API_KEY", ""),
		MetabaseHTTPProxy: getEnv("METABASE_HTTP_PROXY", ""),
		Host:              getEnv("HOST", "0.0.0.0"),
		Port:              port,
		MCPPath:           getEnv("MCP_PATH", "/mcp"),
		ClientCacheTTL:    ttl,
	}

	// METABASE_CREDENTIALS accepts base64url-encoded "user:pass" or "apikey:<key>",
	// the same format used for URL path encoding.
	// When set, it takes precedence over METABASE_USER_EMAIL / METABASE_PASSWORD /
	// METABASE_API_KEY, allowing credentials to be stored without plaintext.
	//
	// Generate with:
	//   echo -n 'alice@example.com:password' | base64 | tr '+/' '-_' | tr -d '='
	//   echo -n 'apikey:mb_xxxx'             | base64 | tr '+/' '-_' | tr -d '='
	if raw := getEnv("METABASE_CREDENTIALS", ""); raw != "" {
		user, pass, ok := decodePathCreds(raw)
		if !ok {
			slog.Warn("METABASE_CREDENTIALS is set but could not be decoded as base64url(user:pass), ignoring")
		} else if strings.EqualFold(user, "apikey") {
			cfg.DefaultAPIKey = pass
			cfg.DefaultEmail = ""
			cfg.DefaultPassword = ""
		} else {
			cfg.DefaultEmail = user
			cfg.DefaultPassword = pass
			cfg.DefaultAPIKey = ""
		}
	}

	return cfg
}

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
