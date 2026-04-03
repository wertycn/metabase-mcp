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

	// Transport mode: "stdio" or "http"
	Transport string

	// Named Metabase instances for multi-instance scenarios (e.g. migration).
	// Key is the instance identifier (e.g. "hz", "sg").
	// The default instance (from METABASE_URL) is stored under key "default".
	Instances map[string]*Credentials
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

	// Named instances: register default + any from METABASE_INSTANCES.
	cfg.Instances = make(map[string]*Credentials)
	if cfg.MetabaseURL != "" {
		cfg.Instances["default"] = &Credentials{
			MetabaseURL: cfg.MetabaseURL,
			Email:       cfg.DefaultEmail,
			Password:    cfg.DefaultPassword,
			APIKey:      cfg.DefaultAPIKey,
		}
	}

	// METABASE_INSTANCES is a comma-separated list of instance names.
	// Each instance reads from METABASE_{NAME}_URL, METABASE_{NAME}_EMAIL,
	// METABASE_{NAME}_PASSWORD, METABASE_{NAME}_API_KEY, or
	// METABASE_{NAME}_CREDENTIALS (base64url-encoded, same as METABASE_CREDENTIALS).
	if names := getEnv("METABASE_INSTANCES", ""); names != "" {
		for _, name := range strings.Split(names, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			prefix := "METABASE_" + strings.ToUpper(name) + "_"
			creds := &Credentials{
				MetabaseURL: strings.TrimRight(getEnv(prefix+"URL", ""), "/"),
				Email:       getEnv(prefix+"EMAIL", ""),
				Password:    getEnv(prefix+"PASSWORD", ""),
				APIKey:      getEnv(prefix+"API_KEY", ""),
			}
			if raw := getEnv(prefix+"CREDENTIALS", ""); raw != "" {
				user, pass, ok := decodePathCreds(raw)
				if !ok {
					slog.Warn("Could not decode instance credentials", "instance", name)
				} else if strings.EqualFold(user, "apikey") {
					creds.APIKey = pass
					creds.Email = ""
					creds.Password = ""
				} else {
					creds.Email = user
					creds.Password = pass
					creds.APIKey = ""
				}
			}
			if creds.MetabaseURL == "" {
				slog.Warn("Instance has no URL, skipping", "instance", name)
				continue
			}
			cfg.Instances[name] = creds
			// Also register as default if no default URL was set and this is the first
			if cfg.MetabaseURL == "" && len(cfg.Instances) == 1 {
				cfg.Instances["default"] = creds
			}
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
