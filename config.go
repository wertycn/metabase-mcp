package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
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

	// OutputDir is the directory where query result JSON files are saved.
	OutputDir string

	// Named Metabase instances for multi-instance scenarios (e.g. migration).
	// Key is the instance identifier (e.g. "hz", "sg").
	// The default instance (from METABASE_URL) is stored under key "default".
	Instances map[string]*Credentials
}

func loadConfig() *Config {
	loadConfigFile()

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

	cfg.OutputDir = getEnv("OUTPUT_DIR", filepath.Join(os.TempDir(), "metabase-mcp-output"))

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

// ---------------------------------------------------------------------------
// Config file support
// ---------------------------------------------------------------------------

// fileDefaults holds values loaded from the YAML config file.
// getEnv checks this map as a fallback when the environment variable is unset.
var fileDefaults map[string]string

// configFile is the YAML config file structure.
type configFile struct {
	MetabaseURL         string                   `yaml:"metabase_url"`
	MetabaseCredentials string                   `yaml:"metabase_credentials"`
	MetabaseAPIKey      string                   `yaml:"metabase_api_key"`
	MetabaseUserEmail   string                   `yaml:"metabase_user_email"`
	MetabasePassword    string                   `yaml:"metabase_password"`
	MetabaseHTTPProxy   string                   `yaml:"metabase_http_proxy"`
	Transport           string                   `yaml:"transport"`
	Host                string                   `yaml:"host"`
	Port                int                      `yaml:"port"`
	MCPPath             string                   `yaml:"mcp_path"`
	ClientCacheTTL      int                      `yaml:"client_cache_ttl"`
	OutputDir           string                   `yaml:"output_dir"`
	Instances           map[string]*instanceFile `yaml:"instances"`
}

type instanceFile struct {
	URL         string `yaml:"url"`
	Credentials string `yaml:"credentials"`
	APIKey      string `yaml:"api_key"`
	Email       string `yaml:"email"`
	Password    string `yaml:"password"`
}

// configFilePath returns the path to the config file to load.
// Priority: CONFIG_FILE env var > ~/.config/metabase-mcp/config.yaml.
func configFilePath() string {
	if p := os.Getenv("CONFIG_FILE"); p != "" {
		return p
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "metabase-mcp", "config.yaml")
}

// loadConfigFile reads the YAML config file and populates fileDefaults.
// It is called once at the start of loadConfig.
func loadConfigFile() {
	fileDefaults = make(map[string]string)

	path := configFilePath()
	if path == "" {
		return
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return // file not found is fine
	}

	var fc configFile
	if err := yaml.Unmarshal(data, &fc); err != nil {
		slog.Warn("Failed to parse config file", "path", path, "err", err)
		return
	}

	slog.Info("Loaded config file", "path", path)

	setFileDef("METABASE_URL", fc.MetabaseURL)
	setFileDef("METABASE_CREDENTIALS", fc.MetabaseCredentials)
	setFileDef("METABASE_API_KEY", fc.MetabaseAPIKey)
	setFileDef("METABASE_USER_EMAIL", fc.MetabaseUserEmail)
	setFileDef("METABASE_PASSWORD", fc.MetabasePassword)
	setFileDef("METABASE_HTTP_PROXY", fc.MetabaseHTTPProxy)
	setFileDef("TRANSPORT", fc.Transport)
	setFileDef("HOST", fc.Host)
	if fc.Port != 0 {
		setFileDef("PORT", strconv.Itoa(fc.Port))
	}
	setFileDef("MCP_PATH", fc.MCPPath)
	if fc.ClientCacheTTL != 0 {
		setFileDef("CLIENT_CACHE_TTL", strconv.Itoa(fc.ClientCacheTTL))
	}
	setFileDef("OUTPUT_DIR", fc.OutputDir)

	if len(fc.Instances) > 0 {
		names := make([]string, 0, len(fc.Instances))
		for name, inst := range fc.Instances {
			names = append(names, name)
			prefix := fmt.Sprintf("METABASE_%s_", strings.ToUpper(name))
			setFileDef(prefix+"URL", inst.URL)
			setFileDef(prefix+"CREDENTIALS", inst.Credentials)
			setFileDef(prefix+"API_KEY", inst.APIKey)
			setFileDef(prefix+"EMAIL", inst.Email)
			setFileDef(prefix+"PASSWORD", inst.Password)
		}
		setFileDef("METABASE_INSTANCES", strings.Join(names, ","))
	}
}

func setFileDef(key, value string) {
	if value != "" {
		fileDefaults[key] = value
	}
}

// getEnv returns the value for key with priority: env var > config file > defaultVal.
func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	if v, ok := fileDefaults[key]; ok && v != "" {
		return v
	}
	return defaultVal
}
