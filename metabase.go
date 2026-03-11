package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

// Credentials holds the per-request Metabase authentication details.
type Credentials struct {
	MetabaseURL string
	Email       string
	Password    string
	APIKey      string
}

// key returns a stable hash string used as the client cache key.
func (c *Credentials) key() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%s|%s", c.MetabaseURL, c.Email, c.Password, c.APIKey)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// ---------------------------------------------------------------------------
// Session disk cache
// ---------------------------------------------------------------------------

const sessionCacheTTL = 24 * time.Hour

// userAgent is selected at startup based on the current OS so that Metabase
// sees a plausible browser string regardless of platform.
// version (injected via ldflags) is appended as "MetabaseMCP/<version>".
var userAgent = func() string {
	var base string
	switch runtime.GOOS {
	case "windows":
		base = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	case "darwin":
		base = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	default:
		base = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	return base + " MetabaseMCP/" + version
}()

type sessionEntry struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// diskStore is the JSON structure written to disk.
// sessions has a 24-hour TTL; devices is permanent and never cleared.
type diskStore struct {
	Sessions map[string]*sessionEntry `json:"sessions"`
	Devices  map[string]string        `json:"devices"` // credKey → metabase.DEVICE cookie value
}

// sessionDiskCache persists Metabase session tokens and device IDs to the
// user's local cache directory so that stdio-mode restarts can reuse them.
// Device IDs are stored permanently; session tokens expire after sessionCacheTTL.
type sessionDiskCache struct {
	mu       sync.Mutex
	path     string
	sessions map[string]*sessionEntry
	devices  map[string]string
}

var globalSessionCache *sessionDiskCache

func initSessionCache() {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		cacheDir = os.TempDir()
	}
	dir := filepath.Join(cacheDir, "metabase-mcp")
	_ = os.MkdirAll(dir, 0o700)
	path := filepath.Join(dir, "sessions.json")

	sc := &sessionDiskCache{
		path:     path,
		sessions: make(map[string]*sessionEntry),
		devices:  make(map[string]string),
	}
	sc.load()
	globalSessionCache = sc
}

func (sc *sessionDiskCache) load() {
	data, err := os.ReadFile(sc.path)
	if err != nil {
		return
	}
	var store diskStore
	if json.Unmarshal(data, &store) != nil {
		return
	}
	if store.Sessions != nil {
		sc.sessions = store.Sessions
	}
	if store.Devices != nil {
		sc.devices = store.Devices
	}
}

func (sc *sessionDiskCache) save() {
	store := diskStore{Sessions: sc.sessions, Devices: sc.devices}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(sc.path, data, 0o600)
}

// getSession returns a valid (non-expired) session token, or "" if absent/expired.
func (sc *sessionDiskCache) getSession(key string) string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	e, ok := sc.sessions[key]
	if !ok || time.Now().After(e.ExpiresAt) {
		return ""
	}
	return e.Token
}

// getDeviceID returns the permanent device ID for this credential set.
func (sc *sessionDiskCache) getDeviceID(key string) string {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.devices[key]
}

// setSession saves a session token with a TTL. If deviceID is non-empty it is
// also persisted permanently (overwriting any previous value).
func (sc *sessionDiskCache) setSession(key, token, deviceID string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.sessions[key] = &sessionEntry{
		Token:     token,
		ExpiresAt: time.Now().Add(sessionCacheTTL),
	}
	if deviceID != "" {
		sc.devices[key] = deviceID
	}
	sc.save()
}

// deleteSession removes only the session token; the device ID is kept forever.
func (sc *sessionDiskCache) deleteSession(key string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	delete(sc.sessions, key)
	sc.save()
}

// ---------------------------------------------------------------------------
// MetabaseClient
// ---------------------------------------------------------------------------

// MetabaseClient is a thread-safe HTTP client for the Metabase REST API.
type MetabaseClient struct {
	baseURL      string
	email        string
	password     string
	apiKey       string
	credKey      string // sha256 of credentials, used as disk cache key
	sessionToken string
	deviceID     string // metabase.DEVICE cookie, persisted across restarts
	http         *http.Client
	mu           sync.Mutex
}

func newMetabaseClient(creds *Credentials, proxyURL string) *MetabaseClient {
	c := &MetabaseClient{
		baseURL:  creds.MetabaseURL,
		email:    creds.Email,
		password: creds.Password,
		apiKey:   creds.APIKey,
		credKey:  creds.key(),
		http:     buildHTTPClient(proxyURL),
	}
	// Restore session and device ID from disk cache (email/password auth only).
	// Device ID is always restored (permanent); session token only if not expired.
	if globalSessionCache != nil && creds.APIKey == "" {
		c.deviceID = globalSessionCache.getDeviceID(c.credKey)
		if token := globalSessionCache.getSession(c.credKey); token != "" {
			c.sessionToken = token
			slog.Info("Restored session from disk cache", "email", creds.Email)
		}
	}
	return c
}

// buildHTTPClient creates an http.Client that:
//   - Uses proxyURL if non-empty.
//   - Explicitly ignores HTTP_PROXY / HTTPS_PROXY / NO_PROXY env vars otherwise
//     (transport.Proxy = nil → no proxy).
func buildHTTPClient(proxyURL string) *http.Client {
	// Start from a clone of DefaultTransport to keep good defaults
	// (TLS config, dial timeouts, keep-alive, etc.) but override Proxy.
	base, ok := http.DefaultTransport.(*http.Transport)
	var t *http.Transport
	if ok {
		t = base.Clone()
	} else {
		t = &http.Transport{}
	}

	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err == nil {
			t.Proxy = http.ProxyURL(u)
		} else {
			slog.Warn("Invalid METABASE_HTTP_PROXY URL, proxy disabled", "url", proxyURL, "err", err)
			t.Proxy = nil
		}
	} else {
		// Explicitly disable env-var proxy
		t.Proxy = nil
	}

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: t,
	}
}

// login obtains a Metabase session token via email + password.
// It sends the persisted device ID cookie (if any) so Metabase can recognise
// the client as a known device, and extracts a new device ID from the
// Set-Cookie response when one is issued.  The session token and device ID are
// persisted to disk so they survive process restarts (stdio mode).
func (c *MetabaseClient) login(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{
		"username": c.email,
		"password": c.password,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/session", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if c.deviceID != "" {
		req.AddCookie(&http.Cookie{Name: "metabase.DEVICE", Value: c.deviceID})
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("metabase login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("metabase authentication failed (%d): %s", resp.StatusCode, b)
	}

	// Extract device ID from Set-Cookie if the server issued one.
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "metabase.DEVICE" && cookie.Value != "" {
			c.deviceID = cookie.Value
			break
		}
	}

	var result map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode login response: %w", err)
	}
	token, _ := result["id"].(string)
	if token == "" {
		return fmt.Errorf("metabase login: missing session id in response")
	}
	c.sessionToken = token
	slog.Info("Acquired Metabase session token", "email", c.email, "device_id", c.deviceID)

	// Persist session token (TTL: sessionCacheTTL) and device ID (permanent).
	if globalSessionCache != nil {
		globalSessionCache.setSession(c.credKey, token, c.deviceID)
	}
	return nil
}

func (c *MetabaseClient) authHeaders(ctx context.Context) (map[string]string, error) {
	headers := map[string]string{"Content-Type": "application/json"}
	if c.apiKey != "" {
		headers["X-API-KEY"] = c.apiKey
		return headers, nil
	}
	if c.sessionToken == "" {
		if err := c.login(ctx); err != nil {
			return nil, err
		}
	}
	headers["X-Metabase-Session"] = c.sessionToken
	return headers, nil
}

// Request executes a Metabase API call and returns the decoded JSON body.
// It automatically retries once on 401 (session expiry) when using email/password auth.
func (c *MetabaseClient) Request(ctx context.Context, method, path string, body any, queryParams map[string]string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.doRequest(ctx, method, path, body, queryParams, true)
}

func (c *MetabaseClient) doRequest(
	ctx context.Context,
	method, path string,
	body any,
	queryParams map[string]string,
	retryAuth bool,
) (any, error) {
	apiURL := c.baseURL + "/api" + path

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, apiURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if len(queryParams) > 0 {
		q := req.URL.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		req.URL.RawQuery = q.Encode()
	}

	req.Header.Set("User-Agent", userAgent)

	headers, err := c.authHeaders(ctx)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("metabase API %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	// On 401, refresh session once (email/password only)
	if resp.StatusCode == http.StatusUnauthorized && retryAuth && c.apiKey == "" {
		slog.Info("Session expired, re-authenticating", "email", c.email)
		c.sessionToken = ""
		if globalSessionCache != nil {
			globalSessionCache.deleteSession(c.credKey)
		}
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		resp.Body.Close()
		return c.doRequest(ctx, method, path, body, queryParams, false)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("metabase API %s %s failed (%d): %s", method, path, resp.StatusCode, respBody)
	}

	if len(respBody) == 0 {
		return nil, nil
	}

	var result any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response JSON: %w", err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Client cache
// ---------------------------------------------------------------------------

type cacheEntry struct {
	client   *MetabaseClient
	lastUsed time.Time
}

// ClientCache caches MetabaseClient instances by credential hash with a TTL.
type ClientCache struct {
	mu      sync.Mutex
	entries map[string]*cacheEntry
	ttl     time.Duration
}

var globalCache = &ClientCache{
	entries: make(map[string]*cacheEntry),
}

func initCache(ttlSeconds int) {
	globalCache.ttl = time.Duration(ttlSeconds) * time.Second
}

// Get returns a cached MetabaseClient or creates a new one.
func (cc *ClientCache) Get(creds *Credentials, proxyURL string) *MetabaseClient {
	k := creds.key()
	now := time.Now()

	cc.mu.Lock()
	defer cc.mu.Unlock()

	if e, ok := cc.entries[k]; ok {
		if now.Sub(e.lastUsed) < cc.ttl {
			e.lastUsed = now
			return e.client
		}
		// Expired — evict
		delete(cc.entries, k)
	}

	client := newMetabaseClient(creds, proxyURL)
	cc.entries[k] = &cacheEntry{client: client, lastUsed: now}
	return client
}

// getClient resolves credentials for the current request (from context or
// server defaults) and returns a cached MetabaseClient.
func getClient(ctx context.Context, cfg *Config) (*MetabaseClient, error) {
	creds := getCredsFromContext(ctx)
	if creds == nil {
		creds = &Credentials{
			MetabaseURL: cfg.MetabaseURL,
			Email:       cfg.DefaultEmail,
			Password:    cfg.DefaultPassword,
			APIKey:      cfg.DefaultAPIKey,
		}
	}

	if creds.MetabaseURL == "" {
		return nil, fmt.Errorf(
			"no Metabase URL configured — set METABASE_URL env var " +
				"or pass X-Metabase-Url header")
	}
	if creds.APIKey == "" && creds.Email == "" {
		return nil, fmt.Errorf(
			"no Metabase credentials — use http://email:password@server/mcp, " +
				"http://apikey:<key>@server/mcp, or set METABASE_USER_EMAIL/METABASE_PASSWORD " +
				"or METABASE_API_KEY env vars")
	}

	return globalCache.Get(creds, cfg.MetabaseHTTPProxy), nil
}
