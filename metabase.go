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
// MetabaseClient
// ---------------------------------------------------------------------------

// MetabaseClient is a thread-safe HTTP client for the Metabase REST API.
type MetabaseClient struct {
	baseURL      string
	email        string
	password     string
	apiKey       string
	sessionToken string
	http         *http.Client
	mu           sync.Mutex
}

func newMetabaseClient(creds *Credentials, proxyURL string) *MetabaseClient {
	return &MetabaseClient{
		baseURL:  creds.MetabaseURL,
		email:    creds.Email,
		password: creds.Password,
		apiKey:   creds.APIKey,
		http:     buildHTTPClient(proxyURL),
	}
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

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("metabase login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("metabase authentication failed (%d): %s", resp.StatusCode, b)
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
	slog.Info("Acquired Metabase session token", "email", c.email)
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
