package main

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
)

// ---------------------------------------------------------------------------
// Context key
// ---------------------------------------------------------------------------

type contextKey string

const credentialsCtxKey contextKey = "metabase_credentials"

func withCredentials(ctx context.Context, creds *Credentials) context.Context {
	return context.WithValue(ctx, credentialsCtxKey, creds)
}

func getCredsFromContext(ctx context.Context) *Credentials {
	v := ctx.Value(credentialsCtxKey)
	if v == nil {
		return nil
	}
	c, _ := v.(*Credentials)
	return c
}

// ---------------------------------------------------------------------------
// HTTP credential extraction
// ---------------------------------------------------------------------------

// extractCredentials reads per-request Metabase credentials from the HTTP
// request using the same three-priority scheme as the Python implementation:
//
//  1. Custom headers  (X-Metabase-Api-Key / X-Metabase-Email + X-Metabase-Password)
//  2. HTTP Basic Auth (Authorization: Basic …)
//  3. URL path prefix (/{base64url(user:pass)}/mcp)
//
// Returns the extracted credentials (nil if none found) and the "clean" path
// (with the credential prefix stripped when path encoding was used).
func extractCredentials(r *http.Request, cfg *Config) (*Credentials, string) {
	mbURL := strings.TrimSpace(r.Header.Get("X-Metabase-Url"))
	if mbURL == "" {
		mbURL = cfg.MetabaseURL
	}
	mbURL = strings.TrimRight(mbURL, "/")

	cleanPath := r.URL.Path

	// 1. Custom headers
	apiKey := strings.TrimSpace(r.Header.Get("X-Metabase-Api-Key"))
	email := strings.TrimSpace(r.Header.Get("X-Metabase-Email"))
	password := strings.TrimSpace(r.Header.Get("X-Metabase-Password"))

	if apiKey != "" {
		return &Credentials{MetabaseURL: mbURL, APIKey: apiKey}, cleanPath
	}
	if email != "" && password != "" {
		return &Credentials{MetabaseURL: mbURL, Email: email, Password: password}, cleanPath
	}

	// 2. HTTP Basic Auth
	if user, pass, ok := r.BasicAuth(); ok {
		return buildCreds(user, pass, mbURL), cleanPath
	}

	// 3. URL path prefix: /{base64url_creds}/mcp
	parts := splitPath(r.URL.Path)
	if len(parts) >= 2 {
		if user, pass, ok := decodePathCreds(parts[0]); ok {
			// Strip the credential segment — MCP handler sees the rest
			cleanPath = "/" + strings.Join(parts[1:], "/")
			return buildCreds(user, pass, mbURL), cleanPath
		}
	}

	return nil, cleanPath
}

// buildCreds maps (username, password) → Credentials.
// Supports the "apikey:<key>" convention.
func buildCreds(username, password, mbURL string) *Credentials {
	if strings.EqualFold(username, "apikey") {
		return &Credentials{MetabaseURL: mbURL, APIKey: password}
	}
	return &Credentials{MetabaseURL: mbURL, Email: username, Password: password}
}

// decodePathCreds attempts to decode a URL path segment as base64url(user:pass).
func decodePathCreds(segment string) (user, pass string, ok bool) {
	// Restore padding
	pad := (4 - len(segment)%4) % 4
	decoded, err := base64.URLEncoding.DecodeString(segment + strings.Repeat("=", pad))
	if err != nil {
		return "", "", false
	}
	s := string(decoded)
	idx := strings.IndexByte(s, ':')
	if idx <= 0 {
		return "", "", false
	}
	return s[:idx], s[idx+1:], true
}

// splitPath splits a URL path into non-empty segments.
func splitPath(path string) []string {
	var parts []string
	for _, p := range strings.Split(path, "/") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

// ---------------------------------------------------------------------------
// Credential middleware (HTTP transport)
// ---------------------------------------------------------------------------

// credMiddleware is a net/http middleware that extracts per-request Metabase
// credentials and injects them into the request context.  It also rewrites
// the URL path when credentials were embedded in it so the MCP handler sees
// the clean path (e.g. /mcp instead of /{base64creds}/mcp).
type credMiddleware struct {
	next http.Handler
	cfg  *Config
}

func (m *credMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	creds, cleanPath := extractCredentials(r, m.cfg)

	ctx := withCredentials(r.Context(), creds)

	if cleanPath != r.URL.Path {
		newURL := *r.URL
		newURL.Path = cleanPath
		newURL.RawPath = cleanPath
		r = r.Clone(ctx)
		r.URL = &newURL
	} else {
		r = r.WithContext(ctx)
	}

	m.next.ServeHTTP(w, r)
}
