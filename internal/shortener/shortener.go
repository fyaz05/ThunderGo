// Package shortener wraps an external URL-shortener API. When configured
// (ShortenerAPIKey + ShortenerSite), the service shortens stream/download
// links for non-authorized users. Owner + authorized users always receive
// full-length links (enforced by the caller).
//
// Shortened URLs are cached in a 10k-entry LRU (no TTL — shortened URLs are
// immutable). singleflight collapses concurrent requests for the same URL.
package shortener

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Shortener calls an external URL-shortener API. A nil *Shortener or empty
// API key disables shortening. Configure via New.
type Shortener struct {
	apiKey string
	site   string
	log    *slog.Logger
	client *http.Client

	mu    sync.Mutex
	cache *lruCache // long URL → short URL, LRU-evicted at 10k entries

	sf singleflight.Group
}

// lruCache is a bounded LRU mapping string→string. Front of the order list
// is most-recently-used; back is least-recently-used. NOT goroutine-safe —
// the caller (Shortener) holds mu around get/put.
type lruCache struct {
	cap   int
	items map[string]*list.Element
	order *list.List // front = most recently used
}

// lruEntry is the value stashed in each list.Element.
type lruEntry struct {
	key   string
	value string
}

func newLRUCache(cap int) *lruCache {
	return &lruCache{cap: cap, items: make(map[string]*list.Element, cap), order: list.New()}
}

// get returns the cached value for key, marking it most-recently-used.
func (l *lruCache) get(key string) (string, bool) {
	if l == nil {
		return "", false
	}
	el, ok := l.items[key]
	if !ok {
		return "", false
	}
	l.order.MoveToFront(el)
	return el.Value.(*lruEntry).value, true
}

// put inserts or updates an entry, marking it most-recently-used. Evicts
// the least-recently-used entry when the cap is exceeded.
func (l *lruCache) put(key, value string) {
	if l == nil {
		return
	}
	if el, ok := l.items[key]; ok {
		el.Value.(*lruEntry).value = value
		l.order.MoveToFront(el)
		return
	}
	el := l.order.PushFront(&lruEntry{key: key, value: value})
	l.items[key] = el
	if l.order.Len() > l.cap {
		oldest := l.order.Back()
		if oldest != nil {
			l.order.Remove(oldest)
			delete(l.items, oldest.Value.(*lruEntry).key)
		}
	}
}

// shortenerResponse covers common field names used by popular URL-shortener
// APIs (is.gd uses "shorturl", kutt uses "short" or "url"). First non-empty
// field wins.
type shortenerResponse struct {
	ShortURL string `json:"shorturl"`
	URL      string `json:"url"`
	Short    string `json:"short"`
}

// New returns a Shortener. Returns (nil, nil) if either credential is empty or
// if the site URL is not served over HTTPS. An http:// site is rejected with
// a warning. Returns an error if the site URL is malformed.
func New(apiKey, site string, log *slog.Logger) (*Shortener, error) {
	if apiKey == "" || site == "" {
		return nil, nil
	}
	switch {
	case strings.HasPrefix(site, "https://"):
		// OK — secure scheme.
	case strings.HasPrefix(site, "http://"):
		if log != nil {
			log.Warn("shortener site uses insecure http://; disabling shortener", "site", site)
		}
		return nil, nil
	default:
		if log != nil {
			log.Warn("shortener site lacks an https:// scheme; disabling shortener", "site", site)
		}
		return nil, nil
	}
	if _, err := url.Parse(site); err != nil {
		return nil, fmt.Errorf("shortener: invalid site URL %q: %w", site, err)
	}
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
		// Don't follow redirects — a compromised shortener could redirect
		// to an arbitrary host. The returned URL's host is validated
		// against s.site in callAPI.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: transport,
	}
	return &Shortener{
		apiKey: apiKey,
		site:   strings.TrimRight(site, "/"),
		log:    log,
		client: client,
		cache:  newLRUCache(10000),
	}, nil
}

// Shorten returns the shortened URL for the given long URL. On any error,
// returns the long URL unchanged — shortening is best-effort. Results are
// cached so repeated calls for the same URL don't hit the API.
func (s *Shortener) Shorten(ctx context.Context, longURL string) string {
	if s == nil || longURL == "" {
		return longURL
	}

	// Cache hit (LRU get requires write lock for MoveToFront).
	s.mu.Lock()
	if short, ok := s.cache.get(longURL); ok {
		s.mu.Unlock()
		return short
	}
	s.mu.Unlock()

	// Cache miss — singleflight collapses concurrent requests for the same
	// long URL into a single upstream call.
	v, _, _ := s.sf.Do(longURL, func() (any, error) {
		return s.callAPI(context.Background(), longURL), nil
	})
	short, _ := v.(string)
	if short == "" || short == longURL {
		return longURL
	}

	s.mu.Lock()
	s.cache.put(longURL, short)
	s.mu.Unlock()

	return short
}

func (s *Shortener) callAPI(ctx context.Context, longURL string) string {
	// The API key is sent via Authorization header (not the query string,
	// which is routinely logged by proxies/CDNs). The long URL is not a
	// secret — it's the URL being shortened.
	u := fmt.Sprintf("%s/api?url=%s", s.site, url.QueryEscape(longURL))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return longURL
	}
	if s.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.apiKey)
	}
	if s.log != nil && s.log.Enabled(context.Background(), slog.LevelDebug) {
		s.log.Debug("shortener request", "url", u) // Authorization header intentionally redacted
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if s.log != nil {
			s.log.Warn("shortener request failed; returning long URL", "error", err)
		}
		return longURL
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		if s.log != nil {
			s.log.Warn("shortener returned non-200; returning long URL", "status", resp.StatusCode)
		}
		return longURL
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if err != nil {
		return longURL
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	short := parseShortenerBody(string(body))
	if short == "" {
		return longURL
	}
	// Validate the response is actually an http(s) URL before returning it.
	if !strings.HasPrefix(short, "http://") && !strings.HasPrefix(short, "https://") {
		return longURL
	}
	// Validate the returned URL's host matches the configured site — a
	// compromised shortener could otherwise return any URL.
	if !hostMatches(short, s.site) {
		if s.log != nil {
			s.log.Warn("shortener returned URL with unexpected host; returning long URL",
				"expected_host", siteHost(s.site), "got_host", shortURLHost(short))
		}
		return longURL
	}
	return short
}

// parseShortenerBody extracts the short URL from the API response body.
// Bodies starting with `{` are parsed as JSON (common field names
// shorturl/url/short); other bodies are treated as plain text. Returns an
// empty string if no URL could be extracted.
func parseShortenerBody(body string) string {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "{") {
		var sr shortenerResponse
		if err := json.Unmarshal([]byte(trimmed), &sr); err == nil {
			for _, v := range []string{sr.ShortURL, sr.URL, sr.Short} {
				if v != "" {
					return strings.TrimSpace(v)
				}
			}
		}
		return ""
	}
	return trimmed
}

// siteHost returns the host portion of the configured site URL (for logs).
func siteHost(site string) string {
	parsed, err := url.Parse(site)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return parsed.Host
}

// shortURLHost returns the host portion of a short URL (for logs).
func shortURLHost(shortURL string) string {
	parsed, err := url.Parse(shortURL)
	if err != nil || parsed.Host == "" {
		return ""
	}
	return parsed.Host
}

// normHost returns the lowercased, port-stripped hostname of a URL. Used
// for equality comparisons so "https://Example.com" matches
// "https://example.com:443/". Returns "" on parse failure or empty host.
func normHost(u string) string {
	p, err := url.Parse(u)
	if err != nil {
		return ""
	}
	return strings.ToLower(p.Hostname())
}

// hostMatches reports whether the host of shortURL matches the host of
// site, ignoring case and port. Mismatch (or parse failure) returns false.
func hostMatches(shortURL, site string) bool {
	h := normHost(shortURL)
	return h != "" && h == normHost(site)
}
