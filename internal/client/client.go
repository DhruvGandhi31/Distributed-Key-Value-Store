// Package client implements a leader-aware SDK for talking to a kvstored
// cluster: it starts from a caller-supplied set of candidate node
// addresses, follows HTTP 421 + X-Leader-URL redirects to find the current
// leader, caches that leader for subsequent calls, and falls back to
// retrying across the candidate set (with backoff) if the cached leader
// becomes unreachable or a majority can't be found (e.g. mid-election).
// Callers (cmd/client and anything else that talks to the cluster) never
// need to know which node is currently the leader.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ErrMaxAttempts is returned by Client methods when every candidate
// endpoint and every leader redirect has been tried without a usable
// response.
var ErrMaxAttempts = errors.New("client: exhausted retries without finding a working leader")

// Config configures a Client.
type Config struct {
	// Endpoints is the set of candidate node client-facing base URLs (e.g.
	// "http://127.0.0.1:8001"). Required, at least one. The client starts
	// with the first reachable one and follows leader redirects from
	// there; Endpoints is only the fallback set for when the cached leader
	// itself can't be reached (connection refused/timeout) or the cluster
	// is between leaders.
	Endpoints []string

	// MaxAttempts caps how many endpoints/redirects a single call will
	// follow before giving up. Defaults to 2*len(Endpoints)+1 if <= 0 —
	// enough to survive one full round of "every endpoint is stale" plus
	// a couple of redirect hops.
	MaxAttempts int

	// HTTPClient is used for requests; defaults to a plain http.Client
	// with a 5s timeout if nil.
	HTTPClient *http.Client
}

// Client is a leader-aware kvstored client. Safe for concurrent use.
type Client struct {
	cfg   Config
	httpc *http.Client

	mu     sync.Mutex
	leader string // cached last-known leader base URL; "" if unknown
}

// New constructs a Client from cfg.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("client: Config.Endpoints must not be empty")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 2*len(cfg.Endpoints) + 1
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{cfg: cfg, httpc: cfg.HTTPClient}, nil
}

// Put writes key=value through the cluster, transparently finding the
// leader and retrying on failover.
func (c *Client) Put(ctx context.Context, key string, value []byte) error {
	resp, err := c.do(ctx, http.MethodPut, "/kv/"+url.PathEscape(key), value)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("client: put failed: %s: %s", resp.Status, body)
	}
	return nil
}

// Get reads key. ok is false if the key doesn't exist. Reads are served by
// whichever node answers — not routed through the leader — so this can
// return slightly stale data after a partition (see CLAUDE.md's read-index
// note; not addressed until Phase 10).
func (c *Client) Get(ctx context.Context, key string) (value []byte, ok bool, err error) {
	resp, err := c.do(ctx, http.MethodGet, "/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, false, fmt.Errorf("client: get failed: %s: %s", resp.Status, body)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

// Delete removes key through the cluster, transparently finding the leader
// and retrying on failover.
func (c *Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/kv/"+url.PathEscape(key), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("client: delete failed: %s: %s", resp.Status, body)
	}
	return nil
}

// Scan lists every key with the given prefix (sorted), as [key, value]
// pairs. Same read-only caveat as Get.
func (c *Client) Scan(ctx context.Context, prefix string) ([][2]string, error) {
	resp, err := c.do(ctx, http.MethodGet, "/kv?prefix="+url.QueryEscape(prefix), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("client: scan failed: %s: %s", resp.Status, body)
	}
	var results [][2]string
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}

// do sends method+path (+body, if non-nil) to the cluster, following
// leader redirects and retrying across the candidate endpoint set on
// failure, up to cfg.MaxAttempts total tries. The caller owns closing the
// returned response's Body.
func (c *Client) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt < c.cfg.MaxAttempts; attempt++ {
		base := c.pickEndpoint(attempt)

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, base+path, bodyReader)
		if err != nil {
			return nil, err
		}

		resp, err := c.httpc.Do(req)
		if err != nil {
			// Connection refused/timeout/etc — this endpoint (possibly our
			// cached leader) is no good right now. Drop the cached leader
			// so the next attempt round-robins the configured set instead
			// of hammering the same dead address.
			lastErr = err
			c.clearLeader()
			sleep(ctx, backoff(attempt))
			continue
		}

		switch resp.StatusCode {
		case http.StatusMisdirectedRequest:
			// Not the leader; follow the redirect if one was given.
			leaderURL := resp.Header.Get("X-Leader-URL")
			resp.Body.Close()
			if leaderURL != "" {
				c.setLeader(leaderURL)
				continue
			}
			c.clearLeader()
			sleep(ctx, backoff(attempt))
			continue

		case http.StatusServiceUnavailable:
			// Leader unknown cluster-wide (mid-election) or node shutting
			// down. Back off and retry rather than treating it as fatal.
			resp.Body.Close()
			c.clearLeader()
			sleep(ctx, backoff(attempt))
			continue

		default:
			return resp, nil
		}
	}

	if lastErr != nil {
		return nil, fmt.Errorf("client: giving up after %d attempts: %w", c.cfg.MaxAttempts, lastErr)
	}
	return nil, ErrMaxAttempts
}

// pickEndpoint returns the cached leader if one is known, otherwise
// round-robins across the configured candidate set.
func (c *Client) pickEndpoint(attempt int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	return c.cfg.Endpoints[attempt%len(c.cfg.Endpoints)]
}

func (c *Client) setLeader(base string) {
	c.mu.Lock()
	c.leader = base
	c.mu.Unlock()
}

func (c *Client) clearLeader() {
	c.mu.Lock()
	c.leader = ""
	c.mu.Unlock()
}

// backoff returns a short, linearly-increasing delay capped at 200ms, so
// a run of retries (e.g. while the cluster is mid-election, typically
// settled within a few hundred ms) doesn't spin hot but also doesn't stall
// a caller for long.
func backoff(attempt int) time.Duration {
	d := time.Duration(attempt+1) * 20 * time.Millisecond
	if d > 200*time.Millisecond {
		d = 200 * time.Millisecond
	}
	return d
}

// sleep waits for d or until ctx is done, whichever comes first.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
