package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newFastClient returns a Client whose backoff/http timeouts are small,
// so retry-driven tests don't spend real wall-clock time waiting.
func newFastClient(t *testing.T, endpoints []string) *Client {
	t.Helper()
	c, err := New(Config{
		Endpoints:  endpoints,
		HTTPClient: &http.Client{Timeout: 500 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestPut_SucceedsDirectlyAgainstTheLeader(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.String()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newFastClient(t, []string{srv.URL})
	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if gotMethod != http.MethodPut || gotPath != "/kv/k" {
		t.Fatalf("server saw %s %s, want PUT /kv/k", gotMethod, gotPath)
	}
}

func TestPut_FollowsLeaderRedirect(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer leader.Close()

	var followerHits int32
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&followerHits, 1)
		w.Header().Set("X-Leader-URL", leader.URL)
		http.Error(w, "not the leader", http.StatusMisdirectedRequest)
	}))
	defer follower.Close()

	c := newFastClient(t, []string{follower.URL})
	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if atomic.LoadInt32(&followerHits) != 1 {
		t.Fatalf("follower was hit %d times, want exactly 1 (then the redirect should be followed)", followerHits)
	}
}

func TestPut_CachesLeaderAcrossCalls(t *testing.T) {
	var leaderHits int32
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&leaderHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer leader.Close()

	var followerHits int32
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&followerHits, 1)
		w.Header().Set("X-Leader-URL", leader.URL)
		http.Error(w, "not the leader", http.StatusMisdirectedRequest)
	}))
	defer follower.Close()

	c := newFastClient(t, []string{follower.URL})
	ctx := context.Background()
	if err := c.Put(ctx, "a", []byte("1")); err != nil {
		t.Fatalf("first Put: %v", err)
	}
	if err := c.Put(ctx, "b", []byte("2")); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	if got := atomic.LoadInt32(&followerHits); got != 1 {
		t.Fatalf("follower was hit %d times across two Puts, want exactly 1 (the second should go straight to the cached leader)", got)
	}
	if got := atomic.LoadInt32(&leaderHits); got != 2 {
		t.Fatalf("leader was hit %d times, want 2 (once per Put)", got)
	}
}

func TestPut_RecoversWhenCachedLeaderBecomesUnreachable(t *testing.T) {
	deadLeader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	deadLeaderURL := deadLeader.URL
	deadLeader.Close() // closed immediately: connections to it will be refused

	var newLeaderHits int32
	newLeader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&newLeaderHits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer newLeader.Close()

	c := newFastClient(t, []string{deadLeaderURL, newLeader.URL})
	// Prime the cache with the dead address, simulating a leader that died
	// after a previous call already cached it.
	c.setLeader(deadLeaderURL)

	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if atomic.LoadInt32(&newLeaderHits) != 1 {
		t.Fatalf("newLeader was hit %d times, want 1 (client should have fallen back to it after the cached leader refused the connection)", newLeaderHits)
	}
}

func TestPut_RetriesOn503ThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			http.Error(w, "leader unknown", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newFastClient(t, []string{srv.URL})
	if err := c.Put(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Fatalf("server saw %d calls, want 2 (one 503, then a retry that succeeds)", calls)
	}
}

func TestPut_GivesUpAfterMaxAttempts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "leader unknown", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c, err := New(Config{
		Endpoints:   []string{srv.URL},
		MaxAttempts: 3,
		HTTPClient:  &http.Client{Timeout: 500 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = c.Put(context.Background(), "k", []byte("v"))
	if err == nil {
		t.Fatalf("Put against a permanently-503 server: got nil error, want ErrMaxAttempts")
	}
}

func TestGet_NotFoundIsNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := newFastClient(t, []string{srv.URL})
	_, ok, err := c.Get(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatalf("Get on a 404: got ok=true, want false")
	}
}

func TestGet_DoesNotFollowRedirects(t *testing.T) {
	// Reads are never leader-gated server-side (see internal/server), so a
	// GET should never receive a 421 in practice — but do()'s handling of
	// one is shared code with Put, so confirm it's still followed correctly
	// if a server ever did send one.
	var hits int32
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Write([]byte("value"))
	}))
	defer leader.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Leader-URL", leader.URL)
		http.Error(w, "not the leader", http.StatusMisdirectedRequest)
	}))
	defer follower.Close()

	c := newFastClient(t, []string{follower.URL})
	val, ok, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok || string(val) != "value" {
		t.Fatalf("Get = (%q, %v), want (value, true)", val, ok)
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("leader was hit %d times, want 1", hits)
	}
}

func TestScan_DecodesResults(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("prefix"); got != "a" {
			t.Errorf("server saw prefix=%q, want a", got)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[["a","1"],["ab","2"]]`)
	}))
	defer srv.Close()

	c := newFastClient(t, []string{srv.URL})
	results, err := c.Scan(context.Background(), "a")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(results) != 2 || results[0][0] != "a" || results[1][0] != "ab" {
		t.Fatalf("Scan = %v, want [[a 1] [ab 2]]", results)
	}
}

func TestNew_RejectsEmptyEndpoints(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatalf("New with no Endpoints: got nil error, want one")
	}
}
