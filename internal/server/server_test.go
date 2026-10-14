package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
)

// fakeRaftNode is a test double for RaftNode: its Status is fixed at
// construction, and every Propose call is recorded so tests can assert
// whether requireLeader actually let one through.
type fakeRaftNode struct {
	mu sync.Mutex

	status    raft.Status
	statusErr error

	proposeErr    error
	proposeResult interface{}
	proposeCalls  int
}

func (f *fakeRaftNode) Status(ctx context.Context) (raft.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status, f.statusErr
}

func (f *fakeRaftNode) Propose(ctx context.Context, data []byte) (interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.proposeCalls++
	if f.proposeErr != nil {
		return nil, f.proposeErr
	}
	return f.proposeResult, nil
}

func (f *fakeRaftNode) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.proposeCalls
}

func TestPhase1Mode_WritesApplyDirectlyWithoutARaftNode(t *testing.T) {
	kv := store.New()
	s := New(kv, nil, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/hello", strings.NewReader("world"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200; body: %s", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/kv/hello", nil)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", w.Code)
	}
	if w.Body.String() != "world" {
		t.Fatalf("GET body = %q, want world", w.Body.String())
	}
}

func TestApply_LeaderProposesAndSucceeds(t *testing.T) {
	node := &fakeRaftNode{
		status:        raft.Status{State: raft.Leader},
		proposeResult: &store.ApplyResult{OK: true},
	}
	s := New(store.New(), node, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if node.callCount() != 1 {
		t.Fatalf("Propose called %d times, want 1", node.callCount())
	}
}

func TestApply_FollowerRedirectsToKnownLeader(t *testing.T) {
	node := &fakeRaftNode{
		status: raft.Status{State: raft.Follower, Leader: raft.NodeID("node2")},
	}
	clientURLs := map[raft.NodeID]string{"node2": "http://127.0.0.1:8002"}
	s := New(store.New(), node, clientURLs, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want 421; body: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Leader-URL"); got != "http://127.0.0.1:8002" {
		t.Fatalf("X-Leader-URL = %q, want http://127.0.0.1:8002", got)
	}
	if node.callCount() != 0 {
		t.Fatalf("Propose called %d times, want 0 — a follower must not propose at all", node.callCount())
	}
}

func TestApply_FollowerWithUnknownLeaderReturns503NotABareRedirect(t *testing.T) {
	node := &fakeRaftNode{
		status: raft.Status{State: raft.Follower, Leader: raft.NoLeader}, // mid-election
	}
	s := New(store.New(), node, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if got := w.Header().Get("X-Leader-URL"); got != "" {
		t.Fatalf("X-Leader-URL = %q, want empty — no redirect target should be sent for an unknown leader", got)
	}
}

func TestApply_FollowerLeaderIDNotInClientURLsReturns503(t *testing.T) {
	node := &fakeRaftNode{
		status: raft.Status{State: raft.Follower, Leader: raft.NodeID("node2")},
	}
	// clientURLs doesn't mention node2 at all — e.g. --client-peers wasn't passed.
	s := New(store.New(), node, map[raft.NodeID]string{}, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestApply_ProposeErrorAfterRaceReturns503(t *testing.T) {
	// requireLeader saw this node as leader, but it stepped down in the
	// narrow window before Propose actually ran.
	node := &fakeRaftNode{
		status:     raft.Status{State: raft.Leader},
		proposeErr: raft.ErrNotLeader,
	}
	s := New(store.New(), node, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
}

func TestApply_StatusErrorReturns503(t *testing.T) {
	node := &fakeRaftNode{statusErr: errors.New("boom")}
	s := New(store.New(), node, nil, nil)

	req := httptest.NewRequest(http.MethodPut, "/kv/k", strings.NewReader("v"))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if node.callCount() != 0 {
		t.Fatalf("Propose called %d times, want 0", node.callCount())
	}
}

func TestHandleClusterStatus(t *testing.T) {
	node := &fakeRaftNode{
		status: raft.Status{State: raft.Leader, Leader: "node1", Term: 5, CommitIndex: 3, LastApplied: 3},
	}
	s := New(store.New(), node, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/cluster/status", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var body struct {
		State       string      `json:"state"`
		Leader      raft.NodeID `json:"leader"`
		Term        uint64      `json:"term"`
		CommitIndex uint64      `json:"commit_index"`
		LastApplied uint64      `json:"last_applied"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.State != "Leader" || body.Leader != "node1" || body.Term != 5 {
		t.Fatalf("decoded body = %+v, want State=Leader Leader=node1 Term=5", body)
	}
}

func TestHandleClusterStatus_NotFoundWithoutRaft(t *testing.T) {
	s := New(store.New(), nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/cluster/status", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestHandleKey_GetMissingIsNotFound(t *testing.T) {
	s := New(store.New(), nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}
