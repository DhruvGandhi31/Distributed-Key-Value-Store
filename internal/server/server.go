package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
)

// Metrics is the metrics-reporting interface Server depends on, injected
// the same way raft.Metrics is injected into raft.Node — so this package
// never needs to know anything about Prometheus directly. Handler serves
// whatever /metrics should return.
type Metrics interface {
	IncOp(op string)
	ObserveOpDuration(op string, seconds float64)
	Handler() http.Handler
}

// noopMetrics is the default used when New is called with a nil Metrics;
// its Handler 404s rather than serving an empty or fake metrics page.
type noopMetrics struct{}

func (noopMetrics) IncOp(string)                      {}
func (noopMetrics) ObserveOpDuration(string, float64) {}
func (noopMetrics) Handler() http.Handler             { return http.NotFoundHandler() }

// RaftNode is the subset of *raft.Node the client-facing server needs:
// submitting writes for replication and reporting cluster status. A nil
// RaftNode means Raft is disabled (Phase 1 mode) and writes apply directly
// to kv instead.
type RaftNode interface {
	Propose(ctx context.Context, data []byte) (interface{}, error)
	Status(ctx context.Context) (raft.Status, error)
}

var _ RaftNode = (*raft.Node)(nil)

type Server struct {
	kv      *store.KVStore
	node    RaftNode
	mux     *http.ServeMux
	metrics Metrics
	demo    *DemoConfig // nil unless EnableDemo was called

	// clientURLs maps every node's Raft ID to its client-facing base URL
	// (e.g. "http://127.0.0.1:8002"), so a follower can tell a caller where
	// to find the leader instead of just failing the write. nil/empty is
	// fine — redirection degrades to a plain 503 if a leader ID can't be
	// resolved to a URL.
	clientURLs map[raft.NodeID]string
}

// New builds the client-facing HTTP server. Pass a nil node to run without
// Raft (writes apply directly to kv, matching Phase 1 behavior); pass a
// live *raft.Node to route writes through Propose and enable
// /cluster/status. clientURLs is only consulted when node is non-nil; see
// the Server.clientURLs field doc for its format. metrics may be nil (kv_*
// metrics become no-ops and /metrics 404s).
func New(kv *store.KVStore, node RaftNode, clientURLs map[raft.NodeID]string, metrics Metrics) *Server {
	if metrics == nil {
		metrics = noopMetrics{}
	}
	s := &Server{kv: kv, node: node, clientURLs: clientURLs, mux: http.NewServeMux(), metrics: metrics}
	// /kv/ (trailing slash, single key) vs /kv (scan) - easy to typo, don't
	s.mux.HandleFunc("/kv/", s.handleKey)
	s.mux.HandleFunc("/kv", s.handleScan)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/cluster/status", s.handleClusterStatus)

	s.mux.Handle("/metrics", metrics.Handler())

	return s
}

// DebugHandler serves /debug/pprof/. It is deliberately not part of the
// client API: the client port is public, and pprof exposes the process's
// command line and lets any caller trigger CPU-heavy profiles. Serve it on
// a separate, private listener (kvstored's --debug-addr) instead.
//
// pprof registers onto http.DefaultServeMux via its own init() when
// blank-imported, so the handlers are wired in by hand here, matching
// exactly what the stdlib does for DefaultServeMux. pprof.Index itself
// dispatches the named profiles (heap, goroutine, threadcreate, block,
// mutex, allocs) under /debug/pprof/<name>, so only these 5 routes are
// needed.
func DebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if withCORS(w, r) {
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		defer s.timeOp("get")()
		val, ok := s.kv.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(val)

	case http.MethodPut:
		defer s.timeOp("put")()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		s.apply(w, r, store.Command{Op: store.OpPut, Key: key, Value: body})

	case http.MethodDelete:
		defer s.timeOp("delete")()
		s.apply(w, r, store.Command{Op: store.OpDelete, Key: key})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// timeOp returns a function that, when deferred, records both the op
// counter and its duration in one place — call as `defer s.timeOp("get")()`
// at the top of a handler branch.
func (s *Server) timeOp(op string) func() {
	start := time.Now()
	return func() {
		s.metrics.IncOp(op)
		s.metrics.ObserveOpDuration(op, time.Since(start).Seconds())
	}
}

// apply commits cmd through Raft (if enabled) or straight to kv (Phase 1
// mode), then writes the HTTP response from the resulting ApplyResult.
func (s *Server) apply(w http.ResponseWriter, r *http.Request, cmd store.Command) {
	data, err := cmd.Encode()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var result interface{}
	if s.node != nil {
		if !s.requireLeader(w, r.Context()) {
			return
		}
		result, err = s.node.Propose(r.Context(), data)
		if err != nil {
			// requireLeader already redirected the common case (this node
			// isn't the leader); getting here means it stepped down in the
			// narrow window between that check and this Propose call, or
			// is shutting down. Rare enough that a flat 503 (the smart
			// client backs off and retries, re-discovering the leader on
			// its next attempt) is fine rather than re-deriving a redirect.
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	} else {
		result = s.kv.Apply(raft.LogEntry{Type: raft.EntryNormal, Data: data})
	}

	// this cast should always succeed since Apply only ever returns
	// *ApplyResult, but cheap to check instead of trusting past-me
	res, ok := result.(*store.ApplyResult)
	if !ok || res == nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if res.Err != "" {
		http.Error(w, res.Err, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// requireLeader checks whether this node is currently the Raft leader
// before a write is attempted. If it isn't, it points the caller at the
// leader via the X-Leader-URL response header (when the leader's ID is
// known and resolves to a client-facing URL) and responds 421 Misdirected
// Request — the smart client (internal/client) knows to follow that header
// and retry there instead of failing outright. If the leader is currently
// unknown (e.g. mid-election) or unmapped, it falls back to a plain 503 so
// the caller backs off and retries instead of chasing an empty redirect.
func (s *Server) requireLeader(w http.ResponseWriter, ctx context.Context) bool {
	status, err := s.node.Status(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return false
	}
	if status.State == raft.Leader {
		return true
	}
	if url, ok := s.clientURLs[status.Leader]; ok {
		w.Header().Set("X-Leader-URL", url)
		http.Error(w, "not the leader", http.StatusMisdirectedRequest)
		return false
	}
	http.Error(w, "not the leader, and current leader is unknown", http.StatusServiceUnavailable)
	return false
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer s.timeOp("scan")()
	prefix := r.URL.Query().Get("prefix")
	results := s.kv.Scan(prefix)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if s.node == nil {
		http.Error(w, "raft not enabled", http.StatusNotFound)
		return
	}
	status, err := s.node.Status(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var id raft.NodeID
	var isolatedMS int64
	if s.demo != nil {
		id = s.demo.NodeID
		if s.demo.Isolator != nil {
			isolatedMS = s.demo.Isolator.Remaining().Milliseconds()
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		ID          raft.NodeID `json:"id,omitempty"`
		State       string      `json:"state"`
		Leader      raft.NodeID `json:"leader"`
		Term        uint64      `json:"term"`
		CommitIndex uint64      `json:"commit_index"`
		LastApplied uint64      `json:"last_applied"`
		IsolatedMS  int64       `json:"isolated_ms"`
	}{
		ID:          id,
		State:       status.State.String(),
		Leader:      status.Leader,
		Term:        status.Term,
		CommitIndex: status.CommitIndex,
		LastApplied: status.LastApplied,
		IsolatedMS:  isolatedMS,
	})
}
