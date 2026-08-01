package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
)

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
	kv   *store.KVStore
	node RaftNode
	mux  *http.ServeMux
}

// New builds the client-facing HTTP server. Pass a nil node to run without
// Raft (writes apply directly to kv, matching Phase 1 behavior); pass a
// live *raft.Node to route writes through Propose and enable
// /cluster/status.
func New(kv *store.KVStore, node RaftNode) *Server {
	s := &Server{kv: kv, node: node, mux: http.NewServeMux()}
	s.mux.HandleFunc("/kv/", s.handleKey)
	s.mux.HandleFunc("/kv", s.handleScan)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/cluster/status", s.handleClusterStatus)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
		val, ok := s.kv.Get(key)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(val)

	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		s.apply(w, r, store.Command{Op: store.OpPut, Key: key, Value: body})

	case http.MethodDelete:
		s.apply(w, r, store.Command{Op: store.OpDelete, Key: key})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
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
		result, err = s.node.Propose(r.Context(), data)
		if err != nil {
			// Any Propose failure (not the leader, shutting down, ctx
			// canceled) is surfaced as 503 for now — Phase 6 adds real
			// leader-redirect logic here instead of a flat error.
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	} else {
		result = s.kv.Apply(raft.LogEntry{Type: raft.EntryNormal, Data: data})
	}

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

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		State       string      `json:"state"`
		Leader      raft.NodeID `json:"leader"`
		Term        uint64      `json:"term"`
		CommitIndex uint64      `json:"commit_index"`
		LastApplied uint64      `json:"last_applied"`
	}{
		State:       status.State.String(),
		Leader:      status.Leader,
		Term:        status.Term,
		CommitIndex: status.CommitIndex,
		LastApplied: status.LastApplied,
	})
}
