package transport

import (
	"encoding/gob"
	"net/http"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

// RaftHandler exposes a Node's RPC entry points over HTTP+gob so peers can
// reach it as a raft.Transport target on the wire.
type RaftHandler struct {
	node      *raft.Node
	mux       *http.ServeMux
	partition *Partition
}

// NewRaftHandler builds the peer-facing handler. partition may be nil; when
// non-nil and active, every inbound RPC is refused with 503.
func NewRaftHandler(node *raft.Node, partition *Partition) *RaftHandler {
	h := &RaftHandler{node: node, mux: http.NewServeMux(), partition: partition}
	// mirrors http.go's client one for one, keep them in sync if you add a route
	h.mux.HandleFunc("/raft/vote", h.handleRequestVote)
	h.mux.HandleFunc("/raft/append", h.handleAppendEntries)
	h.mux.HandleFunc("/raft/install-snapshot", h.handleInstallSnapshot)
	return h
}

func (h *RaftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.partition.Isolated() {
		http.Error(w, ErrIsolated.Error(), http.StatusServiceUnavailable)
		return
	}
	h.mux.ServeHTTP(w, r)
}

func (h *RaftHandler) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var args raft.RequestVoteArgs
	if err := gob.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// this only errors on shutdown/ctx timeout, not on a "no" vote - a
	// rejected vote is still a 200 with VoteGranted: false
	reply, err := h.node.HandleRequestVote(r.Context(), args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	gob.NewEncoder(w).Encode(reply)
}

func (h *RaftHandler) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var args raft.AppendEntriesArgs
	if err := gob.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reply, err := h.node.HandleAppendEntries(r.Context(), args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	gob.NewEncoder(w).Encode(reply)
}

func (h *RaftHandler) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var args raft.InstallSnapshotArgs
	if err := gob.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reply, err := h.node.HandleInstallSnapshot(r.Context(), args)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	gob.NewEncoder(w).Encode(reply)
}
