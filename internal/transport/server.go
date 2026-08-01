package transport

import (
	"encoding/gob"
	"net/http"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

// RaftHandler exposes a Node's RPC entry points over HTTP+gob so peers can
// reach it as a raft.Transport target on the wire.
type RaftHandler struct {
	node *raft.Node
	mux  *http.ServeMux
}

func NewRaftHandler(node *raft.Node) *RaftHandler {
	h := &RaftHandler{node: node, mux: http.NewServeMux()}
	h.mux.HandleFunc("/raft/vote", h.handleRequestVote)
	h.mux.HandleFunc("/raft/append", h.handleAppendEntries)
	h.mux.HandleFunc("/raft/install-snapshot", h.handleInstallSnapshot)
	return h
}

func (h *RaftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
