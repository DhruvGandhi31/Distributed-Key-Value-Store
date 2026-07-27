package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
)

type Server struct {
	kv  *store.KVStore
	mux *http.ServeMux
}

func New(kv *store.KVStore) *Server {
	s := &Server{kv: kv, mux: http.NewServeMux()}
	s.mux.HandleFunc("/kv/", s.handleKey)
	s.mux.HandleFunc("/kv", s.handleScan)
	s.mux.HandleFunc("/healthz", s.handleHealthz)
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
		s.apply(w, store.Command{Op: store.OpPut, Key: key, Value: body})

	case http.MethodDelete:
		s.apply(w, store.Command{Op: store.OpDelete, Key: key})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) apply(w http.ResponseWriter, cmd store.Command) {
	data, err := cmd.Encode()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	result := s.kv.Apply(raft.LogEntry{Type: raft.EntryNormal, Data: data})
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
