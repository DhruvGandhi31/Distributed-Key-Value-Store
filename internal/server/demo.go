package server

import (
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

//go:embed ui/index.html
var uiHTML []byte

const (
	defaultIsolation = 15 * time.Second
	minIsolation     = 3 * time.Second
	maxIsolation     = 30 * time.Second
)

// Isolator cuts this node off from its Raft peers for a while and then heals
// it automatically (see transport.Partition).
type Isolator interface {
	IsolateFor(d time.Duration) bool
	Remaining() time.Duration
}

// DemoConfig turns on the live dashboard at /ui. Token gates the one
// state-changing demo endpoint (/demo/isolate); an empty Token leaves the
// dashboard read-only.
type DemoConfig struct {
	NodeID   raft.NodeID
	Token    string
	Isolator Isolator
}

// EnableDemo registers the dashboard routes. Call once, right after New.
func (s *Server) EnableDemo(cfg DemoConfig) {
	s.demo = &cfg
	s.mux.HandleFunc("/ui", s.handleUI)
	s.mux.HandleFunc("/cluster/members", s.handleMembers)
	s.mux.HandleFunc("/demo/isolate", s.handleIsolate)
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/ui", http.StatusFound)
	})
}

func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write(uiHTML)
}

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		Self         raft.NodeID            `json:"self"`
		Members      map[raft.NodeID]string `json:"members"`
		DemoControls bool                   `json:"demo_controls"`
	}{s.demo.NodeID, s.clientURLs, s.demo.Token != ""})
}

func (s *Server) handleIsolate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.demo.Token == "" || s.demo.Isolator == nil {
		http.Error(w, "demo controls are disabled on this deployment", http.StatusForbidden)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Demo-Token")), []byte(s.demo.Token)) != 1 {
		http.Error(w, "invalid demo token", http.StatusUnauthorized)
		return
	}

	d := defaultIsolation
	if v := r.URL.Query().Get("seconds"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "seconds must be an integer", http.StatusBadRequest)
			return
		}
		d = time.Duration(secs) * time.Second
	}
	d = min(max(d, minIsolation), maxIsolation)

	if !s.demo.Isolator.IsolateFor(d) {
		http.Error(w, "node is already isolated", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(struct {
		IsolatedMS int64 `json:"isolated_ms"`
	}{d.Milliseconds()})
}

// withCORS lets the dashboard, served from one node, poll every other node
// directly (each node is a different origin: different port or host). The
// client API was already public and unauthenticated, so this exposes nothing
// new; the only privileged endpoint still requires X-Demo-Token.
func withCORS(w http.ResponseWriter, r *http.Request) (handled bool) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, X-Demo-Token")
	h.Set("Access-Control-Expose-Headers", "X-Leader-URL")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}
