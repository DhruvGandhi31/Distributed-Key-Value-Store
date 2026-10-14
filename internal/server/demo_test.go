package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
)

type fakeIsolator struct {
	until time.Time
	last  time.Duration
}

func (f *fakeIsolator) IsolateFor(d time.Duration) bool {
	if time.Now().Before(f.until) {
		return false
	}
	f.last = d
	f.until = time.Now().Add(d)
	return true
}

func (f *fakeIsolator) Remaining() time.Duration {
	if left := time.Until(f.until); left > 0 {
		return left
	}
	return 0
}

func newDemoServer(token string, iso Isolator) *Server {
	node := &fakeRaftNode{status: raft.Status{State: raft.Leader, Leader: "n1", Term: 3}}
	s := New(store.New(), node, map[raft.NodeID]string{"n1": "http://a:1", "n2": "http://b:2"}, nil)
	s.EnableDemo(DemoConfig{NodeID: "n1", Token: token, Isolator: iso})
	return s
}

func isolate(s *Server, token, query string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/demo/isolate"+query, nil)
	if token != "" {
		req.Header.Set("X-Demo-Token", token)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestDemo_UIIsServed(t *testing.T) {
	s := newDemoServer("", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ui", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<title>") {
		t.Fatalf("GET /ui = %d, want 200 with the embedded page", rec.Code)
	}

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusFound || rec.Header().Get("Location") != "/ui" {
		t.Fatalf("GET / = %d -> %q, want 302 to /ui", rec.Code, rec.Header().Get("Location"))
	}
}

func TestDemo_MembersListsClusterAndSelf(t *testing.T) {
	s := newDemoServer("secret", &fakeIsolator{})
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cluster/members", nil))

	var got struct {
		Self         string            `json:"self"`
		Members      map[string]string `json:"members"`
		DemoControls bool              `json:"demo_controls"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Self != "n1" || len(got.Members) != 2 || !got.DemoControls {
		t.Fatalf("unexpected members response: %+v", got)
	}
}

func TestDemo_IsolateRequiresToken(t *testing.T) {
	iso := &fakeIsolator{}
	s := newDemoServer("secret", iso)

	if rec := isolate(s, "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: status = %d, want 401", rec.Code)
	}
	if rec := isolate(s, "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status = %d, want 401", rec.Code)
	}
	if iso.Remaining() != 0 {
		t.Fatal("node was isolated without a valid token")
	}
}

func TestDemo_IsolateDisabledWithoutConfiguredToken(t *testing.T) {
	s := newDemoServer("", &fakeIsolator{})
	if rec := isolate(s, "anything", ""); rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 when no demo token is configured", rec.Code)
	}
}

func TestDemo_IsolateClampsDurationAndRejectsOverlap(t *testing.T) {
	iso := &fakeIsolator{}
	s := newDemoServer("secret", iso)

	if rec := isolate(s, "secret", "?seconds=3600"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if iso.last != maxIsolation {
		t.Fatalf("duration = %v, want clamped to %v", iso.last, maxIsolation)
	}
	if rec := isolate(s, "secret", ""); rec.Code != http.StatusConflict {
		t.Fatalf("second isolate: status = %d, want 409", rec.Code)
	}

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/cluster/status", nil))
	var st struct {
		ID         string `json:"id"`
		IsolatedMS int64  `json:"isolated_ms"`
	}
	json.NewDecoder(rec.Body).Decode(&st)
	if st.ID != "n1" || st.IsolatedMS <= 0 {
		t.Fatalf("status should report id and remaining isolation, got %+v", st)
	}
}

func TestCORS_PreflightIsAnswered(t *testing.T) {
	s := newDemoServer("", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodOptions, "/kv/x", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204", rec.Code)
	}
	if rec.Header().Get("Access-Control-Expose-Headers") != "X-Leader-URL" {
		t.Fatal("X-Leader-URL must be exposed so the dashboard can follow redirects")
	}
}
