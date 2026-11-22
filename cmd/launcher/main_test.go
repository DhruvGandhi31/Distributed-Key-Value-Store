package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeVM struct {
	status string
	starts int
}

func (f *fakeVM) Status(context.Context) (string, error) { return f.status, nil }
func (f *fakeVM) Start(context.Context) error {
	f.starts++
	f.status = "STAGING"
	return nil
}

// newLauncher points the launcher at a fake dashboard node that reports
// the given leader ("" = no leader elected yet).
func newLauncher(t *testing.T, vm *fakeVM, leader string) *launcher {
	t.Helper()
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"leader": leader})
	}))
	t.Cleanup(node.Close)
	return &launcher{vm: vm, demoURL: node.URL, probe: node.Client()}
}

func get(l *launcher, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	l.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestIndex_StoppedVMIsStartedAndWakingPageShown(t *testing.T) {
	vm := &fakeVM{status: "TERMINATED"}
	rec := get(newLauncher(t, vm, ""), "/")

	if vm.starts != 1 {
		t.Fatalf("Start called %d times, want 1", vm.starts)
	}
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Waking the cluster up") {
		t.Fatalf("got %d, want 200 with the waking page", rec.Code)
	}
}

func TestIndex_ReadyClusterRedirectsToDashboard(t *testing.T) {
	vm := &fakeVM{status: "RUNNING"}
	l := newLauncher(t, vm, "kvstore-0")
	rec := get(l, "/")

	if rec.Code != http.StatusFound || rec.Header().Get("Location") != l.demoURL+"/ui" {
		t.Fatalf("got %d -> %q, want 302 to the dashboard", rec.Code, rec.Header().Get("Location"))
	}
	if vm.starts != 0 {
		t.Fatal("a running VM must not be started again")
	}
}

// The VM can be RUNNING before Raft has a leader; keep the visitor waiting.
func TestStatus_RunningWithoutLeaderIsNotReady(t *testing.T) {
	rec := get(newLauncher(t, &fakeVM{status: "RUNNING"}, ""), "/status")

	var st state
	if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.VM != "RUNNING" || st.Ready {
		t.Fatalf("status = %+v, want RUNNING and not ready", st)
	}
}

// A VM that is still shutting down can't be started yet; a later poll retries.
func TestStatus_StoppingVMIsLeftAlone(t *testing.T) {
	vm := &fakeVM{status: "STOPPING"}
	get(newLauncher(t, vm, ""), "/status")
	if vm.starts != 0 {
		t.Fatalf("Start called %d times on a STOPPING VM, want 0", vm.starts)
	}
}
