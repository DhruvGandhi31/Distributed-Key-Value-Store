package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistry_ExposesExpectedMetrics(t *testing.T) {
	r := New()
	r.SetCurrentTerm(5)
	r.SetCommitIndex(42)
	r.IncLeaderChanges()
	r.ObserveAppendEntriesDuration(0.01)
	r.IncOp("put")
	r.ObserveOpDuration("put", 0.002)

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, want := range []string{
		"raft_current_term 5",
		"raft_commit_index 42",
		"raft_leader_changes_total 1",
		`kv_ops_total{op="put"} 1`,
		`kv_op_duration_seconds_count{op="put"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q\nfull output:\n%s", want, body)
		}
	}
}

// Two Registries must coexist in one process without a "duplicate metrics
// collector registration" panic — exactly the scenario tests/integration
// hits by running several in-process raft.Node instances in one test
// binary. This is the reason Registry uses a private registry instead of
// prometheus.DefaultRegisterer; this test pins that choice down.
func TestRegistry_IsolatedPerInstance(t *testing.T) {
	r1 := New()
	r2 := New()
	r1.SetCurrentTerm(1)
	r2.SetCurrentTerm(2)

	rec1 := httptest.NewRecorder()
	r1.Handler().ServeHTTP(rec1, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec1.Body.String(), "raft_current_term 1") {
		t.Error("r1's /metrics missing its own term value")
	}

	rec2 := httptest.NewRecorder()
	r2.Handler().ServeHTTP(rec2, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(rec2.Body.String(), "raft_current_term 2") {
		t.Error("r2's /metrics missing its own term value")
	}
}
