// Package metrics wires up every Prometheus metric kvstored exposes at
// /metrics. One Registry satisfies both raft.Metrics (term/commitIndex/
// leader-change/replication-latency) and server.Metrics (per-operation
// counts/latency) by structural typing — a single object, constructed once
// in cmd/server/main.go and injected into both layers.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds every metric and its own private Prometheus registry —
// deliberately not the global DefaultRegisterer, so multiple Registries can
// coexist in one process without a "duplicate metrics collector
// registration" panic. That matters for tests/integration, which runs
// several in-process raft.Node instances (each with its own Registry) in a
// single test binary; in production each node is its own OS process, where
// this distinction wouldn't matter, but the code has to work both ways.
type Registry struct {
	reg *prometheus.Registry

	raftCurrentTerm           prometheus.Gauge
	raftCommitIndex           prometheus.Gauge
	raftLeaderChangesTotal    prometheus.Counter
	raftAppendEntriesDuration prometheus.Histogram
	kvOpsTotal                *prometheus.CounterVec
	kvOpDuration              *prometheus.HistogramVec
}

// New creates a Registry with every kvstored metric registered, plus the
// standard Go runtime/process collectors (goroutine count, GC pauses,
// memory, CPU) — useful on their own for diagnosing the slow-follower and
// throughput-regression scenarios this phase exists for.
func New() *Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	factory := promauto.With(reg)
	return &Registry{
		reg: reg,

		raftCurrentTerm: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_current_term",
			Help: "This node's current Raft term.",
		}),
		raftCommitIndex: factory.NewGauge(prometheus.GaugeOpts{
			Name: "raft_commit_index",
			Help: "This node's current Raft commit index.",
		}),
		raftLeaderChangesTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "raft_leader_changes_total",
			Help: "Number of times this node has become the Raft leader.",
		}),
		raftAppendEntriesDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "raft_append_entries_duration_seconds",
			Help:    "Latency of outbound AppendEntries RPCs this node (as leader) sends to peers.",
			Buckets: prometheus.DefBuckets,
		}),
		kvOpsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kv_ops_total",
			Help: "Total number of KV operations handled, by type.",
		}, []string{"op"}),
		kvOpDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kv_op_duration_seconds",
			Help:    "Latency of KV operations, by type, from request received to response written.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
	}
}

// Handler returns the HTTP handler to serve at /metrics, scoped to this
// Registry's own private registry (not the global default).
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
}

// raft.Metrics implementation — see internal/raft/metrics.go.
func (r *Registry) SetCurrentTerm(term uint64)  { r.raftCurrentTerm.Set(float64(term)) }
func (r *Registry) SetCommitIndex(index uint64) { r.raftCommitIndex.Set(float64(index)) }
func (r *Registry) IncLeaderChanges()           { r.raftLeaderChangesTotal.Inc() }
func (r *Registry) ObserveAppendEntriesDuration(seconds float64) {
	r.raftAppendEntriesDuration.Observe(seconds)
}

// server.Metrics implementation — see internal/server/server.go.
func (r *Registry) IncOp(op string) { r.kvOpsTotal.WithLabelValues(op).Inc() }
func (r *Registry) ObserveOpDuration(op string, seconds float64) {
	r.kvOpDuration.WithLabelValues(op).Observe(seconds)
}
