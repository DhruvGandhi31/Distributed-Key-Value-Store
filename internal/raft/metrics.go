package raft

// Metrics is the metrics-reporting interface Node depends on, injected the
// same way Logger is (see logger.go) so Raft internals never need to know
// anything about Prometheus or any other concrete backend.
type Metrics interface {
	SetCurrentTerm(term uint64)
	SetCommitIndex(index uint64)
	IncLeaderChanges()
	ObserveAppendEntriesDuration(seconds float64)
}

// noopMetrics is the default used when Config.Metrics is nil, so call sites
// never need a nil check.
type noopMetrics struct{}

func (noopMetrics) SetCurrentTerm(uint64)                {}
func (noopMetrics) SetCommitIndex(uint64)                {}
func (noopMetrics) IncLeaderChanges()                    {}
func (noopMetrics) ObserveAppendEntriesDuration(float64) {}
