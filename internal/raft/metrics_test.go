package raft

import "testing"

// fakeMetrics records every call, so tests can assert the actual call
// sites fire (not just that a nil Metrics doesn't crash — noopMetrics
// already covers that implicitly, every other test in this package relies
// on the nil-default).
type fakeMetrics struct {
	terms           []uint64
	commitIndexes   []uint64
	leaderChanges   int
	appendDurations []float64
}

func (f *fakeMetrics) SetCurrentTerm(term uint64)  { f.terms = append(f.terms, term) }
func (f *fakeMetrics) SetCommitIndex(index uint64) { f.commitIndexes = append(f.commitIndexes, index) }
func (f *fakeMetrics) IncLeaderChanges()           { f.leaderChanges++ }
func (f *fakeMetrics) ObserveAppendEntriesDuration(seconds float64) {
	f.appendDurations = append(f.appendDurations, seconds)
}

func newSingleNodeForMetrics(t *testing.T, fm *fakeMetrics) *Node {
	t.Helper()
	cfg := Config{
		ID:                 "n1",
		Storage:            newMemStorage(),
		Transport:          newFakeTransport(),
		SM:                 noopStateMachine{},
		Metrics:            fm,
		TickInterval:       testTickInterval,
		ElectionTimeoutMin: testElectionTimeoutMin,
		ElectionTimeoutMax: testElectionTimeoutMax,
		HeartbeatInterval:  testHeartbeatInterval,
		RPCTimeout:         testRPCTimeout,
	}
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// A single-node cluster wins its own election synchronously inside
// startElection (quorum of 1, no RPCs needed) — the simplest deterministic
// way to exercise the term-bump and leader-change metric call sites
// without timing-dependent goroutine coordination.
func TestMetrics_BecomeLeaderRecordsTermAndLeaderChange(t *testing.T) {
	fm := &fakeMetrics{}
	n := newSingleNodeForMetrics(t, fm)

	n.startElection()

	if n.state != Leader {
		t.Fatalf("expected node to become leader, got state=%v", n.state)
	}
	if len(fm.terms) == 0 || fm.terms[len(fm.terms)-1] != 1 {
		t.Fatalf("SetCurrentTerm calls = %v, want last value 1", fm.terms)
	}
	if fm.leaderChanges != 1 {
		t.Fatalf("IncLeaderChanges called %d times, want 1", fm.leaderChanges)
	}
}

// becomeLeader's no-op entry commits immediately in a single-node cluster
// (advanceCommitIndex's quorum-of-1 case), so this also covers the
// SetCommitIndex call site without needing a multi-node setup.
func TestMetrics_CommitIndexAdvanceIsRecorded(t *testing.T) {
	fm := &fakeMetrics{}
	n := newSingleNodeForMetrics(t, fm)

	n.startElection()

	if len(fm.commitIndexes) == 0 {
		t.Fatal("expected at least one SetCommitIndex call")
	}
	if got, want := fm.commitIndexes[len(fm.commitIndexes)-1], n.commitIndex; got != want {
		t.Fatalf("last SetCommitIndex value = %d, want %d (n.commitIndex)", got, want)
	}
}

// stepDown is the other place currentTerm changes (besides startElection),
// reached here via a RequestVote carrying a higher term.
func TestMetrics_StepDownRecordsNewTerm(t *testing.T) {
	fm := &fakeMetrics{}
	n := newSingleNodeForMetrics(t, fm)

	n.handleRequestVote(RequestVoteArgs{Term: 7, CandidateID: "other"})

	if len(fm.terms) == 0 || fm.terms[len(fm.terms)-1] != 7 {
		t.Fatalf("SetCurrentTerm calls = %v, want last value 7", fm.terms)
	}
}
