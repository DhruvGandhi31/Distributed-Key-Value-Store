package raft

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

// noopStateMachine satisfies StateMachine without pulling in internal/store,
// which would create an import cycle (store already imports raft).
type noopStateMachine struct{}

func (noopStateMachine) Apply(entry LogEntry) interface{} { return nil }
func (noopStateMachine) Snapshot() ([]byte, error)        { return nil, nil }
func (noopStateMachine) Restore(data []byte) error        { return nil }

// memStorage is an in-memory Storage for tests — no disk I/O needed to
// exercise election/replication/snapshot logic. entries[i] is the log
// entry at index firstIndex+i, mirroring FileStorage's compaction-aware
// indexing (firstIndex starts at 1 and advances to lastIncludedIndex+1
// after every SaveSnapshot).
type memStorage struct {
	mu            sync.Mutex
	term          uint64
	votedFor      NodeID
	snapshotIndex uint64
	snapshotTerm  uint64
	snapshotData  []byte
	firstIndex    uint64
	entries       []LogEntry
}

func newMemStorage() *memStorage { return &memStorage{firstIndex: 1} }

func (s *memStorage) SaveHardState(term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term = term
	s.votedFor = votedFor
	return nil
}

func (s *memStorage) AppendEntries(newEntries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, newEntries...)
	return nil
}

func (s *memStorage) lastIndexLocked() uint64 {
	if len(s.entries) == 0 {
		return s.snapshotIndex
	}
	return s.firstIndex + uint64(len(s.entries)) - 1
}

func (s *memStorage) Entries(lo, hi uint64) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.firstIndex
	last := s.lastIndexLocked()
	if lo < first {
		lo = first
	}
	if hi > last+1 {
		hi = last + 1
	}
	if lo >= hi || len(s.entries) == 0 {
		return nil, nil
	}
	out := make([]LogEntry, hi-lo)
	copy(out, s.entries[lo-first:hi-first])
	return out, nil
}

func (s *memStorage) EntryAt(index uint64) (LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.firstIndex
	last := s.lastIndexLocked()
	if index < first || index > last {
		return LogEntry{}, os.ErrNotExist
	}
	return s.entries[index-first], nil
}

func (s *memStorage) TermAt(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	s.mu.Lock()
	if index == s.snapshotIndex {
		term := s.snapshotTerm
		s.mu.Unlock()
		return term, nil
	}
	s.mu.Unlock()
	e, err := s.EntryAt(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (s *memStorage) FirstIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstIndex
}

func (s *memStorage) LastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastIndexLocked()
}

func (s *memStorage) LastTerm() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.entries) == 0 {
		return s.snapshotTerm
	}
	return s.entries[len(s.entries)-1].Term
}

func (s *memStorage) TruncateAfter(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := s.firstIndex
	last := s.lastIndexLocked()
	if index >= last {
		return nil
	}
	if index < first-1 {
		s.entries = nil
		return nil
	}
	s.entries = s.entries[:index-first+1]
	return nil
}

func (s *memStorage) SaveSnapshot(data []byte, lastIndex, lastTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lastIndex <= s.snapshotIndex {
		return nil
	}
	if lastIndex < s.lastIndexLocked() {
		s.entries = s.entries[lastIndex-s.firstIndex+1:]
	} else {
		s.entries = nil
	}
	s.firstIndex = lastIndex + 1
	s.snapshotIndex = lastIndex
	s.snapshotTerm = lastTerm
	s.snapshotData = data
	return nil
}

func (s *memStorage) LoadSnapshot() ([]byte, uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotData, s.snapshotIndex, s.snapshotTerm, nil
}

// fakeTransport routes RPCs directly to other in-process *Node instances
// registered under the same NodeID, bypassing the network entirely.
type fakeTransport struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Node
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{nodes: make(map[NodeID]*Node)}
}

func (t *fakeTransport) register(id NodeID, n *Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = n
}

func (t *fakeTransport) peer(id NodeID) *Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nodes[id]
}

func (t *fakeTransport) SendRequestVote(ctx context.Context, peer NodeID, args RequestVoteArgs) (RequestVoteReply, error) {
	n := t.peer(peer)
	if n == nil {
		return RequestVoteReply{}, os.ErrNotExist
	}
	return n.HandleRequestVote(ctx, args)
}

func (t *fakeTransport) SendAppendEntries(ctx context.Context, peer NodeID, args AppendEntriesArgs) (AppendEntriesReply, error) {
	n := t.peer(peer)
	if n == nil {
		return AppendEntriesReply{}, os.ErrNotExist
	}
	return n.HandleAppendEntries(ctx, args)
}

func (t *fakeTransport) SendInstallSnapshot(ctx context.Context, peer NodeID, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	n := t.peer(peer)
	if n == nil {
		return InstallSnapshotReply{}, os.ErrNotExist
	}
	return n.HandleInstallSnapshot(ctx, args)
}

const (
	testTickInterval       = 5 * time.Millisecond
	testElectionTimeoutMin = 30 * time.Millisecond
	testElectionTimeoutMax = 60 * time.Millisecond
	testHeartbeatInterval  = 5 * time.Millisecond
	testRPCTimeout         = 50 * time.Millisecond
)

func newTestCluster(t *testing.T, ids []NodeID) (map[NodeID]*Node, *fakeTransport) {
	t.Helper()
	transport := newFakeTransport()
	nodes := make(map[NodeID]*Node)

	for _, id := range ids {
		var peers []NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			Storage:            newMemStorage(),
			Transport:          transport,
			SM:                 noopStateMachine{},
			TickInterval:       testTickInterval,
			ElectionTimeoutMin: testElectionTimeoutMin,
			ElectionTimeoutMax: testElectionTimeoutMax,
			HeartbeatInterval:  testHeartbeatInterval,
			RPCTimeout:         testRPCTimeout,
		}
		n, err := NewNode(cfg)
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		nodes[id] = n
		transport.register(id, n)
	}
	return nodes, transport
}

func stopAll(nodes map[NodeID]*Node) {
	for _, n := range nodes {
		n.Stop()
	}
}

func TestElection_SingleCandidateWins(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
	nodes, _ := newTestCluster(t, ids)
	defer stopAll(nodes)

	for _, n := range nodes {
		go n.Run()
	}

	deadline := time.Now().Add(2 * time.Second)
	var leaderID NodeID
	for time.Now().Before(deadline) {
		leaders := 0
		followers := 0
		for id, n := range nodes {
			status, err := n.Status(context.Background())
			if err != nil {
				continue
			}
			if status.State == Leader {
				leaders++
				leaderID = id
			} else if status.State == Follower {
				followers++
			}
		}
		if leaders == 1 && followers == len(ids)-1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cluster did not converge on a single leader within timeout (last observed leader=%q)", leaderID)
}

func TestElection_HigherTermStepsDownLeader(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
	nodes, _ := newTestCluster(t, ids)
	defer stopAll(nodes)

	for _, n := range nodes {
		go n.Run()
	}

	var leader *Node
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			status, err := n.Status(context.Background())
			if err == nil && status.State == Leader {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}

	priorStatus, _ := leader.Status(context.Background())
	higherTerm := priorStatus.Term + 10

	// The outsider must claim a log at least as up-to-date as the leader's
	// own (which now has at least the no-op entry becomeLeader appends) —
	// otherwise Raft's election restriction correctly withholds the vote
	// despite the higher term. Read the leader's actual log bounds directly
	// (this test file is in package raft) rather than hardcoding zeros.
	reply, err := leader.HandleRequestVote(context.Background(), RequestVoteArgs{
		Term:         higherTerm,
		CandidateID:  "outsider",
		LastLogIndex: leader.storage.LastIndex(),
		LastLogTerm:  leader.storage.LastTerm(),
	})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted to outsider with higher term, got %+v", reply)
	}

	status, err := leader.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != Follower {
		t.Fatalf("expected leader to step down to Follower, got %s", status.State)
	}
	if status.Term != higherTerm {
		t.Fatalf("expected term %d, got %d", higherTerm, status.Term)
	}
}

func TestElection_NoTimerResetOnRejectedAppendEntries(t *testing.T) {
	nodes, _ := newTestCluster(t, []NodeID{"n1", "n2"})
	n := nodes["n1"]

	n.currentTerm = 5
	n.electionElapsed = 3

	reply := n.handleAppendEntries(AppendEntriesArgs{Term: 4, LeaderID: "n2"})
	if reply.Success {
		t.Fatalf("expected rejection for stale term, got %+v", reply)
	}
	if n.electionElapsed != 3 {
		t.Fatalf("expected electionElapsed unchanged at 3, got %d", n.electionElapsed)
	}
}

func TestElection_NoTimerResetOnVoteGrant(t *testing.T) {
	nodes, _ := newTestCluster(t, []NodeID{"n1", "n2"})
	n := nodes["n1"]

	n.resetElectionTimer()
	n.electionElapsed = 1
	before := n.electionElapsed

	reply := n.handleRequestVote(RequestVoteArgs{Term: n.currentTerm, CandidateID: "n2"})
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got %+v", reply)
	}
	if n.electionElapsed != before {
		t.Fatalf("expected electionElapsed unchanged at %d, got %d", before, n.electionElapsed)
	}
}

func TestRequestVote_RejectsStaleTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, []NodeID{"n1", "n2"})
	n := nodes["n1"]
	n.currentTerm = 5

	reply := n.handleRequestVote(RequestVoteArgs{Term: 3, CandidateID: "n2"})
	if reply.VoteGranted {
		t.Fatalf("expected vote rejected for stale term, got %+v", reply)
	}
	if reply.Term != 5 {
		t.Fatalf("expected reply term 5, got %d", reply.Term)
	}
}

func TestRequestVote_NoDoubleVoteSameTerm(t *testing.T) {
	nodes, _ := newTestCluster(t, []NodeID{"n1", "n2", "n3"})
	n := nodes["n1"]

	reply1 := n.handleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "n2"})
	if !reply1.VoteGranted {
		t.Fatalf("expected first vote granted, got %+v", reply1)
	}

	reply2 := n.handleRequestVote(RequestVoteArgs{Term: 1, CandidateID: "n3"})
	if reply2.VoteGranted {
		t.Fatalf("expected second vote in same term rejected, got %+v", reply2)
	}
}

func TestElection_StaleVoteReplyIgnored(t *testing.T) {
	nodes, _ := newTestCluster(t, []NodeID{"n1", "n2", "n3"})
	n := nodes["n1"]

	n.startElection()
	staleTerm := n.currentTerm

	n.stepDown(staleTerm + 5)

	n.handleVoteResult(voteResult{
		term: staleTerm,
		peer: "n2",
		reply: RequestVoteReply{
			Term:        staleTerm,
			VoteGranted: true,
		},
	})

	if n.state == Leader {
		t.Fatalf("expected stale vote reply to be ignored, but node became Leader")
	}
}
