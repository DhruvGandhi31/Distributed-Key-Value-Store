package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"testing"
	"time"
)

// recordingStateMachine records every entry Apply is called with, so tests
// can assert not just that commitIndex advanced but that the entry was
// actually applied to the state machine, in order.
type recordingStateMachine struct {
	mu      sync.Mutex
	applied []LogEntry
}

func (sm *recordingStateMachine) Apply(entry LogEntry) interface{} {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.applied = append(sm.applied, entry)
	return entry.Data
}

// Snapshot/Restore round-trip the applied log so far through gob, so
// snapshot-related tests can exercise real data instead of a no-op.
func (sm *recordingStateMachine) Snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sm.applied); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (sm *recordingStateMachine) Restore(data []byte) error {
	var applied []LogEntry
	if len(data) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&applied); err != nil {
			return err
		}
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.applied = applied
	return nil
}

func (sm *recordingStateMachine) appliedCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.applied)
}

func newReplicationTestCluster(t *testing.T, ids []NodeID) (map[NodeID]*Node, map[NodeID]*recordingStateMachine, *fakeTransport) {
	t.Helper()
	transport := newFakeTransport()
	nodes := make(map[NodeID]*Node)
	sms := make(map[NodeID]*recordingStateMachine)

	for _, id := range ids {
		var peers []NodeID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		sm := &recordingStateMachine{}
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			Storage:            newMemStorage(),
			Transport:          transport,
			SM:                 sm,
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
		sms[id] = sm
		transport.register(id, n)
	}
	return nodes, sms, transport
}

// awaitLeader polls every node's Status until exactly one reports Leader,
// returning its id and *Node.
func awaitLeader(t *testing.T, nodes map[NodeID]*Node) (NodeID, *Node) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for id, n := range nodes {
			status, err := n.Status(context.Background())
			if err == nil && status.State == Leader {
				return id, n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return "", nil
}

func TestReplication_ProposeCommitsAndAppliesOnFollowers(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
	nodes, sms, _ := newReplicationTestCluster(t, ids)
	defer stopAll(nodes)
	for _, n := range nodes {
		go n.Run()
	}

	_, leader := awaitLeader(t, nodes)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	value, err := leader.Propose(ctx, []byte("hello"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if got, ok := value.([]byte); !ok || string(got) != "hello" {
		t.Fatalf("Propose returned %+v, want []byte(\"hello\")", value)
	}

	deadline := time.Now().Add(2 * time.Second)
	for _, sm := range sms {
		for time.Now().Before(deadline) && sm.appliedCount() < 1 {
			time.Sleep(10 * time.Millisecond)
		}
		if sm.appliedCount() != 1 {
			t.Fatalf("state machine only applied %d entries, want 1", sm.appliedCount())
		}
	}
}

func TestReplication_ProposeFailsWhenNotLeader(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
	nodes, _, _ := newReplicationTestCluster(t, ids)
	defer stopAll(nodes)
	for _, n := range nodes {
		go n.Run()
	}

	leaderID, _ := awaitLeader(t, nodes)

	var follower *Node
	for id, n := range nodes {
		if id != leaderID {
			follower = n
			break
		}
	}

	_, err := follower.Propose(context.Background(), []byte("x"))
	if err != ErrNotLeader {
		t.Fatalf("Propose on a follower: got err=%v, want ErrNotLeader", err)
	}
}

func TestReplication_ConsistencyCheckRejectsOnIndexOrTermMismatch(t *testing.T) {
	nodes, _, _ := newReplicationTestCluster(t, []NodeID{"n1", "n2"})
	n := nodes["n1"]
	n.currentTerm = 3

	if err := n.storage.AppendEntries([]LogEntry{{Index: 1, Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	// PrevLogIndex beyond what we have.
	reply := n.handleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "n2", PrevLogIndex: 5, PrevLogTerm: 1})
	if reply.Success {
		t.Fatalf("expected rejection for PrevLogIndex beyond log, got %+v", reply)
	}

	// PrevLogIndex present but with a different term.
	reply = n.handleAppendEntries(AppendEntriesArgs{Term: 3, LeaderID: "n2", PrevLogIndex: 1, PrevLogTerm: 99})
	if reply.Success {
		t.Fatalf("expected rejection for mismatched PrevLogTerm, got %+v", reply)
	}
	if got := n.storage.LastIndex(); got != 1 {
		t.Fatalf("log should be untouched by a rejected consistency check, LastIndex = %d, want 1", got)
	}
}

func TestReplication_TruncatesDivergentEntriesOnConflict(t *testing.T) {
	nodes, _, _ := newReplicationTestCluster(t, []NodeID{"n1", "n2"})
	n := nodes["n1"]
	n.currentTerm = 1

	// This follower has two entries from an old leader's term 1.
	if err := n.storage.AppendEntries([]LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b-stale")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	// A new leader at term 2 has a different entry at index 2.
	reply := n.handleAppendEntries(AppendEntriesArgs{
		Term:         2,
		LeaderID:     "n2",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Index: 2, Term: 2, Data: []byte("b-new")}},
	})
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}

	got, err := n.storage.EntryAt(2)
	if err != nil {
		t.Fatalf("EntryAt(2): %v", err)
	}
	if got.Term != 2 || string(got.Data) != "b-new" {
		t.Fatalf("entry at index 2 = %+v, want term=2 data=b-new (leader's entry should replace the diverging one)", got)
	}
	if n.storage.LastIndex() != 2 {
		t.Fatalf("LastIndex = %d, want 2", n.storage.LastIndex())
	}
}

func TestReplication_FollowerAdvancesCommitFromLeaderCommit(t *testing.T) {
	nodes, sms, _ := newReplicationTestCluster(t, []NodeID{"n1", "n2"})
	n := nodes["n1"]
	sm := sms["n1"]
	n.currentTerm = 1

	reply := n.handleAppendEntries(AppendEntriesArgs{
		Term:         1,
		LeaderID:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []LogEntry{
			{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("a")},
			{Index: 2, Term: 1, Type: EntryNormal, Data: []byte("b")},
		},
		LeaderCommit: 1, // leader has only committed index 1 so far
	})
	if !reply.Success {
		t.Fatalf("expected success, got %+v", reply)
	}
	if n.commitIndex != 1 {
		t.Fatalf("commitIndex = %d, want 1 (clamped to LeaderCommit)", n.commitIndex)
	}
	if sm.appliedCount() != 1 {
		t.Fatalf("applied %d entries, want exactly 1 (index 2 not yet committed)", sm.appliedCount())
	}
}

func TestReplication_LeaderOnlyCommitsCurrentTermEntriesDirectly(t *testing.T) {
	nodes, sms, _ := newReplicationTestCluster(t, []NodeID{"n1", "n2", "n3"})
	n := nodes["n1"]
	sm := sms["n1"]

	// n1 is leader of term 2, with one entry inherited from a prior term
	// (term 1) already in its log — simulating an entry a previous leader
	// appended but never got to commit before losing leadership.
	if err := n.storage.AppendEntries([]LogEntry{{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("old")}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	n.currentTerm = 2
	n.state = Leader
	n.nextIndex = map[NodeID]uint64{"n2": 2, "n3": 2}
	n.matchIndex = map[NodeID]uint64{"n2": 1, "n3": 1}

	n.advanceCommitIndex()
	if n.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0: a majority-replicated PRIOR-term entry must not be committed directly", n.commitIndex)
	}

	// Now the leader appends its own current-term entry, and a majority
	// replicates through it.
	if err := n.storage.AppendEntries([]LogEntry{{Index: 2, Term: 2, Type: EntryNormal, Data: []byte("new")}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	n.matchIndex["n2"] = 2
	n.matchIndex["n3"] = 2

	n.advanceCommitIndex()
	if n.commitIndex != 2 {
		t.Fatalf("commitIndex = %d, want 2 once a current-term entry is majority-replicated", n.commitIndex)
	}
	if got := sm.appliedCount(); got != 2 {
		t.Fatalf("applied %d entries, want 2 (index 1 commits indirectly alongside index 2)", got)
	}
}

func TestReplication_StepDownFailsPendingProposals(t *testing.T) {
	nodes, _, _ := newReplicationTestCluster(t, []NodeID{"n1", "n2", "n3"})
	n := nodes["n1"]
	n.state = Leader
	n.currentTerm = 1
	n.nextIndex = map[NodeID]uint64{"n2": 1, "n3": 1}
	n.matchIndex = map[NodeID]uint64{"n2": 0, "n3": 0}

	resultCh := make(chan proposalResult, 1)
	n.pendingProposals[1] = resultCh

	n.stepDown(2)

	select {
	case res := <-resultCh:
		if res.err != ErrNotLeader {
			t.Fatalf("pending proposal resolved with err=%v, want ErrNotLeader", res.err)
		}
	default:
		t.Fatal("stepDown left a pending proposal unresolved")
	}
	if len(n.pendingProposals) != 0 {
		t.Fatalf("pendingProposals not cleared after stepDown, len=%d", len(n.pendingProposals))
	}
}
