package raft

import (
	"testing"
	"time"
)

// TestRecovery_NoOpOnElectionReestablishesCommitIndex simulates a full
// cluster restart: every node already has the same committed entry
// persisted on disk from before the "crash" (matching what FileStorage
// would have reloaded via NewFileStorage), but commitIndex/lastApplied are
// volatile and start at 0 everywhere, as they correctly do after a real
// restart. Without becomeLeader's no-op-on-election fix, the newly-elected
// leader would never be able to re-advance commitIndex past 0 (the old
// entry is from a prior term, and Figure 8 forbids committing a prior-term
// entry by direct majority count) — the data would sit safely on disk
// forever but never get replayed into the state machine. This test would
// fail without that fix.
func TestRecovery_NoOpOnElectionReestablishesCommitIndex(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
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

		storage := newMemStorage()
		// Every node already has this entry durably on disk from "before
		// the restart" — it was committed and applied then, but that's
		// exactly the volatile state a real restart loses.
		if err := storage.AppendEntries([]LogEntry{
			{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("pre-restart")},
		}); err != nil {
			t.Fatalf("AppendEntries: %v", err)
		}

		sm := &recordingStateMachine{}
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			Storage:            storage,
			Transport:          transport,
			SM:                 sm,
			InitialTerm:        1, // as if reloaded from FileStorage.InitialHardState
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
	defer stopAll(nodes)
	for _, n := range nodes {
		go n.Run()
	}

	// No client ever calls Propose in this test — the only thing that can
	// re-establish commitIndex is becomeLeader's no-op entry.
	awaitLeader(t, nodes)

	deadline := time.Now().Add(2 * time.Second)
	for id, sm := range sms {
		for time.Now().Before(deadline) && sm.appliedCount() < 1 {
			time.Sleep(10 * time.Millisecond)
		}
		if sm.appliedCount() != 1 {
			t.Fatalf("node %s never re-applied the pre-restart entry (applied=%d) — "+
				"commitIndex was never re-established after election", id, sm.appliedCount())
		}
		if string(sm.applied[0].Data) != "pre-restart" {
			t.Fatalf("node %s applied wrong entry: %+v", id, sm.applied[0])
		}
	}
}
