package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSnapshot_IntervalTriggersCompaction(t *testing.T) {
	ids := []NodeID{"n1", "n2", "n3"}
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
			SM:                 &recordingStateMachine{},
			SnapshotInterval:   3,
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
	defer stopAll(nodes)
	for _, n := range nodes {
		go n.Run()
	}

	_, leader := awaitLeader(t, nodes)

	// becomeLeader's own no-op entry is index 1; propose enough more to
	// cross the SnapshotInterval=3 boundary at least twice.
	for i := 0; i < 8; i++ {
		if _, err := leader.Propose(context.Background(), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Propose #%d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && leader.storage.FirstIndex() <= 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := leader.storage.FirstIndex(); got <= 1 {
		t.Fatalf("leader's FirstIndex = %d, want >1 (a snapshot should have compacted the log by now)", got)
	}

	data, lastIndex, _, err := leader.storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if lastIndex == 0 || len(data) == 0 {
		t.Fatalf("expected a non-empty snapshot, got lastIndex=%d len(data)=%d", lastIndex, len(data))
	}
}

func TestSnapshot_LaggingFollowerCatchesUpViaInstallSnapshot(t *testing.T) {
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
		sm := &recordingStateMachine{}
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			Storage:            newMemStorage(),
			Transport:          transport,
			SM:                 sm,
			SnapshotInterval:   3,
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

	// n3 is registered (so the leader can eventually reach it) but its
	// Run() loop doesn't start yet — simulating a node that's down/behind
	// while n1 and n2 (still a majority of the full 3-node cluster) commit
	// and compact several rounds of entries.
	go nodes["n1"].Run()
	go nodes["n2"].Run()

	_, leader := awaitLeader(t, map[NodeID]*Node{"n1": nodes["n1"], "n2": nodes["n2"]})

	const numWrites = 10
	for i := 0; i < numWrites; i++ {
		if _, err := leader.Propose(context.Background(), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Propose #%d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && leader.storage.FirstIndex() <= 1 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := leader.storage.FirstIndex(); got <= 1 {
		t.Fatalf("leader never compacted its log (FirstIndex=%d) — test setup didn't exercise the scenario it's meant to", got)
	}

	// Now bring n3 online. Its nextIndex (seeded at the original
	// becomeLeader time, before any of the writes above) is far below the
	// leader's post-compaction FirstIndex, so replicateOne must route it
	// through InstallSnapshot — a plain AppendEntries replay of the full
	// history is no longer even possible, since the leader doesn't have
	// those entries anymore.
	go nodes["n3"].Run()

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sms["n3"].appliedCount() < numWrites {
		time.Sleep(10 * time.Millisecond)
	}
	if got := sms["n3"].appliedCount(); got != numWrites {
		t.Fatalf("n3 only caught up %d/%d entries", got, numWrites)
	}
	if idx := nodes["n3"].storage.FirstIndex(); idx <= 1 {
		t.Fatalf("expected n3's own log to start after a compacted prefix (FirstIndex>1) once it installed the snapshot, got %d", idx)
	}
}
