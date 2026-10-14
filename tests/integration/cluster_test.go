package integration

import (
	"context"
	"testing"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

func TestLeaderElection(t *testing.T) {
	cluster := startCluster(t, 3)
	cluster.WaitForLeader(t, 5*time.Second)
}

func TestPutGet(t *testing.T) {
	cluster := startCluster(t, 3)
	cluster.WaitForLeader(t, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cluster.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Reads are served locally and aren't linearizable yet (no read-index),
	// so a read that lands on a follower right after the write can briefly
	// miss it. Poll instead of asserting on the very first read.
	cluster.WaitForValue(t, ctx, "k", "v", 5*time.Second)
}

func TestGet_MissingKeyIsNotFound(t *testing.T) {
	cluster := startCluster(t, 3)
	cluster.WaitForLeader(t, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, ok, err := cluster.Get(ctx, "never-written")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatalf("Get on a never-written key: got ok=true, want false")
	}
}

// TestLeaderFailover is the Phase 6 checkpoint, automated: a write goes
// through, the leader is killed mid-session, and the SAME client — with
// no code change and no manual redirection — keeps working against
// whichever node takes over, and everything committed before the kill is
// still there afterward.
func TestLeaderFailover(t *testing.T) {
	cluster := startCluster(t, 3)
	leader := cluster.WaitForLeader(t, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cluster.Put(ctx, "before-failover", []byte("v1")); err != nil {
		t.Fatalf("Put before failover: %v", err)
	}

	cluster.Kill(leader.id)

	newLeader := cluster.WaitForLeader(t, 5*time.Second)
	if newLeader.id == leader.id {
		t.Fatalf("WaitForLeader returned the killed node %s as leader", leader.id)
	}

	if err := cluster.Put(ctx, "after-failover", []byte("v2")); err != nil {
		t.Fatalf("Put after failover: %v", err)
	}
	cluster.WaitForValue(t, ctx, "before-failover", "v1", 5*time.Second)
	cluster.WaitForValue(t, ctx, "after-failover", "v2", 5*time.Second)
}

// TestFullClusterRestart is the Phase 4 checkpoint, automated: kill every
// node, restart every node from disk, and confirm previously-committed
// data survives with zero writes in between — the no-op-on-election
// mechanism (see becomeLeader in internal/raft) re-establishing
// commitIndex after everyone's volatile state resets to zero
// simultaneously.
func TestFullClusterRestart(t *testing.T) {
	cluster := startCluster(t, 3)
	cluster.WaitForLeader(t, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cluster.Put(ctx, "persistent", []byte("survives")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	ids := make([]raft.NodeID, 0, len(cluster.nodes))
	for id := range cluster.nodes {
		ids = append(ids, id)
	}
	for _, id := range ids {
		cluster.Kill(id)
	}
	for _, id := range ids {
		cluster.Restart(id)
	}

	cluster.WaitForLeader(t, 5*time.Second)

	// Fresh SDK client: the cached one from before the restart may still
	// point at a now-dead address, and we want to confirm the DATA
	// survived, not just that the old client's retry logic papers over it.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	cluster.sdkClient = nil
	cluster.WaitForValue(t, ctx2, "persistent", "survives", 5*time.Second)
}
