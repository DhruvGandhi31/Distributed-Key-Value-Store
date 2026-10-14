package integration

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSnapshotCatchup is the Phase 5 checkpoint, automated: a follower
// falls behind while the rest of the cluster writes past the snapshot
// boundary (so the leader has genuinely compacted away entries the
// follower would otherwise need), then rejoins and must catch up via
// InstallSnapshot — an ordinary AppendEntries replay of the missing
// entries is no longer even possible, since the leader doesn't have them
// anymore.
//
// There's no
// separate TriggerSnapshot call, since compaction is automatic (see
// Config.SnapshotInterval) — a small interval configured at cluster
// startup is enough to make it fire naturally during the write loop
// below.
func TestSnapshotCatchup(t *testing.T) {
	const snapshotInterval = 20
	cluster := startClusterWithSnapshotInterval(t, 3, snapshotInterval)
	cluster.WaitForLeader(t, 5*time.Second)

	laggard := cluster.KillOne()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const numWrites = 100 // several multiples of snapshotInterval, so compaction fires more than once
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := cluster.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Put(%s): %v", key, err)
		}
	}

	cluster.Restart(laggard)
	cluster.WaitForCatchup(t, laggard, 10*time.Second)

	lastKey := fmt.Sprintf("k%d", numWrites-1)
	got, ok, err := cluster.Get(ctx, lastKey)
	if err != nil {
		t.Fatalf("Get(%s): %v", lastKey, err)
	}
	if !ok || string(got) != "v" {
		t.Fatalf("Get(%s) = (%q, %v), want (v, true)", lastKey, got, ok)
	}
}
