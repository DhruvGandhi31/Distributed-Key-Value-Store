package raft

import "testing"

func TestFileStorage_FreshDirZeroValues(t *testing.T) {
	dir := t.TempDir()

	fs, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	term, votedFor, snapIdx, snapTerm := fs.InitialHardState()
	if term != 0 || votedFor != NoLeader || snapIdx != 0 || snapTerm != 0 {
		t.Fatalf("expected zero-valued state, got term=%d votedFor=%q snapIdx=%d snapTerm=%d",
			term, votedFor, snapIdx, snapTerm)
	}
}

func TestFileStorage_SaveAndReloadRoundTrips(t *testing.T) {
	cases := []struct {
		name          string
		term          uint64
		votedFor      NodeID
		snapshotIndex uint64
		snapshotTerm  uint64
	}{
		{"basic", 5, "node2", 0, 0},
		{"emptyVotedFor", 7, NoLeader, 3, 2},
		{"largeValues", 1 << 40, "node-with-a-longer-id", 1 << 30, 1 << 20},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			fs, err := NewFileStorage(dir, "node1")
			if err != nil {
				t.Fatalf("NewFileStorage: %v", err)
			}
			if err := fs.SaveHardState(tc.term, tc.votedFor, tc.snapshotIndex, tc.snapshotTerm); err != nil {
				t.Fatalf("SaveHardState: %v", err)
			}

			reloaded, err := NewFileStorage(dir, "node1")
			if err != nil {
				t.Fatalf("NewFileStorage (reload): %v", err)
			}

			term, votedFor, snapIdx, snapTerm := reloaded.InitialHardState()
			if term != tc.term || votedFor != tc.votedFor || snapIdx != tc.snapshotIndex || snapTerm != tc.snapshotTerm {
				t.Fatalf("round-trip mismatch: got term=%d votedFor=%q snapIdx=%d snapTerm=%d, want term=%d votedFor=%q snapIdx=%d snapTerm=%d",
					term, votedFor, snapIdx, snapTerm, tc.term, tc.votedFor, tc.snapshotIndex, tc.snapshotTerm)
			}
		})
	}
}
