package raft

import (
	"reflect"
	"testing"
)

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

func TestFileStorage_LogEmptyByDefault(t *testing.T) {
	fs, err := NewFileStorage(t.TempDir(), "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if got := fs.LastIndex(); got != 0 {
		t.Fatalf("LastIndex on empty log = %d, want 0", got)
	}
	if got := fs.LastTerm(); got != 0 {
		t.Fatalf("LastTerm on empty log = %d, want 0", got)
	}
	if got := fs.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1", got)
	}
	if term, err := fs.TermAt(0); err != nil || term != 0 {
		t.Fatalf("TermAt(0) = %d, %v; want 0, nil", term, err)
	}
	if _, err := fs.EntryAt(1); err == nil {
		t.Fatalf("EntryAt(1) on empty log: expected error, got nil")
	}
}

func TestFileStorage_AppendAndReloadRoundTrips(t *testing.T) {
	dir := t.TempDir()

	fs, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	want := []LogEntry{
		{Index: 1, Term: 1, Type: EntryNormal, Data: []byte("a")},
		{Index: 2, Term: 1, Type: EntryNormal, Data: []byte("bb")},
		{Index: 3, Term: 2, Type: EntryNoOp, Data: nil},
	}
	if err := fs.AppendEntries(want); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	if got := fs.LastIndex(); got != 3 {
		t.Fatalf("LastIndex = %d, want 3", got)
	}
	if got := fs.LastTerm(); got != 2 {
		t.Fatalf("LastTerm = %d, want 2", got)
	}

	reloaded, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage (reload): %v", err)
	}
	got, err := reloaded.Entries(1, 4)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reloaded entries = %+v, want %+v", got, want)
	}
}

func TestFileStorage_EntriesRangeClamping(t *testing.T) {
	fs, err := NewFileStorage(t.TempDir(), "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	entries := []LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1},
	}
	if err := fs.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	got, err := fs.Entries(0, 100)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if !reflect.DeepEqual(got, entries) {
		t.Fatalf("Entries(0,100) = %+v, want %+v (clamped to actual bounds)", got, entries)
	}

	if got, err := fs.Entries(5, 10); err != nil || len(got) != 0 {
		t.Fatalf("Entries(5,10) on a 3-entry log = %+v, %v; want empty, nil", got, err)
	}
}

func TestFileStorage_TruncateAfterThenAppendPersists(t *testing.T) {
	dir := t.TempDir()

	fs, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 1, Data: []byte("c")},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	if err := fs.TruncateAfter(1); err != nil {
		t.Fatalf("TruncateAfter: %v", err)
	}
	if got := fs.LastIndex(); got != 1 {
		t.Fatalf("LastIndex after truncate = %d, want 1", got)
	}

	replacement := []LogEntry{
		{Index: 2, Term: 2, Data: []byte("x")},
		{Index: 3, Term: 2, Data: []byte("y")},
	}
	if err := fs.AppendEntries(replacement); err != nil {
		t.Fatalf("AppendEntries after truncate: %v", err)
	}

	reloaded, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage (reload): %v", err)
	}
	got, err := reloaded.Entries(1, 4)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	want := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 2, Data: []byte("x")},
		{Index: 3, Term: 2, Data: []byte("y")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reloaded entries after truncate+append = %+v, want %+v", got, want)
	}
}
