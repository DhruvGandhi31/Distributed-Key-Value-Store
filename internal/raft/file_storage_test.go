package raft

import (
	"encoding/binary"
	"os"
	"path/filepath"
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
	// SaveHardState's snapshotIndex/snapshotTerm parameters are written to
	// disk for format stability but are NOT authoritative for recovery —
	// the snapshot file is (see SaveHardState's doc comment). So this test
	// only exercises term/votedFor round-tripping; snapshot-boundary
	// round-tripping via InitialHardState is covered separately by
	// TestFileStorage_SnapshotCompactsLogAndPersists, which goes through
	// SaveSnapshot instead.
	cases := []struct {
		name     string
		term     uint64
		votedFor NodeID
	}{
		{"basic", 5, "node2"},
		{"emptyVotedFor", 7, NoLeader},
		{"largeValues", 1 << 40, "node-with-a-longer-id"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()

			fs, err := NewFileStorage(dir, "node1")
			if err != nil {
				t.Fatalf("NewFileStorage: %v", err)
			}
			if err := fs.SaveHardState(tc.term, tc.votedFor, 0, 0); err != nil {
				t.Fatalf("SaveHardState: %v", err)
			}

			reloaded, err := NewFileStorage(dir, "node1")
			if err != nil {
				t.Fatalf("NewFileStorage (reload): %v", err)
			}

			term, votedFor, snapIdx, snapTerm := reloaded.InitialHardState()
			if term != tc.term || votedFor != tc.votedFor || snapIdx != 0 || snapTerm != 0 {
				t.Fatalf("round-trip mismatch: got term=%d votedFor=%q snapIdx=%d snapTerm=%d, want term=%d votedFor=%q snapIdx=0 snapTerm=0",
					term, votedFor, snapIdx, snapTerm, tc.term, tc.votedFor)
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

func TestFileStorage_RecoversFromTornTrailingWrite(t *testing.T) {
	dir := t.TempDir()

	fs, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	want := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}
	if err := fs.AppendEntries(want); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	// Simulate a crash mid-write: a length prefix was fsync'd but the
	// entry bytes that should follow it never made it to disk.
	logPath := filepath.Join(dir, "node1", logFileName)
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open log for torn-write simulation: %v", err)
	}
	if err := binary.Write(f, binary.BigEndian, uint32(100)); err != nil {
		t.Fatalf("write torn length prefix: %v", err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil { // far short of the 100 bytes promised
		t.Fatalf("write torn payload: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	recovered, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage should recover past a torn trailing write, got error: %v", err)
	}
	got, err := recovered.Entries(1, 3)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered entries = %+v, want %+v (torn trailing record should be silently dropped)", got, want)
	}

	// The recovered storage must still be writable — a subsequent append
	// should succeed and not somehow preserve the torn garbage.
	if err := recovered.AppendEntries([]LogEntry{{Index: 3, Term: 1, Data: []byte("c")}}); err != nil {
		t.Fatalf("AppendEntries after recovery: %v", err)
	}
	if got := recovered.LastIndex(); got != 3 {
		t.Fatalf("LastIndex after post-recovery append = %d, want 3", got)
	}
}

func TestFileStorage_SnapshotCompactsLogAndPersists(t *testing.T) {
	dir := t.TempDir()

	fs, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
		{Index: 3, Term: 2, Data: []byte("c")},
		{Index: 4, Term: 2, Data: []byte("d")},
	}
	if err := fs.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	snapData := []byte("snapshot-of-first-3-entries")
	if err := fs.SaveSnapshot(snapData, 3, 2); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	if got := fs.FirstIndex(); got != 4 {
		t.Fatalf("FirstIndex after snapshot = %d, want 4", got)
	}
	if got := fs.LastIndex(); got != 4 {
		t.Fatalf("LastIndex after snapshot = %d, want 4 (entry 4 survives, it's after lastIndex=3)", got)
	}
	if _, err := fs.EntryAt(3); err == nil {
		t.Fatalf("EntryAt(3) should fail: compacted away by the snapshot")
	}
	if term, err := fs.TermAt(3); err != nil || term != 2 {
		t.Fatalf("TermAt(3) = %d, %v; want 2, nil (snapshot boundary term, even though entry 3 itself is gone)", term, err)
	}
	remaining, err := fs.Entries(1, 5)
	if err != nil {
		t.Fatalf("Entries: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Index != 4 {
		t.Fatalf("Entries(1,5) after compaction = %+v, want only entry 4", remaining)
	}

	// Persistence: reopening should recover the snapshot boundary and only
	// the surviving log entry, not the compacted-away prefix.
	reloaded, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage (reload): %v", err)
	}
	if got := reloaded.FirstIndex(); got != 4 {
		t.Fatalf("reloaded FirstIndex = %d, want 4", got)
	}
	if got := reloaded.LastIndex(); got != 4 {
		t.Fatalf("reloaded LastIndex = %d, want 4", got)
	}
	data, lastIndex, lastTerm, err := reloaded.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if string(data) != string(snapData) || lastIndex != 3 || lastTerm != 2 {
		t.Fatalf("LoadSnapshot = %q,%d,%d; want %q,3,2", data, lastIndex, lastTerm, snapData)
	}
	if _, _, snapIdx, snapTerm := reloaded.InitialHardState(); snapIdx != 3 || snapTerm != 2 {
		t.Fatalf("InitialHardState snapshot boundary = %d,%d; want 3,2 (from the snapshot file, not hardstate's own unused copy)", snapIdx, snapTerm)
	}
}

func TestFileStorage_SnapshotCoveringEntireLogLeavesItEmpty(t *testing.T) {
	fs, err := NewFileStorage(t.TempDir(), "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{
		{Index: 1, Term: 1}, {Index: 2, Term: 1},
	}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := fs.SaveSnapshot([]byte("all"), 2, 1); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if got := fs.FirstIndex(); got != 3 {
		t.Fatalf("FirstIndex = %d, want 3", got)
	}
	if got := fs.LastIndex(); got != 2 {
		t.Fatalf("LastIndex = %d, want 2 (falls back to the snapshot boundary since the log is now empty)", got)
	}
	if got := fs.LastTerm(); got != 1 {
		t.Fatalf("LastTerm = %d, want 1 (falls back to the snapshot boundary)", got)
	}

	// The log should now accept new entries starting at index 3.
	if err := fs.AppendEntries([]LogEntry{{Index: 3, Term: 2}}); err != nil {
		t.Fatalf("AppendEntries after full compaction: %v", err)
	}
	if got := fs.LastIndex(); got != 3 {
		t.Fatalf("LastIndex after append = %d, want 3", got)
	}
}

func TestFileStorage_RedundantSnapshotIsNoOp(t *testing.T) {
	fs, err := NewFileStorage(t.TempDir(), "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}
	if err := fs.AppendEntries([]LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if err := fs.SaveSnapshot([]byte("first"), 2, 1); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	// A second call at or before the existing boundary must not corrupt
	// anything (e.g. two nodes racing to snapshot around the same index).
	if err := fs.SaveSnapshot([]byte("stale"), 1, 1); err != nil {
		t.Fatalf("SaveSnapshot (redundant): %v", err)
	}
	data, lastIndex, _, err := fs.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if string(data) != "first" || lastIndex != 2 {
		t.Fatalf("redundant SaveSnapshot overwrote the boundary: got data=%q lastIndex=%d, want \"first\",2", data, lastIndex)
	}
}

func TestFileStorage_LogFileSizeBoundedAfterSnapshot(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir, "node1")
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	const n = 500
	entries := make([]LogEntry, n)
	for i := range entries {
		entries[i] = LogEntry{Index: uint64(i + 1), Term: 1, Data: make([]byte, 256)}
	}
	if err := fs.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}

	logPath := filepath.Join(dir, "node1", logFileName)
	infoBefore, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log before snapshot: %v", err)
	}

	if err := fs.SaveSnapshot([]byte("compacted"), n-1, 1); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	infoAfter, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log after snapshot: %v", err)
	}
	if infoAfter.Size() >= infoBefore.Size() {
		t.Fatalf("log file size did not shrink after snapshot: before=%d after=%d", infoBefore.Size(), infoAfter.Size())
	}
	// Only entry n should remain (index n-1 was the snapshot boundary).
	if got := fs.LastIndex() - fs.FirstIndex() + 1; got != 1 {
		t.Fatalf("expected exactly 1 surviving entry, got %d", got)
	}
}
