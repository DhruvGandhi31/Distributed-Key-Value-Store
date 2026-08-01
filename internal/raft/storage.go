package raft

// Storage persists the parts of Raft state that must survive a crash.
//
// Phase 2 introduced the hard-state subset (SaveHardState). Phase 3 extends
// it in place with log-entry methods needed for replication. Phase 5 will
// extend it again with SaveSnapshot/LoadSnapshot — extend this interface
// when those phases land, do not redesign it.
//
// FirstIndex always returns 1 until Phase 5 adds log compaction; there is no
// snapshot boundary to skip past yet.
type Storage interface {
	SaveHardState(term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) error

	// AppendEntries durably appends entries to the end of the log. It must
	// fsync before returning success (fsync-before-ack invariant) — callers
	// rely on a successful return meaning the entries cannot be lost by a
	// crash immediately after.
	AppendEntries(entries []LogEntry) error

	// Entries returns the entries in [lo, hi) (half-open range).
	Entries(lo, hi uint64) ([]LogEntry, error)

	// EntryAt returns the single entry at index.
	EntryAt(index uint64) (LogEntry, error)

	// TermAt returns the term of the entry at index. TermAt(0) returns
	// (0, nil) — index 0 is the sentinel "before the log began".
	TermAt(index uint64) (uint64, error)

	// FirstIndex returns the index of the oldest entry still in the log.
	FirstIndex() uint64

	// LastIndex returns the index of the newest entry in the log, or 0 if
	// the log is empty.
	LastIndex() uint64

	// LastTerm returns the term of the newest entry in the log, or 0 if the
	// log is empty.
	LastTerm() uint64

	// TruncateAfter discards all entries with index > index. Used when a
	// follower's log conflicts with the leader's and must be rolled back
	// before the leader's entries can be appended.
	TruncateAfter(index uint64) error
}
