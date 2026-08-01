package raft

// Storage persists the parts of Raft state that must survive a crash.
//
// Phase 2 introduced the hard-state subset (SaveHardState). Phase 3
// extended it with log-entry methods needed for replication. Phase 5 adds
// SaveSnapshot/LoadSnapshot for log compaction — this is now the complete
// interface the guide describes.
//
// FirstIndex returns 1 until the first snapshot is taken; after that it
// returns lastIncludedIndex+1 for whatever snapshot was most recently
// saved.
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
	// (0, nil) — index 0 is the sentinel "before the log began". TermAt of
	// the most recent snapshot's lastIncludedIndex also succeeds (returning
	// lastIncludedTerm) even though that entry itself is no longer in the
	// log — this is what lets AppendEntries's consistency check still work
	// against a follower whose PrevLogIndex lands exactly on the snapshot
	// boundary.
	TermAt(index uint64) (uint64, error)

	// FirstIndex returns the index of the oldest entry still in the log
	// (lastIncludedIndex+1 after a snapshot, 1 if none has been taken yet).
	FirstIndex() uint64

	// LastIndex returns the index of the newest entry in the log, or the
	// most recent snapshot's lastIncludedIndex if the log is empty because
	// everything has been compacted, or 0 if neither exists yet.
	LastIndex() uint64

	// LastTerm returns the term of the newest entry in the log, falling
	// back to the same rule as LastIndex, or 0 if neither exists yet.
	LastTerm() uint64

	// TruncateAfter discards all entries with index > index. Used when a
	// follower's log conflicts with the leader's and must be rolled back
	// before the leader's entries can be appended.
	TruncateAfter(index uint64) error

	// SaveSnapshot persists data as a snapshot covering everything up to
	// and including lastIndex (at lastTerm), then compacts the log by
	// discarding entries with index <= lastIndex. Entries after lastIndex,
	// if any, are left in place — this same method is used both by a
	// leader/follower compacting its own already-consistent log, and by a
	// follower installing a snapshot sent by the leader (see
	// installSnapshot in snapshot.go for why that reuse is safe: any stale
	// leftover suffix self-corrects via the normal AppendEntries
	// consistency check on the next replication round).
	SaveSnapshot(data []byte, lastIndex, lastTerm uint64) error

	// LoadSnapshot returns the most recently saved snapshot, or
	// (nil, 0, 0, nil) if none has been taken yet — that's a valid "no
	// snapshot" result, not an error.
	LoadSnapshot() (data []byte, lastIndex, lastTerm uint64, err error)
}
