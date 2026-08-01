package raft

// Storage persists the parts of Raft state that must survive a crash.
//
// This is a Phase 2 subset covering only the hard state (term, votedFor)
// needed for leader election. Phase 3 will extend this same interface with
// log-entry methods (Entries, EntryAt, TermAt, TruncateAfter) and Phase 5
// with snapshot methods. Extend it in place when those phases land — do not
// redesign it.
type Storage interface {
	SaveHardState(term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) error
	LastIndex() uint64
	LastTerm() uint64
}
