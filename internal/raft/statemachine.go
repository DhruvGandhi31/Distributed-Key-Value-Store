package raft

//EntryType distinguishes log entries that carry a client command from
//internal no-op entries a leader appends on election to commit prior terms

type EntryType uint8

const (
	EntryNoOp   EntryType = 0
	EntryNormal EntryType = 1
)

// LogEntry is a single entry in the replicated log. Raft treats data as an opaque blob
// only the statemachine knows how to interpret it
type LogEntry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// stateMachine is fed committed log entries in order. Implementations must be deterministic
// given that the same sequence of entries, like every node's state machine must end up in the same state
type StateMachine interface {
	Apply(entry LogEntry) interface{}
	Snapshot() ([]byte, error)
	Restore(date []byte) error
}
