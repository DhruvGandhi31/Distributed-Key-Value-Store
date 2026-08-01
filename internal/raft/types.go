package raft

import "errors"

// NodeID identifies a single Raft participant. NoLeader is the zero value,
// used to represent "no leader known" / "no vote cast" without a pointer.
type NodeID string

const NoLeader NodeID = ""

// State is a node's role in the Raft protocol.
type State uint8

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RequestVoteArgs is the payload for the RequestVote RPC (Raft paper Figure 2).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs is the payload for the AppendEntries RPC. It also serves
// as the heartbeat when Entries is empty.
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    uint64
	Success bool

	// ConflictIndex and ConflictTerm are unused in Phase 2 (no log yet).
	// Reserved for Phase 3's fast-backoff optimization on rejected
	// AppendEntries, so followers can skip nextIndex back a whole
	// conflicting term at once instead of one entry per RPC.
	ConflictIndex uint64
	ConflictTerm  uint64
}

// ErrShutdown is returned by Node entry points when the node has stopped.
var ErrShutdown = errors.New("raft: node is shutting down")

// ErrNotLeader is returned by Propose when called on a node that is not
// currently the Raft leader (or steps down before the proposal commits).
var ErrNotLeader = errors.New("raft: not the leader")
