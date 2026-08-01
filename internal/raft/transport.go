package raft

import "context"

// Transport sends RPCs to peer nodes. Implementations handle the actual
// wire protocol (HTTP+gob, in-process routing for tests, etc.) — Raft
// itself only depends on this interface.
type Transport interface {
	SendRequestVote(ctx context.Context, peer NodeID, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, peer NodeID, args AppendEntriesArgs) (AppendEntriesReply, error)
}
