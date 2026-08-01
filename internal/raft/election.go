package raft

import (
	"context"
	"os"
)

// tick advances the node's internal timers by one TickInterval. It is only
// ever called from within Run's runloop.
func (n *Node) tick() {
	switch n.state {
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.randomizedElectionTicks {
			n.startElection()
		}
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.heartbeatTicks {
			n.heartbeatElapsed = 0
			n.replicateAll()
		}
	}
}

// startElection begins a new election round: bumps the term, votes for
// itself, and requests votes from every peer.
func (n *Node) startElection() {
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	n.leader = NoLeader
	n.resetElectionTimer()
	n.votesReceived = map[NodeID]bool{n.id: true}

	if err := n.storage.SaveHardState(n.currentTerm, n.votedFor, 0, 0); err != nil {
		n.logger.Errorf("failed to persist hard state before requesting votes: %v", err)
		fatalExit()
		return
	}

	if len(n.peers) == 0 {
		if len(n.votesReceived) >= n.quorumSize() {
			n.becomeLeader()
		}
		return
	}

	term := n.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: n.storage.LastIndex(),
		LastLogTerm:  n.storage.LastTerm(),
	}

	for _, peer := range n.peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
			defer cancel()
			reply, err := n.transport.SendRequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			select {
			case n.voteResultCh <- voteResult{term: term, peer: peer, reply: reply}:
			case <-n.stopCh:
			}
		}()
	}
}

// handleRequestVote processes an incoming RequestVote RPC and returns the
// reply. Only ever called from within Run's runloop.
func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	if args.Term < n.currentTerm {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	logOK := args.LastLogTerm > n.storage.LastTerm() ||
		(args.LastLogTerm == n.storage.LastTerm() && args.LastLogIndex >= n.storage.LastIndex())

	if (n.votedFor == NoLeader || n.votedFor == args.CandidateID) && logOK {
		if err := n.storage.SaveHardState(n.currentTerm, args.CandidateID, 0, 0); err != nil {
			n.logger.Errorf("failed to persist vote, declining: %v", err)
			return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
		}
		n.votedFor = args.CandidateID
		// Deliberately no resetElectionTimer() here: granting a vote must
		// not extend a follower's election timeout, only a valid
		// AppendEntries from a real leader does.
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

// handleAppendEntries processes an incoming AppendEntries RPC: term check,
// log consistency check against PrevLogIndex/PrevLogTerm, appending any new
// entries (truncating first on divergence), and advancing commitIndex from
// LeaderCommit. Only ever called from within Run's runloop.
func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if n.state == Leader {
		n.logger.Warnf("received AppendEntries from another leader in the same term — this should not happen under Election Safety")
	}
	n.state = Follower
	n.leader = args.LeaderID
	n.resetElectionTimer()

	// Consistency check (Raft §5.3): reject unless our log has an entry at
	// PrevLogIndex whose term matches PrevLogTerm. PrevLogIndex 0 always
	// matches (TermAt(0) == 0, the "before the log began" sentinel).
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > n.storage.LastIndex() {
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
		prevTerm, err := n.storage.TermAt(args.PrevLogIndex)
		if err != nil || prevTerm != args.PrevLogTerm {
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	if len(args.Entries) > 0 {
		if err := n.appendNewEntries(args.PrevLogIndex, args.Entries); err != nil {
			// Non-fatal: unlike a hard-state persist failure, this creates
			// no unsafe in-memory/on-disk divergence — the follower simply
			// stays behind and the leader retries. See replication.go's
			// handleAppendResult backoff-and-retry path.
			n.logger.Errorf("failed to persist appended entries: %v", err)
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if last := n.storage.LastIndex(); newCommit > last {
			newCommit = last
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.applyCommitted()
		}
	}

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// appendNewEntries reconciles this follower's log with entries sent by the
// leader (which start at index prevLogIndex+1): entries already present
// with a matching term are left alone (so a duplicate/delayed RPC never
// discards potentially-committed entries), the first mismatch truncates
// everything from that index onward, and any genuinely new entries are
// appended.
//
// A follower snapshots on its own schedule (applyCommitted's snapshot
// trigger runs on every node, not just the leader), so it can end up
// having already compacted away indices a retransmitted or
// not-yet-caught-up-with AppendEntries still references. Those are always
// safe to skip outright — compaction only ever happens after an index is
// already applied, so idx < FirstIndex() means we're certain it's already
// committed and correct, without needing to look up its term (which would
// fail: it's not in the log anymore).
func (n *Node) appendNewEntries(prevLogIndex uint64, entries []LogEntry) error {
	insertAt := prevLogIndex + 1

	for i, e := range entries {
		idx := insertAt + uint64(i)
		if idx < n.storage.FirstIndex() {
			continue
		}
		if idx > n.storage.LastIndex() {
			return n.storage.AppendEntries(entries[i:])
		}
		existingTerm, err := n.storage.TermAt(idx)
		if err != nil {
			return err
		}
		if existingTerm != e.Term {
			if err := n.storage.TruncateAfter(idx - 1); err != nil {
				return err
			}
			return n.storage.AppendEntries(entries[i:])
		}
		// Same index, same term: already have it, skip.
	}
	return nil
}

// handleVoteResult processes a RequestVote reply gathered by startElection.
func (n *Node) handleVoteResult(res voteResult) {
	if res.reply.Term > n.currentTerm {
		n.stepDown(res.reply.Term)
		return
	}
	if n.state != Candidate || res.term != n.currentTerm {
		return
	}
	if res.reply.VoteGranted {
		n.votesReceived[res.peer] = true
		if len(n.votesReceived) >= n.quorumSize() {
			n.becomeLeader()
		}
	}
}

// becomeLeader transitions the node to Leader, (re)initializes per-peer
// replication bookkeeping, and appends a no-op entry for this term before
// asserting authority with a heartbeat rather than waiting for the next
// tick.
//
// The no-op entry matters more than it looks: commitIndex is volatile by
// design (never persisted — see CLAUDE.md) and always starts at 0 in a
// fresh Node. Per the Figure 8 rule, a leader can only advance commitIndex
// by directly counting majority replicas of a CURRENT-term entry, never an
// inherited older-term one. Without appending something in its own term, a
// newly-elected leader with no immediate client writes would never
// re-establish commitIndex at all — every previously-committed entry would
// sit correctly on disk but never get replayed into the state machine.
// This bites hardest exactly when every node in the cluster restarts
// simultaneously (Phase 4's crash-recovery checkpoint): nobody's in-memory
// commitIndex survives, so without this fix the newly-elected leader would
// never recover what was already safely committed before the restart.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	n.logger.Infof("became Leader term=%d", n.currentTerm)

	last := n.storage.LastIndex()
	n.nextIndex = make(map[NodeID]uint64, len(n.peers))
	n.matchIndex = make(map[NodeID]uint64, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = last + 1
		n.matchIndex[peer] = 0
	}

	noop := LogEntry{Index: last + 1, Term: n.currentTerm, Type: EntryNoOp}
	if err := n.storage.AppendEntries([]LogEntry{noop}); err != nil {
		n.logger.Errorf("failed to append no-op entry on election: %v", err)
		fatalExit()
		return
	}

	n.heartbeatElapsed = 0
	n.replicateAll()
	n.advanceCommitIndex() // covers the single-node-cluster (no peers) case
}

// stepDown reverts the node to Follower at the given (higher-or-equal)
// term, persisting the change before it takes effect, and fails any
// proposals this node was carrying as leader — they're no longer
// guaranteed to commit under this node's watch, so callers must retry
// against whichever node becomes leader next.
func (n *Node) stepDown(term uint64) {
	if term == n.currentTerm && n.state == Follower {
		return
	}

	n.currentTerm = term
	n.state = Follower
	n.votedFor = NoLeader
	n.leader = NoLeader

	if err := n.storage.SaveHardState(n.currentTerm, n.votedFor, 0, 0); err != nil {
		n.logger.Errorf("failed to persist hard state while stepping down: %v", err)
		fatalExit()
		return
	}

	n.failPendingProposals(ErrNotLeader)
}

// failPendingProposals resolves every still-outstanding Propose call with
// err and clears the map. Called on step-down (proposals from a term this
// node no longer leads can't be guaranteed to commit) and on shutdown (so
// callers don't hang until their context times out).
func (n *Node) failPendingProposals(err error) {
	for index, ch := range n.pendingProposals {
		delete(n.pendingProposals, index)
		ch <- proposalResult{err: err}
	}
}

// resetElectionTimer restarts the election countdown with a freshly
// randomized timeout, to avoid split-vote livelock among peers.
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	spread := n.electionTicksMax - n.electionTicksMin + 1
	n.randomizedElectionTicks = n.electionTicksMin + n.rng.Intn(spread)
}

// quorumSize returns the number of votes (including the node's own)
// needed to win an election or commit an entry.
func (n *Node) quorumSize() int {
	return (len(n.peers)+1)/2 + 1
}

// status builds the Status snapshot returned to external callers.
func (n *Node) status() Status {
	return Status{
		State:       n.state,
		Leader:      n.leader,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
	}
}

// fatalExit terminates the process. Used when a durable persist of Raft
// hard state fails: proceeding with an in-memory-only change risks a
// double-vote or term violation after crash recovery, which is worse than
// crashing now.
func fatalExit() {
	os.Exit(1)
}
