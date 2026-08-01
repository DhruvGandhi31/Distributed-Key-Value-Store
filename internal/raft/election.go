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
			n.broadcastHeartbeat()
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

// handleAppendEntries processes an incoming AppendEntries RPC and returns
// the reply. Only ever called from within Run's runloop.
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

	// No log consistency check yet: Entries is always empty in Phase 2.
	// Phase 3 adds the PrevLogIndex/PrevLogTerm match check and entry
	// append/truncate logic here.
	n.state = Follower
	n.leader = args.LeaderID
	n.resetElectionTimer()

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// broadcastHeartbeat sends an empty AppendEntries to every peer.
func (n *Node) broadcastHeartbeat() {
	term := n.currentTerm
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: n.storage.LastIndex(),
		PrevLogTerm:  n.storage.LastTerm(),
		Entries:      nil,
		LeaderCommit: n.commitIndex,
	}

	for _, peer := range n.peers {
		peer := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
			defer cancel()
			reply, err := n.transport.SendAppendEntries(ctx, peer, args)
			if err != nil {
				return
			}
			select {
			case n.appendResultCh <- appendResult{term: term, peer: peer, reply: reply}:
			case <-n.stopCh:
			}
		}()
	}
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

// handleAppendResult processes an AppendEntries reply gathered by
// broadcastHeartbeat.
func (n *Node) handleAppendResult(res appendResult) {
	if res.reply.Term > n.currentTerm {
		n.stepDown(res.reply.Term)
		return
	}
}

// becomeLeader transitions the node to Leader and immediately asserts
// authority with a heartbeat, rather than waiting for the next tick.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	n.logger.Infof("became Leader term=%d", n.currentTerm)
	n.heartbeatElapsed = 0
	n.broadcastHeartbeat()
}

// stepDown reverts the node to Follower at the given (higher-or-equal)
// term, persisting the change before it takes effect.
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
	return Status{State: n.state, Leader: n.leader, Term: n.currentTerm}
}

// fatalExit terminates the process. Used when a durable persist of Raft
// hard state fails: proceeding with an in-memory-only change risks a
// double-vote or term violation after crash recovery, which is worse than
// crashing now.
func fatalExit() {
	os.Exit(1)
}
