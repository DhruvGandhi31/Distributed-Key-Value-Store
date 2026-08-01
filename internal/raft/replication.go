package raft

import (
	"context"
	"sort"
)

// maxEntriesPerAppend caps how many log entries a single AppendEntries RPC
// carries, so a far-behind follower doesn't force one enormous request.
const maxEntriesPerAppend = 256

// replicateAll sends an AppendEntries (heartbeat if there's nothing new) to
// every peer. Called on the heartbeat tick, immediately on becoming leader,
// and immediately after a proposal is appended, so a write's replication
// doesn't wait for the next heartbeat tick.
func (n *Node) replicateAll() {
	for _, peer := range n.peers {
		n.replicateOne(peer)
	}
}

// replicateOne sends peer everything from nextIndex[peer] onward (or
// nothing, as a heartbeat, if peer is already caught up). Only ever called
// from within Run's runloop; the actual RPC happens in a spawned goroutine
// so a slow/unreachable peer can't stall the runloop.
func (n *Node) replicateOne(peer NodeID) {
	next := n.nextIndex[peer]
	if next < 1 {
		next = 1
	}

	// If peer needs an entry we've already compacted away, a normal
	// AppendEntries can't supply it — send a snapshot instead. next ==
	// FirstIndex() is still fine: TermAt(FirstIndex()-1) == TermAt of the
	// snapshot boundary works (see Storage's doc comment), so only
	// next < FirstIndex() is actually unanswerable.
	if next < n.storage.FirstIndex() {
		n.sendInstallSnapshot(peer)
		return
	}

	prevIndex := next - 1
	prevTerm, err := n.storage.TermAt(prevIndex)
	if err != nil {
		n.logger.Errorf("replicateOne(%s): TermAt(%d): %v", peer, prevIndex, err)
		return
	}

	last := n.storage.LastIndex()
	hi := next + maxEntriesPerAppend
	if hi > last+1 {
		hi = last + 1
	}
	entries, err := n.storage.Entries(next, hi)
	if err != nil {
		n.logger.Errorf("replicateOne(%s): Entries(%d,%d): %v", peer, next, hi, err)
		return
	}

	term := n.currentTerm
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
		defer cancel()
		reply, err := n.transport.SendAppendEntries(ctx, peer, args)
		if err != nil {
			return
		}
		select {
		case n.appendResultCh <- appendResult{
			term:         term,
			peer:         peer,
			prevLogIndex: prevIndex,
			numEntries:   len(entries),
			reply:        reply,
		}:
		case <-n.stopCh:
		}
	}()
}

// handleAppendResult processes an AppendEntries reply gathered by
// replicateOne, updating nextIndex/matchIndex and advancing commitIndex on
// success, or backing nextIndex off and retrying immediately on a
// consistency-check rejection.
func (n *Node) handleAppendResult(res appendResult) {
	if res.reply.Term > n.currentTerm {
		n.stepDown(res.reply.Term)
		return
	}
	if n.state != Leader || res.term != n.currentTerm {
		return // stale reply from a round this node has moved past
	}

	if res.reply.Success {
		matched := res.prevLogIndex + uint64(res.numEntries)
		if matched > n.matchIndex[res.peer] {
			n.matchIndex[res.peer] = matched
		}
		if matched+1 > n.nextIndex[res.peer] {
			n.nextIndex[res.peer] = matched + 1
		}
		n.advanceCommitIndex()
		return
	}

	// Consistency check failed: back off by one and retry immediately
	// (rather than waiting for the next heartbeat tick), clamped so
	// nextIndex never goes below 1.
	if n.nextIndex[res.peer] > 1 {
		n.nextIndex[res.peer]--
	}
	n.replicateOne(res.peer)
}

// sendInstallSnapshot sends peer the leader's current snapshot, for when
// nextIndex[peer] has fallen behind FirstIndex() (the entries it needs
// have already been compacted out of the log). Only ever called from
// within Run's runloop; the actual RPC happens in a spawned goroutine.
func (n *Node) sendInstallSnapshot(peer NodeID) {
	data, lastIndex, lastTerm, err := n.storage.LoadSnapshot()
	if err != nil {
		n.logger.Errorf("sendInstallSnapshot(%s): LoadSnapshot: %v", peer, err)
		return
	}
	if lastIndex == 0 {
		// nextIndex[peer] < FirstIndex() but there's no snapshot to send —
		// shouldn't happen (FirstIndex()==1 with no snapshot means nothing
		// is compacted, so the < FirstIndex() check in replicateOne
		// couldn't have triggered), but don't send a nonsense RPC either.
		n.logger.Errorf("sendInstallSnapshot(%s): peer needs a snapshot but none exists", peer)
		return
	}

	term := n.currentTerm
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              data,
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), n.rpcTimeout)
		defer cancel()
		reply, err := n.transport.SendInstallSnapshot(ctx, peer, args)
		if err != nil {
			return
		}
		select {
		case n.installSnapshotResultCh <- installSnapshotResult{
			term:      term,
			peer:      peer,
			lastIndex: lastIndex,
			reply:     reply,
		}:
		case <-n.stopCh:
		}
	}()
}

// handleInstallSnapshotResult processes an InstallSnapshot reply gathered
// by sendInstallSnapshot, advancing nextIndex/matchIndex past the
// installed snapshot boundary on success.
func (n *Node) handleInstallSnapshotResult(res installSnapshotResult) {
	if res.reply.Term > n.currentTerm {
		n.stepDown(res.reply.Term)
		return
	}
	if n.state != Leader || res.term != n.currentTerm {
		return // stale reply from a round this node has moved past
	}

	if res.lastIndex > n.matchIndex[res.peer] {
		n.matchIndex[res.peer] = res.lastIndex
	}
	if res.lastIndex+1 > n.nextIndex[res.peer] {
		n.nextIndex[res.peer] = res.lastIndex + 1
	}
	n.advanceCommitIndex()
}

// advanceCommitIndex checks whether a new log index has been replicated to
// a majority and can become the new commitIndex. Per Raft's Figure 8 safety
// rule, a leader may only commit an entry from its own current term by
// counting replicas directly — an entry from an earlier term is committed
// only indirectly, as a side effect of a later current-term entry
// committing (which this same majority-index computation naturally
// achieves, since commitIndex only ever moves forward across all entries up
// to and including the new one).
func (n *Node) advanceCommitIndex() {
	if n.state != Leader {
		return
	}

	match := make([]uint64, 0, len(n.peers)+1)
	match = append(match, n.storage.LastIndex()) // the leader's own log
	for _, peer := range n.peers {
		match = append(match, n.matchIndex[peer])
	}
	sort.Slice(match, func(i, j int) bool { return match[i] > match[j] })

	candidate := match[n.quorumSize()-1]
	if candidate <= n.commitIndex {
		return
	}

	term, err := n.storage.TermAt(candidate)
	if err != nil || term != n.currentTerm {
		return // only commit entries from the current term directly
	}

	n.commitIndex = candidate
	n.applyCommitted()
}

// applyCommitted applies every entry with lastApplied < index <=
// commitIndex to the state machine, in order, and resolves any pending
// Propose call waiting on that index. Runs synchronously inside the
// runloop: this project's KVStore is an in-memory map, so Apply is fast
// enough that a separate apply goroutine (and the cross-goroutine
// synchronization it would need for pendingProposals/lastApplied) isn't
// worth the complexity. If a slower state machine is ever introduced, this
// is the place that would need to move to a worker goroutine feeding
// results back through a channel rather than mutating Node fields directly.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, err := n.storage.EntryAt(n.lastApplied)
		if err != nil {
			n.logger.Errorf("failed to load entry %d for apply: %v", n.lastApplied, err)
			fatalExit()
			return
		}

		var result interface{}
		if entry.Type == EntryNormal {
			result = n.sm.Apply(entry)
		}

		if ch, ok := n.pendingProposals[entry.Index]; ok {
			delete(n.pendingProposals, entry.Index)
			ch <- proposalResult{value: result}
		}

		if n.snapshotInterval > 0 && n.lastApplied%n.snapshotInterval == 0 {
			n.maybeSnapshot()
		}
	}
}

// maybeSnapshot serializes the state machine and hands it to Storage to
// persist as a snapshot covering everything up to lastApplied, compacting
// the log in the process. Failures here are logged, not fatal: a missed
// snapshot just means the log stays a bit larger for now, not a safety
// violation — unlike a hard-state persist failure, there's no risk of an
// unsafe in-memory/on-disk divergence from skipping it.
func (n *Node) maybeSnapshot() {
	snap, err := n.sm.Snapshot()
	if err != nil {
		n.logger.Errorf("snapshot failed: Snapshot: %v", err)
		return
	}
	term, err := n.storage.TermAt(n.lastApplied)
	if err != nil {
		n.logger.Errorf("snapshot failed: TermAt(%d): %v", n.lastApplied, err)
		return
	}
	if err := n.storage.SaveSnapshot(snap, n.lastApplied, term); err != nil {
		n.logger.Errorf("snapshot failed: SaveSnapshot: %v", err)
		return
	}
	n.logger.Infof("snapshot taken at index=%d term=%d", n.lastApplied, term)
}

// handlePropose processes a client proposal dequeued from proposeCh:
// rejects it outright if this node isn't the leader, otherwise appends it
// to the local log, registers it to be resolved once applied, and
// replicates it to followers immediately.
func (n *Node) handlePropose(p proposal) {
	if n.state != Leader {
		p.result <- proposalResult{err: ErrNotLeader}
		return
	}

	entry := LogEntry{
		Index: n.storage.LastIndex() + 1,
		Term:  n.currentTerm,
		Type:  EntryNormal,
		Data:  p.data,
	}
	if err := n.storage.AppendEntries([]LogEntry{entry}); err != nil {
		n.logger.Errorf("leader failed to persist proposed entry: %v", err)
		p.result <- proposalResult{err: err}
		return
	}

	n.pendingProposals[entry.Index] = p.result
	n.replicateAll()
	n.advanceCommitIndex() // handles the single-node-cluster case immediately
}
