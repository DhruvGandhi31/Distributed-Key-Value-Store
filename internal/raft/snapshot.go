package raft

// installSnapshot processes an incoming InstallSnapshot RPC. Only ever
// called from within Run's runloop.
//
// Unlike the real Raft paper's partial-log-retention optimization (keep
// any suffix that already matches past LastIncludedIndex), this always
// hands off to Storage.SaveSnapshot, which only ever discards entries <=
// lastIndex and leaves anything after untouched. If this follower had
// stale entries beyond LastIncludedIndex from an old, abandoned leader,
// they aren't explicitly wiped here — they self-correct via the normal
// AppendEntries consistency check (mismatched term -> TruncateAfter) on
// the very next replication round from the real leader. Reusing that
// already-tested machinery instead of adding a second, separate
// log-replacement path is a deliberate simplification.
func (n *Node) installSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	if args.Term < n.currentTerm {
		return InstallSnapshotReply{Term: n.currentTerm}
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	n.state = Follower
	n.leader = args.LeaderID
	n.resetElectionTimer()

	if args.LastIncludedIndex <= n.lastApplied {
		// Stale or duplicate snapshot — we're already at least this far
		// along. Nothing to do, just ack at the current term.
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	if err := n.storage.SaveSnapshot(args.Data, args.LastIncludedIndex, args.LastIncludedTerm); err != nil {
		n.logger.Errorf("failed to persist installed snapshot: %v", err)
		return InstallSnapshotReply{Term: n.currentTerm} // non-fatal: leader will retry
	}
	if err := n.sm.Restore(args.Data); err != nil {
		n.logger.Errorf("failed to restore state machine from installed snapshot: %v", err)
		return InstallSnapshotReply{Term: n.currentTerm}
	}

	n.commitIndex = args.LastIncludedIndex
	n.lastApplied = args.LastIncludedIndex

	return InstallSnapshotReply{Term: n.currentTerm}
}
