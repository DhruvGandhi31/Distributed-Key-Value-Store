# Phase 4 — Persistent Storage (WAL + Crash Recovery)

## Goal

Data survives crashes. On restart, a node recovers its term, vote, and log from disk and rejoins the cluster — and critically, previously-committed data actually becomes visible again, not just present-but-inert on disk.

## Why this phase was mostly a bug hunt, not new plumbing

By the time Phase 3 finished, `FileStorage` already persisted both hard state (`term`/`votedFor`) and the log itself, both fsync'd, both crash-safe against a torn trailing write (see Phase 2/3 docs — the guide's Step 4.1 WAL format landed early because Phase 2's election needed `votedFor` durability and Phase 3's replication needed real log durability). So the honest question for Phase 4 wasn't "how do I persist the log" — it was already done — but **"does a full cluster restart actually work end-to-end?"** It didn't, quite.

## The gap: commitIndex has nowhere to come from after everyone restarts

`commitIndex` and `lastApplied` are correctly volatile — the Raft paper never persists them, and this codebase doesn't either (`internal/raft/node.go`'s `NewNode` always starts both at `0`). That's fine when *some* node in the cluster never crashed: a surviving leader (or a follower being promoted, provided it wasn't the one that restarted) already knows the real `commitIndex` and pushes it to everyone else via `LeaderCommit` on its next heartbeat.

But Phase 4's actual checkpoint is "kill **all** nodes, restart **all** of them" — every node's `commitIndex` resets to `0` simultaneously. The entries are still safely on every node's disk (log persistence works fine), but per the Figure 8 safety rule already implemented in Phase 3, **a leader can only advance `commitIndex` by directly counting majority replicas of an entry from its own current term** — never an inherited entry from a prior term, no matter how many replicas already have it. Without a fresh current-term entry to anchor that computation, a newly-elected leader with no immediate client write would sit forever with `commitIndex = 0`, even though the data is right there on disk. `GET` reads the in-memory `KVStore` directly, which starts empty on every restart — so the write would appear lost, even though nothing was actually lost.

### The fix

`becomeLeader()` now appends a blank no-op entry (`EntryNoOp`, already defined back in Phase 1's `statemachine.go` but never used until now) for its own term immediately upon election, before the first heartbeat goes out:

```go
noop := LogEntry{Index: last + 1, Term: n.currentTerm, Type: EntryNoOp}
if err := n.storage.AppendEntries([]LogEntry{noop}); err != nil {
    n.logger.Errorf("failed to append no-op entry on election: %v", err)
    fatalExit()
    return
}
n.heartbeatElapsed = 0
n.replicateAll()
n.advanceCommitIndex() // covers the single-node-cluster (no peers) case
```

Once that no-op replicates to a majority, `advanceCommitIndex`'s existing majority-match computation (unchanged from Phase 3) naturally re-establishes `commitIndex` up through it — which, because `commitIndex` only ever moves forward across *all* entries up to and including the new one, also correctly re-commits everything that was committed before the restart. This is the standard, well-known fix (etcd/raft does the same thing; the Raft dissertation describes it in §3.6.1, "Committing entries from previous terms") — not a project-specific workaround.

The trailing `advanceCommitIndex()` call matters for a subtle reason: for a single-node cluster (no peers), there's no async `AppendEntries` reply to trigger commit advancement, since `replicateAll()` has nothing to send. Without this explicit call, a lone node would append its own no-op and then just... never commit it.

### A test that would have failed before the fix

`TestRecovery_NoOpOnElectionReestablishesCommitIndex` (new, `internal/raft/recovery_test.go`) constructs the exact failure scenario: three nodes, each pre-seeded with the same committed entry (simulating "this was committed before the crash"), each starting with `commitIndex`/`lastApplied` at zero (simulating "volatile state lost in the restart"), and — critically — **no client ever calls `Propose`**. The only thing that can make the pre-existing entry visible again is the no-op mechanism. Traced through the code without the fix: `handleAppendResult` would still call `advanceCommitIndex` on every successful heartbeat reply, matchIndex would still reach the old entry's index on a majority, but `TermAt(candidate) != currentTerm` (the entry is from term 1, the new leader is term 2) would correctly refuse to commit it — confirming this is a real gap the test catches, not a hypothetical one.

### A side effect on an existing test

`TestElection_HigherTermStepsDownLeader` started failing once leaders actually had log entries. It sent a synthetic "outsider" `RequestVote` with `LastLogIndex`/`LastLogTerm` left at their zero value, relying on the leader's log *also* being empty for the up-to-date check to trivially pass. That was never really testing the log-comparison logic — it happened to route around it. Now that a real leader always has at least the no-op entry, Raft's election-restriction safety property (a candidate can't win a vote if its log isn't at least as up-to-date, regardless of term) correctly rejects that unrealistic candidate. Fixed by having the test read the leader's actual `storage.LastIndex()`/`LastTerm()` (it's an in-package test) instead of assuming zero. This is the safety property working as intended, not a regression.

### Hardening: a dedicated torn-write recovery test

`FileStorage.loadLog` already treated a truncated trailing record as "end of log, not an error" since Phase 2/3 — necessary because a real crash can happen mid-`fsync`, leaving a length prefix on disk with fewer (or zero) of the promised payload bytes following it. This was implemented but never had a test exercising it directly. `TestFileStorage_RecoversFromTornTrailingWrite` (new) writes two valid entries, then manually appends a length prefix claiming 100 bytes followed by only 3 garbage bytes (simulating exactly that crash), and confirms `NewFileStorage` recovers the two valid entries cleanly, doesn't error, and remains writable afterward.

## Verified behavior (checkpoint)

Ran the actual guide's Phase 4 checkpoint against a real 3-node cluster:

```
kvctl -addr http://127.0.0.1:8002 put persistent key
# commit_index: 5 on all three nodes (includes a few no-op entries from
# startup election churn), term: 6

scripts/cluster.ps1 -Action stop      # kill ALL three nodes, not just the leader
scripts/cluster.ps1 -Action start     # restart ALL three from disk

scripts/cluster.ps1 -Action status
# node1  leader at term 7    (higher than before — not reset to 0)

kvctl -addr http://127.0.0.1:8001 get persistent
# → key                       (survived, with ZERO writes since restart)

GET /cluster/status on all 3 nodes → commit_index: 6, last_applied: 6, matching
```

The `commit_index` bump from 5 to 6 across the restart, with no client write in between, is the no-op entry from the new election doing exactly what it's supposed to.

## Known limitations (expected — later phases address these)

- **The log still grows unboundedly** — `FirstIndex()` is hardcoded to `1`; there's no compaction. A very long-lived cluster with heavy write volume will eventually have a large `log` file and a slow `NewFileStorage` scan on startup. Phase 5.
- **A very slow-to-restart node doesn't get a fast catch-up** — it'll be corrected via the normal one-entry-at-a-time `nextIndex` backoff from Phase 3, not an `InstallSnapshot` fast path (doesn't exist until Phase 5).
- **No corruption detection beyond "torn trailing write"** — a bit-flip in the *middle* of an otherwise well-formed log file (not just a torn tail) would currently decode as a wrong-but-valid-looking entry rather than being detected and rejected. Not addressed by this project (no checksums per entry); noting it as a known gap rather than silently assuming it's covered.

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test ./...

.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status
# note the leader, e.g. node2 at term N

.\bin\kvctl.exe -addr http://127.0.0.1:8002 put persistent key
Invoke-WebRequest http://127.0.0.1:8001/cluster/status -UseBasicParsing | Select -Expand Content

# kill EVERY node, not just the leader:
.\scripts\cluster.ps1 -Action stop

# restart the whole cluster from disk:
.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status
# term should be higher than before, not reset to 0

# no write happens here — this is reading purely from what survived the restart:
.\bin\kvctl.exe -addr http://127.0.0.1:8001 get persistent
# → key

.\scripts\cluster.ps1 -Action stop
```
