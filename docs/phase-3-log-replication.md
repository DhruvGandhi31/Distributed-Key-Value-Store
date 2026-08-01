# Phase 3 — Log Replication

## Goal

Writes on the leader are replicated to all followers and only acknowledged to the client once a majority has durably persisted them. Followers that lag or diverge get corrected automatically. This is where the KV store stops being "one node that happens to also run an election" and becomes an actual replicated state machine.

## What was built

```
internal/raft/
  storage.go            — Storage interface extended with log-entry methods
  file_storage.go         — FileStorage's log persistence (from Phase 2's extension)
  node.go                  — nextIndex/matchIndex, proposeCh, pendingProposals, Propose(), extended Status
  election.go               — real AppendEntries consistency check, becomeLeader init, stepDown cleanup
  replication.go             — replicateOne/replicateAll, handleAppendResult, advanceCommitIndex, applyCommitted, handlePropose
  replication_test.go         — replication-specific unit tests
internal/server/server.go   — writes routed through node.Propose(); GET /cluster/status
cmd/server/main.go          — server.New(kv, node) instead of server.New(kv)
```

## Design

### Unifying heartbeats and replication

Phase 2 had a separate `broadcastHeartbeat()` that always sent empty `AppendEntries`. Phase 3 replaces it with `replicateOne(peer)`: it always sends whatever `[nextIndex[peer], nextIndex[peer]+256)` currently holds, which is naturally empty when a follower is caught up — so a heartbeat is just a replication RPC with no new entries, exactly matching `AppendEntriesArgs`'s existing doc comment ("also serves as the heartbeat when Entries is empty"). `tick()`'s Leader branch, `becomeLeader()`, and — new this phase — `handlePropose()` all call the same `replicateAll()`, so a write's replication doesn't wait for the next heartbeat tick.

### The consistency check, for real this time

Phase 2's `handleAppendEntries` was a stub (no log existed yet). Phase 3 implements the actual Raft §5.3 check:

```go
if args.PrevLogIndex > 0 {
    if args.PrevLogIndex > n.storage.LastIndex() {
        return AppendEntriesReply{Term: n.currentTerm, Success: false}
    }
    prevTerm, err := n.storage.TermAt(args.PrevLogIndex)
    if err != nil || prevTerm != args.PrevLogTerm {
        return AppendEntriesReply{Term: n.currentTerm, Success: false}
    }
}
```

`TermAt(0)` returns `(0, nil)` as a sentinel, so `PrevLogIndex == 0` always matches — this is what lets a brand-new follower with an empty log accept the very first entries.

`appendNewEntries` then reconciles the follower's log with what the leader sent: entries already present with a matching term are left alone (so a duplicate or delayed RPC never discards something that might already be committed), the first mismatch truncates everything from that index onward via `Storage.TruncateAfter`, and any genuinely new entries get appended. This is the standard "find first divergence, cut there, append the rest" algorithm — nothing exotic, just careful about not truncating on a no-op retransmit.

### `nextIndex`/`matchIndex` and the backoff loop

On becoming leader, `nextIndex[peer]` is seeded to `lastIndex+1` for every peer (optimistic: assume everyone's caught up) and `matchIndex[peer]` to `0` (pessimistic: prove it). `handleAppendResult` does the bookkeeping:

- **Success**: `matchIndex[peer]` advances to `prevLogIndex + numEntries` sent in *that specific request* (not whatever `nextIndex[peer]` happens to be by the time the reply lands — `appendResult` carries its own `prevLogIndex`/`numEntries` for exactly this reason, since replies can arrive out of order or after `nextIndex` has already moved). Then `advanceCommitIndex()` runs.
- **Rejection**: `nextIndex[peer]` decrements by one, clamped to a minimum of `1` (the invariant CLAUDE.md calls out explicitly), and `replicateOne(peer)` fires again immediately rather than waiting for the next heartbeat tick. This is the guide's plain decrement-by-one backoff, not the `ConflictIndex`/`ConflictTerm` fast-backoff optimization — those fields stay reserved-but-unused; decrement-by-one is fully correct (just slower for a badly-lagging follower), and the cluster sizes this project deals with don't need the optimization yet.

### The Figure 8 rule — the invariant most implementations get wrong

This is the one that isn't in the Raft paper's headline pseudocode but absolutely is in the correctness proof: **a leader may only commit a log entry from its own current term by counting replicas directly.** An entry from an earlier term — even one a majority of the cluster already has on disk — must never be committed just because it hit a majority. It becomes safe only as a side effect of a *later, current-term* entry committing.

```go
func (n *Node) advanceCommitIndex() {
    ...
    candidate := match[n.quorumSize()-1]   // sorted matchIndex, majority-th value
    if candidate <= n.commitIndex { return }

    term, err := n.storage.TermAt(candidate)
    if err != nil || term != n.currentTerm {
        return // refuse to commit an entry from an earlier term directly
    }
    n.commitIndex = candidate
    n.applyCommitted()
}
```

Why this matters: without it, a narrow window exists where a leader could commit an entry that a *differently-elected* future leader would be forced to overwrite, violating Leader Completeness (a committed entry must appear in every future leader's log). `TestReplication_LeaderOnlyCommitsCurrentTermEntriesDirectly` reproduces the exact scenario — an inherited prior-term entry sitting at majority `matchIndex` with `commitIndex` correctly refusing to move — then shows it becomes safe (and gets applied) the moment a current-term entry also reaches majority. This invariant is now listed in CLAUDE.md alongside the others from Phase 2, since it's just as load-bearing and easy to silently skip.

### `Propose`, and why apply happens inline

`Propose(ctx, data) (interface{}, error)` — note the return type matches `StateMachine.Apply`'s `interface{}`, not just an `error` — hands a proposal to the runloop over `proposeCh` and blocks on a per-call result channel. Inside the runloop, `handlePropose` rejects immediately with `ErrNotLeader` if this node isn't leader, otherwise appends the entry to local storage, registers `pendingProposals[index] = resultChan`, and calls `replicateAll()`.

The guide's Step 3.4 sketches a **separate** `applyLoop()` goroutine watching `commitIndex`. This project deliberately does not do that — `applyCommitted()` runs synchronously inside `Run()`'s single runloop, right after `commitIndex` advances (from either `advanceCommitIndex` on the leader or the `LeaderCommit` clamp on a follower). Reasoning: `KVStore.Apply` is an in-memory map operation with no I/O, so there's no latency benefit to a separate goroutine, and running it inline means `pendingProposals` and `lastApplied` are *never* touched by more than one goroutine — a stricter, simpler reading of "single runloop owns all Raft state" than introducing a second mutation path that would need its own channel-based handoff back into the runloop. If a slower state machine (one doing real I/O per `Apply`) is ever introduced, this is the one place that would need to move to a worker goroutine feeding results back through a channel — flagged in the code as exactly that.

A proposal that hasn't been applied yet when this node steps down (discovers a higher term, or loses an `AppendEntries` race) is failed with `ErrNotLeader` — `stepDown()` calls `failPendingProposals`, since an entry proposed under this node's leadership is no longer guaranteed to ever commit once someone else is leader. The same cleanup runs on `Stop()`, so an HTTP handler blocked in `Propose` isn't left hanging until its request context times out.

### HTTP wiring

`internal/server.Server` now takes a `RaftNode` (an interface — just `Propose` and `Status` — not a concrete `*raft.Node`, so the server package doesn't need to import the whole Raft implementation to depend on it). `New(kv, nil)` preserves Phase 1 behavior exactly (writes go straight to `kv.Apply`); `New(kv, node)` routes `PUT`/`DELETE` through `node.Propose` instead. Any `Propose` error — not leader, shutting down, canceled — currently surfaces as a flat `503 Service Unavailable`; Phase 6 replaces this with real leader-redirect logic (the client retrying against whoever `/cluster/status` says the leader is).

`GET /cluster/status` returns `{state, leader, term, commit_index, last_applied}`, built from a new `Status` field set (`CommitIndex`, `LastApplied` added to what Phase 2 already exposed). This is what the guide's Phase 3 checkpoint curls directly.

## Verified behavior (checkpoint)

Ran a real 3-node cluster via `scripts/cluster.ps1`:

```
kvctl -addr http://127.0.0.1:8001 put hello world      → OK   (node1 is leader)
kvctl -addr http://127.0.0.1:8001 get hello              → world

GET http://127.0.0.1:8001/cluster/status   → {"state":"Leader",...,"commit_index":1,"last_applied":1}
GET http://127.0.0.1:8002/cluster/status   → {"state":"Follower",...,"commit_index":1,"last_applied":1}
GET http://127.0.0.1:8003/cluster/status   → {"state":"Follower",...,"commit_index":1,"last_applied":1}
```

All three nodes agree. Then the actual Phase 3 checkpoint — kill the leader mid-cluster and confirm committed data survives:

```
Stop-Process -Id (node1's PID) -Force
# node2 becomes leader at a higher term (re-election, same as Phase 2)

kvctl -addr http://127.0.0.1:8002 get hello   → world    (survived)
kvctl -addr http://127.0.0.1:8002 get key2    → value2   (survived)
kvctl -addr http://127.0.0.1:8002 put key3 value3   → OK  (new leader accepts writes)

GET http://127.0.0.1:8002/cluster/status   → {"state":"Leader",...,"commit_index":3,...}
GET http://127.0.0.1:8003/cluster/status   → {"state":"Follower",...,"commit_index":3,...}
```

No committed data was lost, and the surviving follower converged to the same `commit_index` as the new leader.

Unit tests (`internal/raft/replication_test.go`) cover: `Propose` committing and applying on every node, `Propose` rejecting on a non-leader, the consistency check rejecting on both index-out-of-range and term-mismatch, divergent-entry truncation, follower `commitIndex` advancement clamped to `LeaderCommit`, the Figure 8 current-term-only commit rule, and pending proposals failing on step-down. All pass with `-race` (`CGO_ENABLED=1 GOARCH=amd64`, same environment note as Phase 2).

## Known limitations (expected — later phases address these)

- **No snapshotting** — the in-memory + on-disk log grows unboundedly; `FirstIndex()` is hardcoded to `1` (Phase 5).
- **No leader redirection** — a write against a follower gets a flat `503`, not a redirect to the actual leader (Phase 6).
- **No fast conflict backoff** — `nextIndex` backs off one entry per rejected RPC; `ConflictIndex`/`ConflictTerm` are reserved in the RPC struct but unused (fine at this project's scale, would matter for a badly-lagging follower in a larger cluster).
- **Reads are not linearizable** — `GET` reads the local `KVStore` directly; a partitioned-off former leader could still serve stale reads to a client that doesn't know it's stale. Read-index/lease reads are Phase 10.
- **Crash recovery is not yet a hardened, tested checkpoint of its own** — `FileStorage` already persists and reloads the log correctly (see `TestFileStorage_AppendAndReloadRoundTrips`/`TruncateAfterThenAppendPersists`), but Phase 4 is where "kill all nodes, restart, verify nothing lost" becomes the explicit, dedicated checkpoint.

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test ./...

.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status
# note the leader, e.g. node1 at term N

.\bin\kvctl.exe -addr http://127.0.0.1:8001 put hello world
.\bin\kvctl.exe -addr http://127.0.0.1:8001 get hello

# check replication across all three nodes:
Invoke-WebRequest http://127.0.0.1:8001/cluster/status -UseBasicParsing | Select -Expand Content
Invoke-WebRequest http://127.0.0.1:8002/cluster/status -UseBasicParsing | Select -Expand Content
Invoke-WebRequest http://127.0.0.1:8003/cluster/status -UseBasicParsing | Select -Expand Content

# kill the leader, confirm data survives and the new leader accepts writes:
Stop-Process -Id (Get-Content .\data\node1.pid) -Force
.\scripts\cluster.ps1 -Action status
.\bin\kvctl.exe -addr http://127.0.0.1:8002 get hello

.\scripts\cluster.ps1 -Action stop
```
