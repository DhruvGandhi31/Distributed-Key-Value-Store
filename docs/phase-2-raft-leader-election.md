# Phase 2 — Raft Core: Leader Election

## Goal

Give every `kvstored` process a real Raft identity: nodes exchange `RequestVote`/`AppendEntries` RPCs over HTTP+gob, elect a leader, send heartbeats, and re-elect within the election timeout if the leader dies. No log replication yet — entries are always empty this phase, and client writes still apply directly to the local `KVStore`, bypassing Raft entirely. That's Phase 3.

## What was built

```
internal/raft/
  types.go            — NodeID, State, RequestVote/AppendEntries RPC structs, ErrShutdown
  storage.go           — Storage interface (Phase 2 hard-state subset)
  file_storage.go       — FileStorage: fsync'd, atomically-renamed hard-state persistence
  file_storage_test.go  — round-trip persistence tests
  logger.go             — Logger interface + stdlib-backed default
  transport.go           — Transport interface (SendRequestVote/SendAppendEntries)
  node.go                — Config, Node, NewNode, Run() runloop, channel-based entry points
  election.go             — all election/heartbeat state-machine logic
  election_test.go        — fakeTransport-based unit tests
internal/transport/
  server.go            — RaftHandler: POST /raft/vote, POST /raft/append
  http.go               — Client: gob-over-HTTP implementation of raft.Transport
cmd/server/main.go     — --id/--raft-addr/--peers/--data-dir; Raft mode when --id is set
scripts/cluster.ps1    — 3-node local cluster manager for manual verification
```

## Design

### `internal/raft` — the runloop

All Raft state (`currentTerm`, `votedFor`, `state`, `leader`, `commitIndex`, `lastApplied`, timers) lives on the `Node` struct and is touched **only** from inside `Node.Run()`, a single goroutine. Every external interaction — an incoming RPC, a status query, RPC replies coming back from peers — goes through a typed channel and is processed one at a time by the runloop's `select`. This is the project's core invariant (CLAUDE.md: "single-runloop ownership") and it's why there's no mutex anywhere in `election.go`.

```go
select {
case <-ticker.C:            n.tick()
case msg := <-n.requestVoteCh:   msg.reply <- n.handleRequestVote(msg.args)
case msg := <-n.appendEntriesCh: msg.reply <- n.handleAppendEntries(msg.args)
case res := <-n.voteResultCh:    n.handleVoteResult(res)
case res := <-n.appendResultCh:  n.handleAppendResult(res)
case replyCh := <-n.queryCh:     replyCh <- n.status()
case <-n.stopCh:                 return
}
```

`HandleRequestVote`/`HandleAppendEntries` (the exported, external-facing methods called by the HTTP layer) just package the request into a struct with a buffered reply channel, hand it to the runloop, and block on the reply — with `ctx.Done()`/`stopCh` as escape hatches so a canceled request or a shutting-down node never leaks a goroutine.

### Election timer semantics — narrower than the Raft paper

Per CLAUDE.md's explicit invariant, the election timer resets **only** on a valid (same-or-higher-term, non-rejected) `AppendEntries` — not on granting a vote, and not merely as a side effect of stepping down on a higher term seen in an RPC reply. This is stricter than the vanilla Raft paper (which also resets on vote-granting). `TestElection_NoTimerResetOnVoteGrant` and `TestElection_NoTimerResetOnRejectedAppendEntries` pin this down explicitly so a future refactor doesn't accidentally "fix" it back to the textbook behavior.

### `Storage` interface — deliberately narrowed for this phase

The guide formally introduces the full `Storage` interface (log entries, snapshots, everything) in Phase 3. Since Phase 2's election logic already needs to persist hard state, `storage.go` defines a 3-method subset now:

```go
type Storage interface {
    SaveHardState(term uint64, votedFor NodeID, snapshotIndex, snapshotTerm uint64) error
    LastIndex() uint64
    LastTerm() uint64
}
```

`SaveHardState`'s 4-argument signature already matches what Phase 5 (snapshotting) will need, so extending this interface later means adding methods, not changing this one's shape. `FileStorage.LastIndex()`/`LastTerm()` unconditionally return `0` this phase — there's no log yet, so every node looks equally "up to date" during vote requests, which is correct since nothing has appended anything.

`FileStorage.SaveHardState` writes to a `.tmp` file, calls `f.Sync()` (fsync) before closing, then `os.Rename`s it into place — a crash mid-write can never leave a torn hard-state file, and the fsync happens strictly before the function returns, which is what lets the invariant "fsync before acking" hold: `startElection()` and `handleRequestVote()`'s vote-grant path both call `SaveHardState` and only proceed (send RequestVote RPCs / reply `VoteGranted: true`) after it returns successfully.

### Failure handling on persistence errors — a deliberate asymmetry

- `startElection()` and `stepDown()` treat a `SaveHardState` error as **fatal** (`os.Exit(1)`). Both are changing the node's term/vote; continuing with an in-memory-only change risks a double-vote or term violation if the process crashes right after, which is exactly the scenario the fsync invariant exists to prevent — better to crash loudly now than corrupt an invariant silently.
- `handleRequestVote()` treats a `SaveHardState` error as **non-fatal**: it just declines the vote (`VoteGranted: false`) and logs the error. A voter that can't currently persist safely chooses not to vote rather than crashing — the candidate will simply not get this vote and can retry with another peer or a later term.

### Stale RPC replies are explicitly discarded

Vote/heartbeat RPCs are fired from short-lived goroutines (one per peer, per election/heartbeat round) and their replies land on `voteResultCh`/`appendResultCh` asynchronously — by the time a reply arrives, the node may have moved to a new term or lost candidacy. `handleVoteResult` checks both `res.reply.Term > currentTerm` (step down) **and** `state == Candidate && res.term == currentTerm` (else discard) before counting a vote. `TestElection_StaleVoteReplyIgnored` reproduces this directly: start an election, force the node past that round via `stepDown`, then feed back a vote reply from the abandoned round and assert it's ignored.

### `internal/transport` — HTTP+gob wire format

`RaftHandler` exposes `POST /raft/vote` and `POST /raft/append`, gob-decoding the request body into the RPC arg struct and gob-encoding the reply. `Client` implements `raft.Transport` the same way in reverse. One deliberate correctness point: a non-`200` HTTP response is surfaced as a Go `error`, never gob-decoded as if it were a valid reply — decoding a non-200 body would silently produce a zero-value `{Term:0, VoteGranted:false}`, which looks exactly like a legitimate "no" vote instead of a transport failure, and would be a very hard bug to track down later.

### `cmd/server/main.go` — two independent HTTP listeners

When `--id` is empty, `kvstored` runs exactly as it did in Phase 1 (single client HTTP server, no Raft) — that code path is untouched. When `--id` is set, the node starts **two** separate `http.Server`s: one for `--raft-addr` (peer RPCs) and one for `--client-addr` (KV API), each in its own goroutine. Keeping them on separate listeners means client request load can never delay a heartbeat or vote reply. `node.Run()` runs in a third goroutine; `SIGINT`/`SIGTERM` trigger `node.Stop()` and a graceful `Shutdown(ctx)` of both servers.

**Client writes still bypass Raft in this phase** — `internal/server`'s `PUT`/`DELETE` handlers call `kv.Apply()` directly, exactly as in Phase 1. There's a comment at the call site in `main.go` making this explicit so it isn't mistaken for replicated writes. Replication through the Raft log is Phase 3.

### `scripts/cluster.ps1`

A dev-only 3-node manager (`-Action start|stop|status`) that launches `bin/kvstored.exe` three times with distinct ports/data dirs and a shared `--peers` list, tracks PIDs for teardown, and greps each node's log for `"became Leader term="` to report cluster status. **Known limitation**: `status` reports the *last* term a node logged becoming leader, not its current live state — after killing a former leader, its stale "leader at term N" line remains in its log (though `Running` will correctly show `False`). Good enough for manual verification; a real status endpoint (`GET /cluster/status` via `node.Status(ctx)`) would fix this and is a natural, cheap addition for Phase 3.

## Verified behavior (checkpoint)

Ran the actual 3-node cluster via `scripts/cluster.ps1`:

```
.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status
# node2  leader at term 2

Stop-Process -Id (Get-Content .\data\node2.pid) -Force

.\scripts\cluster.ps1 -Action status
# node1  leader at term 3   (re-election confirmed)

.\scripts\cluster.ps1 -Action stop
```

Unit tests (`go test -race ./...`, run with `CGO_ENABLED=1 GOARCH=amd64` — see note below) pass, covering: single-candidate election convergence, step-down on higher term, no timer reset on rejected `AppendEntries`, no timer reset on vote grant, stale-term vote rejection, no double-vote in the same term, and stale vote-reply discarding.

**Environment note**: this machine's default `go env GOARCH` is `386`, and Go's race detector isn't supported on `windows/386`. Running `go test -race` requires `GOARCH=amd64` (with `CGO_ENABLED=1`; MinGW gcc is already on `PATH`). Plain `go test ./...` works fine on the default toolchain — this only affects `-race`. Worth keeping in mind if CI's `go-version`/arch pinning ever needs adjusting once Phase 7 stands up GitHub Actions test matrices for this package.

## Known limitations (expected — later phases address these)

- **No log replication** — `AppendEntries` always carries empty `Entries`; `LastIndex()`/`LastTerm()` are hardcoded to `0` (Phase 3).
- **Client writes are not replicated** — `PUT`/`DELETE` still hit the local `KVStore` directly, bypassing consensus entirely (Phase 3).
- **No crash recovery beyond hard state** — only `term`/`votedFor` survive a restart; there's no log file yet, so a restarted node starts with an empty (correct, since none exists) log (Phase 4).
- **`cluster.ps1 status` shows historical, not live, leader state** — see note above.
- **No leader redirection in the client** — `kvctl`/`internal/server` don't know or care who the Raft leader is yet (Phase 6).

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test ./...

.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status
# note the leader, e.g. node2 at term N

Stop-Process -Id (Get-Content .\data\node2.pid) -Force
# wait a few seconds for re-election

.\scripts\cluster.ps1 -Action status
# a different node should now be leader at term N+1 or higher

.\scripts\cluster.ps1 -Action stop
```
