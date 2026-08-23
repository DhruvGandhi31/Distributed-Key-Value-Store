# Phase 7 — Testing and CI

## Goal

Comprehensive test coverage that catches regressions before they reach production — unit tests for the pieces that had none, and real integration tests that exercise the actual code path a deployed node runs (real HTTP+gob transport, real on-disk storage, real client SDK), not just the in-memory fakes `internal/raft`'s own tests use.

## What existed already

`internal/raft` already had solid unit test coverage from Phases 2–5 (`election_test.go`, `replication_test.go`, `file_storage_test.go`, `recovery_test.go`, `snapshot_test.go`), all built around `fakeTransport` (in-process channel routing) and `memStorage`/`recordingStateMachine` test doubles. CI (`.github/workflows/ci.yml`) already existed too — gofmt check, `go vet`, `go test -race`, `go build` — set up back in Phase 1 per this project's standing convention of getting CI running as soon as there's a buildable module, rather than waiting for a dedicated testing phase. Neither needed rework here.

What had **zero** automated coverage going into this phase: `internal/store` (the state machine itself), `internal/client` (the Phase 6 smart-client SDK — its redirect/retry/failover logic was only verified live, never automated), `internal/server` (same gap, for the server-side half of that logic), and — the biggest one — nothing anywhere exercised the real HTTP transport, the real `cmd/server` wiring, or a real multi-node cluster. Every existing Raft test ran entirely in-process through `fakeTransport`.

## What was added

### Unit tests

- **`internal/store/kvstore_test.go`** — `Put`/`Get`/`Delete`/`CAS` semantics (including CAS against a missing key, and rejecting on a mismatched `Expected`), prefix `Scan` ordering, `Snapshot`/`Restore` round-tripping, and confirming `Restore` replaces the whole map rather than merging into whatever was there before (a distinction called out in `kvstore.go` — see the "just swap the whole map" comment — that this test now actually pins down instead of just asserting in a comment).
- **`internal/client/client_test.go`** — built on `httptest.Server` so it doesn't need a real cluster: a direct write to a fake "leader" succeeds; a `421`+`X-Leader-URL` from a fake "follower" gets followed transparently; the cached leader is genuinely reused across calls (asserted via a request counter, not just "no error"); a cached leader that starts refusing connections gets dropped and the client falls back to the rest of its configured endpoint set; a `503` gets retried rather than surfaced as a failure; and a permanently-`503` server eventually gives up with `ErrMaxAttempts`. This directly closes the gap flagged in the Phase 6 doc's "Known Limitations" — the redirect/retry state machine had only been checked by hand before.
- **`internal/server/server_test.go`** — the other half of that same gap: `requireLeader`'s three outcomes (proceed as leader, `421` redirect when the leader is known, `503` when it isn't) tested directly against a fake `RaftNode`, including confirming a redirected request never actually reaches `Propose` at all, and that a race-window `ErrNotLeader` from `Propose` itself (this node was leader when checked, then stepped down before the call landed) still degrades to a plain `503` rather than panicking or hanging.

### Integration tests (`tests/integration`)

The real centerpiece of this phase. `harness_test.go` builds a `Cluster` of real nodes — each one is wired exactly like `cmd/server/main.go` does (`raft.NewFileStorage` in a per-node temp dir, a real `raft.Node`, `transport.NewRaftHandler`/`transport.NewClient` for peer RPC, `server.New` for the client API) — but as goroutines inside the test process instead of separate OS processes, listening on `127.0.0.1:0` (OS-assigned ports) instead of fixed ones. `Cluster.Put`/`Get` go through a real `internal/client.Client` pointed at the nodes' real addresses, so a test doesn't just check "did Raft replicate this" — it checks the whole stack a real `kvctl` invocation would exercise, redirect and retry included.

- **`TestLeaderElection`**, **`TestPutGet`**, **`TestGet_MissingKeyIsNotFound`** — baseline sanity.
- **`TestLeaderFailover`** — the Phase 6 checkpoint, automated: write, kill the leader, confirm a different node takes over, and the *same* client (no code change, no manual redirection) keeps working and still sees the pre-failover data.
- **`TestFullClusterRestart`** — the Phase 4 checkpoint, automated: kill every node, restart every node from disk, confirm previously-committed data survived with zero writes in between. This is the exact scenario the no-op-on-election mechanism exists for (see `CLAUDE.md`'s invariant list) — without it, this test would hang or come back empty.
- **`TestSnapshotCatchup`** (`snapshot_test.go`) — the Phase 5 checkpoint, automated: kill a follower, write well past the configured snapshot interval so the leader genuinely compacts its log, restart the follower, and confirm it catches up (via `InstallSnapshot` — a plain replay of the missing entries isn't even possible anymore, the leader doesn't have them) and can see the last key written.

### A real design lesson from getting these green

The first runs of `TestLeaderFailover` and `TestFullClusterRestart` failed — not because of a product bug, but because of a test-design mistake: both asserted on a single `Get` immediately after a leadership change. Reads in this project aren't linearizable yet (`CLAUDE.md`'s read-index note, still Phase 10 work) — a `Get` can land on any node, not just the leader, and a follower can have an entry safely committed in its log without having *applied* it locally yet (that requires one more heartbeat carrying the updated `LeaderCommit`). Asserting instantaneously was testing for a consistency guarantee the system doesn't claim to provide yet, not a real regression. Fixed by adding `Cluster.WaitForValue` — a short poll instead of a single check — which is both the correct way to test genuinely-eventual consistency and a good concrete reminder of exactly what Phase 10 needs to fix.

Also worth recording: `Cluster.Kill` closes a node's HTTP listeners and calls `node.Stop()`, but `Stop()` only *signals* the runloop goroutine to exit — it doesn't block until it has. In one run, a "killed" node still won a brief re-election (visible in the test log as e.g. `node1 became Leader term=3` after `Kill(node1)` had already been called) before its `Run()` goroutine actually noticed `stopCh` and exited — its own inbound HTTP was already closed, but it could still complete outbound RPCs to still-live peers in that window. The cluster self-healed within the normal `WaitForLeader` timeout once the zombie leader's heartbeats actually stopped, so this isn't a correctness bug (a slow-to-die process behaving this way for a few milliseconds is a reasonable enough approximation of reality), but it's worth knowing if a future test's log looks like an extra, unexpected term bump right after a `Kill` call — it's noise, not a symptom.

## Known limitations (expected — later phases address these)

- **No fault-injection / network-partition tests** — every integration test kills a node outright; nothing simulates a partial partition (some peers reachable, others not) or dropped/reordered/duplicated RPCs. Worth adding once there's a reason to (e.g. before relying on this project for anything beyond a showcase).
- **No property-based or fuzz testing of the Raft state machine itself** — coverage here is scenario-based (specific sequences of events), which is how the guide's own test list is structured, but doesn't give the same confidence as, say, a Jepsen-style linearizability checker would.
- **Integration test timing is tuned for speed, not realism** — 10ms ticks and 100–200ms election timeouts are far tighter than the production defaults (50ms/300–600ms). Fine for exercising correctness quickly; not representative of real-world latency behavior.

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
gofmt -l .                              # should print nothing
go test -race -timeout 120s ./...       # everything, including tests/integration

# just the new integration suite, verbose:
go test -race -v -timeout 60s ./tests/integration/...
```
