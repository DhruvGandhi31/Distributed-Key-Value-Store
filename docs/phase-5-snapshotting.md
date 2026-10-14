# Phase 5 — Snapshotting and Log Compaction

## Goal

The log stops growing unboundedly. A node periodically serializes its state machine into a snapshot, discards the log entries that snapshot already covers, and a follower that has fallen too far behind to be caught up by ordinary `AppendEntries` (because the leader no longer has the entries it needs) catches up via a new `InstallSnapshot` RPC instead.

## What changed

### Storage grows a movable compaction boundary

Through Phase 4, `FirstIndex()` was hardcoded to `1` — the log was assumed to start at the beginning of time. `Storage` gained two new methods:

```go
SaveSnapshot(data []byte, lastIndex, lastTerm uint64) error
LoadSnapshot() (data []byte, lastIndex, lastTerm uint64, err error)
```

`FileStorage`'s implementation writes the snapshot atomically (write-tmp, fsync, rename — the same pattern already used for hard state and log truncation), then compacts the in-memory and on-disk log by discarding every entry with `index <= lastIndex` and rewriting the log file with whatever's left. `firstIndex` becomes `lastIncludedIndex + 1` from that point on.

One subtlety: `TermAt(index)` has to keep working for `index == the snapshot boundary itself`, even though that entry is no longer in the log. This matters because `AppendEntries`'s consistency check calls `TermAt(PrevLogIndex)`, and a follower that's freshly caught up via a snapshot will very plausibly have `PrevLogIndex` land exactly on that boundary on the next heartbeat. `TermAt` special-cases this by comparing against the stored `snapshotTerm` before falling through to a normal log lookup.

The snapshot file itself — not the hard-state file's own `snapshotIndex`/`snapshotTerm` fields — is the single authoritative source for the compaction boundary on recovery. `SaveHardState` still writes those two fields (keeping the on-disk format stable), but every call site in `election.go` passes `0` for them, and `NewFileStorage` now reads the boundary from the snapshot file directly via a new `readSnapshotFile()` before it loads the log, so the log loader knows which leading entries to skip. Threading the real snapshot boundary through every term/vote-only `SaveHardState` call would have meant tracking it in more places for no benefit — the snapshot file already has to be authoritative for followers receiving `InstallSnapshot`, so it might as well be authoritative everywhere.

### A third RPC: InstallSnapshot

`Transport` gained `SendInstallSnapshot`, mirrored through the HTTP+gob transport exactly like the existing two RPCs (`/raft/install-snapshot`, same handler shape as `/raft/append` and `/raft/vote`). On the runloop side, `replicateOne` now checks whether a follower's `nextIndex` has fallen behind the leader's `FirstIndex()` — meaning a normal `AppendEntries` literally cannot supply what it needs anymore, the entries are gone — and if so, sends the leader's current snapshot instead of an `AppendEntries`.

The follower-side handler (`installSnapshot` in `snapshot.go`) reuses `Storage.SaveSnapshot` rather than adding a separate "replace the whole log" method. This was a deliberate simplification: `SaveSnapshot`'s existing semantics (discard `<= lastIndex`, keep anything after untouched) are exactly right for a follower compacting its *own* already-consistent log, and it turns out to also be safe for installing a leader's snapshot. If a follower had stale entries beyond the snapshot boundary left over from an old, abandoned leader, they aren't explicitly wiped by `installSnapshot` — but they don't need to be. The very next `AppendEntries` from the real leader will hit the normal consistency check, find a term mismatch, and `TruncateAfter` them, exactly as it would for any other log divergence. Reusing that already-tested path avoided a second log-replacement code path that would need its own correctness argument.

### Automatic triggering

Leaders and followers both snapshot on their own schedule — `applyCommitted` checks `lastApplied % SnapshotInterval == 0` after every apply and calls `maybeSnapshot()`, which asks the state machine (`KVStore.Snapshot`/`Restore`, gob-encoding the whole map) to serialize itself and hands the result to `Storage.SaveSnapshot`. This runs on *every* node, not just the leader — each node compacts its own log independently once it's applied enough entries, rather than the leader dictating when followers compact.

`cmd/server/main.go` gained `--snapshot-interval` (default 1000) and now checks for an existing snapshot on startup: if one exists, it restores the state machine from it and seeds `InitialSnapshotIndex` so the node's `commitIndex`/`lastApplied` start at the snapshot boundary instead of `0` — otherwise `applyCommitted` would try to replay entries starting at index 1, which no longer exist once the log has been compacted past that point.

## A real bug this phase's tests found

`appendNewEntries` (Phase 3 code) called `Storage.TermAt(idx)` for every index a leader sent, assuming the follower's own log always started at index 1. But since compaction now runs independently on every node — not just the leader — a follower can advance its own `FirstIndex()` past an index the leader is still retransmitting (e.g. a duplicate or slightly-stale `AppendEntries`). That index is safe to skip outright (compaction only ever happens after an index is already applied, so anything below `FirstIndex()` is certainly already correct), but the code didn't know that and called `TermAt` on an index no longer in the log, which returned `"file does not exist"` and made the whole `AppendEntries` fail. Fixed with a one-line guard:

```go
if idx < n.storage.FirstIndex() {
    continue
}
```

This didn't surface in casual testing — it only shows up when a follower is lagging far enough behind that the leader has already compacted past entries it's still trying to send, which is exactly the scenario `TestSnapshot_LaggingFollowerCatchesUpViaInstallSnapshot` was written to exercise. Without the fix that test caught 2/10 applied entries on the joining follower instead of 10/10.

## A real bug the live checkpoint found (not in the Raft code)

Verifying this phase live via `scripts/cluster.ps1` turned up a second, unrelated bug — not in `internal/raft` at all, but in how the cluster script launches nodes. `Start-Process -ArgumentList` never quotes array elements before Windows flattens them into the child process's command line. Since this repository's own path contains spaces (`...\My Full Fledged projects\...`), the absolute `--data-dir` value split into multiple tokens on the way to `kvstored.exe`, and Go's `flag.Parse()` silently stopped parsing at the first token that didn't look like a flag — which also swallowed `--snapshot-interval` (it came after `--data-dir` on the command line). Every node was quietly writing its Raft state to `D:\Development\My\nodeN` instead of `data/nodeN`, and running with the default 1000-entry snapshot interval no matter what `-SnapshotInterval` was passed. Fixed by having `Start-Nodes` quote any argument containing whitespace itself and pass `Start-Process` one pre-built string instead of a raw array (see `scripts/cluster.ps1`, commit `7394c7d`). Not a Raft correctness bug, but worth recording here since it's exactly the kind of thing that makes a live checkpoint worth running instead of trusting the unit tests alone — the unit tests never launch a real OS process, so they had no way to catch it.

## Verified behavior (checkpoint)

Ran a real 3-node cluster with `--snapshot-interval 20`:

```
.\scripts\cluster.ps1 -Action start -SnapshotInterval 20

# 60 kvctl put calls against the leader
# GET /cluster/status on all 3 nodes → commit_index: 61, last_applied: 61

grep snapshot data\logs\node1.log
# snapshot taken at index=20 term=1
# snapshot taken at index=40 term=1
# snapshot taken at index=60 term=1

# log files stay tiny post-compaction (158 bytes, not 60+ entries worth)
Get-Item data\node1\log, data\node2\log, data\node3\log | Select Length

.\scripts\cluster.ps1 -Action start-node4
# node4 joins cold, with no prior log or snapshot

GET http://127.0.0.1:8004/cluster/status
# → commit_index: 61, last_applied: 61 (fully caught up)

.\bin\kvctl.exe -addr http://127.0.0.1:8004 get key1   # → value1
.\bin\kvctl.exe -addr http://127.0.0.1:8004 get key60  # → value60

Get-Item data\node4\log | Select Length
# 158 bytes — the same tiny post-compaction size as the leader's log, not
# anywhere near large enough to hold 61 individually-replicated entries.
# node4 received a snapshot, not a full historical replay.
```

The log-size comparison is the actual proof here: if `node4` had caught up via ordinary `AppendEntries` from index 1, its log file would need to hold all 61 entries' worth of `keyN=valueN` data. Instead it's byte-for-byte the same size as the already-compacted leader's log, which is only possible if it received the snapshot at index 60 and replicated the one entry after it.

## Known limitations (expected — later phases address these)

- **No InstallSnapshot chunking** — the entire snapshot is sent in one RPC payload. Fine for this project's map-of-bytes state machine at any realistic test scale; a state machine with a genuinely large data set would need the chunked-transfer variant the Raft paper describes.
- **A follower's own compaction schedule is independent of the leader's** — this is a deliberate simplification (see above), but it does mean two nodes' snapshot boundaries can differ by a few entries at any given moment, which is fine (each snapshot is still self-consistent) but worth knowing if you're comparing `data/nodeN/snapshot` files directly across nodes.
- **`ConflictIndex`/`ConflictTerm` on `AppendEntriesReply` are still unused** — nextIndex backoff is still one entry at a time on a rejection. Hasn't been a practical problem since a rejection now routes a sufficiently-far-behind follower to `InstallSnapshot` instead, but the fast-backoff optimization from the Raft paper is still on the table for later.

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test -race -timeout 120s ./...

.\scripts\cluster.ps1 -Action start -SnapshotInterval 20
.\scripts\cluster.ps1 -Action status

# write past the interval a few times over
1..60 | ForEach-Object { .\bin\kvctl.exe -addr http://127.0.0.1:8001 put "key$_" "value$_" }

Invoke-WebRequest http://127.0.0.1:8001/cluster/status -UseBasicParsing | Select -Expand Content
Select-String -Path .\data\logs\node1.log -Pattern "snapshot taken"
Get-Item .\data\node1\log | Select Length

# bring a cold follower online after the leader has already compacted:
.\scripts\cluster.ps1 -Action start-node4
Invoke-WebRequest http://127.0.0.1:8004/cluster/status -UseBasicParsing | Select -Expand Content
.\bin\kvctl.exe -addr http://127.0.0.1:8004 get key1
Get-Item .\data\node4\log | Select Length   # should match the leader's post-compaction size

.\scripts\cluster.ps1 -Action stop
```
