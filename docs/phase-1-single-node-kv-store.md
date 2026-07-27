# Phase 1 — Single-Node KV Store


## Goal

A single-process, in-memory, HTTP-accessible key-value store with no clustering or persistence yet. This is the foundation the Raft engine (Phase 2+) plugs into later — the KV store never needs to know Raft exists; it only implements `raft.StateMachine`.

## What was built

```
go.mod
internal/
  raft/
    statemachine.go   — LogEntry, EntryType, StateMachine interface
  store/
    command.go         — wire format for KV operations (Command, ApplyResult)
    kvstore.go          — in-memory KV state machine (KVStore)
  server/
    server.go           — client-facing HTTP API
cmd/
  server/main.go        — kvstored binary entrypoint
  client/main.go        — kvctl CLI entrypoint
```

Module path: `github.com/DhruvGandhi31/Distributed-Key-Value-Store` (`go.mod` targets Go 1.21; built/tested with Go 1.25).

### `internal/raft` — `statemachine.go`

Defines the seam between the future Raft engine and any state machine built on top of it. Raft only ever sees an opaque `[]byte` payload per `LogEntry`; it has no idea it's carrying KV commands.

```go
type EntryType uint8

const (
    EntryNoOp   EntryType = 0
    EntryNormal EntryType = 1
)

type LogEntry struct {
    Index uint64
    Term  uint64
    Type  EntryType
    Data  []byte
}

type StateMachine interface {
    Apply(entry LogEntry) interface{}
    Snapshot() ([]byte, error)
    Restore(data []byte) error
}
```

**Deviation from `build-guide.md`**: the guide's Step 1.2 snippet references `EntryType`/`EntryNormal` without defining them (they're formally introduced in `types.go` in Phase 2, Step 2.1). Since `store.KVStore.Apply` needs them to compile in Phase 1, they're defined here instead. When Phase 2 builds out `types.go` (`NodeID`, `State`, RPC structs, errors), `EntryType` stays in `statemachine.go` — it's conceptually part of the log-entry/state-machine contract, not the RPC layer, so there's no need to move it later.

### `internal/store` — `command.go` + `kvstore.go`

`Command` is the gob-encoded wire format stored inside `LogEntry.Data`. `KVStore` implements `raft.StateMachine` over an in-memory `map[string][]byte` guarded by an `RWMutex`. Supported operations: `Put`, `Delete`, `CAS` (compare-and-swap — implemented now even though nothing calls it yet, since it's part of the `Command` wire format and cheap to include). Reads (`Get`, `Scan`) bypass `Apply` entirely — in Phase 1 there's no log to go through, so they read the map directly.

**Deviation from `build-guide.md`**: the guide's `Snapshot()` returns `buf.Bytes(), gob.NewEncoder(&buf).Encode(kv.data)` in a single return statement. Go evaluates return operands left-to-right, so `buf.Bytes()` is evaluated *before* `Encode` writes into `buf` — the snippet as written always returns an empty slice. Fixed by encoding first, then returning `buf.Bytes()` as a separate step.

### `internal/server` — `server.go`

Wires `KVStore` directly to HTTP handlers (no leader/follower logic yet — that arrives in Phase 6 once there's a cluster to redirect within).

| Method | Path | Behavior |
|---|---|---|
| `PUT` | `/kv/{key}` | body → value; `Apply(OpPut)` |
| `GET` | `/kv/{key}` | `Get(key)`; `404` if absent |
| `DELETE` | `/kv/{key}` | `Apply(OpDelete)`; not an error if key absent (idempotent) |
| `GET` | `/kv?prefix=...` | `Scan(prefix)`, JSON array of `[key, value]` pairs |
| `GET` | `/healthz` | always `200` |

### `cmd/server` / `cmd/client`

`kvstored` takes `--client-addr` (default `127.0.0.1:8000`) and serves the API above. `kvctl` is a thin CLI (`put`/`get`/`delete`/`scan`) that talks to it over plain `net/http` — no smart leader-discovery yet, that's Phase 6.

**Deviation from `build-guide.md`**: the guide suggests a `--data-dir` flag on `kvstored` from Phase 1 onward. It's omitted here until Phase 4 (persistence) actually uses it — an unused flag is dead code, and Go won't even compile an unused local variable, so keeping it would've required a throwaway `_ = dataDir`.

## Bug found during verification

`Command.Key` was accidentally typed as lowercase `key` while transcribing the code, making it unexported. This didn't just break `internal/store` — `gob` only encodes exported struct fields, so even if it had compiled, the key would have silently vanished from every encoded `Command`. It surfaced immediately as a compile error in `go build ./cmd/server` (and would have in any code review, since `internal/server` also references `Command.Key`). Fixed by capitalizing the field. This is why `go vet ./...` and a full build are part of the Phase 1 checkpoint, not just `go build ./cmd/client` — a partial build can hide a broken package.

## Verified behavior (checkpoint)

```
put hello world       → OK
get hello              → world
put hello2 world2     → OK
scan hel                → hello=world, hello2=world2
delete hello           → OK
get hello               → not found
GET /healthz            → 200
```

## Known limitations (expected — later phases address these)

- **No persistence** — process restart loses all data (Phase 4).
- **No clustering** — single process, no replication or fault tolerance (Phase 2–3).
- **No leader redirection / smart client** — `kvctl` just talks to whatever address it's given (Phase 6).
- **No snapshotting** — irrelevant with no log yet (Phase 5).
- **No auth/TLS** — plaintext HTTP only (Phase 10).

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...

.\bin\kvstored.exe --client-addr 127.0.0.1:8000
# in a second terminal:
.\bin\kvctl.exe put hello world
.\bin\kvctl.exe get hello
.\bin\kvctl.exe delete hello
.\bin\kvctl.exe get hello
```
