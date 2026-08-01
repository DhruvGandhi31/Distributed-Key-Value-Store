# Distributed Key-Value Store

[![CI](https://github.com/DhruvGandhi31/Distributed-Key-Value-Store/actions/workflows/ci.yml/badge.svg)](https://github.com/DhruvGandhi31/Distributed-Key-Value-Store/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/DhruvGandhi31/Distributed-Key-Value-Store)](https://goreportcard.com/report/github.com/DhruvGandhi31/Distributed-Key-Value-Store)
[![Go Reference](https://pkg.go.dev/badge/github.com/DhruvGandhi31/Distributed-Key-Value-Store.svg)](https://pkg.go.dev/github.com/DhruvGandhi31/Distributed-Key-Value-Store)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A from-scratch, fault-tolerant, distributed key-value store built on a **custom Raft consensus implementation** — no third-party Raft library. Built incrementally, one phase at a time. 

## Why this project exists

Most Raft implementations you can find are libraries you import and trust. This one is built from first principles — leader election, log replication, crash recovery, snapshotting, and a leader-aware client — to actually understand the algorithm and the hard edges around it (split votes, log divergence, fsync ordering, lagging followers) rather than just consume it.

## Status

🚧 **Under active, incremental development.** Phases 1–4 are done: a single-node KV store, custom Raft leader election, log replication, and crash recovery — writes are proposed through Raft, replicated to a majority, durably fsync'd, and survive killing every node in the cluster and restarting from cold. See the [Roadmap](#roadmap) below for what's built and what's next. Follow along in [`build-guide.md`](build-guide.md) for the full phase-by-phase design and implementation notes.

## Features

Planned, in build order (see [Roadmap](#roadmap)):

- In-memory KV store with `PUT` / `GET` / `DELETE` / prefix `SCAN` / compare-and-swap
- Custom Raft consensus: leader election, log replication, majority commit
- Crash-safe persistence via a write-ahead log (WAL) with fsync
- Snapshotting and log compaction so the WAL doesn't grow unboundedly
- A smart client SDK that transparently finds the leader and retries on failover
- Dockerized 3-node cluster, deployable with one command

## Architecture

```
Client (kvctl / SDK)
      │  HTTP
      ▼
┌─────────────┐   Raft RPC (HTTP+gob)   ┌─────────────┐   ┌─────────────┐
│   Node 1    │◄───────────────────────►│   Node 2    │◄─►│   Node 3    │
│  (Leader)   │                         │ (Follower)  │   │ (Follower)  │
└─────┬───────┘                         └─────┬───────┘   └─────┬───────┘
      │                                       │                 │
      ▼                                       ▼                 ▼
  KV state machine                     KV state machine    KV state machine
  + WAL on disk                        + WAL on disk       + WAL on disk
```

Each node runs a single-goroutine Raft "runloop" that owns all consensus state; the KV store is a pluggable state machine that only sees committed, ordered commands. Full design rationale lives in [`build-guide.md`](build-guide.md).

## Getting Started

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test -race -timeout 120s ./...
```

### Single node (no Raft)

```powershell
.\bin\kvstored.exe --client-addr 127.0.0.1:8000
# in a second terminal:
.\bin\kvctl.exe put hello world
.\bin\kvctl.exe get hello
```

### 3-node Raft cluster (replicated writes)

```powershell
.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status   # shows which node is leader, and at what term

# writes must go to the leader; replication is visible via /cluster/status
.\bin\kvctl.exe -addr http://127.0.0.1:8001 put hello world
.\bin\kvctl.exe -addr http://127.0.0.1:8001 get hello
Invoke-WebRequest http://127.0.0.1:8002/cluster/status -UseBasicParsing | Select -Expand Content

# kill the leader — a survivor wins re-election and keeps all committed data:
Stop-Process -Id (Get-Content .\data\node1.pid) -Force
.\scripts\cluster.ps1 -Action status

.\scripts\cluster.ps1 -Action stop
```

### Crash recovery (kill every node, restart from cold)

```powershell
.\scripts\cluster.ps1 -Action stop     # kill ALL nodes, not just the leader
.\scripts\cluster.ps1 -Action start    # restart the whole cluster from disk
.\scripts\cluster.ps1 -Action status   # term should be higher than before, not reset

# no write happens here — this reads purely from what survived the restart:
.\bin\kvctl.exe -addr http://127.0.0.1:8001 get hello
```

## Roadmap

- [x] Phase 1 — Single-node KV store (in-memory, HTTP API, CLI)
- [x] Phase 2 — Raft core: leader election
- [x] Phase 3 — Log replication
- [x] Phase 4 — Persistent storage (WAL + crash recovery)
- [ ] Phase 5 — Snapshotting and log compaction
- [ ] Phase 6 — Smart client and CLI (leader redirection, retries)
- [ ] Phase 7 — Testing (unit + integration) and CI
- [ ] Phase 8 — Docker and deployment
- [ ] Phase 9 — Observability (metrics, structured logging, pprof)
- [ ] Phase 10 — Advanced Raft hardening (pre-vote, read-index, joint consensus, TLS)
