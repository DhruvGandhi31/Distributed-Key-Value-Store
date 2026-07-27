# Distributed Key-Value Store

[![CI](https://github.com/DhruvGandhi31/Distributed-Key-Value-Store/actions/workflows/ci.yml/badge.svg)](https://github.com/DhruvGandhi31/Distributed-Key-Value-Store/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/DhruvGandhi31/Distributed-Key-Value-Store)](https://goreportcard.com/report/github.com/DhruvGandhi31/Distributed-Key-Value-Store)
[![Go Reference](https://pkg.go.dev/badge/github.com/DhruvGandhi31/Distributed-Key-Value-Store.svg)](https://pkg.go.dev/github.com/DhruvGandhi31/Distributed-Key-Value-Store)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A from-scratch, fault-tolerant, distributed key-value store built on a **custom Raft consensus implementation** — no third-party Raft library. Built incrementally, one phase at a time. 

## Why this project exists

Most Raft implementations you can find are libraries you import and trust. This one is built from first principles — leader election, log replication, crash recovery, snapshotting, and a leader-aware client — to actually understand the algorithm and the hard edges around it (split votes, log divergence, fsync ordering, lagging followers) rather than just consume it.

## Status

🚧 **Under active, incremental development.** See the [Roadmap](#roadmap) below for what's built and what's next. Follow along in [`build-guide.md`](build-guide.md) for the full phase-by-phase design and implementation notes.

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

> Coming in Phase 1 — a single-node build/run/CLI walkthrough will land here once the server binary exists.


```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test -race -timeout 120s ./...
```

## Roadmap

- [ ] Phase 1 — Single-node KV store (in-memory, HTTP API, CLI)
- [ ] Phase 2 — Raft core: leader election
- [ ] Phase 3 — Log replication
- [ ] Phase 4 — Persistent storage (WAL + crash recovery)
- [ ] Phase 5 — Snapshotting and log compaction
- [ ] Phase 6 — Smart client and CLI (leader redirection, retries)
- [ ] Phase 7 — Testing (unit + integration) and CI
- [ ] Phase 8 — Docker and deployment
- [ ] Phase 9 — Observability (metrics, structured logging, pprof)
- [ ] Phase 10 — Advanced Raft hardening (pre-vote, read-index, joint consensus, TLS)
