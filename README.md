# Distributed Key-Value Store

[![CI](https://github.com/DhruvGandhi31/Distributed-Key-Value-Store/actions/workflows/ci.yml/badge.svg)](https://github.com/DhruvGandhi31/Distributed-Key-Value-Store/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/DhruvGandhi31/Distributed-Key-Value-Store)](https://goreportcard.com/report/github.com/DhruvGandhi31/Distributed-Key-Value-Store)
[![Go Reference](https://pkg.go.dev/badge/github.com/DhruvGandhi31/Distributed-Key-Value-Store.svg)](https://pkg.go.dev/github.com/DhruvGandhi31/Distributed-Key-Value-Store)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**A database that keeps working when servers fail.** Three servers each keep a full copy of the data. If one crashes or gets cut off, the others elect a new leader in about a second and keep accepting writes, with nothing lost.

Under the hood, it's a distributed key-value store built on my own implementation of the **Raft consensus algorithm** in Go, written from scratch with no Raft library. It is deployed on Kubernetes on Google Cloud.

**[▶ Open the live dashboard](http://34.60.149.77:8081/ui)**: watch the real cluster running on Google Cloud.

![Live dashboard: a write lands on all three nodes, the leader is disconnected, a new leader is elected, writes keep working, and the old leader catches up when it reconnects](docs/assets/dashboard-failover.gif)

<sub>The built-in dashboard during a real failover, recorded from a running cluster rather than a mock-up. The leader is cut off, the other two elect a new one, a write during the outage still succeeds, and the old leader steps down and catches up when it returns.</sub>

## In plain English

Picture three bank tellers who must agree before any transaction goes into the ledger. One acts as the lead teller. Every change goes through them and only counts once most tellers have written it down. If the lead teller walks away, the others quickly pick a new one and carry on. When the missing teller returns, they copy whatever they missed.

That agreement process is called **consensus**. It is what lets systems like etcd (the database behind Kubernetes), CockroachDB and Consul survive hardware failures without losing data. This project implements it from first principles instead of importing a library.

## What this project demonstrates

| Area | What I built |
|---|---|
| **Distributed systems** | Leader election, log replication, majority commit, crash recovery, log compaction with snapshot transfer, and leader redirection, all from the Raft paper |
| **Correctness under failure** | Data is written to disk before it is acknowledged. 76 tests, including integration tests that start real multi-node clusters, kill nodes and check that no data is lost |
| **Backend engineering (Go)** | HTTP API, a smart client SDK with retries, and a CLI. One goroutine owns all consensus state and everything else talks to it over channels instead of shared locks; CI runs every test under Go's race detector |
| **Cloud and DevOps** | Multi-stage Docker image, Kubernetes StatefulSet with persistent volumes on a GCP VM, and a GitHub Actions CI pipeline running vet, race-detector tests and builds |
| **Observability** | Prometheus metrics, structured JSON logging, and live CPU/memory profiling |
| **Product sense** | A live dashboard that turns an invisible algorithm into something you can see and break during a demo |

## How it works

```mermaid
%%{init: {'theme': 'neutral'}}%%
flowchart LR
    C(["kvctl / client SDK"])

    subgraph cluster ["Raft cluster (3 nodes)"]
        direction TB
        L["Node 1 — Leader"]
        F1["Node 2 — Follower"]
        F2["Node 3 — Follower"]
    end

    P(["Prometheus"])

    C -->|"HTTP /kv"| L
    C -.->|"write to a follower:<br/>421 + X-Leader-URL"| F1
    L <-->|"AppendEntries<br/>HTTP + gob"| F1
    L <-->|"AppendEntries<br/>HTTP + gob"| F2
    P -.->|"scrape /metrics"| cluster

    classDef leader fill:#ffec99,stroke:#f08c00,color:#1e1e1e
    classDef follower fill:#a5d8ff,stroke:#1971c2,color:#1e1e1e
    classDef external fill:#d0bfff,stroke:#9c36b5,color:#1e1e1e
    class L leader
    class F1,F2 follower
    class C,P external
```

1. **One leader.** The nodes elect a leader. If it goes silent, the others hold a new election.
2. **Every write goes through the leader.** It saves the write to its own disk and sends it to the followers. Once a **majority** has it on disk, the write is committed and the client gets an OK.
3. **Clients don't need to know who leads.** A follower answers with a redirect to the current leader, and the client SDK follows it automatically.
4. **Logs don't grow forever.** Each node periodically compacts its log into a snapshot. A follower that falls far behind is sent the snapshot instead of the full history.

**Go deeper:** [Architecture with diagrams](docs/architecture.md) covers the write path, election states, catch-up logic, the Kubernetes layout and how the dashboard works.

## Engineering highlights

Bugs I found along the way are often the more interesting part:

- **"Lost" data after a full restart that wasn't actually lost.** After every node restarted at once, committed data sat safely on disk but never came back. Raft's safety rules stop a new leader from committing older entries directly, so nothing could be replayed until a client happened to write. The fix, which etcd also uses, is for each new leader to immediately write an empty entry for its own term. [Phase 4 write-up](docs/phase-4-persistent-storage.md)
- **A race between log compaction and retransmission.** Because every node compacts its own log independently, a follower could receive an entry it had already compacted away and reject the whole batch. A test written specifically for lagging followers caught it, with only 2 of 10 entries arriving. [Phase 5 write-up](docs/phase-5-snapshotting.md)
- **Redirects that worked inside Kubernetes but not outside it.** Followers were redirecting clients to internal cluster DNS names that nobody on the internet can resolve. I fixed it by giving each pod its own public address. [Phase 8 write-up](docs/phase-8-docker-and-deployment.md)
- **A silently ignored flag.** A path with spaces caused the cluster script to drop later command-line flags, so nodes were writing to the wrong directory. Unit tests couldn't catch this; running the real cluster did. [Phase 5 write-up](docs/phase-5-snapshotting.md)

## Try it

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client

$env:KV_DEMO_TOKEN = "pick-any-secret"
.\scripts\cluster.ps1 -Action start -IncludeNode4
Start-Process "http://127.0.0.1:8001/ui?token=pick-any-secret"   # the dashboard above

.\bin\kvctl.exe -addr http://127.0.0.1:8002 put hello world      # any node works, followers redirect
.\bin\kvctl.exe -addr http://127.0.0.1:8003 get hello
```

More scenarios (crash recovery, snapshot catch-up, failover from the CLI) are in [Running locally](docs/running-locally.md). Docker Compose and Kubernetes manifests are in [`deployments/`](deployments/) and [`docker-compose.yml`](docker-compose.yml).

## Tech stack

**Go** (standard library, plus `prometheus/client_golang`) · **Raft**, custom implementation · HTTP + `encoding/gob` between nodes · **Docker** (distroless runtime) · **Kubernetes** (k3s StatefulSet) · **Google Cloud** · **GitHub Actions** · **Prometheus** · `log/slog`

## Project structure

```
cmd/server          kvstored: one cluster node
cmd/client          kvctl: command-line client
internal/raft       consensus: election, replication, snapshots, durable storage
internal/store      the key-value state machine
internal/transport  node-to-node RPCs over HTTP
internal/server     client-facing HTTP API and the dashboard
internal/client     smart client SDK (leader discovery and retries)
internal/metrics    Prometheus metrics
tests/integration   real multi-node clusters over real HTTP
deployments/        Dockerfile and Kubernetes manifests
docs/               architecture and per-phase design write-ups
```

## Roadmap

Built in ten phases, each with a design write-up in [`docs/`](docs/):

- [x] 1–3: single-node store, Raft leader election, log replication
- [x] 4–5: crash recovery (write-ahead log), snapshots and log compaction
- [x] 6–7: smart client with leader redirection, test suite and CI
- [x] 8: Docker, Kubernetes, deployment on Google Cloud
- [x] 9: observability (metrics, structured logs, profiling) and the live dashboard
- [ ] 10: pre-vote, linearizable reads (read-index), membership changes, TLS

**Known limitation:** reads are served from whichever node receives them, so a follower can briefly return stale data. Linearizable reads are the first item in Phase 10.

The full phase-by-phase design notes are in [`build-guide.md`](build-guide.md).
