# Architecture

How the system is put together, from the whole cluster down to a single write. For why each piece was built the way it was, see the per-phase write-ups in this folder.

- [Cluster overview](#cluster-overview)
- [Inside one node](#inside-one-node)
- [Write path](#write-path)
- [Raft roles](#raft-roles)
- [Catching up a lagging follower](#catching-up-a-lagging-follower)
- [Deployment (Kubernetes / k3s)](#deployment-kubernetes--k3s)
- [Live dashboard](#live-dashboard)

## Cluster overview

Every node holds a full replica. Only the leader accepts writes; it replicates them to followers over a separate peer port, and an entry commits once a majority has it durably on disk.

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

## Inside one node

Each node runs two HTTP listeners, one for clients and one for Raft peers. Everything Raft-related goes through **one runloop goroutine** that owns term, vote, log, and commit index. Other goroutines reach it through channels rather than shared locks. The KV store is a pluggable state machine and only ever sees committed entries, in log order.

```mermaid
%%{init: {'theme': 'neutral'}}%%
flowchart TB
    subgraph proc ["kvstored process"]
        direction TB
        API["Client server<br/>/kv · /cluster/status<br/>/metrics · /debug/pprof"]
        TR["Peer transport<br/>/raft/vote · /raft/append<br/>/raft/install-snapshot"]
        RL{{"Raft runloop<br/>single goroutine"}}
        SM["KV state machine<br/>in-memory map"]
        DISK[("FileStorage<br/>hard state · log · snapshot<br/>fsync before ack")]
        MET["Metrics registry<br/>Prometheus"]
    end

    API -->|"Propose(cmd)"| RL
    API -->|"GET: local read"| SM
    TR -->|"RPCs via channels"| RL
    RL -->|"persist"| DISK
    RL -->|"apply committed entries"| SM
    RL -.->|"term, commit index,<br/>leader changes"| MET

    classDef io fill:#a5d8ff,stroke:#1971c2,color:#1e1e1e
    classDef core fill:#ffec99,stroke:#f08c00,color:#1e1e1e
    classDef data fill:#b2f2bb,stroke:#2f9e44,color:#1e1e1e
    classDef aux fill:#e9ecef,stroke:#868e96,color:#1e1e1e
    class API,TR io
    class RL core
    class SM,DISK data
    class MET aux
```

> Reads are served from the local state machine on whichever node receives them, so a follower or a partitioned old leader can return stale data. Linearizable reads (read-index) are planned for Phase 10.

## Write path

What happens on `kvctl put k v` when the client starts out talking to a follower:

```mermaid
%%{init: {'theme': 'neutral'}}%%
sequenceDiagram
    autonumber
    participant C as Client SDK
    participant F as Follower
    participant L as Leader
    participant D as Leader disk
    participant P as Other followers

    C->>F: PUT /kv/k
    F-->>C: 421 Misdirected + X-Leader-URL
    C->>L: PUT /kv/k (retry at leader)
    activate L
    L->>D: append entry + fsync
    par replicate in parallel
        L->>P: AppendEntries(entry)
        P->>P: append + fsync
        P-->>L: success
    end
    Note over L,P: A majority has the entry on disk, so commitIndex advances.<br/>Only entries from the current term commit this way (Raft §5.4.2).
    L->>L: apply to KV state machine
    L-->>C: 200 OK
    deactivate L
    Note over L,P: Followers learn the new commitIndex on the next heartbeat and apply it too.
```

## Raft roles

```mermaid
%%{init: {'theme': 'neutral'}}%%
flowchart LR
    START(( )) --> FO["Follower"]
    FO -->|"election timeout"| CA["Candidate"]
    CA -->|"majority of votes"| LE["Leader"]
    CA -->|"timeout: new term,<br/>new election"| CA
    CA -.->|"higher term or<br/>current leader"| FO
    LE -.->|"higher term"| FO

    classDef follower fill:#a5d8ff,stroke:#1971c2,color:#1e1e1e
    classDef candidate fill:#e9ecef,stroke:#868e96,color:#1e1e1e
    classDef leader fill:#ffec99,stroke:#f08c00,color:#1e1e1e
    class FO follower
    class CA candidate
    class LE leader
```

Dotted arrows are step-downs: any RPC that carries a higher term sends a node straight back to Follower.

- `votedFor` and `currentTerm` are fsync'd **before** any RequestVote goes out. A node that crashes mid-election can't vote twice in the same term.
- A new leader immediately appends a **no-op entry** for its term. Raft only commits older entries indirectly, through a current-term entry, so without the no-op a cluster restarted from cold could never re-advance its commit index until a client wrote something.

## Catching up a lagging follower

Every node compacts its own log into a snapshot once it has applied `--snapshot-interval` entries. If a follower is so far behind that the entries it needs have already been compacted on the leader, the leader sends its snapshot instead:

```mermaid
%%{init: {'theme': 'neutral'}}%%
flowchart TB
    S(["Heartbeat or new entry for a peer"]) --> Q{"Entries the peer needs<br/>already compacted?"}
    Q -->|"Yes"| IS["Send InstallSnapshot"]
    Q -->|"No"| AE["Send AppendEntries<br/>from nextIndex"]
    AE --> R{"Follower log<br/>matches?"}
    R -->|"No"| BK["nextIndex -= 1<br/>(never below 1)"]
    R -->|"Yes"| OK(["Advance matchIndex<br/>and nextIndex"])
    IS --> OK
    BK -.->|"retry next round"| S

    classDef decision fill:#ffec99,stroke:#f08c00,color:#1e1e1e
    classDef snap fill:#b2f2bb,stroke:#2f9e44,color:#1e1e1e
    class Q,R decision
    class IS snap
```

## Deployment (Kubernetes / k3s)

The live cluster runs as a 3-replica StatefulSet on a single k3s VM. Each pod gets its own persistent volume and its own public port. A follower's redirect has to send an outside client to the leader's public address, and a single shared load balancer can't provide one stable address per pod.

```mermaid
%%{init: {'theme': 'neutral'}}%%
flowchart TB
    U(["Client on the internet"])

    subgraph vm ["GCP VM · static IP · k3s"]
        direction TB
        subgraph lbs ["Per-pod LoadBalancer Services"]
            direction LR
            S0[":8081"]
            S1[":8082"]
            S2[":8083"]
        end
        subgraph sts ["StatefulSet kvstore (3 replicas)"]
            direction LR
            P0["kvstore-0"]
            P1["kvstore-1"]
            P2["kvstore-2"]
        end
        subgraph pvcs ["PersistentVolumeClaims (1Gi each)"]
            direction LR
            V0[("data-kvstore-0")]
            V1[("data-kvstore-1")]
            V2[("data-kvstore-2")]
        end
        H["Headless Service: stable peer DNS for Raft on :7000"]
    end

    U --> S0 & S1 & S2
    S0 --> P0
    S1 --> P1
    S2 --> P2
    P0 --- V0
    P1 --- V1
    P2 --- V2
    V0 ~~~ H
    V1 ~~~ H
    V2 ~~~ H

    classDef ext fill:#d0bfff,stroke:#9c36b5,color:#1e1e1e
    classDef svc fill:#a5d8ff,stroke:#1971c2,color:#1e1e1e
    classDef pod fill:#ffec99,stroke:#f08c00,color:#1e1e1e
    classDef vol fill:#b2f2bb,stroke:#2f9e44,color:#1e1e1e
    class U ext
    class S0,S1,S2,H svc
    class P0,P1,P2 pod
    class V0,V1,V2 vol
```

## Live dashboard

Every node serves a small dashboard at `/ui`. It is one HTML file embedded in the binary with `go:embed`, with no frontend build step and no external assets. The page asks the node it was loaded from for the member list (`/cluster/members`), then polls every node's `/cluster/status` and its own copy of the watched key twice a second. That is how it shows each replica converge, or fall behind, in real time. Nodes send permissive CORS headers so the page can reach peers on other ports. The client API was already public and unauthenticated, so this exposes nothing new.

The "Disconnect" button calls `POST /demo/isolate` on one node. The node then drops all of its Raft traffic in both directions for 15 seconds (`internal/transport/partition.go`), while its consensus code keeps running untouched. Everything you see afterwards is the real algorithm reacting:

- the other nodes miss heartbeats and elect a new leader;
- the cut-off node keeps its old view (an isolated leader still believes it leads, but can't commit anything);
- when the cut heals, it sees the newer term, steps down, and catches up.

The endpoint requires a shared secret in the `X-Demo-Token` header (`--demo-token` flag or `KV_DEMO_TOKEN` env var), compared in constant time. With no token configured the dashboard is read-only. Durations are clamped to 3–30 seconds, and a node that is already isolated refuses a second request.
