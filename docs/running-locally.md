# Running locally

Every scenario below runs on one machine with no Docker. Commands are PowerShell (`scripts/cluster.ps1` is the cluster manager); bash equivalents are straightforward.

## Build and test

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test -race -timeout 120s ./...   # includes tests/integration: real multi-node clusters over real HTTP
```

## Single node (no Raft)

```powershell
.\bin\kvstored.exe --client-addr 127.0.0.1:8000
# in a second terminal:
.\bin\kvctl.exe put hello world
.\bin\kvctl.exe get hello
```

## 3-node Raft cluster (replicated writes)

```powershell
.\scripts\cluster.ps1 -Action start
.\scripts\cluster.ps1 -Action status   # shows which node is leader, and at what term

# as of Phase 6, a write to ANY node works — a follower transparently
# redirects the smart client to the leader, so you don't need to know
# which node that is:
.\bin\kvctl.exe -addr http://127.0.0.1:8002 put hello world
.\bin\kvctl.exe -addr http://127.0.0.1:8002 get hello
Invoke-WebRequest http://127.0.0.1:8002/cluster/status -UseBasicParsing | Select -Expand Content

.\scripts\cluster.ps1 -Action stop
```

> `node4` is always part of every node's configured peer set (see below), even when its process isn't running — so a plain `-Action start` cluster is really a 4-node Raft group with only 3 processes alive. That's still a majority (3 of 4) for normal operation, but killing one of those 3 afterward would drop you to 2 of 4 — **not** a majority — and the cluster would never re-elect. Use `-IncludeNode4` (see the next section) for any test that kills a node and expects a survivor to take over.

## Crash recovery (kill every node, restart from cold)

```powershell
.\scripts\cluster.ps1 -Action stop     # kill ALL nodes, not just the leader
.\scripts\cluster.ps1 -Action start    # restart the whole cluster from disk
.\scripts\cluster.ps1 -Action status   # term should be higher than before, not reset

# no write happens here — this reads purely from what survived the restart:
.\bin\kvctl.exe -addr http://127.0.0.1:8001 get hello
```

## Snapshotting and log compaction

```powershell
# --snapshot-interval controls how often (in applied entries) each node
# compacts its own log; defaults to 1000, set low here just to see it fire
.\scripts\cluster.ps1 -Action start -SnapshotInterval 20

# write past the interval a few times over
1..60 | ForEach-Object { .\bin\kvctl.exe -addr http://127.0.0.1:8001 put "key$_" "value$_" }

# each node has independently snapshotted and compacted its own log by now
Select-String -Path .\data\logs\node1.log -Pattern "snapshot taken"

# bring a cold node online after the leader has already compacted its log —
# it catches up via a snapshot transfer, not a full historical replay
.\scripts\cluster.ps1 -Action start-node4
.\bin\kvctl.exe -addr http://127.0.0.1:8004 get key1

.\scripts\cluster.ps1 -Action stop
```

## Leader failover (smart client transparently follows the new leader)

```powershell
# node4 always counts toward quorum once it's in the peer list (see the
# note above) — include it so a single kill still leaves a majority:
.\scripts\cluster.ps1 -Action start -IncludeNode4
.\scripts\cluster.ps1 -Action status   # note the leader's PID

.\bin\kvctl.exe -addr http://127.0.0.1:8003 put before-failover value1

Stop-Process -Id <leader's PID from above> -Force
.\scripts\cluster.ps1 -Action status   # a different node leads now, higher term

# same command, same follower address the whole time — the SDK found the
# new leader on its own:
.\bin\kvctl.exe -addr http://127.0.0.1:8003 put after-failover value2
.\bin\kvctl.exe -addr http://127.0.0.1:8003 get after-failover

# --addr also accepts a comma-separated fallback list, so kvctl recovers
# even if the first address you give it happens to be down:
.\bin\kvctl.exe -addr "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003,http://127.0.0.1:8004" get after-failover

.\scripts\cluster.ps1 -Action stop
```

## Live dashboard

```powershell
$env:KV_DEMO_TOKEN = "pick-any-secret"            # unlocks the "Disconnect" buttons
.\scripts\cluster.ps1 -Action start -IncludeNode4  # 4 nodes, so cutting one off still leaves a majority
Start-Process "http://127.0.0.1:8001/ui?token=pick-any-secret"
```

Write a key, disconnect the leader, write again, and watch the old leader step down and catch up when it reconnects. Without a token the dashboard still works, read-only.
