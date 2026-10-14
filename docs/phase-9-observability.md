# Phase 9 — Observability

## Goal

Operators can see what the cluster is doing without reading source code: Prometheus metrics at `/metrics`, structured `log/slog` logging, and CPU/memory profiling at `/debug/pprof/` (on a private `--debug-addr` listener since 2026-10-15).

## Design decision: a real dependency, for the first time

This project's `go.mod` had zero third-party dependencies through Phase 8 — even Raft itself is from scratch. Phase 9's metrics requirement (real Prometheus histograms, correct text-exposition formatting) was a genuine fork in the road: hand-roll a minimal exporter to keep that streak, or pull in `prometheus/client_golang`, the canonical real-world library. Asked directly, the call was to use the real library — consistent with "not just make it work" from this project's own stated goals, and avoids reinventing histogram bucketing by hand for no real benefit.

**A version pin mattered here.** `go get`'s default (`client_golang@latest`, v1.25.0) silently bumped `go.mod`'s `go` directive from `1.21` to `1.26.0` — client_golang's newer releases require a newer Go than this project targets. That would have broken the Dockerfile (`golang:1.23-alpine`, which backs the live GCP deployment) and the local dev machine (Go 1.25 installed). Pinned to **`client_golang v1.20.5`** instead, which keeps `go.mod` at `go 1.21` with no toolchain auto-download — `go mod tidy` confirmed no other transitive dependency forced a bump either.

## Metrics (`/metrics`)

All 6 planned metrics, implemented in `internal/metrics`:

- `raft_current_term` (gauge)
- `raft_commit_index` (gauge)
- `raft_leader_changes_total` (counter)
- `raft_append_entries_duration_seconds` (histogram) — latency of outbound `AppendEntries` RPCs the leader sends to peers, timed around the actual `transport.SendAppendEntries` call in `replication.go`'s `replicateOne` goroutine.
- `kv_ops_total{op="put|get|delete|scan"}` (counter)
- `kv_op_duration_seconds{op="..."}` (histogram)

Plus the standard Go runtime/process collectors (goroutine count, GC pauses, memory, CPU) — not in the planned list, but directly useful for the "diagnose slow followers" goal this phase states, and essentially free to add via `collectors.NewGoCollector()`/`NewProcessCollector()`.

**`kv_ops_total`'s label set is `put|get|delete|scan`, not `put|get|delete|scan|cas`** as originally planned — `cas` isn't actually wired to an HTTP route anywhere in the codebase (it exists at the `internal/store` level, tested there since Phase 7, but `internal/server` never exposed it). Not something this phase added or fixed; just noting the gap rather than silently matching a label that can never fire.

### Injected, not global — and why that matters for tests

`internal/raft.Metrics` and `internal/server.Metrics` are small interfaces (mirroring the existing `raft.Logger` pattern — the project already had this DI shape in place, with a comment anticipating exactly this future swap). `internal/metrics.Registry` is the one concrete type satisfying both, by structural typing, constructed once in `cmd/server/main.go` and passed into both `raft.Config.Metrics` and `server.New`.

Each `Registry` holds its **own private `prometheus.Registry`** rather than registering to the global `prometheus.DefaultRegisterer`. This isn't just hygiene — `tests/integration` runs several in-process `raft.Node`s in one test binary, and the global registry would panic on the second node's registration ("duplicate metrics collector registration attempted", since metric names must be unique per registry). `TestRegistry_IsolatedPerInstance` in `internal/metrics/metrics_test.go` pins this down directly: two `Registry` instances, two different term values, each one's own `/metrics` output only reflects its own state.

## Structured logging (`log/slog`)

`raft.Logger`'s interface was reshaped from `Debugf/Infof/Warnf/Errorf(format string, args ...interface{})` to `Debug/Info/Warn/Error(msg string, args ...any)` — matching `*slog.Logger`'s own method set exactly, so a real `*slog.Logger` satisfies the interface directly with zero adapter code. All 19 call sites across `election.go`, `replication.go`, and `snapshot.go` were converted from interpolated format strings to real structured fields (`"term", n.currentTerm`, `"peer", peer`, `"index", idx`, `"error", err`, etc.) rather than one opaque message string — this is what makes the JSON output in production actually machine-parseable per-field, not just "valid JSON wrapping a formatted sentence."

`cmd/server/main.go` builds one shared `*slog.Logger` (new `--log-format` flag: `text` default for dev, `json` for production/log aggregation) with `nodeID` attached once via `.With("nodeID", *id)`, so every subsequent log line — both the top-level server logging and everything inside Raft — carries it automatically, rather than needing to thread it through every call site by hand. This same logger replaced every `log.Printf`/`log.Fatalf` call in `main.go` (`log.Fatalf` has no direct slog equivalent — replaced with a small local `fatal(logger, msg, args...)` helper that logs at Error then `os.Exit(1)`).

The old `debugRaft` package-level bool (gating `Debugf` output, "don't commit this as true, it's incredibly noisy") was removed without replacement — `slog`'s default handler level is `Info`, meaning `Debug` calls are silently dropped unless a handler is explicitly configured with `Level: slog.LevelDebug`. Same behavior, no extra code needed.

## pprof (`/debug/pprof/`)

`net/http/pprof` normally self-registers onto `http.DefaultServeMux` via a blank import's `init()`, but `internal/server.Server` uses its own `http.ServeMux`. Wired by hand instead — `Index`, `Cmdline`, `Profile`, `Symbol`, `Trace` registered directly, which is exactly what the stdlib's own `init()` does; `pprof.Index` itself dispatches the named profiles (`heap`, `goroutine`, `threadcreate`, `block`, `mutex`, `allocs`) under `/debug/pprof/<name>`, so no separate routes were needed for those.

**Known caveat, not addressed this phase**: `/metrics` and `/debug/pprof/` share the same mux and port as the client-facing KV API (`8000` internally) — meaning on the live GCP deployment (`docs/phase-8-docker-and-deployment.md`), both are reachable from the open internet on ports `8081`-`8083`, same as `/kv`. `pprof` in particular can leak process information and lets anyone trigger a CPU profile (a mild resource-exhaustion vector). Acceptable for a personal showcase project with no real traffic or secrets; would need separate auth or a non-public port before this pattern belongs anywhere with real users — that's Phase 10 territory (TLS/auth), not retrofitted here. **Update (2026-10-15):** pprof has since moved to its own listener, `--debug-addr`, which is off by default; see "Follow-up: review fixes" below. `/metrics` is still public.

## Not done this phase

**Grafana dashboards** (the plan's 4th bullet) — skipped. There's no deployed Grafana instance to target, and a dashboard JSON written without one to actually point it at and screenshot would be unverified guesswork rather than a real artifact. Worth doing once/if a Grafana instance exists to scrape the live GCP deployment's `/metrics`.

## A real environment issue found along the way (not caused by this phase)

Running the project's standard `go test -race -timeout 120s ./...` failed outright: `-race is not supported on windows/386`. `go env` / `go version` traced this to the installed Go toolchain itself being the **32-bit Windows build** (`C:\Program Files (x86)\Go`, `go1.25.5 windows/386`) — not a GOARCH override, not anything this phase's changes touched, just a pre-existing fact about this dev machine that happened to surface here because Phase 9 was the first time a `go get` triggered a toolchain-version check. The race detector requires a 64-bit toolchain, full stop — fixing it means installing 64-bit Go, which wasn't done autonomously (installing a different toolchain is a real system change). CI (`ubuntu-latest`, 64-bit) is and always has been unaffected — `-race` works fine there. All verification in this phase was done with plain `go test` locally instead, which passed cleanly.

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
gofmt -l .                         # should print nothing
go test -timeout 120s ./...        # -race unavailable locally, see above; CI runs it

./bin/kvstored.exe -client-addr=127.0.0.1:8000 -debug-addr=127.0.0.1:6060 &
curl http://127.0.0.1:8000/metrics
curl http://127.0.0.1:6060/debug/pprof/
```

## Follow-up: live demo dashboard (2026-10-15)

Metrics answer "what is the cluster doing" for an operator, but not for someone watching a demo. So a small dashboard was added at `/ui` on every node: one HTML file embedded with `go:embed`, with no frontend build step. It shows each node's role, term and commit index, plus its own copy of the last-written key, and a token-gated "Disconnect for 15s" button per node.

Disconnecting is a real fault injection, not an animation. `internal/transport/partition.go` makes the node drop its Raft traffic in both directions while its consensus code keeps running, so the election, the stale isolated leader and the step-down on reconnect are all the real algorithm. Supporting changes:

- `/cluster/members`
- `id` and `isolated_ms` fields on `/cluster/status`
- permissive CORS so one page can poll every node
- the `--demo-token` flag / `KV_DEMO_TOKEN` env var, compared in constant time; no token leaves the dashboard read-only

Verified with a scripted headless-browser run against a real 3-node cluster: write, disconnect the leader, write again, reconnect. That run produced `docs/assets/dashboard-failover.gif`, used in the README. Design detail is in [`architecture.md`](architecture.md#live-dashboard).

### Deployed to the live cluster

The dashboard runs on the GCP k3s cluster at `http://34.60.149.77:8081/ui`, linked from the README. The token lives in a Kubernetes Secret, `kvstore-demo` (key `token`), created by hand on the cluster and never committed. `deployments/k8s/kvstore.yaml` passes it to each pod as `KV_DEMO_TOKEN` through a `secretKeyRef` marked `optional: true`. Without the Secret the pods still start and the dashboard is read-only. The image was rebuilt and pushed to GHCR as `:latest` and `:b8dbb0d`, and applying the manifest triggered a rolling update. The StatefulSet replaces one pod at a time, so the cluster kept a majority throughout. The token is read only at pod startup, so rotating it means updating the Secret and running `kubectl rollout restart`.

## Follow-up: benchmark (2026-10-15)

Metrics show live behavior but not a baseline to compare against, so `cmd/bench` (`kvbench`) was added. It measures sequential write latency (p50/p95/p99), concurrent write throughput, and failover time. Failover is measured by cutting the leader off through `/demo/isolate` and timing until another node is a connected leader and until a write commits on it. It calls the HTTP API directly rather than the client SDK, so redirects and retries don't distort the timings.

Two runs on a local 3-node cluster (i7-13650HX, SSD, fsync on every write):

| Metric | Result |
|---|---|
| Sequential write latency (1,000 writes) | p50 2.7–5.0 ms, p95 3.9–6.7 ms, p99 4.6–8.6 ms |
| Throughput, 8 concurrent clients | ~370–475 writes/s, 0 errors |
| Throughput, 32 concurrent clients | ~150–200 writes/s, 0 errors |
| Failover to a new leader (20 trials) | median 0.29–0.36 s, min 0.26 s, max 1.26 s |
| Failover to the first accepted write | median 0.29–0.38 s, max 1.27 s |

Failover time is dominated by the randomized 300–600 ms election timeout; the slowest trials look like split votes that needed a second round. The notable result is that **throughput falls as more clients are added**. The likely cause is that each write currently gets its own fsync and replication round, but this hasn't been profiled yet. Write batching is planned for Phase 10, and these numbers are the baseline to measure it against.

Other numbers gathered for the README at the same time:
- 76 tests, with 77% statement coverage across `internal/` when the integration tests are counted (`go test -coverpkg=./internal/... ./internal/... ./tests/...`). Per-package coverage understates `transport`, which is mostly exercised from `tests/integration`.
- About 3,300 non-blank lines of Go plus about 2,600 lines of tests.

The README was also expanded with an "At a glance" numbers table, the five detailed Mermaid diagrams (copied back from `architecture.md`), Design decisions, How it's tested, and Performance sections.

## Follow-up: review fixes (2026-10-15)

A mock recruiter review of the whole project (one senior technical reviewer, one non-technical reviewer, and a defender checking each claim against the code) found two real correctness bugs. Both are fixed, and each has a regression test that fails on the old code.

- **Torn-write recovery lost later writes.** `loadLog` stopped reading at a half-written record but left its bytes in the file. Because `AppendEntries` opens the log with `O_APPEND`, every later entry (fsync'd and acknowledged) landed *behind* the garbage, and the next restart's scan stopped before reaching it. Recovery now truncates the file to the last good record and fsyncs before anything else is appended. `TestFileStorage_RecoversFromTornTrailingWrite` now reopens the storage after the post-recovery append.
- **Follower commit clamp.** A follower clamped `commitIndex` to `min(LeaderCommit, LastIndex())`. Raft's Figure 2 says to clamp to the index of the last *new* entry, `PrevLogIndex + len(Entries)`, because only entries up to there are known to match the leader. With a stale tail from an older term past the batch (for example when the leader caps a batch at 256 entries), the old code could commit and apply entries the leader never had. `TestReplication_FollowerDoesNotCommitUncheckedStaleTail` covers it.

Other changes from the same review:

- `/debug/pprof` moved off the public client port onto its own listener, `--debug-addr`, which is off by default. The k8s manifest binds it to the pod's loopback (`127.0.0.1:6060`), reachable only via `kubectl port-forward`. `/metrics` stays on the client port.
- `TestPutGet` read its key immediately after writing it. Reads aren't linearizable yet, so it failed about 1 run in 10; it now polls with `WaitForValue`.
- References to private planning files were removed from tracked code comments and docs.
- README: failover wording aligned with the dashboard ("under a second, typically 0.3 s"); the stale-read limitation reworded accurately ("for as long as the partition lasts"); client request deduplication added to the Phase 10 list; the detailed diagrams folded into a collapsible block; Engineering highlights moved up; and a Docker-based "Try it" plus an About section added.

Still open from the review, in rough priority order:

- parent-directory fsync after each rename, and a per-record CRC;
- client request deduplication;
- read-index plus check-quorum;
- HTTPS on a real domain;
- Kubernetes probes, a PodDisruptionBudget, resource limits, and SHA-tagged images published from CI;
- a randomized chaos test with a linearizability check.

Separately, running the integration suite repeatedly (`-count=3` or more) on Windows occasionally fails with "connection refused" or a hard-state rename "Access is denied". The same failure rate shows up on the pre-fix code, so it's an existing test-harness or Windows file-locking issue, not something these fixes introduced. Single runs pass.