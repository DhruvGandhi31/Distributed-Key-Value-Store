# Phase 6 — Smart Client and CLI

## Goal

A caller never needs to know which node is currently the Raft leader. A write to any node either succeeds directly (if it happens to land on the leader) or gets redirected there automatically; a node failure mid-session is invisible to the caller beyond a brief retry delay.

## What changed

### Server: HTTP 421 + X-Leader-URL instead of a flat 503

Through Phase 5, a write to a follower just returned 503 with no way for the caller to know where to actually go. `internal/server/server.go` gained `requireLeader`, checked before every `Propose`:

```go
func (s *Server) requireLeader(w http.ResponseWriter, ctx context.Context) bool {
    status, err := s.node.Status(ctx)
    if err != nil {
        http.Error(w, err.Error(), http.StatusServiceUnavailable)
        return false
    }
    if status.State == raft.Leader {
        return true
    }
    if url, ok := s.clientURLs[status.Leader]; ok {
        w.Header().Set("X-Leader-URL", url)
        http.Error(w, "not the leader", http.StatusMisdirectedRequest)
        return false
    }
    http.Error(w, "not the leader, and current leader is unknown", http.StatusServiceUnavailable)
    return false
}
```

`421 Misdirected Request` is the guide's suggested status — a somewhat unusual choice (it's normally an HTTP/2 thing about a connection being reused for the wrong origin) but distinctive enough that a client can safely treat it as "redirect me" without colliding with any status the KV API otherwise returns. Falling back to 503 when the leader is genuinely unknown (mid-election) or unmapped is deliberate: redirecting to nothing would just bounce the caller in a loop, whereas 503 tells the smart client to back off and retry the same request instead.

`Server` needed a new dependency to make this work: `clientURLs map[raft.NodeID]string`, the *client*-facing address of every node (not the Raft peer address `Transport` already has — those are different ports). `server.New` picked up a third parameter for it. `cmd/server/main.go` gained a `--client-peers` flag (same `id=host:port` format as `--peers`, parsed with the same helper) to populate it; it's optional — an empty/missing `--client-peers` just means every redirect falls back to 503 instead of a URL, not a startup error, so existing single-node or test setups that don't pass it keep working unchanged.

### A real client SDK: internal/client

New package, `internal/client`. `Client.do()` is the one place that knows about redirects and retries — `Put`/`Get`/`Delete`/`Scan` are thin wrappers over it:

```go
func (c *Client) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
    for attempt := 0; attempt < c.cfg.MaxAttempts; attempt++ {
        base := c.pickEndpoint(attempt)
        // ... issue the request ...
        switch resp.StatusCode {
        case http.StatusMisdirectedRequest:
            // follow X-Leader-URL, or clear the cached leader and retry
        case http.StatusServiceUnavailable:
            // leader unknown / node shutting down — back off and retry
        default:
            return resp, nil
        }
    }
}
```

`pickEndpoint` returns the cached leader if one is known, otherwise round-robins across the caller-supplied `Endpoints` set. A connection failure (dead node, not just "wrong node") clears the cached leader and falls through to the same round-robin, so the client recovers whether the problem is "this node isn't the leader" or "this node is completely gone." `Endpoints` is genuinely just a fallback set, not a discovery mechanism — if a caller supplies exactly one address and that one node is down, the client has nothing else to try and correctly gives up after `MaxAttempts`. Reads (`Get`/`Scan`) go through the same `do()` machinery for endpoint selection and retry, but a follower never 421s a `GET` — reads are still served locally by whichever node answers (see the existing read-index/lease-read note in `CLAUDE.md`; that's Phase 10 work, unrelated to this phase). Only writes are leader-routed here.

`cmd/client/main.go` was rewritten to call the SDK (`client.New(client.Config{Endpoints: ...})`) instead of raw `http.Post`/`http.Get`. `--addr` now accepts a comma-separated list, so `kvctl` can be pointed at more than one candidate node — the CLI itself stays leader-agnostic; the SDK does the discovery.

### cluster.ps1

Gained a `$clientPeers` string (same shape as the existing `$peers` string, but built from `ClientAddr` instead of `RaftAddr`) passed to every node via the new `--client-peers` flag, so the whole 3-or-4-node dev cluster gets real leader-redirect URLs out of the box.

## A test-design pitfall worth recording (not a code bug)

While verifying this live, killing the cluster's leader initially produced *no* re-election at all — term climbed into the hundreds with every node stuck as `Candidate` forever. That looked like a real bug at first, but it wasn't: `cluster.ps1`'s peer list always includes `node4` (by design, so it can join late without restarting the others — see Phase 5's docs), which makes the cluster's true size 4 even when `node4`'s process was never started. With only 3 processes running and the leader killed, only 2 of 4 configured peers were left alive — never a majority of 4, so Raft correctly refused to ever elect anyone. Rerunning the same test with all 4 nodes actually started (`-IncludeNode4`) elected a new leader normally once the old one was killed, since 3 of 4 is a real majority. Worth remembering for any future live test against this script: the peer *count* that matters for quorum math is `len($allNodes)`, not how many processes you happened to launch.

## Verified behavior (checkpoint)

Ran a real 4-node cluster (`-IncludeNode4`, so a single kill still leaves a majority):

```
.\scripts\cluster.ps1 -Action start -IncludeNode4
# node1 leader at term 1

# write through a FOLLOWER's address — the SDK should redirect transparently:
.\bin\kvctl.exe -addr http://127.0.0.1:8003 put before-failover value1
.\bin\kvctl.exe -addr http://127.0.0.1:8003 get before-failover   # → value1

# kill the leader:
Stop-Process -Id (Get-Content .\data\node1.pid) -Force

# 3 of 4 nodes remain — a real majority — so re-election succeeds:
# GET /cluster/status on a survivor → {"state":"Follower","leader":"node4","term":4,...}

# same SDK, pointed at a still-alive follower, writes through the NEW leader
# with zero code changes and zero manual redirection:
.\bin\kvctl.exe -addr http://127.0.0.1:8003 put after-failover value2
.\bin\kvctl.exe -addr http://127.0.0.1:8003 get after-failover    # → value2

# multi-endpoint fallback: first address is the dead node, SDK recovers anyway
.\bin\kvctl.exe -addr "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003" put via-fallback-list value3
# → succeeds
```

Also confirmed directly with `curl` against a follower (bypassing the SDK entirely) that the raw HTTP contract is right: a `PUT` to a non-leader returns `421` with an `X-Leader-Url: http://127.0.0.1:8002`-style header, not just a bare error.

## Known limitations (expected — later phases address these)

- **No unit/integration tests yet for `internal/client` or the redirect path in `internal/server`** — this phase was verified live end-to-end (above) rather than with automated tests, matching the guide's phase order: Phase 7 is dedicated to testing and CI. Worth writing tests for the redirect-and-retry state machine (cached leader, connection-failure fallback, 503 backoff) once that phase starts, since it's exactly the kind of retry/backoff logic that's easy to get subtly wrong and hard to eyeball-verify forever.
- **Reads still aren't linearizable** — unchanged from earlier phases, still gated on Phase 10's read-index/lease-read work. The smart client doesn't paper over this; `Get`/`Scan` intentionally aren't leader-routed.
- **No exponential jitter on backoff** — `do()`'s backoff is a flat linear ramp capped at 200ms. Fine at this project's scale; a thundering-herd scenario with many concurrent clients might want jitter added.

## How to reproduce

```powershell
go build -o bin/kvstored.exe ./cmd/server
go build -o bin/kvctl.exe ./cmd/client
go vet ./...
go test -race -timeout 120s ./...

.\scripts\cluster.ps1 -Action start -IncludeNode4
.\scripts\cluster.ps1 -Action status

.\bin\kvctl.exe -addr http://127.0.0.1:8003 put before-failover value1
.\bin\kvctl.exe -addr http://127.0.0.1:8003 get before-failover

# kill whichever node is currently leader:
.\scripts\cluster.ps1 -Action status   # note the leader's PID
Stop-Process -Id <that PID> -Force

.\scripts\cluster.ps1 -Action status   # a different node should now lead, higher term

.\bin\kvctl.exe -addr http://127.0.0.1:8003 put after-failover value2
.\bin\kvctl.exe -addr http://127.0.0.1:8003 get after-failover

.\scripts\cluster.ps1 -Action stop
```
