// Package integration spins up real kvstored clusters in-process — real
// HTTP+gob Raft transport, real on-disk FileStorage in per-node temp
// directories, real client-facing HTTP servers with leader-redirect wired
// up — on OS-assigned localhost ports. It's the same wiring cmd/server's
// main() does, minus the OS-process boundary, so these tests exercise the
// actual code path a deployed node runs rather than the in-memory fake
// transport internal/raft's own unit tests use.
package integration

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/client"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/server"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/transport"
)

// Timing tuned tight so elections and failover settle in well under a
// second — matching the timeouts internal/raft's own tests use, not the
// production defaults (which favor stability over test speed).
const (
	tickInterval       = 10 * time.Millisecond
	electionTimeoutMin = 100 * time.Millisecond
	electionTimeoutMax = 200 * time.Millisecond
	heartbeatInterval  = 20 * time.Millisecond
	rpcTimeout         = 100 * time.Millisecond
)

// testNode is one cluster member. No locking: every Cluster/testNode
// method here is only ever called from the single test goroutine that
// owns the Cluster — the raft.Node underneath already handles its own
// concurrency (single runloop goroutine, channel-based entry points).
type testNode struct {
	id         raft.NodeID
	raftAddr   string // host:port, stable across Kill/Restart
	clientAddr string // host:port, stable across Kill/Restart
	dataDir    string // stable across Kill/Restart — that's what makes Restart a real recovery test

	node      *raft.Node
	raftSrv   *http.Server
	clientSrv *http.Server
	alive     bool
}

// Cluster is a set of testNodes, all mutually peered, reachable over real
// HTTP on 127.0.0.1.
type Cluster struct {
	t          *testing.T
	nodes      map[raft.NodeID]*testNode
	raftAddrs  map[raft.NodeID]string // every node's peer address, by ID — stable for the cluster's lifetime
	clientURLs map[raft.NodeID]string // every node's client-facing URL, by ID — same

	sdkClient *client.Client // lazily built, then reused so the leader cache actually helps
}

// startCluster brings up n nodes with production-default snapshotting
// (effectively off at this scale). Use startClusterWithSnapshotInterval
// directly for a test that needs compaction to actually fire.
func startCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	return startClusterWithSnapshotInterval(t, n, 0)
}

// startClusterWithSnapshotInterval brings up n mutually-peered nodes and
// waits for every HTTP server to be listening (not for a leader to be
// elected — callers that need one should call WaitForLeader). Registers a
// t.Cleanup to tear everything down, so tests don't need their own defer.
func startClusterWithSnapshotInterval(t *testing.T, n int, snapshotInterval uint64) *Cluster {
	t.Helper()

	type reserved struct {
		id           raft.NodeID
		raftLn, clLn net.Listener
		dataDir      string
	}

	var all []reserved
	for i := 1; i <= n; i++ {
		id := raft.NodeID(fmt.Sprintf("node%d", i))

		// Listeners are opened up front, before any node is constructed —
		// every node's Peers/Transport/clientURLs need the full address
		// book from the start, and opening the real listener (rather than
		// picking a free port, closing it, and hoping nothing else grabs
		// it before the real bind) avoids a TOCTOU race entirely.
		raftLn, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen (raft, %s): %v", id, err)
		}
		clLn, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen (client, %s): %v", id, err)
		}
		all = append(all, reserved{id: id, raftLn: raftLn, clLn: clLn, dataDir: t.TempDir()})
	}

	c := &Cluster{
		t:          t,
		nodes:      make(map[raft.NodeID]*testNode, n),
		raftAddrs:  make(map[raft.NodeID]string, n),
		clientURLs: make(map[raft.NodeID]string, n),
	}
	for _, r := range all {
		c.raftAddrs[r.id] = r.raftLn.Addr().String()
		c.clientURLs[r.id] = "http://" + r.clLn.Addr().String()
	}

	for _, r := range all {
		c.nodes[r.id] = c.startNode(r.id, r.dataDir, r.raftLn, r.clLn, snapshotInterval)
	}

	t.Cleanup(c.Stop)
	return c
}

// startNode wires up one node exactly like cmd/server/main.go does: open
// (or reopen) FileStorage, restore from a snapshot if one exists, build
// the raft.Node, and serve both the peer-facing and client-facing HTTP
// APIs on the given (already-listening) listeners. Shared by both initial
// cluster startup and Restart, so a restarted node goes through the same
// recovery path a real process restart would.
func (c *Cluster) startNode(id raft.NodeID, dataDir string, raftLn, clientLn net.Listener, snapshotInterval uint64) *testNode {
	t := c.t

	fs, err := raft.NewFileStorage(dataDir, id)
	if err != nil {
		t.Fatalf("NewFileStorage(%s): %v", id, err)
	}
	initTerm, initVotedFor, _, _ := fs.InitialHardState()

	kv := store.New()
	var initSnapshotIndex uint64
	snapData, snapIndex, _, err := fs.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot(%s): %v", id, err)
	}
	if snapIndex > 0 {
		if err := kv.Restore(snapData); err != nil {
			t.Fatalf("Restore(%s): %v", id, err)
		}
		initSnapshotIndex = snapIndex
	}

	var peers []raft.NodeID
	transportAddrs := make(map[raft.NodeID]string, len(c.raftAddrs)-1)
	for peerID, addr := range c.raftAddrs {
		if peerID == id {
			continue
		}
		peers = append(peers, peerID)
		transportAddrs[peerID] = addr
	}

	cfg := raft.Config{
		ID:                   id,
		Peers:                peers,
		Storage:              fs,
		Transport:            transport.NewClient(transportAddrs),
		SM:                   kv,
		Logger:               raft.NewDefaultLogger("[" + string(id) + "] "),
		InitialTerm:          initTerm,
		InitialVotedFor:      initVotedFor,
		InitialSnapshotIndex: initSnapshotIndex,
		SnapshotInterval:     snapshotInterval,
		TickInterval:         tickInterval,
		ElectionTimeoutMin:   electionTimeoutMin,
		ElectionTimeoutMax:   electionTimeoutMax,
		HeartbeatInterval:    heartbeatInterval,
		RPCTimeout:           rpcTimeout,
	}
	node, err := raft.NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode(%s): %v", id, err)
	}
	go node.Run()

	raftSrv := &http.Server{Handler: transport.NewRaftHandler(node)}
	go raftSrv.Serve(raftLn)

	clientSrv := &http.Server{Handler: server.New(kv, node, c.clientURLs, nil)}
	go clientSrv.Serve(clientLn)

	return &testNode{
		id:         id,
		raftAddr:   raftLn.Addr().String(),
		clientAddr: clientLn.Addr().String(),
		dataDir:    dataDir,
		node:       node,
		raftSrv:    raftSrv,
		clientSrv:  clientSrv,
		alive:      true,
	}
}

// Kill stops id's Raft runloop and closes its HTTP listeners — simulating
// the process dying — but leaves its on-disk data directory untouched, so
// Restart can bring it back from exactly where it left off. Safe to call
// on an already-dead node (no-op).
func (c *Cluster) Kill(id raft.NodeID) {
	tn := c.nodes[id]
	if !tn.alive {
		return
	}
	tn.node.Stop()
	tn.raftSrv.Close()
	tn.clientSrv.Close()
	tn.alive = false
}

// Restart brings a killed node back on the SAME addresses it had before
// (other live nodes' Transport already has those addresses baked in) and
// the SAME data directory, so it recovers from disk exactly like a real
// process restart — including catching up via InstallSnapshot if the rest
// of the cluster has compacted past what its own log still has.
func (c *Cluster) Restart(id raft.NodeID) {
	t := c.t
	old := c.nodes[id]
	if old.alive {
		t.Fatalf("Restart(%s): still alive, Kill it first", id)
	}

	raftLn, err := net.Listen("tcp", old.raftAddr)
	if err != nil {
		t.Fatalf("re-listen raft addr %s for %s: %v", old.raftAddr, id, err)
	}
	clientLn, err := net.Listen("tcp", old.clientAddr)
	if err != nil {
		t.Fatalf("re-listen client addr %s for %s: %v", old.clientAddr, id, err)
	}

	c.nodes[id] = c.startNode(id, old.dataDir, raftLn, clientLn, 0)
}

// Stop tears down every node. Registered automatically via t.Cleanup by
// startCluster; safe to call again (or early) from a test that wants
// explicit control over teardown timing.
func (c *Cluster) Stop() {
	for id := range c.nodes {
		c.Kill(id)
	}
}

// WaitForLeader polls every live node's Status (in-process, not over
// HTTP — this is cluster bookkeeping, not something under test) until
// exactly one reports Leader.
func (c *Cluster) WaitForLeader(t *testing.T, timeout time.Duration) *testNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *testNode
		count := 0
		for _, tn := range c.nodes {
			if !tn.alive {
				continue
			}
			status, err := tn.node.Status(context.Background())
			if err == nil && status.State == raft.Leader {
				leader = tn
				count++
			}
		}
		if count == 1 {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no single leader elected within %s", timeout)
	return nil
}

// KillOne waits for a leader, then kills an arbitrary NON-leader node and
// returns its ID — used by the snapshot-catchup test to simulate a
// follower that falls behind while the rest of the cluster keeps writing.
func (c *Cluster) KillOne() raft.NodeID {
	t := c.t
	leader := c.WaitForLeader(t, 5*time.Second)
	for id, tn := range c.nodes {
		if id == leader.id || !tn.alive {
			continue
		}
		c.Kill(id)
		return id
	}
	t.Fatalf("KillOne: no non-leader node available to kill")
	return ""
}

// WaitForCatchup waits until id's lastApplied reaches the current
// leader's commitIndex — i.e. it has genuinely caught up, whether via
// ordinary replication or InstallSnapshot.
func (c *Cluster) WaitForCatchup(t *testing.T, id raft.NodeID, timeout time.Duration) {
	t.Helper()
	leader := c.WaitForLeader(t, timeout)
	leaderStatus, err := leader.node.Status(context.Background())
	if err != nil {
		t.Fatalf("leader Status: %v", err)
	}
	target := leaderStatus.CommitIndex

	tn := c.nodes[id]
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		status, err := tn.node.Status(context.Background())
		if err == nil && status.LastApplied >= target {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %s never caught up to commitIndex %d within %s", id, target, timeout)
}

// sdk lazily builds one client.Client for the cluster's lifetime, with
// every node's address as a candidate endpoint, and reuses it — so its
// leader cache actually does something across repeated Put/Get calls
// instead of rediscovering the leader from scratch every time.
func (c *Cluster) sdk() *client.Client {
	if c.sdkClient != nil {
		return c.sdkClient
	}
	var endpoints []string
	for _, url := range c.clientURLs {
		endpoints = append(endpoints, url)
	}
	cl, err := client.New(client.Config{Endpoints: endpoints})
	if err != nil {
		c.t.Fatalf("client.New: %v", err)
	}
	c.sdkClient = cl
	return cl
}

// Put/Get go through the real smart-client SDK over real HTTP against
// whichever node's address it happens to pick — exercising the full
// stack end to end (transport, leader-redirect, retry) rather than
// shortcutting through a specific node's Propose in-process.
func (c *Cluster) Put(ctx context.Context, key string, value []byte) error {
	return c.sdk().Put(ctx, key, value)
}

func (c *Cluster) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.sdk().Get(ctx, key)
}

// WaitForValue polls Get(key) until it returns want or timeout elapses.
// Reads in this project aren't linearizable yet (see CLAUDE.md's
// read-index note — Phase 10 work): a Get can be served by any node, not
// just the leader, so a read immediately after a write or a leadership
// change can transiently miss data that's already durably committed but
// not yet applied on whichever node happens to answer. Tests that assert
// on data visibility after a failover/restart should poll through this
// rather than asserting on a single Get, to test real behavior instead of
// getting flaky on exactly the staleness window the project already
// documents as a known, accepted gap.
func (c *Cluster) WaitForValue(t *testing.T, ctx context.Context, key, want string, timeout time.Duration) {
	t.Helper()
	var lastGot []byte
	var lastOK bool
	var lastErr error
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		lastGot, lastOK, lastErr = c.Get(ctx, key)
		if lastErr == nil && lastOK && string(lastGot) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Get(%s) never converged to %q within %s: last result = (%q, %v, %v)", key, want, timeout, lastGot, lastOK, lastErr)
}
