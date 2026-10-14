package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/metrics"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/server"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/transport"
)

// fatal logs msg as an error (with the same structured fields/format as
// every other log line) and exits — slog has no built-in Fatal, unlike the
// stdlib log package this replaces.
func fatal(logger *slog.Logger, msg string, args ...any) {
	logger.Error(msg, args...)
	os.Exit(1)
}

func main() {
	clientAddr := flag.String("client-addr", "127.0.0.1:8000", "address for the client-facing HTTP")
	id := flag.String("id", "", "this node's Raft ID (enables Raft mode when non-empty)") // empty id = phase 1 standalone mode, no raft at all
	raftAddr := flag.String("raft-addr", "", "address for the Raft peer-facing HTTP server")
	peers := flag.String("peers", "", "comma-separated id=host:port list of all nodes in the cluster, including this one")
	clientPeers := flag.String("client-peers", "", "comma-separated id=host:port list of every node's CLIENT-facing address, including this one; used to point a caller at the leader on a 421 redirect. Optional — a follower falls back to a plain 503 for anyone it can't resolve a URL for")
	dataDir := flag.String("data-dir", "./data", "directory for this node's persistent Raft state")
	snapshotInterval := flag.Uint64("snapshot-interval", 1000, "take a snapshot and compact the log every N applied entries")
	logFormat := flag.String("log-format", "text", "log output format: text (human-readable) or json (production/log-aggregation)")
	demoToken := flag.String("demo-token", os.Getenv("KV_DEMO_TOKEN"), "shared secret that unlocks the dashboard's \"disconnect node\" button (/demo/isolate); empty leaves the dashboard read-only. Defaults to $KV_DEMO_TOKEN")
	flag.Parse()

	var handler slog.Handler
	if *logFormat == "json" {
		handler = slog.NewJSONHandler(os.Stderr, nil)
	} else {
		handler = slog.NewTextHandler(os.Stderr, nil)
	}
	logger := slog.New(handler)
	if *id != "" {
		logger = logger.With("nodeID", *id)
	}

	reg := metrics.New()

	if *id == "" {
		kv := store.New()
		srv := server.New(kv, nil, nil, reg)

		logger.Info("kvstored listening", "addr", *clientAddr)
		if err := http.ListenAndServe(*clientAddr, srv); err != nil {
			fatal(logger, "server exited", "error", err)
		}
		return
	}

	peerAddrs, err := parsePeers(*peers)
	if err != nil {
		fatal(logger, "invalid --peers", "error", err)
	}
	if _, ok := peerAddrs[raft.NodeID(*id)]; !ok {
		fatal(logger, "invalid --peers: this node's --id is not present in --peers", "id", *id)
	}

	var cfgPeers []raft.NodeID
	transportAddrs := make(map[raft.NodeID]string, len(peerAddrs)-1)
	for peerID, addr := range peerAddrs {
		if peerID == raft.NodeID(*id) {
			continue
		}
		cfgPeers = append(cfgPeers, peerID)
		transportAddrs[peerID] = addr
	}

	// clientURLs feeds the leader-redirect header on a 421 — see
	// server.Server.requireLeader. Left nil (not an error) if --client-peers
	// wasn't passed; a follower just falls back to 503 in that case.
	var clientURLs map[raft.NodeID]string
	if strings.TrimSpace(*clientPeers) != "" {
		addrs, err := parsePeers(*clientPeers)
		if err != nil {
			fatal(logger, "invalid --client-peers", "error", err)
		}
		clientURLs = make(map[raft.NodeID]string, len(addrs))
		for peerID, addr := range addrs {
			clientURLs[peerID] = "http://" + addr
		}
	}

	fs, err := raft.NewFileStorage(*dataDir, raft.NodeID(*id))
	if err != nil {
		fatal(logger, "failed to open storage", "error", err)
	}

	initTerm, initVotedFor, _, _ := fs.InitialHardState()

	kv := store.New()

	// If a snapshot survived from before a restart, restore the state
	// machine from it and seed the node's commitIndex/lastApplied at its
	// boundary — otherwise applyCommitted would try to replay entries
	// starting at index 1, which no longer exist once the log has been
	// compacted past that point.
	var initSnapshotIndex uint64
	snapData, snapIndex, _, err := fs.LoadSnapshot()
	if err != nil {
		fatal(logger, "failed to load snapshot", "error", err)
	}
	if snapIndex > 0 {
		if err := kv.Restore(snapData); err != nil {
			fatal(logger, "failed to restore state machine from snapshot", "index", snapIndex, "error", err)
		}
		initSnapshotIndex = snapIndex
		logger.Info("restored state machine from snapshot", "index", snapIndex)
	}

	// Shared by the outbound RPC client and the inbound RPC handler so the
	// dashboard can cut this node off from its peers in both directions.
	partition := transport.NewPartition()

	cfg := raft.Config{
		ID:                   raft.NodeID(*id),
		Peers:                cfgPeers,
		Storage:              fs,
		Transport:            transport.NewClient(transportAddrs, partition),
		SM:                   kv,
		Logger:               logger,
		Metrics:              reg,
		InitialTerm:          initTerm,
		InitialVotedFor:      initVotedFor,
		InitialSnapshotIndex: initSnapshotIndex,
		SnapshotInterval:     *snapshotInterval,
	}

	node, err := raft.NewNode(cfg)
	if err != nil {
		fatal(logger, "failed to create raft node", "error", err)
	}

	go node.Run()

	raftHandler := transport.NewRaftHandler(node, partition)
	raftSrv := &http.Server{Addr: *raftAddr, Handler: raftHandler}
	go func() {
		logger.Info("raft peer server listening", "addr", *raftAddr)
		if err := raftSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(logger, "raft server exited", "error", err)
		}
	}()

	// Phase 3: client writes are proposed through Raft and applied once a
	// majority has persisted them. Phase 6: a follower redirects the caller
	// to the leader instead of just failing.
	clientHandler := server.New(kv, node, clientURLs, reg)
	clientHandler.EnableDemo(server.DemoConfig{NodeID: raft.NodeID(*id), Token: *demoToken, Isolator: partition})
	clientSrv := &http.Server{Addr: *clientAddr, Handler: clientHandler}
	go func() {
		logger.Info("kvstored listening", "addr", *clientAddr)
		if err := clientSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(logger, "client server exited", "error", err)
		}
	}()

	// block here until ctrl+c or the OS asks us to die
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("shutting down")
	node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := raftSrv.Shutdown(ctx); err != nil {
		logger.Error("raft server shutdown", "error", err)
	}
	if err := clientSrv.Shutdown(ctx); err != nil {
		logger.Error("client server shutdown", "error", err)
	}
	logger.Info("shutdown complete")
}

func parsePeers(peers string) (map[raft.NodeID]string, error) {
	if strings.TrimSpace(peers) == "" {
		return nil, errors.New("--peers must not be empty")
	}

	result := make(map[raft.NodeID]string)
	for _, entry := range strings.Split(peers, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, errors.New("malformed --peers entry " + entry + ", expected id=host:port")
		}
		result[raft.NodeID(parts[0])] = parts[1]
	}
	if len(result) == 0 {
		return nil, errors.New("--peers must not be empty")
	}
	return result, nil
}
