package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/server"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/store"
	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/transport"
)

func main() {
	clientAddr := flag.String("client-addr", "127.0.0.1:8000", "address for the client-facing HTTP")
	id := flag.String("id", "", "this node's Raft ID (enables Raft mode when non-empty)")
	raftAddr := flag.String("raft-addr", "", "address for the Raft peer-facing HTTP server")
	peers := flag.String("peers", "", "comma-separated id=host:port list of all nodes in the cluster, including this one")
	dataDir := flag.String("data-dir", "./data", "directory for this node's persistent Raft state")
	flag.Parse()

	if *id == "" {
		kv := store.New()
		srv := server.New(kv, nil)

		log.Printf("kvstored listening on %s", *clientAddr)
		if err := http.ListenAndServe(*clientAddr, srv); err != nil {
			log.Fatalf("server exited: %v", err)
		}
		return
	}

	peerAddrs, err := parsePeers(*peers)
	if err != nil {
		log.Fatalf("invalid --peers: %v", err)
	}
	if _, ok := peerAddrs[raft.NodeID(*id)]; !ok {
		log.Fatalf("invalid --peers: this node's --id %q is not present in --peers", *id)
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

	fs, err := raft.NewFileStorage(*dataDir, raft.NodeID(*id))
	if err != nil {
		log.Fatalf("failed to open storage: %v", err)
	}

	initTerm, initVotedFor, _, _ := fs.InitialHardState()

	kv := store.New()

	cfg := raft.Config{
		ID:              raft.NodeID(*id),
		Peers:           cfgPeers,
		Storage:         fs,
		Transport:       transport.NewClient(transportAddrs),
		SM:              kv,
		Logger:          raft.NewDefaultLogger("[" + *id + "] "),
		InitialTerm:     initTerm,
		InitialVotedFor: initVotedFor,
	}

	node, err := raft.NewNode(cfg)
	if err != nil {
		log.Fatalf("failed to create raft node: %v", err)
	}

	go node.Run()

	raftHandler := transport.NewRaftHandler(node)
	raftSrv := &http.Server{Addr: *raftAddr, Handler: raftHandler}
	go func() {
		log.Printf("raft peer server listening on %s", *raftAddr)
		if err := raftSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("raft server exited: %v", err)
		}
	}()

	// Phase 3: client writes are proposed through Raft and applied once a
	// majority has persisted them.
	clientHandler := server.New(kv, node)
	clientSrv := &http.Server{Addr: *clientAddr, Handler: clientHandler}
	go func() {
		log.Printf("kvstored listening on %s", *clientAddr)
		if err := clientSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("client server exited: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down")
	node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := raftSrv.Shutdown(ctx); err != nil {
		log.Printf("raft server shutdown: %v", err)
	}
	if err := clientSrv.Shutdown(ctx); err != nil {
		log.Printf("client server shutdown: %v", err)
	}
	log.Printf("shutdown complete")
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
