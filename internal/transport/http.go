package transport

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"net/http"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

var _ raft.Transport = (*Client)(nil)

// Client sends Raft RPCs to peers over HTTP+gob. It carries no top-level
// http.Client timeout — each RPC's deadline is entirely driven by the
// context the caller passes in (Node's RPCTimeout), so a client-level
// timeout would either double up on that deadline or cut it short.
type Client struct {
	httpc *http.Client
	addrs map[raft.NodeID]string
}

func NewClient(addrs map[raft.NodeID]string) *Client {
	return &Client{httpc: &http.Client{}, addrs: addrs}
}

func (c *Client) SendRequestVote(ctx context.Context, peer raft.NodeID, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply

	addr, ok := c.addrs[peer]
	if !ok {
		return reply, fmt.Errorf("transport: unknown peer %q", peer)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		return reply, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/raft/vote", &buf)
	if err != nil {
		return reply, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("transport: peer %q returned status %d", peer, resp.StatusCode)
	}

	if err := gob.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func (c *Client) SendInstallSnapshot(ctx context.Context, peer raft.NodeID, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, error) {
	var reply raft.InstallSnapshotReply

	addr, ok := c.addrs[peer]
	if !ok {
		return reply, fmt.Errorf("transport: unknown peer %q", peer)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		return reply, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/raft/install-snapshot", &buf)
	if err != nil {
		return reply, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("transport: peer %q returned status %d", peer, resp.StatusCode)
	}

	if err := gob.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply, err
	}
	return reply, nil
}

func (c *Client) SendAppendEntries(ctx context.Context, peer raft.NodeID, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply

	addr, ok := c.addrs[peer]
	if !ok {
		return reply, fmt.Errorf("transport: unknown peer %q", peer)
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		return reply, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/raft/append", &buf)
	if err != nil {
		return reply, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpc.Do(req)
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("transport: peer %q returned status %d", peer, resp.StatusCode)
	}

	if err := gob.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply, err
	}
	return reply, nil
}
