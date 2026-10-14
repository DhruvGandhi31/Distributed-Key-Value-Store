package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DhruvGandhi31/Distributed-Key-Value-Store/internal/raft"
)

func TestPartition_NilIsNeverIsolated(t *testing.T) {
	var p *Partition
	if p.Isolated() || p.Remaining() != 0 {
		t.Fatal("nil Partition should never report isolated")
	}
}

func TestPartition_IsolatesThenHeals(t *testing.T) {
	p := NewPartition()
	if !p.IsolateFor(50 * time.Millisecond) {
		t.Fatal("first IsolateFor should succeed")
	}
	if !p.Isolated() {
		t.Fatal("expected isolated right after IsolateFor")
	}
	if p.IsolateFor(time.Second) {
		t.Fatal("IsolateFor while already isolated should be refused")
	}
	time.Sleep(80 * time.Millisecond)
	if p.Isolated() {
		t.Fatal("expected partition to heal on its own")
	}
	if !p.IsolateFor(time.Millisecond) {
		t.Fatal("IsolateFor after healing should succeed again")
	}
}

// While isolated, outbound RPCs must fail without reaching the peer at all.
func TestClient_IsolatedFailsWithoutNetwork(t *testing.T) {
	hits := 0
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer peer.Close()

	p := NewPartition()
	p.IsolateFor(time.Minute)
	c := NewClient(map[raft.NodeID]string{"n2": peer.Listener.Addr().String()}, p)

	_, err := c.SendAppendEntries(context.Background(), "n2", raft.AppendEntriesArgs{})
	if !errors.Is(err, ErrIsolated) {
		t.Fatalf("err = %v, want ErrIsolated", err)
	}
	if hits != 0 {
		t.Fatalf("peer received %d requests while isolated, want 0", hits)
	}
}

func TestRaftHandler_IsolatedRefusesInbound(t *testing.T) {
	p := NewPartition()
	p.IsolateFor(time.Minute)
	h := NewRaftHandler(nil, p) // node is never reached while isolated

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/raft/append", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
