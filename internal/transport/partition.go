package transport

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrIsolated is returned by every outbound RPC while this node is isolated.
var ErrIsolated = errors.New("transport: node is isolated from its peers")

// Partition simulates a network partition around one node: while it is
// active, the node's outbound Raft RPCs fail immediately and its inbound
// ones are refused, exactly as if its network cable were pulled. The Raft
// runloop keeps running untouched, so what the cluster does next (a new
// election, the isolated node's term climbing, a step-down once it heals)
// is the real algorithm reacting, not a scripted animation.
//
// A nil *Partition is valid and never isolated.
type Partition struct {
	until atomic.Int64 // unix nanos; isolated while now < until
}

func NewPartition() *Partition { return &Partition{} }

// IsolateFor cuts the node off for d, then heals automatically. It returns
// false (and changes nothing) if the node is already isolated.
func (p *Partition) IsolateFor(d time.Duration) bool {
	now := time.Now().UnixNano()
	for {
		cur := p.until.Load()
		if now < cur {
			return false
		}
		if p.until.CompareAndSwap(cur, now+int64(d)) {
			return true
		}
	}
}

// Remaining reports how much longer the node stays isolated (0 if it isn't).
func (p *Partition) Remaining() time.Duration {
	if p == nil {
		return 0
	}
	left := time.Until(time.Unix(0, p.until.Load()))
	if left < 0 {
		return 0
	}
	return left
}

func (p *Partition) Isolated() bool { return p.Remaining() > 0 }
