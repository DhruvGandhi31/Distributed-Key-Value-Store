// Package raft implements a from-scratch Raft consensus module. All Raft
// state (term, votedFor, log, commitIndex) is owned by a single runloop
// goroutine (Node.Run); every other goroutine talks to it through channels,
// never through shared-memory locking.
package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Config configures a new Node.
type Config struct {
	ID        NodeID
	Peers     []NodeID
	Storage   Storage
	Transport Transport
	SM        StateMachine
	Logger    Logger

	InitialTerm          uint64
	InitialVotedFor      NodeID
	InitialSnapshotIndex uint64 // seeds lastApplied/commitIndex after a restart with a snapshot on disk

	TickInterval       time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration

	// SnapshotInterval triggers an automatic snapshot every N applied
	// entries (0 uses defaultSnapshotInterval). Set it small in tests to
	// exercise compaction without needing thousands of entries.
	SnapshotInterval uint64
}

const (
	defaultTickInterval       = 50 * time.Millisecond
	defaultElectionTimeoutMin = 300 * time.Millisecond
	defaultElectionTimeoutMax = 600 * time.Millisecond
	defaultHeartbeatInterval  = 50 * time.Millisecond
	defaultRPCTimeout         = 200 * time.Millisecond
	defaultSnapshotInterval   = 1000
)

// Status is a point-in-time snapshot of a Node's Raft role, exposed via
// Node.Status without exposing the internal mutable state directly.
type Status struct {
	State       State
	Leader      NodeID
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
}

type requestVoteMsg struct {
	args  RequestVoteArgs
	reply chan RequestVoteReply
}

type appendEntriesMsg struct {
	args  AppendEntriesArgs
	reply chan AppendEntriesReply
}

type voteResult struct {
	term  uint64
	peer  NodeID
	reply RequestVoteReply
}

// appendResult carries back what was sent, not just the reply, since
// matchIndex bookkeeping on success needs to know prevLogIndex+numEntries,
// and both fields must reflect the specific request this reply answers
// (not whatever nextIndex[peer] happens to be by the time the reply lands).
type appendResult struct {
	term         uint64
	peer         NodeID
	prevLogIndex uint64
	numEntries   int
	reply        AppendEntriesReply
}

// proposal is a client command submitted via Propose, carried into the
// runloop over proposeCh.
type proposal struct {
	data   []byte
	result chan proposalResult
}

type proposalResult struct {
	value interface{}
	err   error
}

type installSnapshotMsg struct {
	args  InstallSnapshotArgs
	reply chan InstallSnapshotReply
}

// installSnapshotResult carries back the boundary that was sent (not just
// the reply), for the same reason appendResult does: nextIndex/matchIndex
// bookkeeping needs to know what this specific reply is answering for.
type installSnapshotResult struct {
	term      uint64
	peer      NodeID
	lastIndex uint64
	reply     InstallSnapshotReply
}

// Node is a single Raft participant. All fields below are only ever read
// or written from within Run's runloop goroutine; external callers only
// interact through the channel-based entry points (HandleRequestVote,
// HandleAppendEntries, Status, Stop).
type Node struct {
	id        NodeID
	peers     []NodeID
	storage   Storage
	transport Transport
	sm        StateMachine
	logger    Logger

	rpcTimeout time.Duration

	// Persistent Raft state (must be durable before it is acted on).
	currentTerm uint64
	votedFor    NodeID

	// Volatile Raft state.
	state       State
	leader      NodeID
	commitIndex uint64
	lastApplied uint64

	tickInterval time.Duration

	electionElapsed  int
	heartbeatElapsed int

	electionTicksMin        int
	electionTicksMax        int
	heartbeatTicks          int
	randomizedElectionTicks int

	rng *rand.Rand

	votesReceived map[NodeID]bool

	// Leader-only replication bookkeeping, (re)initialized in becomeLeader.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// pendingProposals maps a not-yet-applied log index to the channel
	// waiting on its outcome. Entries are removed here (and their channel
	// signaled) either when applyCommitted reaches that index, or when the
	// node steps down before that happens.
	pendingProposals map[uint64]chan proposalResult

	snapshotInterval uint64

	requestVoteCh           chan requestVoteMsg
	appendEntriesCh         chan appendEntriesMsg
	installSnapshotCh       chan installSnapshotMsg
	voteResultCh            chan voteResult
	appendResultCh          chan appendResult
	installSnapshotResultCh chan installSnapshotResult
	proposeCh               chan proposal
	queryCh                 chan chan Status
	stopCh                  chan struct{}
	stopOnce                sync.Once
}

// NewNode constructs a Node from cfg, applying defaults for any zero-valued
// timing fields. It does not start the runloop — call Run in a goroutine.
func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("raft: Config.ID must not be empty")
	}
	if cfg.Storage == nil {
		return nil, fmt.Errorf("raft: Config.Storage must not be nil")
	}
	if cfg.Transport == nil {
		return nil, fmt.Errorf("raft: Config.Transport must not be nil")
	}
	if cfg.SM == nil {
		return nil, fmt.Errorf("raft: Config.SM must not be nil")
	}

	if cfg.TickInterval <= 0 {
		cfg.TickInterval = defaultTickInterval
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = defaultElectionTimeoutMin
	}
	if cfg.ElectionTimeoutMax <= 0 {
		cfg.ElectionTimeoutMax = defaultElectionTimeoutMax
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = defaultRPCTimeout
	}
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = defaultSnapshotInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = NewDefaultLogger(string(cfg.ID) + " ")
	}

	electionTicksMin := int(cfg.ElectionTimeoutMin / cfg.TickInterval)
	electionTicksMax := int(cfg.ElectionTimeoutMax / cfg.TickInterval)
	if electionTicksMin < 1 {
		electionTicksMin = 1
	}
	if electionTicksMax < electionTicksMin {
		electionTicksMax = electionTicksMin
	}
	heartbeatTicks := int(cfg.HeartbeatInterval / cfg.TickInterval)
	if heartbeatTicks < 1 {
		heartbeatTicks = 1
	}

	seed := time.Now().UnixNano()
	for _, c := range cfg.ID {
		seed = seed*31 + int64(c)
	}

	n := &Node{
		id:        cfg.ID,
		peers:     cfg.Peers,
		storage:   cfg.Storage,
		transport: cfg.Transport,
		sm:        cfg.SM,
		logger:    cfg.Logger,

		rpcTimeout: cfg.RPCTimeout,

		currentTerm: cfg.InitialTerm,
		votedFor:    cfg.InitialVotedFor,

		state:       Follower,
		leader:      NoLeader,
		commitIndex: cfg.InitialSnapshotIndex,
		lastApplied: cfg.InitialSnapshotIndex,

		tickInterval: cfg.TickInterval,

		electionTicksMin: electionTicksMin,
		electionTicksMax: electionTicksMax,
		heartbeatTicks:   heartbeatTicks,

		rng: rand.New(rand.NewSource(seed)),

		votesReceived: make(map[NodeID]bool),

		nextIndex:        make(map[NodeID]uint64),
		matchIndex:       make(map[NodeID]uint64),
		pendingProposals: make(map[uint64]chan proposalResult),

		snapshotInterval: cfg.SnapshotInterval,

		requestVoteCh:           make(chan requestVoteMsg),
		appendEntriesCh:         make(chan appendEntriesMsg),
		installSnapshotCh:       make(chan installSnapshotMsg),
		voteResultCh:            make(chan voteResult),
		appendResultCh:          make(chan appendResult),
		installSnapshotResultCh: make(chan installSnapshotResult),
		proposeCh:               make(chan proposal),
		queryCh:                 make(chan chan Status),
		stopCh:                  make(chan struct{}),
	}
	return n, nil
}

// Run is the single-goroutine runloop that owns all Raft state. Call it in
// its own goroutine; it returns when Stop is called.
func (n *Node) Run() {
	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	n.resetElectionTimer()

	for {
		select {
		case <-ticker.C:
			n.tick()
		case msg := <-n.requestVoteCh:
			msg.reply <- n.handleRequestVote(msg.args)
		case msg := <-n.appendEntriesCh:
			msg.reply <- n.handleAppendEntries(msg.args)
		case msg := <-n.installSnapshotCh:
			msg.reply <- n.installSnapshot(msg.args)
		case res := <-n.voteResultCh:
			n.handleVoteResult(res)
		case res := <-n.appendResultCh:
			n.handleAppendResult(res)
		case res := <-n.installSnapshotResultCh:
			n.handleInstallSnapshotResult(res)
		case p := <-n.proposeCh:
			n.handlePropose(p)
		case replyCh := <-n.queryCh:
			replyCh <- n.status()
		case <-n.stopCh:
			n.failPendingProposals(ErrShutdown)
			return
		}
	}
}

// Stop signals the runloop to exit. Safe to call more than once.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopCh)
	})
}

// HandleRequestVote is the external entry point for an incoming RequestVote
// RPC. It hands the request to the runloop and waits for the result.
func (n *Node) HandleRequestVote(ctx context.Context, args RequestVoteArgs) (RequestVoteReply, error) {
	msg := requestVoteMsg{args: args, reply: make(chan RequestVoteReply, 1)}

	select {
	case n.requestVoteCh <- msg:
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	case <-n.stopCh:
		return RequestVoteReply{}, ErrShutdown
	}

	select {
	case reply := <-msg.reply:
		return reply, nil
	case <-ctx.Done():
		return RequestVoteReply{}, ctx.Err()
	case <-n.stopCh:
		return RequestVoteReply{}, ErrShutdown
	}
}

// HandleAppendEntries is the external entry point for an incoming
// AppendEntries RPC. It hands the request to the runloop and waits for the
// result.
func (n *Node) HandleAppendEntries(ctx context.Context, args AppendEntriesArgs) (AppendEntriesReply, error) {
	msg := appendEntriesMsg{args: args, reply: make(chan AppendEntriesReply, 1)}

	select {
	case n.appendEntriesCh <- msg:
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	case <-n.stopCh:
		return AppendEntriesReply{}, ErrShutdown
	}

	select {
	case reply := <-msg.reply:
		return reply, nil
	case <-ctx.Done():
		return AppendEntriesReply{}, ctx.Err()
	case <-n.stopCh:
		return AppendEntriesReply{}, ErrShutdown
	}
}

// HandleInstallSnapshot is the external entry point for an incoming
// InstallSnapshot RPC. It hands the request to the runloop and waits for
// the result.
func (n *Node) HandleInstallSnapshot(ctx context.Context, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	msg := installSnapshotMsg{args: args, reply: make(chan InstallSnapshotReply, 1)}

	select {
	case n.installSnapshotCh <- msg:
	case <-ctx.Done():
		return InstallSnapshotReply{}, ctx.Err()
	case <-n.stopCh:
		return InstallSnapshotReply{}, ErrShutdown
	}

	select {
	case reply := <-msg.reply:
		return reply, nil
	case <-ctx.Done():
		return InstallSnapshotReply{}, ctx.Err()
	case <-n.stopCh:
		return InstallSnapshotReply{}, ErrShutdown
	}
}

// Propose submits data to be replicated as a new log entry. It blocks until
// the entry is committed and applied to the state machine (returning the
// StateMachine.Apply result), fails with ErrNotLeader if this node isn't
// the leader (either when the proposal is submitted or if it steps down
// before the entry commits), or returns ctx's error if ctx is canceled
// first.
func (n *Node) Propose(ctx context.Context, data []byte) (interface{}, error) {
	p := proposal{data: data, result: make(chan proposalResult, 1)}

	select {
	case n.proposeCh <- p:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrShutdown
	}

	select {
	case res := <-p.result:
		return res.value, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.stopCh:
		return nil, ErrShutdown
	}
}

// Status returns a snapshot of the node's current Raft role.
func (n *Node) Status(ctx context.Context) (Status, error) {
	replyCh := make(chan Status, 1)

	select {
	case n.queryCh <- replyCh:
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.stopCh:
		return Status{}, ErrShutdown
	}

	select {
	case status := <-replyCh:
		return status, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.stopCh:
		return Status{}, ErrShutdown
	}
}

// State returns the node's current Raft role.
func (n *Node) State(ctx context.Context) (State, error) {
	status, err := n.Status(ctx)
	return status.State, err
}

// Leader returns the node's current known leader.
func (n *Node) Leader(ctx context.Context) (NodeID, error) {
	status, err := n.Status(ctx)
	return status.Leader, err
}

// Term returns the node's current term.
func (n *Node) Term(ctx context.Context) (uint64, error) {
	status, err := n.Status(ctx)
	return status.Term, err
}
