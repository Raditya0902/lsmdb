// Package raftnode runs the deterministic Raft state machine with persistence,
// transport, ticking, and ordered state-machine application.
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"lsmdb/internal/raft"
)

// StableStore persists effects emitted by the Raft module.
type StableStore interface {
	Persist(raft.Update) error
	Close() error
}

// Transport sends an asynchronous Raft message to another node.
type Transport interface {
	Send(context.Context, raft.Message) error
}

// StateMachine applies committed entries in strict index order.
type StateMachine interface {
	Apply(index uint64, command []byte) error
	AppliedIndex() uint64
	Snapshot() (index uint64, data []byte, err error)
	Restore(index uint64, data []byte) error
	Close() error
}

// Config controls the runtime rather than the Raft protocol.
type Config struct {
	TickInterval time.Duration
	QueueSize    int
	// SnapshotThreshold compacts after this many applied entries. Zero disables it.
	SnapshotThreshold uint64
}

type proposalResult struct {
	index uint64
	err   error
}

type proposalEvent struct {
	data   []byte
	result chan proposalResult
}

type membershipEvent struct {
	voters []uint64
	result chan proposalResult
}

type messageEvent struct {
	message raft.Message
	result  chan error
}

type statusEvent struct{ result chan raft.Status }
type stopEvent struct{ result chan error }
type readEvent struct{ result chan readResult }

type readResult struct {
	index uint64
	err   error
}

type pendingProposal struct{ result chan proposalResult }
type pendingRead struct {
	result chan readResult
	acks   map[uint64]struct{}
}

// Runtime serializes all access to a Raft Node in one event loop.
type Runtime struct {
	node              *raft.Node
	store             StableStore
	transport         Transport
	machine           StateMachine
	events            chan any
	outgoing          chan raft.Message
	done              chan struct{}
	snapshotThreshold uint64
	once              sync.Once
}

// Start begins a runtime. The supplied node must have been restored using the
// state machine's applied index.
func Start(cfg Config, node *raft.Node, store StableStore, transport Transport, machine StateMachine) (*Runtime, error) {
	if node == nil || store == nil || machine == nil {
		return nil, errors.New("raft runtime requires node, store, and state machine")
	}
	if cfg.TickInterval <= 0 {
		return nil, errors.New("raft runtime tick interval must be positive")
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}
	runtime := &Runtime{
		node: node, store: store, transport: transport, machine: machine,
		events: make(chan any, cfg.QueueSize), outgoing: make(chan raft.Message, cfg.QueueSize),
		done:              make(chan struct{}),
		snapshotThreshold: cfg.SnapshotThreshold,
	}
	go runtime.sendLoop()
	go runtime.run(cfg.TickInterval)
	return runtime, nil
}

// Propose waits until a command is committed and locally applied.
func (r *Runtime) Propose(ctx context.Context, command []byte) (uint64, error) {
	result := make(chan proposalResult, 1)
	event := proposalEvent{data: append([]byte(nil), command...), result: result}
	select {
	case r.events <- event:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.done:
		return 0, raft.ErrStopped
	}
	select {
	case response := <-result:
		return response.index, response.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.done:
		return 0, raft.ErrStopped
	}
}

// ChangeMembership waits for the final configuration entry to commit locally.
func (r *Runtime) ChangeMembership(ctx context.Context, voters []uint64) (uint64, error) {
	result := make(chan proposalResult, 1)
	event := membershipEvent{voters: append([]uint64(nil), voters...), result: result}
	select {
	case r.events <- event:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.done:
		return 0, raft.ErrStopped
	}
	select {
	case response := <-result:
		return response.index, response.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.done:
		return 0, raft.ErrStopped
	}
}

// Step enqueues an inbound protocol message.
func (r *Runtime) Step(ctx context.Context, message raft.Message) error {
	result := make(chan error, 1)
	select {
	case r.events <- messageEvent{message: message, result: result}:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return raft.ErrStopped
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return raft.ErrStopped
	}
}

// Status returns a race-free status snapshot.
func (r *Runtime) Status(ctx context.Context) (raft.Status, error) {
	result := make(chan raft.Status, 1)
	select {
	case r.events <- statusEvent{result: result}:
	case <-ctx.Done():
		return raft.Status{}, ctx.Err()
	case <-r.done:
		return raft.Status{}, raft.ErrStopped
	}
	select {
	case status := <-result:
		return status, nil
	case <-ctx.Done():
		return raft.Status{}, ctx.Err()
	case <-r.done:
		return raft.Status{}, raft.ErrStopped
	}
}

// LinearizableRead confirms leadership with a current-term quorum and returns
// an index that has already been applied locally.
func (r *Runtime) LinearizableRead(ctx context.Context) (uint64, error) {
	result := make(chan readResult, 1)
	select {
	case r.events <- readEvent{result: result}:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.done:
		return 0, raft.ErrStopped
	}
	select {
	case response := <-result:
		return response.index, response.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.done:
		return 0, raft.ErrStopped
	}
}

// Close stops the runtime after closing the state machine and stable store.
func (r *Runtime) Close() error {
	var err error
	r.once.Do(func() {
		result := make(chan error, 1)
		select {
		case r.events <- stopEvent{result: result}:
			err = <-result
		case <-r.done:
			err = nil
		}
	})
	return err
}

func (r *Runtime) run(tickInterval time.Duration) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	pending := make(map[uint64]pendingProposal)
	pendingReads := make(map[uint64]pendingRead)
	var nextReadContext uint64
	for {
		select {
		case <-ticker.C:
			if err := r.processUpdate(r.node.Tick(), pending, pendingReads); err != nil {
				r.fail(err, pending, pendingReads)
				return
			}
		case raw := <-r.events:
			switch event := raw.(type) {
			case proposalEvent:
				index, update, err := r.node.Propose(event.data)
				if err != nil {
					event.result <- proposalResult{err: err}
					continue
				}
				pending[index] = pendingProposal{result: event.result}
				if err := r.processUpdate(update, pending, pendingReads); err != nil {
					r.fail(err, pending, pendingReads)
					return
				}
			case membershipEvent:
				index, update, err := r.node.ProposeMembership(event.voters)
				if err != nil {
					event.result <- proposalResult{err: err}
					continue
				}
				if index <= r.node.Status().CommitIndex {
					event.result <- proposalResult{index: index}
					continue
				}
				pending[index] = pendingProposal{result: event.result}
				if err := r.processUpdate(update, pending, pendingReads); err != nil {
					r.fail(err, pending, pendingReads)
					return
				}
			case messageEvent:
				err := r.processUpdate(r.node.Step(event.message), pending, pendingReads)
				if err == nil && event.message.Type == raft.MsgAppendResponse && !event.message.Reject && event.message.Context != 0 {
					r.acknowledgeRead(event.message, pendingReads)
				}
				event.result <- err
				if err != nil {
					r.fail(err, pending, pendingReads)
					return
				}
			case statusEvent:
				event.result <- r.node.Status()
			case readEvent:
				nextReadContext++
				contextID := nextReadContext
				update, err := r.node.ReadProbe(contextID)
				if err != nil {
					event.result <- readResult{err: err}
					continue
				}
				pendingReads[contextID] = pendingRead{
					result: event.result, acks: map[uint64]struct{}{r.node.Status().ID: {}},
				}
				if err := r.processUpdate(update, pending, pendingReads); err != nil {
					r.fail(err, pending, pendingReads)
					return
				}
				if r.node.HasQuorum(map[uint64]struct{}{r.node.Status().ID: {}}) {
					event.result <- readResult{index: r.node.Status().CommitIndex}
					delete(pendingReads, contextID)
				}
			case stopEvent:
				err := r.closeResources()
				event.result <- err
				close(r.done)
				close(r.outgoing)
				return
			}
		}
	}
}

func (r *Runtime) processUpdate(update raft.Update, pending map[uint64]pendingProposal, pendingReads map[uint64]pendingRead) error {
	if err := r.store.Persist(update); err != nil {
		return fmt.Errorf("persist raft update: %w", err)
	}
	if update.Snapshot != nil && r.machine.AppliedIndex() < update.Snapshot.Index {
		if err := r.machine.Restore(update.Snapshot.Index, update.Snapshot.Data); err != nil {
			return fmt.Errorf("restore state snapshot %d: %w", update.Snapshot.Index, err)
		}
	}
	for _, message := range update.Messages {
		select {
		case r.outgoing <- message:
		case <-r.done:
			return raft.ErrStopped
		}
	}
	for _, entry := range update.Committed {
		if entry.Index <= r.machine.AppliedIndex() {
			if waiter, ok := pending[entry.Index]; ok {
				waiter.result <- proposalResult{index: entry.Index}
				delete(pending, entry.Index)
			}
			continue
		}
		if entry.Index != r.machine.AppliedIndex()+1 {
			return fmt.Errorf("commit apply gap: got %d, want %d", entry.Index, r.machine.AppliedIndex()+1)
		}
		command := entry.Data
		membership, err := raft.IsMembershipEntry(entry.Data)
		if err != nil {
			return fmt.Errorf("decode committed entry %d: %w", entry.Index, err)
		}
		if membership {
			command = nil
		}
		if err := r.machine.Apply(entry.Index, command); err != nil {
			return fmt.Errorf("apply committed entry %d: %w", entry.Index, err)
		}
		if waiter, ok := pending[entry.Index]; ok {
			waiter.result <- proposalResult{index: entry.Index}
			delete(pending, entry.Index)
		}
	}
	if r.snapshotThreshold > 0 {
		status := r.node.Status()
		if applied := r.machine.AppliedIndex(); applied > status.SnapshotIndex && applied-status.SnapshotIndex >= r.snapshotThreshold {
			index, data, err := r.machine.Snapshot()
			if err != nil {
				return fmt.Errorf("create state snapshot: %w", err)
			}
			snapshotUpdate, err := r.node.CreateSnapshot(index, data)
			if err != nil {
				return fmt.Errorf("compact raft log: %w", err)
			}
			if err := r.store.Persist(snapshotUpdate); err != nil {
				return fmt.Errorf("persist raft snapshot: %w", err)
			}
		}
	}
	if update.RoleChanged && r.node.Status().Role != raft.Leader {
		for index, waiter := range pending {
			waiter.result <- proposalResult{index: index, err: raft.ErrNotLeader}
			delete(pending, index)
		}
		for contextID, read := range pendingReads {
			read.result <- readResult{err: raft.ErrNotLeader}
			delete(pendingReads, contextID)
		}
	}
	return nil
}

func (r *Runtime) acknowledgeRead(message raft.Message, pendingReads map[uint64]pendingRead) {
	read, ok := pendingReads[message.Context]
	if !ok || r.node.Status().Role != raft.Leader || message.Term != r.node.Status().Term {
		return
	}
	read.acks[message.From] = struct{}{}
	pendingReads[message.Context] = read
	if !r.node.HasQuorum(read.acks) {
		return
	}
	index := r.node.Status().CommitIndex
	if r.machine.AppliedIndex() < index {
		return
	}
	read.result <- readResult{index: index}
	delete(pendingReads, message.Context)
}

func (r *Runtime) sendLoop() {
	for message := range r.outgoing {
		if r.transport == nil {
			continue
		}
		// Independent peer RPCs must not head-of-line block quorum traffic when one
		// destination is partitioned. Raft tolerates delayed/reordered messages.
		go func(message raft.Message) {
			timeout := 500 * time.Millisecond
			if message.Type == raft.MsgSnapshot {
				timeout = 30 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			_ = r.transport.Send(ctx, message)
		}(message)
	}
}

func (r *Runtime) fail(cause error, pending map[uint64]pendingProposal, pendingReads map[uint64]pendingRead) {
	for index, waiter := range pending {
		waiter.result <- proposalResult{index: index, err: cause}
	}
	for contextID, read := range pendingReads {
		read.result <- readResult{err: cause}
		delete(pendingReads, contextID)
	}
	_ = r.closeResources()
	close(r.done)
	close(r.outgoing)
}

func (r *Runtime) closeResources() error {
	var first error
	if err := r.machine.Close(); err != nil {
		first = err
	}
	if err := r.store.Close(); err != nil && first == nil {
		first = err
	}
	return first
}
