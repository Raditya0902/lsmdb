// Package raftnode runs the deterministic Raft state machine with persistence,
// transport, ticking, and ordered state-machine application.
package raftnode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"lsmdb/internal/raft"
)

// StableStore persists effects emitted by the Raft module.
type StableStore interface {
	Persist(raft.Update) error
	PersistSnapshot(raft.Update, func(io.Writer) error) error
	OpenSnapshot(index uint64) (reader io.ReadCloser, size uint64, checksum uint32, err error)
	Close() error
}

// Transport sends an asynchronous Raft message to another node.
type Transport interface {
	Send(context.Context, raft.Message) error
	SendSnapshot(context.Context, raft.Message, io.Reader, uint64, uint32) error
}

// StateMachine applies committed entries in strict index order.
type StateMachine interface {
	Apply(index uint64, command []byte) error
	AppliedIndex() uint64
	WriteSnapshot(io.Writer) (index uint64, err error)
	RestoreSnapshot(index uint64, size uint64, reader io.Reader) error
	Close() error
}

// Config controls the runtime rather than the Raft protocol.
type Config struct {
	TickInterval time.Duration
	QueueSize    int
	// SnapshotThreshold compacts after this many applied entries. Zero disables it.
	SnapshotThreshold uint64
}

// Batch caps (D022). A proposal counts as its data plus
// raft.EntryOverheadBytes, as in the append cap, so a full batch fits one
// capped append; a proposal over raft.MaxEntryBytes counts nothing, because it
// is rejected on its own.
const (
	maxBatchEntries = 256
	maxBatchBytes   = 1 << 20
)

type proposalResult struct {
	index uint64
	term  uint64
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
	message      raft.Message
	snapshotData io.Reader
	result       chan error
}

type statusEvent struct{ result chan raft.Status }
type stopEvent struct{ result chan error }
type readEvent struct{ result chan readResult }

type readResult struct {
	index uint64
	err   error
}

type pendingProposal struct{ result chan proposalResult }

// pendingMembership waits for a voter set to become the committed configuration.
// Its final entry's index cannot be predicted: client proposals accepted while the
// joint configuration is uncommitted are appended before it.
type pendingMembership struct {
	voters []uint64
	result chan proposalResult
}
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
	// membership is owned by the event loop; at most one change runs at a time.
	membership *pendingMembership
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

// Propose waits until a command is committed and locally applied. It returns
// the command's log index and the term of the entry at that index.
func (r *Runtime) Propose(ctx context.Context, command []byte) (index, term uint64, err error) {
	result := make(chan proposalResult, 1)
	event := proposalEvent{data: append([]byte(nil), command...), result: result}
	select {
	case r.events <- event:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-r.done:
		return 0, 0, raft.ErrStopped
	}
	select {
	case response := <-result:
		return response.index, response.term, response.err
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-r.done:
		return 0, 0, raft.ErrStopped
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
	return r.step(ctx, message, nil)
}

// StepSnapshot enqueues an inbound snapshot whose validated bytes are streamed
// separately from the deterministic Raft message metadata.
func (r *Runtime) StepSnapshot(ctx context.Context, message raft.Message, data io.Reader) error {
	if data == nil {
		return errors.New("snapshot data reader is required")
	}
	result := make(chan error, 1)
	select {
	case r.events <- messageEvent{message: message, snapshotData: data, result: result}:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return raft.ErrStopped
	}
	// Once enqueued, the runtime owns the stream until persistence finishes. An
	// RPC cancellation cannot revoke its reader while the Raft update is applying.
	select {
	case err := <-result:
		return err
	case <-r.done:
		return raft.ErrStopped
	}
}

func (r *Runtime) step(ctx context.Context, message raft.Message, data io.Reader) error {
	result := make(chan error, 1)
	select {
	case r.events <- messageEvent{message: message, snapshotData: data, result: result}:
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
// It returns nil if the runtime had already stopped itself.
func (r *Runtime) Close() error {
	var err error
	r.once.Do(func() {
		result := make(chan error, 1)
		select {
		case r.events <- stopEvent{result: result}:
			// The event queue can accept the stop event after the loop has
			// already stopped itself, and then nothing answers it.
			select {
			case err = <-result:
			case <-r.done:
				select {
				case err = <-result:
				default:
				}
			}
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
	// deferred holds events read while draining a batch, handled in arrival
	// order before the queue is read again (D022).
	var deferred []any
	for {
		var raw any
		if len(deferred) > 0 {
			raw, deferred = deferred[0], deferred[1:]
		} else {
			select {
			case <-ticker.C:
				if err := r.processUpdate(r.node.Tick(), pending, pendingReads); err != nil {
					r.fail(err, pending, pendingReads)
					return
				}
				continue
			case raw = <-r.events:
			}
		}
		switch event := raw.(type) {
		case proposalEvent:
			batch := []proposalEvent{event}
			if len(deferred) == 0 {
				batch, deferred = r.drainProposals(event)
			}
			if err := r.proposeBatch(batch, pending, pendingReads); err != nil {
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
			r.membership = &pendingMembership{voters: event.voters, result: event.result}
			if err := r.processUpdate(update, pending, pendingReads); err != nil {
				r.fail(err, pending, pendingReads)
				return
			}
		case messageEvent:
			err := r.processUpdateWithSnapshot(r.node.Step(event.message), event.snapshotData, pending, pendingReads)
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

// drainProposals reads queued events without blocking and gathers proposals
// behind first into one batch, up to maxBatchEntries proposals, maxBatchBytes
// accounted bytes or a queue's worth of events. Other events, and a proposal
// that would overflow the byte cap, are returned in arrival order.
func (r *Runtime) drainProposals(first proposalEvent) (batch []proposalEvent, others []any) {
	batch = []proposalEvent{first}
	size := proposalBytes(first)
	for drained := 0; drained < cap(r.events) && len(batch) < maxBatchEntries && size < maxBatchBytes; drained++ {
		var raw any
		select {
		case raw = <-r.events:
		default:
			return batch, others
		}
		next, ok := raw.(proposalEvent)
		if !ok {
			others = append(others, raw)
			continue
		}
		if size+proposalBytes(next) > maxBatchBytes {
			return batch, append(others, raw)
		}
		batch = append(batch, next)
		size += proposalBytes(next)
	}
	return batch, others
}

func proposalBytes(event proposalEvent) int {
	if len(event.data) > raft.MaxEntryBytes {
		return 0
	}
	return len(event.data) + raft.EntryOverheadBytes
}

// proposeBatch appends a batch with one ProposeBatch and persists it with one
// update. Each proposal waits at its own index; one too large to append, or a
// batch proposed on a non-leader, is answered at once.
func (r *Runtime) proposeBatch(batch []proposalEvent, pending map[uint64]pendingProposal, pendingReads map[uint64]pendingRead) error {
	data := make([][]byte, len(batch))
	for i, event := range batch {
		data[i] = event.data
	}
	indexes, update, err := r.node.ProposeBatch(data)
	if err != nil {
		for _, event := range batch {
			event.result <- proposalResult{err: err}
		}
		return nil
	}
	for i, event := range batch {
		if indexes[i] == 0 {
			event.result <- proposalResult{err: raft.ErrEntryTooLarge}
			continue
		}
		pending[indexes[i]] = pendingProposal{result: event.result}
	}
	if len(update.Entries) == 0 {
		return nil
	}
	return r.processUpdate(update, pending, pendingReads)
}

func (r *Runtime) processUpdate(update raft.Update, pending map[uint64]pendingProposal, pendingReads map[uint64]pendingRead) error {
	return r.processUpdateWithSnapshot(update, nil, pending, pendingReads)
}

func (r *Runtime) processUpdateWithSnapshot(update raft.Update, snapshotData io.Reader, pending map[uint64]pendingProposal, pendingReads map[uint64]pendingRead) error {
	var err error
	if update.Snapshot != nil && snapshotData != nil {
		err = r.store.PersistSnapshot(update, func(writer io.Writer) error {
			_, copyErr := io.Copy(writer, snapshotData)
			return copyErr
		})
	} else {
		err = r.store.Persist(update)
	}
	if err != nil {
		return fmt.Errorf("persist raft update: %w", err)
	}
	if update.DroppedAppend != "" {
		log.Printf("raft: dropped malformed append %s", update.DroppedAppend)
	}
	if update.Snapshot != nil && r.machine.AppliedIndex() < update.Snapshot.Index {
		reader, size, _, err := r.store.OpenSnapshot(update.Snapshot.Index)
		if err != nil {
			return fmt.Errorf("open state snapshot %d: %w", update.Snapshot.Index, err)
		}
		restoreErr := r.machine.RestoreSnapshot(update.Snapshot.Index, size, reader)
		closeErr := reader.Close()
		if restoreErr != nil {
			return fmt.Errorf("restore state snapshot %d: %w", update.Snapshot.Index, restoreErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close state snapshot %d: %w", update.Snapshot.Index, closeErr)
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
				waiter.result <- proposalResult{index: entry.Index, term: entry.Term}
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
			waiter.result <- proposalResult{index: entry.Index, term: entry.Term}
			delete(pending, entry.Index)
		}
	}
	if r.snapshotThreshold > 0 {
		status := r.node.Status()
		if applied := r.machine.AppliedIndex(); applied > status.SnapshotIndex && applied-status.SnapshotIndex >= r.snapshotThreshold {
			index := applied
			snapshotUpdate, err := r.node.CreateSnapshot(index, nil)
			if err != nil {
				return fmt.Errorf("compact raft log: %w", err)
			}
			if err := r.store.PersistSnapshot(snapshotUpdate, func(writer io.Writer) error {
				writtenIndex, writeErr := r.machine.WriteSnapshot(writer)
				if writeErr != nil {
					return writeErr
				}
				if writtenIndex != index {
					return fmt.Errorf("state snapshot index changed from %d to %d", index, writtenIndex)
				}
				return nil
			}); err != nil {
				return fmt.Errorf("persist raft snapshot: %w", err)
			}
		}
	}
	r.resolveMembership()
	if update.RoleChanged && r.node.Status().Role != raft.Leader {
		if r.membership != nil {
			r.membership.result <- proposalResult{err: raft.ErrNotLeader}
			r.membership = nil
		}
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

// resolveMembership completes a pending ChangeMembership once its voter set is the
// applied, non-joint configuration, returning that configuration's log index.
func (r *Runtime) resolveMembership() {
	if r.membership == nil {
		return
	}
	current := r.node.Status().Membership
	if len(current.JointVoters) != 0 || current.Index > r.machine.AppliedIndex() || !sameVoters(current.Voters, r.membership.voters) {
		return
	}
	r.membership.result <- proposalResult{index: current.Index}
	r.membership = nil
}

func sameVoters(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	sortedA := append([]uint64(nil), a...)
	sortedB := append([]uint64(nil), b...)
	sort.Slice(sortedA, func(i, j int) bool { return sortedA[i] < sortedA[j] })
	sort.Slice(sortedB, func(i, j int) bool { return sortedB[i] < sortedB[j] })
	for i := range sortedA {
		if sortedA[i] != sortedB[i] {
			return false
		}
	}
	return true
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
				timeout = 30 * time.Minute
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if message.Type != raft.MsgSnapshot {
				_ = r.transport.Send(ctx, message)
				return
			}
			if message.Snapshot == nil {
				return
			}
			reader, size, checksum, err := r.store.OpenSnapshot(message.Snapshot.Index)
			if err != nil {
				return
			}
			defer reader.Close()
			_ = r.transport.SendSnapshot(ctx, message, reader, size, checksum)
		}(message)
	}
}

func (r *Runtime) fail(cause error, pending map[uint64]pendingProposal, pendingReads map[uint64]pendingRead) {
	if r.membership != nil {
		r.membership.result <- proposalResult{err: cause}
		r.membership = nil
	}
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
