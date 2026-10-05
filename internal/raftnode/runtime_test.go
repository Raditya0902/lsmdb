package raftnode_test

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnet"
	"lsmdb/internal/raftnode"
	"lsmdb/internal/raftstore"
)

type memoryMachine struct {
	mu      sync.Mutex
	applied uint64
	values  map[uint64]string
}

func (m *memoryMachine) Apply(index uint64, command []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = index
	m.values[index] = string(command)
	return nil
}

func (m *memoryMachine) AppliedIndex() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied
}

func (m *memoryMachine) WriteSnapshot(writer io.Writer) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := writer.Write([]byte("snapshot"))
	return m.applied, err
}
func (m *memoryMachine) RestoreSnapshot(index uint64, _ uint64, _ io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = index
	return nil
}

func (m *memoryMachine) Close() error { return nil }

func TestRuntimePersistsThenAppliesProposal(t *testing.T) {
	dir := t.TempDir()
	store, err := raftstore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.New(raft.Config{
		ID: 1, Peers: []uint64{1}, ElectionTickMin: 2, ElectionTickMax: 4,
		HeartbeatTicks: 1, CheckQuorumTicks: 2, RandomSeed: 1,
	}, raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	machine := &memoryMachine{values: make(map[uint64]string)}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, node, store, nil, machine)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		status, err := runtime.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == raft.Leader {
			break
		}
		time.Sleep(time.Millisecond)
	}
	index, err := runtime.Propose(ctx, []byte("command"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if index != 2 {
		t.Fatalf("index = %d, want 2", index)
	}
	if got := machine.AppliedIndex(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
	machine.mu.Lock()
	got := machine.values[2]
	machine.mu.Unlock()
	if got != "command" {
		t.Fatalf("applied command = %q", got)
	}

	hard, entries := store.Load()
	if hard.Term == 0 || len(entries) != 2 || string(entries[1].Data) != "command" {
		t.Fatalf("durable state hard=%+v entries=%#v", hard, entries)
	}
}

func TestRuntimeAutomaticallySnapshotsAndCompacts(t *testing.T) {
	store, err := raftstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.New(raft.Config{ID: 1, Peers: []uint64{1}, ElectionTickMin: 2, ElectionTickMax: 4, HeartbeatTicks: 1, CheckQuorumTicks: 2, RandomSeed: 1}, raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	machine := &memoryMachine{values: make(map[uint64]string)}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond, SnapshotThreshold: 1}, node, store, nil, machine)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		status, err := runtime.Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Role == raft.Leader {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := runtime.Propose(ctx, []byte("command")); err != nil {
		t.Fatal(err)
	}
	status, err := runtime.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.SnapshotIndex != 2 || status.RetainedLogEntries != 0 {
		t.Fatalf("status after compaction = %+v", status)
	}
	if snapshot := store.LoadSnapshot(); snapshot.Index != 2 {
		t.Fatalf("durable snapshot = %#v", snapshot)
	}
	_, entries := store.Load()
	if len(entries) != 0 {
		t.Fatalf("retained entries = %#v", entries)
	}
}

// slowElectionConfig keeps elections and quorum checks far beyond the test's
// duration so a short partition cannot depose the leader.
func slowElectionConfig(id uint64) raft.Config {
	return raft.Config{
		ID: id, Peers: []uint64{1}, ElectionTickMin: 10000, ElectionTickMax: 20000,
		HeartbeatTicks: 1, CheckQuorumTicks: 10000, RandomSeed: id,
	}
}

// startElectedLeader elects a single-voter node deterministically, persists and
// applies what the election produced, then starts its runtime.
func startElectedLeader(t *testing.T, transport raftnode.Transport) *raftnode.Runtime {
	t.Helper()
	store, err := raftstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.New(slowElectionConfig(1), raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	machine := &memoryMachine{values: make(map[uint64]string)}
	for node.Status().Role != raft.Leader {
		update := node.Tick()
		if err := store.Persist(update); err != nil {
			t.Fatal(err)
		}
		for _, entry := range update.Committed {
			if err := machine.Apply(entry.Index, entry.Data); err != nil {
				t.Fatal(err)
			}
		}
	}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, node, store, transport, machine)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func startLearner(t *testing.T, transport raftnode.Transport) *raftnode.Runtime {
	t.Helper()
	store, err := raftstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.New(slowElectionConfig(2), raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, node, store, transport, &memoryMachine{values: make(map[uint64]string)})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func waitForStatus(t *testing.T, runtime *raftnode.Runtime, ready func(raft.Status) bool) raft.Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		status, err := runtime.Status(ctx)
		if err != nil {
			t.Fatalf("waiting for status: %v", err)
		}
		if ready(status) {
			return status
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChangeMembershipCompletesWithConcurrentProposal(t *testing.T) {
	network := raftnet.New()
	leader := startElectedLeader(t, network.Adapter(1))
	defer leader.Close()
	learner := startLearner(t, network.Adapter(2))
	defer learner.Close()
	network.Register(1, leader)
	network.Register(2, learner)

	// Keep the joint configuration uncommitted until a client proposal is accepted.
	network.Drop(1, 2, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type result struct {
		index uint64
		err   error
	}
	membershipDone := make(chan result, 1)
	go func() {
		index, err := leader.ChangeMembership(ctx, []uint64{1, 2})
		membershipDone <- result{index, err}
	}()
	joint := waitForStatus(t, leader, func(status raft.Status) bool { return len(status.Membership.JointVoters) > 0 })

	proposalDone := make(chan result, 1)
	go func() {
		index, err := leader.Propose(ctx, []byte("write"))
		proposalDone <- result{index, err}
	}()
	waitForStatus(t, leader, func(status raft.Status) bool { return status.LastLogIndex > joint.LastLogIndex })
	network.Drop(1, 2, false)

	deadline := time.After(3 * time.Second)
	select {
	case done := <-membershipDone:
		if done.err != nil {
			t.Fatalf("ChangeMembership: %v", done.err)
		}
		final := waitForStatus(t, leader, func(raft.Status) bool { return true })
		if len(final.Membership.JointVoters) != 0 || len(final.Membership.Voters) != 2 || done.index != final.Membership.Index {
			t.Fatalf("ChangeMembership returned index %d; membership = %+v", done.index, final.Membership)
		}
	case <-deadline:
		t.Fatal("ChangeMembership did not return within 3s after the partition healed")
	}
	select {
	case done := <-proposalDone:
		if done.err != nil {
			t.Fatalf("Propose: %v", done.err)
		}
	case <-deadline:
		t.Fatal("Propose did not return within 3s after the partition healed")
	}
}

// callLog records store and transport calls in the order the runtime makes them.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(call string) {
	l.mu.Lock()
	l.calls = append(l.calls, call)
	l.mu.Unlock()
}

func (l *callLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

type recordingStore struct{ log *callLog }

func (s recordingStore) Persist(update raft.Update) error {
	if update.HardState != nil {
		s.log.add(fmt.Sprintf("persist term %d", update.HardState.Term))
	}
	return nil
}
func (s recordingStore) PersistSnapshot(raft.Update, func(io.Writer) error) error { return nil }
func (s recordingStore) OpenSnapshot(uint64) (io.ReadCloser, uint64, uint32, error) {
	return nil, 0, 0, fmt.Errorf("no snapshot")
}
func (s recordingStore) Close() error { return nil }

type recordingTransport struct{ log *callLog }

func (t recordingTransport) Send(_ context.Context, message raft.Message) error {
	t.log.add(fmt.Sprintf("send %d term %d", message.Type, message.Term))
	return nil
}
func (t recordingTransport) SendSnapshot(context.Context, raft.Message, io.Reader, uint64, uint32) error {
	return nil
}

func TestRuntimePersistsAdoptedTermBeforeSendingAtThatTerm(t *testing.T) {
	log := &callLog{}
	node, err := raft.New(raft.Config{
		ID: 1, Peers: []uint64{1, 2, 3}, ElectionTickMin: 2, ElectionTickMax: 4,
		HeartbeatTicks: 1, CheckQuorumTicks: 2, RandomSeed: 1,
	}, raft.HardState{Term: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, node,
		recordingStore{log: log}, recordingTransport{log: log}, &memoryMachine{values: make(map[uint64]string)})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	waitForStatus(t, runtime, func(status raft.Status) bool { return status.Role == raft.PreCandidate })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rejection := raft.Message{Type: raft.MsgPreVoteResponse, From: 2, To: 1, Term: 5, Reject: true}
	if err := runtime.Step(ctx, rejection); err != nil {
		t.Fatal(err)
	}
	// Pre-votes proposing term 6 are only built once term 5 is the node's term.
	firstSendAt := fmt.Sprintf("send %d term 6", raft.MsgPreVote)
	deadline := time.Now().Add(5 * time.Second)
	for {
		calls := log.snapshot()
		persisted, sent := -1, -1
		for i, call := range calls {
			if call == "persist term 5" && persisted < 0 {
				persisted = i
			}
			if call == firstSendAt && sent < 0 {
				sent = i
			}
		}
		if sent >= 0 {
			if persisted < 0 || persisted > sent {
				t.Fatalf("term 5 persisted at call %d, first term-6 pre-vote sent at call %d: %v", persisted, sent, calls)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no pre-vote at term 6 was sent: %v", calls)
		}
		time.Sleep(time.Millisecond)
	}
}
