package raftnode_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"lsmdb/internal/raft"
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
