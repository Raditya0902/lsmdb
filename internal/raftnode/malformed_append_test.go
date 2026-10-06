package raftnode_test

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"
	"lsmdb/internal/raftstore"
)

func TestRuntimeKeepsServingAfterMalformedAppend(t *testing.T) {
	var logged bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logged)
	defer log.SetOutput(previous)

	store, err := raftstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	node, err := raft.New(raft.Config{
		ID: 2, Peers: []uint64{1, 2, 3}, ElectionTickMin: 10000, ElectionTickMax: 20000,
		HeartbeatTicks: 1, CheckQuorumTicks: 10000, RandomSeed: 2,
	}, raft.HardState{Term: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, node, store, nil, &memoryMachine{values: make(map[uint64]string)})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Entry 2 has term 0, which the store refuses to write.
	malformed := raft.Message{
		Type: raft.MsgAppend, From: 1, To: 2, Term: 1,
		Entries: []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 0}},
	}
	if err := runtime.Step(ctx, malformed); err != nil {
		t.Fatalf("Step(malformed append) = %v, want nil", err)
	}
	status, err := runtime.Status(ctx)
	if err != nil {
		t.Fatalf("Status after a malformed append = %v, want the runtime still running", err)
	}
	if status.LastLogIndex != 0 || status.MalformedAppendsDropped != 1 {
		t.Fatalf("status = last index %d, dropped %d; want 0 and 1", status.LastLogIndex, status.MalformedAppendsDropped)
	}
	if !strings.Contains(logged.String(), "dropped malformed append from 1") {
		t.Fatalf("log = %q, want a dropped-append line", logged.String())
	}
}
