package raftnode_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"
	"lsmdb/internal/raftstore"
)

// captureTransport records every message the runtime sends.
type captureTransport struct {
	mu   sync.Mutex
	sent []raft.Message
}

func (c *captureTransport) Send(_ context.Context, message raft.Message) error {
	c.mu.Lock()
	c.sent = append(c.sent, message)
	c.mu.Unlock()
	return nil
}

func (c *captureTransport) SendSnapshot(context.Context, raft.Message, io.Reader, uint64, uint32) error {
	return nil
}

// acks returns the success responses sent so far, in send order.
func (c *captureTransport) acks() []raft.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []raft.Message
	for _, message := range c.sent {
		if message.Type == raft.MsgAppendResponse && !message.Reject {
			out = append(out, message)
		}
	}
	return out
}

// startGatedFollower starts node 1 of three voters, which never campaigns,
// on a gate store and a capturing transport.
func startGatedFollower(t *testing.T) (*raftnode.Runtime, *gateStore, *captureTransport) {
	t.Helper()
	store, err := raftstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := slowElectionConfig(1)
	cfg.Peers = []uint64{1, 2, 3}
	node, err := raft.New(cfg, raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gate, transport := newGateStore(store), &captureTransport{}
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, node, gate, transport,
		&memoryMachine{values: make(map[uint64]string)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	t.Cleanup(func() { close(gate.stop) }) // runs first
	return runtime, gate, transport
}

// appendOne is an append from leader 2 carrying the single entry index at
// term, after prev at prevTerm.
func appendOne(term, prev, prevTerm, index uint64, context uint64) raft.Message {
	return raft.Message{
		Type: raft.MsgAppend, From: 2, To: 1, Term: term, LogIndex: prev, LogTerm: prevTerm,
		Entries: []raft.Entry{{Index: index, Term: term, Data: []byte{byte(index)}}}, Context: context,
	}
}

func stepAsync(runtime *raftnode.Runtime, message raft.Message) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- runtime.Step(ctx, message)
	}()
	return done
}

// holdAndQueueAppends steps the first message and holds its persist, queues
// the others behind it in order, and releases the persist. It returns each
// Step's result channel in message order.
func holdAndQueueAppends(t *testing.T, runtime *raftnode.Runtime, gate *gateStore, messages ...raft.Message) []<-chan error {
	t.Helper()
	gate.holdNext()
	results := []<-chan error{stepAsync(runtime, messages[0])}
	waitEntered(t, gate)
	for i, message := range messages[1:] {
		results = append(results, stepAsync(runtime, message))
		waitQueued(t, runtime, i+1)
	}
	gate.release <- struct{}{}
	return results
}

func stepErrors(t *testing.T, results []<-chan error) []error {
	t.Helper()
	out := make([]error, len(results))
	for i, result := range results {
		select {
		case out[i] = <-result:
		case <-time.After(10 * time.Second):
			t.Fatalf("step %d never returned", i)
		}
	}
	return out
}

// followTermOne makes node 1 a follower of node 2 at term 1 holding entry 1.
func followTermOne(t *testing.T, runtime *raftnode.Runtime) {
	t.Helper()
	if err := runtime.Step(context.Background(), appendOne(1, 0, 0, 1, 0)); err != nil {
		t.Fatal(err)
	}
}

func TestQueuedAppendsArePersistedTogether(t *testing.T) {
	runtime, gate, _ := startGatedFollower(t)
	followTermOne(t, runtime)
	results := holdAndQueueAppends(t, runtime, gate,
		appendOne(1, 1, 1, 2, 0), appendOne(1, 2, 1, 3, 0), appendOne(1, 3, 1, 4, 0), appendOne(1, 4, 1, 5, 0))
	for i, err := range stepErrors(t, results) {
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 1, 3}) {
		t.Fatalf("entries per persist = %v, want [1 1 3]: the three queued appends in one persist", got)
	}
}

func TestCoalescingStopsAtHardState(t *testing.T) {
	runtime, gate, _ := startGatedFollower(t)
	followTermOne(t, runtime)
	// Behind the held append: two at term 1, then two from the same leader at
	// term 2. The first term-2 append changes hard state.
	results := holdAndQueueAppends(t, runtime, gate,
		appendOne(1, 1, 1, 2, 0), appendOne(1, 2, 1, 3, 0), appendOne(1, 3, 1, 4, 0),
		appendOne(2, 4, 1, 5, 0), appendOne(2, 5, 2, 6, 0))
	for i, err := range stepErrors(t, results) {
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	want := []persistCall{{1, true}, {1, false}, {2, false}, {1, true}, {1, false}}
	if got := gate.persistCalls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("persist calls (entries, hard state) = %v, want %v: the hard-state update alone, after the merged prefix", got, want)
	}
}

func TestEveryCoalescedResponseIsSent(t *testing.T) {
	runtime, gate, transport := startGatedFollower(t)
	followTermOne(t, runtime)
	results := holdAndQueueAppends(t, runtime, gate,
		appendOne(1, 1, 1, 2, 0), appendOne(1, 2, 1, 3, 7), appendOne(1, 3, 1, 4, 8))
	stepErrors(t, results)
	deadline := time.Now().Add(5 * time.Second)
	for {
		contexts := map[uint64]bool{}
		for _, ack := range transport.acks() {
			contexts[ack.Context] = true
		}
		if contexts[7] && contexts[8] {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("acks carried contexts %v, want both 7 and 8: a superseded response still answers its read probe", contexts)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNoResponseBeforeMergedPersist(t *testing.T) {
	runtime, gate, transport := startGatedFollower(t)
	followTermOne(t, runtime)
	gate.holdAfterNext(1) // the merged persist of indexes 3 and 4
	results := holdAndQueueAppends(t, runtime, gate,
		appendOne(1, 1, 1, 2, 0), appendOne(1, 2, 1, 3, 0), appendOne(1, 3, 1, 4, 0))
	waitEntered(t, gate)
	time.Sleep(50 * time.Millisecond) // time for any early send to reach the transport
	for _, ack := range transport.acks() {
		if ack.LogIndex >= 3 {
			t.Fatalf("ack for index %d sent while its persist was still held", ack.LogIndex)
		}
	}
	gate.release <- struct{}{}
	for i, err := range stepErrors(t, results) {
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
}

func TestPersistFailureFailsEveryMergedAppend(t *testing.T) {
	runtime, gate, _ := startGatedFollower(t)
	followTermOne(t, runtime)
	injected := errors.New("injected merged persist failure")
	gate.mu.Lock()
	gate.failAt, gate.err = 3, injected // 1: entry 1, 2: the held append, 3: the merged one
	gate.mu.Unlock()
	errs := stepErrors(t, holdAndQueueAppends(t, runtime, gate,
		appendOne(1, 1, 1, 2, 0), appendOne(1, 2, 1, 3, 0), appendOne(1, 3, 1, 4, 0)))
	if errs[0] != nil {
		t.Fatalf("held append: %v", errs[0])
	}
	for i, err := range errs[1:] {
		if !errors.Is(err, injected) {
			t.Fatalf("merged append %d returned %v, want the injected failure", i, err)
		}
	}
}
