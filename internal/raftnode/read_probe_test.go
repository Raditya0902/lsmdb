package raftnode_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"
)

type readOutcome struct {
	index uint64
	err   error
}

func readAsync(runtime *raftnode.Runtime, timeout time.Duration) <-chan readOutcome {
	done := make(chan readOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		index, err := runtime.LinearizableRead(ctx)
		done <- readOutcome{index, err}
	}()
	return done
}

// readyLeader starts node 1 as leader of three voters with a capturing
// transport and commits its no-op through an ack from node 2, so reads are
// accepted.
func readyLeader(t *testing.T) (*raftnode.Runtime, *gateStore, *captureTransport, uint64) {
	t.Helper()
	transport := &captureTransport{}
	runtime, gate := startGatedLeaderWith(t, []uint64{1, 2, 3}, &memoryMachine{values: make(map[uint64]string)}, 0, transport)
	term := waitForStatus(t, runtime, func(raft.Status) bool { return true }).Term
	if err := runtime.Step(context.Background(), raft.Message{Type: raft.MsgAppendResponse, From: 2, To: 1, Term: term, LogIndex: 1}); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, runtime, func(status raft.Status) bool { return status.CommitIndex == 1 })
	return runtime, gate, transport, term
}

// queueReadsBehindHeldProposal holds the persist of a proposal, queues count
// reads behind it, and releases the persist.
func queueReadsBehindHeldProposal(t *testing.T, runtime *raftnode.Runtime, gate *gateStore, count int, timeout time.Duration) []<-chan readOutcome {
	t.Helper()
	gate.holdNext()
	proposeAsync(runtime, []byte("held")) // never commits: no follower acknowledges it
	waitEntered(t, gate)
	var reads []<-chan readOutcome
	for i := 0; i < count; i++ {
		reads = append(reads, readAsync(runtime, timeout))
		waitQueued(t, runtime, i+1)
	}
	gate.release <- struct{}{}
	return reads
}

// probeContexts returns the distinct read contexts sent to peer.
func probeContexts(transport *captureTransport, peer uint64) map[uint64]bool {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	contexts := map[uint64]bool{}
	for _, message := range transport.sent {
		if message.Type == raft.MsgAppend && message.To == peer && message.Context != 0 {
			contexts[message.Context] = true
		}
	}
	return contexts
}

func TestQueuedReadsShareOneProbe(t *testing.T) {
	runtime, gate, transport, term := readyLeader(t)
	reads := queueReadsBehindHeldProposal(t, runtime, gate, 4, 5*time.Second)
	var contexts map[uint64]bool
	deadline := time.Now().Add(5 * time.Second)
	for contexts = probeContexts(transport, 2); len(contexts) == 0 && time.Now().Before(deadline); contexts = probeContexts(transport, 2) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // any further probe would be sent by now
	if contexts = probeContexts(transport, 2); len(contexts) != 1 {
		t.Fatalf("probes to node 2 carried %d read contexts, want 1 shared by the 4 queued reads", len(contexts))
	}
	for readContext := range contexts {
		ack := raft.Message{Type: raft.MsgAppendResponse, From: 2, To: 1, Term: term, LogIndex: 1, Context: readContext}
		if err := runtime.Step(context.Background(), ack); err != nil {
			t.Fatal(err)
		}
	}
	for i, read := range reads {
		select {
		case outcome := <-read:
			if outcome.err != nil || outcome.index != 1 {
				t.Fatalf("read %d returned index %d error %v, want index 1", i, outcome.index, outcome.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("read %d did not complete after the quorum ack", i)
		}
	}
}

func TestSharedProbeStillNeedsQuorum(t *testing.T) {
	runtime, gate, _, _ := readyLeader(t)
	// No follower ever answers the probe.
	for i, read := range queueReadsBehindHeldProposal(t, runtime, gate, 3, 300*time.Millisecond) {
		outcome := <-read
		if !errors.Is(outcome.err, context.DeadlineExceeded) {
			t.Fatalf("read %d returned index %d error %v without a quorum ack, want its deadline", i, outcome.index, outcome.err)
		}
	}
}

// waitProbes waits up to timeout for probes to peer to carry n distinct read
// contexts and returns the contexts seen.
func waitProbes(transport *captureTransport, peer uint64, n int, timeout time.Duration) map[uint64]bool {
	deadline := time.Now().Add(timeout)
	contexts := probeContexts(transport, peer)
	for len(contexts) < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		contexts = probeContexts(transport, peer)
	}
	return contexts
}

func ackProbe(t *testing.T, runtime *raftnode.Runtime, term, logIndex, readContext uint64) {
	t.Helper()
	ack := raft.Message{Type: raft.MsgAppendResponse, From: 2, To: 1, Term: term, LogIndex: logIndex, Context: readContext}
	if err := runtime.Step(context.Background(), ack); err != nil {
		t.Fatal(err)
	}
}

func TestLateReadGetsItsOwnProbe(t *testing.T) {
	runtime, _, transport, term := readyLeader(t)
	early := readAsync(runtime, 5*time.Second)
	earlyContexts := waitProbes(transport, 2, 1, 5*time.Second)
	if len(earlyContexts) != 1 {
		t.Fatalf("early read sent %d probe contexts, want 1", len(earlyContexts))
	}

	// A write commits while the early probe is unanswered.
	written := proposeAsync(runtime, []byte("x"))
	waitForStatus(t, runtime, func(status raft.Status) bool { return status.LastLogIndex == 2 })
	ackProbe(t, runtime, term, 2, 0)
	if result := <-written; result.err != nil || result.index != 2 {
		t.Fatalf("write returned index %d error %v, want index 2", result.index, result.err)
	}

	late := readAsync(runtime, 5*time.Second)
	var lateContext uint64
	for readContext := range waitProbes(transport, 2, 2, time.Second) {
		if !earlyContexts[readContext] {
			lateContext = readContext
		}
	}
	for readContext := range earlyContexts {
		ackProbe(t, runtime, term, 2, readContext)
	}
	if outcome := <-early; outcome.err != nil {
		t.Fatalf("early read returned error %v after its quorum ack", outcome.err)
	}
	time.Sleep(20 * time.Millisecond) // a late read completed by the same ack would have returned by now
	select {
	case outcome := <-late:
		t.Fatalf("late read returned index %d error %v on the earlier probe's ack, want it to wait for its own probe", outcome.index, outcome.err)
	default:
	}
	if lateContext == 0 {
		t.Fatal("late read sent no probe of its own")
	}

	ackProbe(t, runtime, term, 2, lateContext)
	select {
	case outcome := <-late:
		if outcome.err != nil || outcome.index != 2 {
			t.Fatalf("late read returned index %d error %v, want index 2, the commit index at its arrival", outcome.index, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late read did not complete after its own probe's quorum ack")
	}
}
