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
