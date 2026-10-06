package raftnode_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/kvstate"
	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"
	"lsmdb/internal/raftstore"
)

// gateStore wraps a durable store. It can hold the next entry-carrying
// Persist until released, and fail the n-th one; it records how many entries
// each entry-carrying Persist wrote.
type gateStore struct {
	inner   raftnode.StableStore
	entered chan struct{}
	release chan struct{}

	mu      sync.Mutex
	hold    bool
	failAt  int
	err     error
	batches []int
}

func newGateStore(inner raftnode.StableStore) *gateStore {
	return &gateStore{inner: inner, entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (s *gateStore) Persist(update raft.Update) error {
	if len(update.Entries) > 0 {
		s.mu.Lock()
		s.batches = append(s.batches, len(update.Entries))
		hold, fail := s.hold, s.failAt == len(s.batches)
		s.hold = false
		s.mu.Unlock()
		if hold {
			s.entered <- struct{}{}
			<-s.release
		}
		if fail {
			return s.err
		}
	}
	return s.inner.Persist(update)
}

func (s *gateStore) PersistSnapshot(update raft.Update, write func(io.Writer) error) error {
	return s.inner.PersistSnapshot(update, write)
}

func (s *gateStore) OpenSnapshot(index uint64) (io.ReadCloser, uint64, uint32, error) {
	return s.inner.OpenSnapshot(index)
}

func (s *gateStore) Close() error { return s.inner.Close() }

func (s *gateStore) entriesPerPersist() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.batches...)
}

func (s *gateStore) holdNext() {
	s.mu.Lock()
	s.hold = true
	s.mu.Unlock()
}

// startGatedLeader elects node 1 deterministically among the given voters,
// persists and applies what the election produced, and starts its runtime on
// a gate store with no transport. With three voters nothing commits, because
// no follower ever answers.
func startGatedLeader(t *testing.T, voters []uint64, machine raftnode.StateMachine, queueSize int) (*raftnode.Runtime, *gateStore) {
	t.Helper()
	store, err := raftstore.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := slowElectionConfig(1)
	cfg.Peers = voters
	node, err := raft.New(cfg, raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(update raft.Update) {
		if err := store.Persist(update); err != nil {
			t.Fatal(err)
		}
		for _, entry := range update.Committed {
			if err := machine.Apply(entry.Index, entry.Data); err != nil {
				t.Fatal(err)
			}
		}
	}
	for role := node.Status().Role; role != raft.Leader && role != raft.PreCandidate; role = node.Status().Role {
		apply(node.Tick())
	}
	if node.Status().Role == raft.PreCandidate {
		apply(node.Step(raft.Message{Type: raft.MsgPreVoteResponse, From: 2, To: 1, Term: node.Status().Term + 1}))
		apply(node.Step(raft.Message{Type: raft.MsgVoteResponse, From: 2, To: 1, Term: node.Status().Term}))
	}
	if node.Status().Role != raft.Leader {
		t.Fatalf("node 1 did not become leader: %+v", node.Status())
	}
	gate := newGateStore(store)
	runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond, QueueSize: queueSize}, node, gate, nil, machine)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return runtime, gate
}

type proposal struct {
	index uint64
	err   error
}

func proposeAsync(runtime *raftnode.Runtime, data []byte) <-chan proposal {
	done := make(chan proposal, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		index, _, err := runtime.Propose(ctx, data)
		done <- proposal{index, err}
	}()
	return done
}

// waitQueued waits until at least n events wait in the runtime's queue.
func waitQueued(t *testing.T, runtime *raftnode.Runtime, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for raftnode.QueuedEvents(runtime) < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d events queued after 5s, want %d", raftnode.QueuedEvents(runtime), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitEntered(t *testing.T, gate *gateStore) {
	t.Helper()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the held persist never started")
	}
}

// holdAndQueue proposes the first payload and holds its persist, queues one
// proposal per remaining payload behind it in order, then releases the
// persist. It returns the results in payload order.
func holdAndQueue(t *testing.T, runtime *raftnode.Runtime, gate *gateStore, payloads ...[]byte) []<-chan proposal {
	t.Helper()
	gate.holdNext()
	results := []<-chan proposal{proposeAsync(runtime, payloads[0])}
	waitEntered(t, gate)
	for i, payload := range payloads[1:] {
		results = append(results, proposeAsync(runtime, payload))
		waitQueued(t, runtime, i+1)
	}
	gate.release <- struct{}{}
	return results
}

func collect(t *testing.T, results []<-chan proposal) []proposal {
	t.Helper()
	out := make([]proposal, len(results))
	for i, result := range results {
		select {
		case out[i] = <-result:
		case <-time.After(10 * time.Second):
			t.Fatalf("proposal %d never completed", i)
		}
	}
	return out
}

func payloads(count, size int) [][]byte {
	out := make([][]byte, count)
	for i := range out {
		out[i] = bytes.Repeat([]byte{byte('a' + i%26)}, size)
	}
	return out
}

func TestQueuedProposalsArePersistedTogether(t *testing.T) {
	runtime, gate := startGatedLeader(t, []uint64{1}, &memoryMachine{values: make(map[uint64]string)}, 0)
	results := collect(t, holdAndQueue(t, runtime, gate, payloads(6, 8)...))
	for i, result := range results {
		if result.err != nil {
			t.Fatalf("proposal %d: %v", i, result.err)
		}
	}
	if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 5}) {
		t.Fatalf("entries per persist = %v, want [1 5]: the five queued proposals in one persist", got)
	}
}

func TestEachBatchedProposalCompletesAtItsOwnIndex(t *testing.T) {
	runtime, gate := startGatedLeader(t, []uint64{1}, &memoryMachine{values: make(map[uint64]string)}, 0)
	results := collect(t, holdAndQueue(t, runtime, gate, payloads(6, 8)...))
	// The no-op is index 1 and the held proposal index 2.
	for i, result := range results {
		if result.err != nil || result.index != uint64(i+2) {
			t.Fatalf("proposal %d returned index %d error %v, want index %d", i, result.index, result.err, i+2)
		}
	}
}

func TestBatchCaps(t *testing.T) {
	t.Run("256 entries", func(t *testing.T) {
		runtime, gate := startGatedLeader(t, []uint64{1}, &memoryMachine{values: make(map[uint64]string)}, 512)
		collect(t, holdAndQueue(t, runtime, gate, payloads(301, 8)...))
		if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 256, 44}) {
			t.Fatalf("entries per persist = %v, want [1 256 44]", got)
		}
	})
	t.Run("1 MiB accounted", func(t *testing.T) {
		// 300 KiB + 32 accounted bytes each: three fit 1 MiB, four do not.
		runtime, gate := startGatedLeader(t, []uint64{1}, &memoryMachine{values: make(map[uint64]string)}, 0)
		collect(t, holdAndQueue(t, runtime, gate, append([][]byte{[]byte("held")}, payloads(5, 300<<10)...)...))
		if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 3, 2}) {
			t.Fatalf("entries per persist = %v, want [1 3 2]", got)
		}
	})
	t.Run("a 4 MiB proposal goes alone", func(t *testing.T) {
		runtime, gate := startGatedLeader(t, []uint64{1}, &memoryMachine{values: make(map[uint64]string)}, 0)
		queued := append([][]byte{[]byte("held"), bytes.Repeat([]byte{'x'}, 4<<20)}, payloads(2, 8)...)
		collect(t, holdAndQueue(t, runtime, gate, queued...))
		if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 1, 2}) {
			t.Fatalf("entries per persist = %v, want [1 1 2]", got)
		}
	})
}

// demote sends the leader an empty append from node 2 at a higher term.
func demote(ctx context.Context, runtime *raftnode.Runtime, term uint64) error {
	return runtime.Step(ctx, raft.Message{Type: raft.MsgAppend, From: 2, To: 1, Term: term})
}

func TestLeaderChangeMidBatchFailsEveryWaiter(t *testing.T) {
	runtime, gate := startGatedLeader(t, []uint64{1, 2, 3}, &memoryMachine{values: make(map[uint64]string)}, 0)
	term := waitForStatus(t, runtime, func(raft.Status) bool { return true }).Term
	results := holdAndQueue(t, runtime, gate, payloads(5, 8)...)
	// Both persists finish before the new leader's append arrives; nothing
	// commits, because no follower answers.
	waitForStatus(t, runtime, func(status raft.Status) bool { return status.LastLogIndex == 6 })
	if err := demote(context.Background(), runtime, term+1); err != nil {
		t.Fatal(err)
	}
	for i, result := range collect(t, results) {
		if !errors.Is(result.err, raft.ErrNotLeader) {
			t.Fatalf("proposal %d returned index %d error %v, want ErrNotLeader", i, result.index, result.err)
		}
	}
}

func TestProposalsDrainedOnFollowerEachGetNotLeader(t *testing.T) {
	runtime, gate := startGatedLeader(t, []uint64{1, 2, 3}, &memoryMachine{values: make(map[uint64]string)}, 0)
	status := waitForStatus(t, runtime, func(raft.Status) bool { return true })
	if err := demote(context.Background(), runtime, status.Term+1); err != nil {
		t.Fatal(err)
	}
	// Hold the follower's persist of an append from the new leader, and queue
	// proposals behind it.
	gate.holdNext()
	stepped := make(chan error, 1)
	go func() {
		stepped <- runtime.Step(context.Background(), raft.Message{
			Type: raft.MsgAppend, From: 2, To: 1, Term: status.Term + 1,
			LogIndex: status.LastLogIndex, LogTerm: status.Term,
			Entries: []raft.Entry{{Index: status.LastLogIndex + 1, Term: status.Term + 1, Data: []byte("x")}},
		})
	}()
	waitEntered(t, gate)
	var results []<-chan proposal
	for i, payload := range payloads(3, 8) {
		results = append(results, proposeAsync(runtime, payload))
		waitQueued(t, runtime, i+1)
	}
	gate.release <- struct{}{}
	if err := <-stepped; err != nil {
		t.Fatal(err)
	}
	for i, result := range collect(t, results) {
		if !errors.Is(result.err, raft.ErrNotLeader) {
			t.Fatalf("proposal %d on a follower returned error %v, want ErrNotLeader", i, result.err)
		}
	}
}

func TestPersistFailureMidBatchFailsEveryWaiter(t *testing.T) {
	machine := &memoryMachine{values: make(map[uint64]string)}
	runtime, gate := startGatedLeader(t, []uint64{1}, machine, 0)
	injected := errors.New("injected batch persist failure")
	gate.mu.Lock()
	gate.failAt, gate.err = 2, injected // the held proposal is persist 1, the batch persist 2
	gate.mu.Unlock()
	results := collect(t, holdAndQueue(t, runtime, gate, payloads(5, 8)...))
	if results[0].err != nil || results[0].index != 2 {
		t.Fatalf("held proposal returned index %d error %v, want index 2", results[0].index, results[0].err)
	}
	for i, result := range results[1:] {
		if !errors.Is(result.err, injected) {
			t.Fatalf("batched proposal %d returned index %d error %v, want the injected failure", i, result.index, result.err)
		}
	}
	if applied := machine.AppliedIndex(); applied != 2 {
		t.Fatalf("applied index = %d, want 2: nothing from the failed batch is applied", applied)
	}
}

func TestOversizeProposalInBatchFailsAlone(t *testing.T) {
	runtime, gate := startGatedLeader(t, []uint64{1}, &memoryMachine{values: make(map[uint64]string)}, 0)
	oversize := bytes.Repeat([]byte{'x'}, raft.MaxEntryBytes+1)
	results := collect(t, holdAndQueue(t, runtime, gate, []byte("held"), []byte("a"), oversize, []byte("b")))
	if !errors.Is(results[2].err, raft.ErrEntryTooLarge) {
		t.Fatalf("oversize proposal returned error %v, want ErrEntryTooLarge", results[2].err)
	}
	for _, i := range []int{0, 1, 3} {
		if results[i].err != nil {
			t.Fatalf("proposal %d: %v", i, results[i].err)
		}
	}
	if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("entries per persist = %v, want [1 2]", got)
	}
}

func TestEventsDrainedWithBatchRunAfterItInOrder(t *testing.T) {
	runtime, gate := startGatedLeader(t, []uint64{1, 2, 3}, &memoryMachine{values: make(map[uint64]string)}, 0)
	term := waitForStatus(t, runtime, func(raft.Status) bool { return true }).Term
	gate.holdNext()
	results := []<-chan proposal{proposeAsync(runtime, []byte("held"))}
	waitEntered(t, gate)
	// Queue: proposal, demotion, status, proposal.
	results = append(results, proposeAsync(runtime, []byte("p1")))
	waitQueued(t, runtime, 1)
	demoted := make(chan error, 1)
	go func() { demoted <- demote(context.Background(), runtime, term+1) }()
	waitQueued(t, runtime, 2)
	statuses := make(chan raft.Status, 1)
	go func() {
		status, _ := runtime.Status(context.Background())
		statuses <- status
	}()
	waitQueued(t, runtime, 3)
	results = append(results, proposeAsync(runtime, []byte("p2")))
	waitQueued(t, runtime, 4)
	gate.release <- struct{}{}

	if err := <-demoted; err != nil {
		t.Fatal(err)
	}
	status := <-statuses
	if status.Role != raft.Follower || status.Term != term+1 {
		t.Fatalf("status = %s at term %d, want follower at term %d: the status ran before the demotion queued ahead of it", status.Role, status.Term, term+1)
	}
	if got := gate.entriesPerPersist(); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("entries per persist = %v, want [1 2]: p1 and p2 batched ahead of the demotion", got)
	}
	collect(t, results)
}

func encodePut(t *testing.T, key, value, clientID string, seq uint64) []byte {
	t.Helper()
	command, err := kvstate.EncodeCommand(lsmdbv1.Command_OPERATION_PUT, []byte(key), []byte(value), clientID, seq)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func TestRetriedClientWriteIsAppliedOnce(t *testing.T) {
	machine, err := kvstate.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runtime, gate := startGatedLeader(t, []uint64{1}, machine, 0)
	value := func(key string) string {
		t.Helper()
		got, found, err := machine.Get([]byte(key))
		if err != nil || !found {
			t.Fatalf("Get(%q) = %q, %v, %v", key, got, found, err)
		}
		return string(got)
	}

	t.Run("retry after a newer write", func(t *testing.T) {
		for _, command := range [][]byte{
			encodePut(t, "k", "v1", "client-a", 5),
			encodePut(t, "k", "v2", "client-a", 6),
			encodePut(t, "k", "v1", "client-a", 5), // the retry of seq 5 arrives late
		} {
			if result := collect(t, []<-chan proposal{proposeAsync(runtime, command)})[0]; result.err != nil {
				t.Fatal(result.err)
			}
		}
		if got := value("k"); got != "v2" {
			t.Fatalf("k = %q, want v2: the late retry of seq 5 must be a no-op", got)
		}
	})

	t.Run("same seq twice in one batch", func(t *testing.T) {
		results := collect(t, holdAndQueue(t, runtime, gate,
			encodePut(t, "held", "x", "client-c", 1),
			encodePut(t, "j", "first", "client-b", 1),
			encodePut(t, "j", "second", "client-b", 1),
		))
		for i, result := range results {
			if result.err != nil {
				t.Fatalf("proposal %d: %v", i, result.err)
			}
		}
		if got := value("j"); got != "first" {
			t.Fatalf("j = %q, want first: seq 1 applies once", got)
		}
	})
}
