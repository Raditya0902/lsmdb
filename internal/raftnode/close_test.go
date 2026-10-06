package raftnode_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"
)

var errInjectedPersist = errors.New("injected persist failure")

// entryFailingStore fails every persist that carries log entries.
type entryFailingStore struct{}

func (entryFailingStore) Persist(update raft.Update) error {
	if len(update.Entries) > 0 {
		return errInjectedPersist
	}
	return nil
}
func (entryFailingStore) PersistSnapshot(raft.Update, func(io.Writer) error) error { return nil }
func (entryFailingStore) OpenSnapshot(uint64) (io.ReadCloser, uint64, uint32, error) {
	return nil, 0, 0, errors.New("no snapshot")
}
func (entryFailingStore) Close() error { return nil }

// electedSingleVoter ticks a single-voter node until it leads. Nothing is
// persisted: the runtime under test only sees what happens after Start.
func electedSingleVoter(t *testing.T) *raft.Node {
	t.Helper()
	node, err := raft.New(slowElectionConfig(1), raft.HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for node.Status().Role != raft.Leader {
		node.Tick()
	}
	return node
}

func TestCloseReturnsAfterRuntimeStoppedItself(t *testing.T) {
	// After a persist failure the runtime stops itself, but its event queue
	// still has room, so Close can enqueue a stop event that nothing will
	// answer. Each runtime gives that race a fresh chance.
	const runtimes = 32
	for i := 1; i <= runtimes; i++ {
		runtime, err := raftnode.Start(raftnode.Config{TickInterval: time.Millisecond}, electedSingleVoter(t),
			entryFailingStore{}, nil, &memoryMachine{values: make(map[uint64]string)})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _, err = runtime.Propose(ctx, []byte("command"))
		cancel()
		// The runtime hands the proposal the injected error and then closes
		// done. A Propose that reaches its second select only after both
		// may return either, so ErrStopped is also a correct answer here.
		if !errors.Is(err, errInjectedPersist) && !errors.Is(err, raft.ErrStopped) {
			t.Fatalf("runtime %d: Propose error = %v, want the injected persist failure or raft.ErrStopped", i, err)
		}
		closed := make(chan error, 1)
		go func() { closed <- runtime.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("runtime %d: Close after a self-stop = %v, want nil", i, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("runtime %d of %d: Close did not return within 2s after the runtime stopped itself", i, runtimes)
		}
	}
}
