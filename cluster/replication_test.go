package cluster

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnet"
	"lsmdb/internal/raftnode"
	"lsmdb/internal/raftstore"
)

// countingMachine applies entries by recording the index only.
type countingMachine struct {
	mu      sync.Mutex
	applied uint64
}

func (m *countingMachine) Apply(index uint64, _ []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = index
	return nil
}

func (m *countingMachine) AppliedIndex() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied
}

func (m *countingMachine) WriteSnapshot(io.Writer) (uint64, error) { return m.AppliedIndex(), nil }

func (m *countingMachine) RestoreSnapshot(index uint64, _ uint64, _ io.Reader) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = index
	return nil
}

func (m *countingMachine) Close() error { return nil }

type memoryReplica struct {
	runtime *raftnode.Runtime
	metrics *nodeMetrics
}

// startMemoryCluster runs three durable runtimes over the in-memory network,
// with the benchmark's Raft settings and the phase-10 counting wrappers. The
// network steps each message synchronously under the sender's context, so the
// runtime's 500 ms send deadline and bounded event queue apply as over gRPC.
func startMemoryCluster(t *testing.T) map[uint64]*memoryReplica {
	t.Helper()
	network := raftnet.New()
	replicas := make(map[uint64]*memoryReplica)
	for id := uint64(1); id <= 3; id++ {
		store, err := raftstore.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		node, err := raft.New(raft.Config{
			ID: id, Peers: []uint64{1, 2, 3}, ElectionTickMin: 5, ElectionTickMax: 10,
			HeartbeatTicks: 1, CheckQuorumTicks: 5, RandomSeed: id,
		}, raft.HardState{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		metrics := newNodeMetrics(id)
		runtime, err := raftnode.Start(raftnode.Config{TickInterval: 20 * time.Millisecond}, node,
			&observedStore{inner: store, metrics: metrics},
			&observedTransport{inner: network.Adapter(id), metrics: metrics}, &countingMachine{})
		if err != nil {
			t.Fatal(err)
		}
		network.Register(id, runtime)
		replicas[id] = &memoryReplica{runtime: runtime, metrics: metrics}
	}
	t.Cleanup(func() {
		for _, replica := range replicas {
			_ = replica.runtime.Close()
		}
	})
	return replicas
}

// waitForMemoryLeader returns the leader once it has committed an entry of its
// own term, so proposals and reads are accepted.
func waitForMemoryLeader(t *testing.T, replicas map[uint64]*memoryReplica) *memoryReplica {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, replica := range replicas {
			status, err := replica.runtime.Status(context.Background())
			if err == nil && status.Role == raft.Leader && status.CommitIndex > 0 {
				return replica
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func totalAppends(t *testing.T, replicas map[uint64]*memoryReplica) float64 {
	t.Helper()
	var total float64
	for _, replica := range replicas {
		for name, value := range mustSnapshot(t, replica.metrics) {
			if strings.HasPrefix(name, "lsmdb_raft_append_messages_total{") {
				total += value
			}
		}
	}
	return total
}

// proposeConcurrently writes count commands from the given number of
// goroutines, all through the leader, and returns the first failure.
func proposeConcurrently(leader *memoryReplica, proposers, count int) error {
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for p := 0; p < proposers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := p; i < count; i += proposers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := leader.runtime.Propose(ctx, []byte(fmt.Sprintf("write-%d", i)))
				cancel()
				if err != nil {
					errs <- fmt.Errorf("write %d: %w", i, err)
				}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func TestAppendEntriesPerCommittedEntryStaysBounded(t *testing.T) {
	const proposers, writes, bound = 4, 200, 20.0
	replicas := startMemoryCluster(t)
	leader := waitForMemoryLeader(t, replicas)
	start, err := leader.runtime.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	appendsBefore := totalAppends(t, replicas)
	if err := proposeConcurrently(leader, proposers, writes); err != nil {
		t.Fatal(err)
	}
	end, err := leader.runtime.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	committed := float64(end.CommitIndex - start.CommitIndex)
	if committed < writes {
		t.Fatalf("committed %v entries, want at least %d", committed, writes)
	}
	perEntry := (totalAppends(t, replicas) - appendsBefore) / committed
	t.Logf("%d proposers, %v committed entries: %.1f AppendEntries per committed entry", proposers, committed, perEntry)
	if perEntry > bound {
		t.Fatalf("AppendEntries per committed entry = %.1f, want <= %v", perEntry, bound)
	}
}

func TestLinearizableReadCompletesDuringConcurrentWrites(t *testing.T) {
	replicas := startMemoryCluster(t)
	leader := waitForMemoryLeader(t, replicas)
	writeErr := make(chan error, 1)
	go func() { writeErr <- proposeConcurrently(leader, 4, 120) }()
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		index, err := leader.runtime.LinearizableRead(ctx)
		cancel()
		if err != nil {
			t.Fatalf("read %d during writes: %v", i, err)
		}
		status, err := leader.runtime.Status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 || index > status.CommitIndex {
			t.Fatalf("read %d returned index %d, want 1..%d", i, index, status.CommitIndex)
		}
	}
	if err := <-writeErr; err != nil {
		t.Fatal(err)
	}
}
