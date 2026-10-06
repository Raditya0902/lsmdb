package cluster

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"
)

// appendSizeRecorder records AppendEntries that carry more than one entry and
// exceed the default 1 MiB cap under D019's accounting (32 bytes per entry).
type appendSizeRecorder struct {
	mu       sync.Mutex
	appends  int
	oversize []string
}

func (r *appendSizeRecorder) wrap(inner raftnode.Transport) raftnode.Transport {
	return &recordingTransport{inner: inner, recorder: r}
}

type recordingTransport struct {
	inner    raftnode.Transport
	recorder *appendSizeRecorder
}

func (t *recordingTransport) Send(ctx context.Context, message raft.Message) error {
	if message.Type == raft.MsgAppend {
		accounted := 0
		for _, entry := range message.Entries {
			accounted += len(entry.Data) + 32
		}
		t.recorder.mu.Lock()
		t.recorder.appends++
		if len(message.Entries) > 1 && accounted > 1<<20 {
			t.recorder.oversize = append(t.recorder.oversize,
				fmt.Sprintf("%s to %d: %d entries, %d bytes", message.Origin, message.To, len(message.Entries), accounted))
		}
		t.recorder.mu.Unlock()
	}
	return t.inner.Send(ctx, message)
}

func (t *recordingTransport) SendSnapshot(ctx context.Context, message raft.Message, data io.Reader, size uint64, checksum uint32) error {
	return t.inner.SendSnapshot(ctx, message, data, size, checksum)
}

func memoryStatuses(t *testing.T, replicas map[uint64]*memoryReplica) map[uint64]raft.Status {
	t.Helper()
	statuses := make(map[uint64]raft.Status, len(replicas))
	for id, replica := range replicas {
		current, err := replica.runtime.Status(context.Background())
		if err != nil {
			t.Fatalf("status of node %d: %v", id, err)
		}
		statuses[id] = current
	}
	return statuses
}

func TestMemoryClusterShipsLargeEntriesInCappedChunks(t *testing.T) {
	const proposers, writes, valueSize = 4, 12, 400 << 10
	recorder := &appendSizeRecorder{}
	replicas := startMemoryCluster(t, recorder.wrap)
	leader := waitForMemoryLeader(t, replicas)
	before := memoryStatuses(t, replicas)
	value := bytes.Repeat([]byte{'v'}, valueSize)
	if err := proposeData(leader, proposers, writes, func(int) []byte { return value }); err != nil {
		t.Fatal(err)
	}
	after := memoryStatuses(t, replicas)
	for deadline := time.Now().Add(3 * time.Second); !equalCommitIndexes(after) && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		after = memoryStatuses(t, replicas)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	t.Logf("%d appends sent, %d over the cap; terms %d -> %d", recorder.appends, len(recorder.oversize), maxTerm(before), maxTerm(after))
	for _, message := range recorder.oversize {
		t.Errorf("append over the 1 MiB cap: %s", message)
	}
	if maxTerm(after) != maxTerm(before) {
		t.Errorf("term moved from %d to %d during the writes", maxTerm(before), maxTerm(after))
	}
	if !equalCommitIndexes(after) {
		t.Errorf("commit indexes differ: %d, %d, %d", after[1].CommitIndex, after[2].CommitIndex, after[3].CommitIndex)
	}
}

func TestSmallValueAppendsPerEntryStayBounded(t *testing.T) {
	// The phase-12a bound (test 6) for 128-byte values: capping must not change
	// small-value replication.
	const proposers, writes, bound = 4, 200, 20.0
	replicas := startMemoryCluster(t)
	leader := waitForMemoryLeader(t, replicas)
	start, err := leader.runtime.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	appendsBefore := totalAppends(t, replicas)
	value := bytes.Repeat([]byte{'s'}, 128)
	if err := proposeData(leader, proposers, writes, func(int) []byte { return value }); err != nil {
		t.Fatal(err)
	}
	end, err := leader.runtime.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	committed := float64(end.CommitIndex - start.CommitIndex)
	perEntry := (totalAppends(t, replicas) - appendsBefore) / committed
	t.Logf("%v committed 128-byte entries: %.1f AppendEntries per committed entry", committed, perEntry)
	if perEntry > bound {
		t.Fatalf("AppendEntries per committed entry = %.1f, want <= %v", perEntry, bound)
	}
}
