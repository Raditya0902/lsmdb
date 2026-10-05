package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"lsmdb/internal/raft"
)

type fakeStableStore struct{ err error }

func (s fakeStableStore) Persist(raft.Update) error                                { return s.err }
func (s fakeStableStore) PersistSnapshot(raft.Update, func(io.Writer) error) error { return s.err }
func (s fakeStableStore) OpenSnapshot(uint64) (io.ReadCloser, uint64, uint32, error) {
	return nil, 0, 0, errors.New("no snapshot")
}
func (s fakeStableStore) Close() error { return nil }

type fakeTransport struct{}

func (fakeTransport) Send(context.Context, raft.Message) error { return nil }
func (fakeTransport) SendSnapshot(context.Context, raft.Message, io.Reader, uint64, uint32) error {
	return nil
}

func entries(n int) []raft.Entry {
	out := make([]raft.Entry, n)
	for i := range out {
		out[i] = raft.Entry{Index: uint64(i + 1), Term: 1}
	}
	return out
}

func mustSnapshot(t *testing.T, metrics *nodeMetrics) map[string]float64 {
	t.Helper()
	values, err := metrics.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return values
}

func TestObservedStoreCountsLogSyncs(t *testing.T) {
	metrics := newNodeMetrics(1)
	store := &observedStore{inner: fakeStableStore{}, metrics: metrics}
	hard := &raft.HardState{Term: 2, VotedFor: 1}
	for _, update := range []raft.Update{
		{},
		{Entries: entries(1)},
		{Entries: entries(3), HardState: hard},
		{HardState: hard},
	} {
		if err := store.Persist(update); err != nil {
			t.Fatal(err)
		}
	}
	failing := &observedStore{inner: fakeStableStore{err: errors.New("disk full")}, metrics: metrics}
	if err := failing.Persist(raft.Update{Entries: entries(5), HardState: hard}); err == nil {
		t.Fatal("failing store reported success")
	}
	got := mustSnapshot(t, metrics)
	want := map[string]float64{
		"lsmdb_raft_log_syncs_total":        2,
		"lsmdb_raft_log_sync_entries_total": 4,
		"lsmdb_raft_hardstate_syncs_total":  2,
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}
}

func TestObservedTransportCountsAppendsByOrigin(t *testing.T) {
	metrics := newNodeMetrics(1)
	transport := &observedTransport{inner: fakeTransport{}, metrics: metrics}
	for _, message := range []raft.Message{
		{Type: raft.MsgAppend, Origin: raft.OriginHeartbeat},
		{Type: raft.MsgAppend, Origin: raft.OriginHeartbeat},
		{Type: raft.MsgAppend, Origin: raft.OriginAckResend},
		{Type: raft.MsgAppend},
		{Type: raft.MsgVote, Origin: raft.OriginProposal},
		{Type: raft.MsgAppendResponse, Origin: raft.OriginProposal},
	} {
		if err := transport.Send(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	got := mustSnapshot(t, metrics)
	want := map[string]float64{
		"heartbeat": 2, "ack_resend": 1, "other": 1, "proposal": 0, "commit_advance": 0,
	}
	for origin, value := range want {
		key := fmt.Sprintf("lsmdb_raft_append_messages_total{origin=%s}", origin)
		if count, ok := got[key]; !ok || count != value {
			t.Errorf("%s = (%v, present=%v), want %v", key, count, ok, value)
		}
	}
}

func TestNodeMetricSnapshotExposesReplicationCounters(t *testing.T) {
	addresses := map[uint64]string{1: freeAddress(t), 2: freeAddress(t), 3: freeAddress(t)}
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		node, err := StartNode(NodeConfig{
			ID: id, ListenAddress: addresses[id], DataDir: fmt.Sprintf("%s/node-%d", t.TempDir(), id),
			Peers: addresses, TickInterval: 20 * time.Millisecond,
			ElectionTickMin: 5, ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5,
		})
		if err != nil {
			t.Fatalf("StartNode(%d): %v", id, err)
		}
		nodes[id] = node
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	}()
	leaderID := waitForLeader(t, nodes, 0)
	client, err := NewClient([]string{addresses[1], addresses[2], addresses[3]})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if client.Attempts() == 0 {
		t.Fatal("client reported no attempts")
	}
	values, err := nodes[leaderID].MetricSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"lsmdb_raft_log_syncs_total", "lsmdb_raft_log_sync_entries_total",
		"lsmdb_raft_append_messages_total{origin=proposal}", "lsmdb_raft_apply_seconds_count",
	} {
		if values[name] <= 0 {
			t.Errorf("%s = %v, want > 0", name, values[name])
		}
	}
	if _, ok := values["lsmdb_raft_snapshot_seconds_count"]; !ok {
		t.Error("snapshot histogram is not registered")
	}
}
