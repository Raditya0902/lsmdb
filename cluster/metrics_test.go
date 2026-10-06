package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/db"
	"lsmdb/internal/kvstate"
	"lsmdb/internal/raft"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// failingTransport returns the next queued error for each send.
type failingTransport struct{ errs []error }

func (t *failingTransport) next() error {
	err := t.errs[0]
	t.errs = t.errs[1:]
	return err
}
func (t *failingTransport) Send(context.Context, raft.Message) error { return t.next() }
func (t *failingTransport) SendSnapshot(context.Context, raft.Message, io.Reader, uint64, uint32) error {
	return t.next()
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

func TestObservedStoreSplitsLogSyncsByRole(t *testing.T) {
	metrics := newNodeMetrics(1)
	store := &observedStore{inner: fakeStableStore{}, metrics: metrics}
	answer := func(kind raft.MessageType) []raft.Message { return []raft.Message{{Type: kind, To: 2}} }
	for _, update := range []raft.Update{
		{Entries: entries(2), Messages: answer(raft.MsgAppend)}, // leader: proposals sent on
		{Entries: entries(1)}, // leader: no follower ready to send to
		{Entries: entries(3), Messages: answer(raft.MsgAppendResponse)}, // follower: answers an append
		{Entries: entries(4), HardState: &raft.HardState{Term: 2}, Messages: answer(raft.MsgAppendResponse)},
		{HardState: &raft.HardState{Term: 3}, Messages: answer(raft.MsgAppendResponse)}, // no entries: not a log sync
	} {
		if err := store.Persist(update); err != nil {
			t.Fatal(err)
		}
	}
	got := mustSnapshot(t, metrics)
	want := map[string]float64{
		"lsmdb_raft_log_syncs_by_role_total{role=leader}":          2,
		"lsmdb_raft_log_sync_entries_by_role_total{role=leader}":   3,
		"lsmdb_raft_log_syncs_by_role_total{role=follower}":        2,
		"lsmdb_raft_log_sync_entries_by_role_total{role=follower}": 7,
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %v, want %v", name, got[name], value)
		}
	}
	for _, metric := range []string{"lsmdb_raft_log_syncs", "lsmdb_raft_log_sync_entries"} {
		split := got[metric+"_by_role_total{role=leader}"] + got[metric+"_by_role_total{role=follower}"]
		if total := got[metric+"_total"]; split != total {
			t.Errorf("%s: leader + follower = %v, want the total %v", metric, split, total)
		}
	}
}

func TestLogSyncRolesAreRegisteredAtZero(t *testing.T) {
	got := mustSnapshot(t, newNodeMetrics(1))
	for _, name := range []string{
		"lsmdb_raft_log_syncs_by_role_total{role=leader}", "lsmdb_raft_log_syncs_by_role_total{role=follower}",
		"lsmdb_raft_log_sync_entries_by_role_total{role=leader}", "lsmdb_raft_log_sync_entries_by_role_total{role=follower}",
	} {
		if value, ok := got[name]; !ok || value != 0 {
			t.Errorf("%s = %v (present %v), want 0 before any persist", name, value, ok)
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
	// A proposal sends nothing to a follower whose earlier append is still in
	// flight (D022), so wait until both followers hold the leader's whole log.
	for caughtUp := false; !caughtUp; {
		status, err := nodes[leaderID].Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		caughtUp = status.CommitIndex > 0
		for _, id := range []uint64{1, 2, 3} {
			caughtUp = caughtUp && status.MatchIndex[id] == status.LastLogIndex
		}
		time.Sleep(5 * time.Millisecond)
	}
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

func TestObservedTransportClassifiesSendFailures(t *testing.T) {
	sendErrors := []error{
		context.DeadlineExceeded,
		fmt.Errorf("deliver: %w", context.DeadlineExceeded),
		status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
		status.FromContextError(context.DeadlineExceeded).Err(),
		context.Canceled,
		errors.New("raft message dropped"),
		status.Error(codes.Unavailable, "connection refused"),
		nil,
	}
	metrics := newNodeMetrics(1)
	transport := &observedTransport{inner: &failingTransport{errs: append([]error(nil), sendErrors...)}, metrics: metrics}
	for i := 0; i < len(sendErrors)-1; i++ {
		_ = transport.Send(context.Background(), raft.Message{Type: raft.MsgAppend, To: 2})
	}
	_ = transport.SendSnapshot(context.Background(), raft.Message{Type: raft.MsgSnapshot, To: 3}, nil, 0, 0)
	got := mustSnapshot(t, metrics)
	want := map[string]float64{
		"lsmdb_raft_transport_send_failures_total{class=deadline}": 4,
		"lsmdb_raft_transport_send_failures_total{class=other}":    3,
		"lsmdb_raft_transport_failures_total{peer_id=2}":           7,
	}
	for name, value := range want {
		if count, ok := got[name]; !ok || count != value {
			t.Errorf("%s = (%v, present=%v), want %v", name, count, ok, value)
		}
	}
}

func TestSendFailureClassesAreRegisteredAtZero(t *testing.T) {
	got := mustSnapshot(t, newNodeMetrics(1))
	for _, class := range []string{"deadline", "other"} {
		name := "lsmdb_raft_transport_send_failures_total{class=" + class + "}"
		if value, ok := got[name]; !ok || value != 0 {
			t.Errorf("%s = (%v, present=%v), want 0 before any failure", name, value, ok)
		}
	}
}

func TestEngineCountersAreRegisteredAndGrow(t *testing.T) {
	machine, err := kvstate.Open(t.TempDir(), &db.Options{FlushThreshold: 2, CompactionThreshold: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer machine.Close()
	metrics := newNodeMetrics(1)
	metrics.registerEngineStats(machine.EngineStats)
	names := []string{
		"lsmdb_engine_flushes_total", "lsmdb_engine_flush_seconds_total",
		"lsmdb_engine_compactions_total", "lsmdb_engine_compaction_seconds_total",
	}
	before := mustSnapshot(t, metrics)
	for _, name := range names {
		if value, ok := before[name]; !ok || value != 0 {
			t.Errorf("%s before any apply = (%v, present=%v), want 0", name, value, ok)
		}
	}
	for index := uint64(1); index <= 8; index++ {
		data, err := kvstate.EncodeCommand(lsmdbv1.Command_OPERATION_PUT, []byte(fmt.Sprintf("key-%d", index)), []byte("v"), "client", index)
		if err != nil {
			t.Fatal(err)
		}
		if err := machine.Apply(index, data); err != nil {
			t.Fatal(err)
		}
	}
	after := mustSnapshot(t, metrics)
	for _, name := range names {
		if after[name] <= before[name] {
			t.Errorf("%s = %v after 8 applies, want above %v", name, after[name], before[name])
		}
	}
}

func TestObservedStoreCountsDroppedAppendsOncePerMessage(t *testing.T) {
	metrics := newNodeMetrics(1)
	const name = "lsmdb_raft_malformed_appends_dropped_total"
	if value, ok := mustSnapshot(t, metrics)[name]; !ok || value != 0 {
		t.Fatalf("%s before any drop = (%v, present=%v), want 0", name, value, ok)
	}
	store := &observedStore{inner: fakeStableStore{}, metrics: metrics}
	for _, update := range []raft.Update{
		{DroppedAppend: "entry 4: term 0"}, {}, {DroppedAppend: "entry 3: index gap"}, {Entries: entries(1)},
	} {
		if err := store.Persist(update); err != nil {
			t.Fatal(err)
		}
	}
	if got := mustSnapshot(t, metrics)[name]; got != 2 {
		t.Fatalf("%s = %v after 2 dropped appends, want 2", name, got)
	}
}
