package cluster

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"lsmdb/db"
	"lsmdb/internal/raft"
	"lsmdb/internal/raftnode"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeMetrics struct {
	labels            prometheus.Labels
	registry          *prometheus.Registry
	role              *prometheus.GaugeVec
	term              prometheus.Gauge
	leader            prometheus.Gauge
	commit            prometheus.Gauge
	applied           prometheus.Gauge
	logLength         prometheus.Gauge
	snapshotIndex     prometheus.Gauge
	replicationLag    *prometheus.GaugeVec
	elections         prometheus.Counter
	leadershipChanges prometheus.Counter
	quorumLoss        prometheus.Counter
	proposals         *prometheus.CounterVec
	transportFailures *prometheus.CounterVec
	sendFailures      *prometheus.CounterVec
	rpcRequests       *prometheus.CounterVec
	rpcDuration       *prometheus.HistogramVec
	logSyncs          prometheus.Counter
	logSyncEntries    prometheus.Counter
	hardStateSyncs    prometheus.Counter
	droppedAppends    prometheus.Counter
	appendMessages    *prometheus.CounterVec
	applySeconds      prometheus.Histogram
	snapshotSeconds   prometheus.Histogram
}

func newNodeMetrics(nodeID uint64) *nodeMetrics {
	constant := prometheus.Labels{"node_id": strconv.FormatUint(nodeID, 10)}
	m := &nodeMetrics{
		labels:            constant,
		registry:          prometheus.NewRegistry(),
		role:              prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lsmdb_raft_role", Help: "One-hot Raft role.", ConstLabels: constant}, []string{"role"}),
		term:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "lsmdb_raft_term", Help: "Current Raft term.", ConstLabels: constant}),
		leader:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "lsmdb_raft_leader_id", Help: "Known leader node ID.", ConstLabels: constant}),
		commit:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "lsmdb_raft_commit_index", Help: "Highest committed log index.", ConstLabels: constant}),
		applied:           prometheus.NewGauge(prometheus.GaugeOpts{Name: "lsmdb_raft_applied_index", Help: "Highest locally applied log index.", ConstLabels: constant}),
		logLength:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "lsmdb_raft_log_length", Help: "Number of retained Raft log entries after compaction.", ConstLabels: constant}),
		snapshotIndex:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "lsmdb_raft_snapshot_index", Help: "Highest durable Raft snapshot index.", ConstLabels: constant}),
		replicationLag:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "lsmdb_raft_replication_lag", Help: "Leader log entries not yet matched by a peer.", ConstLabels: constant}, []string{"peer_id"}),
		elections:         prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_elections_total", Help: "Observed election attempts.", ConstLabels: constant}),
		leadershipChanges: prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_leadership_changes_total", Help: "Observed transitions into leader.", ConstLabels: constant}),
		quorumLoss:        prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_quorum_loss_total", Help: "Leader stepdowns without a higher term.", ConstLabels: constant}),
		proposals:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lsmdb_proposals_total", Help: "Client proposals by outcome.", ConstLabels: constant}, []string{"result"}),
		transportFailures: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lsmdb_raft_transport_failures_total", Help: "Failed outbound Raft RPCs.", ConstLabels: constant}, []string{"peer_id"}),
		sendFailures:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lsmdb_raft_transport_send_failures_total", Help: "Failed outbound Raft sends by error class: deadline (the send's deadline expired) or other.", ConstLabels: constant}, []string{"class"}),
		rpcRequests:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lsmdb_grpc_requests_total", Help: "gRPC requests by method and status.", ConstLabels: constant}, []string{"method", "code"}),
		rpcDuration:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lsmdb_grpc_request_duration_seconds", Help: "gRPC request latency.", ConstLabels: constant, Buckets: prometheus.DefBuckets}, []string{"method"}),
		logSyncs:          prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_log_syncs_total", Help: "Raft log append syncs: successful persists carrying at least one entry.", ConstLabels: constant}),
		logSyncEntries:    prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_log_sync_entries_total", Help: "Log entries written by Raft log append syncs.", ConstLabels: constant}),
		hardStateSyncs:    prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_hardstate_syncs_total", Help: "Successful persists carrying a term or vote change.", ConstLabels: constant}),
		droppedAppends:    prometheus.NewCounter(prometheus.CounterOpts{Name: "lsmdb_raft_malformed_appends_dropped_total", Help: "Appends dropped because their entries could not have come from a correct leader.", ConstLabels: constant}),
		appendMessages:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lsmdb_raft_append_messages_total", Help: "AppendEntries messages handed to the transport, by emitting code path.", ConstLabels: constant}, []string{"origin"}),
		applySeconds:      prometheus.NewHistogram(prometheus.HistogramOpts{Name: "lsmdb_raft_apply_seconds", Help: "Time per committed-entry state-machine apply.", ConstLabels: constant, Buckets: prometheus.ExponentialBuckets(1e-6, 4, 12)}),
		snapshotSeconds:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "lsmdb_raft_snapshot_seconds", Help: "Time per durable Raft snapshot persist (creation or install).", ConstLabels: constant, Buckets: prometheus.ExponentialBuckets(1e-3, 2, 16)}),
	}
	for _, origin := range []raft.MessageOrigin{raft.OriginOther, raft.OriginHeartbeat, raft.OriginProposal, raft.OriginCommitAdvance, raft.OriginAckResend} {
		m.appendMessages.WithLabelValues(origin.String())
	}
	for _, class := range []string{sendFailureDeadline, sendFailureOther} {
		m.sendFailures.WithLabelValues(class)
	}
	m.registry.MustRegister(
		m.role, m.term, m.leader, m.commit, m.applied, m.logLength, m.snapshotIndex, m.replicationLag,
		m.elections, m.leadershipChanges, m.quorumLoss, m.proposals,
		m.transportFailures, m.sendFailures, m.rpcRequests, m.rpcDuration,
		m.logSyncs, m.logSyncEntries, m.hardStateSyncs, m.droppedAppends, m.appendMessages, m.applySeconds, m.snapshotSeconds,
	)
	return m
}

// registerEngineStats exposes the state machine's flush and compaction counts
// and times, read from stats at each scrape.
func (m *nodeMetrics) registerEngineStats(stats func() db.Stats) {
	counter := func(name, help string, value func(db.Stats) float64) prometheus.Collector {
		return prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help, ConstLabels: m.labels},
			func() float64 { return value(stats()) })
	}
	m.registry.MustRegister(
		counter("lsmdb_engine_flushes_total", "Engine memtable flushes published.",
			func(s db.Stats) float64 { return float64(s.Flushes) }),
		counter("lsmdb_engine_flush_seconds_total", "Time in engine flushes, excluding the compactions they trigger.",
			func(s db.Stats) float64 { return s.FlushTime.Seconds() }),
		counter("lsmdb_engine_compactions_total", "Engine compactions published.",
			func(s db.Stats) float64 { return float64(s.Compactions) }),
		counter("lsmdb_engine_compaction_seconds_total", "Time in engine compactions.",
			func(s db.Stats) float64 { return s.CompactionTime.Seconds() }),
	)
}

// snapshot flattens the registry into name{label=value,...} keys, omitting the
// node_id constant label. Histograms appear as name_sum and name_count.
func (m *nodeMetrics) snapshot() (map[string]float64, error) {
	families, err := m.registry.Gather()
	if err != nil {
		return nil, err
	}
	values := make(map[string]float64)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			var labels []string
			for _, label := range metric.GetLabel() {
				if label.GetName() != "node_id" {
					labels = append(labels, label.GetName()+"="+label.GetValue())
				}
			}
			suffix := ""
			if len(labels) > 0 {
				suffix = "{" + strings.Join(labels, ",") + "}"
			}
			name := family.GetName()
			switch {
			case metric.GetCounter() != nil:
				values[name+suffix] = metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				values[name+suffix] = metric.GetGauge().GetValue()
			case metric.GetHistogram() != nil:
				values[name+"_sum"+suffix] = metric.GetHistogram().GetSampleSum()
				values[name+"_count"+suffix] = float64(metric.GetHistogram().GetSampleCount())
			}
		}
	}
	return values, nil
}

func (m *nodeMetrics) unaryInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	response, err := handler(ctx, request)
	m.rpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
	m.rpcRequests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
	return response, err
}

func (m *nodeMetrics) streamInterceptor(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(server, stream)
	m.rpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
	m.rpcRequests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
	return err
}

func (m *nodeMetrics) serve(address string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() { _ = server.ListenAndServe() }()
	return server
}

func (m *nodeMetrics) observeStatus(previous, current raft.Status, applied uint64) {
	for _, role := range []raft.Role{raft.Follower, raft.PreCandidate, raft.Candidate, raft.Leader} {
		value := 0.0
		if current.Role == role {
			value = 1
		}
		m.role.WithLabelValues(role.String()).Set(value)
	}
	m.term.Set(float64(current.Term))
	m.leader.Set(float64(current.LeaderID))
	m.commit.Set(float64(current.CommitIndex))
	m.applied.Set(float64(applied))
	m.logLength.Set(float64(current.RetainedLogEntries))
	m.snapshotIndex.Set(float64(current.SnapshotIndex))
	if current.Role == raft.Candidate && previous.Role != raft.Candidate {
		m.elections.Inc()
	}
	if current.Role == raft.Leader && previous.Role != raft.Leader {
		m.leadershipChanges.Inc()
	}
	if previous.Role == raft.Leader && current.Role != raft.Leader && previous.Term == current.Term {
		m.quorumLoss.Inc()
	}
	for peer, match := range current.MatchIndex {
		lag := current.LastLogIndex - min(current.LastLogIndex, match)
		m.replicationLag.WithLabelValues(strconv.FormatUint(peer, 10)).Set(float64(lag))
	}
}

type observedTransport struct {
	inner   raftnode.Transport
	metrics *nodeMetrics
}

func (t *observedTransport) Send(ctx context.Context, message raft.Message) error {
	if message.Type == raft.MsgAppend {
		t.metrics.appendMessages.WithLabelValues(message.Origin.String()).Inc()
	}
	err := t.inner.Send(ctx, message)
	t.countFailure(message, err)
	return err
}

func (t *observedTransport) SendSnapshot(ctx context.Context, message raft.Message, reader io.Reader, size uint64, checksum uint32) error {
	err := t.inner.SendSnapshot(ctx, message, reader, size, checksum)
	t.countFailure(message, err)
	return err
}

func (t *observedTransport) countFailure(message raft.Message, err error) {
	if err == nil {
		return
	}
	t.metrics.transportFailures.WithLabelValues(strconv.FormatUint(message.To, 10)).Inc()
	t.metrics.sendFailures.WithLabelValues(sendFailureClass(err)).Inc()
}

const (
	sendFailureDeadline = "deadline"
	sendFailureOther    = "other"
)

// sendFailureClass separates sends whose deadline expired, whether the error
// came from the local context or back from the peer as a gRPC status, from
// every other failure.
func sendFailureClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return sendFailureDeadline
	}
	return sendFailureOther
}

// observedStore counts Raft persistence effects. raftstore.Store.append issues
// exactly one raft.log fsync per persist that carries entries.
type observedStore struct {
	inner   raftnode.StableStore
	metrics *nodeMetrics
}

func (s *observedStore) Persist(update raft.Update) error {
	err := s.inner.Persist(update)
	if err == nil {
		s.count(update)
	}
	return err
}

func (s *observedStore) PersistSnapshot(update raft.Update, writeData func(io.Writer) error) error {
	start := time.Now()
	err := s.inner.PersistSnapshot(update, writeData)
	if err == nil {
		s.metrics.snapshotSeconds.Observe(time.Since(start).Seconds())
		s.count(update)
	}
	return err
}

func (s *observedStore) count(update raft.Update) {
	if len(update.Entries) > 0 {
		s.metrics.logSyncs.Inc()
		s.metrics.logSyncEntries.Add(float64(len(update.Entries)))
	}
	if update.HardState != nil {
		s.metrics.hardStateSyncs.Inc()
	}
	if update.DroppedAppend != "" {
		s.metrics.droppedAppends.Inc()
	}
}

func (s *observedStore) OpenSnapshot(index uint64) (io.ReadCloser, uint64, uint32, error) {
	return s.inner.OpenSnapshot(index)
}

func (s *observedStore) Close() error { return s.inner.Close() }

// observedMachine times committed-entry application.
type observedMachine struct {
	inner   raftnode.StateMachine
	metrics *nodeMetrics
}

func (m *observedMachine) Apply(index uint64, command []byte) error {
	start := time.Now()
	err := m.inner.Apply(index, command)
	m.metrics.applySeconds.Observe(time.Since(start).Seconds())
	return err
}

func (m *observedMachine) AppliedIndex() uint64 { return m.inner.AppliedIndex() }

func (m *observedMachine) WriteSnapshot(writer io.Writer) (uint64, error) {
	return m.inner.WriteSnapshot(writer)
}

func (m *observedMachine) RestoreSnapshot(index uint64, size uint64, reader io.Reader) error {
	return m.inner.RestoreSnapshot(index, size, reader)
}

func (m *observedMachine) Close() error { return m.inner.Close() }
