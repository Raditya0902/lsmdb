package cluster

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"lsmdb/internal/raft"
	"lsmdb/internal/raftgrpc"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type nodeMetrics struct {
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
	rpcRequests       *prometheus.CounterVec
	rpcDuration       *prometheus.HistogramVec
}

func newNodeMetrics(nodeID uint64) *nodeMetrics {
	constant := prometheus.Labels{"node_id": strconv.FormatUint(nodeID, 10)}
	m := &nodeMetrics{
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
		rpcRequests:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "lsmdb_grpc_requests_total", Help: "gRPC requests by method and status.", ConstLabels: constant}, []string{"method", "code"}),
		rpcDuration:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "lsmdb_grpc_request_duration_seconds", Help: "gRPC request latency.", ConstLabels: constant, Buckets: prometheus.DefBuckets}, []string{"method"}),
	}
	m.registry.MustRegister(
		m.role, m.term, m.leader, m.commit, m.applied, m.logLength, m.snapshotIndex, m.replicationLag,
		m.elections, m.leadershipChanges, m.quorumLoss, m.proposals,
		m.transportFailures, m.rpcRequests, m.rpcDuration,
	)
	return m
}

func (m *nodeMetrics) unaryInterceptor(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	response, err := handler(ctx, request)
	m.rpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
	m.rpcRequests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
	return response, err
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
	inner   *raftgrpc.Transport
	metrics *nodeMetrics
}

func (t *observedTransport) Send(ctx context.Context, message raft.Message) error {
	err := t.inner.Send(ctx, message)
	if err != nil {
		t.metrics.transportFailures.WithLabelValues(strconv.FormatUint(message.To, 10)).Inc()
	}
	return err
}
