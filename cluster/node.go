// Package cluster exposes the replicated KV node and retrying Go client.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/db"
	"lsmdb/internal/kvstate"
	"lsmdb/internal/raft"
	"lsmdb/internal/raftgrpc"
	"lsmdb/internal/raftnode"
	"lsmdb/internal/raftstore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeConfig configures one member of a static cluster.
type NodeConfig struct {
	ID                uint64
	ListenAddress     string
	MetricsAddress    string
	DataDir           string
	Peers             map[uint64]string
	TickInterval      time.Duration
	ElectionTickMin   int
	ElectionTickMax   int
	HeartbeatTicks    int
	CheckQuorumTicks  int
	SnapshotThreshold uint64
	DBOptions         *db.Options
}

func (c *NodeConfig) defaults() {
	if c.TickInterval <= 0 {
		c.TickInterval = 50 * time.Millisecond
	}
	if c.ElectionTickMin <= 0 {
		c.ElectionTickMin = 6
	}
	if c.ElectionTickMax <= 0 {
		c.ElectionTickMax = 12
	}
	if c.HeartbeatTicks <= 0 {
		c.HeartbeatTicks = 1
	}
	if c.CheckQuorumTicks <= 0 {
		c.CheckQuorumTicks = c.ElectionTickMin
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 1000
	}
}

func (c NodeConfig) validate() error {
	if c.ID == 0 || c.ListenAddress == "" || c.DataDir == "" {
		return errors.New("node ID, listen address, and data directory are required")
	}
	if len(c.Peers) == 0 || c.Peers[c.ID] == "" {
		return errors.New("static peers must contain the local node")
	}
	return nil
}

// Node owns the network listener, Raft runtime, stable store, and LSM state machine.
type Node struct {
	config        NodeConfig
	runtime       *raftnode.Runtime
	machine       *kvstate.Machine
	transport     *raftgrpc.Transport
	metrics       *nodeMetrics
	metricsServer *http.Server
	metricsCancel context.CancelFunc
	server        *grpc.Server
	listener      net.Listener
	closeOnce     sync.Once
	closeErr      error
}

// StartNode opens durable state and starts serving client and Raft gRPC traffic.
func StartNode(config NodeConfig) (*Node, error) {
	config.defaults()
	if err := config.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(config.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create node data dir: %w", err)
	}
	machine, err := kvstate.Open(filepath.Join(config.DataDir, "state"), config.DBOptions)
	if err != nil {
		return nil, fmt.Errorf("open state machine: %w", err)
	}
	store, err := raftstore.Open(filepath.Join(config.DataDir, "raft"))
	if err != nil {
		_ = machine.Close()
		return nil, fmt.Errorf("open raft store: %w", err)
	}
	hard, entries := store.Load()
	snapshot := store.LoadSnapshot()
	if snapshot.Index > machine.AppliedIndex() {
		if err := machine.Restore(snapshot.Index, snapshot.Data); err != nil {
			_ = store.Close()
			_ = machine.Close()
			return nil, fmt.Errorf("restore state snapshot: %w", err)
		}
	}
	peerIDs := make([]uint64, 0, len(config.Peers))
	for id := range config.Peers {
		peerIDs = append(peerIDs, id)
	}
	sort.Slice(peerIDs, func(i, j int) bool { return peerIDs[i] < peerIDs[j] })
	core, err := raft.New(raft.Config{
		ID: config.ID, Peers: peerIDs, ElectionTickMin: config.ElectionTickMin,
		ElectionTickMax: config.ElectionTickMax, HeartbeatTicks: config.HeartbeatTicks,
		CheckQuorumTicks: config.CheckQuorumTicks, RandomSeed: config.ID,
		AppliedIndex: machine.AppliedIndex(),
	}, hard, entries, snapshot)
	if err != nil {
		_ = store.Close()
		_ = machine.Close()
		return nil, fmt.Errorf("restore raft core: %w", err)
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		_ = store.Close()
		_ = machine.Close()
		return nil, fmt.Errorf("listen on %s: %w", config.ListenAddress, err)
	}
	transport := raftgrpc.New(config.Peers)
	metrics := newNodeMetrics(config.ID)
	observed := &observedTransport{inner: transport, metrics: metrics}
	runtime, err := raftnode.Start(
		raftnode.Config{TickInterval: config.TickInterval, SnapshotThreshold: config.SnapshotThreshold}, core, store, observed, machine,
	)
	if err != nil {
		_ = listener.Close()
		_ = transport.Close()
		_ = store.Close()
		_ = machine.Close()
		return nil, err
	}
	node := &Node{
		config: config, runtime: runtime, machine: machine, transport: transport,
		metrics: metrics,
		server: grpc.NewServer(
			grpc.UnaryInterceptor(metrics.unaryInterceptor),
			grpc.StreamInterceptor(metrics.streamInterceptor),
			grpc.MaxRecvMsgSize(kvstate.MaxValueBytes+64*1024),
			grpc.MaxSendMsgSize(kvstate.MaxValueBytes+64*1024),
		),
		listener: listener,
	}
	if config.MetricsAddress != "" {
		node.metricsServer = metrics.serve(config.MetricsAddress)
	}
	metricsContext, metricsCancel := context.WithCancel(context.Background())
	node.metricsCancel = metricsCancel
	go node.observeMetrics(metricsContext)
	handler := &handler{node: node}
	lsmdbv1.RegisterKVServer(node.server, handler)
	lsmdbv1.RegisterRaftServer(node.server, handler)
	go func() { _ = node.server.Serve(listener) }()
	return node, nil
}

// Address returns the actual bound address, useful when configured with port zero.
func (n *Node) Address() string { return n.listener.Addr().String() }

// Status returns the local Raft status.
func (n *Node) Status(ctx context.Context) (raft.Status, error) { return n.runtime.Status(ctx) }

// Close stops network traffic and closes durable state.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.server.Stop()
		if n.metricsCancel != nil {
			n.metricsCancel()
		}
		if n.metricsServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := n.metricsServer.Shutdown(ctx); err != nil {
				n.closeErr = err
			}
			cancel()
		}
		if err := n.runtime.Close(); err != nil {
			n.closeErr = err
		}
		if err := n.transport.Close(); err != nil && n.closeErr == nil {
			n.closeErr = err
		}
		_ = n.listener.Close()
	})
	return n.closeErr
}

type handler struct {
	lsmdbv1.UnimplementedKVServer
	lsmdbv1.UnimplementedRaftServer
	node *Node
}

func (h *handler) Put(ctx context.Context, request *lsmdbv1.PutRequest) (*lsmdbv1.WriteResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	command, err := kvstate.EncodeCommand(
		lsmdbv1.Command_OPERATION_PUT, request.Key, request.Value, request.ClientId, request.RequestSeq,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return h.write(ctx, command)
}

func (h *handler) Delete(ctx context.Context, request *lsmdbv1.DeleteRequest) (*lsmdbv1.WriteResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	command, err := kvstate.EncodeCommand(
		lsmdbv1.Command_OPERATION_DELETE, request.Key, nil, request.ClientId, request.RequestSeq,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return h.write(ctx, command)
}

func (h *handler) write(ctx context.Context, command []byte) (*lsmdbv1.WriteResponse, error) {
	index, err := h.node.runtime.Propose(ctx, command)
	if err != nil {
		h.node.metrics.proposals.WithLabelValues("error").Inc()
		return nil, h.node.rpcError(err)
	}
	h.node.metrics.proposals.WithLabelValues("committed").Inc()
	current, err := h.node.runtime.Status(ctx)
	if err != nil {
		return nil, h.node.rpcError(err)
	}
	return &lsmdbv1.WriteResponse{Term: current.Term, LogIndex: index}, nil
}

func (n *Node) observeMetrics(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var previous raft.Status
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statusContext, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			current, err := n.runtime.Status(statusContext)
			cancel()
			if err != nil {
				continue
			}
			n.metrics.observeStatus(previous, current, n.machine.AppliedIndex())
			previous = current
		}
	}
}

func (h *handler) Get(ctx context.Context, request *lsmdbv1.GetRequest) (*lsmdbv1.GetResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	readIndex, err := h.node.runtime.LinearizableRead(ctx)
	if err != nil {
		return nil, h.node.rpcError(err)
	}
	value, found, err := h.node.machine.Get(request.Key)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &lsmdbv1.GetResponse{Found: found, Value: value, ReadIndex: readIndex}, nil
}

func (h *handler) Status(ctx context.Context, _ *lsmdbv1.StatusRequest) (*lsmdbv1.StatusResponse, error) {
	current, err := h.node.runtime.Status(ctx)
	if err != nil {
		return nil, h.node.rpcError(err)
	}
	peers := make([]*lsmdbv1.Peer, 0, len(h.node.config.Peers))
	for id, address := range h.node.config.Peers {
		peers = append(peers, &lsmdbv1.Peer{Id: id, Address: address})
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Id < peers[j].Id })
	return &lsmdbv1.StatusResponse{
		NodeId: current.ID, Role: current.Role.String(), Term: current.Term,
		LeaderId: current.LeaderID, CommitIndex: current.CommitIndex,
		AppliedIndex: h.node.machine.AppliedIndex(), Peers: peers,
		SnapshotIndex: current.SnapshotIndex, RetainedLogEntries: current.RetainedLogEntries,
	}, nil
}

func (h *handler) Send(ctx context.Context, request *lsmdbv1.RaftMessage) (*lsmdbv1.RaftAck, error) {
	return h.handleRaftMessage(ctx, request)
}

func (h *handler) InstallSnapshot(stream grpc.ClientStreamingServer[lsmdbv1.SnapshotChunk, lsmdbv1.RaftAck]) error {
	message, err := raftgrpc.ReceiveSnapshot(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if message.To != h.node.config.ID {
		return status.Errorf(codes.InvalidArgument, "snapshot addressed to node %d", message.To)
	}
	if err := h.node.runtime.Step(stream.Context(), message); err != nil {
		return h.node.rpcError(err)
	}
	return stream.SendAndClose(&lsmdbv1.RaftAck{})
}

func (h *handler) handleRaftMessage(ctx context.Context, request *lsmdbv1.RaftMessage) (*lsmdbv1.RaftAck, error) {
	message, err := raftgrpc.FromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if message.To != h.node.config.ID {
		return nil, status.Errorf(codes.InvalidArgument, "message addressed to node %d", message.To)
	}
	if err := h.node.runtime.Step(ctx, message); err != nil {
		return nil, h.node.rpcError(err)
	}
	return &lsmdbv1.RaftAck{}, nil
}

func (n *Node) rpcError(err error) error {
	if errors.Is(err, raft.ErrNotLeader) {
		current, _ := n.runtime.Status(context.Background())
		detail := &lsmdbv1.NotLeader{LeaderId: current.LeaderID, LeaderAddress: n.config.Peers[current.LeaderID]}
		base := status.New(codes.FailedPrecondition, "node is not the Raft leader")
		withDetail, detailErr := base.WithDetails(detail)
		if detailErr == nil {
			return withDetail.Err()
		}
		return base.Err()
	}
	if errors.Is(err, raft.ErrReadNotReady) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, err.Error())
}
