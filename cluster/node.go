// Package cluster exposes the replicated KV node and retrying Go client.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	Voters            []uint64
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
	if len(c.Voters) == 0 {
		for id := range c.Peers {
			c.Voters = append(c.Voters, id)
		}
		sort.Slice(c.Voters, func(i, j int) bool { return c.Voters[i] < c.Voters[j] })
	}
}

func (c NodeConfig) validate() error {
	if c.ID == 0 || c.ListenAddress == "" || c.DataDir == "" {
		return errors.New("node ID, listen address, and data directory are required")
	}
	if len(c.Peers) == 0 || c.Peers[c.ID] == "" {
		return errors.New("static peers must contain the local node")
	}
	seen := make(map[uint64]struct{}, len(c.Voters))
	for _, id := range c.Voters {
		if id == 0 || c.Peers[id] == "" {
			return fmt.Errorf("initial voter %d is not in the peer map", id)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate initial voter %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) == 0 {
		return errors.New("at least one initial voter is required")
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
	snapshot := store.SnapshotMetadata()
	if snapshot.Index > machine.AppliedIndex() {
		reader, size, _, openErr := store.OpenSnapshot(snapshot.Index)
		if openErr != nil {
			_ = store.Close()
			_ = machine.Close()
			return nil, fmt.Errorf("open state snapshot: %w", openErr)
		}
		restoreErr := machine.RestoreSnapshot(snapshot.Index, size, reader)
		closeErr := reader.Close()
		if restoreErr != nil {
			_ = store.Close()
			_ = machine.Close()
			return nil, fmt.Errorf("restore state snapshot: %w", restoreErr)
		}
		if closeErr != nil {
			_ = store.Close()
			_ = machine.Close()
			return nil, fmt.Errorf("close state snapshot: %w", closeErr)
		}
	}
	core, err := raft.New(raft.Config{
		ID: config.ID, Peers: config.Voters, ElectionTickMin: config.ElectionTickMin,
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
		VoterIds: current.Membership.Voters, JointVoterIds: current.Membership.JointVoters,
	}, nil
}

func (h *handler) ChangeMembership(ctx context.Context, request *lsmdbv1.ChangeMembershipRequest) (*lsmdbv1.ChangeMembershipResponse, error) {
	if request == nil || len(request.VoterIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one voter ID is required")
	}
	seen := make(map[uint64]struct{}, len(request.VoterIds))
	for _, id := range request.VoterIds {
		if h.node.config.Peers[id] == "" {
			return nil, status.Errorf(codes.InvalidArgument, "voter %d is not in the configured peer map", id)
		}
		if _, ok := seen[id]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate voter %d", id)
		}
		seen[id] = struct{}{}
	}
	index, err := h.node.runtime.ChangeMembership(ctx, request.VoterIds)
	if err != nil {
		return nil, h.node.rpcError(err)
	}
	current, err := h.node.runtime.Status(ctx)
	if err != nil {
		return nil, h.node.rpcError(err)
	}
	return &lsmdbv1.ChangeMembershipResponse{Term: current.Term, LogIndex: index}, nil
}

func (h *handler) Send(ctx context.Context, request *lsmdbv1.RaftMessage) (*lsmdbv1.RaftAck, error) {
	return h.handleRaftMessage(ctx, request)
}

func (h *handler) InstallSnapshot(stream grpc.ClientStreamingServer[lsmdbv1.SnapshotChunk, lsmdbv1.RaftAck]) error {
	tmp, err := os.CreateTemp(h.node.config.DataDir, ".snapshot-receive-*")
	if err != nil {
		return status.Error(codes.Internal, "create snapshot staging file")
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	message, err := raftgrpc.ReceiveSnapshotTo(stream, tmp)
	if err != nil {
		_ = tmp.Close()
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return status.Error(codes.Internal, "sync snapshot staging file")
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		_ = tmp.Close()
		return status.Error(codes.Internal, "rewind snapshot staging file")
	}
	if message.To != h.node.config.ID {
		_ = tmp.Close()
		return status.Errorf(codes.InvalidArgument, "snapshot addressed to node %d", message.To)
	}
	if err := h.node.runtime.StepSnapshot(stream.Context(), message, tmp); err != nil {
		_ = tmp.Close()
		return h.node.rpcError(err)
	}
	if err := tmp.Close(); err != nil {
		return status.Error(codes.Internal, "close snapshot staging file")
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
	if errors.Is(err, raft.ErrMembershipChangeInProgress) || errors.Is(err, raft.ErrNoMembershipChange) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, err.Error())
}
