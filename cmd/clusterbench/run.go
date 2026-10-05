package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"lsmdb/cluster"
	"lsmdb/internal/benchenv"
	"lsmdb/internal/raft"
)

const opTimeout = 10 * time.Second

// runOnce measures one fresh three-node cluster with the given client count.
func runOnce(opts options, clients, repetition int) (RunResult, error) {
	result := RunResult{Repetition: repetition, Clients: clients}
	base, err := os.MkdirTemp(opts.dataDir, "lsmdb-clusterbench-")
	if err != nil {
		return result, fmt.Errorf("create run dir: %w", err)
	}
	defer os.RemoveAll(base)

	if result.FsyncBefore, err = benchenv.MeasureFsync(base, fsyncProbeSamples); err != nil {
		return result, err
	}
	nodes, addresses, err := startCluster(opts, base)
	if err != nil {
		return result, err
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	}()
	if err := waitLeader(nodes, 10*time.Second); err != nil {
		return result, err
	}
	workers, err := connectClients(clients, addresses, opts.valueSize)
	if err != nil {
		return result, err
	}
	defer func() {
		for _, worker := range workers {
			_ = worker.client.Close()
		}
	}()

	runContext, stopClients := context.WithCancel(context.Background())
	var group sync.WaitGroup
	for i, worker := range workers {
		group.Add(1)
		go func(client int, worker *clientWorker) {
			defer group.Done()
			worker.loop(runContext, client, opts.seed)
		}(i, worker)
	}

	time.Sleep(opts.warmup)
	windowStart := time.Now()
	startStatus, startErr := statusAll(nodes)
	startCounters, startCountersErr := metricSnapshots(nodes)
	time.Sleep(opts.duration)
	windowEnd := time.Now()
	endStatus, endErr := statusAll(nodes)
	endCounters, endCountersErr := metricSnapshots(nodes)
	stopClients()
	group.Wait()
	if err := errors.Join(startErr, endErr, startCountersErr, endCountersErr); err != nil {
		return result, fmt.Errorf("read window boundaries: %w", err)
	}

	var ops []opRecord
	for _, worker := range workers {
		ops = append(ops, worker.ops...)
	}
	window := collectWindow(ops, windowStart, windowEnd)
	result.WindowSeconds = windowEnd.Sub(windowStart).Seconds()
	result.OpsOK, result.OpsFailed, result.Retries = window.OK, window.Failed, window.Retries
	result.Throughput = float64(window.OK) / result.WindowSeconds
	result.Latency = latencySummary(window.Latencies)
	result.TermsStart, result.TermsEnd = terms(startStatus), terms(endStatus)
	result.Valid, result.InvalidReason = validity(result.TermsStart, result.TermsEnd)
	result.ElectionsInWindow = maxOf(result.TermsEnd) - min(maxOf(result.TermsStart), maxOf(result.TermsEnd))
	result.CommittedEntries = maxCommit(endStatus) - min(maxCommit(startStatus), maxCommit(endStatus))
	result.Counters = counterDeltas(startCounters, endCounters, result.CommittedEntries)

	if result.FsyncAfter, err = benchenv.MeasureFsync(base, fsyncProbeSamples); err != nil {
		return result, err
	}
	return result, nil
}

type clientWorker struct {
	client *cluster.Client
	value  []byte
	ops    []opRecord
}

// connectClients creates one client per worker and issues one untimed Put each
// so every client has found the leader before the warmup starts.
func connectClients(count int, addresses []string, valueSize int) ([]*clientWorker, error) {
	workers := make([]*clientWorker, 0, count)
	for i := 0; i < count; i++ {
		client, err := cluster.NewClient(addresses)
		if err != nil {
			return nil, err
		}
		worker := &clientWorker{client: client, value: make([]byte, valueSize)}
		workers = append(workers, worker)
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		_, err = client.Put(ctx, []byte(fmt.Sprintf("leader-probe-%02d", i)), worker.value)
		cancel()
		if err != nil {
			for _, opened := range workers {
				_ = opened.client.Close()
			}
			return nil, fmt.Errorf("client %d leader discovery: %w", i, err)
		}
	}
	return workers, nil
}

// loop issues closed-loop Puts until ctx is cancelled, recording every result.
func (w *clientWorker) loop(ctx context.Context, client int, seed int64) {
	rng := newKeyRand(seed, client)
	for ctx.Err() == nil {
		key := []byte(keyFor(client, rng))
		attemptsBefore := w.client.Attempts()
		opContext, cancel := context.WithTimeout(ctx, opTimeout)
		start := time.Now()
		_, err := w.client.Put(opContext, key, w.value)
		done := time.Now()
		cancel()
		w.ops = append(w.ops, opRecord{
			done: done, latency: done.Sub(start), ok: err == nil,
			attempts: w.client.Attempts() - attemptsBefore,
		})
	}
}

func startCluster(opts options, base string) ([]*cluster.Node, []string, error) {
	peers := make(map[uint64]string, clusterNodes)
	for id := uint64(1); id <= clusterNodes; id++ {
		address, err := freeAddress()
		if err != nil {
			return nil, nil, err
		}
		peers[id] = address
	}
	nodes := make([]*cluster.Node, 0, clusterNodes)
	addresses := make([]string, 0, clusterNodes)
	for id := uint64(1); id <= clusterNodes; id++ {
		node, err := cluster.StartNode(cluster.NodeConfig{
			ID: id, ListenAddress: peers[id], DataDir: filepath.Join(base, fmt.Sprintf("node-%d", id)),
			Peers: peers, TickInterval: opts.tick, ElectionTickMin: electionTickMin,
			ElectionTickMax: electionTickMax, HeartbeatTicks: heartbeatTicks,
			CheckQuorumTicks: checkQuorumTicks, SnapshotThreshold: opts.snapshotThreshold,
		})
		if err != nil {
			for _, started := range nodes {
				_ = started.Close()
			}
			return nil, nil, fmt.Errorf("start node %d: %w", id, err)
		}
		nodes = append(nodes, node)
		addresses = append(addresses, peers[id])
	}
	return nodes, addresses, nil
}

func freeAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	return address, listener.Close()
}

func waitLeader(nodes []*cluster.Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		statuses, err := statusAll(nodes)
		if err == nil {
			for _, status := range statuses {
				if status.Role == raft.Leader {
					return nil
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("no leader elected within %s", timeout)
}

func statusAll(nodes []*cluster.Node) ([]raft.Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	statuses := make([]raft.Status, 0, len(nodes))
	for _, node := range nodes {
		status, err := node.Status(ctx)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func metricSnapshots(nodes []*cluster.Node) ([]map[string]float64, error) {
	snapshots := make([]map[string]float64, 0, len(nodes))
	for _, node := range nodes {
		values, err := node.MetricSnapshot()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, values)
	}
	return snapshots, nil
}

func terms(statuses []raft.Status) []uint64 {
	out := make([]uint64, len(statuses))
	for i, status := range statuses {
		out[i] = status.Term
	}
	return out
}

func maxOf(values []uint64) uint64 {
	var highest uint64
	for _, value := range values {
		highest = max(highest, value)
	}
	return highest
}

func maxCommit(statuses []raft.Status) uint64 {
	var highest uint64
	for _, status := range statuses {
		highest = max(highest, status.CommitIndex)
	}
	return highest
}
