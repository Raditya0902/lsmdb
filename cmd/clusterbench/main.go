package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"lsmdb/cluster"
	"lsmdb/internal/raft"
)

type result struct {
	Operations      int     `json:"operations"`
	Concurrency     int     `json:"concurrency"`
	OpsPerSecond    float64 `json:"ops_per_second"`
	P50MS           float64 `json:"p50_ms"`
	P95MS           float64 `json:"p95_ms"`
	P99MS           float64 `json:"p99_ms"`
	FailoverMS      float64 `json:"failover_ms"`
	FailedOps       int     `json:"failed_operations"`
	FailoverOK      bool    `json:"failover_succeeded"`
	GoVersion       string  `json:"go_version"`
	OperatingSystem string  `json:"operating_system"`
	Architecture    string  `json:"architecture"`
}

func main() {
	operations := flag.Int("operations", 1000, "number of replicated writes")
	valueSize := flag.Int("value-size", 128, "value size in bytes")
	concurrency := flag.Int("concurrency", 1, "number of independent clients issuing writes")
	timeout := flag.Duration("timeout", 2*time.Minute, "write-workload timeout")
	flag.Parse()
	if *operations <= 0 || *valueSize < 0 || *concurrency <= 0 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "operations, concurrency, and timeout must be positive; value-size must be non-negative")
		os.Exit(2)
	}

	base, err := os.MkdirTemp("", "lsmdb-clusterbench-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(base)
	addresses := map[uint64]string{1: freeAddress(), 2: freeAddress(), 3: freeAddress()}
	nodes := make(map[uint64]*cluster.Node)
	for id := uint64(1); id <= 3; id++ {
		node, err := cluster.StartNode(cluster.NodeConfig{
			ID: id, ListenAddress: addresses[id], DataDir: fmt.Sprintf("%s/node-%d", base, id),
			Peers: addresses, TickInterval: 20 * time.Millisecond,
			ElectionTickMin: 5, ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5,
		})
		if err != nil {
			panic(err)
		}
		nodes[id] = node
	}
	defer func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	}()

	electionContext, cancelElection := context.WithTimeout(context.Background(), 10*time.Second)
	leader := waitLeader(electionContext, nodes)
	cancelElection()
	clusterAddresses := []string{addresses[1], addresses[2], addresses[3]}
	clients := make([]*cluster.Client, *concurrency)
	for i := range clients {
		client, err := cluster.NewClient(clusterAddresses)
		if err != nil {
			panic(err)
		}
		clients[i] = client
	}
	defer func() {
		for _, client := range clients {
			_ = client.Close()
		}
	}()
	value := make([]byte, *valueSize)
	latencies := make([]time.Duration, *operations)
	var failed atomic.Int64
	var workers sync.WaitGroup
	workloadContext, cancelWorkload := context.WithTimeout(context.Background(), *timeout)
	started := time.Now()
	for worker, client := range clients {
		workers.Add(1)
		go func(worker int, client *cluster.Client) {
			defer workers.Done()
			for i := worker; i < *operations; i += *concurrency {
				operationStart := time.Now()
				_, err := client.Put(workloadContext, []byte(fmt.Sprintf("key-%09d", i)), value)
				latencies[i] = time.Since(operationStart)
				if err != nil {
					failed.Add(1)
				}
			}
		}(worker, client)
	}
	workers.Wait()
	duration := time.Since(started)
	cancelWorkload()

	_ = nodes[leader].Close()
	failoverContext, cancelFailover := context.WithTimeout(context.Background(), 10*time.Second)
	failoverStarted := time.Now()
	_, failoverErr := clients[0].Put(failoverContext, []byte("failover-probe"), value)
	failover := time.Since(failoverStarted)
	cancelFailover()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	failedWrites := int(failed.Load())
	output := result{
		Operations: *operations, Concurrency: *concurrency,
		OpsPerSecond: float64(*operations-failedWrites) / duration.Seconds(),
		P50MS:        percentile(latencies, 0.50), P95MS: percentile(latencies, 0.95),
		P99MS: percentile(latencies, 0.99), FailoverMS: float64(failover.Microseconds()) / 1000,
		FailedOps: failedWrites, FailoverOK: failoverErr == nil,
		GoVersion: runtime.Version(), OperatingSystem: runtime.GOOS,
		Architecture: runtime.GOARCH,
	}
	encoded, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(encoded))
}

func freeAddress() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitLeader(ctx context.Context, nodes map[uint64]*cluster.Node) uint64 {
	for {
		for id, node := range nodes {
			status, err := node.Status(ctx)
			if err == nil && status.Role == raft.Leader {
				return id
			}
		}
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func percentile(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * quantile)
	return float64(values[index].Microseconds()) / 1000
}
