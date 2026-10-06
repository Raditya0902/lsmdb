package cluster

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	lsmdbv1 "lsmdb/api/lsmdb/v1"
	"lsmdb/internal/kvstate"
	"lsmdb/internal/raft"
)

// startGRPCCluster runs three nodes over localhost gRPC with the benchmark's
// Raft timing, so the server's message-size limit applies to every append.
func startGRPCCluster(t *testing.T) (map[uint64]*Node, map[uint64]string) {
	t.Helper()
	addresses := map[uint64]string{1: freeAddress(t), 2: freeAddress(t), 3: freeAddress(t)}
	nodes := make(map[uint64]*Node)
	dir := t.TempDir()
	for id := uint64(1); id <= 3; id++ {
		node, err := StartNode(NodeConfig{
			ID: id, ListenAddress: addresses[id], DataDir: filepath.Join(dir, fmt.Sprintf("node-%d", id)),
			Peers: addresses, TickInterval: 20 * time.Millisecond,
			ElectionTickMin: 5, ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5,
		})
		if err != nil {
			t.Fatalf("StartNode(%d): %v", id, err)
		}
		nodes[id] = node
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	})
	return nodes, addresses
}

func addressList(addresses map[uint64]string) []string {
	return []string{addresses[1], addresses[2], addresses[3]}
}

func nodeStatuses(t *testing.T, nodes map[uint64]*Node) map[uint64]raft.Status {
	t.Helper()
	statuses := make(map[uint64]raft.Status, len(nodes))
	for id, node := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		current, err := node.Status(ctx)
		cancel()
		if err != nil {
			t.Fatalf("status of node %d: %v", id, err)
		}
		statuses[id] = current
	}
	return statuses
}

func maxTerm(statuses map[uint64]raft.Status) uint64 {
	var term uint64
	for _, current := range statuses {
		term = max(term, current.Term)
	}
	return term
}

func equalCommitIndexes(statuses map[uint64]raft.Status) bool {
	commit := statuses[1].CommitIndex
	for _, current := range statuses {
		if current.CommitIndex != commit {
			return false
		}
	}
	return true
}

// TestLargeValueWritesKeepOneLeader is the analysis report's A3 run: 4 clients
// write 24 values of 2.5 MiB. Uncapped appends that carry two such entries
// exceed the 4 MiB + 64 KiB receive limit, so a follower hears nothing.
func TestLargeValueWritesKeepOneLeader(t *testing.T) {
	if raceEnabled {
		t.Skip("until phase-12b item 2: under -race, each follower's per-append deep copy of a log of up to 60 MiB stalls replication")
	}
	const clients, writes, valueSize = 4, 24, 5 << 19
	nodes, addresses := startGRPCCluster(t)
	waitForLeader(t, nodes, 0)
	before := nodeStatuses(t, nodes)
	value := bytes.Repeat([]byte{'v'}, valueSize)

	var wg sync.WaitGroup
	errs := make(chan error, writes)
	for c := 0; c < clients; c++ {
		client, err := NewClient(addressList(addresses))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := c; i < writes; i += clients {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				_, err := client.Put(ctx, []byte(fmt.Sprintf("large-%02d", i)), value)
				cancel()
				if err != nil {
					errs <- fmt.Errorf("write %d: %w", i, err)
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	failed := 0
	for err := range errs {
		failed++
		t.Log(err)
	}

	// Followers get a short settling time to reach the leader's commit index.
	after := nodeStatuses(t, nodes)
	for deadline := time.Now().Add(3 * time.Second); !equalCommitIndexes(after) && time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		after = nodeStatuses(t, nodes)
	}
	for id := uint64(1); id <= 3; id++ {
		t.Logf("node %d: role %s, term %d -> %d, commit %d, last index %d",
			id, after[id].Role, before[id].Term, after[id].Term, after[id].CommitIndex, after[id].LastLogIndex)
	}
	elections := maxTerm(after) - maxTerm(before)
	t.Logf("%d of %d writes failed; %d elections", failed, writes, elections)
	if failed != 0 {
		t.Errorf("%d writes failed", failed)
	}
	if elections != 0 {
		t.Errorf("%d elections during the large-value writes, want 0", elections)
	}
	if !equalCommitIndexes(after) {
		t.Errorf("commit indexes differ after settling: a follower is lagging")
	}
}

func TestMaxSizeValueCommitsAndReplicates(t *testing.T) {
	nodes, addresses := startGRPCCluster(t)
	waitForLeader(t, nodes, 0)
	client, err := NewClient(addressList(addresses))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	key := bytes.Repeat([]byte{'k'}, kvstate.MaxKeyBytes)
	value := bytes.Repeat([]byte{'v'}, kvstate.MaxValueBytes)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := client.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("4 MiB Put: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for id, node := range nodes {
		for node.machine.AppliedIndex() < response.LogIndex && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if applied := node.machine.AppliedIndex(); applied < response.LogIndex {
			t.Fatalf("node %d applied %d, want at least %d", id, applied, response.LogIndex)
		}
		stored, found, err := node.machine.Get(key)
		if err != nil || !found || !bytes.Equal(stored, value) {
			t.Fatalf("node %d holds found=%v len=%d err=%v, want the 4 MiB value", id, found, len(stored), err)
		}
	}
}

func TestOversizedPutIsRejectedAndLeaderKeepsServing(t *testing.T) {
	nodes, addresses := startGRPCCluster(t)
	leaderID := waitForLeader(t, nodes, 0)
	before := nodeStatuses(t, nodes)[leaderID]
	connection, err := grpc.NewClient(addresses[leaderID], grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(8<<20)))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// The request fits the server's 4 MiB + 64 KiB receive limit and passes the
	// key and value checks, but its encoded command exceeds the Raft entry limit.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = lsmdbv1.NewKVClient(connection).Put(ctx, &lsmdbv1.PutRequest{
		Key: bytes.Repeat([]byte{'k'}, kvstate.MaxKeyBytes), Value: bytes.Repeat([]byte{'v'}, kvstate.MaxValueBytes),
		ClientId: string(bytes.Repeat([]byte{'c'}, 20<<10)), RequestSeq: 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized Put error = %v, want InvalidArgument", err)
	}

	after := nodeStatuses(t, nodes)[leaderID]
	if after.Role != raft.Leader || after.Term != before.Term {
		t.Fatalf("leader %d after the oversized Put: role %s term %d, want leader at term %d", leaderID, after.Role, after.Term, before.Term)
	}
	client, err := NewClient(addressList(addresses))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Put(ctx, []byte("after"), []byte("ok")); err != nil {
		t.Fatalf("Put after the oversized one: %v", err)
	}
	got, err := client.Get(ctx, []byte("after"))
	if err != nil || !got.Found || string(got.Value) != "ok" {
		t.Fatalf("Get after the oversized Put = %+v, %v", got, err)
	}
}
