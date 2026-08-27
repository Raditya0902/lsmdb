package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"lsmdb/internal/raft"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForLeader(t *testing.T, nodes map[uint64]*Node, excluded uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, node := range nodes {
			if id == excluded || node == nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			status, err := node.Status(ctx)
			cancel()
			if err == nil && status.Role == raft.Leader {
				return id
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("cluster did not elect a leader")
	return 0
}

func TestThreeNodeWriteReadFailoverAndRecovery(t *testing.T) {
	addresses := map[uint64]string{1: freeAddress(t), 2: freeAddress(t), 3: freeAddress(t)}
	configs := make(map[uint64]NodeConfig)
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		configs[id] = NodeConfig{
			ID: id, ListenAddress: addresses[id], DataDir: fmt.Sprintf("%s/node-%d", t.TempDir(), id),
			Peers: addresses, TickInterval: 20 * time.Millisecond,
			ElectionTickMin: 5, ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5,
		}
		node, err := StartNode(configs[id])
		if err != nil {
			t.Fatalf("StartNode(%d): %v", id, err)
		}
		nodes[id] = node
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
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
	if _, err := client.Put(ctx, []byte("alpha"), []byte("one")); err != nil {
		t.Fatalf("Put before failover: %v", err)
	}
	got, err := client.Get(ctx, []byte("alpha"))
	if err != nil || !got.Found || string(got.Value) != "one" {
		t.Fatalf("Get before failover = (%+v, %v)", got, err)
	}

	if err := nodes[leaderID].Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}
	nodes[leaderID] = nil
	newLeaderID := waitForLeader(t, nodes, leaderID)
	if newLeaderID == leaderID {
		t.Fatal("failed node remained leader")
	}
	write, err := client.Put(ctx, []byte("alpha"), []byte("two"))
	if err != nil {
		t.Fatalf("Put after failover: %v", err)
	}
	got, err = client.Get(ctx, []byte("alpha"))
	if err != nil || !got.Found || string(got.Value) != "two" {
		t.Fatalf("Get after failover = (%+v, %v)", got, err)
	}

	restarted, err := StartNode(configs[leaderID])
	if err != nil {
		t.Fatalf("restart old leader: %v", err)
	}
	nodes[leaderID] = restarted
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if restarted.machine.AppliedIndex() >= write.LogIndex {
			value, found, readErr := restarted.machine.Get([]byte("alpha"))
			if readErr == nil && found && string(value) == "two" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("restarted node did not converge to index %d", write.LogIndex)
}

func TestOfflineFollowerRecoversThroughInstalledSnapshot(t *testing.T) {
	addresses := map[uint64]string{1: freeAddress(t), 2: freeAddress(t), 3: freeAddress(t)}
	configs := make(map[uint64]NodeConfig)
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		configs[id] = NodeConfig{ID: id, ListenAddress: addresses[id], DataDir: fmt.Sprintf("%s/node-%d", t.TempDir(), id), Peers: addresses, TickInterval: 20 * time.Millisecond, ElectionTickMin: 5, ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5, SnapshotThreshold: 3}
		node, err := StartNode(configs[id])
		if err != nil {
			t.Fatalf("StartNode(%d): %v", id, err)
		}
		nodes[id] = node
	}
	defer func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	}()
	leaderID := waitForLeader(t, nodes, 0)
	offlineID := uint64(1)
	if offlineID == leaderID {
		offlineID = 2
	}
	if err := nodes[offlineID].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[offlineID] = nil
	client, err := NewClient([]string{addresses[leaderID]})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var lastIndex uint64
	for i := 1; i <= 8; i++ {
		result, err := client.Put(ctx, []byte("snapshot-key"), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		lastIndex = result.LogIndex
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		status, err := nodes[leaderID].Status(ctx)
		if err == nil && status.SnapshotIndex >= lastIndex {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	leaderStatus, err := nodes[leaderID].Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if leaderStatus.SnapshotIndex < lastIndex {
		t.Fatalf("leader did not compact through %d: %+v", lastIndex, leaderStatus)
	}
	restarted, err := StartNode(configs[offlineID])
	if err != nil {
		t.Fatalf("restart follower: %v", err)
	}
	nodes[offlineID] = restarted
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, statusErr := restarted.Status(ctx)
		value, found, readErr := restarted.machine.Get([]byte("snapshot-key"))
		if statusErr == nil && readErr == nil && found && string(value) == "value-8" && status.SnapshotIndex >= lastIndex && restarted.machine.AppliedIndex() >= lastIndex {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	status, _ := restarted.Status(context.Background())
	t.Fatalf("follower did not install snapshot through %d: status=%+v applied=%d", lastIndex, status, restarted.machine.AppliedIndex())
}
