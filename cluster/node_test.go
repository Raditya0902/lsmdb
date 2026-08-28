package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
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
	wantValue := bytes.Repeat([]byte("z"), (1<<20)+123)
	for i := 1; i <= 8; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		if i == 8 {
			value = wantValue
		}
		result, err := client.Put(ctx, []byte("snapshot-key"), value)
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
		if statusErr == nil && readErr == nil && found && bytes.Equal(value, wantValue) && status.SnapshotIndex >= lastIndex && restarted.machine.AppliedIndex() >= lastIndex {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	status, _ := restarted.Status(context.Background())
	t.Fatalf("follower did not install snapshot through %d: status=%+v applied=%d", lastIndex, status, restarted.machine.AppliedIndex())
}

func TestJointConsensusAddsNodeRestartsItAndRemovesLeader(t *testing.T) {
	addresses := map[uint64]string{1: freeAddress(t), 2: freeAddress(t), 3: freeAddress(t), 4: freeAddress(t)}
	initialVoters := []uint64{1, 2, 3}
	configs := make(map[uint64]NodeConfig)
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 4; id++ {
		configs[id] = NodeConfig{ID: id, ListenAddress: addresses[id], DataDir: fmt.Sprintf("%s/node-%d", t.TempDir(), id), Peers: addresses, Voters: initialVoters, TickInterval: 20 * time.Millisecond, ElectionTickMin: 5, ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5, SnapshotThreshold: 100}
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
	client, err := NewClient([]string{addresses[1], addresses[2], addresses[3], addresses[4]})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	change, err := client.ChangeMembership(ctx, []uint64{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("add node 4: %v", err)
	}
	if change.LogIndex == 0 {
		t.Fatal("membership response has zero index")
	}
	waitMembership := func(id uint64, voters []uint64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			statusCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
			status, statusErr := nodes[id].Status(statusCtx)
			stop()
			if statusErr == nil && equalUint64s(status.Membership.Voters, voters) && len(status.Membership.JointVoters) == 0 && status.CommitIndex >= change.LogIndex {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		status, _ := nodes[id].Status(context.Background())
		t.Fatalf("node %d membership = %+v", id, status)
	}
	for id := uint64(1); id <= 4; id++ {
		waitMembership(id, []uint64{1, 2, 3, 4})
	}

	if err := nodes[4].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[4] = nil
	restarted, err := StartNode(configs[4])
	if err != nil {
		t.Fatalf("restart node 4: %v", err)
	}
	nodes[4] = restarted
	waitMembership(4, []uint64{1, 2, 3, 4})

	remaining := make([]uint64, 0, 3)
	for _, id := range []uint64{1, 2, 3, 4} {
		if id != leaderID {
			remaining = append(remaining, id)
		}
	}
	change, err = client.ChangeMembership(ctx, remaining)
	if err != nil {
		t.Fatalf("remove leader %d: %v", leaderID, err)
	}
	newLeader := waitForLeader(t, nodes, leaderID)
	if newLeader == leaderID {
		t.Fatal("removed leader remained leader")
	}
	if _, err := client.Put(ctx, []byte("after-membership"), []byte("ok")); err != nil {
		t.Fatalf("write after leader removal: %v", err)
	}
}

func TestRuntimePeerDirectoryAddsUnconfiguredVoter(t *testing.T) {
	addresses := map[uint64]string{1: freeAddress(t), 2: freeAddress(t), 3: freeAddress(t), 4: freeAddress(t)}
	peerFile := filepath.Join(t.TempDir(), "peers.json")
	writeDirectory := func(entries map[uint64]string) {
		t.Helper()
		data, err := json.Marshal(entries)
		if err != nil {
			t.Fatal(err)
		}
		tmp := peerFile + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, peerFile); err != nil {
			t.Fatal(err)
		}
	}
	initialAddresses := map[uint64]string{1: addresses[1], 2: addresses[2], 3: addresses[3]}
	writeDirectory(initialAddresses)
	resolver, err := NewFilePeerResolver(peerFile, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	initialVoters := []uint64{1, 2, 3}
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		config := NodeConfig{
			ID: id, ListenAddress: addresses[id], DataDir: fmt.Sprintf("%s/node-%d", t.TempDir(), id),
			Peers: initialAddresses, PeerResolver: resolver, Voters: initialVoters,
			TickInterval: 20 * time.Millisecond, ElectionTickMin: 5, ElectionTickMax: 10,
			HeartbeatTicks: 1, CheckQuorumTicks: 5, SnapshotThreshold: 100,
		}
		node, err := StartNode(config)
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
	waitForLeader(t, nodes, 0)

	// Node 4 was absent from every running node's static Peers map. Publishing
	// the directory update makes it routable without changing voter membership.
	writeDirectory(addresses)
	node4, err := StartNode(NodeConfig{
		ID: 4, ListenAddress: addresses[4], DataDir: filepath.Join(t.TempDir(), "node-4"),
		Peers: map[uint64]string{4: addresses[4]}, PeerResolver: resolver, Voters: initialVoters,
		TickInterval: 20 * time.Millisecond, ElectionTickMin: 5, ElectionTickMax: 10,
		HeartbeatTicks: 1, CheckQuorumTicks: 5, SnapshotThreshold: 100,
	})
	if err != nil {
		t.Fatalf("StartNode(4): %v", err)
	}
	nodes[4] = node4

	client, err := NewClient([]string{addresses[1], addresses[2], addresses[3]})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	change, err := client.ChangeMembership(ctx, []uint64{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("add discovered node 4: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		converged := true
		for id := uint64(1); id <= 4; id++ {
			statusCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
			status, statusErr := nodes[id].Status(statusCtx)
			stop()
			if statusErr != nil || !equalUint64s(status.Membership.Voters, []uint64{1, 2, 3, 4}) || len(status.Membership.JointVoters) != 0 || status.CommitIndex < change.LogIndex {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("discovered node did not join the final voter configuration")
}

func equalUint64s(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
