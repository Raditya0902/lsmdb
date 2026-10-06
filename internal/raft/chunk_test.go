package raft

import (
	"bytes"
	"testing"
)

// testEntryOverhead is D019's accounted framing per entry; raftgrpc tests check
// it against the encoded protobuf size.
const testEntryOverhead = 32

func accountedBytes(entries []Entry) int {
	total := 0
	for _, entry := range entries {
		total += len(entry.Data) + testEntryOverhead
	}
	return total
}

func newCappedCluster(t *testing.T, maxAppendBytes uint64, heartbeatTicks int) map[uint64]*Node {
	t.Helper()
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		cfg := testConfig(id)
		cfg.maxAppendBytes = maxAppendBytes
		cfg.HeartbeatTicks = heartbeatTicks
		node, err := New(cfg, HardState{}, nil)
		if err != nil {
			t.Fatalf("New(%d): %v", id, err)
		}
		nodes[id] = node
	}
	return nodes
}

// proposeWhileCutOff proposes one entry per size while node 2 hears nothing;
// node 3 acknowledges, so the leader keeps quorum and commits.
func proposeWhileCutOff(t *testing.T, nodes map[uint64]*Node, leader *Node, sizes ...int) {
	t.Helper()
	for _, size := range sizes {
		_, update, err := leader.Propose(bytes.Repeat([]byte{'x'}, size))
		if err != nil {
			t.Fatal(err)
		}
		route(t, nodes, update.Messages, isolated(2))
	}
}

func repeatSize(size, count int) []int {
	sizes := make([]int, count)
	for i := range sizes {
		sizes[i] = size
	}
	return sizes
}

func TestAppendMessagesStayWithinByteCap(t *testing.T) {
	const capBytes = 1024
	nodes := newCappedCluster(t, capBytes, 1)
	leader := electNodeOne(t, nodes)
	checked := 0
	check := func(message Message) {
		if message.Type != MsgAppend {
			return
		}
		checked++
		if len(message.Entries) > 1 && accountedBytes(message.Entries) > capBytes {
			t.Errorf("%s append to %d carries %d entries, %d accounted bytes; cap %d",
				message.Origin, message.To, len(message.Entries), accountedBytes(message.Entries), capBytes)
		}
	}
	cutOff := func(message Message) bool { check(message); return isolated(2)(message) }
	for i := 0; i < 20; i++ {
		_, update, err := leader.Propose(bytes.Repeat([]byte{'x'}, 200))
		if err != nil {
			t.Fatal(err)
		}
		route(t, nodes, update.Messages, cutOff)
	}
	probe, err := leader.ReadProbe(7)
	if err != nil {
		t.Fatal(err)
	}
	route(t, nodes, probe.Messages, cutOff)
	// Reconnected: heartbeats, follow-ups and acks all flow.
	for tick := 0; tick < 40 && leader.Status().MatchIndex[2] < leader.Status().LastLogIndex; tick++ {
		route(t, nodes, leader.Tick().Messages, func(message Message) bool { check(message); return false })
	}
	if match, last := leader.Status().MatchIndex[2], leader.Status().LastLogIndex; match != last {
		t.Fatalf("follower 2 matchIndex %d, want %d", match, last)
	}
	t.Logf("checked %d appends", checked)
}

func TestEntryLargerThanCapShipsAlone(t *testing.T) {
	nodes := newCappedCluster(t, 1024, 1)
	leader := electNodeOne(t, nodes)
	proposeWhileCutOff(t, nodes, leader, 4096, 100)
	heartbeat := appendsTo(leader.Tick(), 2)
	if len(heartbeat) != 1 || len(heartbeat[0].Entries) != 1 || len(heartbeat[0].Entries[0].Data) != 4096 {
		t.Fatalf("heartbeat to 2 = %d messages, entries %v; want the 4096-byte entry alone", len(heartbeat), entrySizes(heartbeat))
	}
	response := nodes[2].Step(heartbeat[0]).Messages
	if len(response) != 1 || response[0].Reject {
		t.Fatalf("follower response = %+v", response)
	}
	followUp := appendsTo(leader.Step(response[0]), 2)
	if len(followUp) != 1 || len(followUp[0].Entries) != 1 || len(followUp[0].Entries[0].Data) != 100 {
		t.Fatalf("follow-up to 2 = entries %v, want the 100-byte entry alone", entrySizes(followUp))
	}
}

func entrySizes(messages []Message) [][]int {
	var out [][]int
	for _, message := range messages {
		var sizes []int
		for _, entry := range message.Entries {
			sizes = append(sizes, len(entry.Data))
		}
		out = append(out, sizes)
	}
	return out
}

func TestCappedAppendDoesNotCommitStaleSuffix(t *testing.T) {
	// The follower holds 2..6 from term 1, uncommitted. The term-2 leader shares
	// only 1..3; a capped message proves index 2 and carries LeaderCommit 6.
	var entries []Entry
	for index := uint64(1); index <= 6; index++ {
		entries = append(entries, Entry{Index: index, Term: 1, Data: []byte{byte(index)}})
	}
	node, err := New(testConfig(2), HardState{Term: 2}, entries)
	if err != nil {
		t.Fatal(err)
	}
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{entries[1]}, LeaderCommit: 6,
	})
	if got := node.Status().CommitIndex; got != 2 {
		t.Fatalf("commit = %d, want 2: only index 2 was proved", got)
	}
	for _, entry := range update.Committed {
		if entry.Index > 2 {
			t.Fatalf("committed unproved stale entry %d", entry.Index)
		}
	}
	if len(update.Messages) != 1 || update.Messages[0].LogIndex != 2 {
		t.Fatalf("response = %+v, want an ack for index 2", update.Messages)
	}
}

func TestFarBehindFollowerCatchesUpThroughCappedChunks(t *testing.T) {
	// 20 entries of 200 bytes: 4 fit a 1024-byte cap (4 x 232), so C = 5 chunks.
	// Follower 2 acked the no-op, so nextIndex = matchIndex + 1 and R = 0.
	const capBytes, heartbeatTicks, entries, chunks = 1024, 2, 20, 5
	nodes := newCappedCluster(t, capBytes, heartbeatTicks)
	leader := electNodeOne(t, nodes)
	if match := leader.Status().MatchIndex[2]; match != 1 {
		t.Fatalf("matchIndex[2] = %d before the cut, want 1", match)
	}
	proposeWhileCutOff(t, nodes, leader, repeatSize(200, entries)...)
	// Only heartbeats deliver: every follow-up is lost, and so are the acks
	// to heartbeat rounds 1, 3 and 4.
	lostAcks := map[int]bool{1: true, 3: true, 4: true}
	k := len(lostAcks)
	round, caughtUpAt := 0, 0
	for tick := 1; tick <= 4*(chunks+k)*heartbeatTicks && caughtUpAt == 0; tick++ {
		messages := leader.Tick().Messages
		if len(appendsTo(Update{Messages: messages}, 2)) > 0 {
			round++
		}
		current := round
		route(t, nodes, messages, func(message Message) bool {
			if message.Type == MsgAppend && message.To == 2 {
				if len(message.Entries) > 1 && accountedBytes(message.Entries) > capBytes {
					t.Errorf("append to 2 carries %d accounted bytes, cap %d", accountedBytes(message.Entries), capBytes)
				}
				return message.Origin == OriginAckResend
			}
			return message.Type == MsgAppendResponse && message.From == 2 && lostAcks[current]
		})
		if leader.Status().Role != Leader {
			t.Fatalf("leader stepped down at tick %d", tick)
		}
		if leader.Status().MatchIndex[2] == leader.Status().LastLogIndex {
			caughtUpAt = tick
		}
	}
	bound := (0 + chunks + k) * heartbeatTicks
	if caughtUpAt == 0 || caughtUpAt > bound {
		t.Fatalf("caught up at tick %d (0 = never), want <= (R + C + k) x HeartbeatTicks = %d", caughtUpAt, bound)
	}
	t.Logf("caught up at tick %d after %d heartbeat rounds; bound %d", caughtUpAt, round, bound)
}

func TestStaleRejectionDoesNotStrandCappedFollower(t *testing.T) {
	nodes := newCappedCluster(t, 1024, 1)
	leader := electNodeOne(t, nodes)
	proposeWhileCutOff(t, nodes, leader, repeatSize(200, 20)...)
	// Two heartbeat rounds with follow-ups lost deliver two 4-entry chunks.
	dropFollowUps := func(message Message) bool { return message.Origin == OriginAckResend && message.To == 2 }
	for i := 0; i < 2; i++ {
		route(t, nodes, leader.Tick().Messages, dropFollowUps)
	}
	if match := leader.Status().MatchIndex[2]; match != 9 {
		t.Fatalf("matchIndex[2] = %d after two chunks, want 9", match)
	}
	// A delayed rejection from before those acks names index 2.
	stale := Message{Type: MsgAppendResponse, From: 2, To: 1, Term: leader.Status().Term, Reject: true, RejectHint: 2}
	strandCheck := func(message Message) bool {
		if message.Type == MsgAppend && message.To == 2 {
			if match := leader.Status().MatchIndex[2]; message.LogIndex < match {
				t.Errorf("append to 2 starts after index %d, below matchIndex %d", message.LogIndex, match)
			}
		}
		return false
	}
	route(t, nodes, leader.Step(stale).Messages, strandCheck)
	for tick := 0; tick < 10 && leader.Status().MatchIndex[2] < leader.Status().LastLogIndex; tick++ {
		route(t, nodes, leader.Tick().Messages, strandCheck)
	}
	if match, last := leader.Status().MatchIndex[2], leader.Status().LastLogIndex; match != last {
		t.Fatalf("follower 2 stranded at matchIndex %d, want %d", match, last)
	}
}

func TestDefaultAppendCapIsOneMiB(t *testing.T) {
	// Production configs cannot set the cap, so this is the cap they run with.
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	const accounted = 128 << 10 // 8 entries fill 1 MiB exactly
	proposeWhileCutOff(t, nodes, leader, repeatSize(accounted-testEntryOverhead, 10)...)
	heartbeat := appendsTo(leader.Tick(), 2)
	if len(heartbeat) != 1 || len(heartbeat[0].Entries) != 8 {
		t.Fatalf("heartbeat to 2 carries %v entries, want 8 (1 MiB accounted)", entrySizes(heartbeat))
	}
}

func TestSmallPendingEntriesShipInOneAppend(t *testing.T) {
	// The cap must bound large messages without splitting small batches: 20
	// pending 128-byte entries (3,200 accounted bytes) fit one append.
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	proposeWhileCutOff(t, nodes, leader, repeatSize(128, 20)...)
	heartbeat := appendsTo(leader.Tick(), 2)
	if len(heartbeat) != 1 || len(heartbeat[0].Entries) != 20 {
		t.Fatalf("heartbeat to 2 = %d messages carrying %v entries, want one append with all 20", len(heartbeat), entrySizes(heartbeat))
	}
}
