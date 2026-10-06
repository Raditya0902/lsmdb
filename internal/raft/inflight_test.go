package raft

import "testing"

// testResendTicks is D022's resend timeout: an entry-carrying append that no
// response has settled is sent again after this many leader ticks.
const testResendTicks = 5

// testSendDeadlineTicks is the runtime's 500 ms send deadline in 20 ms ticks:
// a copy counts as in flight for this long.
const testSendDeadlineTicks = 25

func entryAppendsTo(update Update, peer uint64) []Message {
	var out []Message
	for _, message := range appendsTo(update, peer) {
		if len(message.Entries) > 0 {
			out = append(out, message)
		}
	}
	return out
}

// resendTo ticks the leader until it re-sends entries to peer, which must
// happen within testResendTicks ticks of the lost append (D022).
func resendTo(t *testing.T, leader *Node, peer uint64) []Message {
	t.Helper()
	for tick := 1; tick <= testResendTicks; tick++ {
		if sent := entryAppendsTo(leader.Tick(), peer); len(sent) > 0 {
			return sent
		}
	}
	t.Fatalf("no entries re-sent to %d within %d ticks", peer, testResendTicks)
	return nil
}

func TestHeartbeatCarriesNoEntriesWhileAppendOutstanding(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	if _, _, err := leader.Propose([]byte("x")); err != nil { // the append to 2 is in flight
		t.Fatal(err)
	}
	heartbeat := appendsTo(leader.Tick(), 2)
	if len(heartbeat) != 1 {
		t.Fatalf("tick sent %d appends to 2, want one heartbeat", len(heartbeat))
	}
	if got := heartbeat[0]; len(got.Entries) != 0 || got.LogIndex != 1 || got.LeaderCommit != 1 {
		t.Fatalf("heartbeat to 2 = prev %d, %d entries, commit %d; want an entry-less heartbeat at prev 1 with commit 1",
			got.LogIndex, len(got.Entries), got.LeaderCommit)
	}
}

func TestProposalWhileOutstandingWaitsForTheAck(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	_, first, err := leader.Propose([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"b", "c"} {
		_, update, err := leader.Propose([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		if sent := entryAppendsTo(update, 2); len(sent) != 0 {
			t.Fatalf("proposal %q sent entries %v to 2 while index 2 was in flight", data, entrySizes(sent))
		}
	}
	// Follower 2 receives index 2 and acknowledges it; that ack commits index 2
	// and must ship 3 and 4 together.
	toTwo := appendsTo(first, 2)
	if len(toTwo) != 1 {
		t.Fatalf("first proposal sent %d appends to 2, want 1", len(toTwo))
	}
	response := nodes[2].Step(toTwo[0]).Messages
	if len(response) != 1 || response[0].Reject || response[0].LogIndex != 2 {
		t.Fatalf("follower 2 response = %+v, want an ack for index 2", response)
	}
	next := entryAppendsTo(leader.Step(response[0]), 2)
	if len(next) != 1 || next[0].LogIndex != 2 || len(next[0].Entries) != 2 {
		t.Fatalf("after the ack, entry-carrying appends to 2 = %d, first %+v; want one carrying 3 and 4", len(next), next)
	}
}

func TestUnackedAppendIsResentAfterResendTicks(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	if _, _, err := leader.Propose([]byte("x")); err != nil { // lost: never delivered
		t.Fatal(err)
	}
	for tick := 1; tick <= testResendTicks; tick++ {
		sent := entryAppendsTo(leader.Tick(), 2)
		if tick < testResendTicks && len(sent) != 0 {
			t.Fatalf("tick %d re-sent entries %v to 2 before the resend timeout of %d ticks", tick, entrySizes(sent), testResendTicks)
		}
		if tick == testResendTicks && (len(sent) != 1 || sent[0].LogIndex != 1 || len(sent[0].Entries) != 1) {
			t.Fatalf("tick %d sent entries %v to 2, want index 2 re-sent once", tick, entrySizes(sent))
		}
	}
}

func TestSilentFollowerGetsAtMostFiveCopiesPerSendDeadline(t *testing.T) {
	// Node 3 never answers; node 2 keeps the leader's quorum. A new entry is
	// proposed between every two ticks, so there is always something to send
	// to node 3. Sends are counted per tick: a send between ticks t-1 and t
	// belongs to tick t-1, the tick whose interval it falls in.
	const ticks = 120
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	var sentAt []int
	record := func(tick int, update Update) {
		for range entryAppendsTo(update, 3) {
			sentAt = append(sentAt, tick)
		}
	}
	for tick := 1; tick <= ticks; tick++ {
		_, update, err := leader.Propose([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		record(tick-1, update)
		route(t, nodes, update.Messages, isolated(3))
		update = leader.Tick()
		record(tick, update)
		route(t, nodes, update.Messages, isolated(3))
		if leader.Status().Role != Leader {
			t.Fatalf("leader stepped down at tick %d", tick)
		}
	}
	worst := 0
	for start := 0; start <= ticks; start++ {
		inWindow := 0
		for _, at := range sentAt {
			if at >= start && at < start+testSendDeadlineTicks {
				inWindow++
			}
		}
		worst = max(worst, inWindow)
	}
	if worst > 5 {
		t.Fatalf("up to %d entry-carrying appends to the silent follower within %d ticks, want at most 5; at %v", worst, testSendDeadlineTicks, sentAt)
	}
	t.Logf("%d entry-carrying appends to node 3 over %d ticks; at most %d in any %d-tick window; at %v", len(sentAt), ticks, worst, testSendDeadlineTicks, sentAt)
}

func TestCommitReachesCaughtUpFollowerThroughEntryLessAppend(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	_, update, err := leader.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	// Both followers store index 2 and acknowledge it, but every commit
	// advance to node 3 is lost.
	route(t, nodes, update.Messages, func(message Message) bool {
		return message.To == 3 && message.Origin == OriginCommitAdvance
	})
	if commit, held := nodes[3].Status().CommitIndex, nodes[3].Status().LastLogIndex; commit != 1 || held != 2 {
		t.Fatalf("node 3 commit %d last %d after the lost commit advance, want 1 and 2", commit, held)
	}
	heartbeat := appendsTo(leader.Tick(), 3)
	if len(heartbeat) != 1 || len(heartbeat[0].Entries) != 0 {
		t.Fatalf("heartbeat to the caught-up node 3 = %+v, want one entry-less append", heartbeat)
	}
	nodes[3].Step(heartbeat[0])
	if commit := nodes[3].Status().CommitIndex; commit != 2 {
		t.Fatalf("node 3 commit after the heartbeat = %d, want 2", commit)
	}
}

func TestSlowChunkDoesNotStepLeaderDown(t *testing.T) {
	// Every entry-carrying append is held in transit for the whole test; small
	// messages and responses flow. Heartbeats must keep the leader's quorum.
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	held := func(message Message) bool { return message.Type == MsgAppend && len(message.Entries) > 0 }
	_, update, err := leader.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	route(t, nodes, update.Messages, held)
	windows := 4 * testConfig(1).CheckQuorumTicks
	for tick := 1; tick <= windows; tick++ {
		route(t, nodes, leader.Tick().Messages, held)
		if leader.Status().Role != Leader {
			t.Fatalf("leader stepped down at tick %d while its only entry was still in transit", tick)
		}
	}
}
