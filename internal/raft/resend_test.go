package raft

import "testing"

// leaderWithPendingEntries elects node 1, commits its no-op on all three nodes,
// then proposes two more entries (indexes 2 and 3) without delivering them.
func leaderWithPendingEntries(t *testing.T) (*Node, map[uint64]*Node) {
	t.Helper()
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	for _, data := range []string{"a", "b"} {
		if _, _, err := leader.Propose([]byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	if last := leader.Status().LastLogIndex; last != 3 {
		t.Fatalf("leader last index = %d, want 3", last)
	}
	return leader, nodes
}

func ack(leader *Node, from, index uint64) Message {
	return Message{Type: MsgAppendResponse, From: from, To: 1, Term: leader.Status().Term, LogIndex: index}
}

func appendsTo(update Update, peer uint64) []Message {
	var out []Message
	for _, message := range update.Messages {
		if message.Type == MsgAppend && message.To == peer {
			out = append(out, message)
		}
	}
	return out
}

// route delivers messages breadth-first, as deliverAll does, except that any
// message for which lose returns true is discarded.
func route(t *testing.T, nodes map[uint64]*Node, initial []Message, lose func(Message) bool) {
	t.Helper()
	queue := append([]Message(nil), initial...)
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 1000 {
			t.Fatal("message delivery did not quiesce")
		}
		message := queue[0]
		queue = queue[1:]
		if lose(message) {
			continue
		}
		queue = append(queue, nodes[message.To].Step(message).Messages...)
	}
}

func isolated(id uint64) func(Message) bool {
	return func(message Message) bool { return message.To == id || message.From == id }
}

func TestDuplicateOrStaleAckSendsNoAppend(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index uint64
	}{
		{"duplicate ack", 2},
		{"stale ack", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, _ := leaderWithPendingEntries(t)
			leader.Step(ack(leader, 3, 3)) // commits 3; no later ack changes commit
			leader.Step(ack(leader, 2, 2)) // advances follower 2 to 2; entry 3 still pending
			update := leader.Step(ack(leader, 2, tc.index))
			if len(update.Messages) != 0 {
				t.Fatalf("%s produced %d messages, want none: %+v", tc.name, len(update.Messages), update.Messages)
			}
			if match := leader.Status().MatchIndex[2]; match != 2 {
				t.Fatalf("matchIndex[2] = %d, want 2 unchanged", match)
			}
		})
	}
}

func TestAdvancingAckWithPendingEntriesSendsNextAppend(t *testing.T) {
	leader, _ := leaderWithPendingEntries(t)
	leader.Step(ack(leader, 3, 3)) // commit reaches 3 now, so the next ack cannot broadcast
	update := leader.Step(ack(leader, 2, 2))
	appends := appendsTo(update, 2)
	if len(update.Messages) != 1 || len(appends) != 1 {
		t.Fatalf("advancing ack produced %+v, want exactly one append to 2", update.Messages)
	}
	next := appends[0]
	if next.Origin != OriginAckResend || next.LogIndex != 2 || len(next.Entries) != 1 || next.Entries[0].Index != 3 {
		t.Fatalf("follow-up = origin %s prev %d entries %+v, want ack_resend prev 2 entries [3]", next.Origin, next.LogIndex, next.Entries)
	}
}

// freshLeaderAheadOfFollowers elects node 1 with a three-entry log over
// followers holding only index 1, and loses the no-op append. The leader's
// nextIndex is then optimistic (4) while no follower has acked (matchIndex 0).
func freshLeaderAheadOfFollowers(t *testing.T) *Node {
	t.Helper()
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		entries := []Entry{{Index: 1, Term: 1}}
		if id == 1 {
			entries = append(entries, Entry{Index: 2, Term: 1}, Entry{Index: 3, Term: 1})
		}
		node, err := New(testConfig(id), HardState{Term: 1}, entries)
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = node
	}
	leader := nodes[1]
	for tick := 0; tick < 20 && leader.Status().Role != Leader; tick++ {
		route(t, nodes, leader.Tick().Messages, func(message Message) bool { return message.Type == MsgAppend })
	}
	if leader.Status().Role != Leader {
		t.Fatal("node 1 did not become leader")
	}
	return leader
}

func TestRejectionStillProbesWithLowerNextIndex(t *testing.T) {
	for _, tc := range []struct {
		name      string
		hint      uint64
		wantPrev  uint64
		wantFirst uint64
	}{
		{"hint below nextIndex", 2, 1, 2},
		{"no hint decrements nextIndex", 0, 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := freshLeaderAheadOfFollowers(t) // nextIndex[3] = 4, matchIndex[3] = 0
			reject := Message{Type: MsgAppendResponse, From: 3, To: 1, Term: leader.Status().Term, Reject: true, RejectHint: tc.hint}
			appends := appendsTo(leader.Step(reject), 3)
			if len(appends) != 1 {
				t.Fatalf("rejection produced %d appends to 3, want one probe", len(appends))
			}
			probe := appends[0]
			if probe.LogIndex != tc.wantPrev || len(probe.Entries) == 0 || probe.Entries[0].Index != tc.wantFirst {
				t.Fatalf("probe prev %d entries %+v, want prev %d starting at %d", probe.LogIndex, probe.Entries, tc.wantPrev, tc.wantFirst)
			}
		})
	}
	t.Run("stale rejection below matchIndex", func(t *testing.T) {
		// After an ack for index 3, a rejection naming index 2 is stale: nextIndex
		// stays at matchIndex + 1 (D019).
		leader, _ := leaderWithPendingEntries(t)
		leader.Step(ack(leader, 3, 3))
		reject := Message{Type: MsgAppendResponse, From: 3, To: 1, Term: leader.Status().Term, Reject: true, RejectHint: 2}
		appends := appendsTo(leader.Step(reject), 3)
		if len(appends) != 1 || appends[0].LogIndex != 3 {
			t.Fatalf("probe after a stale rejection = %+v, want prev 3", appends)
		}
	})
}

// TestFollowerCatchesUpAfterLossWithinResendBound pins D022's catch-up path:
// with entry-less heartbeats, a lost append, ack or resend is recovered by the
// resend after testResendTicks ticks, so k losses cost at most
// (k + 1) x testResendTicks ticks (D018's bound with D022's timeout).
func TestFollowerCatchesUpAfterLossWithinResendBound(t *testing.T) {
	t.Run("append never delivered", func(t *testing.T) {
		nodes := newTestCluster(t)
		leader := electNodeOne(t, nodes)
		_, update, err := leader.Propose([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		route(t, nodes, update.Messages, func(Message) bool { return true }) // the append to 2 is lost
		if leader.Status().CommitIndex != 1 || nodes[2].Status().LastLogIndex != 1 {
			t.Fatalf("after the loss: commit %d, follower last %d; want 1 and 1", leader.Status().CommitIndex, nodes[2].Status().LastLogIndex)
		}
		for tick := 1; tick <= testResendTicks; tick++ {
			route(t, nodes, leader.Tick().Messages, isolated(3))
			if commit := leader.Status().CommitIndex; tick < testResendTicks && commit != 1 || tick == testResendTicks && commit != 2 {
				t.Fatalf("commit after %d ticks = %d; want 2 only at the resend, tick %d", tick, commit, testResendTicks)
			}
		}
	})

	t.Run("ack lost", func(t *testing.T) {
		nodes := newTestCluster(t)
		leader := electNodeOne(t, nodes)
		_, update, err := leader.Propose([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		route(t, nodes, update.Messages, func(message Message) bool {
			return isolated(3)(message) || message.Type == MsgAppendResponse
		})
		if nodes[2].Status().LastLogIndex != 2 {
			t.Fatalf("follower last = %d, want 2: the append should have arrived", nodes[2].Status().LastLogIndex)
		}
		if match, commit := leader.Status().MatchIndex[2], leader.Status().CommitIndex; match != 1 || commit != 1 {
			t.Fatalf("after the lost ack: matchIndex[2] %d commit %d, want 1 and 1", match, commit)
		}
		for tick := 1; tick <= testResendTicks; tick++ {
			route(t, nodes, leader.Tick().Messages, isolated(3))
		}
		if match, commit := leader.Status().MatchIndex[2], leader.Status().CommitIndex; match != 2 || commit != 2 {
			t.Fatalf("after %d ticks: matchIndex[2] %d commit %d, want 2 and 2", testResendTicks, match, commit)
		}
	})

	t.Run("append and next k resends lost", func(t *testing.T) {
		// Node 3 stays connected so the leader keeps its quorum while follower 2
		// hears nothing; catch-up is measured on follower 2's matchIndex.
		// HeartbeatTicks 2 shows the bound does not depend on it.
		const heartbeatTicks, k = 2, 3
		nodes := make(map[uint64]*Node)
		for id := uint64(1); id <= 3; id++ {
			cfg := testConfig(id)
			cfg.HeartbeatTicks = heartbeatTicks
			node, err := New(cfg, HardState{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			nodes[id] = node
		}
		leader := electNodeOne(t, nodes)
		_, update, err := leader.Propose([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		toFollower2 := func(message Message) bool { return message.To == 2 }
		route(t, nodes, update.Messages, toFollower2) // the append, and the commit advance it triggers, are lost
		if match := leader.Status().MatchIndex[2]; match != 1 {
			t.Fatalf("matchIndex[2] = %d after the lost append, want 1", match)
		}
		resends, caughtUpAt := 0, 0
		for tick := 1; tick <= 4*(k+1)*testResendTicks && caughtUpAt == 0; tick++ {
			messages := leader.Tick().Messages
			if len(entryAppendsTo(Update{Messages: messages}, 2)) > 0 {
				resends++
			}
			lose := func(Message) bool { return false }
			if resends <= k {
				lose = toFollower2
			}
			route(t, nodes, messages, lose)
			if leader.Status().Role != Leader {
				t.Fatalf("leader stepped down at tick %d", tick)
			}
			if leader.Status().MatchIndex[2] == 2 {
				caughtUpAt = tick
			}
		}
		if caughtUpAt == 0 {
			t.Fatalf("follower never caught up after %d lost resends", k)
		}
		if bound := (k + 1) * testResendTicks; caughtUpAt > bound {
			t.Fatalf("caught up at tick %d, beyond the bound (k+1) x %d = %d", caughtUpAt, testResendTicks, bound)
		}
		if resends != k+1 {
			t.Fatalf("caught up on resend %d, want %d: the first %d were lost", resends, k+1, k)
		}
		t.Logf("caught up at tick %d; bound (k+1) x %d = %d", caughtUpAt, testResendTicks, (k+1)*testResendTicks)
	})
}

func TestIdleLeaderKeepsQuorumOnNonAdvancingAcks(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	match := leader.Status().MatchIndex
	windows := 4 * testConfig(1).CheckQuorumTicks
	for tick := 1; tick <= windows; tick++ {
		route(t, nodes, leader.Tick().Messages, func(Message) bool { return false })
		if leader.Status().Role != Leader {
			t.Fatalf("leader stepped down at tick %d of an idle, caught-up cluster", tick)
		}
		if tick == windows/2 {
			probe, err := leader.ReadProbe(9)
			if err != nil {
				t.Fatal(err)
			}
			for _, message := range probe.Messages {
				for _, response := range nodes[message.To].Step(message).Messages {
					if response.Type != MsgAppendResponse || response.Reject || response.Context != 9 {
						t.Fatalf("read probe response = %+v, want a success carrying context 9", response)
					}
					if appends := appendsTo(leader.Step(response), response.From); len(appends) != 0 {
						t.Fatalf("non-advancing read ack produced %d appends, want none", len(appends))
					}
				}
			}
		}
	}
	for id, index := range leader.Status().MatchIndex {
		if index != match[id] {
			t.Fatalf("matchIndex[%d] moved from %d to %d without new entries", id, match[id], index)
		}
	}
}
