package raft

import "testing"

// followerHoldingTwo returns follower 2 holding entries 1 (term 1) and 2 (term
// secondTerm), at term secondTerm, with nothing committed and leader 1 known.
func followerHoldingTwo(t *testing.T, secondTerm uint64) *Node {
	t.Helper()
	node, err := New(testConfig(2), HardState{Term: secondTerm}, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: secondTerm}})
	if err != nil {
		t.Fatal(err)
	}
	node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: secondTerm, LogIndex: 2, LogTerm: secondTerm})
	return node
}

// stepDropped steps message into node and checks that nothing changed except
// the drop counter: no reply, no persistence, no commit, term, leader, role,
// log or election-timer change.
func stepDropped(t *testing.T, node *Node, message Message) Update {
	t.Helper()
	before := node.Status()
	elapsed := node.electionElapsed
	log := append([]Entry(nil), node.log...)
	update := node.Step(message)
	after := node.Status()
	if update.DroppedAppend == "" {
		t.Errorf("DroppedAppend is empty, want a reason")
	}
	if len(update.Messages) != 0 || len(update.Entries) != 0 || update.TruncateFrom != 0 ||
		len(update.Committed) != 0 || update.HardState != nil || update.RoleChanged {
		t.Errorf("update has %d messages, %d entries, truncate %d, %d committed, hardstate %v, role changed %v; want only DroppedAppend",
			len(update.Messages), len(update.Entries), update.TruncateFrom, len(update.Committed), update.HardState != nil, update.RoleChanged)
	}
	if after.Term != before.Term || after.LeaderID != before.LeaderID || after.Role != before.Role ||
		after.CommitIndex != before.CommitIndex || after.LastLogIndex != before.LastLogIndex {
		t.Errorf("term %d -> %d, leader %d -> %d, role %s -> %s, commit %d -> %d, last index %d -> %d; want unchanged",
			before.Term, after.Term, before.LeaderID, after.LeaderID, before.Role, after.Role,
			before.CommitIndex, after.CommitIndex, before.LastLogIndex, after.LastLogIndex)
	}
	if node.electionElapsed != elapsed {
		t.Errorf("electionElapsed %d -> %d, want unchanged", elapsed, node.electionElapsed)
	}
	if len(node.log) != len(log) {
		t.Fatalf("log length %d -> %d", len(log), len(node.log))
	}
	for i := range log {
		if node.log[i].Index != log[i].Index || node.log[i].Term != log[i].Term {
			t.Errorf("log[%d] = %+v, want %+v", i, node.log[i], log[i])
		}
	}
	if after.MalformedAppendsDropped != before.MalformedAppendsDropped+1 {
		t.Errorf("MalformedAppendsDropped %d -> %d, want +1", before.MalformedAppendsDropped, after.MalformedAppendsDropped)
	}
	return update
}

func TestMalformedAppendIsDroppedBeforeAnyChange(t *testing.T) {
	big := make([]byte, MaxEntryBytes+1)
	// The message term is 2. Against a follower at term 1, a dropped message
	// must not even move the term; LeaderCommit 4 must not move the commit.
	cases := []struct {
		name       string
		secondTerm uint64 // the follower's term and its entry 2's term
		logIndex   uint64
		logTerm    uint64
		entries    []Entry
	}{
		{"gap at the second entry", 1, 2, 1, []Entry{{Index: 3, Term: 1}, {Index: 5, Term: 1}}},
		{"term 0 at the second entry", 1, 2, 1, []Entry{{Index: 3, Term: 1}, {Index: 4, Term: 0}}},
		{"second entry over MaxEntryBytes", 1, 2, 1, []Entry{{Index: 3, Term: 1}, {Index: 4, Term: 1, Data: big}}},
		{"conflict then a malformed entry", 1, 1, 1, []Entry{{Index: 2, Term: 2}, {Index: 3, Term: 0}}},
		{"terms decrease", 1, 2, 1, []Entry{{Index: 3, Term: 2}, {Index: 4, Term: 1}}},
		{"first term below LogTerm", 2, 2, 2, []Entry{{Index: 3, Term: 1}}},
		{"term above the message term", 1, 2, 1, []Entry{{Index: 3, Term: 1}, {Index: 4, Term: 3}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stepDropped(t, followerHoldingTwo(t, tc.secondTerm), Message{
				Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: tc.logIndex, LogTerm: tc.logTerm,
				Entries: tc.entries, LeaderCommit: 4,
			})
		})
	}
}

func TestWellFormedAppendIsStillAccepted(t *testing.T) {
	// Control for the cases above: terms rise within the message term.
	node := followerHoldingTwo(t, 1)
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}}, LeaderCommit: 4,
	})
	if update.DroppedAppend != "" || len(update.Entries) != 2 || node.Status().CommitIndex != 4 {
		t.Fatalf("update %+v, commit %d; want 2 entries appended and commit 4", update, node.Status().CommitIndex)
	}
}

func TestAppendWithMalformedFirstEntryIsDroppedNotRejected(t *testing.T) {
	// Behavior change (item 4): a bad first entry used to draw a reject with
	// RejectHint LogIndex+1. It is now dropped with no reply.
	node := followerHoldingTwo(t, 1)
	stepDropped(t, node, Message{
		Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 4, Term: 1}},
	})
}

func TestLeaderProgressesAfterDroppedAppend(t *testing.T) {
	node := followerHoldingTwo(t, 1)
	stepDropped(t, node, Message{
		Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 1}, {Index: 4, Term: 0}}, LeaderCommit: 4,
	})
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 1}, {Index: 4, Term: 1}}, LeaderCommit: 4,
	})
	if len(update.Entries) != 2 || node.Status().CommitIndex != 4 || node.Status().LastLogIndex != 4 {
		t.Fatalf("after the valid append: %d entries, commit %d, last %d; want 2, 4, 4",
			len(update.Entries), node.Status().CommitIndex, node.Status().LastLogIndex)
	}
	if len(update.Messages) != 1 || update.Messages[0].Type != MsgAppendResponse || update.Messages[0].Reject || update.Messages[0].LogIndex != 4 {
		t.Fatalf("reply = %+v, want an ack at 4", update.Messages)
	}
}

func TestMalformedAppendsAreCountedOncePerMessage(t *testing.T) {
	node := followerHoldingTwo(t, 1)
	// Two violations in one message, then one, then a valid message.
	node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 0}, {Index: 9, Term: 0}}})
	node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 1}, {Index: 5, Term: 1}}})
	node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 1, LogIndex: 2, LogTerm: 1,
		Entries: []Entry{{Index: 3, Term: 1}}})
	if got := node.Status().MalformedAppendsDropped; got != 2 {
		t.Fatalf("MalformedAppendsDropped = %d, want 2", got)
	}
}
