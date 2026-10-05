package raft

import "testing"

func appendOrigins(update Update) map[MessageOrigin]int {
	counts := map[MessageOrigin]int{}
	for _, message := range update.Messages {
		if message.Type == MsgAppend {
			counts[message.Origin]++
		}
	}
	return counts
}

func TestAppendOriginTags(t *testing.T) {
	node, err := New(testConfig(1), HardState{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20 && node.Status().Role != PreCandidate; i++ {
		node.Tick()
	}
	node.Step(Message{Type: MsgPreVoteResponse, From: 2, To: 1, Term: 1})
	steps := []struct {
		name string
		run  func() Update
		want map[MessageOrigin]int
	}{
		{"leader election no-op", func() Update {
			return node.Step(Message{Type: MsgVoteResponse, From: 2, To: 1, Term: 1})
		}, map[MessageOrigin]int{OriginOther: 2}},
		{"proposal", func() Update {
			node.Propose([]byte("a"))
			_, update, _ := node.Propose([]byte("b"))
			return update
		}, map[MessageOrigin]int{OriginProposal: 2}},
		{"ack that advances commit", func() Update {
			return node.Step(Message{Type: MsgAppendResponse, From: 2, To: 1, Term: 1, LogIndex: 2})
		}, map[MessageOrigin]int{OriginCommitAdvance: 2, OriginAckResend: 1}},
		// D018: an ack that does not advance matchIndex sends nothing.
		{"duplicate ack", func() Update {
			return node.Step(Message{Type: MsgAppendResponse, From: 2, To: 1, Term: 1, LogIndex: 2})
		}, map[MessageOrigin]int{}},
		{"rejection retry", func() Update {
			return node.Step(Message{Type: MsgAppendResponse, From: 3, To: 1, Term: 1, Reject: true, RejectHint: 1})
		}, map[MessageOrigin]int{OriginAckResend: 1}},
		{"heartbeat", func() Update { return node.Tick() }, map[MessageOrigin]int{OriginHeartbeat: 2}},
		{"read probe", func() Update {
			update, err := node.ReadProbe(7)
			if err != nil {
				t.Fatalf("ReadProbe: %v", err)
			}
			return update
		}, map[MessageOrigin]int{OriginOther: 2}},
		{"snapshot response", func() Update {
			return node.Step(Message{Type: MsgSnapshotResponse, From: 3, To: 1, Term: 1, LogIndex: 1})
		}, map[MessageOrigin]int{OriginOther: 1}},
		{"membership proposal", func() Update {
			_, update, err := node.ProposeMembership([]uint64{1, 2, 3, 4})
			if err != nil {
				t.Fatalf("ProposeMembership: %v", err)
			}
			return update
		}, map[MessageOrigin]int{OriginOther: 3}},
	}
	for _, step := range steps {
		got := appendOrigins(step.run())
		if len(got) != len(step.want) {
			t.Fatalf("%s: origins = %v, want %v", step.name, got, step.want)
		}
		for origin, count := range step.want {
			if got[origin] != count {
				t.Fatalf("%s: origins = %v, want %v", step.name, got, step.want)
			}
		}
	}
	if node.Status().Role != Leader {
		t.Fatalf("role = %s, want leader throughout", node.Status().Role)
	}
}

func TestMessageOriginNames(t *testing.T) {
	want := map[MessageOrigin]string{
		OriginOther: "other", OriginHeartbeat: "heartbeat", OriginProposal: "proposal",
		OriginCommitAdvance: "commit_advance", OriginAckResend: "ack_resend",
	}
	for origin, name := range want {
		if origin.String() != name {
			t.Errorf("%d.String() = %q, want %q", origin, origin.String(), name)
		}
	}
}
