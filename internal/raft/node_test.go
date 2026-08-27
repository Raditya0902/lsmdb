package raft

import "testing"

func testConfig(id uint64) Config {
	return Config{
		ID: id, Peers: []uint64{1, 2, 3}, ElectionTickMin: 5,
		ElectionTickMax: 10, HeartbeatTicks: 1, CheckQuorumTicks: 5,
		RandomSeed: id,
	}
}

func newTestCluster(t *testing.T) map[uint64]*Node {
	t.Helper()
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		node, err := New(testConfig(id), HardState{}, nil)
		if err != nil {
			t.Fatalf("New(%d): %v", id, err)
		}
		nodes[id] = node
	}
	return nodes
}

func deliverAll(t *testing.T, nodes map[uint64]*Node, initial []Message) []Entry {
	t.Helper()
	queue := append([]Message(nil), initial...)
	var committed []Entry
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 1000 {
			t.Fatal("message delivery did not quiesce")
		}
		message := queue[0]
		queue = queue[1:]
		node := nodes[message.To]
		if node == nil {
			t.Fatalf("message addressed to unknown node %d", message.To)
		}
		update := node.Step(message)
		queue = append(queue, update.Messages...)
		committed = append(committed, update.Committed...)
	}
	return committed
}

func electNodeOne(t *testing.T, nodes map[uint64]*Node) *Node {
	t.Helper()
	node := nodes[1]
	for ticks := 0; ticks < 20 && node.Status().Role != Leader; ticks++ {
		update := node.Tick()
		deliverAll(t, nodes, update.Messages)
	}
	if node.Status().Role != Leader {
		t.Fatalf("node 1 did not become leader: %+v", node.Status())
	}
	return node
}

func TestPreVoteDoesNotIncreaseTermWithoutQuorum(t *testing.T) {
	node, err := New(testConfig(1), HardState{Term: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		node.Tick()
	}
	status := node.Status()
	if status.Term != 4 {
		t.Fatalf("isolated pre-vote changed term to %d", status.Term)
	}
	if status.Role != PreCandidate {
		t.Fatalf("role = %s, want pre-candidate", status.Role)
	}
}

func TestFollowerRejectsPreVoteWhileLeaderIsActive(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 4})
	update := node.Step(Message{Type: MsgPreVote, From: 3, To: 2, Term: 5})
	if len(update.Messages) != 1 || !update.Messages[0].Reject {
		t.Fatalf("active follower pre-vote response = %#v", update.Messages)
	}
}

func TestElectionAndMajorityCommit(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)

	index, update, err := leader.Propose([]byte("put:a=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if index != 2 { // index 1 is the leader's current-term no-op
		t.Fatalf("proposal index = %d, want 2", index)
	}
	committed := append([]Entry(nil), update.Committed...)
	committed = append(committed, deliverAll(t, nodes, update.Messages)...)
	if leader.Status().CommitIndex != 2 {
		t.Fatalf("leader commit = %d, want 2", leader.Status().CommitIndex)
	}
	found := false
	for _, entry := range committed {
		if entry.Index == 2 && string(entry.Data) == "put:a=1" {
			found = true
		}
	}
	if !found {
		t.Fatal("proposal was not emitted as committed")
	}
	for id, node := range nodes {
		entries := node.Entries()
		if len(entries) != 2 || string(entries[1].Data) != "put:a=1" {
			t.Fatalf("node %d log did not converge: %#v", id, entries)
		}
	}
}

func TestFollowerTruncatesConflictingSuffix(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 3}, []Entry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 2, Data: []byte("conflict")},
	})
	if err != nil {
		t.Fatal(err)
	}
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 3, LogIndex: 1, LogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 3, Data: []byte("winner")}}, LeaderCommit: 2,
	})
	if update.TruncateFrom != 2 {
		t.Fatalf("TruncateFrom = %d, want 2", update.TruncateFrom)
	}
	entries := node.Entries()
	if len(entries) != 2 || entries[1].Term != 3 || string(entries[1].Data) != "winner" {
		t.Fatalf("log after repair = %#v", entries)
	}
	if node.Status().CommitIndex != 2 {
		t.Fatalf("commit = %d, want 2", node.Status().CommitIndex)
	}
}

func TestFollowerNeverTruncatesCommittedEntry(t *testing.T) {
	cfg := testConfig(2)
	cfg.AppliedIndex = 1
	node, err := New(cfg, HardState{Term: 2}, []Entry{{Index: 1, Term: 1, Data: []byte("committed")}})
	if err != nil {
		t.Fatal(err)
	}
	update := node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 2, LogIndex: 0, Entries: []Entry{{Index: 1, Term: 2, Data: []byte("unsafe")}}})
	if len(update.Messages) != 1 || !update.Messages[0].Reject {
		t.Fatalf("response = %#v", update.Messages)
	}
	if string(node.Entries()[0].Data) != "committed" {
		t.Fatal("committed entry was replaced")
	}
}

func TestFollowerRejectsMalformedMembershipWithoutMutatingLog(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bad := append(append([]byte(nil), membershipPrefix...), []byte("not-json")...)
	update := node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 2, Entries: []Entry{{Index: 1, Term: 2, Data: bad}}})
	if len(update.Messages) != 1 || !update.Messages[0].Reject {
		t.Fatalf("response = %#v", update.Messages)
	}
	if len(update.Entries) != 0 || len(node.Entries()) != 0 {
		t.Fatal("malformed membership changed the log")
	}
}

func TestLeaderStepsDownAfterLosingQuorum(t *testing.T) {
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)

	// The first check window consumes successful election-era responses. The
	// second has no follower responses and must demote the leader.
	for i := 0; i < 2*testConfig(1).CheckQuorumTicks; i++ {
		leader.Tick()
	}
	if leader.Status().Role != Follower {
		t.Fatalf("role after quorum loss = %s, want follower", leader.Status().Role)
	}
	if _, _, err := leader.Propose([]byte("unsafe")); err != ErrNotLeader {
		t.Fatalf("proposal after quorum loss error = %v, want ErrNotLeader", err)
	}
}

func TestStaleLeaderCannotOverwriteNewerLog(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 5}, []Entry{{Index: 1, Term: 5}})
	if err != nil {
		t.Fatal(err)
	}
	update := node.Step(Message{
		Type: MsgAppend, From: 1, To: 2, Term: 4, LogIndex: 0,
		Entries: []Entry{{Index: 1, Term: 4, Data: []byte("stale")}},
	})
	if len(update.Messages) != 1 || !update.Messages[0].Reject || update.Messages[0].Term != 5 {
		t.Fatalf("stale append response = %#v", update.Messages)
	}
	if node.Entries()[0].Term != 5 {
		t.Fatal("stale leader changed the log")
	}
}

func TestFollowerInstallsSnapshotThenContinuesAppending(t *testing.T) {
	node, err := New(testConfig(2), HardState{Term: 3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	update := node.Step(Message{Type: MsgSnapshot, From: 1, To: 2, Term: 4, Snapshot: &Snapshot{Index: 3, Term: 2, Data: []byte("image")}})
	if update.Snapshot == nil || update.Snapshot.Index != 3 {
		t.Fatalf("snapshot update = %#v", update.Snapshot)
	}
	if len(update.Messages) != 1 || update.Messages[0].Type != MsgSnapshotResponse || update.Messages[0].Reject {
		t.Fatalf("snapshot response = %#v", update.Messages)
	}
	update = node.Step(Message{Type: MsgAppend, From: 1, To: 2, Term: 4, LogIndex: 3, LogTerm: 2, Entries: []Entry{{Index: 4, Term: 4, Data: []byte("next")}}, LeaderCommit: 4})
	if len(update.Committed) != 1 || update.Committed[0].Index != 4 {
		t.Fatalf("post-snapshot committed = %#v", update.Committed)
	}
	status := node.Status()
	if status.SnapshotIndex != 3 || status.LastLogIndex != 4 || status.RetainedLogEntries != 1 {
		t.Fatalf("status = %+v", status)
	}
}

func TestLeaderSendsSnapshotToFollowerBehindCompactedPrefix(t *testing.T) {
	cfg := testConfig(1)
	cfg.AppliedIndex = 3
	node, err := New(cfg, HardState{Term: 1}, nil, Snapshot{Index: 3, Term: 1, Data: []byte("image")})
	if err != nil {
		t.Fatal(err)
	}
	var update Update
	for i := 0; i < 20 && node.Status().Role != PreCandidate; i++ {
		update = node.Tick()
	}
	update = node.Step(Message{Type: MsgPreVoteResponse, From: 2, To: 1, Term: 2})
	update = node.Step(Message{Type: MsgVoteResponse, From: 2, To: 1, Term: 2})
	if node.Status().Role != Leader {
		t.Fatalf("role = %s", node.Status().Role)
	}
	update = node.Step(Message{Type: MsgAppendResponse, From: 3, To: 1, Term: 2, Reject: true, RejectHint: 1})
	if len(update.Messages) != 1 || update.Messages[0].Type != MsgSnapshot || update.Messages[0].Snapshot == nil {
		t.Fatalf("catch-up message = %#v", update.Messages)
	}
}

func newMembershipCluster(t *testing.T) map[uint64]*Node {
	t.Helper()
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 4; id++ {
		node, err := New(testConfig(id), HardState{}, nil)
		if err != nil {
			t.Fatalf("New(%d): %v", id, err)
		}
		nodes[id] = node
	}
	return nodes
}

func TestJointConsensusRequiresOldAndNewMajorities(t *testing.T) {
	nodes := newMembershipCluster(t)
	leader := electNodeOne(t, nodes)
	finalIndex, update, err := leader.ProposeMembership([]uint64{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	if finalIndex != 3 {
		t.Fatalf("final index = %d, want 3", finalIndex)
	}
	if _, _, err := leader.ProposeMembership([]uint64{1, 2}); err != ErrMembershipChangeInProgress {
		t.Fatalf("overlapping change error = %v", err)
	}

	// Node 2 plus the leader is an old majority, but only two of four new voters.
	var toFour []Message
	for _, message := range update.Messages {
		switch message.To {
		case 2:
			response := nodes[2].Step(message)
			for _, reply := range response.Messages {
				leader.Step(reply)
			}
		case 4:
			toFour = append(toFour, message)
		}
	}
	if leader.Status().CommitIndex != 1 {
		t.Fatalf("joint entry committed without new majority: %+v", leader.Status())
	}

	// Catching up node 4 creates both required majorities and commits C_old,new.
	queue := toFour
	var completion Update
	for steps := 0; len(queue) > 0 && steps < 20; steps++ {
		message := queue[0]
		queue = queue[1:]
		response := nodes[message.To].Step(message)
		for _, reply := range response.Messages {
			if reply.To == 1 {
				completion = leader.Step(reply)
				queue = append(queue, completion.Messages...)
			} else {
				queue = append(queue, reply)
			}
		}
		if leader.Status().CommitIndex >= 2 {
			break
		}
	}
	if leader.Status().CommitIndex != 2 {
		t.Fatalf("joint entry did not commit: %+v", leader.Status())
	}
	if leader.Status().Membership.Index != 3 || len(leader.Status().Membership.JointVoters) != 0 {
		t.Fatalf("final configuration was not appended: %+v", leader.Status().Membership)
	}
	deliverAll(t, nodes, completion.Messages)
	if leader.Status().CommitIndex != 3 {
		t.Fatalf("final configuration did not commit: %+v", leader.Status())
	}
	for id, node := range nodes {
		if !equalVoters(node.Status().Membership.Voters, []uint64{1, 2, 3, 4}) {
			t.Fatalf("node %d membership = %+v", id, node.Status().Membership)
		}
	}
}

func TestRemovedLeaderStepsDownAfterFinalConfigurationCommits(t *testing.T) {
	nodes := newMembershipCluster(t)
	leader := electNodeOne(t, nodes)
	_, update, err := leader.ProposeMembership([]uint64{2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	deliverAll(t, nodes, update.Messages)
	status := leader.Status()
	if status.CommitIndex != 3 || status.Role != Follower {
		t.Fatalf("removed leader status = %+v", status)
	}
	if _, _, err := leader.Propose([]byte("unsafe")); err != ErrNotLeader {
		t.Fatalf("removed leader proposal error = %v", err)
	}
}

func TestMembershipSurvivesSnapshotRestore(t *testing.T) {
	nodes := newMembershipCluster(t)
	leader := electNodeOne(t, nodes)
	_, update, err := leader.ProposeMembership([]uint64{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	deliverAll(t, nodes, update.Messages)
	status := leader.Status()
	snapshotUpdate, err := leader.CreateSnapshot(status.CommitIndex, []byte("state"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := *snapshotUpdate.Snapshot
	cfg := testConfig(1)
	cfg.AppliedIndex = snapshot.Index
	restored, err := New(cfg, HardState{Term: status.Term}, nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !equalVoters(restored.Status().Membership.Voters, []uint64{1, 2, 3, 4}) {
		t.Fatalf("restored membership = %+v", restored.Status().Membership)
	}
}

func TestJointConfigurationControlsElectionAndReadQuorums(t *testing.T) {
	joint := Entry{Index: 1, Term: 1, Data: encodeMembership(membershipCommand{Phase: configJoint, Old: []uint64{1, 2, 3}, New: []uint64{1, 3, 4}})}
	node, err := New(testConfig(1), HardState{Term: 1}, []Entry{joint})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20 && node.Status().Role != PreCandidate; i++ {
		node.Tick()
	}
	if node.Status().Role != PreCandidate {
		t.Fatalf("role = %s", node.Status().Role)
	}
	node.Step(Message{Type: MsgPreVoteResponse, From: 2, To: 1, Term: 2})
	if node.Status().Role != PreCandidate {
		t.Fatalf("old majority alone advanced election: %s", node.Status().Role)
	}
	node.Step(Message{Type: MsgPreVoteResponse, From: 4, To: 1, Term: 2})
	if node.Status().Role != Candidate {
		t.Fatalf("joint majorities did not advance election: %s", node.Status().Role)
	}
	if node.HasQuorum(map[uint64]struct{}{1: {}, 2: {}}) {
		t.Fatal("old-only acknowledgements satisfied joint quorum")
	}
	if !node.HasQuorum(map[uint64]struct{}{1: {}, 2: {}, 4: {}}) {
		t.Fatal("old and new majorities did not satisfy joint quorum")
	}
}
