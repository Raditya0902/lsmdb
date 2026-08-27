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
