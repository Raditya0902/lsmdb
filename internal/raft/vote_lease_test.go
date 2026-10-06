package raft

import "testing"

// leadership is one node leading at one term.
type leadership struct{ id, term uint64 }

// leaseSim runs a three-node cluster in lockstep: on each tick every running
// node ticks, then the resulting messages are delivered until none remain. A
// stopped node neither ticks nor sends nor receives; lose drops further
// messages.
type leaseSim struct {
	t       *testing.T
	nodes   map[uint64]*Node
	stopped map[uint64]bool
	lose    func(Message) bool
	// ignoredVotes counts vote requests at a term above the receiver's that
	// left the receiver's term unchanged.
	ignoredVotes int
	// leaders lists every leadership observed after a tick, in order.
	leaders []leadership
}

func newLeaseSim(t *testing.T, nodes map[uint64]*Node) *leaseSim {
	return &leaseSim{t: t, nodes: nodes, stopped: map[uint64]bool{}, lose: func(Message) bool { return false }}
}

// newSeededCluster builds three nodes whose election timers draw from seed.
func newSeededCluster(t *testing.T, seed uint64) map[uint64]*Node {
	t.Helper()
	nodes := make(map[uint64]*Node)
	for id := uint64(1); id <= 3; id++ {
		cfg := testConfig(id)
		cfg.RandomSeed = seed<<8 | id
		node, err := New(cfg, HardState{}, nil)
		if err != nil {
			t.Fatalf("New(%d): %v", id, err)
		}
		nodes[id] = node
	}
	return nodes
}

func (s *leaseSim) dropped(message Message) bool {
	return s.stopped[message.From] || s.stopped[message.To] || s.lose(message)
}

func (s *leaseSim) tick() {
	s.t.Helper()
	var queue []Message
	for id := uint64(1); id <= 3; id++ {
		if !s.stopped[id] {
			queue = append(queue, s.nodes[id].Tick().Messages...)
		}
	}
	for steps := 0; len(queue) > 0; steps++ {
		if steps > 1000 {
			s.t.Fatal("message delivery did not quiesce")
		}
		message := queue[0]
		queue = queue[1:]
		if s.dropped(message) {
			continue
		}
		receiver := s.nodes[message.To]
		before := receiver.Status().Term
		queue = append(queue, receiver.Step(message).Messages...)
		if message.Type == MsgVote && message.Term > before && receiver.Status().Term == before {
			s.ignoredVotes++
		}
	}
	if id, term := s.leader(); id != 0 {
		if last := len(s.leaders) - 1; last < 0 || s.leaders[last] != (leadership{id, term}) {
			s.leaders = append(s.leaders, leadership{id, term})
		}
	}
}

// leader returns the running leader with the highest term, or 0.
func (s *leaseSim) leader() (id, term uint64) {
	for candidate := uint64(1); candidate <= 3; candidate++ {
		status := s.nodes[candidate].Status()
		if !s.stopped[candidate] && status.Role == Leader && status.Term >= term {
			id, term = candidate, status.Term
		}
	}
	return id, term
}

// tickUntilNewLeader ticks until a running node leads at a term above
// oldTerm, and returns the number of ticks, or limit+1 if none did.
func (s *leaseSim) tickUntilNewLeader(oldTerm uint64, limit int) int {
	s.t.Helper()
	for ticks := 1; ticks <= limit; ticks++ {
		s.tick()
		if id, term := s.leader(); id != 0 && term > oldTerm {
			return ticks
		}
	}
	return limit + 1
}

// elect ticks until a leader exists and every node has heard from it.
func (s *leaseSim) elect() (id, term uint64) {
	s.t.Helper()
	for ticks := 0; ticks < 20*testConfig(1).ElectionTickMax; ticks++ {
		s.tick()
		if id, term = s.leader(); id == 0 {
			continue
		}
		s.tick()
		for peer := uint64(1); peer <= 3; peer++ {
			if status := s.nodes[peer].Status(); status.LeaderID != id || status.Term != term {
				s.t.Fatalf("node %d after election: leader %d term %d, want %d at %d", peer, status.LeaderID, status.Term, id, term)
			}
		}
		return id, term
	}
	s.t.Fatal("no leader elected")
	return 0, 0
}

func tickUntilRole(t *testing.T, node *Node, role Role) []Message {
	t.Helper()
	for ticks := 0; ticks < 2*testConfig(1).ElectionTickMax; ticks++ {
		if update := node.Tick(); node.Status().Role == role {
			return update.Messages
		}
	}
	t.Fatalf("node %d never became %s", node.Status().ID, role)
	return nil
}

// campaignAgainstLiveLeader reproduces the A3 elections up to the vote
// requests. Node 1 leads; node 3's lease lapses, so it grants node 2's
// pre-vote, and node 2 becomes a candidate. It returns node 2's vote requests
// by receiver.
func campaignAgainstLiveLeader(t *testing.T) (map[uint64]*Node, map[uint64]Message) {
	t.Helper()
	nodes := newTestCluster(t)
	term := electNodeOne(t, nodes).Status().Term
	tickUntilRole(t, nodes[3], PreCandidate)

	var preVote Message
	for _, message := range tickUntilRole(t, nodes[2], PreCandidate) {
		if message.Type == MsgPreVote && message.To == 3 {
			preVote = message
		}
	}
	reply := nodes[3].Step(preVote)
	if len(reply.Messages) != 1 || reply.Messages[0].Reject {
		t.Fatalf("lapsed node 3 pre-vote reply = %+v, want one grant", reply.Messages)
	}
	update := nodes[2].Step(reply.Messages[0])
	if status := nodes[2].Status(); status.Role != Candidate || status.Term != term+1 {
		t.Fatalf("node 2 after pre-vote grant: %s at %d, want candidate at %d", status.Role, status.Term, term+1)
	}
	votes := make(map[uint64]Message)
	for _, message := range update.Messages {
		if message.Type == MsgVote {
			votes[message.To] = message
		}
	}
	if len(votes) != 2 {
		t.Fatalf("node 2 vote requests = %+v, want two", update.Messages)
	}
	return nodes, votes
}

func TestVoterOutOfLeaseGrantsHigherTermVote(t *testing.T) {
	nodes, votes := campaignAgainstLiveLeader(t)
	term := votes[3].Term

	// Node 3 has not heard from node 1 since its lease lapsed.
	update := nodes[3].Step(votes[3])
	if status := nodes[3].Status(); status.Term != term || status.VotedFor != 2 {
		t.Fatalf("node 3 after vote request: term %d votedFor %d, want %d and 2", status.Term, status.VotedFor, term)
	}
	if len(update.Messages) != 1 || update.Messages[0].Reject {
		t.Fatalf("node 3 vote reply = %+v, want one grant", update.Messages)
	}
	nodes[2].Step(update.Messages[0])
	if status := nodes[2].Status(); status.Role != Leader || status.Term != term {
		t.Fatalf("node 2 after the grant: %s at %d, want leader at %d", status.Role, status.Term, term)
	}
}

func TestRejoiningNodeWithUnchangedTermDoesNotDeposeLeader(t *testing.T) {
	sim := newLeaseSim(t, newTestCluster(t))
	leader, term := sim.elect()
	rejoiner := uint64(3)
	if leader == rejoiner {
		rejoiner = 2
	}
	maxTicks := testConfig(1).ElectionTickMax

	sim.lose = isolated(rejoiner)
	for i := 0; i < 3*maxTicks; i++ {
		sim.tick()
	}
	if status := sim.nodes[rejoiner].Status(); status.Term != term {
		t.Fatalf("cut-off node %d term = %d, want %d (pre-vote keeps it)", rejoiner, status.Term, term)
	}

	// The node reaches the other follower first, then the leader too.
	sim.lose = func(message Message) bool {
		return (message.From == leader && message.To == rejoiner) || (message.From == rejoiner && message.To == leader)
	}
	for i := 0; i < 3*maxTicks; i++ {
		sim.tick()
	}
	sim.lose = func(Message) bool { return false }
	for i := 0; i < 3*maxTicks; i++ {
		sim.tick()
	}
	if len(sim.leaders) != 1 {
		t.Fatalf("leaderships = %+v, want only node %d at %d", sim.leaders, leader, term)
	}
	if status := sim.nodes[rejoiner].Status(); status.LeaderID != leader || status.Term != term {
		t.Fatalf("rejoined node %d: leader %d term %d, want %d at %d", rejoiner, status.LeaderID, status.Term, leader, term)
	}
}

func TestRejoiningNodeWithHigherTermCausesOneLeaderChange(t *testing.T) {
	sim := newLeaseSim(t, newTestCluster(t))
	leader, term := sim.elect()
	rejoiner := uint64(3)
	if leader == rejoiner {
		rejoiner = 2
	}
	maxTicks := testConfig(1).ElectionTickMax

	// A candidate whose vote requests are all lost keeps term+1.
	sim.lose = isolated(rejoiner)
	sim.nodes[rejoiner].startElection()
	for i := 0; i < 3*maxTicks; i++ {
		sim.tick()
	}
	if status := sim.nodes[rejoiner].Status(); status.Term != term+1 {
		t.Fatalf("cut-off node %d term = %d, want %d", rejoiner, status.Term, term+1)
	}

	sim.lose = func(Message) bool { return false }
	if ticks := sim.tickUntilNewLeader(term, 2*maxTicks); ticks > 2*maxTicks {
		t.Fatalf("no leader above term %d within %d ticks of rejoining: %+v", term, 2*maxTicks, sim.leaders)
	}
	for i := 0; i < 3*maxTicks; i++ {
		sim.tick()
	}
	if len(sim.leaders) != 2 {
		t.Fatalf("leaderships = %+v, want exactly one change after node %d rejoins", sim.leaders, rejoiner)
	}
	final := sim.leaders[1]
	for id := uint64(1); id <= 3; id++ {
		if status := sim.nodes[id].Status(); status.LeaderID != final.id || status.Term != final.term {
			t.Fatalf("node %d: leader %d term %d, want %d at %d", id, status.LeaderID, status.Term, final.id, final.term)
		}
	}
}

// failoverSeeds are the timer seeds the failover tests run over.
const failoverSeeds = 20

func TestFailoverAfterLeaderStopWithinTickBound(t *testing.T) {
	bound := 2 * testConfig(1).ElectionTickMax
	for seed := uint64(1); seed <= failoverSeeds; seed++ {
		sim := newLeaseSim(t, newSeededCluster(t, seed))
		leader, term := sim.elect()
		sim.stopped[leader] = true
		ticks := sim.tickUntilNewLeader(term, bound)
		if ticks > bound {
			t.Fatalf("seed %d: no leader within %d ticks of stopping node %d", seed, bound, leader)
		}
		id, _ := sim.leader()
		t.Logf("seed %d: node %d stopped, node %d leads after %d ticks", seed, leader, id, ticks)
	}
}

// TestLapsedSurvivorsGrantVotesAfterLeaderStop checks that once the leader has
// stopped, no survivor ignores a vote request: a survivor grants a pre-vote
// only after its own timeout has cleared its leader, so the vote lease adds no
// delay to a failover. Split votes can still take several election timeouts,
// so the tick limit here is a liveness limit, not a failover bound; the
// per-case tick counts are logged for comparison across changes.
func TestLapsedSurvivorsGrantVotesAfterLeaderStop(t *testing.T) {
	cfg := testConfig(1)
	limit := 10 * cfg.ElectionTickMax
	for _, lag := range []int{0, cfg.ElectionTickMin - 1, cfg.ElectionTickMax - 1} {
		for seed := uint64(1); seed <= failoverSeeds; seed++ {
			sim := newLeaseSim(t, newSeededCluster(t, seed))
			leader, term := sim.elect()
			// The first survivor stops hearing the leader lag ticks before the second.
			first := uint64(1)
			for first == leader {
				first++
			}
			sim.lose = func(message Message) bool {
				return (message.From == leader && message.To == first) || (message.From == first && message.To == leader)
			}
			for i := 0; i < lag; i++ {
				sim.tick()
			}
			sim.lose = func(Message) bool { return false }
			sim.stopped[leader] = true

			ticks := sim.tickUntilNewLeader(term, limit)
			if ticks > limit {
				t.Fatalf("lag %d seed %d: no leader within %d ticks of stopping node %d", lag, seed, limit, leader)
			}
			if sim.ignoredVotes != 0 {
				t.Fatalf("lag %d seed %d: %d vote requests ignored, want 0 once the leader is gone", lag, seed, sim.ignoredVotes)
			}
			t.Logf("lag %d seed %d: leader after %d ticks, %d leaderships", lag, seed, ticks, len(sim.leaders))
		}
	}
}

func TestLeaderThatLosesQuorumLeavesLeaseWithinTwoCheckWindows(t *testing.T) {
	sim := newLeaseSim(t, newTestCluster(t))
	id, term := sim.elect()
	leader := sim.nodes[id]
	window := testConfig(1).CheckQuorumTicks

	// No follower hears the leader or answers it from here on.
	sim.lose = isolated(id)
	ticks := 0
	for ticks < 2*window && leader.Status().Role == Leader {
		sim.tick()
		ticks++
	}
	if leader.Status().Role == Leader {
		t.Fatalf("leader still leads %d ticks after losing its quorum", ticks)
	}

	candidate := id%3 + 1
	status := leader.Status()
	update := leader.Step(Message{Type: MsgVote, From: candidate, To: id, Term: term + 1, LogIndex: status.LastLogIndex, LogTerm: term})
	if status := leader.Status(); status.Term != term+1 || status.VotedFor != candidate {
		t.Fatalf("former leader after vote request: term %d votedFor %d, want %d and %d", status.Term, status.VotedFor, term+1, candidate)
	}
	if len(update.Messages) != 1 || update.Messages[0].Reject {
		t.Fatalf("former leader vote reply = %+v, want one grant", update.Messages)
	}
}

// stepIgnoredVote steps a vote request into node and checks that it was
// ignored: no reply, no persistence, and no change to term, vote, role,
// leader, election timer or log; only the ignored-vote counter moves.
func stepIgnoredVote(t *testing.T, node *Node, message Message) {
	t.Helper()
	before := node.Status()
	elapsed, timeout := node.electionElapsed, node.electionTimeout
	update := node.Step(message)
	after := node.Status()
	if len(update.Messages) != 0 || len(update.Entries) != 0 || update.TruncateFrom != 0 ||
		len(update.Committed) != 0 || update.HardState != nil || update.RoleChanged {
		t.Errorf("node %d: update has messages %+v, %d entries, truncate %d, %d committed, hardstate %+v, role changed %v; want nothing",
			before.ID, update.Messages, len(update.Entries), update.TruncateFrom, len(update.Committed), update.HardState, update.RoleChanged)
	}
	if after.Term != before.Term || after.VotedFor != before.VotedFor || after.Role != before.Role ||
		after.LeaderID != before.LeaderID || after.LastLogIndex != before.LastLogIndex {
		t.Errorf("node %d: term %d -> %d, votedFor %d -> %d, role %s -> %s, leader %d -> %d, last index %d -> %d; want unchanged",
			before.ID, before.Term, after.Term, before.VotedFor, after.VotedFor, before.Role, after.Role,
			before.LeaderID, after.LeaderID, before.LastLogIndex, after.LastLogIndex)
	}
	if node.electionElapsed != elapsed || node.electionTimeout != timeout {
		t.Errorf("node %d: election timer %d/%d -> %d/%d, want unchanged",
			before.ID, elapsed, timeout, node.electionElapsed, node.electionTimeout)
	}
	if after.VotesIgnoredInLease != before.VotesIgnoredInLease+1 {
		t.Errorf("node %d: VotesIgnoredInLease %d -> %d, want +1", before.ID, before.VotesIgnoredInLease, after.VotesIgnoredInLease)
	}
}

func TestVoterInLeaseIgnoresHigherTermVote(t *testing.T) {
	nodes, votes := campaignAgainstLiveLeader(t)
	leader := nodes[1]
	term := leader.Status().Term

	// Node 3 granted the pre-vote while lapsed, then hears leader 1 again.
	for _, message := range appendsTo(leader.Tick(), 3) {
		deliverAll(t, nodes, []Message{message})
	}
	if status := nodes[3].Status(); status.Role != Follower || status.LeaderID != 1 || status.Term != term {
		t.Fatalf("node 3 after leader append: %s, leader %d, term %d; want follower of 1 at %d", status.Role, status.LeaderID, status.Term, term)
	}

	stepIgnoredVote(t, nodes[3], votes[3])
	stepIgnoredVote(t, leader, votes[1])
	if status := leader.Status(); status.Role != Leader || status.Term != term {
		t.Fatalf("leader 1 after vote request: %s at %d, want leader at %d", status.Role, status.Term, term)
	}
}

// followerWithLongTimeout returns a cluster led by node 1 whose follower 3 has
// just heard the leader and drew the longest election timeout,
// ElectionTickMax-1, so its pre-vote lease outlasts its vote lease.
func followerWithLongTimeout(t *testing.T) map[uint64]*Node {
	t.Helper()
	nodes := newTestCluster(t)
	leader := electNodeOne(t, nodes)
	longest := testConfig(3).ElectionTickMax - 1
	for draws := 0; draws < 100 && nodes[3].electionTimeout != longest; draws++ {
		deliverAll(t, nodes, leader.Tick().Messages)
	}
	if nodes[3].electionTimeout != longest || nodes[3].electionElapsed != 0 {
		t.Fatalf("node 3 timer %d/%d, want 0/%d", nodes[3].electionElapsed, nodes[3].electionTimeout, longest)
	}
	return nodes
}

func TestVoteLeaseEndsAtMinimumElectionTimeout(t *testing.T) {
	cfg := testConfig(3)
	cases := []struct {
		elapsed int
		ignored bool
	}{
		{elapsed: 0, ignored: true},
		{elapsed: cfg.ElectionTickMin - 1, ignored: true},
		{elapsed: cfg.ElectionTickMin, ignored: false},
		{elapsed: cfg.ElectionTickMax - 2, ignored: false},
	}
	for _, tc := range cases {
		nodes := followerWithLongTimeout(t)
		follower := nodes[3]
		for i := 0; i < tc.elapsed; i++ {
			follower.Tick()
		}
		status := follower.Status()
		if status.Role != Follower || status.LeaderID != 1 {
			t.Fatalf("elapsed %d: node 3 is %s with leader %d, want a follower of 1", tc.elapsed, status.Role, status.LeaderID)
		}
		term := status.Term
		candidate := nodes[2].Status()

		// The pre-vote check still uses the randomized timeout: rejected throughout.
		preVote := follower.Step(Message{Type: MsgPreVote, From: 2, To: 3, Term: term + 1, LogIndex: candidate.LastLogIndex, LogTerm: term})
		if len(preVote.Messages) != 1 || !preVote.Messages[0].Reject {
			t.Fatalf("elapsed %d: pre-vote reply = %+v, want a rejection", tc.elapsed, preVote.Messages)
		}

		vote := Message{Type: MsgVote, From: 2, To: 3, Term: term + 1, LogIndex: candidate.LastLogIndex, LogTerm: term}
		if tc.ignored {
			stepIgnoredVote(t, follower, vote)
			continue
		}
		update := follower.Step(vote)
		if status := follower.Status(); status.Term != term+1 || status.VotedFor != 2 || status.VotesIgnoredInLease != 0 {
			t.Fatalf("elapsed %d: term %d votedFor %d ignored %d, want %d, 2 and 0", tc.elapsed, status.Term, status.VotedFor, status.VotesIgnoredInLease, term+1)
		}
		if len(update.Messages) != 1 || update.Messages[0].Reject {
			t.Fatalf("elapsed %d: vote reply = %+v, want one grant", tc.elapsed, update.Messages)
		}
	}
}
