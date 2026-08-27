package raft

import (
	"fmt"
	"sort"
)

// Node is a deterministic Raft state machine. It performs no I/O and owns no
// goroutines; callers drive it with Tick, Step, and Propose.
type Node struct {
	cfg Config

	role     Role
	term     uint64
	votedFor uint64
	leaderID uint64
	snapshot Snapshot
	log      []Entry
	commit   uint64

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	quorumElapsed    int
	random           uint64

	votes        map[uint64]bool
	nextIndex    map[uint64]uint64
	matchIndex   map[uint64]uint64
	recentActive map[uint64]bool
	peersSet     map[uint64]struct{}
}

// New constructs a node from durable hard state and log entries.
func New(cfg Config, hard HardState, entries []Entry, restored ...Snapshot) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	var snapshot Snapshot
	if len(restored) > 1 {
		return nil, fmt.Errorf("at most one restored snapshot is allowed")
	}
	if len(restored) == 1 {
		snapshot = cloneSnapshot(restored[0])
		if (snapshot.Index == 0) != (snapshot.Term == 0) || (snapshot.Index == 0 && len(snapshot.Data) != 0) {
			return nil, fmt.Errorf("restored snapshot requires positive index and term")
		}
	}
	log := make([]Entry, len(entries))
	for i, entry := range entries {
		if entry.Index != snapshot.Index+uint64(i)+1 || entry.Term == 0 {
			return nil, fmt.Errorf("raft log is not contiguous at position %d", i)
		}
		log[i] = cloneEntry(entry)
	}
	peers := append([]uint64(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	cfg.Peers = peers
	n := &Node{
		cfg: cfg, role: Follower, term: hard.Term, votedFor: hard.VotedFor,
		snapshot: snapshot, log: log, commit: cfg.AppliedIndex, random: cfg.RandomSeed, votes: make(map[uint64]bool),
		nextIndex: make(map[uint64]uint64), matchIndex: make(map[uint64]uint64),
		recentActive: make(map[uint64]bool),
		peersSet:     make(map[uint64]struct{}, len(peers)),
	}
	for _, peer := range peers {
		n.peersSet[peer] = struct{}{}
	}
	if hard.VotedFor != 0 {
		if _, ok := n.peersSet[hard.VotedFor]; !ok {
			return nil, fmt.Errorf("durable vote references non-member %d", hard.VotedFor)
		}
	}
	if cfg.AppliedIndex < snapshot.Index || cfg.AppliedIndex > n.lastIndex() {
		return nil, fmt.Errorf("applied index %d is outside snapshot/log range [%d,%d]", cfg.AppliedIndex, snapshot.Index, n.lastIndex())
	}
	if n.random == 0 {
		n.random = cfg.ID*6364136223846793005 + 1442695040888963407
	}
	n.resetElectionTimer()
	return n, nil
}

// Tick advances logical time by one tick.
func (n *Node) Tick() Update {
	var update Update
	if n.role == Leader {
		n.heartbeatElapsed++
		n.quorumElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			update.Messages = append(update.Messages, n.broadcastAppend()...)
		}
		if n.quorumElapsed >= n.cfg.CheckQuorumTicks {
			n.quorumElapsed = 0
			active := 1
			for _, peer := range n.cfg.Peers {
				if peer != n.cfg.ID && n.recentActive[peer] {
					active++
				}
				n.recentActive[peer] = false
			}
			if active < n.quorum() {
				update.merge(n.becomeFollower(n.term, 0))
			}
		}
		return update
	}

	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		update.merge(n.startPreVote())
	}
	return update
}

// Step handles one protocol message.
func (n *Node) Step(message Message) Update {
	var update Update
	if message.To != 0 && message.To != n.cfg.ID {
		return update
	}
	if _, member := n.peersSet[message.From]; !member || message.From == n.cfg.ID {
		return update
	}

	if message.Type != MsgPreVote && message.Type != MsgPreVoteResponse {
		if message.Term > n.term {
			update.merge(n.becomeFollower(message.Term, 0))
		} else if message.Term < n.term {
			update.Messages = append(update.Messages, n.rejectStale(message))
			return update
		}
	}

	switch message.Type {
	case MsgPreVote:
		update.Messages = append(update.Messages, n.handlePreVote(message))
	case MsgPreVoteResponse:
		update.merge(n.handlePreVoteResponse(message))
	case MsgVote:
		update.merge(n.handleVote(message))
	case MsgVoteResponse:
		update.merge(n.handleVoteResponse(message))
	case MsgAppend:
		update.merge(n.handleAppend(message))
	case MsgAppendResponse:
		update.merge(n.handleAppendResponse(message))
	case MsgSnapshot:
		update.merge(n.handleSnapshot(message))
	case MsgSnapshotResponse:
		update.merge(n.handleSnapshotResponse(message))
	}
	return update
}

// Compact installs a locally-created snapshot boundary and discards its log prefix.
func (n *Node) Compact(snapshot Snapshot) (Update, error) {
	if snapshot.Index <= n.snapshot.Index {
		return Update{}, nil
	}
	if snapshot.Index > n.commit || snapshot.Term == 0 || n.termAt(snapshot.Index) != snapshot.Term {
		return Update{}, fmt.Errorf("invalid snapshot boundary %d/%d at commit %d", snapshot.Index, snapshot.Term, n.commit)
	}
	n.compactLog(snapshot)
	copy := cloneSnapshot(snapshot)
	return Update{Snapshot: &copy}, nil
}

// CreateSnapshot binds opaque state-machine bytes to a committed log term.
func (n *Node) CreateSnapshot(index uint64, data []byte) (Update, error) {
	return n.Compact(Snapshot{Index: index, Term: n.termAt(index), Data: append([]byte(nil), data...)})
}

// Propose appends a command on the leader and starts replication.
func (n *Node) Propose(data []byte) (uint64, Update, error) {
	if n.role != Leader {
		return 0, Update{}, ErrNotLeader
	}
	entry := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: append([]byte(nil), data...)}
	n.log = append(n.log, entry)
	n.matchIndex[n.cfg.ID] = entry.Index
	n.nextIndex[n.cfg.ID] = entry.Index + 1
	update := Update{Entries: []Entry{cloneEntry(entry)}}
	update.Messages = append(update.Messages, n.broadcastAppend()...)
	update.merge(n.maybeCommit())
	return entry.Index, update, nil
}

// ReadProbe emits a contextual heartbeat used to confirm current leadership.
func (n *Node) ReadProbe(context uint64) (Update, error) {
	if n.role != Leader {
		return Update{}, ErrNotLeader
	}
	if n.commit == 0 || n.termAt(n.commit) != n.term {
		return Update{}, ErrReadNotReady
	}
	update := Update{}
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			update.Messages = append(update.Messages, n.appendMessage(peer, context))
		}
	}
	return update, nil
}

// QuorumSize returns the fixed-membership majority size.
func (n *Node) QuorumSize() int { return n.quorum() }

// Status returns a snapshot suitable for diagnostics.
func (n *Node) Status() Status {
	status := Status{
		ID: n.cfg.ID, Role: n.role, Term: n.term, LeaderID: n.leaderID,
		CommitIndex: n.commit, LastLogIndex: n.lastIndex(), VotedFor: n.votedFor,
		SnapshotIndex: n.snapshot.Index, RetainedLogEntries: uint64(len(n.log)),
	}
	if n.role == Leader {
		status.MatchIndex = make(map[uint64]uint64, len(n.matchIndex))
		for id, index := range n.matchIndex {
			status.MatchIndex[id] = index
		}
	}
	return status
}

// Entries returns a defensive copy for persistence and tests.
func (n *Node) Entries() []Entry {
	entries := make([]Entry, len(n.log))
	for i := range n.log {
		entries[i] = cloneEntry(n.log[i])
	}
	return entries
}

func (n *Node) startPreVote() Update {
	changed := n.role != PreCandidate
	n.role = PreCandidate
	n.leaderID = 0
	n.votes = map[uint64]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	update := Update{RoleChanged: changed}
	if n.quorum() == 1 {
		return n.startElection()
	}
	lastIndex, lastTerm := n.lastIndex(), n.termAt(n.lastIndex())
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		update.Messages = append(update.Messages, Message{
			Type: MsgPreVote, From: n.cfg.ID, To: peer, Term: n.term + 1,
			LogIndex: lastIndex, LogTerm: lastTerm,
		})
	}
	return update
}

func (n *Node) startElection() Update {
	n.role = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leaderID = 0
	n.votes = map[uint64]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	update := Update{HardState: n.hardState(), RoleChanged: true}
	if n.quorum() == 1 {
		update.merge(n.becomeLeader())
		return update
	}
	lastIndex, lastTerm := n.lastIndex(), n.termAt(n.lastIndex())
	for _, peer := range n.cfg.Peers {
		if peer == n.cfg.ID {
			continue
		}
		update.Messages = append(update.Messages, Message{
			Type: MsgVote, From: n.cfg.ID, To: peer, Term: n.term,
			LogIndex: lastIndex, LogTerm: lastTerm,
		})
	}
	return update
}

func (n *Node) becomeLeader() Update {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.heartbeatElapsed = 0
	n.quorumElapsed = 0
	n.nextIndex = make(map[uint64]uint64, len(n.cfg.Peers))
	n.matchIndex = make(map[uint64]uint64, len(n.cfg.Peers))
	n.recentActive = make(map[uint64]bool, len(n.cfg.Peers))
	last := n.lastIndex()
	for _, peer := range n.cfg.Peers {
		n.nextIndex[peer] = last + 1
	}
	n.matchIndex[n.cfg.ID] = last

	noop := Entry{Index: last + 1, Term: n.term}
	n.log = append(n.log, noop)
	n.matchIndex[n.cfg.ID] = noop.Index
	n.nextIndex[n.cfg.ID] = noop.Index + 1
	update := Update{Entries: []Entry{noop}, RoleChanged: true}
	update.Messages = append(update.Messages, n.broadcastAppend()...)
	update.merge(n.maybeCommit())
	return update
}

func (n *Node) becomeFollower(term, leader uint64) Update {
	changed := n.role != Follower || n.term != term
	hardChanged := term != n.term || (term > n.term && n.votedFor != 0)
	if term > n.term {
		n.term = term
		n.votedFor = 0
	}
	n.role = Follower
	n.leaderID = leader
	n.votes = make(map[uint64]bool)
	n.resetElectionTimer()
	update := Update{RoleChanged: changed}
	if hardChanged {
		update.HardState = n.hardState()
	}
	return update
}

func (n *Node) handlePreVote(message Message) Message {
	leaderIsRecent := n.leaderID != 0 && n.electionElapsed < n.electionTimeout
	grant := message.Term >= n.term+1 && !leaderIsRecent && n.isUpToDate(message.LogIndex, message.LogTerm)
	return Message{
		Type: MsgPreVoteResponse, From: n.cfg.ID, To: message.From, Term: message.Term,
		Reject: !grant,
	}
}

func (n *Node) handlePreVoteResponse(message Message) Update {
	if n.role != PreCandidate || message.Term != n.term+1 {
		return Update{}
	}
	n.votes[message.From] = !message.Reject
	granted, rejected := n.countVotes()
	if granted >= n.quorum() {
		return n.startElection()
	}
	if rejected >= n.quorum() {
		return n.becomeFollower(n.term, 0)
	}
	return Update{}
}

func (n *Node) handleVote(message Message) Update {
	canVote := n.votedFor == 0 || n.votedFor == message.From
	grant := canVote && n.isUpToDate(message.LogIndex, message.LogTerm)
	update := Update{}
	if grant {
		n.votedFor = message.From
		n.leaderID = 0
		n.resetElectionTimer()
		update.HardState = n.hardState()
	}
	update.Messages = append(update.Messages, Message{
		Type: MsgVoteResponse, From: n.cfg.ID, To: message.From, Term: n.term,
		Reject: !grant,
	})
	return update
}

func (n *Node) handleVoteResponse(message Message) Update {
	if n.role != Candidate || message.Term != n.term {
		return Update{}
	}
	n.votes[message.From] = !message.Reject
	granted, rejected := n.countVotes()
	if granted >= n.quorum() {
		return n.becomeLeader()
	}
	if rejected >= n.quorum() {
		return n.becomeFollower(n.term, 0)
	}
	return Update{}
}

func (n *Node) handleAppend(message Message) Update {
	update := Update{}
	if n.role != Follower || n.leaderID != message.From {
		update.merge(n.becomeFollower(n.term, message.From))
	}
	n.leaderID = message.From
	n.resetElectionTimer()

	if message.LogIndex > n.lastIndex() || n.termAt(message.LogIndex) != message.LogTerm {
		hint := n.lastIndex() + 1
		if message.LogIndex <= n.lastIndex() {
			conflictTerm := n.termAt(message.LogIndex)
			hint = message.LogIndex
			for hint > n.snapshot.Index+1 && n.termAt(hint-1) == conflictTerm {
				hint--
			}
		}
		update.Messages = append(update.Messages, Message{
			Type: MsgAppendResponse, From: n.cfg.ID, To: message.From, Term: n.term,
			Reject: true, RejectHint: hint,
		})
		return update
	}

	for i, entry := range message.Entries {
		expected := message.LogIndex + uint64(i) + 1
		if entry.Index != expected || entry.Term == 0 {
			update.Messages = append(update.Messages, Message{
				Type: MsgAppendResponse, From: n.cfg.ID, To: message.From, Term: n.term,
				Reject: true, RejectHint: message.LogIndex + 1,
			})
			return update
		}
		if entry.Index <= n.lastIndex() {
			if n.termAt(entry.Index) == entry.Term {
				continue
			}
			n.log = n.log[:entry.Index-n.snapshot.Index-1]
			update.TruncateFrom = entry.Index
		}
		for _, remaining := range message.Entries[i:] {
			n.log = append(n.log, cloneEntry(remaining))
			update.Entries = append(update.Entries, cloneEntry(remaining))
		}
		break
	}

	lastNew := message.LogIndex + uint64(len(message.Entries))
	if lastNew > n.lastIndex() {
		lastNew = n.lastIndex()
	}
	if message.LeaderCommit > n.commit {
		old := n.commit
		n.commit = min(message.LeaderCommit, n.lastIndex())
		update.Committed = append(update.Committed, n.entriesBetween(old+1, n.commit+1)...)
	}
	update.Messages = append(update.Messages, Message{
		Type: MsgAppendResponse, From: n.cfg.ID, To: message.From, Term: n.term,
		LogIndex: lastNew, Context: message.Context,
	})
	return update
}

func (n *Node) handleSnapshot(message Message) Update {
	if message.Snapshot == nil || message.Snapshot.Index == 0 || message.Snapshot.Term == 0 {
		return Update{Messages: []Message{{Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term, Reject: true, RejectHint: n.lastIndex() + 1}}}
	}
	update := Update{}
	if n.role != Follower || n.leaderID != message.From {
		update.merge(n.becomeFollower(n.term, message.From))
	}
	n.leaderID = message.From
	n.resetElectionTimer()
	snapshot := cloneSnapshot(*message.Snapshot)
	if snapshot.Index > n.snapshot.Index {
		if snapshot.Index <= n.commit && n.termAt(snapshot.Index) != snapshot.Term {
			update.Messages = append(update.Messages, Message{
				Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term,
				Reject: true, RejectHint: n.lastIndex() + 1,
			})
			return update
		}
		n.compactLog(snapshot)
		if n.commit < snapshot.Index {
			n.commit = snapshot.Index
		}
		update.Snapshot = &snapshot
	}
	update.Messages = append(update.Messages, Message{
		Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term,
		LogIndex: max(snapshot.Index, n.snapshot.Index),
	})
	return update
}

func (n *Node) handleSnapshotResponse(message Message) Update {
	if n.role != Leader || message.Term != n.term {
		return Update{}
	}
	n.recentActive[message.From] = true
	if message.Reject {
		return Update{Messages: []Message{n.appendMessage(message.From, 0)}}
	}
	matched := min(message.LogIndex, n.lastIndex())
	if matched > n.matchIndex[message.From] {
		n.matchIndex[message.From] = matched
		n.nextIndex[message.From] = matched + 1
	}
	update := n.maybeCommit()
	if n.nextIndex[message.From] <= n.lastIndex() {
		update.Messages = append(update.Messages, n.appendMessage(message.From, 0))
	}
	return update
}

func (n *Node) handleAppendResponse(message Message) Update {
	if n.role != Leader || message.Term != n.term {
		return Update{}
	}
	n.recentActive[message.From] = true
	if message.Reject {
		next := n.nextIndex[message.From]
		if message.RejectHint > 0 && message.RejectHint < next {
			next = message.RejectHint
		} else if next > 1 {
			next--
		}
		n.nextIndex[message.From] = max(1, next)
		return Update{Messages: []Message{n.appendMessage(message.From, message.Context)}}
	}
	if message.LogIndex > n.matchIndex[message.From] {
		matched := min(message.LogIndex, n.lastIndex())
		n.matchIndex[message.From] = matched
		n.nextIndex[message.From] = matched + 1
	}
	update := n.maybeCommit()
	if n.nextIndex[message.From] <= n.lastIndex() {
		update.Messages = append(update.Messages, n.appendMessage(message.From, message.Context))
	}
	return update
}

func (n *Node) maybeCommit() Update {
	matches := make([]uint64, 0, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		matches = append(matches, n.matchIndex[peer])
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate <= n.commit || n.termAt(candidate) != n.term {
		return Update{}
	}
	old := n.commit
	n.commit = candidate
	update := Update{Committed: n.entriesBetween(old+1, n.commit+1)}
	update.Messages = append(update.Messages, n.broadcastAppend()...)
	return update
}

func (n *Node) broadcastAppend() []Message {
	messages := make([]Message, 0, len(n.cfg.Peers)-1)
	for _, peer := range n.cfg.Peers {
		if peer != n.cfg.ID {
			messages = append(messages, n.appendMessage(peer, 0))
		}
	}
	return messages
}

func (n *Node) appendMessage(peer, context uint64) Message {
	next := n.nextIndex[peer]
	if next == 0 {
		next = n.lastIndex() + 1
	}
	if next <= n.snapshot.Index {
		snapshot := cloneSnapshot(n.snapshot)
		return Message{Type: MsgSnapshot, From: n.cfg.ID, To: peer, Term: n.term, Snapshot: &snapshot}
	}
	return Message{
		Type: MsgAppend, From: n.cfg.ID, To: peer, Term: n.term,
		LogIndex: next - 1, LogTerm: n.termAt(next - 1),
		Entries: n.entriesBetween(next, n.lastIndex()+1), LeaderCommit: n.commit,
		Context: context,
	}
}

func (n *Node) rejectStale(message Message) Message {
	response := Message{From: n.cfg.ID, To: message.From, Term: n.term, Reject: true}
	switch message.Type {
	case MsgVote:
		response.Type = MsgVoteResponse
	case MsgAppend:
		response.Type = MsgAppendResponse
		response.RejectHint = n.lastIndex() + 1
	case MsgSnapshot:
		response.Type = MsgSnapshotResponse
	default:
		response.Type = message.Type
	}
	return response
}

func (n *Node) hardState() *HardState {
	return &HardState{Term: n.term, VotedFor: n.votedFor}
}

func (n *Node) countVotes() (granted, rejected int) {
	for _, vote := range n.votes {
		if vote {
			granted++
		} else {
			rejected++
		}
	}
	return granted, rejected
}

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) isUpToDate(index, term uint64) bool {
	localTerm := n.termAt(n.lastIndex())
	return term > localTerm || (term == localTerm && index >= n.lastIndex())
}

func (n *Node) lastIndex() uint64 { return n.snapshot.Index + uint64(len(n.log)) }

func (n *Node) termAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == n.snapshot.Index {
		return n.snapshot.Term
	}
	if index <= n.snapshot.Index || index > n.lastIndex() {
		return 0
	}
	return n.log[index-n.snapshot.Index-1].Term
}

func (n *Node) entriesBetween(from, to uint64) []Entry {
	if from >= to || from <= n.snapshot.Index || from > n.lastIndex() {
		return nil
	}
	to = min(to, n.lastIndex()+1)
	entries := make([]Entry, 0, to-from)
	for _, entry := range n.log[from-n.snapshot.Index-1 : to-n.snapshot.Index-1] {
		entries = append(entries, cloneEntry(entry))
	}
	return entries
}

func (n *Node) compactLog(snapshot Snapshot) {
	var suffix []Entry
	if snapshot.Index < n.lastIndex() && n.termAt(snapshot.Index) == snapshot.Term {
		suffix = n.entriesBetween(snapshot.Index+1, n.lastIndex()+1)
	}
	n.snapshot = cloneSnapshot(snapshot)
	n.log = suffix
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.random = n.random*6364136223846793005 + 1442695040888963407
	span := uint64(n.cfg.ElectionTickMax - n.cfg.ElectionTickMin)
	n.electionTimeout = n.cfg.ElectionTickMin + int(n.random%span)
}

func cloneEntry(entry Entry) Entry {
	entry.Data = append([]byte(nil), entry.Data...)
	return entry
}
