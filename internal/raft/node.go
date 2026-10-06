package raft

import (
	"errors"
	"fmt"
)

// Node is a deterministic Raft state machine. It performs no I/O and owns no
// goroutines; callers drive it with Tick, Step, and Propose.
type Node struct {
	cfg Config

	role              Role
	term              uint64
	votedFor          uint64
	leaderID          uint64
	snapshot          Snapshot
	log               []Entry
	commit            uint64
	initialMembership Membership
	membership        Membership

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	quorumElapsed    int
	random           uint64

	votes        map[uint64]bool
	nextIndex    map[uint64]uint64
	matchIndex   map[uint64]uint64
	recentActive map[uint64]bool
	// outstanding is the last index of the entry-carrying append in flight to
	// each peer, or 0; outstandingAge counts leader ticks since it was sent.
	// While one is outstanding, appends to that peer carry no entries (D022).
	outstanding    map[uint64]uint64
	outstandingAge map[uint64]int

	malformedAppendsDropped uint64
	votesIgnoredInLease     uint64
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
		if snapshot.Membership.Index > snapshot.Index {
			return nil, errors.New("snapshot membership index exceeds snapshot index")
		}
	}
	log := make([]Entry, len(entries))
	for i, entry := range entries {
		if entry.Index != snapshot.Index+uint64(i)+1 || entry.Term == 0 {
			return nil, fmt.Errorf("raft log is not contiguous at position %d", i)
		}
		log[i] = cloneEntry(entry)
	}
	peers, err := normalizeVoters(cfg.Peers)
	if err != nil {
		return nil, err
	}
	cfg.Peers = peers
	initialMembership := Membership{Voters: append([]uint64(nil), peers...)}
	snapshotMembership := initialMembership
	if snapshot.Index > 0 && len(snapshot.Membership.Voters) > 0 {
		if err := validateMembership(snapshot.Membership); err != nil {
			return nil, fmt.Errorf("restored snapshot membership: %w", err)
		}
		snapshotMembership = cloneMembership(snapshot.Membership)
	}
	n := &Node{
		cfg: cfg, role: Follower, term: hard.Term, votedFor: hard.VotedFor,
		snapshot: snapshot, log: log, commit: cfg.AppliedIndex, random: cfg.RandomSeed, votes: make(map[uint64]bool),
		nextIndex: make(map[uint64]uint64), matchIndex: make(map[uint64]uint64),
		recentActive:      make(map[uint64]bool),
		initialMembership: initialMembership, membership: snapshotMembership,
	}
	if err := n.rebuildMembership(); err != nil {
		return nil, err
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
		expired := n.expireOutstanding()
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.heartbeatElapsed = 0
			update.Messages = append(update.Messages, n.broadcastAppend(OriginHeartbeat)...)
		} else {
			// An expired append is re-sent now, not at the next heartbeat.
			for _, peer := range expired {
				update.Messages = append(update.Messages, n.appendMessage(peer, 0, OriginHeartbeat))
			}
		}
		if n.quorumElapsed >= n.cfg.CheckQuorumTicks {
			n.quorumElapsed = 0
			active := map[uint64]struct{}{}
			if n.isVoter(n.cfg.ID) {
				active[n.cfg.ID] = struct{}{}
			}
			for _, peer := range n.peers() {
				if peer != n.cfg.ID && n.recentActive[peer] {
					active[peer] = struct{}{}
				}
				n.recentActive[peer] = false
			}
			if !n.membership.hasQuorum(func(id uint64) bool { _, ok := active[id]; return ok }) {
				update.merge(n.becomeFollower(n.term, 0))
			}
		}
		return update
	}
	if !n.isVoter(n.cfg.ID) {
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
	if !n.isCommunicationPeer(message.From) || message.From == n.cfg.ID {
		return update
	}
	// A malformed append changes nothing, not even the term or the election
	// timer, so a leader that only sends such messages is replaced.
	if message.Type == MsgAppend {
		if reason := malformedAppend(message); reason != "" {
			n.malformedAppendsDropped++
			update.DroppedAppend = fmt.Sprintf("from %d at term %d: %s", message.From, message.Term, reason)
			return update
		}
	}
	// A vote request at a higher term reaching a node in its lease is ignored
	// before the term is adopted (D021). A reply would carry this node's lower
	// term, and the candidate's stale-term answer would end the lease anyway.
	if message.Type == MsgVote && message.Term > n.term && n.inLease() {
		n.votesIgnoredInLease++
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
	expected, err := n.membershipAt(snapshot.Index)
	if err != nil {
		return Update{}, err
	}
	if len(snapshot.Membership.Voters) == 0 {
		snapshot.Membership = expected
	}
	if snapshot.Membership.Index > snapshot.Index {
		return Update{}, errors.New("snapshot membership index exceeds snapshot index")
	}
	if !equalVoters(snapshot.Membership.Voters, expected.Voters) || !equalVoters(snapshot.Membership.JointVoters, expected.JointVoters) || snapshot.Membership.Index != expected.Index {
		return Update{}, errors.New("snapshot membership does not match its log index")
	}
	n.compactLog(snapshot)
	if err := n.rebuildMembership(); err != nil {
		return Update{}, err
	}
	copy := cloneSnapshot(snapshot)
	return Update{Snapshot: &copy}, nil
}

// ProposeMembership begins a two-entry joint-consensus voter transition.
// The returned index is the final configuration entry that completes the change.
func (n *Node) ProposeMembership(voters []uint64) (uint64, Update, error) {
	if n.role != Leader {
		return 0, Update{}, ErrNotLeader
	}
	if len(n.membership.JointVoters) > 0 || n.membership.Index > n.commit {
		return 0, Update{}, ErrMembershipChangeInProgress
	}
	newVoters, err := normalizeVoters(voters)
	if err != nil {
		return 0, Update{}, err
	}
	if equalVoters(newVoters, n.membership.Voters) {
		return n.membership.Index, Update{}, nil
	}
	entry := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: encodeMembership(membershipCommand{Phase: configJoint, Old: n.membership.Voters, New: newVoters})}
	n.log = append(n.log, entry)
	if err := n.rebuildMembership(); err != nil {
		return 0, Update{}, err
	}
	for _, peer := range n.peers() {
		if n.nextIndex[peer] == 0 {
			n.nextIndex[peer] = entry.Index
		}
	}
	n.matchIndex[n.cfg.ID] = entry.Index
	n.nextIndex[n.cfg.ID] = entry.Index + 1
	update := Update{Entries: []Entry{cloneEntry(entry)}}
	update.Messages = append(update.Messages, n.broadcastAppend(OriginOther)...)
	update.merge(n.maybeCommit())
	return entry.Index + 1, update, nil
}

// CreateSnapshot binds opaque state-machine bytes to a committed log term.
func (n *Node) CreateSnapshot(index uint64, data []byte) (Update, error) {
	membership, err := n.membershipAt(index)
	if err != nil {
		return Update{}, err
	}
	return n.Compact(Snapshot{Index: index, Term: n.termAt(index), Data: append([]byte(nil), data...), Membership: membership})
}

// Propose appends a command on the leader and starts replication.
func (n *Node) Propose(data []byte) (uint64, Update, error) {
	if len(data) > MaxEntryBytes {
		return 0, Update{}, ErrEntryTooLarge
	}
	indexes, update, err := n.ProposeBatch([][]byte{data})
	if err != nil {
		return 0, Update{}, err
	}
	return indexes[0], update, nil
}

// ProposeBatch appends commands on the leader in input order and starts
// replication once, so the whole batch is persisted by one Update (D022).
// indexes[i] is command i's log index, or 0 if it exceeds MaxEntryBytes; the
// other commands are appended anyway. Unless this node is leader it returns
// ErrNotLeader and appends nothing.
func (n *Node) ProposeBatch(data [][]byte) (indexes []uint64, update Update, err error) {
	if n.role != Leader {
		return nil, Update{}, ErrNotLeader
	}
	indexes = make([]uint64, len(data))
	for i, command := range data {
		if len(command) > MaxEntryBytes {
			continue
		}
		entry := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: append([]byte(nil), command...)}
		n.log = append(n.log, entry)
		update.Entries = append(update.Entries, cloneEntry(entry))
		indexes[i] = entry.Index
	}
	if len(update.Entries) == 0 {
		return indexes, Update{}, nil
	}
	n.matchIndex[n.cfg.ID] = n.lastIndex()
	n.nextIndex[n.cfg.ID] = n.lastIndex() + 1
	update.Messages = append(update.Messages, n.broadcastAppend(OriginProposal)...)
	update.merge(n.maybeCommit())
	return indexes, update, nil
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
	for _, peer := range n.peers() {
		if peer != n.cfg.ID {
			update.Messages = append(update.Messages, n.appendMessage(peer, context, OriginOther))
		}
	}
	return update, nil
}

// HasQuorum reports whether the supplied IDs satisfy the active stable or joint configuration.
func (n *Node) HasQuorum(ids map[uint64]struct{}) bool {
	return n.membership.hasQuorum(func(id uint64) bool { _, ok := ids[id]; return ok })
}

// Status returns a snapshot suitable for diagnostics.
func (n *Node) Status() Status {
	status := Status{
		ID: n.cfg.ID, Role: n.role, Term: n.term, LeaderID: n.leaderID,
		CommitIndex: n.commit, LastLogIndex: n.lastIndex(), VotedFor: n.votedFor,
		SnapshotIndex: n.snapshot.Index, RetainedLogEntries: uint64(len(n.log)),
		Membership:              cloneMembership(n.membership),
		MalformedAppendsDropped: n.malformedAppendsDropped,
		VotesIgnoredInLease:     n.votesIgnoredInLease,
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
	if n.HasQuorum(map[uint64]struct{}{n.cfg.ID: {}}) {
		return n.startElection()
	}
	lastIndex, lastTerm := n.lastIndex(), n.termAt(n.lastIndex())
	for _, peer := range n.peers() {
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
	if n.HasQuorum(map[uint64]struct{}{n.cfg.ID: {}}) {
		update.merge(n.becomeLeader())
		return update
	}
	lastIndex, lastTerm := n.lastIndex(), n.termAt(n.lastIndex())
	for _, peer := range n.peers() {
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
	n.nextIndex = make(map[uint64]uint64, len(n.peers()))
	n.matchIndex = make(map[uint64]uint64, len(n.peers()))
	n.recentActive = make(map[uint64]bool, len(n.peers()))
	n.outstanding = make(map[uint64]uint64, len(n.peers()))
	n.outstandingAge = make(map[uint64]int, len(n.peers()))
	last := n.lastIndex()
	for _, peer := range n.peers() {
		n.nextIndex[peer] = last + 1
	}
	n.matchIndex[n.cfg.ID] = last

	noop := Entry{Index: last + 1, Term: n.term}
	n.log = append(n.log, noop)
	n.matchIndex[n.cfg.ID] = noop.Index
	n.nextIndex[n.cfg.ID] = noop.Index + 1
	update := Update{Entries: []Entry{noop}, RoleChanged: true}
	update.Messages = append(update.Messages, n.broadcastAppend(OriginOther)...)
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

// inLease reports whether this node is in its vote lease (D021). A leader is in
// lease by role; any other node while it has a leader it heard within
// ElectionTickMin ticks. The randomized election timeout only spreads
// campaigns, so it does not lengthen the lease.
func (n *Node) inLease() bool {
	return n.role == Leader || (n.leaderID != 0 && n.electionElapsed < n.cfg.ElectionTickMin)
}

func (n *Node) handlePreVote(message Message) Message {
	leaderIsRecent := n.leaderID != 0 && n.electionElapsed < n.electionTimeout
	grant := n.isVoter(n.cfg.ID) && message.Term >= n.term+1 && !leaderIsRecent && n.isUpToDate(message.LogIndex, message.LogTerm)
	// A grant echoes the proposed term; a rejection reports this node's own term so a
	// pre-candidate behind it can catch up. The receiver's term never changes here.
	term := message.Term
	if !grant {
		term = n.term
	}
	return Message{
		Type: MsgPreVoteResponse, From: n.cfg.ID, To: message.From, Term: term,
		Reject: !grant,
	}
}

func (n *Node) handlePreVoteResponse(message Message) Update {
	// A live voter is at a newer term; adopt it so later pre-votes can succeed.
	if message.Reject && message.Term > n.term {
		return n.becomeFollower(message.Term, 0)
	}
	granted := !message.Reject && message.Term == n.term+1
	rejected := message.Reject && message.Term <= n.term
	if n.role != PreCandidate || (!granted && !rejected) {
		return Update{}
	}
	n.votes[message.From] = granted
	if n.membership.hasQuorum(func(id uint64) bool { return n.votes[id] }) {
		return n.startElection()
	}
	if n.membership.voteLost(func(id uint64) bool { vote, ok := n.votes[id]; return ok && !vote }) {
		return n.becomeFollower(n.term, 0)
	}
	return Update{}
}

func (n *Node) handleVote(message Message) Update {
	canVote := n.votedFor == 0 || n.votedFor == message.From
	grant := n.isVoter(n.cfg.ID) && canVote && n.isUpToDate(message.LogIndex, message.LogTerm)
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
	if n.membership.hasQuorum(func(id uint64) bool { return n.votes[id] }) {
		return n.becomeLeader()
	}
	if n.membership.voteLost(func(id uint64) bool { vote, ok := n.votes[id]; return ok && !vote }) {
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

	// Entries are never modified in place, so undoing this append needs only the
	// old length and any truncated tail. Membership is the fold over the log, so
	// it changes only on truncation or an appended configuration entry (D019).
	oldLen := len(n.log)
	var truncatedTail []Entry
	rebuild := false
	// Step has already checked every entry with malformedAppend.
	for i, entry := range message.Entries {
		if entry.Index <= n.lastIndex() {
			if n.termAt(entry.Index) == entry.Term {
				continue
			}
			if entry.Index <= n.commit {
				update.Messages = append(update.Messages, Message{Type: MsgAppendResponse, From: n.cfg.ID, To: message.From, Term: n.term, Reject: true, RejectHint: n.commit + 1})
				return update
			}
			keep := entry.Index - n.snapshot.Index - 1
			truncatedTail = append([]Entry(nil), n.log[keep:]...)
			n.log = n.log[:keep]
			update.TruncateFrom = entry.Index
			rebuild = true
		}
		for _, remaining := range message.Entries[i:] {
			rebuild = rebuild || hasMembershipPrefix(remaining.Data)
			n.log = append(n.log, cloneEntry(remaining))
			update.Entries = append(update.Entries, cloneEntry(remaining))
		}
		break
	}
	if rebuild {
		if err := n.rebuildMembership(); err != nil {
			n.log = append(n.log[:oldLen-len(truncatedTail)], truncatedTail...)
			update.TruncateFrom = 0
			update.Entries = nil
			update.Messages = append(update.Messages, Message{Type: MsgAppendResponse, From: n.cfg.ID, To: message.From, Term: n.term, Reject: true, RejectHint: n.lastIndex() + 1})
			return update
		}
	}

	// LogIndex is the previous index, so lastNew is the last entry this message proved.
	lastNew := message.LogIndex + uint64(len(message.Entries))
	if lastNew > n.lastIndex() {
		lastNew = n.lastIndex()
	}
	// Entries beyond lastNew may be a stale suffix; a delayed append must not move commit backward.
	if commit := min(message.LeaderCommit, lastNew); commit > n.commit {
		old := n.commit
		n.commit = commit
		update.Committed = append(update.Committed, n.entriesBetween(old+1, n.commit+1)...)
	}
	update.Messages = append(update.Messages, Message{
		Type: MsgAppendResponse, From: n.cfg.ID, To: message.From, Term: n.term,
		LogIndex: lastNew, Context: message.Context,
	})
	return update
}

// malformedAppend returns why an append's entries cannot have come from a
// correct leader, or "" if they are well formed. Entry i must have index
// LogIndex+i+1, a term from LogTerm up to the message term that never
// decreases, and at most MaxEntryBytes of data, as the stores require. It
// reads only the message.
func malformedAppend(message Message) string {
	previous := message.LogTerm
	for i, entry := range message.Entries {
		want := message.LogIndex + uint64(i) + 1
		switch {
		case entry.Index != want:
			return fmt.Sprintf("entry %d has index %d, want %d", i, entry.Index, want)
		case entry.Term == 0:
			return fmt.Sprintf("entry %d (index %d) has term 0", i, entry.Index)
		case len(entry.Data) > MaxEntryBytes:
			return fmt.Sprintf("entry %d (index %d) has %d bytes, over %d", i, entry.Index, len(entry.Data), MaxEntryBytes)
		case entry.Term > message.Term:
			return fmt.Sprintf("entry %d (index %d) has term %d, above the message term %d", i, entry.Index, entry.Term, message.Term)
		case i == 0 && entry.Term < previous:
			return fmt.Sprintf("entry 0 (index %d) has term %d, below LogTerm %d", entry.Index, entry.Term, previous)
		case entry.Term < previous:
			return fmt.Sprintf("entry %d (index %d) has term %d, below the previous entry's %d", i, entry.Index, entry.Term, previous)
		}
		previous = entry.Term
	}
	return ""
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
	if len(snapshot.Membership.Voters) == 0 {
		snapshot.Membership = cloneMembership(n.initialMembership)
	}
	if err := validateMembership(snapshot.Membership); err != nil {
		update.Messages = append(update.Messages, Message{Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term, Reject: true, RejectHint: n.lastIndex() + 1})
		return update
	}
	if snapshot.Membership.Index > snapshot.Index {
		update.Messages = append(update.Messages, Message{Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term, Reject: true, RejectHint: n.lastIndex() + 1})
		return update
	}
	if snapshot.Index > n.snapshot.Index {
		if snapshot.Index <= n.commit && n.termAt(snapshot.Index) != snapshot.Term {
			update.Messages = append(update.Messages, Message{
				Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term,
				Reject: true, RejectHint: n.lastIndex() + 1,
			})
			return update
		}
		oldSnapshot, oldLog, oldMembership := n.snapshot, n.log, n.membership
		n.compactLog(snapshot)
		if err := n.rebuildMembership(); err != nil {
			n.snapshot, n.log, n.membership = oldSnapshot, oldLog, oldMembership
			update.Messages = append(update.Messages, Message{Type: MsgSnapshotResponse, From: n.cfg.ID, To: message.From, Term: n.term, Reject: true, RejectHint: n.lastIndex() + 1})
			return update
		}
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
		n.outstanding[message.From] = 0
		return Update{Messages: []Message{n.appendMessage(message.From, 0, OriginOther)}}
	}
	matched := min(message.LogIndex, n.lastIndex())
	if matched > n.matchIndex[message.From] {
		n.matchIndex[message.From] = matched
		n.nextIndex[message.From] = matched + 1
	}
	n.settleOutstanding(message.From)
	update := n.maybeCommit()
	if n.nextIndex[message.From] <= n.lastIndex() {
		update.Messages = append(update.Messages, n.appendMessage(message.From, 0, OriginOther))
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
		// Everything through matchIndex matches this term's log, so a rejection
		// below it is stale. Under capped appends, letting it lower nextIndex
		// would re-ship an already-matched chunk forever (D019).
		n.nextIndex[message.From] = max(1, next, n.matchIndex[message.From]+1)
		// A rejection settles whatever was in flight: the probe goes now.
		n.outstanding[message.From] = 0
		return Update{Messages: []Message{n.appendMessage(message.From, message.Context, OriginAckResend)}}
	}
	advanced := false
	if message.LogIndex > n.matchIndex[message.From] {
		matched := min(message.LogIndex, n.lastIndex())
		advanced = matched > n.matchIndex[message.From]
		n.matchIndex[message.From] = matched
		n.nextIndex[message.From] = matched + 1
	}
	n.settleOutstanding(message.From)
	update := n.maybeCommit()
	// A duplicate or stale ack proves nothing new, and answering it sustained
	// endless append/ack chains (D018). A lost message is re-sent once its
	// append has been outstanding for the resend timeout (D022). The follow-up
	// is skipped when the commit advance above already carried entries.
	if advanced && n.outstanding[message.From] == 0 && n.nextIndex[message.From] <= n.lastIndex() {
		update.Messages = append(update.Messages, n.appendMessage(message.From, message.Context, OriginAckResend))
	}
	return update
}

func (n *Node) maybeCommit() Update {
	candidate := n.lastIndex()
	for candidate > n.commit {
		if n.termAt(candidate) == n.term && n.membership.hasQuorum(func(id uint64) bool { return n.matchIndex[id] >= candidate }) {
			break
		}
		candidate--
	}
	if candidate <= n.commit {
		return Update{}
	}
	old := n.commit
	n.commit = candidate
	update := Update{Committed: n.entriesBetween(old+1, n.commit+1)}
	if len(n.membership.JointVoters) > 0 && n.membership.Index <= n.commit {
		final := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: encodeMembership(membershipCommand{Phase: configFinal, New: n.membership.JointVoters})}
		n.log = append(n.log, final)
		if err := n.rebuildMembership(); err != nil {
			panic(err)
		}
		n.matchIndex[n.cfg.ID] = final.Index
		n.nextIndex[n.cfg.ID] = final.Index + 1
		update.Entries = append(update.Entries, cloneEntry(final))
	}
	update.Messages = append(update.Messages, n.broadcastAppend(OriginCommitAdvance)...)
	if n.membership.Index <= n.commit && !n.isVoter(n.cfg.ID) {
		update.merge(n.becomeFollower(n.term, 0))
	}
	return update
}

func (n *Node) broadcastAppend(origin MessageOrigin) []Message {
	peers := n.replicationPeers()
	messages := make([]Message, 0, len(peers)-1)
	for _, peer := range peers {
		if peer == n.cfg.ID {
			continue
		}
		// A new proposal waits for the ack of the append in flight; its
		// follow-up carries every entry proposed meanwhile (D022).
		if origin == OriginProposal && n.outstanding[peer] != 0 {
			continue
		}
		messages = append(messages, n.appendMessage(peer, 0, origin))
	}
	return messages
}

// appendMessage builds the next append to peer. While an entry-carrying append
// to peer is outstanding it carries no entries, only the commit index and any
// read context (D022); otherwise it carries the capped chunk from nextIndex and
// becomes the outstanding append.
func (n *Node) appendMessage(peer, context uint64, origin MessageOrigin) Message {
	next := n.nextIndex[peer]
	if next == 0 {
		next = n.lastIndex() + 1
	}
	if next <= n.snapshot.Index {
		snapshot := cloneSnapshot(n.snapshot)
		return Message{Type: MsgSnapshot, From: n.cfg.ID, To: peer, Term: n.term, Snapshot: &snapshot, Origin: origin}
	}
	message := Message{
		Type: MsgAppend, From: n.cfg.ID, To: peer, Term: n.term,
		LogIndex: next - 1, LogTerm: n.termAt(next - 1),
		LeaderCommit: n.commit, Context: context, Origin: origin,
	}
	if n.outstanding[peer] != 0 {
		return message
	}
	message.Entries = n.cappedEntries(next)
	if len(message.Entries) > 0 {
		n.outstanding[peer] = message.Entries[len(message.Entries)-1].Index
		n.outstandingAge[peer] = 0
	}
	return message
}

// settleOutstanding ends peer's outstanding append once peer has matched
// everything it carried.
func (n *Node) settleOutstanding(peer uint64) {
	if n.outstanding[peer] != 0 && n.matchIndex[peer] >= n.outstanding[peer] {
		n.outstanding[peer] = 0
	}
}

// expireOutstanding ages every outstanding append by one tick and ends those
// that reached the resend timeout, returning their peers in ascending order.
func (n *Node) expireOutstanding() []uint64 {
	limit := n.cfg.inflightResendTicks
	if limit == 0 {
		limit = defaultInflightResendTicks
	}
	var expired []uint64
	for _, peer := range n.replicationPeers() {
		if n.outstanding[peer] == 0 {
			continue
		}
		n.outstandingAge[peer]++
		if n.outstandingAge[peer] >= limit {
			n.outstanding[peer] = 0
			expired = append(expired, peer)
		}
	}
	return expired
}

// cappedEntries returns the entries from index from whose accounted size fits
// the per-message cap, always at least one, so a single larger entry still
// ships alone (D019).
func (n *Node) cappedEntries(from uint64) []Entry {
	if from <= n.snapshot.Index || from > n.lastIndex() {
		return nil
	}
	limit := n.cfg.maxAppendBytes
	if limit == 0 {
		limit = defaultMaxAppendBytes
	}
	to, size := from, uint64(0)
	for ; to <= n.lastIndex(); to++ {
		entrySize := uint64(len(n.log[to-n.snapshot.Index-1].Data)) + EntryOverheadBytes
		if to > from && size+entrySize > limit {
			break
		}
		size += entrySize
	}
	return n.entriesBetween(from, to)
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

func (n *Node) rebuildMembership() error {
	base := n.initialMembership
	if n.snapshot.Index > 0 && len(n.snapshot.Membership.Voters) > 0 {
		base = n.snapshot.Membership
	}
	membership := cloneMembership(base)
	for _, entry := range n.log {
		command, config, err := decodeMembership(entry.Data)
		if err != nil {
			return fmt.Errorf("membership entry %d: %w", entry.Index, err)
		}
		if !config {
			continue
		}
		membership, err = applyMembership(membership, command, entry.Index)
		if err != nil {
			return fmt.Errorf("membership entry %d: %w", entry.Index, err)
		}
	}
	n.membership = membership
	return nil
}

func (n *Node) membershipAt(index uint64) (Membership, error) {
	base := n.initialMembership
	if n.snapshot.Index > 0 && len(n.snapshot.Membership.Voters) > 0 {
		base = n.snapshot.Membership
	}
	membership := cloneMembership(base)
	for _, entry := range n.log {
		if entry.Index > index {
			break
		}
		command, config, err := decodeMembership(entry.Data)
		if err != nil {
			return Membership{}, err
		}
		if config {
			membership, err = applyMembership(membership, command, entry.Index)
			if err != nil {
				return Membership{}, err
			}
		}
	}
	return membership, nil
}

func applyMembership(current Membership, command membershipCommand, index uint64) (Membership, error) {
	switch command.Phase {
	case configJoint:
		if len(current.JointVoters) != 0 || !equalVoters(current.Voters, command.Old) {
			return Membership{}, errors.New("joint configuration does not match current voters")
		}
		return Membership{Voters: append([]uint64(nil), command.Old...), JointVoters: append([]uint64(nil), command.New...), Index: index}, nil
	case configFinal:
		if len(current.JointVoters) == 0 || !equalVoters(current.JointVoters, command.New) {
			return Membership{}, errors.New("final configuration has no matching joint configuration")
		}
		return Membership{Voters: append([]uint64(nil), command.New...), Index: index}, nil
	default:
		return Membership{}, errors.New("unknown membership phase")
	}
}

func equalVoters(a, b []uint64) bool {
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

func (n *Node) peers() []uint64        { return unionVoters(n.membership) }
func (n *Node) isVoter(id uint64) bool { return containsVoter(n.peers(), id) }
func (n *Node) replicationPeers() []uint64 {
	if len(n.membership.JointVoters) == 0 && n.membership.Index > n.commit {
		if prior, err := n.membershipAt(n.membership.Index - 1); err == nil && len(prior.JointVoters) > 0 {
			return unionVoters(prior)
		}
	}
	return n.peers()
}
func (n *Node) isCommunicationPeer(id uint64) bool {
	return containsVoter(n.replicationPeers(), id)
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
