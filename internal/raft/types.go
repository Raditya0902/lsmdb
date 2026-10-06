// Package raft implements a deterministic Raft consensus state machine.
package raft

import (
	"errors"
	"fmt"
)

// Role is the election role of a node.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// MessageType identifies one Raft protocol message.
type MessageType uint8

const (
	MsgPreVote MessageType = iota
	MsgPreVoteResponse
	MsgVote
	MsgVoteResponse
	MsgAppend
	MsgAppendResponse
	MsgSnapshot
	MsgSnapshotResponse
)

// Entry is one replicated log entry. Indexes begin at one and are contiguous.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// HardState is the term and vote that must survive restart.
type HardState struct {
	Term     uint64
	VotedFor uint64
}

// Snapshot is a durable state-machine image at one committed log position.
// Data is opaque to the consensus core.
type Snapshot struct {
	Index      uint64
	Term       uint64
	Data       []byte
	Membership Membership
}

// Message contains fields shared by vote and append RPCs.
type Message struct {
	Type         MessageType
	From         uint64
	To           uint64
	Term         uint64
	LogIndex     uint64
	LogTerm      uint64
	Entries      []Entry
	LeaderCommit uint64
	Reject       bool
	RejectHint   uint64
	Context      uint64
	Snapshot     *Snapshot
	// Origin records which code path emitted an append, for instrumentation
	// only. It is never serialized and consensus logic never reads it.
	Origin MessageOrigin
}

// MessageOrigin labels the code path that emitted an append message.
type MessageOrigin uint8

const (
	// OriginOther covers elections, read probes, membership, and snapshot replies.
	OriginOther MessageOrigin = iota
	// OriginHeartbeat is the periodic leader broadcast from Tick.
	OriginHeartbeat
	// OriginProposal is the broadcast that follows a client proposal.
	OriginProposal
	// OriginCommitAdvance is the broadcast made when the leader's commit index advances.
	OriginCommitAdvance
	// OriginAckResend is any append sent directly in reply to an append response.
	OriginAckResend
)

func (o MessageOrigin) String() string {
	switch o {
	case OriginHeartbeat:
		return "heartbeat"
	case OriginProposal:
		return "proposal"
	case OriginCommitAdvance:
		return "commit_advance"
	case OriginAckResend:
		return "ack_resend"
	default:
		return "other"
	}
}

// Update describes effects produced by one deterministic state transition.
// Runtimes persist HardState/TruncateFrom/Entries before sending Messages.
type Update struct {
	HardState    *HardState
	TruncateFrom uint64
	Entries      []Entry
	Messages     []Message
	Committed    []Entry
	RoleChanged  bool
	Snapshot     *Snapshot
	// DroppedAppend says why a malformed append was dropped. It is empty
	// otherwise; a dropped append has no other effect.
	DroppedAppend string
}

func (u *Update) merge(other Update) {
	if other.HardState != nil {
		copy := *other.HardState
		u.HardState = &copy
	}
	if other.TruncateFrom != 0 {
		u.TruncateFrom = other.TruncateFrom
	}
	u.Entries = append(u.Entries, other.Entries...)
	u.Messages = append(u.Messages, other.Messages...)
	u.Committed = append(u.Committed, other.Committed...)
	u.RoleChanged = u.RoleChanged || other.RoleChanged
	if other.Snapshot != nil {
		copy := cloneSnapshot(*other.Snapshot)
		u.Snapshot = &copy
	}
	if other.DroppedAppend != "" {
		u.DroppedAppend = other.DroppedAppend
	}
}

// Config controls logical tick timing and the immutable bootstrap voter set.
// ID may be absent from Peers when this node starts as a non-voting learner.
type Config struct {
	ID               uint64
	Peers            []uint64
	ElectionTickMin  int
	ElectionTickMax  int
	HeartbeatTicks   int
	CheckQuorumTicks int
	RandomSeed       uint64
	// AppliedIndex is a commit watermark derived from a durable state machine on restart.
	AppliedIndex uint64
	// maxAppendBytes overrides the per-message entry cap; tests only (D019).
	maxAppendBytes uint64
	// inflightResendTicks overrides the resend timeout; tests only (D022).
	inflightResendTicks int
}

func (c Config) validate() error {
	if c.ID == 0 {
		return errors.New("raft ID must be non-zero")
	}
	if len(c.Peers) == 0 {
		return errors.New("raft peers must not be empty")
	}
	seen := make(map[uint64]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		if peer == 0 {
			return errors.New("raft peer ID must be non-zero")
		}
		if _, ok := seen[peer]; ok {
			return fmt.Errorf("duplicate raft peer %d", peer)
		}
		seen[peer] = struct{}{}
	}
	if c.ElectionTickMin <= 0 || c.ElectionTickMax <= c.ElectionTickMin {
		return errors.New("invalid election tick range")
	}
	if c.HeartbeatTicks <= 0 || c.HeartbeatTicks >= c.ElectionTickMin {
		return errors.New("heartbeat ticks must be positive and below election minimum")
	}
	if c.CheckQuorumTicks <= 0 || c.CheckQuorumTicks > c.ElectionTickMin {
		return errors.New("check quorum ticks must be positive and at most election minimum")
	}
	return nil
}

// Status is a read-only snapshot of consensus progress.
type Status struct {
	ID                 uint64
	Role               Role
	Term               uint64
	LeaderID           uint64
	CommitIndex        uint64
	LastLogIndex       uint64
	SnapshotIndex      uint64
	RetainedLogEntries uint64
	VotedFor           uint64
	MatchIndex         map[uint64]uint64
	Membership         Membership
	// MalformedAppendsDropped counts appends dropped since start because
	// their entries could not have come from a correct leader.
	MalformedAppendsDropped uint64
	// VotesIgnoredInLease counts vote requests at a higher term ignored
	// since start because this node was in its vote lease (D021).
	VotesIgnoredInLease uint64
}

var (
	// ErrNotLeader is returned when a proposal reaches a non-leader.
	ErrNotLeader = errors.New("raft node is not leader")
	// ErrStopped is reserved for the runtime interface.
	ErrStopped = errors.New("raft node is stopped")
	// ErrReadNotReady means the leader has not committed an entry in its current term.
	ErrReadNotReady = errors.New("raft leader is not ready for linearizable reads")
	// ErrMembershipChangeInProgress prevents overlapping voter changes.
	ErrMembershipChangeInProgress = errors.New("raft membership change is already in progress")
	// ErrNoMembershipChange means the requested voter set is already active.
	ErrNoMembershipChange = errors.New("requested raft membership is already active")
	// ErrEntryTooLarge rejects a proposal whose data exceeds MaxEntryBytes.
	ErrEntryTooLarge = fmt.Errorf("raft entry data exceeds %d bytes", MaxEntryBytes)
)

// Entry and message size bounds (D019). The core imports no protobuf; the
// transport adapter's tests check these bounds against encoded sizes.
const (
	// MaxEntryBytes bounds one entry's data. It covers a key-value command with a
	// 4 MiB value and a 16 KiB key, and the stores use the same limit.
	MaxEntryBytes = 4<<20 + 32<<10
	// EntryOverheadBytes bounds one entry's framing inside an append message.
	EntryOverheadBytes = 32
	// MessageEnvelopeBytes bounds an append message's fields other than entries.
	MessageEnvelopeBytes = 121
	// defaultMaxAppendBytes caps the accounted entry bytes in one append.
	defaultMaxAppendBytes = 1 << 20
	// defaultInflightResendTicks is how many leader ticks an entry-carrying
	// append stays outstanding without a response before it is sent again
	// (D022). With 20 ms ticks and the runtime's 500 ms send deadline, at most
	// five copies of a chunk are in flight to one follower.
	defaultInflightResendTicks = 5
)

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Data = append([]byte(nil), snapshot.Data...)
	snapshot.Membership = cloneMembership(snapshot.Membership)
	return snapshot
}
