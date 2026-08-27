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
	Index uint64
	Term  uint64
	Data  []byte
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
}

// Config controls logical tick timing. Peers must contain ID.
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
}

func (c Config) validate() error {
	if c.ID == 0 {
		return errors.New("raft ID must be non-zero")
	}
	if len(c.Peers) == 0 {
		return errors.New("raft peers must not be empty")
	}
	found := false
	seen := make(map[uint64]struct{}, len(c.Peers))
	for _, peer := range c.Peers {
		if peer == 0 {
			return errors.New("raft peer ID must be non-zero")
		}
		if _, ok := seen[peer]; ok {
			return fmt.Errorf("duplicate raft peer %d", peer)
		}
		seen[peer] = struct{}{}
		found = found || peer == c.ID
	}
	if !found {
		return errors.New("raft peers must contain local ID")
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
}

var (
	// ErrNotLeader is returned when a proposal reaches a non-leader.
	ErrNotLeader = errors.New("raft node is not leader")
	// ErrStopped is reserved for the runtime interface.
	ErrStopped = errors.New("raft node is stopped")
	// ErrReadNotReady means the leader has not committed an entry in its current term.
	ErrReadNotReady = errors.New("raft leader is not ready for linearizable reads")
)

func cloneSnapshot(snapshot Snapshot) Snapshot {
	snapshot.Data = append([]byte(nil), snapshot.Data...)
	return snapshot
}
