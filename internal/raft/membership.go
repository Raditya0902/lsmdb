package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

var membershipPrefix = []byte{0, 'l', 's', 'm', 'd', 'b', '-', 'm', 'e', 'm', 'b', 'e', 'r', '-', 1}

const (
	configJoint = "joint"
	configFinal = "final"
)

// Membership is the replicated voter configuration at a log position.
// JointVoters is non-empty only while both voter sets must reach quorum.
type Membership struct {
	Voters      []uint64 `json:"voters"`
	JointVoters []uint64 `json:"joint_voters,omitempty"`
	Index       uint64   `json:"index,omitempty"`
}

type membershipCommand struct {
	Phase string   `json:"phase"`
	Old   []uint64 `json:"old,omitempty"`
	New   []uint64 `json:"new"`
}

func encodeMembership(command membershipCommand) []byte {
	data, _ := json.Marshal(command)
	return append(append([]byte(nil), membershipPrefix...), data...)
}

func decodeMembership(data []byte) (membershipCommand, bool, error) {
	if !bytes.HasPrefix(data, membershipPrefix) {
		return membershipCommand{}, false, nil
	}
	var command membershipCommand
	if err := json.Unmarshal(data[len(membershipPrefix):], &command); err != nil {
		return command, true, fmt.Errorf("decode membership entry: %w", err)
	}
	if command.Phase != configJoint && command.Phase != configFinal {
		return command, true, errors.New("invalid membership phase")
	}
	var err error
	command.New, err = normalizeVoters(command.New)
	if err != nil {
		return command, true, err
	}
	if command.Phase == configJoint {
		command.Old, err = normalizeVoters(command.Old)
		if err != nil {
			return command, true, err
		}
	}
	return command, true, nil
}

// IsMembershipEntry identifies internal configuration commands for the runtime.
func IsMembershipEntry(data []byte) (bool, error) {
	_, config, err := decodeMembership(data)
	return config, err
}

func normalizeVoters(voters []uint64) ([]uint64, error) {
	if len(voters) == 0 {
		return nil, errors.New("voter set must not be empty")
	}
	result := append([]uint64(nil), voters...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	for i, id := range result {
		if id == 0 {
			return nil, errors.New("voter ID must be non-zero")
		}
		if i > 0 && result[i-1] == id {
			return nil, fmt.Errorf("duplicate voter %d", id)
		}
	}
	return result, nil
}

func validateMembership(m Membership) error {
	voters, err := normalizeVoters(m.Voters)
	if err != nil {
		return err
	}
	if !equalVoters(voters, m.Voters) {
		return errors.New("membership voters must be sorted")
	}
	if len(m.JointVoters) > 0 {
		joint, err := normalizeVoters(m.JointVoters)
		if err != nil {
			return err
		}
		if !equalVoters(joint, m.JointVoters) {
			return errors.New("joint voters must be sorted")
		}
	}
	return nil
}

func cloneMembership(m Membership) Membership {
	m.Voters = append([]uint64(nil), m.Voters...)
	m.JointVoters = append([]uint64(nil), m.JointVoters...)
	return m
}

func containsVoter(voters []uint64, id uint64) bool {
	i := sort.Search(len(voters), func(i int) bool { return voters[i] >= id })
	return i < len(voters) && voters[i] == id
}

func majority(voters []uint64) int { return len(voters)/2 + 1 }

func unionVoters(m Membership) []uint64 {
	set := make(map[uint64]struct{}, len(m.Voters)+len(m.JointVoters))
	for _, id := range m.Voters {
		set[id] = struct{}{}
	}
	for _, id := range m.JointVoters {
		set[id] = struct{}{}
	}
	result := make([]uint64, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func hasSetQuorum(voters []uint64, acknowledged func(uint64) bool) bool {
	count := 0
	for _, id := range voters {
		if acknowledged(id) {
			count++
		}
	}
	return count >= majority(voters)
}

func (m Membership) hasQuorum(acknowledged func(uint64) bool) bool {
	if !hasSetQuorum(m.Voters, acknowledged) {
		return false
	}
	return len(m.JointVoters) == 0 || hasSetQuorum(m.JointVoters, acknowledged)
}

func (m Membership) voteLost(rejected func(uint64) bool) bool {
	if hasSetQuorum(m.Voters, rejected) {
		return true
	}
	return len(m.JointVoters) > 0 && hasSetQuorum(m.JointVoters, rejected)
}
