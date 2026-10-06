package main

import (
	"context"
	"errors"
	"time"

	"lsmdb/cluster"
	"lsmdb/internal/benchenv"
	"lsmdb/internal/raft"
)

const (
	modeThroughput = "throughput"
	modeFailover   = "failover"
	probeKey       = "failover-probe"
)

// FailoverResult is one leader stop measured after the throughput window.
// FailoverMs runs from the start of the leader's shutdown to the first
// successful write; AfterCloseMs starts once Close has returned, which is how
// the pre-rewrite tool (bd67696) measured it.
type FailoverResult struct {
	StoppedLeader uint64  `json:"stopped_leader"`
	StoppedTerm   uint64  `json:"stopped_term"`
	OK            bool    `json:"probe_succeeded"`
	Error         string  `json:"probe_error,omitempty"`
	FailoverMs    float64 `json:"failover_ms"`
	CloseMs       float64 `json:"leader_close_ms"`
	AfterCloseMs  float64 `json:"failover_after_close_ms"`
	ProbeAttempts uint64  `json:"probe_attempts"`
	NewLeader     uint64  `json:"new_leader"`
	NewTerm       uint64  `json:"new_term"`
}

// currentLeader returns the index of the leader at the highest term, so a
// deposed leader that has not yet noticed is never the one stopped.
func currentLeader(statuses []raft.Status) (int, error) {
	index := -1
	for i, status := range statuses {
		if status.Role == raft.Leader && (index < 0 || status.Term > statuses[index].Term) {
			index = i
		}
	}
	if index < 0 {
		return 0, errors.New("no node reports itself leader")
	}
	return index, nil
}

func failoverTimings(stopStart, closed, done time.Time, probeErr error) FailoverResult {
	result := FailoverResult{
		OK:           probeErr == nil,
		FailoverMs:   benchenv.Milliseconds(done.Sub(stopStart)),
		CloseMs:      benchenv.Milliseconds(closed.Sub(stopStart)),
		AfterCloseMs: benchenv.Milliseconds(done.Sub(closed)),
	}
	if probeErr != nil {
		result.Error = probeErr.Error()
	}
	return result
}

// measureFailover stops the current leader and times one probe write through
// a client that was talking to it, then records who leads afterwards.
func measureFailover(nodes []*cluster.Node, probe *cluster.Client, value []byte) (*FailoverResult, error) {
	statuses, err := statusAll(nodes)
	if err != nil {
		return nil, err
	}
	index, err := currentLeader(statuses)
	if err != nil {
		return nil, err
	}
	attemptsBefore := probe.Attempts()
	stopStart := time.Now()
	_ = nodes[index].Close()
	closed := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	_, probeErr := probe.Put(ctx, []byte(probeKey), value)
	done := time.Now()
	cancel()

	result := failoverTimings(stopStart, closed, done, probeErr)
	result.StoppedLeader, result.StoppedTerm = statuses[index].ID, statuses[index].Term
	result.ProbeAttempts = probe.Attempts() - attemptsBefore
	survivors := append(append([]*cluster.Node{}, nodes[:index]...), nodes[index+1:]...)
	if after, err := statusAll(survivors); err == nil {
		if leader, err := currentLeader(after); err == nil {
			result.NewLeader, result.NewTerm = after[leader].ID, after[leader].Term
		}
	}
	return &result, nil
}
