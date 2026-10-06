package main

import (
	"testing"
	"time"

	"lsmdb/internal/raft"
)

func TestModeFlag(t *testing.T) {
	opts, err := parseFlags(nil)
	if err != nil || opts.mode != modeThroughput {
		t.Fatalf("default mode = %q (err %v), want %q", opts.mode, err, modeThroughput)
	}
	opts, err = parseFlags([]string{"-mode", "failover"})
	if err != nil || opts.mode != modeFailover {
		t.Fatalf("-mode failover = %q (err %v)", opts.mode, err)
	}
	if buildConfig(opts).Mode != modeFailover {
		t.Fatal("config does not record the mode")
	}
	if _, err := parseFlags([]string{"-mode", "chaos"}); err == nil {
		t.Fatal("unknown mode accepted")
	}
}

func TestCurrentLeaderPicksHighestTermLeader(t *testing.T) {
	statuses := []raft.Status{
		{ID: 1, Role: raft.Leader, Term: 2}, // deposed but not yet aware
		{ID: 2, Role: raft.Follower, Term: 3},
		{ID: 3, Role: raft.Leader, Term: 3},
	}
	if index, err := currentLeader(statuses); err != nil || statuses[index].ID != 3 {
		t.Fatalf("currentLeader = %d (err %v), want node 3", index, err)
	}
	if _, err := currentLeader([]raft.Status{{ID: 1, Role: raft.Follower, Term: 4}}); err == nil {
		t.Fatal("currentLeader found a leader among followers")
	}
}

func TestFailoverTimingsSeparateCloseFromRecovery(t *testing.T) {
	stop := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	got := failoverTimings(stop, stop.Add(30*time.Millisecond), stop.Add(250*time.Millisecond), nil)
	if !got.OK || got.FailoverMs != 250 || got.CloseMs != 30 || got.AfterCloseMs != 220 {
		t.Fatalf("timings = %+v, want ok, 250 total, 30 close, 220 after close", got)
	}
	failed := failoverTimings(stop, stop, stop.Add(time.Second), errTest)
	if failed.OK || failed.Error == "" {
		t.Fatalf("failed probe = %+v, want not ok with an error", failed)
	}
}

type testError string

func (e testError) Error() string { return string(e) }

const errTest = testError("probe timed out")

func TestSummaryReportsFailoverOverSuccessfulProbes(t *testing.T) {
	runs := []RunResult{
		{Clients: 1, Valid: true, Failover: &FailoverResult{OK: true, FailoverMs: 300, AfterCloseMs: 290}},
		{Clients: 1, Valid: false, Failover: &FailoverResult{OK: true, FailoverMs: 200, AfterCloseMs: 195}},
		{Clients: 1, Valid: true, Failover: &FailoverResult{OK: false, FailoverMs: 9000}},
		{Clients: 4, Valid: true},
	}
	summary := summarize(runs)
	one := summary[0]
	if one.FailoverProbesOK != 2 || one.FailoverProbesFailed != 1 {
		t.Fatalf("probe counts = ok %d failed %d, want 2 and 1", one.FailoverProbesOK, one.FailoverProbesFailed)
	}
	if one.FailoverMs == nil || *one.FailoverMs != (Range{Median: 250, Min: 200, Max: 300}) {
		t.Fatalf("failover range = %+v, want median 250 over the two successful probes", one.FailoverMs)
	}
	if one.FailoverAfterCloseMs == nil || one.FailoverAfterCloseMs.Max != 290 {
		t.Fatalf("after-close range = %+v", one.FailoverAfterCloseMs)
	}
	if four := summary[1]; four.FailoverMs != nil || four.FailoverProbesOK != 0 {
		t.Fatalf("throughput-only runs gained failover fields: %+v", four)
	}
}

func TestFailoverModeStopsCurrentLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a three-node cluster")
	}
	opts := options{
		mode: modeFailover, warmup: 0, duration: 300 * time.Millisecond, repetitions: 1,
		snapshotThreshold: 1_000_000, tick: 20 * time.Millisecond, seed: 1, valueSize: 16,
		dataDir: t.TempDir(),
	}
	result, err := runOnce(opts, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f := result.Failover
	if f == nil || !f.OK {
		t.Fatalf("failover = %+v, want a successful probe", f)
	}
	if f.NewLeader == 0 || f.NewLeader == f.StoppedLeader || f.NewTerm <= f.StoppedTerm {
		t.Fatalf("failover = %+v, want a different leader at a higher term", f)
	}
	if f.FailoverMs < f.AfterCloseMs || f.FailoverMs <= 0 {
		t.Fatalf("failover = %+v, want total >= after-close > 0", f)
	}
}
