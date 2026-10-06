package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCounterDeltaRatios(t *testing.T) {
	ack := "lsmdb_raft_append_messages_total{origin=ack_resend}"
	heartbeat := "lsmdb_raft_append_messages_total{origin=heartbeat}"
	before := []map[string]float64{
		{"lsmdb_raft_log_syncs_total": 10, "lsmdb_raft_log_sync_entries_total": 10, ack: 5, heartbeat: 1, "lsmdb_raft_hardstate_syncs_total": 1},
		{"lsmdb_raft_log_syncs_total": 20, "lsmdb_raft_log_sync_entries_total": 20, ack: 0, heartbeat: 0},
	}
	after := []map[string]float64{
		{"lsmdb_raft_log_syncs_total": 110, "lsmdb_raft_log_sync_entries_total": 110, ack: 805, heartbeat: 21, "lsmdb_raft_hardstate_syncs_total": 1},
		{"lsmdb_raft_log_syncs_total": 220, "lsmdb_raft_log_sync_entries_total": 320, ack: 0, heartbeat: 0},
	}
	got := counterDeltas(before, after, 100)
	if got.LogSyncs != 300 || got.LogSyncEntries != 400 || got.HardStateSyncs != 0 {
		t.Fatalf("deltas = %+v", got)
	}
	if got.SyncsPerCommittedEntry != 3 || got.EntriesPerSync != 400.0/300.0 {
		t.Fatalf("sync ratios = %v per entry, %v entries per sync", got.SyncsPerCommittedEntry, got.EntriesPerSync)
	}
	if got.AppendMessages["ack_resend"] != 800 || got.AppendMessages["heartbeat"] != 20 || got.AppendMessagesTotal != 820 {
		t.Fatalf("append deltas = %v total %v", got.AppendMessages, got.AppendMessagesTotal)
	}
	if got.AppendPerCommittedEntry != 8.2 || got.AppendPerCommittedEntryByOrigin["ack_resend"] != 8 {
		t.Fatalf("append ratios = %v by origin %v", got.AppendPerCommittedEntry, got.AppendPerCommittedEntryByOrigin)
	}
}

func TestCounterDeltasWithNothingCommittedStayEncodable(t *testing.T) {
	got := counterDeltas([]map[string]float64{{}}, []map[string]float64{{}}, 0)
	if _, err := json.Marshal(got); err != nil {
		t.Fatalf("zero-commit counters are not JSON-encodable: %v", err)
	}
	if got.SyncsPerCommittedEntry != 0 || got.EntriesPerSync != 0 {
		t.Fatalf("ratios without data = %+v, want zero", got)
	}
}

func TestCounterDeltasCountSendFailuresInWindowOnly(t *testing.T) {
	deadline := "lsmdb_raft_transport_send_failures_total{class=deadline}"
	other := "lsmdb_raft_transport_send_failures_total{class=other}"
	// Failures before the window started (warmup, elections) must not be counted.
	before := []map[string]float64{{deadline: 40, other: 2}, {deadline: 0, other: 0}, {deadline: 5, other: 1}}
	after := []map[string]float64{{deadline: 47, other: 2}, {deadline: 3, other: 0}, {deadline: 5, other: 4}}
	got := counterDeltas(before, after, 100)
	if got.SendFailuresDeadline != 10 || got.SendFailuresOther != 3 {
		t.Fatalf("send failures in window = %v deadline, %v other; want 10 and 3", got.SendFailuresDeadline, got.SendFailuresOther)
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"send_failures_deadline":10`, `"send_failures_other":3`} {
		if !strings.Contains(string(data), field) {
			t.Errorf("counters JSON lacks %s: %s", field, data)
		}
	}
}

func TestCounterDeltasIncludeEngineFlushesAndCompactions(t *testing.T) {
	before := []map[string]float64{
		{"lsmdb_engine_flushes_total": 3, "lsmdb_engine_flush_seconds_total": 0.5, "lsmdb_engine_compactions_total": 1, "lsmdb_engine_compaction_seconds_total": 2},
		{"lsmdb_engine_flushes_total": 0, "lsmdb_engine_flush_seconds_total": 0, "lsmdb_engine_compactions_total": 0, "lsmdb_engine_compaction_seconds_total": 0},
	}
	after := []map[string]float64{
		{"lsmdb_engine_flushes_total": 5, "lsmdb_engine_flush_seconds_total": 0.75, "lsmdb_engine_compactions_total": 2, "lsmdb_engine_compaction_seconds_total": 3.5},
		{"lsmdb_engine_flushes_total": 4, "lsmdb_engine_flush_seconds_total": 1, "lsmdb_engine_compactions_total": 1, "lsmdb_engine_compaction_seconds_total": 0.5},
	}
	got := counterDeltas(before, after, 100)
	if got.Flushes != 6 || got.FlushSeconds != 1.25 || got.Compactions != 2 || got.CompactionSeconds != 2 {
		t.Fatalf("engine deltas = %v flushes in %v s, %v compactions in %v s; want 6, 1.25, 2, 2",
			got.Flushes, got.FlushSeconds, got.Compactions, got.CompactionSeconds)
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"flushes":6`, `"flush_seconds":1.25`, `"compactions":2`, `"compaction_seconds":2`} {
		if !strings.Contains(string(data), field) {
			t.Errorf("counters JSON lacks %s: %s", field, data)
		}
	}
}

func TestCounterDeltasSplitLogSyncsByRole(t *testing.T) {
	syncs := func(leader, follower float64) map[string]float64 {
		return map[string]float64{
			"lsmdb_raft_log_syncs_total":                        leader + follower,
			"lsmdb_raft_log_syncs_by_role_total{role=leader}":   leader,
			"lsmdb_raft_log_syncs_by_role_total{role=follower}": follower,
			// Entries are four per leader sync and two per follower sync.
			"lsmdb_raft_log_sync_entries_total":                        4*leader + 2*follower,
			"lsmdb_raft_log_sync_entries_by_role_total{role=leader}":   4 * leader,
			"lsmdb_raft_log_sync_entries_by_role_total{role=follower}": 2 * follower,
		}
	}
	// Node 1 led before the window and node 2 during it.
	before := []map[string]float64{syncs(7, 3), syncs(0, 9), syncs(0, 9)}
	after := []map[string]float64{syncs(7, 13), syncs(50, 9), syncs(0, 59)}
	got := counterDeltas(before, after, 200)
	if got.LeaderLogSyncs != 50 || got.FollowerLogSyncs != 60 || got.LeaderLogSyncEntries != 200 || got.FollowerLogSyncEntries != 120 {
		t.Fatalf("split = leader %v syncs %v entries, follower %v syncs %v entries; want 50, 200, 60, 120",
			got.LeaderLogSyncs, got.LeaderLogSyncEntries, got.FollowerLogSyncs, got.FollowerLogSyncEntries)
	}
	if got.LeaderLogSyncs+got.FollowerLogSyncs != got.LogSyncs || got.LeaderLogSyncEntries+got.FollowerLogSyncEntries != got.LogSyncEntries {
		t.Fatalf("leader + follower = %v syncs, %v entries; want the totals %v and %v",
			got.LeaderLogSyncs+got.FollowerLogSyncs, got.LeaderLogSyncEntries+got.FollowerLogSyncEntries, got.LogSyncs, got.LogSyncEntries)
	}
	if got.LeaderEntriesPerSync != 4 || got.FollowerEntriesPerSync != 2 {
		t.Fatalf("entries per sync = %v leader, %v follower; want 4 and 2", got.LeaderEntriesPerSync, got.FollowerEntriesPerSync)
	}
	data, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"leader_log_syncs":50`, `"leader_log_sync_entries":200`, `"follower_log_syncs":60`, `"follower_log_sync_entries":120`,
		`"leader_entries_per_log_sync":4`, `"follower_entries_per_log_sync":2`,
	} {
		if !strings.Contains(string(data), field) {
			t.Errorf("counters JSON lacks %s: %s", field, data)
		}
	}
}
