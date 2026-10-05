package main

import (
	"encoding/json"
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
