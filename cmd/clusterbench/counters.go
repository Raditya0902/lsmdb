package main

import "strings"

const (
	appendMetricPrefix      = "lsmdb_raft_append_messages_total{origin="
	sendFailureMetricPrefix = "lsmdb_raft_transport_send_failures_total{class="
	roleSyncsMetricPrefix   = "lsmdb_raft_log_syncs_by_role_total{role="
	roleEntriesMetricPrefix = "lsmdb_raft_log_sync_entries_by_role_total{role="
)

// Counters holds replication counter deltas over the measurement window,
// summed across the three nodes, and ratios per committed entry. The leader
// and follower log sync fields split the totals; each pair sums to its total.
type Counters struct {
	LogSyncs                        float64            `json:"log_syncs"`
	LogSyncEntries                  float64            `json:"log_sync_entries"`
	LeaderLogSyncs                  float64            `json:"leader_log_syncs"`
	LeaderLogSyncEntries            float64            `json:"leader_log_sync_entries"`
	FollowerLogSyncs                float64            `json:"follower_log_syncs"`
	FollowerLogSyncEntries          float64            `json:"follower_log_sync_entries"`
	HardStateSyncs                  float64            `json:"hardstate_syncs"`
	AppendMessagesTotal             float64            `json:"append_messages_total"`
	AppendMessages                  map[string]float64 `json:"append_messages_by_origin"`
	ApplySeconds                    float64            `json:"apply_seconds"`
	ApplyCount                      float64            `json:"apply_count"`
	SnapshotSeconds                 float64            `json:"snapshot_seconds"`
	SnapshotCount                   float64            `json:"snapshot_count"`
	SyncsPerCommittedEntry          float64            `json:"log_syncs_per_committed_entry"`
	EntriesPerSync                  float64            `json:"entries_per_log_sync"`
	LeaderEntriesPerSync            float64            `json:"leader_entries_per_log_sync"`
	FollowerEntriesPerSync          float64            `json:"follower_entries_per_log_sync"`
	AppendPerCommittedEntry         float64            `json:"append_messages_per_committed_entry"`
	AppendPerCommittedEntryByOrigin map[string]float64 `json:"append_messages_per_committed_entry_by_origin"`
	// SendFailuresDeadline counts outbound Raft sends whose deadline expired;
	// SendFailuresOther counts every other failed send.
	SendFailuresDeadline float64 `json:"send_failures_deadline"`
	SendFailuresOther    float64 `json:"send_failures_other"`
	// Engine flushes and compactions in the window. Flush time excludes the
	// compactions flushes trigger; apply time includes both.
	Flushes           float64 `json:"flushes"`
	FlushSeconds      float64 `json:"flush_seconds"`
	Compactions       float64 `json:"compactions"`
	CompactionSeconds float64 `json:"compaction_seconds"`
}

// counterDeltas subtracts per-node registry snapshots taken at window start
// from those taken at window end. Ratios are zero when their denominator is.
func counterDeltas(before, after []map[string]float64, committed uint64) Counters {
	delta := func(name string) float64 {
		var total float64
		for i := range after {
			if i < len(before) {
				total += after[i][name] - before[i][name]
			}
		}
		return total
	}
	counters := Counters{
		LogSyncs: delta("lsmdb_raft_log_syncs_total"), LogSyncEntries: delta("lsmdb_raft_log_sync_entries_total"),
		LeaderLogSyncs: delta(roleSyncsMetricPrefix + "leader}"), LeaderLogSyncEntries: delta(roleEntriesMetricPrefix + "leader}"),
		FollowerLogSyncs: delta(roleSyncsMetricPrefix + "follower}"), FollowerLogSyncEntries: delta(roleEntriesMetricPrefix + "follower}"),
		HardStateSyncs: delta("lsmdb_raft_hardstate_syncs_total"),
		ApplySeconds:   delta("lsmdb_raft_apply_seconds_sum"), ApplyCount: delta("lsmdb_raft_apply_seconds_count"),
		SnapshotSeconds: delta("lsmdb_raft_snapshot_seconds_sum"), SnapshotCount: delta("lsmdb_raft_snapshot_seconds_count"),
		AppendMessages: map[string]float64{}, AppendPerCommittedEntryByOrigin: map[string]float64{},
		SendFailuresDeadline: delta(sendFailureMetricPrefix + "deadline}"),
		SendFailuresOther:    delta(sendFailureMetricPrefix + "other}"),
		Flushes:              delta("lsmdb_engine_flushes_total"),
		FlushSeconds:         delta("lsmdb_engine_flush_seconds_total"),
		Compactions:          delta("lsmdb_engine_compactions_total"),
		CompactionSeconds:    delta("lsmdb_engine_compaction_seconds_total"),
	}
	origins := map[string]bool{}
	for _, values := range after {
		for name := range values {
			if strings.HasPrefix(name, appendMetricPrefix) {
				origins[strings.TrimSuffix(strings.TrimPrefix(name, appendMetricPrefix), "}")] = true
			}
		}
	}
	for origin := range origins {
		count := delta(appendMetricPrefix + origin + "}")
		counters.AppendMessages[origin] = count
		counters.AppendMessagesTotal += count
	}
	ratio := func(numerator, denominator float64) float64 {
		if denominator == 0 {
			return 0
		}
		return numerator / denominator
	}
	entries := float64(committed)
	counters.SyncsPerCommittedEntry = ratio(counters.LogSyncs, entries)
	counters.EntriesPerSync = ratio(counters.LogSyncEntries, counters.LogSyncs)
	counters.LeaderEntriesPerSync = ratio(counters.LeaderLogSyncEntries, counters.LeaderLogSyncs)
	counters.FollowerEntriesPerSync = ratio(counters.FollowerLogSyncEntries, counters.FollowerLogSyncs)
	counters.AppendPerCommittedEntry = ratio(counters.AppendMessagesTotal, entries)
	for origin, count := range counters.AppendMessages {
		counters.AppendPerCommittedEntryByOrigin[origin] = ratio(count, entries)
	}
	return counters
}
