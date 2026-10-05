package main

import (
	"errors"
	"os"
	"time"

	"lsmdb/internal/benchenv"
)

const (
	schemaVersion     = 1
	electionTickMin   = 5
	electionTickMax   = 10
	heartbeatTicks    = 1
	checkQuorumTicks  = 5
	fsyncProbeSamples = 200
	clusterNodes      = 3
	seedScope         = "workload key sequence only; Raft election timing is seeded by node ID and identical across repetitions"
)

// options holds parsed command-line flags.
type options struct {
	clients           []int
	warmup            time.Duration
	duration          time.Duration
	repetitions       int
	snapshotThreshold uint64
	tick              time.Duration
	seed              int64
	valueSize         int
	official          bool
	out               string
	dataDir           string
}

// Config is the reproducible run configuration recorded in the report. It holds
// no paths, so equal flags always produce an identical Config.
type Config struct {
	Clients           []int   `json:"clients"`
	WarmupSeconds     float64 `json:"warmup_seconds"`
	DurationSeconds   float64 `json:"duration_seconds"`
	Repetitions       int     `json:"repetitions"`
	SnapshotThreshold uint64  `json:"snapshot_threshold"`
	TickMs            float64 `json:"tick_ms"`
	ElectionTickMin   int     `json:"election_tick_min"`
	ElectionTickMax   int     `json:"election_tick_max"`
	HeartbeatTicks    int     `json:"heartbeat_ticks"`
	CheckQuorumTicks  int     `json:"check_quorum_ticks"`
	Nodes             int     `json:"nodes"`
	Seed              int64   `json:"seed"`
	SeedScope         string  `json:"seed_scope"`
	ValueSize         int     `json:"value_size"`
	FsyncProbeSamples int     `json:"fsync_probe_samples"`
	Official          bool    `json:"official"`
}

func buildConfig(opts options) Config {
	return Config{
		Clients: append([]int(nil), opts.clients...), WarmupSeconds: opts.warmup.Seconds(),
		DurationSeconds: opts.duration.Seconds(), Repetitions: opts.repetitions,
		SnapshotThreshold: opts.snapshotThreshold, TickMs: float64(opts.tick.Microseconds()) / 1000,
		ElectionTickMin: electionTickMin, ElectionTickMax: electionTickMax,
		HeartbeatTicks: heartbeatTicks, CheckQuorumTicks: checkQuorumTicks, Nodes: clusterNodes,
		Seed: opts.seed, SeedScope: seedScope, ValueSize: opts.valueSize,
		FsyncProbeSamples: fsyncProbeSamples, Official: opts.official,
	}
}

// RunResult is one fresh cluster measured at one client count.
type RunResult struct {
	Repetition        int                 `json:"repetition"`
	Clients           int                 `json:"clients"`
	Valid             bool                `json:"valid"`
	InvalidReason     string              `json:"invalid_reason,omitempty"`
	WindowSeconds     float64             `json:"window_seconds"`
	OpsOK             int                 `json:"ops_ok"`
	OpsFailed         int                 `json:"ops_failed"`
	Retries           uint64              `json:"retries"`
	Throughput        float64             `json:"throughput_ops_per_sec"`
	Latency           LatencyMs           `json:"latency_ms"`
	TermsStart        []uint64            `json:"terms_start"`
	TermsEnd          []uint64            `json:"terms_end"`
	ElectionsInWindow uint64              `json:"elections_in_window"`
	CommittedEntries  uint64              `json:"committed_entries"`
	FsyncBefore       benchenv.FsyncStats `json:"fsync_before"`
	FsyncAfter        benchenv.FsyncStats `json:"fsync_after"`
	Counters          Counters            `json:"counters"`
}

// Report is the complete JSON output of one clusterbench invocation.
type Report struct {
	SchemaVersion int                  `json:"schema_version"`
	Label         string               `json:"label"`
	Environment   benchenv.Environment `json:"environment"`
	Config        Config               `json:"config"`
	Limitations   []string             `json:"limitations"`
	Runs          []RunResult          `json:"runs"`
	Summary       []ClientSummary      `json:"summary"`
}

var limitations = []string{
	"Repetitions share Raft election timing (seeded by node ID), so run-to-run variance may be understated.",
	"Snapshot cost is excluded: the snapshot threshold keeps snapshots out of the window; it is measured separately.",
	"All three nodes and the clients share one process, CPU, and disk; fsyncs from different nodes contend.",
	"Clients are closed-loop, so latency under overload is subject to coordinated omission.",
}

// runLabel marks a run official only on Linux with an explicit request.
func runLabel(goos string, official bool) (string, error) {
	if !official {
		return "secondary", nil
	}
	if goos != "linux" {
		return "", errors.New("-official runs require Linux; runs on other systems are secondary smoke runs")
	}
	return "official", nil
}

// defaultOutputDir keeps secondary runs out of the repository.
func defaultOutputDir(label string) string {
	if label == "official" {
		return "benchmarks/results"
	}
	return os.TempDir()
}
