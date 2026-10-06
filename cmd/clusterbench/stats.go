package main

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	"lsmdb/internal/benchenv"
)

// opRecord is one completed client Put.
type opRecord struct {
	done     time.Time
	latency  time.Duration
	ok       bool
	attempts uint64
}

// windowStats aggregates the ops that completed inside the measurement window.
type windowStats struct {
	OK        int
	Failed    int
	Retries   uint64
	Latencies []time.Duration // successful ops only, ascending
}

// collectWindow keeps ops that completed in [start, end). Failed ops are
// counted but never enter the latency set; retries are attempts beyond the first.
func collectWindow(ops []opRecord, start, end time.Time) windowStats {
	var stats windowStats
	for _, op := range ops {
		if op.done.Before(start) || !op.done.Before(end) {
			continue
		}
		if op.attempts > 1 {
			stats.Retries += op.attempts - 1
		}
		if !op.ok {
			stats.Failed++
			continue
		}
		stats.OK++
		stats.Latencies = append(stats.Latencies, op.latency)
	}
	benchenv.SortDurations(stats.Latencies)
	return stats
}

// LatencyMs reports nearest-rank percentiles in milliseconds.
type LatencyMs struct {
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p99_9"`
	Max  float64 `json:"max"`
}

func latencySummary(sorted []time.Duration) LatencyMs {
	at := func(q float64) float64 { return benchenv.Milliseconds(benchenv.Percentile(sorted, q)) }
	return LatencyMs{P50: at(0.50), P95: at(0.95), P99: at(0.99), P999: at(0.999), Max: at(1.0)}
}

// validity rejects a run when any node's term changed across the window,
// which means an election happened while it was being measured.
func validity(termsStart, termsEnd []uint64) (bool, string) {
	if len(termsStart) != len(termsEnd) {
		return false, fmt.Sprintf("read %d terms at window start and %d at end", len(termsStart), len(termsEnd))
	}
	for i := range termsStart {
		if termsStart[i] != termsEnd[i] {
			return false, fmt.Sprintf("node %d term changed from %d to %d during the window", i+1, termsStart[i], termsEnd[i])
		}
	}
	return true, ""
}

// Range is the median and extremes of one statistic across valid runs.
type Range struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

func describe(values []float64) Range {
	if len(values) == 0 {
		return Range{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	median := sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = (sorted[len(sorted)/2-1] + sorted[len(sorted)/2]) / 2
	}
	return Range{Median: median, Min: sorted[0], Max: sorted[len(sorted)-1]}
}

// ClientSummary describes all runs at one client count. Statistics use valid
// runs only; invalid runs are only counted.
type ClientSummary struct {
	Clients     int   `json:"clients"`
	ValidRuns   int   `json:"valid_runs"`
	InvalidRuns int   `json:"invalid_runs"`
	Throughput  Range `json:"throughput_ops_per_sec"`
	P50Ms       Range `json:"p50_ms"`
	P99Ms       Range `json:"p99_ms"`
	// Failover fields are set only in failover mode. They cover every run whose
	// probe succeeded, valid or not, because the stop happens after the window.
	FailoverProbesOK     int    `json:"failover_probes_ok,omitempty"`
	FailoverProbesFailed int    `json:"failover_probes_failed,omitempty"`
	FailoverMs           *Range `json:"failover_ms,omitempty"`
	FailoverAfterCloseMs *Range `json:"failover_after_close_ms,omitempty"`
}

func summarize(runs []RunResult) []ClientSummary {
	byClients := map[int]*ClientSummary{}
	values := map[int][3][]float64{}
	failovers := map[int][2][]float64{}
	for _, run := range runs {
		summary := byClients[run.Clients]
		if summary == nil {
			summary = &ClientSummary{Clients: run.Clients}
			byClients[run.Clients] = summary
		}
		if f := run.Failover; f != nil && f.OK {
			summary.FailoverProbesOK++
			v := failovers[run.Clients]
			v[0], v[1] = append(v[0], f.FailoverMs), append(v[1], f.AfterCloseMs)
			failovers[run.Clients] = v
		} else if f != nil {
			summary.FailoverProbesFailed++
		}
		if !run.Valid {
			summary.InvalidRuns++
			continue
		}
		summary.ValidRuns++
		v := values[run.Clients]
		v[0] = append(v[0], run.Throughput)
		v[1] = append(v[1], run.Latency.P50)
		v[2] = append(v[2], run.Latency.P99)
		values[run.Clients] = v
	}
	out := make([]ClientSummary, 0, len(byClients))
	for clients, summary := range byClients {
		v := values[clients]
		summary.Throughput, summary.P50Ms, summary.P99Ms = describe(v[0]), describe(v[1]), describe(v[2])
		if f, ok := failovers[clients]; ok {
			total, afterClose := describe(f[0]), describe(f[1])
			summary.FailoverMs, summary.FailoverAfterCloseMs = &total, &afterClose
		}
		out = append(out, *summary)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Clients < out[j].Clients })
	return out
}

func newKeyRand(seed int64, client int) *rand.Rand {
	return rand.New(rand.NewSource(seed*1_000_003 + int64(client)))
}

func keyFor(client int, rng *rand.Rand) string {
	return fmt.Sprintf("c%02d-%09d", client, rng.Int63n(1_000_000_000))
}
