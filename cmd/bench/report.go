package main

import (
	"fmt"
	"os"
	"time"

	"lsmdb/benchmarks"
	"lsmdb/internal/benchcompare"
	"lsmdb/internal/benchenv"
)

const schemaVersion = 1

// Repetition holds every workload result from one pass over both engines.
type Repetition struct {
	Repetition int                         `json:"repetition"`
	Results    []benchmarks.WorkloadResult `json:"results"`
}

// WorkloadSummary is the median and extremes of one workload on one engine
// across repetitions.
type WorkloadSummary struct {
	Workload    string             `json:"workload"`
	Engine      string             `json:"engine"`
	Repetitions int                `json:"repetitions"`
	OpsPerSec   benchcompare.Range `json:"ops_per_sec"`
	P50Ms       benchcompare.Range `json:"p50_ms"`
	P99Ms       benchcompare.Range `json:"p99_ms"`
	P999Ms      benchcompare.Range `json:"p99_9_ms"`
}

// Config records the run parameters. Workload keys, values, counts and seeds
// are fixed in package benchmarks and identical on every run.
type Config struct {
	Repetitions int  `json:"repetitions"`
	Official    bool `json:"official"`
}

// Report is the complete JSON output of one cmd/bench invocation.
type Report struct {
	SchemaVersion int                  `json:"schema_version"`
	Label         string               `json:"label"`
	Environment   benchenv.Environment `json:"environment"`
	Config        Config               `json:"config"`
	Limitations   []string             `json:"limitations"`
	Runs          []Repetition         `json:"runs"`
	Summary       []WorkloadSummary    `json:"summary"`
}

var limitations = []string{
	"Throughput is ops divided by the wall time of each workload's timed section; pre-population is excluded.",
	"Repetitions run sequentially in one process; each workload gets a fresh database, but the OS page cache and Go heap carry over.",
	"The engines do not sync the same way: SQLite runs with synchronous=NORMAL in WAL mode (see benchmarks/bench_sqlite.go).",
	"All workloads except G are single-threaded.",
}

// summarizeWorkloads groups results by workload and engine in first-seen order.
func summarizeWorkloads(runs []Repetition) []WorkloadSummary {
	type key struct{ workload, engine string }
	var order []key
	values := map[key][4][]float64{}
	for _, run := range runs {
		for _, r := range run.Results {
			k := key{r.Workload, r.Engine}
			v, seen := values[k]
			if !seen {
				order = append(order, k)
			}
			v[0], v[1] = append(v[0], r.OpsPerSec), append(v[1], r.P50Ms)
			v[2], v[3] = append(v[2], r.P99Ms), append(v[3], r.P999Ms)
			values[k] = v
		}
	}
	out := make([]WorkloadSummary, 0, len(order))
	for _, k := range order {
		v := values[k]
		out = append(out, WorkloadSummary{
			Workload: k.workload, Engine: k.engine, Repetitions: len(v[0]),
			OpsPerSec: benchcompare.Describe(v[0]), P50Ms: benchcompare.Describe(v[1]),
			P99Ms: benchcompare.Describe(v[2]), P999Ms: benchcompare.Describe(v[3]),
		})
	}
	return out
}

// createOutput opens the result file exclusively so no earlier result,
// including the committed results.json, is ever overwritten.
func createOutput(path, label, sha string) (*os.File, error) {
	if path == "" {
		return benchenv.CreateResultFile(benchenv.DefaultOutputDir(label), "embedded", sha, time.Now())
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create -out file: %w", err)
	}
	return f, nil
}
