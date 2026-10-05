package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"lsmdb/benchmarks"
	"lsmdb/internal/benchcompare"
)

func TestBenchDefaultOutputIsNotResultsJSON(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	cwd := t.TempDir()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })

	f, err := createOutput("", "secondary", "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if dir := filepath.Dir(f.Name()); dir != filepath.Clean(os.TempDir()) {
		t.Errorf("secondary result written to %q, want the OS temp dir %q", dir, os.TempDir())
	}
	if name := filepath.Base(f.Name()); name == "results.json" || !strings.HasSuffix(name, "-embedded.json") {
		t.Errorf("default result file = %q, want <date>-<sha>-embedded.json", name)
	}
	if _, err := os.Stat(filepath.Join(cwd, "results.json")); !os.IsNotExist(err) {
		t.Errorf("results.json was created in the working directory (stat err %v)", err)
	}

	existing := filepath.Join(cwd, "results.json")
	if err := os.WriteFile(existing, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := createOutput(existing, "secondary", "0123456789abcdef"); err == nil {
		t.Error("createOutput overwrote an existing -out file")
	}
	if data, _ := os.ReadFile(existing); string(data) != "keep" {
		t.Errorf("existing file changed to %q", data)
	}
}

func TestRepetitionSummaryMinMedianMax(t *testing.T) {
	result := func(workload, engine string, ops, p50, p99, p999 float64) benchmarks.WorkloadResult {
		return benchmarks.WorkloadResult{Workload: workload, Engine: engine, OpsPerSec: ops, P50Ms: p50, P99Ms: p99, P999Ms: p999}
	}
	runs := []Repetition{
		{Repetition: 1, Results: []benchmarks.WorkloadResult{result("A", "LSM", 300, 1, 9, 20), result("A", "SQLite", 50, 2, 5, 6)}},
		{Repetition: 2, Results: []benchmarks.WorkloadResult{result("A", "LSM", 100, 3, 7, 30), result("A", "SQLite", 70, 2, 5, 6)}},
		{Repetition: 3, Results: []benchmarks.WorkloadResult{result("A", "LSM", 200, 2, 8, 10), result("A", "SQLite", 60, 2, 5, 6)}},
	}
	got := summarizeWorkloads(runs)
	if len(got) != 2 || got[0].Engine != "LSM" || got[1].Engine != "SQLite" {
		t.Fatalf("summary = %+v, want LSM then SQLite in first-seen order", got)
	}
	lsm := got[0]
	if lsm.Repetitions != 3 {
		t.Errorf("repetitions = %d, want 3", lsm.Repetitions)
	}
	checks := map[string][2]benchcompare.Range{
		"ops/s": {lsm.OpsPerSec, {Median: 200, Min: 100, Max: 300}},
		"p50":   {lsm.P50Ms, {Median: 2, Min: 1, Max: 3}},
		"p99":   {lsm.P99Ms, {Median: 8, Min: 7, Max: 9}},
		"p99.9": {lsm.P999Ms, {Median: 20, Min: 10, Max: 30}},
	}
	for name, pair := range checks {
		if pair[0] != pair[1] {
			t.Errorf("LSM %s = %+v, want %+v", name, pair[0], pair[1])
		}
	}
}
