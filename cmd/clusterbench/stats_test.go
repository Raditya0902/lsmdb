package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

var windowStart = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func at(offset time.Duration) time.Time { return windowStart.Add(offset) }

func TestWindowExcludesOpsOutsideWindow(t *testing.T) {
	end := at(10 * time.Second)
	ops := []opRecord{
		{done: at(-time.Millisecond), latency: 1 * time.Millisecond, ok: true, attempts: 1}, // before start
		{done: at(0), latency: 2 * time.Millisecond, ok: true, attempts: 1},                 // at start: included
		{done: at(5 * time.Second), latency: 3 * time.Millisecond, ok: true, attempts: 1},   // inside
		{done: end, latency: 4 * time.Millisecond, ok: true, attempts: 1},                   // at end: excluded
		{done: at(11 * time.Second), latency: 5 * time.Millisecond, ok: true, attempts: 1},  // after
	}
	got := collectWindow(ops, windowStart, end)
	if got.OK != 2 || got.Failed != 0 {
		t.Fatalf("counts = ok %d failed %d, want 2 and 0", got.OK, got.Failed)
	}
	want := []time.Duration{2 * time.Millisecond, 3 * time.Millisecond}
	if len(got.Latencies) != len(want) || got.Latencies[0] != want[0] || got.Latencies[1] != want[1] {
		t.Fatalf("latencies = %v, want %v", got.Latencies, want)
	}
}

func TestFailedOpsCountedButNotInLatency(t *testing.T) {
	ops := []opRecord{
		{done: at(time.Second), latency: 2 * time.Millisecond, ok: true, attempts: 1},
		{done: at(2 * time.Second), latency: 900 * time.Millisecond, ok: false, attempts: 1},
		{done: at(20 * time.Second), latency: 900 * time.Millisecond, ok: false, attempts: 1}, // outside
	}
	got := collectWindow(ops, windowStart, at(10*time.Second))
	if got.OK != 1 || got.Failed != 1 {
		t.Fatalf("counts = ok %d failed %d, want 1 and 1", got.OK, got.Failed)
	}
	if len(got.Latencies) != 1 || got.Latencies[0] != 2*time.Millisecond {
		t.Fatalf("latencies = %v, want only the successful op", got.Latencies)
	}
	if summary := latencySummary(got.Latencies); summary.Max != 2 {
		t.Fatalf("max = %v ms, want 2 (failed op must not count)", summary.Max)
	}
}

func TestRetriesAreAttemptsMinusCalls(t *testing.T) {
	ops := []opRecord{
		{done: at(time.Second), ok: true, attempts: 1},
		{done: at(2 * time.Second), ok: true, attempts: 3},
		{done: at(3 * time.Second), ok: false, attempts: 2},
		{done: at(30 * time.Second), ok: true, attempts: 9}, // outside the window
	}
	if got := collectWindow(ops, windowStart, at(10*time.Second)).Retries; got != 3 {
		t.Fatalf("retries = %d, want 3", got)
	}
}

func TestTermChangeMarksRunInvalid(t *testing.T) {
	cases := []struct {
		name       string
		start, end []uint64
		valid      bool
	}{
		{"stable", []uint64{2, 2, 2}, []uint64{2, 2, 2}, true},
		{"one node moved", []uint64{2, 2, 2}, []uint64{2, 3, 2}, false},
		{"all moved", []uint64{2, 2, 2}, []uint64{4, 4, 4}, false},
		{"missing node", []uint64{2, 2, 2}, []uint64{2, 2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valid, reason := validity(tc.start, tc.end)
			if valid != tc.valid {
				t.Fatalf("valid = %v (%q), want %v", valid, reason, tc.valid)
			}
			if !valid && reason == "" {
				t.Fatal("invalid run has no reason")
			}
		})
	}
}

func TestInvalidRunsExcludedFromSummary(t *testing.T) {
	runs := []RunResult{
		{Clients: 1, Valid: true, Throughput: 10, Latency: LatencyMs{P99: 5}},
		{Clients: 1, Valid: true, Throughput: 20, Latency: LatencyMs{P99: 7}},
		{Clients: 1, Valid: false, Throughput: 1000, Latency: LatencyMs{P99: 1}},
		{Clients: 4, Valid: false, Throughput: 30},
	}
	summary := summarize(runs)
	if len(summary) != 2 {
		t.Fatalf("summary = %+v, want one entry per client count", summary)
	}
	one := summary[0]
	if one.Clients != 1 || one.ValidRuns != 2 || one.InvalidRuns != 1 {
		t.Fatalf("clients=1 summary = %+v", one)
	}
	if one.Throughput.Median != 15 || one.Throughput.Min != 10 || one.Throughput.Max != 20 || one.P99Ms.Max != 7 {
		t.Fatalf("clients=1 statistics include the invalid run: %+v", one)
	}
	if four := summary[1]; four.ValidRuns != 0 || four.InvalidRuns != 1 || four.Throughput.Max != 0 {
		t.Fatalf("clients=4 summary = %+v, want no statistics", four)
	}
}

func TestSameSeedProducesSameConfig(t *testing.T) {
	opts := options{
		clients: []int{1, 4}, warmup: 5 * time.Second, duration: 30 * time.Second,
		repetitions: 5, snapshotThreshold: 1_000_000, tick: 20 * time.Millisecond,
		seed: 7, valueSize: 128,
	}
	first, err := json.Marshal(buildConfig(opts))
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(buildConfig(opts))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("same flags produced different config:\n%s\n%s", first, second)
	}
	if !strings.Contains(string(first), `"seed_scope"`) {
		t.Fatalf("config does not state the seed scope: %s", first)
	}
	opts.seed = 8
	third, _ := json.Marshal(buildConfig(opts))
	if bytes.Equal(first, third) {
		t.Fatal("a different seed produced an identical config")
	}
	if keyFor(0, newKeyRand(7, 0)) != keyFor(0, newKeyRand(7, 0)) || keyFor(0, newKeyRand(7, 0)) == keyFor(0, newKeyRand(8, 0)) {
		t.Fatal("key sequence does not follow the seed")
	}
}

func TestOfficialRequiresLinux(t *testing.T) {
	cases := []struct {
		goos     string
		official bool
		label    string
		wantErr  bool
	}{
		{"linux", true, "official", false},
		{"linux", false, "secondary", false},
		{"darwin", false, "secondary", false},
		{"darwin", true, "", true},
	}
	for _, tc := range cases {
		label, err := runLabel(tc.goos, tc.official)
		if (err != nil) != tc.wantErr || label != tc.label {
			t.Errorf("runLabel(%s, %v) = (%q, %v), want (%q, error=%v)", tc.goos, tc.official, label, err, tc.label, tc.wantErr)
		}
	}
	if dir := defaultOutputDir("secondary"); dir != os.TempDir() {
		t.Errorf("secondary output dir = %q, want the OS temp dir", dir)
	}
	if dir := defaultOutputDir("official"); dir != "benchmarks/results" {
		t.Errorf("official output dir = %q, want benchmarks/results", dir)
	}
}
