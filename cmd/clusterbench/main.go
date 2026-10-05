// Command clusterbench measures replicated write throughput and latency on
// fresh in-process three-node clusters and writes a JSON report.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"lsmdb/internal/benchenv"
)

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (options, error) {
	flags := flag.NewFlagSet("clusterbench", flag.ContinueOnError)
	clients := flags.String("clients", "1,2,4,8,16", "comma-separated client counts; each runs on a fresh cluster")
	opts := options{}
	flags.DurationVar(&opts.warmup, "warmup", 5*time.Second, "time clients run before the measurement window")
	flags.DurationVar(&opts.duration, "duration", 30*time.Second, "measurement window length")
	flags.IntVar(&opts.repetitions, "repetitions", 5, "fresh-cluster repetitions per client count")
	flags.Uint64Var(&opts.snapshotThreshold, "snapshot-threshold", 1_000_000, "applied entries between Raft snapshots; high by default to keep snapshot cost out of the window")
	flags.DurationVar(&opts.tick, "tick", 20*time.Millisecond, "Raft tick interval")
	flags.Int64Var(&opts.seed, "seed", 1, "seed for the workload key sequence only; Raft election timing is seeded by node ID and does not vary")
	flags.IntVar(&opts.valueSize, "value-size", 128, "value size in bytes")
	flags.BoolVar(&opts.official, "official", false, "label the run official (Linux only); otherwise it is a secondary smoke run")
	flags.StringVar(&opts.out, "out", "", "result file to create (never overwritten); default <dir>/<date>-<sha>-cluster.json in benchmarks/results for official runs, the OS temp dir otherwise")
	flags.StringVar(&opts.dataDir, "data-dir", "", "parent directory for node data; default the OS temp dir")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	for _, field := range strings.Split(*clients, ",") {
		count, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || count <= 0 {
			return options{}, fmt.Errorf("invalid -clients entry %q", field)
		}
		opts.clients = append(opts.clients, count)
	}
	if opts.warmup < 0 || opts.duration <= 0 || opts.repetitions <= 0 || opts.tick <= 0 || opts.valueSize < 0 {
		return options{}, errors.New("-duration, -repetitions and -tick must be positive; -warmup and -value-size non-negative")
	}
	if opts.dataDir == "" {
		opts.dataDir = os.TempDir()
	}
	return opts, nil
}

func run(opts options) error {
	label, err := benchenv.RunLabel(runtime.GOOS, opts.official)
	if err != nil {
		return err
	}
	report := Report{
		SchemaVersion: schemaVersion, Label: label, Environment: benchenv.Collect(opts.dataDir),
		Config: buildConfig(opts), Limitations: limitations,
	}
	out, err := createOutput(opts.out, label, report.Environment.GitSHA)
	if err != nil {
		return err
	}
	defer out.Close()
	fmt.Fprintf(os.Stderr, "clusterbench (%s): writing %s\n", label, out.Name())

	for repetition := 1; repetition <= opts.repetitions; repetition++ {
		for _, clients := range opts.clients {
			result, err := runOnce(opts, clients, repetition)
			if err != nil {
				return fmt.Errorf("repetition %d, %d clients: %w", repetition, clients, err)
			}
			report.Runs = append(report.Runs, result)
			status := "valid"
			if !result.Valid {
				status = "INVALID: " + result.InvalidReason
			}
			fmt.Fprintf(os.Stderr, "rep %d clients %d: %.1f ops/s p50 %.2f ms p99 %.2f ms failed %d (%s)\n",
				repetition, clients, result.Throughput, result.Latency.P50, result.Latency.P99, result.OpsFailed, status)
		}
	}
	report.Summary = summarize(report.Runs)
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

// createOutput opens the result file exclusively so no earlier result is lost.
func createOutput(path, label, sha string) (*os.File, error) {
	if path == "" {
		return benchenv.CreateResultFile(benchenv.DefaultOutputDir(label), "cluster", sha, time.Now())
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create -out file: %w", err)
	}
	return f, nil
}
