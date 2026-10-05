// cmd/bench runs every workload against both the LSM engine and SQLite for a
// number of repetitions, prints a min-median-max table, and writes a JSON
// report. It never writes results.json unless that path is passed with -out
// and does not exist yet.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"text/tabwriter"

	"lsmdb/benchmarks"
	"lsmdb/internal/benchenv"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("bench", flag.ContinueOnError)
	repetitions := flags.Int("repetitions", 3, "passes over every workload; each workload gets a fresh database")
	official := flags.Bool("official", false, "label the run official (Linux only); otherwise it is a secondary smoke run")
	outFile := flags.String("out", "", "result file to create (never overwritten); default <dir>/<date>-<sha>-embedded.json in benchmarks/results for official runs, the OS temp dir otherwise")
	dataDir := flags.String("data-dir", "", "parent directory for databases; default the OS temp dir")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *repetitions <= 0 {
		return errors.New("-repetitions must be positive")
	}
	label, err := benchenv.RunLabel(runtime.GOOS, *official)
	if err != nil {
		return err
	}

	dir, err := os.MkdirTemp(*dataDir, "lsmdb-bench-*")
	if err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}
	defer os.RemoveAll(dir)

	report := Report{
		SchemaVersion: schemaVersion, Label: label, Environment: benchenv.Collect(dir),
		Config: Config{Repetitions: *repetitions, Official: *official}, Limitations: limitations,
	}
	out, err := createOutput(*outFile, label, report.Environment.GitSHA)
	if err != nil {
		return err
	}
	defer out.Close()
	fmt.Fprintf(os.Stderr, "bench (%s): writing %s\n", label, out.Name())

	for rep := 1; rep <= *repetitions; rep++ {
		fmt.Fprintf(os.Stderr, "repetition %d/%d: LSM workloads\n", rep, *repetitions)
		lsm, err := benchmarks.RunLSM(filepath.Join(dir, "run"))
		if err != nil {
			return fmt.Errorf("LSM: %w", err)
		}
		fmt.Fprintf(os.Stderr, "repetition %d/%d: SQLite workloads\n", rep, *repetitions)
		sqlite, err := benchmarks.RunSQLite(filepath.Join(dir, "run"))
		if err != nil {
			return fmt.Errorf("SQLite: %w", err)
		}
		report.Runs = append(report.Runs, Repetition{Repetition: rep, Results: append(lsm, sqlite...)})
	}
	report.Summary = summarizeWorkloads(report.Runs)
	printTable(label, report.Summary)

	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return fmt.Errorf("write %s: %w", out.Name(), err)
	}
	fmt.Fprintf(os.Stderr, "results written to %s\n", out.Name())
	return nil
}

func printTable(label string, summary []WorkloadSummary) {
	fmt.Printf("\nlabel: %s (median [min, max] across repetitions)\n", label)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Workload\tEngine\tOps/sec\tP50(ms)\tP99(ms)\tP99.9(ms)")
	for _, s := range summary {
		fmt.Fprintf(w, "%s\t%s\t%.0f [%.0f, %.0f]\t%.3f\t%.3f [%.3f, %.3f]\t%.3f\n",
			s.Workload, s.Engine, s.OpsPerSec.Median, s.OpsPerSec.Min, s.OpsPerSec.Max,
			s.P50Ms.Median, s.P99Ms.Median, s.P99Ms.Min, s.P99Ms.Max, s.P999Ms.Median)
	}
	w.Flush()
}
