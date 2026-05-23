// cmd/bench runs all six workloads against both the LSM engine and SQLite,
// prints a formatted table to stdout, and writes results.json.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"lsmdb/benchmarks"
)

func main() {
	outFile := flag.String("out", "results.json", "path for JSON results output")
	flag.Parse()

	dir, err := os.MkdirTemp("", "lsmdb-bench-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	fmt.Println("Running LSM workloads…")
	lsmResults, err := benchmarks.RunLSM(filepath.Join(dir, "run"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "LSM: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Running SQLite workloads…")
	sqliteResults, err := benchmarks.RunSQLite(filepath.Join(dir, "run"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "SQLite: %v\n", err)
		os.Exit(1)
	}

	all := append(lsmResults, sqliteResults...)
	printTable(all)
	writeJSON(*outFile, all)
}

func printTable(results []benchmarks.WorkloadResult) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "\nWorkload\tEngine\tOps/sec\tP50(ms)\tP95(ms)\tP99(ms)\tDisk(KB)\tBloomSkips\tReadAmp")
	fmt.Fprintln(w, "--------\t------\t-------\t-------\t-------\t-------\t--------\t----------\t-------")
	for _, r := range results {
		fmt.Fprintf(w, "%s\t%s\t%.0f\t%.3f\t%.3f\t%.3f\t%d\t%d\t%.1f\n",
			r.Workload, r.Engine,
			r.OpsPerSec,
			r.P50Ms, r.P95Ms, r.P99Ms,
			r.DiskBytes/1024,
			r.BloomSkips,
			r.ReadAmp,
		)
	}
	w.Flush()
}

func writeJSON(path string, results []benchmarks.WorkloadResult) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", path, err)
		return
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(results); err != nil {
		fmt.Fprintf(os.Stderr, "write json: %v\n", err)
		return
	}
	fmt.Printf("Results written to %s\n", path)
}
