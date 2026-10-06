//go:build ignore

// Command sweep_table prints one row per run from clusterbench reports or
// bench_compare files measured with different -duration values, ordered by
// duration: ops/s, AppendEntries per committed entry, and the share of those
// that were duplicate-ack resends.
//
//	go run scripts/sweep_table.go "$BENCH"/sweep/*.json
//
// It only reads the files; run vm_smoke_check on each one to validate it.
package main

import (
	"fmt"
	"os"

	"lsmdb/internal/benchcheck"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run scripts/sweep_table.go <report.json>...")
		os.Exit(2)
	}
	var rows []benchcheck.SweepRow
	for _, path := range os.Args[1:] {
		data, err := os.ReadFile(path)
		if err != nil {
			fail(err)
		}
		fileRows, err := benchcheck.SweepRows(data)
		if err != nil {
			fail(fmt.Errorf("%s: %w", path, err))
		}
		rows = append(rows, fileRows...)
	}
	if err := benchcheck.WriteSweep(os.Stdout, rows); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "sweep_table:", err)
	os.Exit(2)
}
