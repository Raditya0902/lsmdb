package benchcheck

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
)

// Write prints one line per check, a table of every run, and the verdict.
func Write(w io.Writer, report Report) error {
	fmt.Fprintf(w, "%s file, label %s, %d arms, %d runs\n", report.Kind, report.Label, report.Arms, len(report.Runs))
	failed, warned := 0, 0
	for _, result := range report.Results {
		fmt.Fprintf(w, "%-4s  %-16s %s\n", result.Status, result.Name, result.Detail)
		switch result.Status {
		case Fail:
			failed++
		case Warn:
			warned++
		}
	}
	fmt.Fprintln(w, "\nruns:")
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "arm\trep\tclients\tvalid\tops/s\tp50 ms\tp99 ms\tsyncs/entry\tentries/sync\tappends/entry\tfsync before p50/p99 ms\tfsync after p50/p99 ms\tappends/entry by origin")
	for _, row := range report.Runs {
		run := row.run
		fmt.Fprintf(table, "%s\t%d\t%d\t%s\t%.1f\t%.2f\t%.2f\t%.2f\t%.2f\t%.1f\t%s\t%s\t%s\n",
			armOrDash(row.Arm), run.Repetition, run.Clients, validText(run), run.Throughput, run.Latency.P50, run.Latency.P99,
			run.Counters.SyncsPerEntry, run.Counters.EntriesPerSync, run.Counters.AppendsPerEntry,
			fsyncText(run.FsyncBefore), fsyncText(run.FsyncAfter), originText(run.Counters.ByOrigin))
	}
	if err := table.Flush(); err != nil {
		return err
	}
	verdict := "PASS"
	if failed > 0 {
		verdict = "FAIL"
	}
	_, err := fmt.Fprintf(w, "\nresult: %s (%d failed, %d warnings)\n", verdict, failed, warned)
	return err
}

func armOrDash(arm string) string {
	if arm == "" {
		return "-"
	}
	return arm
}

func validText(run runView) string {
	switch {
	case run.Valid == nil:
		return "missing"
	case *run.Valid:
		return "yes"
	}
	return "NO: " + run.InvalidReason
}

func fsyncText(f fsyncView) string {
	value := func(v *float64) string {
		if v == nil {
			return "-"
		}
		return fmt.Sprintf("%.2f", *v)
	}
	return value(f.P50Ms) + "/" + value(f.P99Ms)
}

func originText(byOrigin map[string]float64) string {
	origins := make([]string, 0, len(byOrigin))
	for origin := range byOrigin {
		origins = append(origins, origin)
	}
	sort.Strings(origins)
	parts := make([]string, len(origins))
	for i, origin := range origins {
		parts[i] = fmt.Sprintf("%s=%.1f", origin, byOrigin[origin])
	}
	return strings.Join(parts, " ")
}
