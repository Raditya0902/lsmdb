package benchcheck

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"text/tabwriter"
)

// SweepRow is one measured run tagged with the window length it ran over,
// for comparing runs of a duration sweep.
type SweepRow struct {
	DurationSeconds float64
	Arm             string
	run             runView
}

// SweepRows reads a clusterbench report or a bench_compare file and returns
// one row per run. Every report must record config.duration_seconds.
func SweepRows(data []byte) ([]SweepRow, error) {
	in, err := parse(data)
	if err != nil {
		return nil, err
	}
	var rows []SweepRow
	for _, u := range in.units {
		duration := u.report.Config.DurationSeconds
		if duration == nil {
			return nil, fmt.Errorf("%s: no config.duration_seconds", u.name)
		}
		for _, run := range u.report.Runs {
			rows = append(rows, SweepRow{DurationSeconds: *duration, Arm: u.arm, run: run})
		}
	}
	return rows, nil
}

// AckResendShare is the fraction of the run's AppendEntries messages that
// were duplicate-ack resends. It is false when the run sent none.
func (r SweepRow) AckResendShare() (float64, bool) {
	total := r.run.Counters.AppendsPerEntry
	if total <= 0 {
		return 0, false
	}
	return r.run.Counters.ByOrigin["ack_resend"] / total, true
}

// WriteSweep prints rows ordered by duration, arm, clients and repetition,
// without reordering the caller's slice.
func WriteSweep(w io.Writer, rows []SweepRow) error {
	sorted := append([]SweepRow(nil), rows...)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		switch {
		case a.DurationSeconds != b.DurationSeconds:
			return a.DurationSeconds < b.DurationSeconds
		case a.Arm != b.Arm:
			return a.Arm < b.Arm
		case a.run.Clients != b.run.Clients:
			return a.run.Clients < b.run.Clients
		}
		return a.run.Repetition < b.run.Repetition
	})
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "duration s\tarm\tclients\trep\tvalid\tops/s\tappends/entry\tack_resend share")
	for _, row := range sorted {
		share := "-"
		if value, ok := row.AckResendShare(); ok {
			share = fmt.Sprintf("%.1f%%", 100*value)
		}
		fmt.Fprintf(table, "%s\t%s\t%d\t%d\t%s\t%.1f\t%.1f\t%s\n",
			strconv.FormatFloat(row.DurationSeconds, 'f', -1, 64), armOrDash(row.Arm), row.run.Clients,
			row.run.Repetition, validText(row.run), row.run.Throughput, row.run.Counters.AppendsPerEntry, share)
	}
	return table.Flush()
}
