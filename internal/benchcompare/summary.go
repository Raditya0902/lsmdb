package benchcompare

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"text/tabwriter"
)

// Range is the median and extremes of one statistic across valid runs.
type Range struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

// Describe returns the median, minimum and maximum of values without
// reordering them. An empty input yields the zero Range.
func Describe(values []float64) Range {
	if len(values) == 0 {
		return Range{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	median := sorted[mid]
	if len(sorted)%2 == 0 {
		median = (sorted[mid-1] + sorted[mid]) / 2
	}
	return Range{Median: median, Min: sorted[0], Max: sorted[len(sorted)-1]}
}

// Meaningful reports whether two ranges are disjoint. Ranges that touch or
// overlap cannot support a claim that one arm differs from the other.
func Meaningful(a, b Range) bool {
	return a.Max < b.Min || b.Max < a.Min
}

// Cell summarises one arm at one client count over its valid runs.
type Cell struct {
	Arm               string `json:"arm"`
	Clients           int    `json:"clients"`
	ValidRuns         int    `json:"valid_runs"`
	InvalidRuns       int    `json:"invalid_runs"`
	Throughput        Range  `json:"throughput_ops_per_sec"`
	P99Ms             Range  `json:"p99_ms"`
	SyncsPerEntry     Range  `json:"log_syncs_per_committed_entry"`
	EntriesPerSync    Range  `json:"entries_per_log_sync"`
	AppendsPerEntry   Range  `json:"append_messages_per_committed_entry"`
	AckResendPerEntry Range  `json:"ack_resend_per_committed_entry"`
	// Send failures per measurement window, over the valid runs that recorded
	// them; reports built before the field existed do not count.
	SendFailuresRecorded int   `json:"send_failures_recorded_runs"`
	SendFailuresDeadline Range `json:"send_failures_deadline"`
	SendFailuresOther    Range `json:"send_failures_other"`
}

// MinValidRuns is the fewest valid runs each arm needs before a comparison
// can be called meaningful. With five runs per arm, two identical
// distributions give disjoint ranges by chance 2/C(10,5), about 0.8% of the time.
const MinValidRuns = 5

// Comparison sets one arm against the reference arm (Base) at one client count.
// When either arm has fewer than MinValidRuns valid runs, InsufficientRuns is
// set and neither meaningful flag is.
type Comparison struct {
	Clients              int     `json:"clients"`
	Base                 string  `json:"base"`
	Other                string  `json:"other"`
	BaseValidRuns        int     `json:"base_valid_runs"`
	OtherValidRuns       int     `json:"other_valid_runs"`
	InsufficientRuns     bool    `json:"insufficient_runs"`
	AppendsRatio         float64 `json:"appends_per_entry_median_ratio"`
	AppendsMeaningful    bool    `json:"appends_per_entry_meaningful"`
	ThroughputRatio      float64 `json:"throughput_median_ratio"`
	ThroughputMeaningful bool    `json:"throughput_meaningful"`
	P99Meaningful        bool    `json:"p99_meaningful"`
}

// Summary is the result of Summarize.
type Summary struct {
	Label       string       `json:"label"`
	Complete    bool         `json:"complete"`
	Reference   string       `json:"reference"`
	Cells       []Cell       `json:"cells"`
	Comparisons []Comparison `json:"comparisons"`
}

// runView is the subset of a clusterbench report the summary reads.
type runView struct {
	Clients    int     `json:"clients"`
	Valid      bool    `json:"valid"`
	Throughput float64 `json:"throughput_ops_per_sec"`
	Latency    struct {
		P99 float64 `json:"p99"`
	} `json:"latency_ms"`
	Counters struct {
		SyncsPerEntry   float64            `json:"log_syncs_per_committed_entry"`
		EntriesPerSync  float64            `json:"entries_per_log_sync"`
		AppendsPerEntry float64            `json:"append_messages_per_committed_entry"`
		ByOrigin        map[string]float64 `json:"append_messages_per_committed_entry_by_origin"`
		SendDeadline    *float64           `json:"send_failures_deadline"`
		SendOther       *float64           `json:"send_failures_other"`
	} `json:"counters"`
}

type reportView struct {
	Label string    `json:"label"`
	Runs  []runView `json:"runs"`
}

type cellKey struct {
	arm     string
	clients int
}

// cellValues collects per-statistic samples from valid runs.
type cellValues struct {
	valid, invalid                                       int
	throughput, p99, syncs, perSync, appends, ackResends []float64
	sendDeadline, sendOther                              []float64
}

// Summarize groups runs by arm and client count, excluding invalid runs from
// every statistic, and compares each arm against the first declared arm.
func Summarize(combined Combined) (Summary, error) {
	return SummarizeAgainst(combined, "")
}

// SummarizeAgainst is Summarize with every other arm compared against the
// named reference arm; an empty name means the first declared arm.
func SummarizeAgainst(combined Combined, reference string) (Summary, error) {
	order := make(map[string]int, len(combined.Arms))
	for i, arm := range combined.Arms {
		order[arm.Name] = i
	}
	if reference == "" && len(combined.Arms) > 0 {
		reference = combined.Arms[0].Name
	}
	if _, ok := order[reference]; !ok && len(combined.Arms) > 0 {
		return Summary{}, fmt.Errorf("reference arm %q is not a declared arm", reference)
	}
	values := map[cellKey]*cellValues{}
	official := len(combined.Runs) > 0
	for _, run := range combined.Runs {
		if _, ok := order[run.Arm]; !ok {
			return Summary{}, fmt.Errorf("run %d names undeclared arm %q", run.Sequence, run.Arm)
		}
		view, err := decodeRun(run)
		if err != nil {
			return Summary{}, err
		}
		official = official && view.label == "official"
		key := cellKey{run.Arm, run.Clients}
		if values[key] == nil {
			values[key] = &cellValues{}
		}
		values[key].add(view.run)
	}
	summary := Summary{Label: "secondary", Complete: combined.Complete, Reference: reference}
	if official {
		summary.Label = "official"
	}
	for key, v := range values {
		summary.Cells = append(summary.Cells, v.cell(key))
	}
	sort.Slice(summary.Cells, func(i, j int) bool {
		a, b := summary.Cells[i], summary.Cells[j]
		if a.Clients != b.Clients {
			return a.Clients < b.Clients
		}
		return order[a.Arm] < order[b.Arm]
	})
	summary.Comparisons = compare(summary.Cells, combined.Arms, reference)
	return summary, nil
}

type decodedRun struct {
	label string
	run   runView
}

func decodeRun(run ArmRun) (decodedRun, error) {
	var view reportView
	if err := json.Unmarshal(run.Report, &view); err != nil {
		return decodedRun{}, fmt.Errorf("run %d report: %w", run.Sequence, err)
	}
	if len(view.Runs) != 1 {
		return decodedRun{}, fmt.Errorf("run %d report holds %d runs, want exactly 1", run.Sequence, len(view.Runs))
	}
	if view.Runs[0].Clients != run.Clients {
		return decodedRun{}, fmt.Errorf("run %d report measured %d clients, slot says %d", run.Sequence, view.Runs[0].Clients, run.Clients)
	}
	return decodedRun{label: view.Label, run: view.Runs[0]}, nil
}

func (v *cellValues) add(run runView) {
	if !run.Valid {
		v.invalid++
		return
	}
	v.valid++
	v.throughput = append(v.throughput, run.Throughput)
	v.p99 = append(v.p99, run.Latency.P99)
	v.syncs = append(v.syncs, run.Counters.SyncsPerEntry)
	v.perSync = append(v.perSync, run.Counters.EntriesPerSync)
	v.appends = append(v.appends, run.Counters.AppendsPerEntry)
	v.ackResends = append(v.ackResends, run.Counters.ByOrigin["ack_resend"])
	if run.Counters.SendDeadline != nil && run.Counters.SendOther != nil {
		v.sendDeadline = append(v.sendDeadline, *run.Counters.SendDeadline)
		v.sendOther = append(v.sendOther, *run.Counters.SendOther)
	}
}

func (v *cellValues) cell(key cellKey) Cell {
	return Cell{
		Arm: key.arm, Clients: key.clients, ValidRuns: v.valid, InvalidRuns: v.invalid,
		Throughput: Describe(v.throughput), P99Ms: Describe(v.p99),
		SyncsPerEntry: Describe(v.syncs), EntriesPerSync: Describe(v.perSync),
		AppendsPerEntry: Describe(v.appends), AckResendPerEntry: Describe(v.ackResends),
		SendFailuresRecorded: len(v.sendDeadline),
		SendFailuresDeadline: Describe(v.sendDeadline), SendFailuresOther: Describe(v.sendOther),
	}
}

func compare(cells []Cell, arms []ArmBuild, base string) []Comparison {
	if len(arms) < 2 {
		return nil
	}
	byKey := make(map[cellKey]Cell, len(cells))
	var clients []int
	for _, cell := range cells {
		byKey[cellKey{cell.Arm, cell.Clients}] = cell
		if len(clients) == 0 || clients[len(clients)-1] != cell.Clients {
			clients = append(clients, cell.Clients)
		}
	}
	var out []Comparison
	for _, count := range clients {
		b := byKey[cellKey{base, count}]
		for _, arm := range arms {
			if arm.Name == base {
				continue
			}
			o := byKey[cellKey{arm.Name, count}]
			cmp := Comparison{
				Clients: count, Base: base, Other: arm.Name,
				BaseValidRuns: b.ValidRuns, OtherValidRuns: o.ValidRuns,
				InsufficientRuns: b.ValidRuns < MinValidRuns || o.ValidRuns < MinValidRuns,
			}
			if b.ValidRuns > 0 && o.ValidRuns > 0 && b.Throughput.Median != 0 {
				cmp.ThroughputRatio = o.Throughput.Median / b.Throughput.Median
			}
			if b.ValidRuns > 0 && o.ValidRuns > 0 && b.AppendsPerEntry.Median != 0 {
				cmp.AppendsRatio = o.AppendsPerEntry.Median / b.AppendsPerEntry.Median
			}
			cmp.AppendsMeaningful = !cmp.InsufficientRuns && Meaningful(b.AppendsPerEntry, o.AppendsPerEntry)
			cmp.ThroughputMeaningful = !cmp.InsufficientRuns && Meaningful(b.Throughput, o.Throughput)
			cmp.P99Meaningful = !cmp.InsufficientRuns && Meaningful(b.P99Ms, o.P99Ms)
			out = append(out, cmp)
		}
	}
	return out
}

// WriteSummary prints the summary as aligned text.
func WriteSummary(w io.Writer, summary Summary) error {
	if _, err := fmt.Fprintf(w, "label: %s\nreference arm: %s\n", summary.Label, summary.Reference); err != nil {
		return err
	}
	if !summary.Complete {
		fmt.Fprintln(w, "INCOMPLETE: the comparison stopped before every slot ran; arms may have unequal runs")
	}
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	// Total AppendEntries per committed entry is the headline replication metric,
	// so it leads. ack_resend's meaning depends on the engine revision; do not
	// compare it across arms.
	fmt.Fprintln(table, "clients\tarm\tvalid\tinvalid\tappends/entry median [min, max]\tops/s median [min, max]\tp99 ms median [min, max]\tsyncs/entry\tentries/sync\tack_resend/entry\tdeadline fails/run\tother fails/run")
	for _, c := range summary.Cells {
		fmt.Fprintf(table, "%d\t%s\t%d\t%d\t%s\t%s\t%s\t%.2f\t%.2f\t%.1f\t%s\t%s\n",
			c.Clients, c.Arm, c.ValidRuns, c.InvalidRuns, formatRange(c.AppendsPerEntry, 1),
			formatRange(c.Throughput, 1), formatRange(c.P99Ms, 2),
			c.SyncsPerEntry.Median, c.EntriesPerSync.Median, c.AckResendPerEntry.Median,
			failureMedian(c, c.SendFailuresDeadline), failureMedian(c, c.SendFailuresOther))
	}
	if err := table.Flush(); err != nil {
		return err
	}
	for _, cmp := range summary.Comparisons {
		if cmp.InsufficientRuns {
			fmt.Fprintf(w, "clients %d, %s vs %s: appends/entry x%.3f; throughput x%.3f; insufficient runs (%s %d valid, %s %d valid; need %d per arm)\n",
				cmp.Clients, cmp.Other, cmp.Base, cmp.AppendsRatio, cmp.ThroughputRatio,
				cmp.Base, cmp.BaseValidRuns, cmp.Other, cmp.OtherValidRuns, MinValidRuns)
			continue
		}
		fmt.Fprintf(w, "clients %d, %s vs %s: appends/entry x%.3f %s; throughput x%.3f %s; p99 %s\n", cmp.Clients, cmp.Other, cmp.Base,
			cmp.AppendsRatio, verdict(cmp.AppendsMeaningful), cmp.ThroughputRatio, verdict(cmp.ThroughputMeaningful), verdict(cmp.P99Meaningful))
	}
	return nil
}

// failureMedian prints a send-failure median, or "-" when no valid run in the
// cell recorded send failures.
func failureMedian(c Cell, r Range) string {
	if c.SendFailuresRecorded == 0 {
		return "-"
	}
	return strconv.FormatFloat(r.Median, 'f', -1, 64)
}

func formatRange(r Range, decimals int) string {
	return fmt.Sprintf("%.*f [%.*f, %.*f]", decimals, r.Median, decimals, r.Min, decimals, r.Max)
}

func verdict(meaningful bool) string {
	if meaningful {
		return "meaningful (ranges disjoint)"
	}
	return "not meaningful (ranges overlap)"
}
