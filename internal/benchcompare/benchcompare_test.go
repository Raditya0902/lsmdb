package benchcompare

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestMedianMinMax(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		want   Range
	}{
		{"empty", nil, Range{}},
		{"single", []float64{4}, Range{Median: 4, Min: 4, Max: 4}},
		{"odd unsorted", []float64{9, 1, 5}, Range{Median: 5, Min: 1, Max: 9}},
		{"even averages middle pair", []float64{4, 1, 3, 10}, Range{Median: 3.5, Min: 1, Max: 10}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := append([]float64(nil), tc.values...)
			if got := Describe(input); got != tc.want {
				t.Fatalf("Describe(%v) = %+v, want %+v", tc.values, got, tc.want)
			}
			if !reflect.DeepEqual(input, tc.values) && tc.values != nil {
				t.Fatalf("Describe reordered its input: %v", input)
			}
		})
	}
}

func TestMeaningfulOnlyWhenRangesDisjoint(t *testing.T) {
	cases := []struct {
		name string
		a, b Range
		want bool
	}{
		{"overlap", Range{Min: 10, Max: 20}, Range{Min: 15, Max: 25}, false},
		{"contained", Range{Min: 10, Max: 30}, Range{Min: 15, Max: 20}, false},
		{"touch", Range{Min: 10, Max: 20}, Range{Min: 20, Max: 30}, false},
		{"disjoint above", Range{Min: 10, Max: 20}, Range{Min: 21, Max: 30}, true},
		{"disjoint below", Range{Min: 31, Max: 40}, Range{Min: 10, Max: 30}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Meaningful(tc.a, tc.b); got != tc.want {
				t.Fatalf("Meaningful(%+v, %+v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			if got := Meaningful(tc.b, tc.a); got != tc.want {
				t.Fatalf("Meaningful is not symmetric for %+v, %+v", tc.a, tc.b)
			}
		})
	}
}

func TestInterleaveOrderAlternates(t *testing.T) {
	got := Schedule([]string{"base", "tip"}, []int{1, 4}, 3)
	want := []Slot{
		{Sequence: 1, Repetition: 1, Clients: 1, Arm: "base"},
		{Sequence: 2, Repetition: 1, Clients: 1, Arm: "tip"},
		{Sequence: 3, Repetition: 1, Clients: 4, Arm: "base"},
		{Sequence: 4, Repetition: 1, Clients: 4, Arm: "tip"},
		{Sequence: 5, Repetition: 2, Clients: 1, Arm: "tip"},
		{Sequence: 6, Repetition: 2, Clients: 1, Arm: "base"},
		{Sequence: 7, Repetition: 2, Clients: 4, Arm: "tip"},
		{Sequence: 8, Repetition: 2, Clients: 4, Arm: "base"},
		{Sequence: 9, Repetition: 3, Clients: 1, Arm: "base"},
		{Sequence: 10, Repetition: 3, Clients: 1, Arm: "tip"},
		{Sequence: 11, Repetition: 3, Clients: 4, Arm: "base"},
		{Sequence: 12, Repetition: 3, Clients: 4, Arm: "tip"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Schedule =\n%+v\nwant\n%+v", got, want)
	}
}

func TestParseArmSpec(t *testing.T) {
	cases := []struct {
		spec    string
		want    Arm
		wantErr bool
	}{
		{"tip=HEAD", Arm{Name: "tip", Rev: "HEAD"}, false},
		{"base=bd67696+ed0e92e,8eae967", Arm{Name: "base", Rev: "bd67696", Picks: []string{"ed0e92e", "8eae967"}}, false},
		{"base = bd67696 + ed0e92e", Arm{Name: "base", Rev: "bd67696", Picks: []string{"ed0e92e"}}, false},
		{"=HEAD", Arm{}, true},
		{"tip=", Arm{}, true},
		{"tip", Arm{}, true},
		{"a/b=HEAD", Arm{}, true},
		{"base=bd67696+", Arm{}, true},
		{"base=bd67696+a,,b", Arm{}, true},
		{"base=-bad", Arm{}, true},
		{"base=bd67696+-x", Arm{}, true},
	}
	for _, tc := range cases {
		got, err := ParseArmSpec(tc.spec)
		if (err != nil) != tc.wantErr {
			t.Errorf("ParseArmSpec(%q) error = %v, wantErr %v", tc.spec, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseArmSpec(%q) = %+v, want %+v", tc.spec, got, tc.want)
		}
	}
}

func TestExtraArgsCannotOverrideDriverFlags(t *testing.T) {
	for _, args := range [][]string{
		{"-clients=4"}, {"--repetitions", "2"}, {"-out", "x.json"}, {"-duration=5s", "-clients", "1"},
	} {
		if err := ValidateExtraArgs(args); err == nil {
			t.Errorf("ValidateExtraArgs(%q) accepted a driver-owned flag", args)
		}
	}
	if err := ValidateExtraArgs([]string{"-duration=10s", "-warmup", "2s", "-seed=3"}); err != nil {
		t.Errorf("ValidateExtraArgs rejected ordinary flags: %v", err)
	}
}

// report builds a minimal clusterbench report holding one run.
func report(t *testing.T, label string, clients int, valid bool, throughput, p99 float64, origins map[string]float64) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"label": label,
		"runs": []map[string]any{{
			"clients": clients, "valid": valid, "throughput_ops_per_sec": throughput,
			"latency_ms": map[string]float64{"p99": p99},
			"counters": map[string]any{
				"log_syncs_per_committed_entry": 3.0, "entries_per_log_sync": 1.0,
				"append_messages_per_committed_entry":           70.0,
				"append_messages_per_committed_entry_by_origin": origins,
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSummaryExcludesInvalidRuns(t *testing.T) {
	origins := map[string]float64{"ack_resend": 66}
	combined := Combined{
		Arms: []ArmBuild{{Arm: Arm{Name: "base"}}, {Arm: Arm{Name: "tip"}}},
		Runs: []ArmRun{
			{Slot: Slot{Sequence: 1, Repetition: 1, Clients: 1, Arm: "base"}, Report: report(t, "secondary", 1, true, 100, 10, origins)},
			{Slot: Slot{Sequence: 2, Repetition: 1, Clients: 1, Arm: "tip"}, Report: report(t, "secondary", 1, true, 200, 5, origins)},
			{Slot: Slot{Sequence: 3, Repetition: 2, Clients: 1, Arm: "tip"}, Report: report(t, "secondary", 1, false, 9999, 1, origins)},
			{Slot: Slot{Sequence: 4, Repetition: 2, Clients: 1, Arm: "base"}, Report: report(t, "secondary", 1, true, 120, 12, origins)},
			{Slot: Slot{Sequence: 5, Repetition: 2, Clients: 1, Arm: "tip"}, Report: report(t, "secondary", 1, true, 210, 6, origins)},
		},
	}
	summary, err := Summarize(combined)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Label != "secondary" {
		t.Errorf("label = %q, want secondary", summary.Label)
	}
	if len(summary.Cells) != 2 {
		t.Fatalf("cells = %+v, want one per arm", summary.Cells)
	}
	tip := summary.Cells[1]
	if tip.Arm != "tip" || tip.ValidRuns != 2 || tip.InvalidRuns != 1 {
		t.Fatalf("tip cell = %+v, want 2 valid and 1 invalid", tip)
	}
	if tip.Throughput != (Range{Median: 205, Min: 200, Max: 210}) || tip.P99Ms.Min != 5 {
		t.Fatalf("tip statistics include the invalid run: %+v", tip)
	}
	if tip.AckResendPerEntry.Median != 66 {
		t.Fatalf("ack_resend per entry = %+v, want 66", tip.AckResendPerEntry)
	}
	if len(summary.Comparisons) != 1 {
		t.Fatalf("comparisons = %+v, want tip against base", summary.Comparisons)
	}
	cmp := summary.Comparisons[0]
	if cmp.Base != "base" || cmp.Other != "tip" || !cmp.ThroughputMeaningful || !cmp.P99Meaningful {
		t.Fatalf("comparison = %+v, want disjoint throughput and p99", cmp)
	}
	var text bytes.Buffer
	if err := WriteSummary(&text, summary); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"secondary", "base", "tip", "meaningful"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("summary text lacks %q:\n%s", want, text.String())
		}
	}
}

func TestComparisonNeedsValidRunsOnBothArms(t *testing.T) {
	combined := Combined{
		Arms: []ArmBuild{{Arm: Arm{Name: "base"}}, {Arm: Arm{Name: "tip"}}},
		Runs: []ArmRun{
			{Slot: Slot{Sequence: 1, Repetition: 1, Clients: 4, Arm: "base"}, Report: report(t, "official", 4, true, 100, 10, nil)},
			{Slot: Slot{Sequence: 2, Repetition: 1, Clients: 4, Arm: "tip"}, Report: report(t, "official", 4, false, 500, 1, nil)},
		},
	}
	summary, err := Summarize(combined)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Label != "official" {
		t.Errorf("label = %q, want official when every run is official", summary.Label)
	}
	cmp := summary.Comparisons[0]
	if cmp.ThroughputMeaningful || cmp.P99Meaningful {
		t.Fatalf("comparison with no valid tip runs was marked meaningful: %+v", cmp)
	}
}

func TestMixedLabelsSummarizeAsSecondary(t *testing.T) {
	combined := Combined{
		Arms: []ArmBuild{{Arm: Arm{Name: "base"}}},
		Runs: []ArmRun{
			{Slot: Slot{Sequence: 1, Repetition: 1, Clients: 1, Arm: "base"}, Report: report(t, "official", 1, true, 1, 1, nil)},
			{Slot: Slot{Sequence: 2, Repetition: 2, Clients: 1, Arm: "base"}, Report: report(t, "secondary", 1, true, 1, 1, nil)},
		},
	}
	summary, err := Summarize(combined)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Label != "secondary" {
		t.Fatalf("label = %q, want secondary when any run is secondary", summary.Label)
	}
}

func TestSummarizeRejectsUnknownArmAndMultiRunReports(t *testing.T) {
	unknown := Combined{
		Arms: []ArmBuild{{Arm: Arm{Name: "base"}}},
		Runs: []ArmRun{{Slot: Slot{Arm: "other", Clients: 1}, Report: report(t, "secondary", 1, true, 1, 1, nil)}},
	}
	if _, err := Summarize(unknown); err == nil {
		t.Error("Summarize accepted a run from an undeclared arm")
	}
	mismatch := Combined{
		Arms: []ArmBuild{{Arm: Arm{Name: "base"}}},
		Runs: []ArmRun{{Slot: Slot{Arm: "base", Clients: 4}, Report: report(t, "secondary", 1, true, 1, 1, nil)}},
	}
	if _, err := Summarize(mismatch); err == nil {
		t.Error("Summarize accepted a report whose client count differs from its slot")
	}
}

func TestIncompleteComparisonIsFlagged(t *testing.T) {
	combined := Combined{
		Arms: []ArmBuild{{Arm: Arm{Name: "base"}}},
		Runs: []ArmRun{{Slot: Slot{Arm: "base", Clients: 1}, Report: report(t, "secondary", 1, true, 1, 1, nil)}},
	}
	summary, err := Summarize(combined)
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	if err := WriteSummary(&text, summary); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "INCOMPLETE") {
		t.Fatalf("summary of an unfinished comparison is not flagged:\n%s", text.String())
	}
}
