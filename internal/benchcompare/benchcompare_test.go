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
	// Two valid runs per arm are below MinValidRuns, so disjoint ranges are not enough.
	if cmp.Base != "base" || cmp.Other != "tip" || !cmp.InsufficientRuns || cmp.ThroughputMeaningful || cmp.P99Meaningful {
		t.Fatalf("comparison = %+v, want insufficient runs and nothing marked meaningful", cmp)
	}
	var text bytes.Buffer
	if err := WriteSummary(&text, summary); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"secondary", "base", "tip", "insufficient runs"} {
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

// armRuns builds a two-arm comparison at one client count with the given
// number of valid runs per arm. Tip throughput is always above base, so the
// ranges are disjoint whenever both arms have runs.
func armRuns(t *testing.T, baseValid, tipValid int) Combined {
	t.Helper()
	combined := Combined{Arms: []ArmBuild{{Arm: Arm{Name: "base"}}, {Arm: Arm{Name: "tip"}}}}
	add := func(arm string, valid int, throughput, p99 float64) {
		for i := 0; i < valid; i++ {
			slot := Slot{Sequence: len(combined.Runs) + 1, Repetition: i + 1, Clients: 1, Arm: arm}
			combined.Runs = append(combined.Runs, ArmRun{Slot: slot, Report: report(t, "secondary", 1, true, throughput+float64(i), p99+float64(i), nil)})
		}
	}
	add("base", baseValid, 100, 10)
	add("tip", tipValid, 200, 50)
	return combined
}

func TestComparisonNeedsMinValidRunsPerArm(t *testing.T) {
	cases := []struct {
		name                 string
		baseValid, tipValid  int
		wantInsufficient     bool
		wantText, forbidText string
	}{
		{"both at threshold", MinValidRuns, MinValidRuns, false, "meaningful (ranges disjoint)", "insufficient runs"},
		{"base one below", MinValidRuns - 1, MinValidRuns, true, "insufficient runs", "meaningful"},
		{"tip one below", MinValidRuns, MinValidRuns - 1, true, "insufficient runs", "meaningful"},
		{"above threshold", MinValidRuns + 2, MinValidRuns + 1, false, "meaningful (ranges disjoint)", "insufficient runs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			summary, err := Summarize(armRuns(t, tc.baseValid, tc.tipValid))
			if err != nil {
				t.Fatal(err)
			}
			cmp := summary.Comparisons[0]
			if cmp.InsufficientRuns != tc.wantInsufficient {
				t.Fatalf("insufficient = %v, want %v: %+v", cmp.InsufficientRuns, tc.wantInsufficient, cmp)
			}
			if wantMeaningful := !tc.wantInsufficient; cmp.ThroughputMeaningful != wantMeaningful || cmp.P99Meaningful != wantMeaningful {
				t.Fatalf("meaningful flags = %v/%v, want %v: %+v", cmp.ThroughputMeaningful, cmp.P99Meaningful, wantMeaningful, cmp)
			}
			var text bytes.Buffer
			if err := WriteSummary(&text, summary); err != nil {
				t.Fatal(err)
			}
			comparisonLine := text.String()[strings.LastIndex(text.String(), "clients 1, tip vs base"):]
			if !strings.Contains(comparisonLine, tc.wantText) || strings.Contains(comparisonLine, tc.forbidText) {
				t.Fatalf("comparison line = %q, want %q and not %q", comparisonLine, tc.wantText, tc.forbidText)
			}
		})
	}
}

func TestOfficialComparisonsNeedMachineAndDiskType(t *testing.T) {
	cases := []struct {
		name          string
		extra         []string
		machine, disk string
		wantErr       bool
	}{
		{"secondary without labels", []string{"-duration=10s"}, "", "", false},
		{"official without labels", []string{"-official"}, "", "", true},
		{"official without disk", []string{"--official"}, "n2-standard-8", "", true},
		{"official=true without machine", []string{"-official=true"}, "", "pd-ssd", true},
		{"official with blank label", []string{"-official"}, "n2-standard-8", "  ", true},
		{"official with labels", []string{"-official", "-duration=30s"}, "n2-standard-8", "pd-ssd", false},
		{"official=false without labels", []string{"-official=false"}, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RequireMachineLabels(tc.extra, tc.machine, tc.disk)
			if (err != nil) != tc.wantErr {
				t.Fatalf("RequireMachineLabels(%q, %q, %q) = %v, want error %v", tc.extra, tc.machine, tc.disk, err, tc.wantErr)
			}
		})
	}
}

func TestMachineLabelsAreDriverFlags(t *testing.T) {
	for _, args := range [][]string{{"-machine-type", "x"}, {"--disk-type=pd-ssd"}} {
		if err := ValidateExtraArgs(args); err == nil {
			t.Errorf("ValidateExtraArgs(%q) passed a label through to arms that may not accept it", args)
		}
	}
}

func TestCombinedRecordsDriverCPUs(t *testing.T) {
	data, err := json.Marshal(Combined{NumCPU: 8, GOMAXPROCS: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"num_cpu":8`, `"gomaxprocs":2`} {
		if !strings.Contains(string(data), want) {
			t.Errorf("combined file lacks %s: %s", want, data)
		}
	}
}

// reportWithCounters builds a one-run secondary report with the given counters.
func reportWithCounters(t *testing.T, clients int, throughput, p99 float64, counters map[string]any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"label": "secondary",
		"runs": []map[string]any{{
			"clients": clients, "valid": true, "throughput_ops_per_sec": throughput,
			"latency_ms": map[string]float64{"p99": p99}, "counters": counters,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// fiveRunsPerArm gives base and fix five valid runs each at 4 clients.
func fiveRunsPerArm(t *testing.T, base, fix func(rep int) map[string]any) Combined {
	combined := Combined{Complete: true, Arms: []ArmBuild{{Arm: Arm{Name: "base"}}, {Arm: Arm{Name: "fix"}}}}
	for rep := 1; rep <= 5; rep++ {
		combined.Runs = append(combined.Runs,
			ArmRun{Slot: Slot{Sequence: 2*rep - 1, Repetition: rep, Clients: 4, Arm: "base"}, Report: reportWithCounters(t, 4, 25, 680, base(rep))},
			ArmRun{Slot: Slot{Sequence: 2 * rep, Repetition: rep, Clients: 4, Arm: "fix"}, Report: reportWithCounters(t, 4, 25, 680, fix(rep))})
	}
	return combined
}

func TestSummaryLeadsWithAppendsPerEntry(t *testing.T) {
	combined := fiveRunsPerArm(t,
		func(rep int) map[string]any {
			return map[string]any{"append_messages_per_committed_entry": 140.0 + float64(rep)}
		},
		func(rep int) map[string]any {
			return map[string]any{"append_messages_per_committed_entry": 4.0 + float64(rep)/10}
		})
	summary, err := Summarize(combined)
	if err != nil {
		t.Fatal(err)
	}
	cmp := summary.Comparisons[0]
	wantRatio := (4.0 + float64(3)/10) / (140.0 + 3)
	if !cmp.AppendsMeaningful || cmp.AppendsRatio != wantRatio {
		t.Fatalf("appends comparison = meaningful %v ratio %v, want disjoint ranges and %v", cmp.AppendsMeaningful, cmp.AppendsRatio, wantRatio)
	}
	if cmp.ThroughputMeaningful {
		t.Fatalf("identical throughput was marked meaningful: %+v", cmp)
	}
	var text bytes.Buffer
	if err := WriteSummary(&text, summary); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(text.String(), "\n")
	header, verdictLine := lines[1], lines[len(lines)-2]
	if a, o := strings.Index(header, "appends/entry"), strings.Index(header, "ops/s"); a < 0 || o < 0 || a > o {
		t.Errorf("header does not lead with appends/entry: %q", header)
	}
	if !strings.Contains(header, "appends/entry median [min, max]") {
		t.Errorf("header lacks the appends/entry range: %q", header)
	}
	if a, o := strings.Index(verdictLine, "appends/entry"), strings.Index(verdictLine, "throughput"); a < 0 || o < 0 || a > o {
		t.Errorf("comparison line does not lead with appends/entry: %q", verdictLine)
	}
	if !strings.Contains(verdictLine, "appends/entry x0.030 meaningful") {
		t.Errorf("comparison line lacks the appends verdict: %q", verdictLine)
	}
}

func TestSummaryShowsSendFailuresOnlyWhereRecorded(t *testing.T) {
	combined := fiveRunsPerArm(t,
		func(int) map[string]any { return map[string]any{"append_messages_per_committed_entry": 140.0} },
		func(rep int) map[string]any {
			return map[string]any{"append_messages_per_committed_entry": 5.0,
				"send_failures_deadline": float64(rep), "send_failures_other": 1.0}
		})
	summary, err := Summarize(combined)
	if err != nil {
		t.Fatal(err)
	}
	base, fix := summary.Cells[0], summary.Cells[1]
	if base.SendFailuresRecorded != 0 {
		t.Errorf("base recorded send failures in %d runs, want 0: its reports predate the field", base.SendFailuresRecorded)
	}
	if fix.SendFailuresRecorded != 5 || fix.SendFailuresDeadline != (Range{Median: 3, Min: 1, Max: 5}) || fix.SendFailuresOther.Median != 1 {
		t.Errorf("fix send failures = %d recorded, deadline %+v, other %+v", fix.SendFailuresRecorded, fix.SendFailuresDeadline, fix.SendFailuresOther)
	}
	var text bytes.Buffer
	if err := WriteSummary(&text, summary); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(text.String(), "\n")
	if !strings.Contains(lines[1], "deadline fails/run") || !strings.Contains(lines[1], "other fails/run") {
		t.Fatalf("header lacks send failure columns: %q", lines[1])
	}
	baseFields, fixFields := strings.Fields(lines[2]), strings.Fields(lines[3])
	if got := baseFields[len(baseFields)-2:]; got[0] != "-" || got[1] != "-" {
		t.Errorf("base row send failures = %v, want - - when unrecorded: %q", got, lines[2])
	}
	if got := fixFields[len(fixFields)-2:]; got[0] != "3" || got[1] != "1" {
		t.Errorf("fix row send failures = %v, want 3 1: %q", got, lines[3])
	}
}

const usageWithLabels = `Usage of clusterbench:
  -clients string
    	comma-separated client counts; each runs on a fresh cluster (default "1,2,4,8,16")
  -disk-type string
    	disk under -data-dir, e.g. local NVMe or pd-ssd; required with -official
  -machine-type string
    	machine or instance type, e.g. n2-standard-8; required with -official
  -official
    	label the run official
`

const usageBeforeLabels = `Usage of clusterbench:
  -clients string
    	comma-separated client counts (default "1,2,4,8,16")
  -official
    	label the run official; record -machine-type and -disk-type elsewhere
`

func TestAcceptsLabelsReadsDeclaredFlagsOnly(t *testing.T) {
	cases := []struct {
		name  string
		usage string
		want  bool
	}{
		{"both flags declared", usageWithLabels, true},
		{"arm built before the flags existed", usageBeforeLabels, false},
		{"only machine-type declared", "  -machine-type string\n    \tmachine\n", false},
		{"empty usage", "", false},
	}
	for _, tc := range cases {
		if got := AcceptsLabels(tc.usage); got != tc.want {
			t.Errorf("%s: AcceptsLabels = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestSlotArgsForwardLabelsOnlyToArmsThatAcceptThem(t *testing.T) {
	extra := []string{"-official", "-warmup", "5s", "-duration", "30s"}
	const machine, disk = "e2-standard-4", "pd-ssd 100GB boot disk (network)"
	base := []string{"-clients=4", "-repetitions=1", "-out=/w/runs/001.json"}

	got := SlotArgs(4, "/w/runs/001.json", extra, machine, disk, true)
	want := append(append(append([]string{}, base...), "-machine-type="+machine, "-disk-type="+disk), extra...)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("forwarding arm args =\n  %q\nwant\n  %q", got, want)
	}
	got = SlotArgs(4, "/w/runs/001.json", extra, machine, disk, false)
	want = append(append([]string{}, base...), extra...)
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("older arm args = %q, want %q (labels stay in the combined file only)", got, want)
	}
	got = SlotArgs(4, "/w/runs/001.json", nil, "", "", true)
	if strings.Join(got, "|") != strings.Join(base, "|") {
		t.Errorf("unlabelled run args = %q, want %q", got, base)
	}
	if err := ValidateExtraArgs([]string{"-machine-type=x"}); err == nil {
		t.Error("labels passed after -- must still be rejected; the driver owns them")
	}
}
