package benchcheck

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// sweepReport returns the good fixture's tip report as a single clusterbench
// report measured over duration seconds, with its one run at repetition rep.
func sweepReport(t *testing.T, duration float64, rep int) []byte {
	t.Helper()
	var combined struct {
		Runs []struct {
			Report map[string]any `json:"report"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(fixture(t, "good-compare"), &combined); err != nil {
		t.Fatal(err)
	}
	report := combined.Runs[1].Report
	report["config"].(map[string]any)["duration_seconds"] = duration
	report["runs"].([]any)[0].(map[string]any)["repetition"] = rep
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func sweepRows(t *testing.T, files ...[]byte) []SweepRow {
	t.Helper()
	var rows []SweepRow
	for _, data := range files {
		got, err := SweepRows(data)
		if err != nil {
			t.Fatalf("SweepRows: %v", err)
		}
		rows = append(rows, got...)
	}
	return rows
}

func TestSweepRowsCarryDurationAndAckResendShare(t *testing.T) {
	rows := sweepRows(t, sweepReport(t, 20, 1))
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	row := rows[0]
	if row.DurationSeconds != 20 || row.Arm != "" {
		t.Fatalf("row = %+v, want duration 20 and no arm", row)
	}
	share, ok := row.AckResendShare()
	if want := 31.4 / 36.7; !ok || share < want-1e-9 || share > want+1e-9 {
		t.Fatalf("AckResendShare = %v, %v; want %v", share, ok, want)
	}
}

func TestSweepRowsReadCompareFiles(t *testing.T) {
	rows := sweepRows(t, fixture(t, "good-compare"))
	if len(rows) != 2 || rows[0].Arm != "base" || rows[1].Arm != "tip" || rows[1].DurationSeconds != 10 {
		t.Fatalf("rows = %+v, want base and tip at 10 s", rows)
	}
}

func TestSweepRowsNeedDuration(t *testing.T) {
	data := mutate(t, sweepReport(t, 10, 1), func(doc map[string]any) {
		delete(doc["config"].(map[string]any), "duration_seconds")
	})
	if _, err := SweepRows(data); err == nil || !strings.Contains(err.Error(), "duration_seconds") {
		t.Fatalf("SweepRows without a duration: err = %v, want one naming duration_seconds", err)
	}
}

func TestAckResendShareWithoutAppends(t *testing.T) {
	data := mutate(t, sweepReport(t, 10, 1), func(doc map[string]any) {
		counters := doc["runs"].([]any)[0].(map[string]any)["counters"].(map[string]any)
		counters["append_messages_per_committed_entry"] = 0
	})
	if _, ok := sweepRows(t, data)[0].AckResendShare(); ok {
		t.Fatal("AckResendShare reported a share for a run with no AppendEntries")
	}
}

func TestWriteSweepSortsByDurationThenRepetition(t *testing.T) {
	rows := sweepRows(t, sweepReport(t, 60, 1), sweepReport(t, 10, 2), sweepReport(t, 10, 1))
	before := append([]SweepRow(nil), rows...)
	var out bytes.Buffer
	if err := WriteSweep(&out, rows); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if rows[i].DurationSeconds != before[i].DurationSeconds || rows[i].run.Repetition != before[i].run.Repetition {
			t.Fatal("WriteSweep reordered its input")
		}
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d lines, want a header and 3 rows:\n%s", len(lines), out.String())
	}
	for _, want := range []string{"duration s", "rep", "ops/s", "appends/entry", "ack_resend share"} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("header lacks %q: %s", want, lines[0])
		}
	}
	order := [][]string{{"10", "1"}, {"10", "2"}, {"60", "1"}}
	for i, want := range order {
		fields := strings.Fields(lines[i+1])
		if fields[0] != want[0] || fields[3] != want[1] {
			t.Errorf("row %d = %q, want duration %s repetition %s", i, lines[i+1], want[0], want[1])
		}
		for _, value := range []string{"405.9", "36.7", "85.6%", "yes"} {
			if !strings.Contains(lines[i+1], value) {
				t.Errorf("row %d lacks %q: %s", i, value, lines[i+1])
			}
		}
	}
}

func TestWriteSweepMarksMissingShare(t *testing.T) {
	data := mutate(t, sweepReport(t, 10, 1), func(doc map[string]any) {
		counters := doc["runs"].([]any)[0].(map[string]any)["counters"].(map[string]any)
		counters["append_messages_per_committed_entry"] = 0
	})
	var out bytes.Buffer
	if err := WriteSweep(&out, sweepRows(t, data)); err != nil {
		t.Fatal(err)
	}
	row := strings.Fields(strings.Split(strings.TrimSpace(out.String()), "\n")[1])
	if got := row[len(row)-1]; got != "-" {
		t.Fatalf("share column = %q, want -", got)
	}
}
