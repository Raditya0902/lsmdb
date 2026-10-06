package benchcompare

import (
	"bytes"
	"strings"
	"testing"
)

// threeArms builds base, cap and copy at clients 1 and 4. Throughput is
// disjoint between base and the others; copy overlaps cap at 1 client, and
// copy has one run too few at 4 clients.
func threeArms(t *testing.T) Combined {
	t.Helper()
	combined := Combined{Arms: []ArmBuild{{Arm: Arm{Name: "base"}}, {Arm: Arm{Name: "cap"}}, {Arm: Arm{Name: "copy"}}}}
	add := func(arm string, clients, valid int, throughput float64) {
		for i := 0; i < valid; i++ {
			slot := Slot{Sequence: len(combined.Runs) + 1, Repetition: i + 1, Clients: clients, Arm: arm}
			combined.Runs = append(combined.Runs, ArmRun{Slot: slot, Report: report(t, "secondary", clients, true, throughput+float64(i), 10, nil)})
		}
	}
	for _, clients := range []int{1, 4} {
		add("base", clients, MinValidRuns, 100)
		add("cap", clients, MinValidRuns, 200)
	}
	add("copy", 1, MinValidRuns, 202)
	add("copy", 4, MinValidRuns-1, 300)
	return combined
}

type pair struct{ base, other string }

func comparisonsAt(summary Summary, clients int) map[pair]Comparison {
	out := map[pair]Comparison{}
	for _, cmp := range summary.Comparisons {
		if cmp.Clients == clients {
			out[pair{cmp.Base, cmp.Other}] = cmp
		}
	}
	return out
}

func TestReferenceArmIsComparedAgainstEveryOtherArm(t *testing.T) {
	summary, err := SummarizeAgainst(threeArms(t), "cap")
	if err != nil {
		t.Fatal(err)
	}
	if summary.Reference != "cap" {
		t.Fatalf("reference = %q, want cap", summary.Reference)
	}
	at1, at4 := comparisonsAt(summary, 1), comparisonsAt(summary, 4)
	if len(at1) != 2 || len(at4) != 2 || len(summary.Comparisons) != 4 {
		t.Fatalf("comparisons = %+v, want copy and base against cap at each client count", summary.Comparisons)
	}
	baseVsCap, ok := at1[pair{"cap", "base"}]
	if !ok || !baseVsCap.ThroughputMeaningful || baseVsCap.InsufficientRuns {
		t.Fatalf("base vs cap at 1 client = %+v (found %v), want meaningful (disjoint ranges)", baseVsCap, ok)
	}
	copyVsCap, ok := at1[pair{"cap", "copy"}]
	if !ok || copyVsCap.ThroughputMeaningful || copyVsCap.InsufficientRuns || copyVsCap.ThroughputRatio <= 1 {
		t.Fatalf("copy vs cap at 1 client = %+v (found %v), want ratio above 1 and not meaningful (ranges overlap)", copyVsCap, ok)
	}
	if short, ok := at4[pair{"cap", "copy"}]; !ok || !short.InsufficientRuns || short.ThroughputMeaningful {
		t.Fatalf("copy vs cap at 4 clients = %+v (found %v), want insufficient runs (copy has %d valid)", short, ok, MinValidRuns-1)
	}
}

func TestDefaultReferenceIsFirstArm(t *testing.T) {
	for _, reference := range []string{"", "base"} {
		summary, err := SummarizeAgainst(threeArms(t), reference)
		if err != nil {
			t.Fatal(err)
		}
		if summary.Reference != "base" {
			t.Fatalf("ref %q: reference = %q, want base", reference, summary.Reference)
		}
		for _, cmp := range summary.Comparisons {
			if cmp.Base != "base" || cmp.Other == "base" {
				t.Fatalf("ref %q: comparison %s vs %s, want every other arm against base", reference, cmp.Other, cmp.Base)
			}
		}
	}
	plain, err := Summarize(threeArms(t))
	if err != nil {
		t.Fatal(err)
	}
	if plain.Reference != "base" || len(plain.Comparisons) != 4 {
		t.Fatalf("Summarize: reference %q with %d comparisons, want base with 4", plain.Reference, len(plain.Comparisons))
	}
}

func TestUnknownReferenceArmIsAnError(t *testing.T) {
	if _, err := SummarizeAgainst(threeArms(t), "tip"); err == nil || !strings.Contains(err.Error(), "tip") {
		t.Fatalf("error = %v, want one naming the unknown arm", err)
	}
}

func TestSummaryHeaderNamesReferenceArm(t *testing.T) {
	summary, err := SummarizeAgainst(threeArms(t), "cap")
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	if err := WriteSummary(&text, summary); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "reference arm: cap\n") {
		t.Fatalf("summary header lacks the reference arm:\n%s", text.String())
	}
	if !strings.Contains(text.String(), "clients 1, copy vs cap:") {
		t.Fatalf("summary lacks the copy vs cap verdict:\n%s", text.String())
	}
}
