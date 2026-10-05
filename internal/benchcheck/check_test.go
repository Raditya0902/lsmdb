package benchcheck

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"lsmdb/internal/benchcompare"
)

// testIdentity is the machine the fixtures pretend to be checked on.
var testIdentity = Identity{Hostname: "bench-vm-7.c.project.internal", Username: "benchuser", Home: "/home/benchuser"}

func expectations(t *testing.T) []benchcompare.Arm {
	t.Helper()
	var arms []benchcompare.Arm
	for _, spec := range []string{"base=bd67696+ed0e92e,8eae967", "tip=8eae967"} {
		arm, err := benchcompare.ParseArmSpec(spec)
		if err != nil {
			t.Fatal(err)
		}
		arms = append(arms, arm)
	}
	return arms
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func check(t *testing.T, data []byte) Report {
	t.Helper()
	report, err := Check(data, Options{Identity: testIdentity, Expect: expectations(t)})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return report
}

func statusOf(t *testing.T, report Report, name string) Result {
	t.Helper()
	for _, result := range report.Results {
		if result.Name == name {
			return result
		}
	}
	t.Fatalf("no %q check in %+v", name, report.Results)
	return Result{}
}

// mutate decodes data, applies edit to the generic document and re-encodes it.
func mutate(t *testing.T, data []byte, edit func(doc map[string]any)) []byte {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	edit(doc)
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func runReports(doc map[string]any) []map[string]any {
	var reports []map[string]any
	for _, run := range doc["runs"].([]any) {
		reports = append(reports, run.(map[string]any)["report"].(map[string]any))
	}
	return reports
}

func TestGoodFixturePassesEveryCheck(t *testing.T) {
	report := check(t, fixture(t, "good-compare"))
	if report.Failed() {
		t.Fatalf("good fixture failed: %+v", report.Results)
	}
	for _, name := range CheckNames {
		result := statusOf(t, report, name)
		want := Pass
		if name == "protocol" {
			want = Warn // the fixture is a 10 s, 1-repetition smoke run
		}
		if result.Status != want {
			t.Errorf("%s = %s (%s), want %s", name, result.Status, result.Detail, want)
		}
	}
	if len(report.Runs) != 2 {
		t.Fatalf("listed %d runs, want 2", len(report.Runs))
	}
}

func TestEachFailureFixtureFailsOnlyItsCheck(t *testing.T) {
	cases := []struct{ fixture, check string }{
		{"missing-env-field", "env.data_dir_fs"},
		{"hostname-present", "privacy"},
		{"wrong-primitive", "fsync.primitive"},
		{"missing-arm", "arms"},
		{"missing-machine-type", "labels"},
	}
	for _, tc := range cases {
		t.Run(tc.fixture, func(t *testing.T) {
			report := check(t, fixture(t, tc.fixture))
			if !report.Failed() {
				t.Fatal("report did not fail")
			}
			for _, result := range report.Results {
				failed := result.Status == Fail
				if failed != (result.Name == tc.check) {
					t.Errorf("%s = %s (%s); only %s should fail", result.Name, result.Status, result.Detail, tc.check)
				}
			}
		})
	}
}

func TestOfficialProtocolHasNoWarning(t *testing.T) {
	data := mutate(t, fixture(t, "good-compare"), func(doc map[string]any) {
		doc["repetitions"] = 5
		for _, report := range runReports(doc) {
			config := report["config"].(map[string]any)
			config["warmup_seconds"], config["duration_seconds"] = 5, 30
		}
	})
	if result := statusOf(t, check(t, data), "protocol"); result.Status != Pass {
		t.Fatalf("protocol = %s (%s), want PASS", result.Status, result.Detail)
	}
	missing := mutate(t, fixture(t, "good-compare"), func(doc map[string]any) {
		delete(runReports(doc)[0]["config"].(map[string]any), "duration_seconds")
	})
	if result := statusOf(t, check(t, missing), "protocol"); result.Status != Fail {
		t.Fatalf("protocol with no duration = %s, want FAIL", result.Status)
	}
}

func TestElectionInvalidatedRunFails(t *testing.T) {
	data := mutate(t, fixture(t, "good-compare"), func(doc map[string]any) {
		run := runReports(doc)[1]["runs"].([]any)[0].(map[string]any)
		run["valid"], run["elections_in_window"] = false, 1
		run["invalid_reason"] = "node 2 term changed from 2 to 3 during the window"
		run["terms_end"] = []int{2, 3, 3}
	})
	result := statusOf(t, check(t, data), "validity")
	if result.Status != Fail || !strings.Contains(result.Detail, "1 invalid") {
		t.Fatalf("validity = %s (%s), want FAIL naming 1 invalid run", result.Status, result.Detail)
	}
}

func TestRunWithoutValidFieldFails(t *testing.T) {
	data := mutate(t, fixture(t, "good-compare"), func(doc map[string]any) {
		delete(runReports(doc)[0]["runs"].([]any)[0].(map[string]any), "valid")
	})
	if result := statusOf(t, check(t, data), "validity"); result.Status != Fail {
		t.Fatalf("validity = %s (%s), want FAIL", result.Status, result.Detail)
	}
}

func TestMissingFsyncSampleFails(t *testing.T) {
	data := mutate(t, fixture(t, "good-compare"), func(doc map[string]any) {
		delete(runReports(doc)[0]["runs"].([]any)[0].(map[string]any)["fsync_after"].(map[string]any), "p99_ms")
	})
	if result := statusOf(t, check(t, data), "fsync.samples"); result.Status != Fail {
		t.Fatalf("fsync.samples = %s (%s), want FAIL", result.Status, result.Detail)
	}
}

func TestArmShaMismatchFails(t *testing.T) {
	cases := map[string]func(doc map[string]any){
		"report built from another revision": func(doc map[string]any) {
			runReports(doc)[1]["environment"].(map[string]any)["git_sha"] = "0123456789abcdef0123456789abcdef01234567"
		},
		"dirty arm": func(doc map[string]any) {
			runReports(doc)[0]["environment"].(map[string]any)["git_dirty"] = true
		},
		"wrong base revision": func(doc map[string]any) {
			doc["arms"].([]any)[0].(map[string]any)["base_sha"] = "5d5952f000000000000000000000000000000000"
		},
		"incomplete": func(doc map[string]any) { doc["complete"] = false },
	}
	for name, edit := range cases {
		t.Run(name, func(t *testing.T) {
			if result := statusOf(t, check(t, mutate(t, fixture(t, "good-compare"), edit)), "arms"); result.Status != Fail {
				t.Fatalf("arms = %s (%s), want FAIL", result.Status, result.Detail)
			}
		})
	}
}

func TestSingleClusterbenchReport(t *testing.T) {
	var combined struct {
		Runs []struct {
			Report map[string]any `json:"report"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(fixture(t, "good-compare"), &combined); err != nil {
		t.Fatal(err)
	}
	single := combined.Runs[1].Report
	single["machine_type"], single["disk_type"] = "n2-standard-8", "local NVMe SSD"
	data, err := json.Marshal(single)
	if err != nil {
		t.Fatal(err)
	}
	report, err := Check(data, Options{Identity: testIdentity, Expect: expectations(t)[1:]})
	if err != nil {
		t.Fatal(err)
	}
	if report.Kind != KindClusterbench || report.Failed() {
		t.Fatalf("kind %q, results %+v", report.Kind, report.Results)
	}
	if result := statusOf(t, report, "arms"); result.Status != Pass {
		t.Fatalf("arms = %s (%s)", result.Status, result.Detail)
	}
}

func TestPrivacyScan(t *testing.T) {
	cases := []struct {
		name, text string
		want       Status
	}{
		{"clean", `{"kernel":"6.8.0-1015-gcp","go":"go1.22.10","cpu":"Xeon @ 2.80GHz","at":"03:23:45","dir":"/mnt/bench"}`, Pass},
		{"public ipv4", `{"err":"dial tcp 10.128.0.7:7000"}`, Fail},
		{"ipv6", `{"err":"dial tcp [fe80::4001:aff:fe80:7]:7000"}`, Fail},
		{"loopback only", `{"err":"dial tcp 127.0.0.1:54321"}`, Warn},
		{"short hostname", `{"kernel":"built on bench-vm-7"}`, Fail},
		{"username", `{"who":"benchuser"}`, Fail},
		{"own home", `{"dir":"/home/benchuser/bench"}`, Fail},
		{"other home", `{"dir":"/home/someone/bench"}`, Fail},
		{"mac home", `{"dir":"/Users/someone/bench"}`, Fail},
		{"name inside a word", `{"dir":"/mnt/benchusers2"}`, Pass},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scanPrivacy(tc.text, testIdentity); got.Status != tc.want {
				t.Fatalf("scanPrivacy(%s) = %s (%s), want %s", tc.text, got.Status, got.Detail, tc.want)
			}
		})
	}
}

func TestPrivacyScanSearchesForbiddenWords(t *testing.T) {
	text := `{"kernel":"Linux version 6.8.0 (buildd@other-vm-3)"}`
	if got := scanPrivacy(text, testIdentity); got.Status != Pass {
		t.Fatalf("another machine's name without -forbid = %s, want PASS", got.Status)
	}
	id := testIdentity
	id.Forbidden = []string{"other-vm-3"}
	if got := scanPrivacy(text, id); got.Status != Fail || !strings.Contains(got.Detail, "other-vm-3") {
		t.Fatalf("forbidden word = %s (%s), want FAIL naming it", got.Status, got.Detail)
	}
}

func TestWriteListsChecksAndRuns(t *testing.T) {
	report := check(t, fixture(t, "wrong-primitive"))
	var out bytes.Buffer
	if err := Write(&out, report); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"FAIL  fsync.primitive", "PASS  privacy", "WARN  protocol",
		"412.5", "405.9", // throughput of each run
		"0.61/1.42", "0.64/1.55", // fsync before of base, after of tip
		"ack_resend=31.4", "result: FAIL",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("output lacks %q:\n%s", want, text)
		}
	}
}

func TestCheckRejectsUnrecognisedInput(t *testing.T) {
	for _, data := range []string{`not json`, `{"something":"else"}`} {
		if _, err := Check([]byte(data), Options{}); err == nil {
			t.Errorf("Check(%q) accepted unrecognised input", data)
		}
	}
}
