// Package benchcheck holds the logic behind scripts/vm_smoke_check.go: it
// reads a clusterbench report or a bench_compare combined file and checks
// that it was recorded on a properly described Linux machine, leaks nothing
// identifying, and measured what it claims. It runs no processes.
package benchcheck

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"lsmdb/internal/benchcompare"
)

// Status is the outcome of one check. Only Fail makes a report fail.
type Status string

// Check outcomes.
const (
	Pass Status = "PASS"
	Fail Status = "FAIL"
	Warn Status = "WARN"
	Skip Status = "SKIP"
)

// Input kinds.
const (
	KindCompare      = "bench_compare"
	KindClusterbench = "clusterbench"
)

// CheckNames lists every check in the order Check reports them.
var CheckNames = []string{
	"env.os", "env.cpu_count", "env.data_dir_fs", "privacy", "fsync.primitive",
	"fsync.samples", "labels", "arms", "validity", "protocol",
}

// Identity names the machine doing the check; none of it may appear in a
// result file. Forbidden adds names of another machine, such as the VM's
// hostname when its file is checked elsewhere.
type Identity struct {
	Hostname  string
	Username  string
	Home      string
	Forbidden []string
}

// Options configures Check. Expect lists the arms the file must hold, in
// bench_compare's name=rev[+pick,...] form; for a single clusterbench report
// it may hold one arm without picks.
type Options struct {
	Identity Identity
	Expect   []benchcompare.Arm
}

// Result is the outcome of one named check.
type Result struct {
	Name   string
	Status Status
	Detail string
}

// Report is everything Check found, ready for Write.
type Report struct {
	Kind    string
	Label   string
	Arms    int
	Results []Result
	Runs    []RunRow
}

// Failed reports whether any check failed.
func (r Report) Failed() bool {
	for _, result := range r.Results {
		if result.Status == Fail {
			return true
		}
	}
	return false
}

// RunRow is one measured run, for the per-run table.
type RunRow struct {
	Arm string
	run runView
}

type envView struct {
	GitSHA         string `json:"git_sha"`
	GitDirty       bool   `json:"git_dirty"`
	GOOS           string `json:"goos"`
	NumCPU         *int   `json:"num_cpu"`
	GOMAXPROCS     int    `json:"gomaxprocs"`
	CPUModel       string `json:"cpu_model"`
	FsyncPrimitive string `json:"fsync_primitive"`
	Filesystem     string `json:"filesystem"`
	MountOptions   string `json:"mount_options"`
}

type fsyncView struct {
	P50Ms *float64 `json:"p50_ms"`
	P99Ms *float64 `json:"p99_ms"`
}

type runView struct {
	Repetition        int     `json:"repetition"`
	Clients           int     `json:"clients"`
	Valid             *bool   `json:"valid"`
	InvalidReason     string  `json:"invalid_reason"`
	ElectionsInWindow uint64  `json:"elections_in_window"`
	Throughput        float64 `json:"throughput_ops_per_sec"`
	Latency           struct {
		P50 float64 `json:"p50"`
		P99 float64 `json:"p99"`
	} `json:"latency_ms"`
	FsyncBefore fsyncView `json:"fsync_before"`
	FsyncAfter  fsyncView `json:"fsync_after"`
	Counters    struct {
		SyncsPerEntry   float64            `json:"log_syncs_per_committed_entry"`
		EntriesPerSync  float64            `json:"entries_per_log_sync"`
		AppendsPerEntry float64            `json:"append_messages_per_committed_entry"`
		ByOrigin        map[string]float64 `json:"append_messages_per_committed_entry_by_origin"`
	} `json:"counters"`
}

type configView struct {
	WarmupSeconds   *float64 `json:"warmup_seconds"`
	DurationSeconds *float64 `json:"duration_seconds"`
	Repetitions     *int     `json:"repetitions"`
}

type reportView struct {
	Label       string     `json:"label"`
	MachineType string     `json:"machine_type"`
	DiskType    string     `json:"disk_type"`
	Environment envView    `json:"environment"`
	Config      configView `json:"config"`
	Runs        []runView  `json:"runs"`
}

// unit is one clusterbench report: the whole file, or one slot of a comparison.
type unit struct {
	name   string // how problems refer to it, e.g. "slot 2 (tip)"
	arm    string
	report reportView
}

// input is a parsed file of either kind. numCPU and gomaxprocs come from the
// bench_compare driver, or from a clusterbench report's own environment.
type input struct {
	kind                  string
	machineType, diskType string
	numCPU, gomaxprocs    *int
	repetitions           *int
	complete              bool
	clients               []int
	arms                  []benchcompare.ArmBuild
	units                 []unit
}

// Check parses data and runs every check against it.
func Check(data []byte, opts Options) (Report, error) {
	in, err := parse(data)
	if err != nil {
		return Report{}, err
	}
	report := Report{Kind: in.kind, Label: labels(in), Arms: len(in.arms)}
	report.Results = []Result{
		checkOS(in), checkCPUCount(in), checkDataDirFS(in), scanPrivacy(string(data), opts.Identity),
		checkPrimitive(in), checkFsyncSamples(in), checkLabels(in), checkArms(in, opts.Expect),
		checkValidity(in), checkProtocol(in),
	}
	for _, u := range in.units {
		for _, run := range u.report.Runs {
			report.Runs = append(report.Runs, RunRow{Arm: u.arm, run: run})
		}
	}
	return report, nil
}

func parse(data []byte) (input, error) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return input{}, fmt.Errorf("not a JSON object: %w", err)
	}
	if _, ok := probe["arms"]; ok {
		return parseCompare(data)
	}
	if _, ok := probe["environment"]; ok {
		var report reportView
		if err := json.Unmarshal(data, &report); err != nil {
			return input{}, fmt.Errorf("clusterbench report: %w", err)
		}
		return input{
			kind: KindClusterbench, machineType: report.MachineType, diskType: report.DiskType,
			numCPU: report.Environment.NumCPU, gomaxprocs: &report.Environment.GOMAXPROCS,
			repetitions: report.Config.Repetitions, complete: true,
			units: []unit{{name: "report", report: report}},
		}, nil
	}
	return input{}, errors.New("neither a bench_compare file (no \"arms\") nor a clusterbench report (no \"environment\")")
}

func parseCompare(data []byte) (input, error) {
	var file struct {
		Complete    bool                    `json:"complete"`
		Clients     []int                   `json:"clients"`
		Repetitions *int                    `json:"repetitions"`
		MachineType string                  `json:"machine_type"`
		DiskType    string                  `json:"disk_type"`
		NumCPU      *int                    `json:"num_cpu"`
		GOMAXPROCS  *int                    `json:"gomaxprocs"`
		Arms        []benchcompare.ArmBuild `json:"arms"`
		Runs        []struct {
			Sequence int             `json:"sequence"`
			Arm      string          `json:"arm"`
			Report   json.RawMessage `json:"report"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return input{}, fmt.Errorf("bench_compare file: %w", err)
	}
	in := input{
		kind: KindCompare, machineType: file.MachineType, diskType: file.DiskType,
		numCPU: file.NumCPU, gomaxprocs: file.GOMAXPROCS,
		repetitions: file.Repetitions, complete: file.Complete, clients: file.Clients, arms: file.Arms,
	}
	for _, run := range file.Runs {
		var report reportView
		if err := json.Unmarshal(run.Report, &report); err != nil {
			return input{}, fmt.Errorf("slot %d report: %w", run.Sequence, err)
		}
		name := fmt.Sprintf("slot %d (%s)", run.Sequence, run.Arm)
		in.units = append(in.units, unit{name: name, arm: run.Arm, report: report})
	}
	return in, nil
}

func labels(in input) string {
	seen := map[string]bool{}
	for _, u := range in.units {
		seen[u.report.Label] = true
	}
	return strings.Join(sortedKeys(seen), ", ")
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
