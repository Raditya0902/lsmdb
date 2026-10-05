package benchcheck

import (
	"fmt"
	"strconv"
	"strings"

	"lsmdb/internal/benchcompare"
)

// Official protocol from benchmarks/RUNBOOK.md section 3.
const (
	officialWarmupSeconds   = 5
	officialDurationSeconds = 30
	officialRepetitions     = 5
	wantPrimitive           = "fsync(2)"
	shortSHALength          = 12
	minCommitPrefix         = 7
)

// perUnit runs describe on every report and fails with the units it rejects.
// Passing details are the distinct values describe returned.
func perUnit(name string, in input, describe func(unit) (string, error)) Result {
	if len(in.units) == 0 {
		return Result{name, Fail, "no reports in the file"}
	}
	values := map[string]bool{}
	var problems []string
	for _, u := range in.units {
		value, err := describe(u)
		if err != nil {
			problems = append(problems, u.name+": "+err.Error())
			continue
		}
		values[value] = true
	}
	if len(problems) > 0 {
		return Result{name, Fail, strings.Join(problems, "; ")}
	}
	return Result{name, Pass, fmt.Sprintf("%s in %d/%d reports", strings.Join(sortedKeys(values), " | "), len(in.units), len(in.units))}
}

func recorded(value string) bool {
	return strings.TrimSpace(value) != "" && value != "unknown"
}

func checkOS(in input) Result {
	return perUnit("env.os", in, func(u unit) (string, error) {
		if u.report.Environment.GOOS != "linux" {
			return "", fmt.Errorf("goos is %q, want linux", u.report.Environment.GOOS)
		}
		return "linux", nil
	})
}

func checkCPUCount(in input) Result {
	return perUnit("env.cpu_count", in, func(u unit) (string, error) {
		env := u.report.Environment
		if env.GOMAXPROCS <= 0 {
			return "", fmt.Errorf("gomaxprocs not recorded")
		}
		return fmt.Sprintf("gomaxprocs %d, %s", env.GOMAXPROCS, env.CPUModel), nil
	})
}

func checkDataDirFS(in input) Result {
	return perUnit("env.data_dir_fs", in, func(u unit) (string, error) {
		env := u.report.Environment
		if !recorded(env.Filesystem) || !recorded(env.MountOptions) {
			return "", fmt.Errorf("filesystem %q, mount_options %q: both must be recorded", env.Filesystem, env.MountOptions)
		}
		return fmt.Sprintf("%s (%s)", env.Filesystem, env.MountOptions), nil
	})
}

func checkPrimitive(in input) Result {
	return perUnit("fsync.primitive", in, func(u unit) (string, error) {
		if got := u.report.Environment.FsyncPrimitive; got != wantPrimitive {
			return "", fmt.Errorf("fsync_primitive %q, want %q", got, wantPrimitive)
		}
		return wantPrimitive, nil
	})
}

func checkFsyncSamples(in input) Result {
	runs := 0
	var problems []string
	for _, u := range in.units {
		for _, run := range u.report.Runs {
			runs++
			for _, probe := range []struct {
				name  string
				value *float64
			}{
				{"before p50", run.FsyncBefore.P50Ms}, {"before p99", run.FsyncBefore.P99Ms},
				{"after p50", run.FsyncAfter.P50Ms}, {"after p99", run.FsyncAfter.P99Ms},
			} {
				if probe.value == nil || *probe.value <= 0 {
					problems = append(problems, fmt.Sprintf("%s rep %d clients %d: fsync %s missing", u.name, run.Repetition, run.Clients, probe.name))
				}
			}
		}
	}
	switch {
	case len(problems) > 0:
		return Result{"fsync.samples", Fail, strings.Join(problems, "; ")}
	case runs == 0:
		return Result{"fsync.samples", Fail, "no runs in the file"}
	}
	return Result{"fsync.samples", Pass, fmt.Sprintf("before and after p50/p99 present in %d/%d runs; values in the runs table", runs, runs)}
}

func checkLabels(in input) Result {
	if !recorded(in.machineType) || !recorded(in.diskType) {
		return Result{"labels", Fail, fmt.Sprintf("machine_type %q, disk_type %q: both must be recorded", in.machineType, in.diskType)}
	}
	return Result{"labels", Pass, fmt.Sprintf("machine %q, disk %q", in.machineType, in.diskType)}
}

func checkValidity(in input) Result {
	total, invalid, elections := 0, 0, 0
	var problems []string
	for _, u := range in.units {
		for _, run := range u.report.Runs {
			total++
			switch {
			case run.Valid == nil:
				problems = append(problems, fmt.Sprintf("%s rep %d clients %d has no valid field", u.name, run.Repetition, run.Clients))
			case !*run.Valid:
				invalid++
				if run.ElectionsInWindow > 0 || strings.Contains(run.InvalidReason, "term changed") {
					elections++
				}
			}
		}
	}
	detail := fmt.Sprintf("%d invalid of %d runs, %d of them by an election in the window", invalid, total, elections)
	switch {
	case len(problems) > 0:
		return Result{"validity", Fail, detail + "; " + strings.Join(problems, "; ")}
	case total == 0:
		return Result{"validity", Fail, "no runs in the file"}
	case elections > 0:
		return Result{"validity", Fail, detail}
	case invalid > 0:
		return Result{"validity", Warn, detail}
	}
	return Result{"validity", Pass, detail}
}

func checkProtocol(in input) Result {
	warmups, durations := map[string]bool{}, map[string]bool{}
	var missing []string
	for _, u := range in.units {
		config := u.report.Config
		if config.WarmupSeconds == nil || config.DurationSeconds == nil {
			missing = append(missing, u.name)
			continue
		}
		warmups[seconds(*config.WarmupSeconds)] = true
		durations[seconds(*config.DurationSeconds)] = true
	}
	if in.repetitions == nil {
		missing = append(missing, "repetitions")
	}
	if len(missing) > 0 || len(in.units) == 0 {
		return Result{"protocol", Fail, "warmup, duration or repetitions not recorded: " + strings.Join(missing, ", ")}
	}
	detail := fmt.Sprintf("warmup %s, duration %s, repetitions %d", strings.Join(sortedKeys(warmups), "|"),
		strings.Join(sortedKeys(durations), "|"), *in.repetitions)
	official := len(warmups) == 1 && warmups[seconds(officialWarmupSeconds)] &&
		len(durations) == 1 && durations[seconds(officialDurationSeconds)] && *in.repetitions == officialRepetitions
	if !official {
		return Result{"protocol", Warn, fmt.Sprintf("%s; the official protocol is %s / %s / %d", detail,
			seconds(officialWarmupSeconds), seconds(officialDurationSeconds), officialRepetitions)}
	}
	return Result{"protocol", Pass, detail}
}

func seconds(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64) + "s"
}

func checkArms(in input, expect []benchcompare.Arm) Result {
	if in.kind == KindClusterbench {
		return checkSingleSHA(in, expect)
	}
	if len(expect) == 0 {
		return Result{"arms", Fail, "no expected arms given; pass -expect name=rev[+pick,...] for each arm"}
	}
	var problems []string
	if !in.complete {
		problems = append(problems, "the file is marked incomplete")
	}
	builds := map[string]benchcompare.ArmBuild{}
	for _, build := range in.arms {
		builds[build.Name] = build
	}
	var described []string
	for _, want := range expect {
		build, ok := builds[want.Name]
		if !ok {
			problems = append(problems, fmt.Sprintf("arm %s missing", want.Name))
			continue
		}
		delete(builds, want.Name)
		units := unitsOf(in, want.Name)
		problems = append(problems, armProblems(build, want, units, expectedRuns(in))...)
		described = append(described, fmt.Sprintf("%s %s+%d picks -> %s (%d runs)",
			want.Name, short(build.BaseSHA), len(build.PickedSHAs), short(build.HeadSHA), len(units)))
	}
	for _, name := range sortedKeys(namesOf(builds)) {
		problems = append(problems, fmt.Sprintf("unexpected arm %s", name))
	}
	if len(problems) > 0 {
		return Result{"arms", Fail, strings.Join(problems, "; ")}
	}
	return Result{"arms", Pass, strings.Join(described, "; ")}
}

func armProblems(build benchcompare.ArmBuild, want benchcompare.Arm, units []unit, wantRuns int) []string {
	var problems []string
	if !sameCommit(build.BaseSHA, want.Rev) {
		problems = append(problems, fmt.Sprintf("arm %s built on %s, expected %s", want.Name, short(build.BaseSHA), want.Rev))
	}
	if len(build.Picks) != len(want.Picks) || len(build.PickedSHAs) != len(want.Picks) {
		problems = append(problems, fmt.Sprintf("arm %s has picks %v (%d applied), expected %v",
			want.Name, build.Picks, len(build.PickedSHAs), want.Picks))
	} else {
		for i := range want.Picks {
			if !sameCommit(build.Picks[i], want.Picks[i]) {
				problems = append(problems, fmt.Sprintf("arm %s pick %d is %s, expected %s", want.Name, i+1, build.Picks[i], want.Picks[i]))
			}
		}
	}
	if wantRuns > 0 && len(units) != wantRuns {
		problems = append(problems, fmt.Sprintf("arm %s has %d runs, expected %d", want.Name, len(units), wantRuns))
	}
	for _, u := range units {
		env := u.report.Environment
		if env.GitSHA != build.HeadSHA {
			problems = append(problems, fmt.Sprintf("%s reports git_sha %s, arm head is %s", u.name, short(env.GitSHA), short(build.HeadSHA)))
		}
		if env.GitDirty {
			problems = append(problems, u.name+" was built from a dirty tree")
		}
	}
	return problems
}

func checkSingleSHA(in input, expect []benchcompare.Arm) Result {
	env := in.units[0].report.Environment
	switch {
	case len(expect) == 0:
		return Result{"arms", Skip, fmt.Sprintf("single clusterbench report built from %s; pass one -expect to check it", short(env.GitSHA))}
	case len(expect) > 1 || len(expect[0].Picks) > 0:
		return Result{"arms", Fail, "a clusterbench report has one binary; pass exactly one -expect without picks"}
	case !sameCommit(env.GitSHA, expect[0].Rev):
		return Result{"arms", Fail, fmt.Sprintf("built from %s, expected %s", short(env.GitSHA), expect[0].Rev)}
	case env.GitDirty:
		return Result{"arms", Fail, "built from a dirty tree"}
	}
	return Result{"arms", Pass, fmt.Sprintf("built from %s, clean", short(env.GitSHA))}
}

func expectedRuns(in input) int {
	if in.repetitions == nil {
		return 0
	}
	return len(in.clients) * *in.repetitions
}

func unitsOf(in input, arm string) []unit {
	var out []unit
	for _, u := range in.units {
		if u.arm == arm {
			out = append(out, u)
		}
	}
	return out
}

func namesOf(builds map[string]benchcompare.ArmBuild) map[string]bool {
	names := map[string]bool{}
	for name := range builds {
		names[name] = true
	}
	return names
}

// sameCommit reports whether two commit ids agree on their common prefix of
// at least minCommitPrefix hex digits.
func sameCommit(a, b string) bool {
	a, b = strings.ToLower(strings.TrimSpace(a)), strings.ToLower(strings.TrimSpace(b))
	n := min(len(a), len(b))
	return n >= minCommitPrefix && a[:n] == b[:n]
}

func short(sha string) string {
	if len(sha) > shortSHALength {
		return sha[:shortSHALength]
	}
	return sha
}
