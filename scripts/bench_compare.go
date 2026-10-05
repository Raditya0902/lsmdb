//go:build ignore

// Command bench_compare runs clusterbench on several git revisions with the
// runs interleaved, then merges and summarises the results.
//
//	go run scripts/bench_compare.go run \
//	    -arm base=bd67696+<item1>,<item2> -arm tip=HEAD \
//	    -clients 1,2,4,8,16 -repetitions 5 -machine-type <type> -disk-type <type> \
//	    -- -official -warmup 5s -duration 30s
//	go run scripts/bench_compare.go summarize <combined.json>
//
// Each arm is built in its own detached git worktree from committed revisions
// only; uncommitted changes in the main checkout are never included. A
// cherry-pick conflict aborts the run without resolving anything. Worktrees
// are removed on exit; per-run reports stay in the work directory. The machine
// and disk type are recorded by the driver, not passed to the arms, whose
// clusterbench binaries may predate those flags.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"lsmdb/internal/benchcompare"
	"lsmdb/internal/benchenv"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "run":
		err = runCommand(ctx, os.Args[2:])
	case "summarize":
		err = summarizeCommand(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench_compare:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: go run scripts/bench_compare.go run -arm name=rev[+pick,...] ... [-clients 1,4] [-repetitions 5] [-out file] [-work dir] [-machine-type t -disk-type t] [-- clusterbench flags]")
	fmt.Fprintln(os.Stderr, "       go run scripts/bench_compare.go summarize <combined.json>")
	os.Exit(2)
}

type runOptions struct {
	arms        []benchcompare.Arm
	clients     []int
	repetitions int
	out, work   string
	machineType string
	diskType    string
	extra       []string
}

func parseRunFlags(args []string) (runOptions, error) {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	opts := runOptions{}
	flags.Func("arm", "name=rev[+pick1,pick2]; repeat for each arm, the first is the baseline", func(spec string) error {
		arm, err := benchcompare.ParseArmSpec(spec)
		opts.arms = append(opts.arms, arm)
		return err
	})
	clients := flags.String("clients", "1,2,4,8,16", "comma-separated client counts")
	flags.IntVar(&opts.repetitions, "repetitions", 5, "interleaved repetitions per arm and client count")
	flags.StringVar(&opts.out, "out", "", "combined result file to create (never overwritten); default <date>-<sha>-compare.json in the OS temp dir")
	flags.StringVar(&opts.work, "work", "", "work directory for worktrees, binaries and per-run reports; default a new OS temp dir")
	flags.StringVar(&opts.machineType, "machine-type", "", "machine or instance type, recorded in the combined file; required with -official")
	flags.StringVar(&opts.diskType, "disk-type", "", "disk under the clusterbench -data-dir, recorded in the combined file; required with -official")
	if err := flags.Parse(args); err != nil {
		return runOptions{}, err
	}
	opts.extra = flags.Args()
	if err := benchcompare.ValidateExtraArgs(opts.extra); err != nil {
		return runOptions{}, err
	}
	if err := benchcompare.RequireMachineLabels(opts.extra, opts.machineType, opts.diskType); err != nil {
		return runOptions{}, err
	}
	if len(opts.arms) < 2 {
		return runOptions{}, errors.New("need at least two -arm flags")
	}
	seen := map[string]bool{}
	for _, arm := range opts.arms {
		if seen[arm.Name] {
			return runOptions{}, fmt.Errorf("duplicate arm name %q", arm.Name)
		}
		seen[arm.Name] = true
	}
	for _, field := range strings.Split(*clients, ",") {
		count, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || count <= 0 {
			return runOptions{}, fmt.Errorf("invalid -clients entry %q", field)
		}
		opts.clients = append(opts.clients, count)
	}
	if opts.repetitions <= 0 {
		return runOptions{}, errors.New("-repetitions must be positive")
	}
	return opts, nil
}

func runCommand(ctx context.Context, args []string) error {
	opts, err := parseRunFlags(args)
	if err != nil {
		return err
	}
	root, err := git(ctx, "", "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	if opts.work == "" {
		if opts.work, err = os.MkdirTemp("", "bench-compare-"); err != nil {
			return err
		}
	}
	head, err := resolve(ctx, root, "HEAD")
	if err != nil {
		return err
	}
	if opts.out != "" {
		if _, err := os.Stat(opts.out); err == nil {
			return fmt.Errorf("-out %s already exists; results are never overwritten", opts.out)
		}
	}
	fmt.Fprintf(os.Stderr, "bench_compare: work dir %s\n", opts.work)

	combined := benchcompare.Combined{
		SchemaVersion: benchcompare.SchemaVersion, Clients: opts.clients,
		Repetitions: opts.repetitions, ExtraArgs: append([]string{}, opts.extra...),
		MachineType: opts.machineType, DiskType: opts.diskType,
	}
	binaries := map[string]string{}
	worktrees := map[string]string{}
	defer func() {
		for name, dir := range worktrees {
			if _, err := git(context.Background(), root, "worktree", "remove", "--force", dir); err != nil {
				fmt.Fprintf(os.Stderr, "bench_compare: remove worktree %s: %v\n", name, err)
			}
		}
	}()
	for _, arm := range opts.arms {
		dir := filepath.Join(opts.work, "worktrees", arm.Name)
		build, err := prepareArm(ctx, root, dir, arm, worktrees)
		if err != nil {
			return err
		}
		bin := filepath.Join(opts.work, "bin", arm.Name+"-clusterbench")
		if err := command(ctx, dir, "go", "build", "-o", bin, "./cmd/clusterbench").Run(); err != nil {
			return fmt.Errorf("arm %s: build clusterbench: %w", arm.Name, err)
		}
		binaries[arm.Name] = bin
		combined.Arms = append(combined.Arms, build)
		fmt.Fprintf(os.Stderr, "arm %s: %s (base %s, picked %v)\n", arm.Name, short(build.HeadSHA), short(build.BaseSHA), build.PickedSHAs)
	}

	// Created only once every arm has built, so a failed setup leaves no empty file.
	out, err := createOutput(opts.out, head)
	if err != nil {
		return err
	}
	defer out.Close()
	fmt.Fprintf(os.Stderr, "bench_compare: writing %s\n", out.Name())

	names := make([]string, len(opts.arms))
	for i, arm := range opts.arms {
		names[i] = arm.Name
	}
	slots := benchcompare.Schedule(names, opts.clients, opts.repetitions)
	for _, slot := range slots {
		fmt.Fprintf(os.Stderr, "[%d/%d] arm %s, %d clients, repetition %d\n", slot.Sequence, len(slots), slot.Arm, slot.Clients, slot.Repetition)
		report, err := runSlot(ctx, opts, slot, binaries[slot.Arm], worktrees[slot.Arm])
		if err != nil {
			return err
		}
		combined.Runs = append(combined.Runs, benchcompare.ArmRun{Slot: slot, Report: report})
		if err := rewrite(out, combined); err != nil {
			return err
		}
	}
	combined.Complete = true
	if err := rewrite(out, combined); err != nil {
		return err
	}
	summary, err := benchcompare.Summarize(combined)
	if err != nil {
		return err
	}
	return benchcompare.WriteSummary(os.Stdout, summary)
}

// prepareArm creates the arm's worktree and applies its cherry-picks. On a
// conflict it aborts the cherry-pick and reports the paths; it never resolves.
func prepareArm(ctx context.Context, root, dir string, arm benchcompare.Arm, worktrees map[string]string) (benchcompare.ArmBuild, error) {
	base, err := resolve(ctx, root, arm.Rev)
	if err != nil {
		return benchcompare.ArmBuild{}, fmt.Errorf("arm %s: %w", arm.Name, err)
	}
	picks := make([]string, len(arm.Picks))
	for i, pick := range arm.Picks {
		if picks[i], err = resolve(ctx, root, pick); err != nil {
			return benchcompare.ArmBuild{}, fmt.Errorf("arm %s: %w", arm.Name, err)
		}
	}
	if _, err := git(ctx, root, "worktree", "add", "--detach", dir, base); err != nil {
		return benchcompare.ArmBuild{}, fmt.Errorf("arm %s: %w", arm.Name, err)
	}
	worktrees[arm.Name] = dir
	if len(picks) > 0 {
		if _, err := git(ctx, dir, append([]string{"cherry-pick"}, picks...)...); err != nil {
			conflicts, _ := git(context.Background(), dir, "diff", "--name-only", "--diff-filter=U")
			_, _ = git(context.Background(), dir, "cherry-pick", "--abort")
			return benchcompare.ArmBuild{}, fmt.Errorf("arm %s: cherry-pick onto %s failed; aborted without resolving. Conflicting paths:\n%s\n%w",
				arm.Name, short(base), conflicts, err)
		}
	}
	head, err := resolve(ctx, dir, "HEAD")
	if err != nil {
		return benchcompare.ArmBuild{}, err
	}
	build := benchcompare.ArmBuild{Arm: arm, BaseSHA: base, HeadSHA: head}
	if len(picks) > 0 {
		list, err := git(ctx, dir, "rev-list", "--reverse", base+".."+head)
		if err != nil {
			return benchcompare.ArmBuild{}, err
		}
		build.PickedSHAs = strings.Fields(list)
	}
	return build, nil
}

// runSlot runs one clusterbench invocation from the arm's worktree, so the
// git revision it records is the arm's own.
func runSlot(ctx context.Context, opts runOptions, slot benchcompare.Slot, bin, dir string) (json.RawMessage, error) {
	path := filepath.Join(opts.work, "runs", fmt.Sprintf("%03d-%s-c%d-r%d.json", slot.Sequence, slot.Arm, slot.Clients, slot.Repetition))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	args := append([]string{"-clients=" + strconv.Itoa(slot.Clients), "-repetitions=1", "-out=" + path}, opts.extra...)
	if err := command(ctx, dir, bin, args...).Run(); err != nil {
		return nil, fmt.Errorf("slot %d (arm %s, %d clients): %w", slot.Sequence, slot.Arm, slot.Clients, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("slot %d wrote invalid JSON to %s", slot.Sequence, path)
	}
	return json.RawMessage(data), nil
}

func summarizeCommand(args []string) error {
	if len(args) != 1 {
		usage()
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	var combined benchcompare.Combined
	if err := json.Unmarshal(data, &combined); err != nil {
		return fmt.Errorf("%s: %w", args[0], err)
	}
	summary, err := benchcompare.Summarize(combined)
	if err != nil {
		return err
	}
	return benchcompare.WriteSummary(os.Stdout, summary)
}

func createOutput(path, sha string) (*os.File, error) {
	if path == "" {
		return benchenv.CreateResultFile(os.TempDir(), "compare", sha, time.Now())
	}
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
}

// rewrite replaces the contents of the file this run created, so a crash
// leaves the runs completed so far with complete=false.
func rewrite(out *os.File, combined benchcompare.Combined) error {
	if err := out.Truncate(0); err != nil {
		return err
	}
	if _, err := out.Seek(0, 0); err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(combined); err != nil {
		return err
	}
	return out.Sync()
}

func command(ctx context.Context, dir, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd
}

func git(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(string(out)), nil
}

func resolve(ctx context.Context, dir, rev string) (string, error) {
	return git(ctx, dir, "rev-parse", "--verify", "--end-of-options", rev+"^{commit}")
}

func short(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
