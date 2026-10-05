// Package benchenv records the environment of a benchmark run, measures fsync
// latency on its data directory, and creates result files without overwriting.
//
// It deliberately records no hostname, username, absolute path, or network
// address, so result files can be published as-is.
package benchenv

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

const unknown = "unknown"

// Environment describes the machine and build that produced a result.
type Environment struct {
	GitSHA         string `json:"git_sha"`
	GitDirty       bool   `json:"git_dirty"`
	GoVersion      string `json:"go_version"`
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
	Kernel         string `json:"kernel"`
	CPUModel       string `json:"cpu_model"`
	RAMBytes       uint64 `json:"ram_bytes"`
	GOMAXPROCS     int    `json:"gomaxprocs"`
	FsyncPrimitive string `json:"fsync_primitive"`
	Filesystem     string `json:"filesystem"`
	MountOptions   string `json:"mount_options"`
	BlockDevice    string `json:"block_device"`
}

// Collect describes the current process and the filesystem holding dataDir.
// Values that cannot be detected are reported as "unknown".
func Collect(dataDir string) Environment {
	sha, dirty := gitRevision()
	env := Environment{
		GitSHA: sha, GitDirty: dirty, GoVersion: runtime.Version(),
		GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0),
		FsyncPrimitive: FsyncPrimitive(runtime.GOOS),
		Kernel:         unknown, CPUModel: unknown, Filesystem: unknown, MountOptions: unknown, BlockDevice: unknown,
	}
	switch runtime.GOOS {
	case "linux":
		collectLinux(&env, dataDir)
	case "darwin":
		collectDarwin(&env, dataDir)
	}
	return env
}

// FsyncPrimitive names the system call (*os.File).Sync issues on goos.
func FsyncPrimitive(goos string) string {
	switch goos {
	case "darwin", "ios":
		return "fcntl(F_FULLFSYNC), falling back to fsync(2) on ENOTSUP"
	case "windows":
		return "FlushFileBuffers"
	default:
		return "fsync(2)"
	}
}

// ShortSHA returns the first twelve characters of sha, or "unknown".
func ShortSHA(sha string) string {
	if len(sha) < 12 {
		return unknown
	}
	return sha[:12]
}

func gitRevision() (string, bool) {
	if info, ok := debug.ReadBuildInfo(); ok {
		var sha string
		dirty := false
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				sha = setting.Value
			case "vcs.modified":
				dirty = setting.Value == "true"
			}
		}
		if sha != "" {
			return sha, dirty
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return unknown, false
	}
	status, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output()
	return strings.TrimSpace(string(out)), err != nil || len(strings.TrimSpace(string(status))) > 0
}

func collectLinux(env *Environment, dataDir string) {
	// osrelease, not /proc/version: the latter names the kernel builder's host.
	if data, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		env.Kernel = strings.TrimSpace(string(data))
	}
	if value := procField("/proc/cpuinfo", "model name"); value != "" {
		env.CPUModel = value
	}
	if value := procField("/proc/meminfo", "MemTotal"); value != "" {
		fields := strings.Fields(value)
		if kib, err := strconv.ParseUint(fields[0], 10, 64); err == nil {
			env.RAMBytes = kib * 1024
		}
	}
	fsType, options, source := linuxMount(dataDir)
	if fsType != "" {
		env.Filesystem, env.MountOptions = fsType, options
	}
	if device := linuxBlockDevice(source); device != "" {
		env.BlockDevice = device
	}
}

func procField(path, key string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), ":")
		if ok && strings.TrimSpace(name) == key {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// linuxMount returns the filesystem type, mount options, and source device of
// the mount containing dir, from /proc/self/mountinfo.
func linuxMount(dir string) (fsType, options, source string) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", "", ""
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", "", ""
	}
	defer f.Close()
	best := -1
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		// Format: id parent major:minor root mountpoint options ... - fstype source superoptions
		before, after, ok := strings.Cut(scanner.Text(), " - ")
		if !ok {
			continue
		}
		left, right := strings.Fields(before), strings.Fields(after)
		if len(left) < 6 || len(right) < 3 {
			continue
		}
		mountPoint := left[4]
		within := abs == mountPoint || mountPoint == "/" || strings.HasPrefix(abs, mountPoint+"/")
		if within && len(mountPoint) > best {
			best = len(mountPoint)
			fsType, options, source = right[0], left[5]+","+right[2], right[1]
		}
	}
	return fsType, options, source
}

func linuxBlockDevice(source string) string {
	if !strings.HasPrefix(source, "/dev/") {
		return ""
	}
	name := filepath.Base(source)
	resolved, err := filepath.EvalSymlinks(filepath.Join("/sys/class/block", name))
	if err != nil {
		return ""
	}
	if _, err := os.Stat(filepath.Join(resolved, "partition")); err == nil {
		name = filepath.Base(filepath.Dir(resolved))
	}
	description := name
	if data, err := os.ReadFile(filepath.Join("/sys/block", name, "queue/rotational")); err == nil {
		description += " rotational=" + strings.TrimSpace(string(data))
	}
	if data, err := os.ReadFile(filepath.Join("/sys/block", name, "device/model")); err == nil {
		description += " model=" + strings.TrimSpace(string(data))
	}
	return description
}

func collectDarwin(env *Environment, dataDir string) {
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		env.Kernel = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
		env.CPUModel = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
		if bytes, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64); err == nil {
			env.RAMBytes = bytes
		}
	}
	fsType, device := darwinStatfs(dataDir)
	if fsType == "" {
		return
	}
	env.Filesystem = fsType
	// `mount` prints "<device> on <mountpoint> (<fstype>, <options>)"; keep only the options.
	if out, err := exec.Command("mount").Output(); err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if strings.HasPrefix(line, device+" on ") {
				if open := strings.LastIndex(line, "("); open >= 0 && strings.HasSuffix(line, ")") {
					env.MountOptions = line[open+1 : len(line)-1]
				}
			}
		}
	}
}

// MeasureFsync times samples rounds of a 4 KiB append plus fsync in a scratch
// file inside dir and removes the file afterwards.
func MeasureFsync(dir string, samples int) (FsyncStats, error) {
	const blockBytes = 4096
	if samples <= 0 {
		return FsyncStats{}, errors.New("fsync samples must be positive")
	}
	f, err := os.CreateTemp(dir, ".fsync-probe-*")
	if err != nil {
		return FsyncStats{}, fmt.Errorf("create fsync probe: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	block := make([]byte, blockBytes)
	durations := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if _, err := f.Write(block); err != nil {
			return FsyncStats{}, fmt.Errorf("write fsync probe: %w", err)
		}
		if err := f.Sync(); err != nil {
			return FsyncStats{}, fmt.Errorf("sync fsync probe: %w", err)
		}
		durations = append(durations, time.Since(start))
	}
	SortDurations(durations)
	return FsyncStats{
		Samples: samples, BlockBytes: blockBytes,
		P50Ms: Milliseconds(Percentile(durations, 0.50)), P99Ms: Milliseconds(Percentile(durations, 0.99)),
	}, nil
}

// FsyncStats summarizes a fsync microbenchmark.
type FsyncStats struct {
	Samples    int     `json:"samples"`
	BlockBytes int     `json:"block_bytes"`
	P50Ms      float64 `json:"p50_ms"`
	P99Ms      float64 `json:"p99_ms"`
}

// Percentile returns the nearest-rank q-quantile of ascending values: the
// element at index ceil(q*n)-1. It returns zero for an empty slice.
func Percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}

// SortDurations sorts values ascending in place.
func SortDurations(values []time.Duration) {
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
}

// Milliseconds converts d to fractional milliseconds.
func Milliseconds(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1e6 }

// CreateResultFile creates <dir>/<date>-<sha12>-<kind>.json exclusively,
// appending -2, -3, ... when a file with that name already exists.
func CreateResultFile(dir, kind, sha string, now time.Time) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create results dir: %w", err)
	}
	base := fmt.Sprintf("%s-%s-%s", now.Format("2006-01-02"), ShortSHA(sha), kind)
	for attempt := 1; attempt < 10000; attempt++ {
		name := base + ".json"
		if attempt > 1 {
			name = fmt.Sprintf("%s-%d.json", base, attempt)
		}
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create result file: %w", err)
		}
	}
	return nil, errors.New("too many result files with the same date and revision")
}
