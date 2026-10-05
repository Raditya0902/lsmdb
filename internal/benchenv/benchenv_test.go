package benchenv

import (
	"encoding/json"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	ms := func(values ...int) []time.Duration {
		out := make([]time.Duration, len(values))
		for i, v := range values {
			out[i] = time.Duration(v) * time.Millisecond
		}
		return out
	}
	thousand := make([]time.Duration, 1000)
	for i := range thousand {
		thousand[i] = time.Duration(i+1) * time.Millisecond
	}
	cases := []struct {
		name   string
		sorted []time.Duration
		q      float64
		want   time.Duration
	}{
		{"empty", nil, 0.5, 0},
		{"single p50", ms(7), 0.5, 7 * time.Millisecond},
		{"single max", ms(7), 1.0, 7 * time.Millisecond},
		{"two p50 is lower", ms(1, 2), 0.5, time.Millisecond},
		{"two p99 is upper", ms(1, 2), 0.99, 2 * time.Millisecond},
		{"ten p50", ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 0.5, 5 * time.Millisecond},
		{"ten p99 is max", ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 0.99, 10 * time.Millisecond},
		{"thousand p99", thousand, 0.99, 990 * time.Millisecond},
		{"thousand p99.9", thousand, 0.999, 999 * time.Millisecond},
		{"thousand max", thousand, 1.0, 1000 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Percentile(tc.sorted, tc.q); got != tc.want {
				t.Fatalf("Percentile(q=%v) = %v, want %v", tc.q, got, tc.want)
			}
		})
	}
}

func TestFsyncMicrobenchmarkRecordsSamples(t *testing.T) {
	dir := t.TempDir()
	stats, err := MeasureFsync(dir, 20)
	if err != nil {
		t.Fatalf("MeasureFsync: %v", err)
	}
	if stats.Samples != 20 || stats.BlockBytes != 4096 {
		t.Fatalf("stats = %+v, want 20 samples of 4096 bytes", stats)
	}
	if stats.P50Ms <= 0 || stats.P99Ms < stats.P50Ms {
		t.Fatalf("implausible fsync stats %+v", stats)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("microbenchmark left files behind: %v %v", entries, err)
	}
}

func TestFsyncPrimitiveForOS(t *testing.T) {
	cases := map[string]string{
		"darwin":  "fcntl(F_FULLFSYNC)",
		"linux":   "fsync(2)",
		"freebsd": "fsync(2)",
	}
	for goos, want := range cases {
		if got := FsyncPrimitive(goos); !strings.HasPrefix(got, want) {
			t.Errorf("FsyncPrimitive(%q) = %q, want prefix %q", goos, got, want)
		}
	}
}

func TestResultPathNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	var names []string
	for i := 0; i < 3; i++ {
		f, err := CreateResultFile(dir, "cluster", "0123456789abcdef", now)
		if err != nil {
			t.Fatalf("CreateResultFile %d: %v", i, err)
		}
		if _, err := f.WriteString("run"); err != nil {
			t.Fatal(err)
		}
		names = append(names, filepath.Base(f.Name()))
		_ = f.Close()
	}
	want := []string{
		"2026-10-05-0123456789ab-cluster.json",
		"2026-10-05-0123456789ab-cluster-2.json",
		"2026-10-05-0123456789ab-cluster-3.json",
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names = %v, want %v", names, want)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, want[0]))
	if err != nil || string(data) != "run" {
		t.Fatalf("first file was modified: %q %v", data, err)
	}
}

var ipv4 = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)

func TestEnvironmentContainsNoIdentifyingData(t *testing.T) {
	env := Collect(t.TempDir())
	encoded, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	if env.GOOS != runtime.GOOS || env.GoVersion == "" || env.FsyncPrimitive == "" {
		t.Fatalf("environment missing basic fields: %s", text)
	}
	forbidden := map[string]string{}
	if host, err := os.Hostname(); err == nil && host != "" {
		forbidden["hostname"] = host
	}
	if current, err := user.Current(); err == nil {
		forbidden["username"] = current.Username
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		forbidden["home directory"] = home
	}
	for label, value := range forbidden {
		if len(value) >= 3 && strings.Contains(text, value) {
			t.Errorf("environment contains %s %q: %s", label, value, text)
		}
	}
	if match := ipv4.FindString(text); match != "" {
		t.Errorf("environment contains an IPv4-like address %q: %s", match, text)
	}
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err == nil && !ip.IsLoopback() && strings.Contains(text, ip.String()) {
				t.Errorf("environment contains interface address %s", ip)
			}
		}
	}
	if strings.Contains(text, `"/Users/`) || strings.Contains(text, `"/home/`) {
		t.Errorf("environment contains an absolute user path: %s", text)
	}
}
