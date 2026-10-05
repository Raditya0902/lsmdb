// Package benchcompare holds the pure logic behind scripts/bench_compare.go:
// arm specifications, the interleaved run order, and the summary of a
// combined result file. It runs no processes and touches no git state.
package benchcompare

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// SchemaVersion identifies the layout of a Combined result file.
const SchemaVersion = 1

// driverFlags are clusterbench flags the driver sets on every invocation, or
// labels the driver records itself because older arms' binaries lack them.
var driverFlags = map[string]bool{
	"clients": true, "repetitions": true, "out": true, "machine-type": true, "disk-type": true,
}

var armName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Arm is one side of a comparison: a git revision, optionally with commits
// cherry-picked onto it in order.
type Arm struct {
	Name  string   `json:"name"`
	Rev   string   `json:"rev"`
	Picks []string `json:"picks,omitempty"`
}

// ArmBuild records the exact commits an arm's binary was built from.
type ArmBuild struct {
	Arm
	BaseSHA    string   `json:"base_sha"`
	HeadSHA    string   `json:"head_sha"`
	PickedSHAs []string `json:"picked_shas,omitempty"`
}

// Slot is one clusterbench invocation in the interleaved schedule.
type Slot struct {
	Sequence   int    `json:"sequence"`
	Repetition int    `json:"repetition"`
	Clients    int    `json:"clients"`
	Arm        string `json:"arm"`
}

// ArmRun is one completed slot with the clusterbench report it produced.
type ArmRun struct {
	Slot
	Report json.RawMessage `json:"report"`
}

// Combined is the merged output of one comparison. Complete is false until
// every scheduled slot has finished, so a crashed run is recognisable.
// NumCPU and GOMAXPROCS are the driver's own: the arms run as its children on
// the same machine, but their pinned binaries may predate recording NumCPU.
type Combined struct {
	SchemaVersion int        `json:"schema_version"`
	Complete      bool       `json:"complete"`
	Clients       []int      `json:"clients"`
	Repetitions   int        `json:"repetitions"`
	ExtraArgs     []string   `json:"clusterbench_args"`
	MachineType   string     `json:"machine_type"`
	DiskType      string     `json:"disk_type"`
	NumCPU        int        `json:"num_cpu"`
	GOMAXPROCS    int        `json:"gomaxprocs"`
	Arms          []ArmBuild `json:"arms"`
	Runs          []ArmRun   `json:"runs"`
}

// ParseArmSpec parses "name=rev" or "name=rev+pick1,pick2". The name becomes
// a directory name; revisions may not start with '-' so they cannot be read
// as git options.
func ParseArmSpec(spec string) (Arm, error) {
	name, rest, ok := strings.Cut(spec, "=")
	name, rest = strings.TrimSpace(name), strings.TrimSpace(rest)
	if !ok || !armName.MatchString(name) {
		return Arm{}, fmt.Errorf("arm %q: want name=rev[+pick,...] with a name of letters, digits, '-' or '_'", spec)
	}
	rev, picks, hasPicks := strings.Cut(rest, "+")
	arm := Arm{Name: name, Rev: strings.TrimSpace(rev)}
	if err := checkRev(arm.Rev); err != nil {
		return Arm{}, fmt.Errorf("arm %q: %w", spec, err)
	}
	if !hasPicks {
		return arm, nil
	}
	for _, pick := range strings.Split(picks, ",") {
		pick = strings.TrimSpace(pick)
		if err := checkRev(pick); err != nil {
			return Arm{}, fmt.Errorf("arm %q cherry-pick: %w", spec, err)
		}
		arm.Picks = append(arm.Picks, pick)
	}
	return arm, nil
}

func checkRev(rev string) error {
	switch {
	case rev == "":
		return errors.New("empty revision")
	case strings.HasPrefix(rev, "-"):
		return fmt.Errorf("revision %q starts with '-'", rev)
	case strings.ContainsAny(rev, " \t\n"):
		return fmt.Errorf("revision %q contains whitespace", rev)
	}
	return nil
}

// Schedule interleaves arms: each repetition runs every client count on every
// arm, and the arm that goes first rotates with the repetition so slow drift
// in the machine does not favour one arm.
func Schedule(arms []string, clients []int, repetitions int) []Slot {
	var slots []Slot
	for rep := 1; rep <= repetitions; rep++ {
		for _, count := range clients {
			for i := range arms {
				arm := arms[(i+rep-1)%len(arms)]
				slots = append(slots, Slot{Sequence: len(slots) + 1, Repetition: rep, Clients: count, Arm: arm})
			}
		}
	}
	return slots
}

// ValidateExtraArgs rejects pass-through arguments that would override the
// flags the driver sets for each slot.
func ValidateExtraArgs(args []string) error {
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if driverFlags[name] {
			return fmt.Errorf("clusterbench flag -%s is set by the driver; use the driver's own -%s", name, name)
		}
	}
	return nil
}

// RequireMachineLabels rejects an official comparison (-official among the
// pass-through arguments) unless both the machine and disk type are named.
func RequireMachineLabels(extra []string, machineType, diskType string) error {
	if !passesOfficial(extra) {
		return nil
	}
	if strings.TrimSpace(machineType) == "" || strings.TrimSpace(diskType) == "" {
		return errors.New("an -official comparison requires the driver's -machine-type and -disk-type")
	}
	return nil
}

func passesOfficial(extra []string) bool {
	official := false
	for _, arg := range extra {
		name, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if !strings.HasPrefix(arg, "-") || name != "official" {
			continue
		}
		official = true
		if hasValue {
			official, _ = strconv.ParseBool(value)
		}
	}
	return official
}
