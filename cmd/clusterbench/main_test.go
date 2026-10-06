package main

import "testing"

func TestOfficialRunsNeedMachineAndDiskType(t *testing.T) {
	rejected := [][]string{
		{"-official"},
		{"-official", "-machine-type", "n2-standard-8"},
		{"-official", "-disk-type", "pd-ssd"},
		{"-official", "-machine-type", " ", "-disk-type", "pd-ssd"},
	}
	for _, args := range rejected {
		if _, err := parseFlags(args); err == nil {
			t.Errorf("parseFlags(%q) accepted an official run without both labels", args)
		}
	}
	opts, err := parseFlags([]string{"-official", "-machine-type", "n2-standard-8", "-disk-type", "pd-ssd"})
	if err != nil {
		t.Fatalf("labelled official run rejected: %v", err)
	}
	if opts.machineType != "n2-standard-8" || opts.diskType != "pd-ssd" {
		t.Fatalf("labels = %q, %q", opts.machineType, opts.diskType)
	}
	if _, err := parseFlags(nil); err != nil {
		t.Fatalf("secondary run without labels rejected: %v", err)
	}
}

func TestReportRecordsMachineAndDiskType(t *testing.T) {
	opts, err := parseFlags([]string{"-machine-type", "m4 laptop", "-disk-type", "internal nvme"})
	if err != nil {
		t.Fatal(err)
	}
	report := newReport(opts, "secondary")
	if report.MachineType != "m4 laptop" || report.DiskType != "internal nvme" {
		t.Fatalf("report labels = %q, %q", report.MachineType, report.DiskType)
	}
}
