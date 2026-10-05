//go:build ignore

// Command vm_smoke_check checks a clusterbench report or a bench_compare
// combined file before its numbers are trusted, and prints PASS, FAIL, WARN or
// SKIP per check followed by a table of every run.
//
//	go run scripts/vm_smoke_check.go \
//	    -expect base=bd67696+ed0e92e,8eae967 -expect tip=8eae967 <file.json>
//
// The privacy check searches for this machine's hostname, username and home
// directory, plus every -forbid name; checking a VM's file elsewhere needs the
// VM's names passed with -forbid. It exits 1 if any check fails. Warnings,
// such as a non-official protocol, do not fail it.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/user"

	"lsmdb/internal/benchcheck"
	"lsmdb/internal/benchcompare"
)

func main() {
	flags := flag.NewFlagSet("vm_smoke_check", flag.ExitOnError)
	var opts benchcheck.Options
	flags.Func("expect", "name=rev[+pick,...] as passed to bench_compare -arm; repeat per arm", func(spec string) error {
		arm, err := benchcompare.ParseArmSpec(spec)
		opts.Expect = append(opts.Expect, arm)
		return err
	})
	flags.Func("forbid", "another name that must not appear, e.g. the VM's hostname or username when checking its file elsewhere; repeatable", func(word string) error {
		opts.Identity.Forbidden = append(opts.Identity.Forbidden, word)
		return nil
	})
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: go run scripts/vm_smoke_check.go [-expect name=rev[+pick,...]]... [-forbid name]... <file.json>")
		flags.PrintDefaults()
	}
	_ = flags.Parse(os.Args[1:])
	if flags.NArg() != 1 {
		flags.Usage()
		os.Exit(2)
	}
	data, err := os.ReadFile(flags.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "vm_smoke_check:", err)
		os.Exit(2)
	}
	opts.Identity = identity(opts.Identity.Forbidden)
	report, err := benchcheck.Check(data, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "vm_smoke_check:", err)
		os.Exit(2)
	}
	if err := benchcheck.Write(os.Stdout, report); err != nil {
		fmt.Fprintln(os.Stderr, "vm_smoke_check:", err)
		os.Exit(2)
	}
	if report.Failed() {
		os.Exit(1)
	}
}

// identity describes the machine running the check; a lookup that fails
// leaves that field empty and the generic patterns still apply.
func identity(forbidden []string) benchcheck.Identity {
	id := benchcheck.Identity{Forbidden: forbidden}
	id.Hostname, _ = os.Hostname()
	if u, err := user.Current(); err == nil {
		id.Username = u.Username
	} else {
		id.Username = os.Getenv("USER")
	}
	id.Home, _ = os.UserHomeDir()
	return id
}
