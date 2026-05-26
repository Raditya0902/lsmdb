package tests

import (
	"fmt"
	"testing"

	"lsmdb/db"
)

func TestScan_BasicRange(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		if err := d.Set(k, []byte("v:"+k)); err != nil {
			t.Fatal(err)
		}
	}

	pairs, err := d.Scan("c", "g")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"c", "d", "e", "f", "g"}
	if len(pairs) != len(want) {
		t.Fatalf("got %d pairs, want %d: %v", len(pairs), len(want), pairs)
	}
	for i, p := range pairs {
		if p.Key != want[i] {
			t.Errorf("pairs[%d].Key = %q, want %q", i, p.Key, want[i])
		}
	}
}

func TestScan_TombstoneExclusion(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := d.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Delete("c"); err != nil {
		t.Fatal(err)
	}

	pairs, err := d.Scan("a", "e")
	if err != nil {
		t.Fatal(err)
	}

	for _, p := range pairs {
		if p.Key == "c" {
			t.Error("deleted key 'c' must not appear in scan results")
		}
	}
	if len(pairs) != 4 {
		t.Errorf("got %d pairs, want 4", len(pairs))
	}
}

func TestScan_MemTableSSTableMerge(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := d.Set(k, []byte("v1:"+k)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"f", "g", "h", "i", "j"} {
		if err := d.Set(k, []byte("v1:"+k)); err != nil {
			t.Fatal(err)
		}
	}

	pairs, err := d.Scan("a", "j")
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	if len(pairs) != len(want) {
		t.Fatalf("got %d pairs, want %d", len(pairs), len(want))
	}
	for i, p := range pairs {
		if p.Key != want[i] {
			t.Errorf("pairs[%d].Key = %q, want %q", i, p.Key, want[i])
		}
	}
}

func TestScan_MemTableOverridesSSTable(t *testing.T) {
	dir := t.TempDir()
	d, err := db.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.Set("x", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := d.ForceFlush(); err != nil {
		t.Fatal(err)
	}
	if err := d.Set("x", []byte("new")); err != nil {
		t.Fatal(err)
	}

	pairs, err := d.Scan("x", "x")
	if err != nil {
		t.Fatal(err)
	}

	if len(pairs) != 1 {
		t.Fatalf("got %d pairs, want 1", len(pairs))
	}
	if string(pairs[0].Value) != "new" {
		t.Errorf("got value %q, want %q", pairs[0].Value, "new")
	}
}

func TestScan_EmptyRange(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	pairs, err := d.Scan("a", "z")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected empty result, got %d pairs", len(pairs))
	}
}

func TestScan_SingleKey(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.Set("m", []byte("v")); err != nil {
		t.Fatal(err)
	}

	pairs, err := d.Scan("m", "m")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 1 || pairs[0].Key != "m" {
		t.Errorf("expected [{m v}], got %v", pairs)
	}
}

func TestScan_FromBeyondMax(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := d.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	pairs, err := d.Scan("z", "zz")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected empty result, got %d pairs", len(pairs))
	}
}

func TestScan_ToBelowMin(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for _, k := range []string{"e", "f", "g"} {
		if err := d.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	pairs, err := d.Scan("a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 0 {
		t.Errorf("expected empty result, got %d pairs", len(pairs))
	}
}

func TestScan_SortedOrder(t *testing.T) {
	d, err := db.Open("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	// Insert in reverse order to verify scan returns ascending.
	for i := 9; i >= 0; i-- {
		k := fmt.Sprintf("key%02d", i)
		if err := d.Set(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	pairs, err := d.Scan("key00", "key09")
	if err != nil {
		t.Fatal(err)
	}
	if len(pairs) != 10 {
		t.Fatalf("got %d pairs, want 10", len(pairs))
	}
	for i, p := range pairs {
		want := fmt.Sprintf("key%02d", i)
		if p.Key != want {
			t.Errorf("pairs[%d].Key = %q, want %q", i, p.Key, want)
		}
	}
}
