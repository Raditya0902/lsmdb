package tests

import (
	"bytes"
	"testing"

	"lsmdb/db"
)

func openDB(t *testing.T) *db.DB {
	t.Helper()
	d, err := db.Open("", db.DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestCorrectness(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(d *db.DB)
		key     string
		wantVal []byte
		wantOk  bool
	}{
		{
			name:    "set then get",
			setup:   func(d *db.DB) { d.Set("k", []byte("v")) },
			key:     "k",
			wantVal: []byte("v"),
			wantOk:  true,
		},
		{
			name:    "missing key",
			setup:   func(d *db.DB) {},
			key:     "missing",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "delete existing key",
			setup: func(d *db.DB) {
				d.Set("k", []byte("v"))
				d.Delete("k")
			},
			key:     "k",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "overwrite value",
			setup: func(d *db.DB) {
				d.Set("k", []byte("v1"))
				d.Set("k", []byte("v2"))
			},
			key:     "k",
			wantVal: []byte("v2"),
			wantOk:  true,
		},
		{
			name: "tombstone beats old value",
			setup: func(d *db.DB) {
				d.Set("k", []byte("v"))
				d.Delete("k")
			},
			key:     "k",
			wantVal: nil,
			wantOk:  false,
		},
		{
			name: "set after delete",
			setup: func(d *db.DB) {
				d.Delete("k")
				d.Set("k", []byte("v"))
			},
			key:     "k",
			wantVal: []byte("v"),
			wantOk:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := openDB(t)
			tc.setup(d)
			got, ok := d.Get(tc.key)
			if ok != tc.wantOk {
				t.Errorf("Get(%q) ok = %v, want %v", tc.key, ok, tc.wantOk)
			}
			if !bytes.Equal(got, tc.wantVal) {
				t.Errorf("Get(%q) value = %q, want %q", tc.key, got, tc.wantVal)
			}
		})
	}
}
