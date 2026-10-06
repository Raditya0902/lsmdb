package db

import "time"

// Stats counts the flushes and compactions this DB has published since Open.
// FlushTime excludes the compaction a flush may trigger; a flush of an empty
// memtable is not counted.
type Stats struct {
	Flushes        uint64
	FlushTime      time.Duration
	Compactions    uint64
	CompactionTime time.Duration
}

// Stats returns the flush and compaction counts and their total time. It
// takes no lock.
func (db *DB) Stats() Stats {
	return Stats{
		Flushes:        db.flushes.Load(),
		FlushTime:      time.Duration(db.flushNanos.Load()),
		Compactions:    db.compactions.Load(),
		CompactionTime: time.Duration(db.compactionNanos.Load()),
	}
}
