package db

// DurabilityMode selects the authority responsible for recovering writes.
type DurabilityMode uint8

const (
	// DurabilityEmbedded uses the DB's own WAL and locally allocated sequence numbers.
	DurabilityEmbedded DurabilityMode = iota
	// DurabilityReplica uses an external replicated log and externally supplied indexes.
	DurabilityReplica
)

// Options controls DB behaviour.
type Options struct {
	// FlushThreshold is the number of memtable entries that triggers a flush to an SSTable.
	// A value ≤ 0 uses the default of 1000.
	FlushThreshold int

	// CompactionThreshold is the number of SSTables that triggers a compaction.
	// A value ≤ 0 uses the default of 4.
	CompactionThreshold int

	// DurabilityMode defaults to DurabilityEmbedded. Replica mode is intended for
	// the Raft state-machine adapter and accepts writes through ApplyBatch only.
	DurabilityMode DurabilityMode
}

// DefaultOptions returns Options suitable for most uses.
func DefaultOptions() *Options { return &Options{} }

// flushThreshold returns the effective flush threshold, applying the default when unset.
func (o *Options) flushThreshold() int {
	if o == nil || o.FlushThreshold <= 0 {
		return 1000
	}
	return o.FlushThreshold
}

// compactionThreshold returns the effective compaction threshold, applying the default when unset.
func (o *Options) compactionThreshold() int {
	if o == nil || o.CompactionThreshold <= 0 {
		return 4
	}
	return o.CompactionThreshold
}
