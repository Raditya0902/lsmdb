// Package compact implements size-tiered SSTable compaction.
// Compact performs a K-way merge across all input SSTables, retaining only the
// highest-seqNum record per key and optionally dropping tombstones at the
// bottom level (when no older SSTables exist below).
package compact

import (
	"container/heap"
	"fmt"

	"lsmdb/internal/memtable"
	"lsmdb/internal/sstable"
)

// Compact merges records from readers into a single new SSTable at outputPath.
// Only the record with the highest seqNum is kept for each key.
// If isBottomLevel is true, DELETE tombstones are dropped because no older
// SSTables exist that they need to hide; otherwise tombstones are preserved.
// Returns the number of records written. When the output is empty (all
// tombstones dropped) no file is created and 0 is returned.
func Compact(readers []*sstable.Reader, outputPath string, isBottomLevel bool) (int, error) {
	if len(readers) == 0 {
		return 0, nil
	}

	// Load all records from every reader. Each slice is sorted ascending by key.
	sources := make([][]memtable.Record, len(readers))
	totalIn := 0
	for i, r := range readers {
		recs, err := r.LoadAll()
		if err != nil {
			return 0, fmt.Errorf("compact: load reader %d: %w", i, err)
		}
		sources[i] = recs
		totalIn += len(recs)
	}

	if totalIn == 0 {
		return 0, nil
	}

	// Seed the heap with the first record from each non-empty source.
	h := &mergeHeap{}
	heap.Init(h)
	for i, src := range sources {
		if len(src) > 0 {
			heap.Push(h, entry{rec: src[0], srcIdx: i, recIdx: 0})
		}
	}

	var output []memtable.Record

	for h.Len() > 0 {
		// Pop the entry with the smallest key; for ties, highest seqNum is first.
		top := heap.Pop(h).(entry)
		winner := top.rec

		// Advance that source's cursor.
		advanceCursor(h, sources, top.srcIdx, top.recIdx)

		// Drain all remaining entries for the same key — they are older versions.
		for h.Len() > 0 && (*h)[0].rec.Key == winner.Key {
			dup := heap.Pop(h).(entry)
			advanceCursor(h, sources, dup.srcIdx, dup.recIdx)
			// dup.rec has a lower seqNum than winner — discard it.
		}

		// At the bottom level, tombstones can be dropped: no older data to hide.
		if winner.Type == memtable.RecordTypeDelete && isBottomLevel {
			continue
		}

		output = append(output, winner)
	}

	if len(output) == 0 {
		return 0, nil // all tombstones dropped — caller must not open the (nonexistent) file
	}

	if err := sstable.Write(outputPath, output); err != nil {
		return 0, fmt.Errorf("compact: write output: %w", err)
	}
	return len(output), nil
}

// advanceCursor pushes the next record from source srcIdx into the heap,
// if one exists past position recIdx.
func advanceCursor(h *mergeHeap, sources [][]memtable.Record, srcIdx, recIdx int) {
	next := recIdx + 1
	if next < len(sources[srcIdx]) {
		heap.Push(h, entry{rec: sources[srcIdx][next], srcIdx: srcIdx, recIdx: next})
	}
}

// ── heap implementation ────────────────────────────────────────────────────

type entry struct {
	rec    memtable.Record
	srcIdx int // which source slice this record came from
	recIdx int // index within that source slice
}

type mergeHeap []entry

func (h mergeHeap) Len() int { return len(h) }

// Less defines the heap ordering: smallest key first; for equal keys,
// highest seqNum first (so the winner is always popped before older versions).
func (h mergeHeap) Less(i, j int) bool {
	ki, kj := h[i].rec.Key, h[j].rec.Key
	if ki != kj {
		return ki < kj
	}
	return h[i].rec.SeqNum > h[j].rec.SeqNum
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(entry)) }

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
