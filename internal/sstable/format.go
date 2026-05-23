// Package sstable implements SSTable (Sorted String Table) writing and reading.
//
// On-disk layout:
//
//	[data records...]
//	[metadata block: minKeyLen(4) | minKey | maxKeyLen(4) | maxKey]
//	[bloom filter:   numBits(8) | numHash(8) | bits...]
//	[index entries:  keyLen(4) | key | offset(8) ...]
//	[footer (48 bytes): metaOffset(8) | bloomOffset(8) | bloomLen(8) |
//	                    indexOffset(8) | indexLen(8) | recordCount(8)]
//
// Data record format:
//
//	| keyLen(4) | valLen(4) | seqNum(8) | type(1) | key | value |
package sstable

// indexStep is the sparse index sampling rate: one index entry every indexStep records.
const indexStep = 16

// footerSize is the fixed byte length of the SSTable footer.
const footerSize = 48

// recordHeaderSize is the fixed-length prefix of every data record.
// Layout: keyLen(4) | valLen(4) | seqNum(8) | type(1)
const recordHeaderSize = 17

// sstFooter holds the decoded footer of an SSTable file.
type sstFooter struct {
	metaOffset  int64
	bloomOffset int64
	bloomLen    int64
	indexOffset int64
	indexLen    int64
	recordCount int64
}

// indexEntry is one entry in the sparse in-memory index loaded by the Reader.
type indexEntry struct {
	key    string
	offset int64 // byte offset of the record in the data section
}
