// Package bloom implements a space-efficient probabilistic membership filter.
// Hashing is deterministic: FNV-1a for h1, bit-rotation of h1 for h2,
// with k positions computed via h(i) = h1 + i*h2 (Kirsch–Mitzenmacher).
// No runtime-randomised hash functions are used.
package bloom

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic set-membership structure.
// MayContain never returns false for a key that was passed to Add.
type BloomFilter struct {
	bits    []uint64
	numBits uint64
	numHash uint64
}

// New returns a BloomFilter sized for expectedKeys elements at the given
// false-positive rate. numBits is rounded up to the next multiple of 64.
func New(expectedKeys int, falsePositiveRate float64) *BloomFilter {
	if expectedKeys <= 0 {
		expectedKeys = 1
	}
	n := float64(expectedKeys)
	p := falsePositiveRate

	// m = -n·ln(p) / (ln2)²
	mTheory := -n * math.Log(p) / (math.Ln2 * math.Ln2)

	// k = (m/n)·ln2, clamped to [1, 30]
	k := math.Round((mTheory / n) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	// Round numBits up to the next multiple of 64 for uint64 storage.
	m := uint64(math.Ceil(mTheory))
	m = ((m + 63) / 64) * 64

	return &BloomFilter{
		bits:    make([]uint64, m/64),
		numBits: m,
		numHash: uint64(k),
	}
}

// Add inserts key into the filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		bf.bits[pos/64] |= 1 << (pos % 64)
	}
}

// MayContain returns false if key is definitely not in the set, or true if
// key might be in the set. Never returns false for a key passed to Add.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < bf.numHash; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if bf.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// Encode serialises the filter to bytes.
// Format: | numBits(8) | numHash(8) | bits... |
func (bf *BloomFilter) Encode() []byte {
	buf := make([]byte, 16+len(bf.bits)*8)
	binary.BigEndian.PutUint64(buf[0:], bf.numBits)
	binary.BigEndian.PutUint64(buf[8:], bf.numHash)
	for i, word := range bf.bits {
		binary.BigEndian.PutUint64(buf[16+i*8:], word)
	}
	return buf
}

// Decode deserialises a BloomFilter from the bytes produced by Encode.
func Decode(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("bloom: too short to decode (%d bytes)", len(data))
	}
	numBits := binary.BigEndian.Uint64(data[0:])
	numHash := binary.BigEndian.Uint64(data[8:])
	if numBits == 0 || numBits%64 != 0 {
		return nil, fmt.Errorf("bloom: invalid numBits %d (must be a nonzero multiple of 64)", numBits)
	}
	numWords := numBits / 64
	if uint64(len(data)-16) < numWords*8 {
		return nil, fmt.Errorf("bloom: data too short for %d bits (need %d bytes, have %d)", numBits, 16+numWords*8, len(data))
	}
	bits := make([]uint64, numWords)
	for i := range bits {
		bits[i] = binary.BigEndian.Uint64(data[16+i*8:])
	}
	return &BloomFilter{bits: bits, numBits: numBits, numHash: numHash}, nil
}

// bloomHashes returns two independent 64-bit hashes of key.
// h1 is FNV-1a; h2 is h1 rotated left 17 bits with the lowest bit forced to 1
// to ensure every position in the bit array is reachable.
func bloomHashes(key []byte) (h1, h2 uint64) {
	hw := fnv.New64a()
	hw.Write(key)
	h1 = hw.Sum64()
	h2 = ((h1 << 17) | (h1 >> 47)) | 1
	return
}
