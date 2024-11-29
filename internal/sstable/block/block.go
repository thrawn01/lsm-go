package block

import (
	"encoding/binary"
	"errors"
	"github.com/thrawn01/lsm-go/internal/assert"
	"math"
)

var ErrEmptyBlock = errors.New("empty block")

const (
	sizeOfUint16InBytes = 2
	sizeOfUint32InBytes = 4
	Tombstone           = math.MaxUint32
)

type Block struct {
	Offsets []uint16
	Data    []byte
}

// Builder builds a block of key value
type Builder struct {
	offsets   []uint16
	data      []byte
	blockSize uint64
}

func NewBuilder(blockSize uint64) *Builder {
	return &Builder{
		offsets:   make([]uint16, 0),
		data:      make([]byte, 0),
		blockSize: blockSize,
	}
}

// estimatedSize estimates the number of key-value pairs in the block
func (b *Builder) estimatedSize() int {
	return sizeOfUint16InBytes +
		(len(b.offsets) * sizeOfUint16InBytes) + // offsets
		len(b.data) // key-value pairs
}

func (b *Builder) Add(key []byte, value []byte) bool {
	assert.True(len(key) > 0, "key must not be empty")

	valueLen := 0
	if len(value) > 0 {
		valueLen = len(value)
	}
	newSize := b.estimatedSize() + len(key) + valueLen + (sizeOfUint16InBytes * 2) + sizeOfUint32InBytes

	// If adding the key-value pair would exceed the block size limit, don't add it.
	// (Unless the block is empty, in which case, allow the block to exceed the limit.)
	if uint64(newSize) > b.blockSize && !b.IsEmpty() {
		return false
	}

	b.offsets = append(b.offsets, uint16(len(b.data)))

	// If value is present then append KeyLength(uint16), Key, ValueLength(uint32), value.
	// if value is absent then append KeyLength(uint16), Key, Tombstone(uint32)
	b.data = binary.BigEndian.AppendUint16(b.data, uint16(len(key)))
	b.data = append(b.data, key...)
	if valueLen > 0 {
		b.data = binary.BigEndian.AppendUint32(b.data, uint32(valueLen))
		b.data = append(b.data, value...)
	} else {
		b.data = binary.BigEndian.AppendUint32(b.data, Tombstone)
	}
	return true
}

func (b *Builder) IsEmpty() bool {
	return len(b.offsets) == 0
}

func (b *Builder) Build() (*Block, error) {
	if b.IsEmpty() {
		return nil, ErrEmptyBlock
	}
	return &Block{
		Data:    b.data,
		Offsets: b.offsets,
	}, nil
}

// Encode encodes the Block into a byte slice.
// - Block.Data is appended to the first len(data) bytes
// - Block.Offsets is appended to the next len(offsets) * sizeOfUint16InBytes bytes
// the last 2 bytes hold the number of offsets
func Encode(b *Block) []byte {
	bufSize := len(b.Data) + len(b.Offsets)*sizeOfUint16InBytes + sizeOfUint16InBytes

	buf := make([]byte, 0, bufSize)
	buf = append(buf, b.Data...)

	for _, offset := range b.Offsets {
		buf = binary.BigEndian.AppendUint16(buf, offset)
	}
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b.Offsets)))
	return buf
}

// Decode converts the encoded byte slice into the provided Block
func Decode(b *Block, bytes []byte) {
	// The last 2 bytes hold the offset count
	offsetCountIndex := len(bytes) - sizeOfUint16InBytes
	offsetCount := binary.BigEndian.Uint16(bytes[offsetCountIndex:])

	offsetStartIndex := offsetCountIndex - (int(offsetCount) * sizeOfUint16InBytes)
	offsets := make([]uint16, 0, offsetCount)

	for i := 0; i < int(offsetCount); i++ {
		index := offsetStartIndex + (i * sizeOfUint16InBytes)
		offsets = append(offsets, binary.BigEndian.Uint16(bytes[index:]))
	}

	b.Data = bytes[:offsetStartIndex]
	b.Offsets = offsets
}
