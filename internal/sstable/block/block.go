package block

import (
	"encoding/binary"
	"errors"
	"github.com/thrawn01/lsm-go/internal/assert"
	"github.com/thrawn01/lsm-go/internal/compress"
	"github.com/thrawn01/lsm-go/internal/flatbuf"
	"github.com/thrawn01/lsm-go/internal/sstable/types"
	"hash/crc32"
)

var (
	ErrEmptyBlock     = errors.New("empty block")
	ErrChecksumFailed = errors.New("block checksum failed")
)

type Block struct {
	Meta    flatbuf.BlockMetaT
	Offsets []uint16
	Data    []byte
}

func (b *Block) FirstKey() []byte {
	if len(b.Offsets) == 0 {
		return nil
	}
	keyLen := binary.BigEndian.Uint16(b.Data[b.Offsets[0]:])
	return b.Data[b.Offsets[0]+2 : b.Offsets[0]+2+keyLen]
}

type Builder struct {
	offsets   []uint16
	data      []byte
	blockSize uint64
}

// NewBuilder builds a block of key values in the following format
//
// +-----------------------------------------------+
// |               KeyValue                        |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Key Length (2 bytes)                   |  |
// |  +-----------------------------------------+  |
// |  |  Key                                    |  |
// |  +-----------------------------------------+  |
// |  |  Value Length (4 bytes)                 |  |
// |  +-----------------------------------------+  |
// |  |  Value                                  |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
//
// If it is a tombstone then KeyValue is represented as.
//
// +-----------------------------------------------+
// |               KeyValue (Tombstone)            |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Key Length (2 bytes)                   |  |
// |  +-----------------------------------------+  |
// |  |  Key                                    |  |
// |  +-----------------------------------------+  |
// |  |  Tombstone (4 bytes)                    |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
//
// The returned Block struct contains the Data as described
// and the Offsets of each key value in the block.
func NewBuilder(blockSize uint64) *Builder {
	return &Builder{
		offsets:   make([]uint16, 0),
		data:      make([]byte, 0),
		blockSize: blockSize,
	}
}

// estimatedSize estimates the number of key-value pairs in the block
func (b *Builder) estimatedSize() int {
	return types.SizeOfUint16 +
		(len(b.offsets) * types.SizeOfUint16) + // offsets
		len(b.data) // key-value pairs
}

func (b *Builder) Add(key []byte, value []byte) bool {
	assert.True(len(key) > 0, "key must not be empty")

	valueLen := 0
	if len(value) > 0 {
		valueLen = len(value)
	}
	newSize := b.estimatedSize() + len(key) + valueLen + (types.SizeOfUint16 * 2) + types.SizeOfUint32

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
		b.data = binary.BigEndian.AppendUint32(b.data, types.Tombstone)
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

// Encode encodes the Block into a byte slice with the following format
//
// +-----------------------------------------------+
// |               Block                           |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  Block.Data                             |  |
// |  |  (List of KeyValues)                    |  |
// |  |  +-----------------------------------+  |  |
// |  |  | KeyValue Pair                     |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                 |  |  |
// |  +-----------------------------------------+  |
// |  +-----------------------------------------+  |
// |  |  Block.Offsets                          |  |
// |  |  +-----------------------------------+  |  |
// |  |  |  Offset of KeyValue (4 bytes)     |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  Number of Offsets (2 bytes)            |  |
// |  +-----------------------------------------+  |
// |  |  CRC32 Checksum (4 bytes)               |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
func Encode(b *Block, codec compress.Codec) ([]byte, error) {
	bufSize := len(b.Data) + len(b.Offsets)*types.SizeOfUint16 + types.SizeOfUint16 + 4 // +4 for CRC32

	buf := make([]byte, 0, bufSize)
	buf = append(buf, b.Data...)

	for _, offset := range b.Offsets {
		buf = binary.BigEndian.AppendUint16(buf, offset)
	}

	buf = binary.BigEndian.AppendUint16(buf, uint16(len(b.Offsets)))

	var err error
	buf, err = compress.Encode(buf, codec)
	if err != nil {
		return nil, err
	}

	// Calculate CRC32 checksum
	checksum := crc32.ChecksumIEEE(buf)
	buf = binary.BigEndian.AppendUint32(buf, checksum)
	return buf, nil
}

// Decode converts the encoded byte slice into the provided Block
func Decode(b *Block, bytes []byte, codec compress.Codec) error {
	assert.True(len(bytes) > 6, "invalid block; block is too small; must be at least 6 bytes")

	// Extract and verify checksum
	dataLen := len(bytes) - 4
	if binary.BigEndian.Uint32(bytes[dataLen:]) != crc32.ChecksumIEEE(bytes[:dataLen]) {
		return ErrChecksumFailed
	}

	// Decompress the data (excluding the checksum)
	uncompressed, err := compress.Decode(bytes[:dataLen], codec)
	if err != nil {
		return err
	}

	// The last 2 bytes of the decompressed data hold the offset count
	offset := len(uncompressed) - 2
	offsetCount := binary.BigEndian.Uint16(uncompressed[offset:])

	offsetStartIndex := offset - (int(offsetCount) * types.SizeOfUint16)
	offsets := make([]uint16, 0, offsetCount)

	for i := 0; i < int(offsetCount); i++ {
		index := offsetStartIndex + (i * types.SizeOfUint16)
		offsets = append(offsets, binary.BigEndian.Uint16(uncompressed[index:]))
	}

	b.Data = uncompressed[:offsetStartIndex]
	b.Offsets = offsets

	return nil
}
