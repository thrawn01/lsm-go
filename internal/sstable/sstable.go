package sstable

import (
	"github.com/thrawn01/lsm-go/internal/flatbuf"
	"github.com/thrawn01/lsm-go/internal/sstable/bloom"
)

// Info contains meta information about the SSTable
type Info struct {
	// contains the FirstKey of the SSTable
	FirstKey []byte

	// the offset at which SSTableIndex starts when SSTable is serialized.
	// SSTableIndex holds the meta info about each block. SSTableIndex is defined in schemas/sst.fbs
	IndexOffset uint64

	// the length of the SSTableIndex.
	IndexLen uint64

	// the offset at which Bloom filter starts when SSTable is serialized.
	FilterOffset uint64

	// the length of the Bloom filter
	FilterLen uint64

	// the codec used to compress/decompress SSTable before writing/reading from object storage
	CompressionCodec CompressionCodec
}

func (s *Info) Clone() *Info {
	var firstKey []byte
	if s.FirstKey != nil {
		firstKey = make([]byte, len(s.FirstKey))
		copy(firstKey, s.FirstKey)
	}
	return &Info{
		FirstKey:         firstKey,
		IndexOffset:      s.IndexOffset,
		IndexLen:         s.IndexLen,
		FilterOffset:     s.FilterOffset,
		FilterLen:        s.FilterLen,
		CompressionCodec: s.CompressionCodec,
	}
}

type Range struct {
	// The lower bound of the range (inclusive).
	Start uint64

	// The upper bound of the range (exclusive).
	End uint64
}

type ReadOnlyBlob interface {
	Len() (int, error)
	ReadRange(r Range) ([]byte, error)
	Read() ([]byte, error)
}

// Serializer provides optional implementations for encoding and decoding SSTableInfo
type Serializer interface {
	Encode(info *Info) []byte
	Decode(data []byte) *Info
}

type Config struct {
	// BlockSize is the size of each block in the SSTable
	BlockSize int

	// MinFilterKeys is the minimum number of keys that must exist in the SSTable
	// before a bloom filter is created. Reads on SSTables with a small number
	// of items is faster than looking up in a bloom filter.
	MinFilterKeys int

	FilterBitsPerKey int

	// The Serializer used to encode and decode sstable.Info from the SSTable.
	Serializer Serializer

	// The codec used to compress and decompress the SSTable
	Compression CompressionCodec
}

// Table is the in memory representation of an SSTable.
type Table struct {
	// Info contains the offset information used to parse the encoded table
	Info *Info

	// Bloom is the bloom filter associated with the encoded table. If
	// the bloom filter is not nil, can be used to identify if a key exists in this table.
	Bloom *bloom.Filter

	// Data is the encoded table suitable for writing to disk
	Data []byte
}

// Index is the in memory representation of the SSTable index
type Index struct {
	// Data contains the flat buffer encoded index
	Data []byte
}

// AsFlatBuf returns the Index marshalled into a flat buffer SSTableIndex struct
func (e Index) AsFlatBuf() *flatbuf.SsTableIndex {
	return flatbuf.GetRootAsSsTableIndex(e.Data, 0)
}

func (e Index) Size() int {
	return len(e.Data)
}

func (e Index) Clone() Index {
	data := make([]byte, len(e.Data))
	copy(data, e.Data)
	return Index{
		Data: data,
	}
}
