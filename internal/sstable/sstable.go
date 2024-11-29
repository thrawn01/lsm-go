package sstable

type CompressionCodec int8

const (
	CompressionNone CompressionCodec = iota
	CompressionSnappy
	CompressionZlib
	CompressionLz4
	CompressionZstd
)

// Info contains meta information about the SSTable
type Info struct {
	// contains the firstKey of the SSTable
	firstKey []byte

	// the offset at which SSTableIndex starts when SSTable is serialized.
	// SSTableIndex holds the meta info about each block. SSTableIndex is defined in schemas/sst.fbs
	indexOffset uint64

	// the length of the SSTableIndex.
	indexLen uint64

	// the offset at which Bloom filter starts when SSTable is serialized.
	filterOffset uint64

	// the length of the Bloom filter
	filterLen uint64

	// the codec used to compress/decompress SSTable before writing/reading from object storage
	compressionCodec CompressionCodec
}

func (s *Info) Clone() *Info {
	var firstKey []byte
	if s.firstKey != nil {
		firstKey = make([]byte, len(s.firstKey))
		copy(firstKey, s.firstKey)
	}
	return &Info{
		firstKey:         firstKey,
		indexOffset:      s.indexOffset,
		indexLen:         s.indexLen,
		filterOffset:     s.filterOffset,
		filterLen:        s.filterLen,
		compressionCodec: s.compressionCodec,
	}
}

// Codec provides optional implementations for encoding and decoding SSTableInfo
type Codec interface {
	Encode(info *Info) []byte
	Decode(data []byte) *Info
}
