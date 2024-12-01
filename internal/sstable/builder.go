package sstable

import (
	"encoding/binary"
	"github.com/thrawn01/lsm-go/internal/flatbuf"
	"github.com/thrawn01/lsm-go/internal/sstable/block"
	"github.com/thrawn01/lsm-go/internal/sstable/bloom"
)

// Builder builds the SSTable in the format outlined
// in the diagram below. The Builder uses the block.Builder
// to build the key value pairs and uses bloom.Builder to
// build the bloom filter if the  total number of keys in
// all blocks meet or exceeds Config.MinFilterKeys.
// Finally, it writes the sstable.Index and sstable.Info
// along with the offset of the sstable.Info
//
// +-----------------------------------------------+
// |               SSTable                         |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  List of Blocks                         |  |
// |  |  +-----------------------------------+  |  |
// |  |  |  block.Block                      |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  List of types.KeyValue pairs  |  |  |
// |  |  |  |  +---------------------------+ |  |  |
// |  |  |  |  |  Key Length (2 bytes)     | |  |  |
// |  |  |  |  |  Key                      | |  |  |
// |  |  |  |  |  Value Length (4 bytes)   | |  |  |
// |  |  |  |  |  Value                    | |  |  |
// |  |  |  |  +---------------------------+ |  |  |
// |  |  |  |  ...                           |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Offsets for each Key          |  |  |
// |  |  |  |  (n * 2 bytes)                 |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Number of Offsets (2 bytes)   |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Checksum (4 bytes)            |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  bloom.Filter (if MinFilterKeys met)    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  sstable.Index                          |  |
// |  |  (List of Block Offsets)                |  |
// |  |  - Block Offset (End of Block)          |  |
// |  |  - FirstKey of this Block               |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  sstable.Info                           |  |
// |  |  - Offset of BloomFilter                |  |
// |  |  - Length of BloomFilter                |  |
// |  |  - Offset of sstable.Index              |  |
// |  |  - Length of sstable.Index              |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  Offset of sstable.Info (4 bytes)       |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
type Builder struct {
	conf         Config
	blockBuilder *block.Builder
	blocks       []*block.Block
	bloomBuilder *bloom.Builder
	keyCount     int
	firstKey     []byte
	lastKey      []byte
}

// NewBuilder creates a new builder used to encode an SSTable
func NewBuilder(conf Config) *Builder {
	return &Builder{
		conf:         conf,
		blockBuilder: block.NewBuilder(uint64(conf.BlockSize)),
		bloomBuilder: bloom.NewFilterBuilder(uint32(conf.FilterBitsPerKey)),
	}
}

// Add a key and value to the SSTable.
func (bu *Builder) Add(key, value []byte) error {
	if len(bu.blocks) == 0 && bu.blockBuilder.IsEmpty() {
		bu.firstKey = make([]byte, len(key))
		copy(bu.firstKey, key)
	}

	if !bu.blockBuilder.Add(key, value) {
		// Add returns false if current block is full.
		// Build the current block and start a new one
		blk, err := bu.blockBuilder.Build()
		if err != nil {
			return err
		}
		bu.blocks = append(bu.blocks, blk)
		bu.blockBuilder = block.NewBuilder(uint64(bu.conf.BlockSize))
		bu.blockBuilder.Add(key, value)
	}

	bu.bloomBuilder.Add(key)
	bu.keyCount++
	bu.lastKey = make([]byte, len(key))
	copy(bu.lastKey, key)

	return nil
}

func (bu *Builder) Build() *Table {
	// Finalize the last block if it's not empty
	if !bu.blockBuilder.IsEmpty() {
		blk, _ := bu.blockBuilder.Build()
		bu.blocks = append(bu.blocks, blk)
	}

	var data []byte
	// Encode the blocks
	for _, b := range bu.blocks {
		d, err := block.Encode(b, bu.conf.Compression)
		if err != nil {
			return nil
		}
		data = append(data, d...)
		b.Meta = flatbuf.BlockMetaT{
			Offset:   uint64(len(data)),
			FirstKey: b.FirstKey(),
		}
	}

	var bloomFilter *bloom.Filter
	if bu.keyCount >= bu.conf.MinFilterKeys {
		bloomFilter = bu.bloomBuilder.Build()
		data = append(data, bloom.Encode(bloomFilter)...)
	}

	// Build the index
	indexT := &flatbuf.SsTableIndexT{
		BlockMeta: make([]*flatbuf.BlockMetaT, len(bu.blocks)),
	}
	for i, b := range bu.blocks {
		indexT.BlockMeta[i] = &flatbuf.BlockMetaT{
			Offset:   b.Meta.Offset,
			FirstKey: b.Meta.FirstKey,
		}
	}
	indexBytes := encodeIndex(indexT)
	indexStartOffset := uint64(len(data))
	data = append(data, indexBytes...)

	// Build and Encode Info
	info := &Info{
		FirstKey:         bu.firstKey,
		IndexOffset:      indexStartOffset,
		IndexLen:         uint64(len(indexBytes)),
		CompressionCodec: bu.conf.Compression,
	}

	if bloomFilter != nil {
		info.FilterOffset = uint64(len(bu.blocks) * bu.conf.BlockSize)
		info.FilterLen = uint64(len(bloomFilter.Data) + 2) // +2 for NumProbes
	}

	infoBytes := encodeInfo(info)
	infoOffset := uint64(len(data))
	data = append(data, infoBytes...)

	// Append the offset of Info at the end
	data = binary.BigEndian.AppendUint64(data, infoOffset)

	return &Table{
		Info:  info,
		Bloom: bloomFilter,
		Data:  data,
	}
}
