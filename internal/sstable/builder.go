package sstable

import (
	"encoding/binary"
	flatbuffers "github.com/google/flatbuffers/go"
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
		// Current block is full, build the current block and start a new one
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

	var bloomFilter *bloom.Filter
	if bu.keyCount >= bu.conf.MinFilterKeys {
		bloomFilter = bu.bloomBuilder.Build()
	}

	// Build the index
	index := bu.buildIndex()

	// Encode everything
	data := bu.encode(bloomFilter, index)

	// Build the Info
	info := &Info{
		IndexOffset:      uint64(len(data) - len(index.Data) - 4), // 4 bytes for info offset
		IndexLen:         uint64(len(index.Data)),
		CompressionCodec: bu.conf.Compression,
		FirstKey:         bu.firstKey,
	}

	if bloomFilter != nil {
		info.FilterOffset = uint64(len(data) - len(bloomFilter.Data) - len(index.Data) - 4)
		info.FilterLen = uint64(len(bloomFilter.Data))
	}

	return &Table{
		Info:  info,
		Bloom: bloomFilter,
		Data:  data,
	}
}

func (bu *Builder) buildIndex() *Index {
	builder := flatbuffers.NewBuilder(0)
	var blockMetaOffsets []flatbuffers.UOffsetT

	offset := uint64(0)
	for _, b := range bu.blocks {
		firstKey := b.FirstKey()

		flatbuf.BlockMetaStart(builder)
		flatbuf.BlockMetaAddOffset(builder, offset)
		flatbuf.BlockMetaAddFirstKey(builder, builder.CreateByteString(firstKey))
		blockMetaOffset := flatbuf.BlockMetaEnd(builder)
		blockMetaOffsets = append(blockMetaOffsets, blockMetaOffset)

		offset += uint64(len(block.Encode(b)))
	}

	flatbuf.SsTableIndexStartBlockMetaVector(builder, len(blockMetaOffsets))
	for i := len(blockMetaOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(blockMetaOffsets[i])
	}
	blockMetaVector := builder.EndVector(len(blockMetaOffsets))

	flatbuf.SsTableIndexStart(builder)
	flatbuf.SsTableIndexAddBlockMeta(builder, blockMetaVector)
	indexOffset := flatbuf.SsTableIndexEnd(builder)

	builder.Finish(indexOffset)

	return &Index{
		Data: builder.FinishedBytes(),
	}
}

func (bu *Builder) encode(bloomFilter *bloom.Filter, index *Index) []byte {
	var data []byte

	// Encode blocks
	for _, b := range bu.blocks {
		data = append(data, block.Encode(b)...)
	}

	// Encode bloom filter if present
	if bloomFilter != nil {
		data = append(data, bloom.Encode(bloomFilter)...)
	}

	// Encode index
	data = append(data, index.Data...)

	// Encode info offset
	infoOffset := uint32(len(data))
	offsetBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(offsetBytes, infoOffset)
	data = append(data, offsetBytes...)

	return data
}
