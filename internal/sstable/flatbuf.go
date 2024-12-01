package sstable

import (
	"github.com/google/flatbuffers/go"
	"github.com/thrawn01/lsm-go/internal/flatbuf"
	"github.com/thrawn01/lsm-go/internal/utils"
)

// encodeIndex encodes a SsTableIndex struct into a flat buffer
func encodeIndex(index *flatbuf.SsTableIndexT) []byte {
	builder := flatbuffers.NewBuilder(0)

	// Create a vector of BlockMeta
	blockMetaOffsets := make([]flatbuffers.UOffsetT, len(index.BlockMeta))
	for i := len(index.BlockMeta) - 1; i >= 0; i-- {
		blockMeta := index.BlockMeta[i]
		firstKeyOffset := builder.CreateByteString(blockMeta.FirstKey)
		flatbuf.BlockMetaStart(builder)
		flatbuf.BlockMetaAddOffset(builder, blockMeta.Offset)
		flatbuf.BlockMetaAddFirstKey(builder, firstKeyOffset)
		blockMetaOffsets[i] = flatbuf.BlockMetaEnd(builder)
	}

	// Manually create the vector of BlockMeta entries
	flatbuf.SsTableIndexStartBlockMetaVector(builder, len(blockMetaOffsets))
	for i := len(blockMetaOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(blockMetaOffsets[i])
	}
	blockMetaVector := builder.EndVector(len(blockMetaOffsets))

	// Start building the SsTableIndex
	flatbuf.SsTableIndexStart(builder)
	flatbuf.SsTableIndexAddBlockMeta(builder, blockMetaVector)
	indexOffset := flatbuf.SsTableIndexEnd(builder)

	builder.Finish(indexOffset)
	return builder.FinishedBytes()
}

func decodeIndex(b []byte) *flatbuf.SsTableIndexT {
	index := flatbuf.GetRootAsSsTableIndex(b, 0)
	indexT := new(flatbuf.SsTableIndexT)
	index.UnPackTo(indexT)
	return indexT
}

// encodeInfo encodes the provided Info into
// flatbuf.SsTableInfo flat buffer format.
func encodeInfo(info *Info) []byte {
	builder := flatbuffers.NewBuilder(0)

	firstKey := builder.CreateByteVector(info.FirstKey)

	flatbuf.SsTableInfoStart(builder)
	flatbuf.SsTableInfoAddFirstKey(builder, firstKey)
	flatbuf.SsTableInfoAddIndexOffset(builder, info.IndexOffset)
	flatbuf.SsTableInfoAddIndexLen(builder, info.IndexLen)
	flatbuf.SsTableInfoAddFilterOffset(builder, info.FilterOffset)
	flatbuf.SsTableInfoAddFilterLen(builder, info.FilterLen)
	flatbuf.SsTableInfoAddCompressionFormat(builder, flatbuf.CompressionFormat(info.CompressionCodec))
	infoOffset := flatbuf.SsTableInfoEnd(builder)

	builder.Finish(infoOffset)
	return builder.FinishedBytes()
}

func decodeInfo(b []byte) *Info {
	fbInfo := flatbuf.GetRootAsSsTableInfo(b, 0)
	info := &Info{
		FirstKey:         fbInfo.FirstKeyBytes(),
		IndexOffset:      fbInfo.IndexOffset(),
		IndexLen:         fbInfo.IndexLen(),
		FilterOffset:     fbInfo.FilterOffset(),
		FilterLen:        fbInfo.FilterLen(),
		CompressionCodec: utils.CompressionCodec(fbInfo.CompressionFormat()),
	}
	return info
}
