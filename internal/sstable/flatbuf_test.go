package sstable

import (
	"github.com/thrawn01/lsm-go/internal/compress"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thrawn01/lsm-go/internal/flatbuf"
)

// NOTE: These test private methods for correctness, feel free to remove these tests in the future.

func TestEncodeDecodeIndex(t *testing.T) {
	// Create a sample SsTableIndexT
	originalIndex := &flatbuf.SsTableIndexT{
		BlockMeta: []*flatbuf.BlockMetaT{
			{Offset: 100, FirstKey: []byte("key1")},
			{Offset: 200, FirstKey: []byte("key2")},
		},
	}

	// Encode the Index
	encoded := encodeIndex(originalIndex)

	// Decode the Index
	decoded := decodeIndex(encoded)

	// Check if the decoded Index matches the original
	assert.Equal(t, len(originalIndex.BlockMeta), len(decoded.BlockMeta))

	for i := 0; i < len(originalIndex.BlockMeta); i++ {
		assert.Equal(t, originalIndex.BlockMeta[i].Offset, decoded.BlockMeta[i].Offset)
		assert.Equal(t, originalIndex.BlockMeta[i].FirstKey, decoded.BlockMeta[i].FirstKey)
	}
}

func TestEncodeDecodeInfo(t *testing.T) {
	// Create a sample Info
	info := &Info{
		FirstKey:         []byte("testkey"),
		IndexOffset:      1000,
		IndexLen:         500,
		FilterOffset:     1500,
		FilterLen:        200,
		CompressionCodec: compress.CodecSnappy,
	}

	// Encode the Info
	encoded := encodeInfo(info)

	// Decode the Info
	decoded := decodeInfo(encoded)

	// Check if the decoded Info matches the original
	assert.Equal(t, info.FirstKey, decoded.FirstKey)
	assert.Equal(t, info.IndexOffset, decoded.IndexOffset)
	assert.Equal(t, info.IndexLen, decoded.IndexLen)
	assert.Equal(t, info.FilterOffset, decoded.FilterOffset)
	assert.Equal(t, info.FilterLen, decoded.FilterLen)
	assert.Equal(t, info.CompressionCodec, decoded.CompressionCodec)
}

func TestIndexAsFlatBuf(t *testing.T) {
	// Create a sample SsTableIndexT
	originalIndex := &flatbuf.SsTableIndexT{
		BlockMeta: []*flatbuf.BlockMetaT{
			{Offset: 100, FirstKey: []byte("key1")},
			{Offset: 200, FirstKey: []byte("key2")},
		},
	}

	result := Index{Data: encodeIndex(originalIndex)}.AsFlatBuf()

	assert.NotNil(t, result)
	assert.Equal(t, len(originalIndex.BlockMeta), len(result.BlockMeta))
	for i, blockMeta := range result.BlockMeta {
		assert.Equal(t, originalIndex.BlockMeta[i].Offset, blockMeta.Offset)
		assert.Equal(t, originalIndex.BlockMeta[i].FirstKey, blockMeta.FirstKey)
	}
}

func TestInfoClone(t *testing.T) {
	// Create a sample Info
	original := &Info{
		FirstKey:         []byte("testkey"),
		IndexOffset:      1000,
		IndexLen:         500,
		FilterOffset:     1500,
		FilterLen:        200,
		CompressionCodec: compress.CodecSnappy,
	}

	// Clone the Info
	cloned := original.Clone()

	// Check if the cloned Info matches the original
	assert.Equal(t, original.FirstKey, cloned.FirstKey)
	assert.Equal(t, original.IndexOffset, cloned.IndexOffset)
	assert.Equal(t, original.IndexLen, cloned.IndexLen)
	assert.Equal(t, original.FilterOffset, cloned.FilterOffset)
	assert.Equal(t, original.FilterLen, cloned.FilterLen)
	assert.Equal(t, original.CompressionCodec, cloned.CompressionCodec)

	// Modify the original to ensure deep copy
	original.FirstKey[0] = 'x'
	assert.NotEqual(t, original.FirstKey, cloned.FirstKey)
}

func TestIndexClone(t *testing.T) {
	// Create a sample Index
	original := Index{
		Data: []byte{1, 2, 3, 4, 5},
	}

	// Clone the Index
	cloned := original.Clone()

	// Check if the cloned Index matches the original
	assert.Equal(t, original.Data, cloned.Data)

	// Modify the original to ensure deep copy
	original.Data[0] = 10
	assert.NotEqual(t, original.Data, cloned.Data)
}
