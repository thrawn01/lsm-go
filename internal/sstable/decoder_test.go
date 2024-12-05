package sstable

import (
	"github.com/kapetan-io/tackle/random"
	"github.com/stretchr/testify/assert"
	"github.com/thrawn01/lsm-go/internal/compress"
	"github.com/thrawn01/lsm-go/internal/sstable/block"
	"testing"
)

type mockBlob struct {
	data []byte
}

func (m *mockBlob) Len() (uint64, error) {
	return uint64(len(m.data)), nil
}

func (m *mockBlob) ReadRange(r Range) ([]byte, error) {
	return m.data[r.Start:r.End], nil
}

func (m *mockBlob) Read() ([]byte, error) {
	return m.data, nil
}

func (m *mockBlob) Id() string {
	return "1234"
}

func TestDecoder_ReadInfo(t *testing.T) {
	// Create a sample SSTable using the Builder
	builder := NewBuilder(Config{
		BlockSize:        1024,
		MinFilterKeys:    10,
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	})

	// Add some sample data
	assert.NoError(t, builder.Add([]byte("key1"), []byte("value1")))
	assert.NoError(t, builder.Add([]byte("key2"), []byte("value2")))
	assert.NoError(t, builder.Add([]byte("key3"), []byte("value3")))

	// Build the SSTable
	table := builder.Build()

	// Create a mock blob with the SSTable data
	blob := &mockBlob{data: table.Data}

	// Create a decoder
	decoder := &Decoder{
		Config: Config{
			BlockSize:        1024,
			MinFilterKeys:    10,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		},
	}

	// Read the Info using the decoder
	info, err := decoder.ReadInfo(blob)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// Verify the Info contents
	assert.Equal(t, table.Info.FirstKey, info.FirstKey)
	assert.Equal(t, table.Info.IndexOffset, info.IndexOffset)
	assert.Equal(t, table.Info.IndexLen, info.IndexLen)
	assert.Equal(t, table.Info.FilterOffset, info.FilterOffset)
	assert.Equal(t, table.Info.FilterLen, info.FilterLen)
	assert.Equal(t, table.Info.CompressionCodec, info.CompressionCodec)
}

func TestDecoder_ReadBloom(t *testing.T) {
	// Create a sample SSTable using the Builder
	builder := NewBuilder(Config{
		BlockSize:        1024,
		MinFilterKeys:    2, // Set this to a small number to ensure a bloom filter is created
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	})

	// Add some sample data
	assert.NoError(t, builder.Add([]byte("key1"), []byte("value1")))
	assert.NoError(t, builder.Add([]byte("key2"), []byte("value2")))
	assert.NoError(t, builder.Add([]byte("key3"), []byte("value3")))

	// Build the SSTable
	table := builder.Build()

	// Create a mock blob with the SSTable data
	blob := &mockBlob{data: table.Data}

	// Create a decoder
	decoder := &Decoder{
		Config: Config{
			BlockSize:        1024,
			MinFilterKeys:    2,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		},
	}

	// Read the Info using the decoder
	info, err := decoder.ReadInfo(blob)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// Read the Bloom filter using the decoder
	bloomFilter, err := decoder.ReadBloom(info, blob)
	assert.NoError(t, err)
	assert.NotNil(t, bloomFilter)

	// Verify the Bloom filter contents
	assert.True(t, bloomFilter.HasKey([]byte("key1")))
	assert.True(t, bloomFilter.HasKey([]byte("key2")))
	assert.True(t, bloomFilter.HasKey([]byte("key3")))
	assert.False(t, bloomFilter.HasKey([]byte("key4"))) // This key wasn't added
}

func TestDecoder_ReadIndex(t *testing.T) {
	// Create a sample SSTable using the Builder
	builder := NewBuilder(Config{
		BlockSize:        1024,
		MinFilterKeys:    2,
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	})

	// Add some sample data
	assert.NoError(t, builder.Add([]byte("key1"), []byte("value1")))
	assert.NoError(t, builder.Add([]byte("key2"), []byte("value2")))
	assert.NoError(t, builder.Add([]byte("key3"), []byte("value3")))

	// Build the SSTable
	table := builder.Build()

	// Create a mock blob with the SSTable data
	blob := &mockBlob{data: table.Data}

	// Create a decoder
	decoder := &Decoder{
		Config: Config{
			BlockSize:        1024,
			MinFilterKeys:    2,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		},
	}

	// Read the Info using the decoder
	info, err := decoder.ReadInfo(blob)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// Read the Index using the decoder
	index, err := decoder.ReadIndex(info, blob)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// Verify the Index only has metadata for one block
	decodedIndex := index.AsFlatBuf()
	assert.NotNil(t, decodedIndex)
	assert.Equal(t, len(decodedIndex.BlockMeta), 1)

	// Check the block's metadata
	firstBlock := decodedIndex.BlockMeta[0]
	assert.NotNil(t, firstBlock)
	assert.Greater(t, firstBlock.Offset, uint64(0))
	assert.Equal(t, []byte("key1"), firstBlock.FirstKey)
}

func TestDecoder_ReadBlocks(t *testing.T) {
	// Create a sample SSTable using the Builder
	builder := NewBuilder(Config{
		BlockSize:        30,
		MinFilterKeys:    2,
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	})

	// Each key will be in its own block as each key exceeds the max block size of 30
	value1 := []byte(random.Alpha("", 30))
	assert.NoError(t, builder.Add([]byte("key1"), value1))
	assert.NoError(t, builder.Add([]byte("key2"), []byte(random.Alpha("", 30))))
	assert.NoError(t, builder.Add([]byte("key3"), []byte(random.Alpha("", 30))))
	assert.NoError(t, builder.Add([]byte("key4"), []byte(random.Alpha("", 30))))
	assert.NoError(t, builder.Add([]byte("key5"), []byte(random.Alpha("", 30))))

	// Build the SSTable
	table := builder.Build()

	// Create a mock blob with the SSTable data
	blob := &mockBlob{data: table.Data}

	// Create a decoder
	decoder := &Decoder{
		Config: Config{
			BlockSize:        30,
			MinFilterKeys:    2,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		},
	}

	// Read the Info using the decoder
	info, err := decoder.ReadInfo(blob)
	assert.NoError(t, err)
	assert.NotNil(t, info)

	// Read the Index using the decoder
	index, err := decoder.ReadIndex(info, blob)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// Test reading all blocks
	blocks, err := decoder.ReadBlocks(info, index, Range{Start: 0, End: uint64(len(index.AsFlatBuf().BlockMeta))}, blob)
	assert.NoError(t, err)
	assert.Equal(t, len(index.AsFlatBuf().BlockMeta), len(blocks))

	// Test reading a subset of blocks
	blocks, err = decoder.ReadBlocks(info, index, Range{Start: 1, End: 3}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(blocks))

	// Test invalid range
	_, err = decoder.ReadBlocks(info, index, Range{Start: 10, End: 20}, blob)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid block range")

	// Test reading and decoding block content
	blocks, err = decoder.ReadBlocks(info, index, Range{Start: 0, End: 1}, blob)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(blocks))

	iter := block.NewIterator(&blocks[0])
	kv, ok := iter.Next()
	assert.True(t, ok)
	assert.Equal(t, []byte("key1"), kv.Key)
	assert.Equal(t, value1, kv.Value)
}

func TestDecoder_ReadIndexFromBytes(t *testing.T) {
	// Create a sample SSTable using the Builder
	builder := NewBuilder(Config{
		BlockSize:        1024,
		MinFilterKeys:    2,
		FilterBitsPerKey: 10,
		Compression:      compress.CodecNone,
	})

	// Add some sample data
	assert.NoError(t, builder.Add([]byte("key1"), []byte("value1")))
	assert.NoError(t, builder.Add([]byte("key2"), []byte("value2")))
	assert.NoError(t, builder.Add([]byte("key3"), []byte("value3")))

	// Build the SSTable
	table := builder.Build()

	// Create a decoder
	decoder := &Decoder{
		Config: Config{
			BlockSize:        1024,
			MinFilterKeys:    2,
			FilterBitsPerKey: 10,
			Compression:      compress.CodecNone,
		},
	}

	// Extract the index bytes from the table data
	indexBytes := table.Data[table.Info.IndexOffset : table.Info.IndexOffset+table.Info.IndexLen]

	// Read the Index using the decoder
	index, err := decoder.ReadIndexFromBytes(table.Info, indexBytes)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	// Verify the Index contents
	decodedIndex := index.AsFlatBuf()
	assert.NotNil(t, decodedIndex)
	assert.Equal(t, 1, len(decodedIndex.BlockMeta))

	// Check the block's metadata
	firstBlock := decodedIndex.BlockMeta[0]
	assert.NotNil(t, firstBlock)
	assert.Greater(t, firstBlock.Offset, uint64(0))
	assert.Equal(t, []byte("key1"), firstBlock.FirstKey)

	// Test error case: insufficient data
	_, err = decoder.ReadIndexFromBytes(table.Info, indexBytes[:len(indexBytes)-1])
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient data")

	// Test case: no index
	noIndexInfo := &Info{IndexLen: 0}
	index, err = decoder.ReadIndexFromBytes(noIndexInfo, []byte{})
	assert.NoError(t, err)
	assert.Nil(t, index)
}
