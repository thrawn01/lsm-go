package sstable

import (
	"github.com/stretchr/testify/assert"
	"github.com/thrawn01/lsm-go/internal/utils"
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
		Compression:      utils.CompressionNone,
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
			Compression:      utils.CompressionNone,
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
