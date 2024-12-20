package block_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/thrawn01/lsm-go/internal/compress"
	"github.com/thrawn01/lsm-go/internal/sstable/block"
	"github.com/thrawn01/lsm-go/internal/sstable/types"
	"testing"
)

func TestNewBuilder(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.IsEmpty())
	assert.True(t, bb.Add([]byte("key1"), []byte("value1")))
	assert.True(t, bb.Add([]byte("key2"), []byte("value2")))
	assert.False(t, bb.IsEmpty())

	b, err := bb.Build()
	assert.NoError(t, err)

	encoded, err := block.Encode(b, compress.CodecNone)
	assert.NoError(t, err)
	var decoded block.Block
	assert.NoError(t, block.Decode(&decoded, encoded, compress.CodecNone))
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlockChecksumVerification(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.Add([]byte("key1"), []byte("value1")))
	assert.True(t, bb.Add([]byte("key2"), []byte("value2")))

	b, err := bb.Build()
	assert.NoError(t, err)

	encoded, err := block.Encode(b, compress.CodecNone)
	assert.NoError(t, err)

	// Test successful decoding
	var decoded block.Block
	err = block.Decode(&decoded, encoded, compress.CodecNone)
	assert.NoError(t, err)
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)

	// Corrupt the data
	encoded[0] ^= 0xFF

	// Test failed decoding due to checksum mismatch
	err = block.Decode(&decoded, encoded, compress.CodecNone)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "block checksum failed")
}

func TestBlockWithTombstone(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.Add([]byte("key1"), []byte("value1")))
	assert.True(t, bb.Add([]byte("key2"), []byte("")))
	assert.True(t, bb.Add([]byte("key3"), []byte("value3")))

	b, err := bb.Build()
	assert.NoError(t, err)

	encoded, err := block.Encode(b, compress.CodecNone)
	assert.NoError(t, err)
	var decoded block.Block
	err = block.Decode(&decoded, encoded, compress.CodecNone)
	assert.NoError(t, err)
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlockIterator(t *testing.T) {
	kvPairs := []types.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	builder := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.Add(kv.Key, kv.Value))
	}

	b, err := builder.Build()
	assert.NoError(t, err)

	iter := block.NewIterator(b)
	for i := 0; i < len(kvPairs); i++ {
		kv, ok := iter.Next()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kv.Key, kvPairs[i].Key))
		assert.True(t, bytes.Equal(kv.Value, kvPairs[i].Value))
	}

	kv, ok := iter.Next()
	assert.False(t, ok)
	assert.Equal(t, types.KV{Key: nil, Value: nil}, kv)
}

func TestNewIteratorAtKey(t *testing.T) {
	kvPairs := []types.KV{
		{Key: []byte("donkey"), Value: []byte("kong")},
		{Key: []byte("kratos"), Value: []byte("atreus")},
		{Key: []byte("super"), Value: []byte("mario")},
	}

	builder := block.NewBuilder(1024)
	for _, kv := range kvPairs {
		assert.True(t, builder.Add(kv.Key, kv.Value))
	}

	b, err := builder.Build()
	assert.NoError(t, err)

	t.Run("KeyFound", func(t *testing.T) {
		iter := block.NewIteratorAtKey(b, []byte("kratos"))
		for i := 1; i < len(kvPairs); i++ {
			kv, ok := iter.Next()
			assert.True(t, ok)
			assert.Equal(t, kvPairs[i].Key, kv.Key)
			assert.Equal(t, kvPairs[i].Value, kv.Value)
		}

		kv, ok := iter.Next()
		assert.False(t, ok)
		assert.Equal(t, types.KV{Key: nil, Value: nil}, kv)
	})

	t.Run("KeyNotFound", func(t *testing.T) {
		iter := block.NewIteratorAtKey(b, []byte("ka"))
		for i := 1; i < len(kvPairs); i++ {
			kv, ok := iter.Next()
			assert.True(t, ok)
			assert.Equal(t, kvPairs[i].Key, kv.Key)
			assert.Equal(t, kvPairs[i].Value, kv.Value)
		}

		kv, ok := iter.Next()
		assert.False(t, ok)
		assert.Equal(t, types.KV{Key: nil, Value: nil}, kv)
	})

	t.Run("KeyAtEnd", func(t *testing.T) {
		iter := block.NewIteratorAtKey(b, []byte("zzz"))
		kv, ok := iter.Next()
		assert.False(t, ok)
		assert.Equal(t, types.KV{Key: nil, Value: nil}, kv)
	})
}

func testCompression(t *testing.T, codec compress.Codec) {
	t.Helper()
	bb := block.NewBuilder(4096)
	assert.True(t, bb.Add([]byte("key1"), []byte("value1")))
	assert.True(t, bb.Add([]byte("key2"), []byte("value2")))

	b, err := bb.Build()
	assert.NoError(t, err)

	encoded, err := block.Encode(b, codec)
	assert.NoError(t, err)

	var decoded block.Block
	err = block.Decode(&decoded, encoded, codec)
	assert.NoError(t, err)
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestCompressionMethods(t *testing.T) {
	codecs := []compress.Codec{
		compress.CodecNone,
		compress.CodecSnappy,
		compress.CodecZlib,
		compress.CodecLz4,
		compress.CodecZstd,
	}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			testCompression(t, codec)
		})
	}
}
