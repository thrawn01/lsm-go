package block_test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)

	encoded := block.Encode(b)
	var decoded block.Block
	block.Decode(&decoded, encoded)
	assert.Equal(t, b.Data, decoded.Data)
	assert.Equal(t, b.Offsets, decoded.Offsets)
}

func TestBlockWithTombstone(t *testing.T) {
	bb := block.NewBuilder(4096)
	assert.True(t, bb.Add([]byte("key1"), []byte("value1")))
	assert.True(t, bb.Add([]byte("key2"), []byte("")))
	assert.True(t, bb.Add([]byte("key3"), []byte("value3")))

	b, err := bb.Build()
	assert.Nil(t, err)

	encoded := block.Encode(b)
	var decoded block.Block
	block.Decode(&decoded, encoded)
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
	assert.Nil(t, err)

	iter := block.NewIterator(b)
	for i := 0; i < len(kvPairs); i++ {
		kv, ok := iter.Next()
		assert.True(t, ok)
		assert.True(t, bytes.Equal(kv.Key, kv.Key))
		assert.True(t, bytes.Equal(kv.Value, kv.Value))
	}

	kv, ok := iter.Next()
	assert.False(t, ok)
	assert.NoError(t, err)
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
	assert.Nil(t, err)

	t.Run("KeyFound", func(t *testing.T) {
		iter := block.NewIteratorAtKey(b, []byte("kratos"))
		for i := 1; i < len(kvPairs); i++ {
			kv, ok := iter.Next()
			assert.True(t, ok)
			assert.Equal(t, kv.Key, kv.Key)
			assert.Equal(t, kv.Value, kv.Value)
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
			assert.Equal(t, kv.Key, kv.Key)
			assert.Equal(t, kv.Value, kv.Value)
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
