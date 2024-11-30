package bloom_test

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thrawn01/lsm-go/internal/sstable/bloom"
	"github.com/thrawn01/lsm-go/internal/sstable/types"
	"testing"
)

func TestFilterBuilder_Build(t *testing.T) {
	fb := bloom.NewFilterBuilder(10)
	fb.Add([]byte("test1"))
	fb.Add([]byte("test2"))
	fb.Add([]byte("test3"))

	filter := fb.Build()
	assert.NotNil(t, filter)
	assert.NotEmpty(t, filter.Data)
	assert.Greater(t, filter.NumProbes, uint16(0))
}
func TestFilter_HasKey(t *testing.T) {
	fb := bloom.NewFilterBuilder(10)
	fb.Add([]byte("test1"))
	fb.Add([]byte("test2"))
	fb.Add([]byte("test3"))

	filter := fb.Build()

	assert.True(t, filter.HasKey([]byte("test1")))
	assert.True(t, filter.HasKey([]byte("test2")))
	assert.True(t, filter.HasKey([]byte("test3")))
	assert.False(t, filter.HasKey([]byte("test4")))
}

func TestEncodeDecode(t *testing.T) {
	fb := bloom.NewFilterBuilder(10)
	fb.Add([]byte("test1"))
	fb.Add([]byte("test2"))
	filter := fb.Build()

	encoded := bloom.Encode(filter)
	decoded := bloom.Decode(encoded)

	assert.Equal(t, filter.NumProbes, decoded.NumProbes)
	assert.Equal(t, filter.Data, decoded.Data)
}

func TestEmptyFilter(t *testing.T) {
	fb := bloom.NewFilterBuilder(10)
	filter := fb.Build()

	assert.Empty(t, filter.Data)
	assert.Equal(t, uint16(0), filter.NumProbes)
	assert.False(t, filter.HasKey([]byte("test")))
}

func TestLargeFilter(t *testing.T) {
	fb := bloom.NewFilterBuilder(10)
	for i := 0; i < 10000; i++ {
		fb.Add([]byte(fmt.Sprintf("test%d", i)))
	}

	filter := fb.Build()
	assert.NotEmpty(t, filter.Data)
	assert.Greater(t, filter.NumProbes, uint16(0))

	// Test for false positives
	falsePositives := 0
	for i := 10000; i < 20000; i++ {
		if filter.HasKey([]byte(fmt.Sprintf("test%d", i))) {
			falsePositives++
		}
	}

	falsePositiveRate := float64(falsePositives) / 10000
	assert.Less(t, falsePositiveRate, 0.1) // Assuming a reasonable false positive rate
}

func TestFilterEffective(t *testing.T) {
	keysToTest := uint32(100000)
	keySize := types.SizeOfUint32
	builder := bloom.NewFilterBuilder(10)

	var i uint32
	for i = 0; i < keysToTest; i++ {
		bytes := make([]byte, keySize)
		binary.BigEndian.PutUint32(bytes, i)
		builder.Add(bytes)
	}
	filter := builder.Build()

	// check all entries in filter
	for i = 0; i < keysToTest; i++ {
		bytes := make([]byte, keySize)
		binary.BigEndian.PutUint32(bytes, i)
		assert.True(t, filter.HasKey(bytes))
	}

	// check false positives
	fp := uint32(0)
	for i := keysToTest; i < keysToTest*2; i++ {
		bytes := make([]byte, keySize)
		binary.BigEndian.PutUint32(bytes, i)
		if filter.HasKey(bytes) {
			fp += 1
		}
	}

	// observed fp is 0.00744
	assert.True(t, float32(fp)/float32(keysToTest) < 0.01)
}
