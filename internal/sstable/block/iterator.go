package block

import (
	"bytes"
	"encoding/binary"
	"github.com/thrawn01/lsm-go/internal/sstable/types"
	"sort"
)

// Iterator iterates through KeyValue pairs present in the Block.
type Iterator struct {
	block       *Block
	offsetIndex uint64
}

func NewIterator(block *Block) *Iterator {
	return &Iterator{
		block:       block,
		offsetIndex: 0,
	}
}

// NewIteratorAtKey Construct an Iterator that starts at the given key, or at the first
// key greater than the given key if the exact key given is not in the block.
func NewIteratorAtKey(block *Block, key []byte) *Iterator {
	index := sort.Search(len(block.Offsets), func(i int) bool {
		off := block.Offsets[i]
		keyLen := binary.BigEndian.Uint16(block.Data[off:])
		off += sizeOfUint16InBytes
		curKey := block.Data[off : off+keyLen]
		return bytes.Compare(curKey, key) >= 0
	})

	return &Iterator{
		block:       block,
		offsetIndex: uint64(index),
	}
}

func (iter *Iterator) Next() (types.KV, bool) {
	for {
		entry, ok := iter.NextEntry()
		if !ok {
			return types.KV{}, false
		}
		if entry.Value.IsTombstone {
			continue
		}
		return types.KV{
			Key:   entry.Key,
			Value: entry.Value.Value,
		}, true
	}
}

func (iter *Iterator) NextEntry() (types.KeyValue, bool) {

	if iter.offsetIndex >= uint64(len(iter.block.Offsets)) {
		return types.KeyValue{}, false
	}
	var result types.KeyValue

	data := iter.block.Data
	offset := iter.block.Offsets[iter.offsetIndex]

	// Read KeyLength(uint16), Key, (ValueLength(uint32), value)/Tombstone(uint32) from data
	keyLen := binary.BigEndian.Uint16(data[offset:])
	offset += sizeOfUint16InBytes

	result.Key = data[offset : offset+keyLen]
	offset += keyLen

	valueLen := binary.BigEndian.Uint32(data[offset:])
	offset += sizeOfUint32InBytes

	if valueLen != Tombstone {
		result.Value = types.Value{
			Value:       data[offset : uint32(offset)+valueLen],
			IsTombstone: false,
		}
	} else {
		result.Value = types.Value{
			IsTombstone: true,
		}
	}

	iter.offsetIndex += 1
	return result, true
}
