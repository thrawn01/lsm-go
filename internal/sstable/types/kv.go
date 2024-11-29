package types

import (
	"encoding/binary"
)

// KV Represents a key-value pair known not to be a tombstone.
type KV struct {
	Key   []byte
	Value []byte
}

// KeyValue is a K/V pair which may be a tombstone.
type KeyValue struct {
	Key   []byte
	Value Value
}

// Value Represents a value that may be a tombstone.
type Value struct {
	Value       []byte
	IsTombstone bool
}

// DecodeValue decodes a value from a byte slice.
// If first byte is 1, then return tombstone else return with value
func DecodeValue(b []byte) Value {
	if b[0] == 1 {
		return Value{IsTombstone: true}
	}

	return Value{
		Value:       b[1:],
		IsTombstone: false,
	}
}

// Encode encodes the value into a byte slice.
// If it is a tombstone return 1 (indicating tombstone) as the only byte
// if it is not a tombstone the value is stored from second byte onwards
func (v Value) Encode() []byte {
	if v.IsTombstone {
		return []byte{1}
	}
	return append([]byte{0}, v.Value...)
}

func (v Value) Size() int64 {
	return int64(binary.Size(v.Value) + binary.Size(v.IsTombstone))
}

func (v Value) GetValue() []byte {
	if v.IsTombstone {
		return nil
	}
	return v.Value
}
