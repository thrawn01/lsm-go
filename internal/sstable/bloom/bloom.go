package bloom

type Filter struct {
	NumProbes uint16
	Data      []byte
}

// HasKey returns true if the key exists in the bloom filter, false if it does not
func (f *Filter) HasKey(key []byte) bool {
	return false // TODO
}

// Decode decodes the bloom filter from the provided byte slice. Decode assumes
// the bloom filter is in the same format used by Encode()
func Decode(f *Filter, key []byte) {
	// TODO
}

// Encode encodes the bloom filter into a byte slice in the following format
//
// | length  | bloom filter |
// | ------- | ------------ |
// | 2 bytes | length bytes |
//
func Encode(f *Filter) []byte {
	return nil // TODO
}

type FilterBuilder struct {
	KeyHashes  []uint64
	BitsPerKey uint32
}

func NewFilterBuilder(bitsPerKey uint32) *FilterBuilder {
	return &FilterBuilder{
		KeyHashes:  make([]uint64, 0),
		BitsPerKey: bitsPerKey,
	}
}

// Add adds a new key to the bloom filter. This method
// assumes the keys added are all unique.
func (f *FilterBuilder) Add(key []byte) {
	// TODO
}

// Build builds the bloom filter using enhanced double hashing from as described
// in https://www.khoury.northeastern.edu/~pete/pub/bloom-filters-verification.pdf
func (f *FilterBuilder) Build() *Filter {
	return nil // TODO
}
