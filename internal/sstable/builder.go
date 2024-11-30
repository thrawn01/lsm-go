package sstable

// Builder builds the SSTable in the format outlined
// in the diagram below. The Builder uses the block.Builder
// to build the key value pairs and uses bloom.Builder to
// build the bloom filter if the  total number of keys in
// all blocks meet or exceeds Config.MinFilterKeys.
// Finally, it writes the sstable.Index and sstable.Info
// along with the offset of the sstable.Info
//
// +-----------------------------------------------+
// |               SSTable                         |
// +-----------------------------------------------+
// |  +-----------------------------------------+  |
// |  |  List of Blocks                         |  |
// |  |  +-----------------------------------+  |  |
// |  |  |  block.Block                      |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  List of types.KeyValue pairs  |  |  |
// |  |  |  |  +---------------------------+ |  |  |
// |  |  |  |  |  Key Length (2 bytes)     | |  |  |
// |  |  |  |  |  Key                      | |  |  |
// |  |  |  |  |  Value Length (4 bytes)   | |  |  |
// |  |  |  |  |  Value                    | |  |  |
// |  |  |  |  +---------------------------+ |  |  |
// |  |  |  |  ...                           |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Offsets for each Key          |  |  |
// |  |  |  |  (n * 2 bytes)                 |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Count of Keys (2 bytes)       |  |  |
// |  |  |  +-------------------------------+|  |  |
// |  |  |  |  Checksum (4 bytes)            |  |  |
// |  |  +-----------------------------------+  |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  bloom.Filter (if MinFilterKeys met)    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  sstable.Index                          |  |
// |  |  (List of Block Offsets)                |  |
// |  |  - Block Offset (End of Block)          |  |
// |  |  - FirstKey of this Block               |  |
// |  |  ...                                    |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  sstable.Info                           |  |
// |  |  - Offset of BloomFilter                |  |
// |  |  - Length of BloomFilter                |  |
// |  |  - Offset of sstable.Index              |  |
// |  |  - Length of sstable.Index              |  |
// |  +-----------------------------------------+  |
// |                                               |
// |  +-----------------------------------------+  |
// |  |  Offset of sstable.Info (4 bytes)       |  |
// |  +-----------------------------------------+  |
// +-----------------------------------------------+
type Builder struct {
}

// NewBuilder creates a new builder used to encode an SSTable
func NewBuilder(conf Config) *Builder {
	return &Builder{} // TODO
}

// Add a key and value to the SSTable.
func (b *Builder) Add(key, value []byte) error {
	return nil // TODO
}

func (b *Builder) Build() *Table {
	return &Table{} // TODO
}
