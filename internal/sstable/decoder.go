package sstable

import "github.com/thrawn01/lsm-go/internal/sstable/bloom"

// Decoder is used to decode portions of the SSTable using the available methods
type Decoder struct {
	// The config used to decode the SSTable
	Config Config
}

// ReadInfo reads the Info from the provided blob. This method assumes a single
// encoded SSTable is available to be read via the provided blob.
func (d *Decoder) ReadInfo(b ReadOnlyBlob) (*Info, error) {
	return nil, nil // TODO
}

// ReadBloom reads  the bloom.Filter from the provided store using blob.ReadRange()
// using the offsets provided by Info.
func (d *Decoder) ReadBloom(info *Info, b ReadOnlyBlob) (*bloom.Filter, error) {
	return nil, nil // TODO
}

// ReadIndex reads the Index from the provided store using blob.ReadRange()
// using the offsets provided by Info.
func (d *Decoder) ReadIndex(info *Info, b ReadOnlyBlob) (*Index, error) {
	return nil, nil // TODO
}

// ReadIndexFromBytes is identical to ReadIndex except it reads the index from the provided
// byte slice.
func (d *Decoder) ReadIndexFromBytes(info *Info, buf []byte) (*Index, error) {
	return nil, nil // TODO
}

func (d *Decoder) ReadBlocks(info *Info, idx *Index, r Range, b ReadOnlyBlob) (*Index, error) {
	return nil, nil // TODO
}
