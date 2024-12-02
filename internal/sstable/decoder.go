package sstable

import (
	"encoding/binary"
	"fmt"
	"github.com/thrawn01/lsm-go/internal/sstable/bloom"
	"github.com/thrawn01/lsm-go/internal/sstable/types"
)

// Decoder is used to decode portions of the SSTable using the available methods
type Decoder struct {
	// The config used to decode the SSTable
	Config Config
}

// ReadInfo reads the Info from the provided blob. This method assumes a single
// encoded SSTable is available to be read via the provided blob.
func (d *Decoder) ReadInfo(b ReadOnlyBlob) (*Info, error) {
	// Get the total size of the blob
	size, err := b.Len()
	if err != nil {
		return nil, err
	}

	// Read the last 8 bytes to get the offset of the Info
	offsetBytes, err := b.ReadRange(Range{Start: size - types.SizeOfUint32, End: size})
	if err != nil {
		return nil, fmt.Errorf("failed to read Info offset: %w", err)
	}

	if len(offsetBytes) != types.SizeOfUint32 {
		return nil, fmt.Errorf("SSTable '%s' Corrupted: blob size is too small; expected atleast"+
			" 4 byte length, got %d", b.Id(), len(offsetBytes))
	}

	// Read the Info data
	infoOffset := uint64(binary.BigEndian.Uint32(offsetBytes))
	if infoOffset >= size {
		return nil, fmt.Errorf("invalid Info offset: %d is greater than or equal to blob size %d", infoOffset, size)
	}

	infoBytes, err := b.ReadRange(Range{Start: infoOffset, End: size - types.SizeOfUint32})
	if err != nil {
		return nil, err
	}

	// Decode the Info
	info := decodeInfo(infoBytes)

	if err := validInfo(info, size, b.Id()); err != nil {
		return nil, err
	}
	return info, nil
}

// ReadBloom reads the bloom.Filter from the provided store using blob.ReadRange()
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

// validInfo returns nil if Info offsets and lengths are less than the total
// size of the SSTable. If not, returns an error in the form
// "SSTable '<id>' Corrupted: <why>"
func validInfo(info *Info, size uint64, id string) error {
	if info.IndexOffset >= size {
		return fmt.Errorf("SSTable '%s' Corrupted: index offset %d is greater than or equal to SSTable size %d",
			id, info.IndexOffset, size)
	}
	if info.IndexOffset+info.IndexLen > size {
		return fmt.Errorf("SSTable '%s' Corrupted: index end offset %d is greater than SSTable size %d",
			id, info.IndexOffset+info.IndexLen, size)
	}
	if info.FilterOffset >= size {
		return fmt.Errorf("SSTable '%s' Corrupted: filter offset %d is greater than or equal to SSTable size %d",
			id, info.FilterOffset, size)
	}
	if info.FilterOffset+info.FilterLen > size {
		return fmt.Errorf("SSTable '%s' Corrupted: filter end offset %d is greater than SSTable size %d",
			id, info.FilterOffset+info.FilterLen, size)
	}
	return nil
}
