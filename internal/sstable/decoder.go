package sstable

import (
	"encoding/binary"
	"fmt"
	"github.com/thrawn01/lsm-go/internal/sstable/block"
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
		return nil, fmt.Errorf("while reading offset %d ReadRange(): %w", size-types.SizeOfUint32, err)
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
	// Check if there's a bloom filter
	if info.FilterLen == 0 {
		return nil, nil
	}

	// Read the bloom filter data
	filterBytes, err := b.ReadRange(Range{
		Start: info.FilterOffset,
		End:   info.FilterOffset + info.FilterLen,
	})
	if err != nil {
		return nil, fmt.Errorf("while reading bloom filter with ReadRange(): %w", err)
	}

	// Decode the bloom filter
	filter := bloom.Decode(filterBytes)

	return filter, nil
}

// ReadIndex reads the Index from the provided store using blob.ReadRange()
// using the offsets provided by Info.
func (d *Decoder) ReadIndex(info *Info, b ReadOnlyBlob) (*Index, error) {
	// Check if there's an index
	if info.IndexLen == 0 {
		return nil, nil
	}

	// Read the index data
	indexBytes, err := b.ReadRange(Range{
		Start: info.IndexOffset,
		End:   info.IndexOffset + info.IndexLen,
	})
	if err != nil {
		return nil, fmt.Errorf("while reading index with ReadRange(): %w", err)
	}

	// Decode the index
	index := &Index{
		Data: indexBytes,
	}

	return index, nil
}

// ReadIndexFromBytes is identical to ReadIndex except it reads the index from the provided
// byte slice.
func (d *Decoder) ReadIndexFromBytes(info *Info, buf []byte) (*Index, error) {
	// Check if there's an index
	if info.IndexLen == 0 {
		return nil, nil
	}

	// Check if the buffer contains enough data
	if uint64(len(buf)) < info.IndexLen {
		return nil, fmt.Errorf("insufficient data: expected %d bytes, got %d", info.IndexLen, len(buf))
	}

	// Extract the index data
	indexBytes := buf[:info.IndexLen]

	// Decode the index
	index := &Index{
		Data: indexBytes,
	}

	return index, nil
}

// ReadBlocks reads a range of blocks. The range of blocks provided by Range is not a range of offsets, instead
// it is a range index blocks defined in flatbuf.SsTableIndexT.BlockMeta. Example: To retrieve the first
// block in the provided blob the range would be Range{Start: 0, End: 1}. ReadBlocks then uses the Index
// to locate the offsets in the blob decoding each block and returning them.
func (d *Decoder) ReadBlocks(info *Info, idx *Index, r Range, b ReadOnlyBlob) ([]block.Block, error) {
	// Decode the index
	indexT := idx.AsFlatBuf()

	// Validate the range
	if r.Start >= uint64(len(indexT.BlockMeta)) || r.End > uint64(len(indexT.BlockMeta)) || r.Start >= r.End {
		return nil, fmt.Errorf("invalid block range: start=%d, end=%d, total blocks=%d", r.Start, r.End, len(indexT.BlockMeta))
	}

	// Calculate the start and end offsets for the range of blocks
	startOffset := uint64(0)
	if r.Start > 0 {
		startOffset = indexT.BlockMeta[r.Start-1].Offset
	}
	endOffset := indexT.BlockMeta[r.End-1].Offset

	// Read all the block data in one call
	blockData, err := b.ReadRange(Range{Start: startOffset, End: endOffset})
	if err != nil {
		return nil, fmt.Errorf("error reading blocks: %w", err)
	}

	blocks := make([]block.Block, 0, r.End-r.Start)

	// Decode each block
	for i := r.Start; i < r.End; i++ {
		start := uint64(0)
		if i > r.Start {
			start = indexT.BlockMeta[i-1].Offset - startOffset
		}
		end := indexT.BlockMeta[i].Offset - startOffset

		var blk block.Block
		err = block.Decode(&blk, blockData[start:end], info.CompressionCodec)
		if err != nil {
			return nil, fmt.Errorf("error decoding block %d: %w", i, err)
		}

		blk.Meta = *indexT.BlockMeta[i]
		blocks = append(blocks, blk)
	}

	return blocks, nil
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
