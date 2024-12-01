package utils

import (
	"bytes"
	"compress/zlib"
	"errors"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	CompressionNone CompressionCodec = iota
	CompressionSnappy
	CompressionZlib
	CompressionLz4
	CompressionZstd
)

// TODO: Consider renaming utils to codec

type CompressionCodec int8

// String converts CompressionCodec to string
func (c CompressionCodec) String() string {
	switch c {
	case CompressionNone:
		return "None"
	case CompressionSnappy:
		return "Snappy"
	case CompressionZlib:
		return "Zlib"
	case CompressionLz4:
		return "LZ4"
	case CompressionZstd:
		return "Zstd"
	default:
		return "Unknown"
	}
}

var ErrInvalidCompressionCodec = errors.New("invalid compression codec")

// Compress the provided byte slice
func Compress(buf []byte, codec CompressionCodec) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return buf, nil

	case CompressionSnappy:
		return snappy.Encode(nil, buf), nil

	case CompressionZlib:
		var b bytes.Buffer
		w := zlib.NewWriter(&b)
		_, err := w.Write(buf)
		_ = w.Close()
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil

	case CompressionLz4:
		var b bytes.Buffer
		w := lz4.NewWriter(&b)
		_, err := w.Write(buf)
		_ = w.Close()
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil

	case CompressionZstd:
		var b bytes.Buffer
		w, err := zstd.NewWriter(&b)
		if err != nil {
			return nil, err
		}
		_, err = w.Write(buf)
		_ = w.Close()
		if err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	default:
		return nil, ErrInvalidCompressionCodec
	}
}

// Decompress the provided byte slice according to the compression codec
func Decompress(buf []byte, codec CompressionCodec) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return buf, nil

	case CompressionSnappy:
		return snappy.Decode(nil, buf)

	case CompressionZlib:
		r, err := zlib.NewReader(bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		defer func() { _ = r.Close() }()
		return io.ReadAll(r)

	case CompressionLz4:
		r := lz4.NewReader(bytes.NewReader(buf))
		return io.ReadAll(r)

	case CompressionZstd:
		r, err := zstd.NewReader(bytes.NewReader(buf))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)

	default:
		return nil, ErrInvalidCompressionCodec
	}
}
