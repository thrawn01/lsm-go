package utils

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressDecompress(t *testing.T) {
	testCases := []struct {
		name   string
		codec  CompressionCodec
		input  []byte
		errMsg string
	}{
		{"None", CompressionNone, []byte("Hello, World!"), ""},
		{"Snappy", CompressionSnappy, []byte("Snappy compression test"), ""},
		{"Zlib", CompressionZlib, []byte("Zlib compression test"), ""},
		{"LZ4", CompressionLz4, []byte("LZ4 compression test"), ""},
		{"Zstd", CompressionZstd, []byte("Zstd compression test"), ""},
		{"Invalid Codec", CompressionCodec(99), []byte("Invalid"), "invalid compression codec"},
		{"Empty Input", CompressionSnappy, nil, ""},
		{"Large Input", CompressionZstd, bytes.Repeat([]byte("Large input test "), 1000), ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Compress
			compressed, err := Compress(tc.input, tc.codec)
			if tc.errMsg != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)

			// Decompress
			decompressed, err := Decompress(compressed, tc.codec)
			require.NoError(t, err)

			// Compare
			assert.Equal(t, tc.input, decompressed)
		})
	}
}

func TestCompressDecompressNonMatchingCodecs(t *testing.T) {
	input := []byte("Mismatched codec test")
	compressed, err := Compress(input, CompressionSnappy)
	require.NoError(t, err)

	_, err = Decompress(compressed, CompressionZlib)
	assert.Error(t, err)
}

func TestDecompressInvalidInput(t *testing.T) {
	invalidInput := []byte("This is not compressed data")
	codecs := []CompressionCodec{CompressionSnappy, CompressionZlib, CompressionLz4, CompressionZstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			_, err := Decompress(invalidInput, codec)
			assert.Error(t, err)
		})
	}
}

func TestCompressDecompressLargeInput(t *testing.T) {
	largeInput := bytes.Repeat([]byte("Large input test "), 100000) // 1.6MB of data
	codecs := []CompressionCodec{CompressionNone, CompressionSnappy, CompressionZlib, CompressionLz4, CompressionZstd}

	for _, codec := range codecs {
		t.Run(codec.String(), func(t *testing.T) {
			compressed, err := Compress(largeInput, codec)
			require.NoError(t, err)

			decompressed, err := Decompress(compressed, codec)
			require.NoError(t, err)

			assert.Equal(t, largeInput, decompressed)
		})
	}
}
