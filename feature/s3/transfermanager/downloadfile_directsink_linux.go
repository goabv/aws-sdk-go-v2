//go:build linux

package transfermanager

import (
	"unsafe"
)

// directBlockSize is the alignment (bytes) O_DIRECT requires for the write offset,
// length, and buffer address. 4096 satisfies the logical block size of common
// devices/filesystems.
const directBlockSize = directIOBlockSize

func directIOAvailable() bool { return true }

// alignedBuf returns a byte slice of length size whose backing-array start is
// aligned to align bytes, as O_DIRECT requires for the buffer address. It relies on
// the Go runtime not moving heap allocations (true for the current non-moving GC).
func alignedBuf(size, align int64) []byte {
	b := make([]byte, size+align)
	off := int64(uintptr(unsafe.Pointer(&b[0])) % uintptr(align))
	if off != 0 {
		off = align - off
	}
	return b[off : off+size]
}

// newSyncChunkBuf allocates a buffer for syncChunkPool, block-aligned for
// O_DIRECT.
func newSyncChunkBuf(size int64) []byte {
	return alignedBuf(size, directBlockSize)
}
