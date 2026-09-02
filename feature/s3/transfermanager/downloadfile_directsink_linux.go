//go:build linux

package transfermanager

import (
	"fmt"
	"syscall"
	"unsafe"
)

// directBlockSize is the alignment (bytes) O_DIRECT requires for the write offset,
// length, and buffer address. 4096 satisfies the logical block size of common
// devices/filesystems.
const directBlockSize = directIOBlockSize

func directIOAvailable() bool { return true }

// pwriteFull writes all of p at off via positioned pwrite on the raw fd. It uses the
// raw descriptor (not *os.File.WriteAt) to avoid os.File's per-fd write lock, so
// concurrent aligned writes to disjoint offsets proceed in parallel.
func pwriteFull(fd int, p []byte, off int64) error {
	for len(p) > 0 {
		n, err := syscall.Pwrite(fd, p, off)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		if n == 0 {
			return fmt.Errorf("pwrite wrote 0 bytes at offset %d", off)
		}
		p = p[n:]
		off += int64(n)
	}
	return nil
}

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
