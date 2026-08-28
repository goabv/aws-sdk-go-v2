//go:build linux

package transfermanager

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// directBlockSize is the alignment (bytes) O_DIRECT requires for the write offset,
// length, and buffer address. 4096 satisfies the logical block size of common
// devices/filesystems.
const directBlockSize = 4096

func directIOAvailable() bool { return true }

// newDirectChunkSink opens path with O_DIRECT and returns a chunkedWriterAt that
// coalesces incoming writes into chunkSize (rounded up to the block size) aligned
// regions and writes them straight to the device, bypassing the page cache.
func newDirectChunkSink(path string, chunkSize int64) (fileSink, error) {
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}
	// O_DIRECT requires block-aligned write lengths; round the region size up so full
	// regions are aligned. Region offsets are multiples of chunkSize, so they are
	// aligned too.
	if r := chunkSize % directBlockSize; r != 0 {
		chunkSize += directBlockSize - r
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open O_DIRECT %q: %w", path, err)
	}
	b := &directBackend{
		f:         f,
		fd:        int(f.Fd()),
		chunkSize: chunkSize,
	}
	return newChunkedWriterAt(b, chunkSize), nil
}

type directBackend struct {
	f         *os.File
	fd        int
	chunkSize int64
}

func (b *directBackend) newBuf() []byte { return alignedBuf(b.chunkSize, directBlockSize) }

func (b *directBackend) writeRegion(buf []byte, n int64, off int64) error {
	// Round the write length up to the block size (O_DIRECT requirement). The buffer
	// is a full, block-aligned chunkSize, so there is room. Bytes in [n, writeLen)
	// lie beyond the object end; zero them so no stale data reaches disk, then let
	// the truncate in finalize chop them off.
	writeLen := n
	if r := writeLen % directBlockSize; r != 0 {
		pad := directBlockSize - r
		tail := buf[writeLen : writeLen+pad]
		for i := range tail {
			tail[i] = 0
		}
		writeLen += pad
	}
	return pwriteFull(b.fd, buf[:writeLen], off)
}

func (b *directBackend) freeBuf(buf []byte) {}

func (b *directBackend) finalize(size int64) error {
	// O_DIRECT writes are padded to the block size, so the file may be a little
	// larger than the object; truncate it back to the exact size.
	if err := b.f.Truncate(size); err != nil {
		b.f.Close()
		return fmt.Errorf("truncate to %d: %w", size, err)
	}
	return b.f.Close()
}

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
