//go:build linux

package transfermanager

import (
	"fmt"
	"os"
	"sync"
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
// regions and writes them straight to the device (bypassing the page cache) behind
// a pool of flushWorkers draining a queueDepth-bounded queue. When poolBuffers is
// set, aligned region buffers are recycled through a sync.Pool so the sink does not
// malloc a fresh region for every chunk at line rate.
func newDirectChunkSink(path string, chunkSize int64, flushWorkers, queueDepth int, poolBuffers bool) (fileSink, error) {
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
	if poolBuffers {
		b.bufPool = newAlignedBufPool(chunkSize, directBlockSize)
	}
	return newChunkedWriterAt(b, chunkSize, flushWorkers, queueDepth), nil
}

type directBackend struct {
	f         *os.File
	fd        int
	chunkSize int64
	bufPool   *alignedBufPool // nil when buffer pooling is disabled
}

func (b *directBackend) newBuf() []byte {
	if b.bufPool != nil {
		return b.bufPool.get()
	}
	return alignedBuf(b.chunkSize, directBlockSize)
}

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

func (b *directBackend) freeBuf(buf []byte) {
	if b.bufPool != nil {
		b.bufPool.put(buf)
	}
}

func (b *directBackend) finalize(size int64) error {
	// O_DIRECT writes are padded to the block size, so the file may be a little
	// larger than the object; truncate it back to the exact size.
	if err := b.f.Truncate(size); err != nil {
		b.f.Close()
		return fmt.Errorf("truncate to %d: %w", size, err)
	}
	// Flush the device write cache and the (truncated) size metadata to stable
	// media before returning. O_DIRECT already sent the data blocks to the device,
	// so this is just a cache flush + metadata commit — one fdatasync per file,
	// cheap relative to the transfer, and it makes the completed file durable.
	if err := syscall.Fdatasync(b.fd); err != nil {
		b.f.Close()
		return fmt.Errorf("fdatasync: %w", err)
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

// alignedBufPool recycles block-aligned region buffers of a fixed size so the
// O_DIRECT sink does not malloc a fresh region for every chunk. With a write-behind
// pool, up to (queueDepth + flushWorkers + in-flight-part-workers) region buffers
// are live at once; recycling them keeps steady-state allocation near zero. The
// non-moving GC keeps a buffer's backing address stable, so a recycled buffer stays
// legal for O_DIRECT. sync.Pool lets the runtime reclaim idle buffers under pressure.
type alignedBufPool struct {
	pool  sync.Pool
	size  int64
	align int64
}

func newAlignedBufPool(size, align int64) *alignedBufPool {
	p := &alignedBufPool{size: size, align: align}
	p.pool.New = func() any { return alignedBuf(size, align) }
	return p
}

func (p *alignedBufPool) get() []byte { return p.pool.Get().([]byte) }

func (p *alignedBufPool) put(b []byte) {
	if int64(cap(b)) < p.size {
		return // lost capacity; drop rather than violate the size/alignment contract
	}
	p.pool.Put(b[:p.size])
}
