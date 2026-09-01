//go:build linux

package transfermanager

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

// newSyncChunkBuf allocates a buffer for fixedSizeBufPool, block-aligned for
// O_DIRECT (matching directBlockSize/alignedBuf, defined in
// downloadfile_directsink_linux.go).
func newSyncChunkBuf(size int64) []byte {
	return alignedBuf(size, directBlockSize)
}

// syncDirectSink is a benchmark-only FileWriterAt variant: no region map, no
// shards, no flush queue/workers. dlChunk.ReadFrom (via syncChunkSink) hands it a
// pooled, chunk-sized buffer per read and blocks on the O_DIRECT pwrite before
// returning, to measure the cost of removing write-behind async dispatch and the
// chunkedWriterAt copy-into-region-buffer step at the same time.
//
// Not wired into newFileSink/NewFileWriterAt; construct directly for A/B
// benchmarking against the write-behind sharded design.
type syncDirectSink struct {
	f         *os.File
	fd        int
	chunkSz   int64
	maxEndMu  sync.Mutex
	maxEndVal int64
}

// newSyncDirectSink opens path with O_DIRECT for synchronous, unpooled-queue
// writes. chunkSize is rounded up to the block size, matching directBackend.
func newSyncDirectSink(path string, chunkSize int64) (*syncDirectSink, error) {
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}
	if r := chunkSize % directBlockSize; r != 0 {
		chunkSize += directBlockSize - r
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open O_DIRECT %q: %w", path, err)
	}

	return &syncDirectSink{f: f, fd: int(f.Fd()), chunkSz: chunkSize}, nil
}

func (s *syncDirectSink) chunkSize() int64 { return s.chunkSz }

// writeSync pads buf's tail to the block size (like directBackend.writeRegion)
// and issues one synchronous pwrite before returning, blocking the caller
// (the part-worker) for the duration of the write.
func (s *syncDirectSink) writeSync(buf []byte, n int64, off int64) error {
	writeLen := n
	if r := writeLen % directBlockSize; r != 0 {
		pad := directBlockSize - r
		tail := buf[writeLen : writeLen+pad]
		for i := range tail {
			tail[i] = 0
		}
		writeLen += pad
	}
	if err := pwriteFull(s.fd, buf[:writeLen], off); err != nil {
		putSyncChunkBuf(buf)
		return err
	}
	putSyncChunkBuf(buf)

	s.maxEndMu.Lock()
	if end := off + n; end > s.maxEndVal {
		s.maxEndVal = end
	}
	s.maxEndMu.Unlock()
	return nil
}

// Close truncates to the exact object size, fdatasyncs, and closes the file.
func (s *syncDirectSink) Close() error {
	s.maxEndMu.Lock()
	size := s.maxEndVal
	s.maxEndMu.Unlock()

	if err := s.f.Truncate(size); err != nil {
		s.f.Close()
		return fmt.Errorf("truncate to %d: %w", size, err)
	}
	if err := syscall.Fdatasync(s.fd); err != nil {
		s.f.Close()
		return fmt.Errorf("fdatasync: %w", err)
	}
	return s.f.Close()
}

// WriteAt exists only so *syncDirectSink satisfies fileSink/io.WriterAt for
// callers that bypass dlChunk.ReadFrom's syncChunkSink fast path (it should never
// be exercised in the benchmark, since ReadFrom always takes the fast path when
// c.w implements syncChunkSink). It is not safe for concurrent use with itself
// and does not share writeSync's buffer pooling.
func (s *syncDirectSink) WriteAt(p []byte, off int64) (int, error) {
	buf := getSyncChunkBuf()
	n := copy(buf, p)
	// writeSync takes ownership of buf, including returning it to the pool.
	if err := s.writeSync(buf, int64(n), off); err != nil {
		return 0, err
	}
	return n, nil
}
