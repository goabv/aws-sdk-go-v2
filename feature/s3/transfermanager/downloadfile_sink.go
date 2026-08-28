package transfermanager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// fileSink is the destination DownloadFile writes an object into. The downloader
// streams part/range bodies into WriteAt (in ~16 KiB pieces from io.Copy); the
// sink coalesces them into fixed-size chunks before writing to disk, and Close
// flushes any partial trailing chunk and finalizes the file.
type fileSink interface {
	io.WriterAt
	io.Closer
}

// chunkSinkBackend abstracts the storage-specific parts of a chunkedWriterAt: how a
// region buffer is allocated, how a completed (or trailing partial) region is
// written to the file, and how the file is finalized. Two backends exist: a
// buffered *os.File backend (all platforms) and an O_DIRECT backend (Linux).
type chunkSinkBackend interface {
	// newBuf returns a buffer of length chunkSize to accumulate one region.
	newBuf() []byte
	// writeRegion writes n bytes of buf to the file at absolute offset off. n may be
	// less than chunkSize for the trailing region of the object.
	writeRegion(buf []byte, n int64, off int64) error
	// freeBuf releases a region buffer after its data has been written.
	freeBuf(buf []byte)
	// finalize completes the file given the total object size (highest offset+len
	// written). Backends that pad writes (O_DIRECT) truncate back to size here.
	finalize(size int64) error
}

const (
	chunkShards    = 256
	chunkShardMask = chunkShards - 1
)

type chunkRegion struct {
	buf    []byte
	filled int64
}

type chunkShard struct {
	mu      sync.Mutex
	regions map[int64]*chunkRegion
}

// chunkedWriterAt is an io.WriterAt that coalesces arbitrarily-sized, possibly
// out-of-order writes into fixed-size (chunkSize) regions and hands each completed
// region to a backend. Incoming writes are keyed to a region by offset/chunkSize;
// the region map is sharded so concurrent writers to different regions do not
// contend. A region is flushed as soon as it fills (chunkSize bytes received); any
// region still partial at Close is flushed then. This gives a fixed disk-write size
// regardless of the download range/part size.
type chunkedWriterAt struct {
	backend   chunkSinkBackend
	chunkSize int64
	maxEnd    atomic.Int64
	shards    [chunkShards]chunkShard
}

func newChunkedWriterAt(backend chunkSinkBackend, chunkSize int64) *chunkedWriterAt {
	w := &chunkedWriterAt{backend: backend, chunkSize: chunkSize}
	for i := range w.shards {
		w.shards[i].regions = make(map[int64]*chunkRegion)
	}
	return w
}

func (w *chunkedWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("chunkedWriterAt: negative offset")
	}
	total := len(p)
	for len(p) > 0 {
		ridx := off / w.chunkSize
		regionStart := ridx * w.chunkSize
		intra := off - regionStart
		take := w.chunkSize - intra
		if take > int64(len(p)) {
			take = int64(len(p))
		}

		sh := &w.shards[uint64(ridx)&chunkShardMask]
		sh.mu.Lock()
		r := sh.regions[ridx]
		if r == nil {
			r = &chunkRegion{buf: w.backend.newBuf()}
			sh.regions[ridx] = r
		}
		copy(r.buf[intra:intra+take], p[:take])
		r.filled += take
		var full []byte
		if r.filled >= w.chunkSize {
			full = r.buf
			delete(sh.regions, ridx)
		}
		sh.mu.Unlock()

		w.bumpMaxEnd(off + take)

		if full != nil {
			// The region is complete; no later WriteAt targets it, so this goroutine
			// solely owns the buffer and can write it outside the shard lock.
			if err := w.backend.writeRegion(full, w.chunkSize, regionStart); err != nil {
				w.backend.freeBuf(full)
				return total - len(p), err
			}
			w.backend.freeBuf(full)
		}

		off += take
		p = p[take:]
	}
	return total, nil
}

func (w *chunkedWriterAt) bumpMaxEnd(end int64) {
	for {
		cur := w.maxEnd.Load()
		if end <= cur {
			return
		}
		if w.maxEnd.CompareAndSwap(cur, end) {
			return
		}
	}
}

// Close flushes any regions that never filled (the object's trailing chunk) and
// finalizes the file. It is not safe to call WriteAt concurrently with Close.
func (w *chunkedWriterAt) Close() error {
	var firstErr error
	for i := range w.shards {
		sh := &w.shards[i]
		sh.mu.Lock()
		for ridx, r := range sh.regions {
			if err := w.backend.writeRegion(r.buf, r.filled, ridx*w.chunkSize); err != nil && firstErr == nil {
				firstErr = err
			}
			w.backend.freeBuf(r.buf)
			delete(sh.regions, ridx)
		}
		sh.mu.Unlock()
	}
	if err := w.backend.finalize(w.maxEnd.Load()); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// bufferedBackend writes regions through *os.File.WriteAt (page cache). Used for
// objects at or below the O_DIRECT threshold and as the fallback on platforms
// without O_DIRECT.
type bufferedBackend struct {
	f         *os.File
	chunkSize int64
}

func (b *bufferedBackend) newBuf() []byte { return make([]byte, b.chunkSize) }

func (b *bufferedBackend) writeRegion(buf []byte, n int64, off int64) error {
	_, err := b.f.WriteAt(buf[:n], off)
	return err
}

func (b *bufferedBackend) freeBuf(buf []byte) {}

func (b *bufferedBackend) finalize(size int64) error {
	return b.f.Close()
}

func newBufferedChunkSink(path string, chunkSize int64) (fileSink, error) {
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	return newChunkedWriterAt(&bufferedBackend{f: f, chunkSize: chunkSize}, chunkSize), nil
}

// newFileSink chooses the destination writer for DownloadFile. Objects strictly
// larger than DirectIOThreshold use the O_DIRECT backend when available (Linux)
// and not disabled; everything else uses the buffered backend. If opening in
// O_DIRECT fails (e.g. the filesystem does not support it), it falls back to the
// buffered writer so the download still succeeds.
func newFileSink(path string, size int64, o *Options) (fileSink, error) {
	chunkSize := o.WriteChunkSizeBytes
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}
	threshold := o.DirectIOThreshold
	if threshold <= 0 {
		threshold = defaultDirectIOThreshold
	}
	if !o.DisableDirectIO && size > threshold && directIOAvailable() {
		if s, err := newDirectChunkSink(path, chunkSize); err == nil {
			return s, nil
		}
		// O_DIRECT unsupported on this file/filesystem; fall back to buffered.
	}
	return newBufferedChunkSink(path, chunkSize)
}
