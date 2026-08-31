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
// sink coalesces them into fixed-size chunks and hands each completed chunk to a
// pool of flush workers (write-behind), so the download part-workers are not
// blocked on disk I/O. Close flushes any partial trailing chunk and finalizes.
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
	// freeBuf releases (or recycles) a region buffer after its data has been written.
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

// flushJob is a completed (or trailing partial) region handed to a flush worker to
// be written to the file at off; n is the number of valid bytes in buf.
type flushJob struct {
	buf []byte
	n   int64
	off int64
}

// chunkedWriterAt is an io.WriterAt that coalesces arbitrarily-sized, possibly
// out-of-order writes into fixed-size (chunkSize) regions and writes them behind a
// bounded queue drained by a pool of flush workers, so the download part-workers
// never block on the disk write (network receive is decoupled from disk write).
//
// Incoming writes are keyed to a region by offset/chunkSize; the region map is
// sharded so concurrent writers to different regions do not contend. A region is
// enqueued for flushing as soon as it fills (chunkSize bytes received); the queue
// bounds how many filled regions may wait (backpressure if the disk falls behind),
// and the worker count caps how many writes hit the device at once. Any region
// still partial at Close is enqueued then. This gives a fixed disk-write size
// regardless of the download range/part size.
type chunkedWriterAt struct {
	backend   chunkSinkBackend
	chunkSize int64
	maxEnd    atomic.Int64

	jobs   chan flushJob
	wg     sync.WaitGroup
	failed atomic.Bool
	errMu  sync.Mutex
	err    error

	shards [chunkShards]chunkShard
}

func newChunkedWriterAt(backend chunkSinkBackend, chunkSize int64, flushWorkers, queueDepth int) *chunkedWriterAt {
	if flushWorkers <= 0 {
		flushWorkers = defaultWriteFlushWorkers
	}
	if queueDepth <= 0 {
		queueDepth = defaultWriteFlushQueueDepth
	}
	w := &chunkedWriterAt{
		backend:   backend,
		chunkSize: chunkSize,
		jobs:      make(chan flushJob, queueDepth),
	}
	for i := range w.shards {
		w.shards[i].regions = make(map[int64]*chunkRegion)
	}
	w.wg.Add(flushWorkers)
	for i := 0; i < flushWorkers; i++ {
		go w.flushLoop()
	}
	return w
}

// flushLoop drains completed regions and writes them to the backend. Up to
// flushWorkers of these run concurrently, so at most that many writes hit the
// device at once. A worker keeps draining after an error (so senders never block)
// but skips the write once a prior write has failed.
func (w *chunkedWriterAt) flushLoop() {
	defer w.wg.Done()
	for job := range w.jobs {
		if !w.failed.Load() {
			if err := w.backend.writeRegion(job.buf, job.n, job.off); err != nil {
				w.setErr(err)
			}
		}
		w.backend.freeBuf(job.buf)
	}
}

func (w *chunkedWriterAt) setErr(err error) {
	w.errMu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.errMu.Unlock()
	w.failed.Store(true)
}

func (w *chunkedWriterAt) loadErr() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.err
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
			// Region complete: hand it to the flush pool. The send blocks when the
			// queue is full (backpressure onto the download) but never on this
			// goroutine's own disk write.
			w.jobs <- flushJob{buf: full, n: w.chunkSize, off: regionStart}
			if w.failed.Load() {
				return total - len(p), w.loadErr()
			}
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

// Close enqueues the trailing partial regions (those that never filled), drains and
// stops the flush pool, then finalizes the file. It must not be called concurrently
// with WriteAt — the downloader has finished all part-workers before DownloadFile
// calls Close.
func (w *chunkedWriterAt) Close() error {
	// Collect partial regions, then enqueue them after releasing the shard locks so a
	// full queue can never deadlock against a worker.
	var partials []flushJob
	for i := range w.shards {
		sh := &w.shards[i]
		sh.mu.Lock()
		for ridx, r := range sh.regions {
			partials = append(partials, flushJob{buf: r.buf, n: r.filled, off: ridx * w.chunkSize})
			delete(sh.regions, ridx)
		}
		sh.mu.Unlock()
	}
	for _, j := range partials {
		w.jobs <- j
	}
	close(w.jobs)
	w.wg.Wait()

	err := w.loadErr()
	if ferr := w.backend.finalize(w.maxEnd.Load()); ferr != nil && err == nil {
		err = ferr
	}
	return err
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
	// Flush buffered data + metadata to stable media before closing, so the
	// completed file is durable. Portable (fsync on Unix, FlushFileBuffers on
	// Windows). One call per file at the end of the transfer.
	if err := b.f.Sync(); err != nil {
		b.f.Close()
		return fmt.Errorf("fsync: %w", err)
	}
	return b.f.Close()
}

func newBufferedChunkSink(path string, chunkSize int64, flushWorkers, queueDepth int) (fileSink, error) {
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	return newChunkedWriterAt(&bufferedBackend{f: f, chunkSize: chunkSize}, chunkSize, flushWorkers, queueDepth), nil
}

// newFileSink chooses the destination writer for DownloadFile. Objects strictly
// larger than DirectIOThreshold use the O_DIRECT backend when available (Linux) and
// not disabled; everything else uses the buffered backend. If opening in O_DIRECT
// fails (e.g. the filesystem does not support it), it falls back to the buffered
// writer so the download still succeeds. Both backends use the same write-behind
// flush pool (WriteFlushWorkers / WriteFlushQueueDepth).
func newFileSink(path string, size int64, o *Options) (fileSink, error) {
	chunkSize := o.WriteChunkSizeBytes
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}
	threshold := o.DirectIOThreshold
	if threshold <= 0 {
		threshold = defaultDirectIOThreshold
	}
	flushWorkers := o.WriteFlushWorkers
	queueDepth := o.WriteFlushQueueDepth
	if !o.DisableDirectIO && size > threshold && directIOAvailable() {
		if s, err := newDirectChunkSink(path, chunkSize, flushWorkers, queueDepth); err == nil {
			return s, nil
		}
		// O_DIRECT unsupported on this file/filesystem; fall back to buffered.
	}
	return newBufferedChunkSink(path, chunkSize, flushWorkers, queueDepth)
}
