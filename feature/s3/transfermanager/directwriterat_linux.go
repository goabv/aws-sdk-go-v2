//go:build linux

package transfermanager

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// enableDirectIO toggles O_DIRECT on an already-open fd via fcntl, preserving the
// other file status flags. The caller opened the file normally; this lets
// DownloadObject opt an existing *os.File into O_DIRECT without reopening it.
//
// syscall.FcntlInt is not available on every linux GOARCH in the standard
// library (this module has no dependency on golang.org/x/sys/unix, which is
// where the portable wrapper normally lives), so this calls fcntl(2) directly
// via syscall.Syscall.
func enableDirectIO(fd int) error {
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(syscall.F_GETFL), 0)
	if errno != 0 {
		return fmt.Errorf("fcntl F_GETFL: %w", errno)
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), uintptr(syscall.F_SETFL), flags|uintptr(syscall.O_DIRECT)); errno != 0 {
		return fmt.Errorf("fcntl F_SETFL O_DIRECT: %w", errno)
	}
	return nil
}

// minDirectWriteWorkers is the floor for directWriteWorkers regardless of
// Concurrency, so low-concurrency downloads still get a small write-behind pool
// instead of falling back to near-lockstep behavior.
const minDirectWriteWorkers = 16

// directWriteQueueDepthFactor is the multiple of directWriteWorkers used as
// queue depth, so a burst can outrun the workers briefly without immediately
// reapplying back-pressure.
const directWriteQueueDepthFactor = 4

// directWriteWorkers returns the number of goroutines draining
// directFileWriterAt's write queue for a download running at the given part
// Concurrency. Scaled with Concurrency rather than fixed: a pool sized for a
// low-concurrency download becomes the bottleneck at high Concurrency, since
// network-read goroutines outrun a fixed-size drain pool and start blocking in
// writeSync's queue send instead of the disk. This bounds how many WriteAt
// calls are in flight at once, decoupling that from the number of
// network-reading part goroutines, while keeping that bound proportional to how
// many goroutines can be producing writes concurrently.
func directWriteWorkers(concurrency int) int {
	if concurrency < minDirectWriteWorkers {
		return minDirectWriteWorkers
	}
	return concurrency
}

// directWriteQueueDepth returns the write-behind queue depth for a pool of the
// given size: directWriteQueueDepthFactor times the worker count.
func directWriteQueueDepth(workers int) int {
	return workers * directWriteQueueDepthFactor
}

type directWriteJob struct {
	buf  []byte
	n    int64
	off  int64
	done chan error // non-nil only for WriteAt's synchronous fallback path
}

// directFileWriterAt wraps a caller-supplied *os.File that DownloadObject has
// opted into O_DIRECT on. It writes via the *os.File's own WriteAt, padding the
// tail of each write to the device block size as O_DIRECT requires.
//
// It implements syncChunkSink so dlChunk.ReadFrom takes its fast path, reading
// directly into a pooled aligned buffer instead of io.Copy's 32KB shuttle buffer.
// Writes are handed to a bounded queue and drained by a pool of worker
// goroutines sized to Concurrency (write-behind) rather than issued on the
// caller's goroutine: at high part concurrency a network-read goroutine that
// blocks in WriteAt stops draining its own socket for the write's duration,
// which idles that connection's share of the NIC even though WriteAt itself
// doesn't serialize across goroutines (disjoint offsets proceed independently
// -- there is nothing to bypass on the disk side, only on the caller's own
// critical path).
type directFileWriterAt struct {
	f *os.File

	queue chan directWriteJob
	wg    sync.WaitGroup

	maxEndMu  sync.Mutex
	maxEndVal int64

	errOnce sync.Once
	errVal  atomic.Pointer[error]
}

// newDirectFileWriterAt enables O_DIRECT on f's fd, starts the write-behind
// worker pool sized for concurrency, and returns a WriterAt that writes through
// it.
func newDirectFileWriterAt(f *os.File, concurrency int) (*directFileWriterAt, error) {
	if err := enableDirectIO(int(f.Fd())); err != nil {
		return nil, err
	}

	workers := directWriteWorkers(concurrency)
	w := &directFileWriterAt{
		f:     f,
		queue: make(chan directWriteJob, directWriteQueueDepth(workers)),
	}
	w.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go w.writeWorker()
	}
	return w, nil
}

// chunkSize satisfies syncChunkSink: dlChunk.ReadFrom fills a buffer this size
// before each writeSync call.
func (w *directFileWriterAt) chunkSize() int64 { return defaultWriteChunkSizeBytes }

// writeSync takes ownership of buf (including eventually returning it to the
// pool) and enqueues it for a worker to write, blocking the caller only if the
// queue is full -- not for the write's duration. It returns the first error
// observed by any worker so far, if one has already occurred, so a failing
// download stops enqueueing promptly instead of continuing to buffer work behind
// a broken destination.
func (w *directFileWriterAt) writeSync(buf []byte, n int64, off int64) error {
	if err := w.err(); err != nil {
		putSyncChunkBuf(buf)
		return err
	}

	w.queue <- directWriteJob{buf: buf, n: n, off: off}
	return nil
}

func (w *directFileWriterAt) writeWorker() {
	defer w.wg.Done()

	for job := range w.queue {
		w.doWrite(job)
	}
}

func (w *directFileWriterAt) doWrite(job directWriteJob) {
	defer putSyncChunkBuf(job.buf)

	writeLen := job.n
	if r := writeLen % directBlockSize; r != 0 {
		pad := directBlockSize - r
		tail := job.buf[writeLen : writeLen+pad]
		for i := range tail {
			tail[i] = 0
		}
		writeLen += pad
	}

	_, err := w.f.WriteAt(job.buf[:writeLen], job.off)
	if err != nil {
		err = fmt.Errorf("write-behind WriteAt at offset %d: %w", job.off, err)
		w.setErr(err)
	} else {
		w.bumpMaxEnd(job.off + job.n)
	}

	if job.done != nil {
		job.done <- err
	}
}

func (w *directFileWriterAt) err() error {
	if p := w.errVal.Load(); p != nil {
		return *p
	}
	return nil
}

func (w *directFileWriterAt) setErr(err error) {
	w.errOnce.Do(func() {
		w.errVal.Store(&err)
	})
}

// drain blocks until every write already handed to writeSync has been issued
// (successfully or not) and its buffer returned to the pool, then stops the
// worker pool. Callers must not call writeSync again after calling drain. It
// returns the first write error observed, if any.
func (w *directFileWriterAt) drain() error {
	close(w.queue)
	w.wg.Wait()
	return w.err()
}

func (w *directFileWriterAt) bumpMaxEnd(end int64) {
	w.maxEndMu.Lock()
	if end > w.maxEndVal {
		w.maxEndVal = end
	}
	w.maxEndMu.Unlock()
}

// WriteAt satisfies io.WriterAt for callers that bypass dlChunk.ReadFrom's fast
// path. DownloadObject's own copy loop always takes that fast path, so this should
// not be exercised in practice. Unlike writeSync, WriteAt's io.WriterAt contract
// requires the write to have landed (or failed) before it returns, so this waits
// for its own job rather than handing back the write-behind pool's "enqueued"
// result.
func (w *directFileWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if err := w.err(); err != nil {
		return 0, err
	}

	buf := getSyncChunkBuf()
	n := copy(buf, p)

	done := make(chan error, 1)
	w.queue <- directWriteJob{buf: buf, n: int64(n), off: off, done: done}
	if err := <-done; err != nil {
		return 0, err
	}
	return n, nil
}

func (w *directFileWriterAt) finalSize() int64 {
	w.maxEndMu.Lock()
	defer w.maxEndMu.Unlock()
	return w.maxEndVal
}

// finalizeDirectFile truncates the file to the exact object size (undoing
// O_DIRECT's block padding) and fdatasyncs so the completed data is durable. It
// does not close the file; the caller who opened it still owns closing it.
func finalizeDirectFile(f *os.File, size int64) error {
	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("truncate to %d: %w", size, err)
	}
	if err := syscall.Fdatasync(int(f.Fd())); err != nil {
		return fmt.Errorf("fdatasync: %w", err)
	}
	return nil
}
