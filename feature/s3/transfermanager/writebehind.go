package transfermanager

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

const writeBehindQueueDepth = 64
const writeBehindWorkers = 64

type writeBehindJob struct {
	buf  []byte
	n    int64
	off  int64
	seq  uint64
	done chan writeBehindResult
}

type writeBehindResult struct {
	n   int
	err error
}

type writeBehindWriterAt struct {
	w         io.WriterAt
	chunkSize int64
	pool      sync.Pool
	queue     chan writeBehindJob
	wg        sync.WaitGroup

	closeMu sync.RWMutex
	closed  bool

	nextSeq          atomic.Uint64
	completeMu       sync.Mutex
	completeCond     *sync.Cond
	completedThrough uint64
	completed        map[uint64]struct{}

	errOnce sync.Once
	errVal  atomic.Pointer[error]
}

func newWriteBehindWriterAt(w io.WriterAt, chunkSize int64) *writeBehindWriterAt {
	if chunkSize <= 0 {
		chunkSize = defaultWriteChunkSizeBytes
	}

	writer := &writeBehindWriterAt{
		w:         w,
		chunkSize: chunkSize,
		queue:     make(chan writeBehindJob, writeBehindQueueDepth),
	}
	writer.pool.New = func() any { return make([]byte, chunkSize) }
	writer.completeCond = sync.NewCond(&writer.completeMu)
	writer.completed = map[uint64]struct{}{}
	writer.wg.Add(writeBehindWorkers)
	for i := 0; i < writeBehindWorkers; i++ {
		go writer.writeWorker()
	}
	return writer
}

func (w *writeBehindWriterAt) getBuffer() []byte {
	return w.pool.Get().([]byte)
}

func (w *writeBehindWriterAt) putBuffer(buf []byte) {
	w.pool.Put(buf[:w.chunkSize])
}

func (w *writeBehindWriterAt) enqueue(buf []byte, n int64, off int64) (uint64, error) {
	if err := w.err(); err != nil {
		w.putBuffer(buf)
		return 0, err
	}

	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	if w.closed {
		w.putBuffer(buf)
		return 0, fmt.Errorf("write-behind is closed")
	}

	seq := w.nextSeq.Add(1)
	w.queue <- writeBehindJob{buf: buf, n: n, off: off, seq: seq}
	return seq, nil
}

func (w *writeBehindWriterAt) writeWorker() {
	defer w.wg.Done()

	for job := range w.queue {
		w.doWrite(job)
	}
}

func (w *writeBehindWriterAt) doWrite(job writeBehindJob) {
	n, err := w.w.WriteAt(job.buf[:job.n], job.off)
	if n != int(job.n) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("write-behind WriteAt at offset %d: %w", job.off, err)
		w.setErr(err)
	}

	w.putBuffer(job.buf)
	w.markComplete(job.seq)
	if job.done != nil {
		job.done <- writeBehindResult{n: n, err: err}
	}
}

func (w *writeBehindWriterAt) WriteAt(p []byte, off int64) (int, error) {
	var written int
	for len(p) > 0 {
		if err := w.err(); err != nil {
			return written, err
		}

		buf := w.getBuffer()
		n := copy(buf, p)
		done := make(chan writeBehindResult, 1)

		w.closeMu.RLock()
		if w.closed {
			w.closeMu.RUnlock()
			w.putBuffer(buf)
			return written, fmt.Errorf("write-behind is closed")
		}
		seq := w.nextSeq.Add(1)
		w.queue <- writeBehindJob{buf: buf, n: int64(n), off: off + int64(written), seq: seq, done: done}
		w.closeMu.RUnlock()

		result := <-done
		written += result.n
		if result.err != nil {
			return written, result.err
		}
		p = p[n:]
	}
	return written, nil
}

func (w *writeBehindWriterAt) waitThrough(seq uint64) error {
	w.completeMu.Lock()
	for w.completedThrough < seq {
		w.completeCond.Wait()
	}
	w.completeMu.Unlock()
	return w.err()
}

func (w *writeBehindWriterAt) markComplete(seq uint64) {
	w.completeMu.Lock()
	defer w.completeMu.Unlock()
	if seq != w.completedThrough+1 {
		w.completed[seq] = struct{}{}
		return
	}

	w.completedThrough = seq
	for {
		next := w.completedThrough + 1
		if _, ok := w.completed[next]; !ok {
			break
		}
		delete(w.completed, next)
		w.completedThrough = next
	}
	w.completeCond.Broadcast()
}

func (w *writeBehindWriterAt) err() error {
	if p := w.errVal.Load(); p != nil {
		return *p
	}
	return nil
}

func (w *writeBehindWriterAt) setErr(err error) {
	w.errOnce.Do(func() {
		w.errVal.Store(&err)
	})
}

func (w *writeBehindWriterAt) drain() error {
	w.closeMu.Lock()
	if !w.closed {
		w.closed = true
		close(w.queue)
	}
	w.closeMu.Unlock()

	w.wg.Wait()
	return w.err()
}
