//go:build linux

package transfermanager

import (
	"fmt"
	"sync"
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

// directFileWriterAt wraps a caller-supplied *os.File that DownloadObject has
// opted into O_DIRECT on. It writes synchronously via nolock pwrite on the raw fd
// (bypassing *os.File's internal fdMutex, which otherwise serializes all
// WriteAt/Write calls on the same *os.File even for disjoint, non-overlapping
// offsets), padding the tail of each write to the device block size as O_DIRECT
// requires.
//
// It implements syncChunkSink so dlChunk.ReadFrom takes its fast path, reading
// directly into a pooled aligned buffer instead of io.Copy's 32KB shuttle buffer.
//
// Correctness depends on every caller offset range being disjoint (true for
// DownloadObject's part/range workers, which own non-overlapping byte ranges) —
// this type does no locking of its own between concurrent WriteAt calls.
//
// It does not close the fd or take ownership of it beyond finalize: DownloadObject
// truncates the padded tail and fdatasyncs before the caller regains the file, but
// the caller is still the one who opened (and must close) the underlying *os.File.
type directFileWriterAt struct {
	fd int

	maxEndMu  sync.Mutex
	maxEndVal int64
}

// newDirectFileWriterAt enables O_DIRECT on fd and returns a WriterAt that writes
// through it.
func newDirectFileWriterAt(fd int) (*directFileWriterAt, error) {
	if err := enableDirectIO(fd); err != nil {
		return nil, err
	}
	return &directFileWriterAt{fd: fd}, nil
}

// chunkSize satisfies syncChunkSink: dlChunk.ReadFrom fills a buffer this size
// before each writeSync call.
func (w *directFileWriterAt) chunkSize() int64 { return defaultWriteChunkSizeBytes }

// writeSync takes ownership of buf (including returning it to the pool) and issues
// one pwrite before returning, blocking the caller for the write's duration.
func (w *directFileWriterAt) writeSync(buf []byte, n int64, off int64) error {
	writeLen := n
	if r := writeLen % directBlockSize; r != 0 {
		pad := directBlockSize - r
		tail := buf[writeLen : writeLen+pad]
		for i := range tail {
			tail[i] = 0
		}
		writeLen += pad
	}
	err := pwriteFull(w.fd, buf[:writeLen], off)
	putSyncChunkBuf(buf)
	if err != nil {
		return err
	}

	w.bumpMaxEnd(off + n)
	return nil
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
// not be exercised in practice, but it keeps directFileWriterAt a valid WriterAt on
// its own terms.
func (w *directFileWriterAt) WriteAt(p []byte, off int64) (int, error) {
	buf := getSyncChunkBuf()
	n := copy(buf, p)
	if err := w.writeSync(buf, int64(n), off); err != nil {
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
// does not close fd; the caller who opened the *os.File still owns closing it.
func finalizeDirectFile(fd int, size int64) error {
	if err := syscall.Ftruncate(fd, size); err != nil {
		return fmt.Errorf("truncate to %d: %w", size, err)
	}
	if err := syscall.Fdatasync(fd); err != nil {
		return fmt.Errorf("fdatasync: %w", err)
	}
	return nil
}
