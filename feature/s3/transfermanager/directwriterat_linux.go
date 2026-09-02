//go:build linux

package transfermanager

import (
	"fmt"
	"os"
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
// opted into O_DIRECT on. It writes via the *os.File's own WriteAt, padding the
// tail of each write to the device block size as O_DIRECT requires.
//
// It implements syncChunkSink so dlChunk.ReadFrom takes its fast path, reading
// directly into a pooled aligned buffer instead of io.Copy's 32KB shuttle buffer
// -- that is the actual benefit of this type; *os.File.WriteAt itself already
// permits concurrent calls at disjoint offsets to proceed independently (it takes
// the fd's non-blocking refcount path, not the lock that serializes plain
// Read/Write), so there is nothing to bypass there.
type directFileWriterAt struct {
	f              *os.File
	writeChunkSize int64

	maxEndMu  sync.Mutex
	maxEndVal int64
}

// newDirectFileWriterAt enables O_DIRECT on f's fd and returns a WriterAt that
// writes through it in writeChunkSize-sized chunks (the caller has already
// rounded this up to the device block size; see forceRangesForDirectIO).
func newDirectFileWriterAt(f *os.File, writeChunkSize int64) (*directFileWriterAt, error) {
	if err := enableDirectIO(int(f.Fd())); err != nil {
		return nil, err
	}
	return &directFileWriterAt{f: f, writeChunkSize: writeChunkSize}, nil
}

// chunkSize satisfies syncChunkSink: dlChunk.ReadFrom fills a buffer this size
// before each writeSync call.
func (w *directFileWriterAt) chunkSize() int64 { return w.writeChunkSize }

// writeSync takes ownership of buf (including returning it to the pool) and issues
// one WriteAt before returning, blocking the caller for the write's duration.
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
	_, err := w.f.WriteAt(buf[:writeLen], off)
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
	buf := getSyncChunkBuf(w.writeChunkSize)
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
