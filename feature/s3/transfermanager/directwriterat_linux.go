//go:build linux

package transfermanager

import (
	"fmt"
	"io"
	"os"
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

// directFileWriterAt applies the buffer length and alignment requirements for
// O_DIRECT. Async queueing is provided by writeBehindWriterAt independently of
// the destination type.
type directFileWriterAt struct {
	f *os.File
}

func newDirectFileWriterAt(f *os.File) (*directFileWriterAt, error) {
	if err := enableDirectIO(int(f.Fd())); err != nil {
		return nil, err
	}

	return &directFileWriterAt{f: f}, nil
}

func (w *directFileWriterAt) WriteAt(p []byte, off int64) (int, error) {
	logicalLen := len(p)
	writeLen := logicalLen
	if r := writeLen % directBlockSize; r != 0 {
		writeLen += directBlockSize - r
		if writeLen > cap(p) {
			return 0, fmt.Errorf("O_DIRECT write buffer capacity %d is smaller than padded length %d", cap(p), writeLen)
		}
		p = p[:writeLen]
		clear(p[logicalLen:])
	}

	n, err := w.f.WriteAt(p, off)
	if err != nil {
		if n > logicalLen {
			n = logicalLen
		}
		return n, err
	}
	if n != writeLen {
		if n > logicalLen {
			n = logicalLen
		}
		return n, io.ErrShortWrite
	}
	return logicalLen, nil
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
