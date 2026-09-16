//go:build linux

package transfermanager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"
)

func openDownloadFile(path string) (*os.File, io.WriterAt, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_DIRECT, 0o644)
	if err == nil {
		return f, &directFileWriterAt{fd: int(f.Fd())}, nil
	}
	if !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.EOPNOTSUPP) {
		return nil, nil, err
	}

	f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

type directFileWriterAt struct {
	fd int
}

func (*directFileWriterAt) writeBufferAlignment() int64 {
	return directIOAlignment
}

func (w *directFileWriterAt) preallocate(size int64) error {
	if size <= 0 {
		return nil
	}
	for {
		err := syscall.Fallocate(w.fd, 0, 0, size)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

func (w *directFileWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off%directIOAlignment != 0 {
		return 0, fmt.Errorf("O_DIRECT write offset %d is not aligned to %d bytes", off, directIOAlignment)
	}
	if uintptr(unsafe.Pointer(unsafe.SliceData(p)))%directIOAlignment != 0 {
		return 0, fmt.Errorf("O_DIRECT write buffer is not aligned to %d bytes", directIOAlignment)
	}

	logicalLen := len(p)
	writeLen := logicalLen
	if remainder := writeLen % directIOAlignment; remainder != 0 {
		writeLen += directIOAlignment - remainder
		if writeLen > cap(p) {
			return 0, fmt.Errorf("O_DIRECT write buffer capacity %d is smaller than padded length %d", cap(p), writeLen)
		}
		p = p[:writeLen]
		clear(p[logicalLen:])
	}

	for {
		n, err := syscall.Pwrite(w.fd, p, off)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		logicalN := min(n, logicalLen)
		if err != nil {
			return logicalN, err
		}
		if n != writeLen {
			return logicalN, io.ErrShortWrite
		}
		return logicalLen, nil
	}
}
