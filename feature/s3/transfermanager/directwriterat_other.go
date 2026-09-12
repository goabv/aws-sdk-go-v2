//go:build !linux

package transfermanager

import (
	"fmt"
	"os"
)

func directIOAvailable() bool { return false }

// newSyncChunkBuf allocates a generic write-behind buffer. Non-Linux
// platforms do not need the additional address alignment required by O_DIRECT.
func newSyncChunkBuf(size int64) []byte { return make([]byte, size) }

// directFileWriterAt is unavailable on non-Linux platforms; DownloadObject never
// constructs one when directIOAvailable() is false.
type directFileWriterAt struct{}

func newDirectFileWriterAt(f *os.File) (*directFileWriterAt, error) {
	return nil, fmt.Errorf("O_DIRECT is not supported on this platform")
}

func (w *directFileWriterAt) WriteAt(p []byte, off int64) (int, error) {
	return 0, fmt.Errorf("O_DIRECT is not supported on this platform")
}

func finalizeDirectFile(f *os.File, size int64) error { return nil }
