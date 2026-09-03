//go:build !linux

package transfermanager

import (
	"fmt"
	"os"
)

func directIOAvailable() bool { return false }

// newSyncChunkBuf is never actually called on non-Linux platforms
// (directIOAvailable is false, so DownloadObject never opts a *os.File into
// O_DIRECT), but syncChunkPool's sync.Pool.New is package-level and must resolve
// on every platform.
func newSyncChunkBuf(size int64) []byte { return make([]byte, size) }

// directFileWriterAt is unavailable on non-Linux platforms; DownloadObject never
// constructs one when directIOAvailable() is false.
type directFileWriterAt struct{}

func newDirectFileWriterAt(f *os.File, concurrency int) (*directFileWriterAt, error) {
	return nil, fmt.Errorf("O_DIRECT is not supported on this platform")
}

func (w *directFileWriterAt) WriteAt(p []byte, off int64) (int, error)       { return 0, nil }
func (w *directFileWriterAt) chunkSize() int64                               { return 0 }
func (w *directFileWriterAt) writeSync(buf []byte, n int64, off int64) error { return nil }
func (w *directFileWriterAt) finalSize() int64                               { return 0 }
func (w *directFileWriterAt) drain() error                                   { return nil }

func finalizeDirectFile(f *os.File, size int64) error { return nil }
