//go:build !linux

package transfermanager

import "fmt"

// directIOAvailable reports that O_DIRECT is not available on this platform, so
// newFileSink always selects the buffered backend.
func directIOAvailable() bool { return false }

// newDirectChunkSink is never called on non-Linux platforms (directIOAvailable is
// false); it exists so the package compiles everywhere.
func newDirectChunkSink(path string, chunkSize int64, flushWorkers, queueDepth int) (fileSink, error) {
	return nil, fmt.Errorf("O_DIRECT is not supported on this platform")
}
