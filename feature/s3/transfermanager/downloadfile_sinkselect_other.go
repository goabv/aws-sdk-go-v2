//go:build !linux

package transfermanager

// newDownloadFileSink falls back to the original write-behind sharded sink
// selection on non-Linux platforms, where syncDirectSink (O_DIRECT-only) is
// unavailable.
func newDownloadFileSink(path string, size int64, o *Options) (fileSink, error) {
	return newFileSink(path, size, o)
}
