//go:build linux

package transfermanager

// newDownloadFileSink is DownloadFile's sink constructor for this benchmark
// build: hardcoded to the synchronous, pooled-buffer O_DIRECT sink
// (syncDirectSink) rather than the write-behind sharded design
// (chunkedWriterAt/newFileSink), to measure removing both the region-map copy
// and the async flush queue at once. size and o.DisableDirectIO/
// o.DirectIOThreshold are accepted for signature parity with newFileSink but
// unused — this build always takes the O_DIRECT-sync path on Linux.
func newDownloadFileSink(path string, size int64, o *Options) (fileSink, error) {
	return newSyncDirectSink(path, o.WriteChunkSizeBytes)
}
