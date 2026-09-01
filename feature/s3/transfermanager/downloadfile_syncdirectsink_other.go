//go:build !linux

package transfermanager

// newSyncChunkBuf allocates a plain (unaligned) buffer on non-Linux platforms,
// where syncDirectSink (the only consumer that needs O_DIRECT alignment) is
// unavailable; alignment doesn't matter here since nothing on this platform
// issues an O_DIRECT write with it.
func newSyncChunkBuf(size int64) []byte {
	return make([]byte, size)
}
