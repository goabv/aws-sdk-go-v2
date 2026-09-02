package transfermanager

import (
	"sync"
)

// syncChunkPools holds one sync.Pool per distinct chunk size requested by a
// syncChunkSink. WriteChunkSizeBytes is configurable (rounded up to the device
// block size by forceRangesForDirectIO), so more than one size can legitimately
// be in play across concurrent downloads with different Options.
//
// A single global map[size]*sync.Pool guarded by one sync.Mutex was tried first,
// alongside a concurrency=512 run that regressed below concurrency=128's
// throughput while roughly doubling peak CPU (81% vs 36-42% at 256): every
// Get/Put serialized on that one lock before ever reaching the pool underneath,
// defeating sync.Pool's own per-P scaling (its fast path avoids locking by
// keeping a private cache per P; only the cross-P steal path synchronizes, and
// only when a P's local cache is empty). sync.Map's load path takes no lock once
// an entry is warm, so looking up the per-size pool no longer serializes callers
// using the same size against each other.
var syncChunkPools sync.Map // int64 chunk size -> *sync.Pool

func syncChunkPoolFor(size int64) *sync.Pool {
	if p, ok := syncChunkPools.Load(size); ok {
		return p.(*sync.Pool)
	}
	p, _ := syncChunkPools.LoadOrStore(size, &sync.Pool{
		New: func() any { return newSyncChunkBuf(size) },
	})
	return p.(*sync.Pool)
}

func getSyncChunkBuf(size int64) []byte { return syncChunkPoolFor(size).Get().([]byte) }
func putSyncChunkBuf(buf []byte)        { syncChunkPoolFor(int64(cap(buf))).Put(buf) }
