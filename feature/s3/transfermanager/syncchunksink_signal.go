package transfermanager

import (
	"sync"
)

// syncChunkPool is a plain sync.Pool of fixed-size buffers for
// dlChunk.ReadFrom's syncChunkSink fast path. Sized once for
// defaultWriteChunkSizeBytes (this benchmark always runs with WriteChunkSizeBytes
// fixed at 8MiB — see bench.config.json's writeChunkSize — so there is only ever
// one buffer size in practice; no per-size map or wrapper mutex is needed).
//
// A wrapper mutex guarding a map[size]*sync.Pool was tried first, alongside a
// concurrency=512 run that regressed below concurrency=128's throughput while
// roughly doubling peak CPU (81% vs 36-42% at 256). The wrapper is the leading
// suspect: every Get/Put serialized on one global lock before ever reaching the
// pool, defeating sync.Pool's own per-P scaling (its fast path avoids locking by
// keeping a private cache per P; only the cross-P steal path synchronizes, and
// only when a P's local cache is empty). Not yet re-benchmarked to confirm the
// wrapper was the full explanation rather than a contributing factor, but a
// single sync.Pool with no wrapper removes that self-inflicted contention
// either way.
var syncChunkPool = sync.Pool{
	New: func() any { return newSyncChunkBuf(defaultWriteChunkSizeBytes) },
}

func getSyncChunkBuf() []byte    { return syncChunkPool.Get().([]byte) }
func putSyncChunkBuf(buf []byte) { syncChunkPool.Put(buf) }
