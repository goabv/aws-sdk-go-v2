//go:build linux

package transfermanager

import (
	"bytes"
	"os"
	"sync"
	"testing"
)

func TestDirectFileWriterAt_WriteBehind(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "directwriterat-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	w, err := newDirectFileWriterAt(f, 200)
	if err != nil {
		t.Skipf("O_DIRECT not available in this environment: %v", err)
	}

	const nChunks = 200
	chunkSize := int(w.chunkSize())

	var wg sync.WaitGroup
	for i := 0; i < nChunks; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := getSyncChunkBuf()
			for j := range buf[:chunkSize] {
				buf[j] = byte(i)
			}
			if err := w.writeSync(buf, int64(chunkSize), int64(i*chunkSize)); err != nil {
				t.Errorf("writeSync chunk %d: %v", i, err)
			}
		}()
	}
	wg.Wait()

	if err := w.drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}

	wantSize := int64(nChunks * chunkSize)
	if got := w.finalSize(); got != wantSize {
		t.Fatalf("finalSize = %d, want %d", got, wantSize)
	}

	if err := finalizeDirectFile(f, wantSize); err != nil {
		t.Fatalf("finalizeDirectFile: %v", err)
	}

	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("read back file: %v", err)
	}
	if int64(len(got)) != wantSize {
		t.Fatalf("file size = %d, want %d", len(got), wantSize)
	}
	for i := 0; i < nChunks; i++ {
		want := bytes.Repeat([]byte{byte(i)}, chunkSize)
		got := got[i*chunkSize : (i+1)*chunkSize]
		if !bytes.Equal(got, want) {
			t.Fatalf("chunk %d mismatch", i)
		}
	}
}

func TestDirectWriteWorkers_ScalesWithConcurrency(t *testing.T) {
	tests := []struct {
		concurrency int
		wantWorkers int
	}{
		{concurrency: 1, wantWorkers: minDirectWriteWorkers},
		{concurrency: minDirectWriteWorkers, wantWorkers: minDirectWriteWorkers},
		{concurrency: 128, wantWorkers: 128},
		{concurrency: 512, wantWorkers: 512},
	}

	for _, tt := range tests {
		workers := directWriteWorkers(tt.concurrency)
		if workers != tt.wantWorkers {
			t.Errorf("directWriteWorkers(%d) = %d, want %d", tt.concurrency, workers, tt.wantWorkers)
		}

		depth := directWriteQueueDepth(workers)
		if want := workers * directWriteQueueDepthFactor; depth != want {
			t.Errorf("directWriteQueueDepth(%d) = %d, want %d", workers, depth, want)
		}
	}
}

func TestDirectFileWriterAt_DrainAfterError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "directwriterat-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	w, err := newDirectFileWriterAt(f, 32)
	if err != nil {
		t.Skipf("O_DIRECT not available in this environment: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	buf := getSyncChunkBuf()
	if err := w.writeSync(buf, w.chunkSize(), 0); err != nil {
		t.Fatalf("writeSync (enqueue) unexpected error: %v", err)
	}

	if err := w.drain(); err == nil {
		t.Fatal("drain: expected error from write to closed file, got nil")
	}

	buf2 := getSyncChunkBuf()
	if err := w.writeSync(buf2, w.chunkSize(), 0); err == nil {
		t.Fatal("writeSync after drain error: expected error to be returned immediately")
	}
}
