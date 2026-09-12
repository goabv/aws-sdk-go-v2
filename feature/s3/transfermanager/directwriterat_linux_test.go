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

	direct, err := newDirectFileWriterAt(f)
	if err != nil {
		t.Skipf("O_DIRECT not available in this environment: %v", err)
	}
	w := newWriteBehindWriterAt(direct, defaultWriteChunkSizeBytes)

	const nChunks = 200
	chunkSize := int(w.chunkSize)

	var wg sync.WaitGroup
	for i := 0; i < nChunks; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := w.getBuffer()
			for j := range buf[:chunkSize] {
				buf[j] = byte(i)
			}
			if _, err := w.enqueue(buf, int64(chunkSize), int64(i*chunkSize)); err != nil {
				t.Errorf("enqueue chunk %d: %v", i, err)
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

func TestDirectFileWriterAt_DrainAfterError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "directwriterat-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer f.Close()

	direct, err := newDirectFileWriterAt(f)
	if err != nil {
		t.Skipf("O_DIRECT not available in this environment: %v", err)
	}
	w := newWriteBehindWriterAt(direct, defaultWriteChunkSizeBytes)

	if err := f.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}

	buf := w.getBuffer()
	if _, err := w.enqueue(buf, w.chunkSize, 0); err != nil {
		t.Fatalf("enqueue unexpected error: %v", err)
	}

	if err := w.drain(); err == nil {
		t.Fatal("drain: expected error from write to closed file, got nil")
	}

	buf2 := w.getBuffer()
	if _, err := w.enqueue(buf2, w.chunkSize, 0); err == nil {
		t.Fatal("enqueue after drain error: expected error to be returned immediately")
	}
}
