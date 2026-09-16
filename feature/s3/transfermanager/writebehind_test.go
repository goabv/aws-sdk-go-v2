package transfermanager

import (
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"
)

type alignmentCheckingWriterAt struct {
	alignment int64
}

func (w *alignmentCheckingWriterAt) writeBufferAlignment() int64 {
	return w.alignment
}

func (w *alignmentCheckingWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if uintptr(unsafe.Pointer(unsafe.SliceData(p)))%uintptr(w.alignment) != 0 {
		return 0, fmt.Errorf("buffer is not aligned to %d", w.alignment)
	}
	if off%w.alignment != 0 {
		return 0, fmt.Errorf("offset is not aligned to %d", w.alignment)
	}
	return len(p), nil
}

type scalingBlockingWriterAt struct {
	release <-chan struct{}
}

func (w *scalingBlockingWriterAt) WriteAt(p []byte, _ int64) (int, error) {
	<-w.release
	return len(p), nil
}

func TestWriteBehindUsesDefaultChunkSize(t *testing.T) {
	writer := newWriteBehindWriterAt(&alignmentCheckingWriterAt{alignment: 1}, 0)
	if got, want := writer.chunkSize, int64(32*1024*1024); got != want {
		t.Fatalf("chunk size = %d, want %d", got, want)
	}
	if err := writer.drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
}

func TestWriteBehindUsesDestinationBufferAlignment(t *testing.T) {
	destination := &alignmentCheckingWriterAt{alignment: directIOAlignment}
	writer := newWriteBehindWriterAt(destination, directIOAlignment)

	if _, err := writer.WriteAt(make([]byte, directIOAlignment), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := writer.drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
}

func TestWriteBehindStartsInitialWorkers(t *testing.T) {
	writer := newWriteBehindWriterAt(&alignmentCheckingWriterAt{alignment: 1}, 1)
	if got := writer.workerCount.Load(); got != writeBehindInitialWorkers {
		t.Fatalf("worker count = %d, want %d", got, writeBehindInitialWorkers)
	}

	if err := writer.drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := writer.workerCount.Load(); got != 0 {
		t.Fatalf("worker count after drain = %d, want 0", got)
	}
}

func TestWriteBehindScalesWorkersWhileQueueRemainsFull(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrites := func() { releaseOnce.Do(func() { close(release) }) }

	writer := newWriteBehindWriterAtWithConfig(&scalingBlockingWriterAt{release: release}, 1, writeBehindWorkerConfig{
		queueDepth:         4,
		initialWorkers:     2,
		workerBatch:        2,
		maxWorkers:         6,
		scaleCheckInterval: time.Millisecond,
		saturationDuration: 3 * time.Millisecond,
	})
	t.Cleanup(func() {
		releaseWrites()
		_ = writer.drain()
	})

	enqueued := make(chan error, 1)
	go func() {
		for i := 0; i < 32; i++ {
			buf := writer.getBuffer()
			if _, err := writer.enqueue(buf, 1, int64(i)); err != nil {
				enqueued <- err
				return
			}
		}
		enqueued <- nil
	}()

	waitForWorkerCount(t, writer, 6)
	time.Sleep(10 * time.Millisecond)
	if got := writer.workerCount.Load(); got != 6 {
		t.Fatalf("worker count = %d, want capped count 6", got)
	}

	releaseWrites()
	select {
	case err := <-enqueued:
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("enqueue did not finish after writes were released")
	}

	if err := writer.drain(); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if got := writer.workerCount.Load(); got != 0 {
		t.Fatalf("worker count after drain = %d, want 0", got)
	}
}

func waitForWorkerCount(t *testing.T, writer *writeBehindWriterAt, want int64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if writer.workerCount.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("worker count = %d, want %d", writer.workerCount.Load(), want)
}
