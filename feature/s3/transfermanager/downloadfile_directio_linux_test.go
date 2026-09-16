//go:build linux

package transfermanager

import (
	"bytes"
	"os"
	"testing"
)

func TestDirectFileWriterAtPadsPhysicalWrite(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "direct-write-*.bin")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer f.Close()

	buf := newWriteBuffer(directIOAlignment, directIOAlignment)
	want := []byte("short direct write")
	copy(buf, want)
	writer := &directFileWriterAt{fd: int(f.Fd())}
	n, err := writer.WriteAt(buf[:len(want)], 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != len(want) {
		t.Fatalf("WriteAt count = %d, want %d", n, len(want))
	}

	got, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(got) != directIOAlignment {
		t.Fatalf("physical file size = %d, want %d", len(got), directIOAlignment)
	}
	if !bytes.Equal(got[:len(want)], want) {
		t.Fatalf("written prefix = %q, want %q", got[:len(want)], want)
	}
	if !bytes.Equal(got[len(want):], make([]byte, directIOAlignment-len(want))) {
		t.Fatal("physical write padding is not zero-filled")
	}
}

func TestDirectFileWriterAtRejectsUnalignedOffset(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "direct-write-*.bin")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer f.Close()

	writer := &directFileWriterAt{fd: int(f.Fd())}
	buf := newWriteBuffer(directIOAlignment, directIOAlignment)
	if _, err := writer.WriteAt(buf, 1); err == nil {
		t.Fatal("WriteAt with unaligned offset returned no error")
	}
}

func TestDirectFileWriterAtPreallocates(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "direct-preallocate-*.bin")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer f.Close()

	writer := &directFileWriterAt{fd: int(f.Fd())}
	want := int64(2 * directIOAlignment)
	if err := writer.preallocate(want); err != nil {
		t.Fatalf("preallocate: %v", err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != want {
		t.Fatalf("file size = %d, want %d", info.Size(), want)
	}
}
