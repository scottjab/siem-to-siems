package destinations

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNDJSONWriterRequiresDirectory(t *testing.T) {
	if _, err := NewNDJSONWriter("", time.Hour); err == nil {
		t.Error("expected error for empty directory")
	}
}

func TestNDJSONWriterWritesLines(t *testing.T) {
	dir := t.TempDir()
	w, err := NewNDJSONWriter(dir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Send(context.Background(), []byte(`{"a":1}`))
	_ = w.Send(context.Background(), []byte(`{"b":2}`))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "events-*.ndjson"))
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %v", files)
	}
	data, _ := os.ReadFile(files[0])
	if want := "{\"a\":1}\n{\"b\":2}\n"; string(data) != want {
		t.Errorf("content = %q, want %q", data, want)
	}
}

func TestNDJSONWriterIdleDoesNotCreateEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	w, err := NewNDJSONWriter(dir, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Several rotation ticks elapse with no events written.
	time.Sleep(220 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "events-*.ndjson"))
	if len(files) != 1 {
		t.Errorf("idle writer should keep a single (empty) file, got %d", len(files))
	}
}

func TestNDJSONWriterRotatesWhenDirty(t *testing.T) {
	dir := t.TempDir()
	// >1s so the second-granularity filenames differ between rotations.
	w, err := NewNDJSONWriter(dir, 1100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Send(context.Background(), []byte("a"))
	time.Sleep(1300 * time.Millisecond) // tick fires; file is dirty so it rotates
	_ = w.Send(context.Background(), []byte("b"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "events-*.ndjson"))
	if len(files) < 2 {
		t.Fatalf("expected rotation into >=2 files, got %d", len(files))
	}
	total := 0
	for _, f := range files {
		data, _ := os.ReadFile(f)
		total += strings.Count(string(data), "\n")
	}
	if total != 2 {
		t.Errorf("total lines across files = %d, want 2", total)
	}
}

func TestNDJSONWriterDefaultsZeroRotation(t *testing.T) {
	// A zero rotation must not panic (NewTicker(0) would); it defaults to 1h.
	dir := t.TempDir()
	w, err := NewNDJSONWriter(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Send(context.Background(), []byte("x"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}
