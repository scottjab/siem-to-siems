package destinations

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// NDJSONWriter writes each event as a single line of raw bytes with a trailing newline.
// It rotates files on a fixed interval and ensures flush on Close.
type NDJSONWriter struct {
	directory string
	rotation  time.Duration

	mu         sync.Mutex
	file       *os.File
	nextRotate time.Time
}

func NewNDJSONWriter(directory string, rotation time.Duration) (*NDJSONWriter, error) {
	if directory == "" {
		return nil, fmt.Errorf("directory required")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	w := &NDJSONWriter{directory: directory, rotation: rotation}
	if err := w.rotateLocked(time.Now()); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *NDJSONWriter) Send(ctx context.Context, eventBytes []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	if now.After(w.nextRotate) {
		if err := w.rotateLocked(now); err != nil {
			return err
		}
	}
	if w.file == nil {
		if err := w.rotateLocked(now); err != nil {
			return err
		}
	}
	// We ignore ctx for file writes; local filesystem writes should be quick.
	if _, err := w.file.Write(eventBytes); err != nil {
		return err
	}
	if _, err := w.file.Write([]byte("\n")); err != nil {
		return err
	}
	// Best-effort flush. Not fsync on every line for performance.
	return nil
}

func (w *NDJSONWriter) rotateLocked(now time.Time) error {
	if w.file != nil {
		_ = w.file.Sync()
		_ = w.file.Close()
		w.file = nil
	}
	ts := now.UTC().Format("20060102-150405")
	name := fmt.Sprintf("events-%s.ndjson", ts)
	path := filepath.Join(w.directory, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.file = f
	if w.rotation <= 0 {
		w.rotation = time.Hour
	}
	w.nextRotate = now.Add(w.rotation)
	return nil
}

func (w *NDJSONWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		_ = w.file.Sync()
		return w.file.Close()
	}
	return nil
}
