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
//
// Rotation is driven by a background ticker so files roll over on schedule even when
// no events are arriving. To avoid producing a flood of empty files during idle periods,
// a tick only rotates when at least one event has been written to the current file.
type NDJSONWriter struct {
	directory string
	rotation  time.Duration

	mu    sync.Mutex
	file  *os.File
	dirty bool // true if events have been written to the current file since it was opened

	done     chan struct{}
	doneOnce sync.Once
	wg       sync.WaitGroup
}

func NewNDJSONWriter(directory string, rotation time.Duration) (*NDJSONWriter, error) {
	if directory == "" {
		return nil, fmt.Errorf("directory required")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	if rotation <= 0 {
		rotation = time.Hour
	}
	w := &NDJSONWriter{
		directory: directory,
		rotation:  rotation,
		done:      make(chan struct{}),
	}
	if err := w.rotateLocked(time.Now()); err != nil {
		return nil, err
	}
	w.wg.Add(1)
	go w.rotateLoop()
	return w, nil
}

func (w *NDJSONWriter) Send(ctx context.Context, eventBytes []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		if err := w.rotateLocked(time.Now()); err != nil {
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
	w.dirty = true
	// Best-effort flush. Not fsync on every line for performance.
	return nil
}

// rotateLoop rotates the file on the configured interval, regardless of event arrival,
// so a quiet sink still rolls over on time. Empty files are skipped via the dirty flag.
func (w *NDJSONWriter) rotateLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.rotation)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case now := <-ticker.C:
			w.mu.Lock()
			if w.dirty {
				_ = w.rotateLocked(now)
			}
			w.mu.Unlock()
		}
	}
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
	w.dirty = false
	return nil
}

func (w *NDJSONWriter) Close() error {
	w.doneOnce.Do(func() { close(w.done) })
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file != nil {
		_ = w.file.Sync()
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}
