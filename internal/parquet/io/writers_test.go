package io

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/scottjab/siem-to-siems/internal/parquet/model"
)

func baseTime() time.Time { return time.Date(2026, 6, 8, 1, 2, 3, 0, time.UTC) }

func structRows(n int) []model.ParquetStructuredNetlogRow {
	out := make([]model.ParquetStructuredNetlogRow, n)
	for i := range out {
		out[i] = model.ParquetStructuredNetlogRow{NodeID: "node", Logtail: &model.LogtailStruct{ID: "x"}}
	}
	return out
}

func cfgRows(n int) []model.ParquetConfigLogRow {
	out := make([]model.ParquetConfigLogRow, n)
	for i := range out {
		out[i] = model.ParquetConfigLogRow{Action: "CREATE", Origin: "o", EventGroupID: "g"}
	}
	return out
}

func numRows[T any](t *testing.T, path string) int64 {
	t.Helper()
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, new(T), 1)
	if err != nil {
		t.Fatalf("reader %s: %v", path, err)
	}
	defer pr.ReadStop()
	return pr.GetNumRows()
}

func glob(t *testing.T, dir, pattern string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestWriteStructuredNetlogParquetBatch(t *testing.T) {
	dir := t.TempDir()
	if err := WriteStructuredNetlogParquetBatch(dir, baseTime(), structRows(3)); err != nil {
		t.Fatal(err)
	}
	files := glob(t, dir, "structured_network_*.parquet")
	if len(files) != 1 {
		t.Fatalf("files = %v", files)
	}
	if got := numRows[model.ParquetStructuredNetlogRow](t, files[0]); got != 3 {
		t.Errorf("rows = %d, want 3", got)
	}

	// Empty batch writes nothing.
	dir2 := t.TempDir()
	if err := WriteStructuredNetlogParquetBatch(dir2, baseTime(), nil); err != nil {
		t.Fatal(err)
	}
	if files := glob(t, dir2, "*.parquet"); len(files) != 0 {
		t.Errorf("empty batch wrote %v", files)
	}
}

func TestWriteConfigParquetBatch(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConfigParquetBatch(dir, baseTime(), cfgRows(2)); err != nil {
		t.Fatal(err)
	}
	files := glob(t, dir, "configuration_logs_*.parquet")
	if len(files) != 1 {
		t.Fatalf("files = %v", files)
	}
	if got := numRows[model.ParquetConfigLogRow](t, files[0]); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
}

func TestWriteNDJSONBatch(t *testing.T) {
	dir := t.TempDir()
	if err := WriteNDJSONBatch(dir, baseTime(), []string{`{"a":1}`, `{"b":2}`}); err != nil {
		t.Fatal(err)
	}
	files := glob(t, dir, "network_*.ndjson")
	if len(files) != 1 {
		t.Fatalf("files = %v", files)
	}
	data, _ := os.ReadFile(files[0])
	if string(data) != "{\"a\":1}\n{\"b\":2}\n" {
		t.Errorf("content = %q", data)
	}

	// Empty batch writes nothing.
	dir2 := t.TempDir()
	if err := WriteNDJSONBatch(dir2, baseTime(), nil); err != nil {
		t.Fatal(err)
	}
	if files := glob(t, dir2, "*.ndjson"); len(files) != 0 {
		t.Errorf("empty batch wrote %v", files)
	}
}

func TestStructuredJournalAndMerge(t *testing.T) {
	dir := t.TempDir()
	if err := WriteStructuredNetlogParquetJournalBatch(dir, baseTime(), structRows(2)); err != nil {
		t.Fatal(err)
	}
	if files := glob(t, dir, "structured_network_journal_*.parquet"); len(files) != 1 {
		t.Fatalf("journal files = %v", files)
	}

	out, n, rows, err := MergeStructuredNetlogJournalFiles(dir, baseTime())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || rows != 2 {
		t.Errorf("merge returned n=%d rows=%d, want 1/2", n, rows)
	}
	if !strings.Contains(filepath.Base(out), "structured_events_") {
		t.Errorf("out = %q", out)
	}
	// Journals are removed after merge.
	if files := glob(t, dir, "structured_network_journal_*.parquet"); len(files) != 0 {
		t.Errorf("journals not removed: %v", files)
	}
	if got := numRows[model.ParquetStructuredNetlogRow](t, out); got != 2 {
		t.Errorf("merged rows = %d, want 2", got)
	}
}

func TestConfigJournalAndMerge(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConfigParquetJournalBatch(dir, baseTime(), cfgRows(3)); err != nil {
		t.Fatal(err)
	}
	out, n, rows, err := MergeConfigJournalFiles(dir, baseTime())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || rows != 3 {
		t.Errorf("merge returned n=%d rows=%d, want 1/3", n, rows)
	}
	if got := numRows[model.ParquetConfigLogRow](t, out); got != 3 {
		t.Errorf("merged rows = %d, want 3", got)
	}
}

func TestMergeJournalsNoFiles(t *testing.T) {
	dir := t.TempDir()
	out, n, rows, err := MergeStructuredNetlogJournalFiles(dir, baseTime())
	if err != nil {
		t.Fatal(err)
	}
	if out != "" || n != 0 || rows != 0 {
		t.Errorf("empty dir merge = %q/%d/%d", out, n, rows)
	}
}

func TestMergeDailyStructured(t *testing.T) {
	dir := t.TempDir()
	t1, t2, t3 := baseTime(), baseTime().Add(time.Second), baseTime().Add(2*time.Second)

	// Produce two structured_events_* files via journal+merge with distinct timestamps.
	if err := WriteStructuredNetlogParquetJournalBatch(dir, t1, structRows(2)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := MergeStructuredNetlogJournalFiles(dir, t1); err != nil {
		t.Fatal(err)
	}
	if err := WriteStructuredNetlogParquetJournalBatch(dir, t2, structRows(3)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := MergeStructuredNetlogJournalFiles(dir, t2); err != nil {
		t.Fatal(err)
	}
	if files := glob(t, dir, "structured_events_*.parquet"); len(files) != 2 {
		t.Fatalf("expected 2 structured_events files, got %v", files)
	}

	out, n, rows, err := MergeDailyStructuredNetlogFiles(dir, t3)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || rows != 5 {
		t.Errorf("daily merge n=%d rows=%d, want 2/5", n, rows)
	}
	if !strings.Contains(filepath.Base(out), "structured_events_daily_") {
		t.Errorf("out = %q", out)
	}
	// The per-rotation inputs were consumed; only the daily file remains.
	if files := glob(t, dir, "structured_events_*.parquet"); len(files) != 1 {
		t.Errorf("expected only the daily file to remain, got %v", files)
	}
	if got := numRows[model.ParquetStructuredNetlogRow](t, out); got != 5 {
		t.Errorf("daily rows = %d, want 5", got)
	}
}

func TestMergeDailyConfig(t *testing.T) {
	dir := t.TempDir()
	t1, t2, t3 := baseTime(), baseTime().Add(time.Second), baseTime().Add(2*time.Second)

	if err := WriteConfigParquetJournalBatch(dir, t1, cfgRows(1)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := MergeConfigJournalFiles(dir, t1); err != nil {
		t.Fatal(err)
	}
	if err := WriteConfigParquetJournalBatch(dir, t2, cfgRows(2)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := MergeConfigJournalFiles(dir, t2); err != nil {
		t.Fatal(err)
	}

	out, n, rows, err := MergeDailyConfigFiles(dir, t3)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || rows != 3 {
		t.Errorf("daily merge n=%d rows=%d, want 2/3", n, rows)
	}
	if got := numRows[model.ParquetConfigLogRow](t, out); got != 3 {
		t.Errorf("daily rows = %d, want 3", got)
	}
}

func TestMergeDailyNoFiles(t *testing.T) {
	dir := t.TempDir()
	out, n, _, err := MergeDailyStructuredNetlogFiles(dir, baseTime())
	if err != nil {
		t.Fatal(err)
	}
	if out != "" || n != 0 {
		t.Errorf("empty daily merge = %q/%d", out, n)
	}
}
