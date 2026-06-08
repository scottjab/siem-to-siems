package destinations

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/scottjab/siem-to-siems/internal/parquet/model"
)

// A Splunk-HEC-style line carrying a Tailscale netlog event with traffic.
const sampleNetlog = `{"time":"1700000000","event":{"nodeId":"n123","start":"2026-06-08T00:00:00Z","end":"2026-06-08T00:01:00Z","virtualTraffic":[{"proto":6,"src":"100.64.0.1:1234","dst":"100.64.0.2:443","txPkts":10,"txBytes":1000,"rxPkts":8,"rxBytes":800}]},"fields":{"recorded":"2026-06-08T00:01:01Z"}}`

// A Splunk-HEC-style line carrying a Tailscale configuration-audit event.
const sampleConfigLog = `{"time":"1700000001","event":{"eventGroupID":"grp-1","origin":"admin-console","action":"CREATE","actor":{"id":"u1","login":"alice@example.com"},"target":{"id":"acl-policy"},"new":{"hujson":"{}"}},"fields":{"recorded":"2026-06-08T00:02:01Z"}}`

func TestParquetSinkWritesReadableParquet(t *testing.T) {
	dir := t.TempDir()
	// Journaling path: rotate > journal, so Close() writes a journal then merges it
	// into structured_events_<ts>.parquet.
	s, err := NewParquetSink(ParquetOptions{
		OutputDir:       dir,
		RotateEvery:     10 * time.Minute,
		JournalEvery:    time.Minute,
		DailyMergeEvery: 0,
	})
	if err != nil {
		t.Fatalf("NewParquetSink: %v", err)
	}

	if err := s.Send(context.Background(), []byte(sampleNetlog)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Close stops the loop and performs the final flush + journal rollup.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "structured_events_*.parquet"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 rolled-up parquet file, got %d: %v", len(matches), matches)
	}

	fr, err := local.NewLocalFileReader(matches[0])
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, new(model.ParquetStructuredNetlogRow), 2)
	if err != nil {
		t.Fatalf("parquet reader: %v", err)
	}
	defer pr.ReadStop()

	if got := pr.GetNumRows(); got != 1 {
		t.Fatalf("expected 1 row, got %d", got)
	}
	rows := make([]model.ParquetStructuredNetlogRow, 1)
	if err := pr.Read(&rows); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	if rows[0].NodeID != "n123" {
		t.Fatalf("nodeId = %q, want n123", rows[0].NodeID)
	}
	if rows[0].VirtualTraffic == nil || len(rows[0].VirtualTraffic.Conns) != 1 {
		t.Fatalf("expected 1 virtual-traffic conn, got %+v", rows[0].VirtualTraffic)
	}
}

func TestParquetSinkWritesConfigLogs(t *testing.T) {
	dir := t.TempDir()
	// Journaling path: rotate > journal, so Close() writes a config journal then
	// merges it into configuration_logs_<ts>.parquet.
	s, err := NewParquetSink(ParquetOptions{
		OutputDir:       dir,
		RotateEvery:     10 * time.Minute,
		JournalEvery:    time.Minute,
		DailyMergeEvery: 0,
	})
	if err != nil {
		t.Fatalf("NewParquetSink: %v", err)
	}

	if err := s.Send(context.Background(), []byte(sampleConfigLog)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "configuration_logs_*.parquet"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	// Exclude any journal files; the rolled-up file is configuration_logs_<ts>.parquet.
	var finals []string
	for _, m := range matches {
		if !strings.Contains(filepath.Base(m), "_journal_") {
			finals = append(finals, m)
		}
	}
	if len(finals) != 1 {
		t.Fatalf("expected 1 rolled-up config parquet file, got %d: %v", len(finals), finals)
	}

	fr, err := local.NewLocalFileReader(finals[0])
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, new(model.ParquetConfigLogRow), 1)
	if err != nil {
		t.Fatalf("parquet reader: %v", err)
	}
	defer pr.ReadStop()

	if got := pr.GetNumRows(); got != 1 {
		t.Fatalf("expected 1 row, got %d", got)
	}
	rows := make([]model.ParquetConfigLogRow, 1)
	if err := pr.Read(&rows); err != nil {
		t.Fatalf("read rows: %v", err)
	}
	r := rows[0]
	if r.EventGroupID != "grp-1" || r.Origin != "admin-console" || r.Action != "CREATE" {
		t.Fatalf("unexpected config row: group=%q origin=%q action=%q", r.EventGroupID, r.Origin, r.Action)
	}
	// Nested actor/target/new are stringified JSON; ensure they round-tripped.
	if !strings.Contains(r.Actor, "alice@example.com") {
		t.Fatalf("actor JSON missing login: %q", r.Actor)
	}
	if r.RecordedMs == 0 {
		t.Fatalf("recorded_ms not populated")
	}
}

// Non-journaling path: rotate == journal, so Close() writes the final
// structured_network_<ts>.parquet directly (no journal rollup).
func TestParquetSinkNonJournalingDirectWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := NewParquetSink(ParquetOptions{
		OutputDir:       dir,
		RotateEvery:     5 * time.Minute,
		JournalEvery:    5 * time.Minute,
		DailyMergeEvery: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), []byte(sampleNetlog)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if files, _ := filepath.Glob(filepath.Join(dir, "structured_network_*.parquet")); len(files) != 1 {
		t.Errorf("expected 1 structured_network file, got %v", files)
	}
	// No journal rollup files in this mode.
	if files, _ := filepath.Glob(filepath.Join(dir, "structured_events_*.parquet")); len(files) != 0 {
		t.Errorf("did not expect rolled-up files, got %v", files)
	}
}

// Daily merge: Close() rolls up journals into structured_events_* then consolidates
// them into structured_events_daily_*.
func TestParquetSinkDailyMerge(t *testing.T) {
	dir := t.TempDir()
	s, err := NewParquetSink(ParquetOptions{
		OutputDir:       dir,
		RotateEvery:     10 * time.Minute,
		JournalEvery:    time.Minute,
		DailyMergeEvery: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), []byte(sampleNetlog)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	daily, _ := filepath.Glob(filepath.Join(dir, "structured_events_daily_*.parquet"))
	if len(daily) != 1 {
		t.Fatalf("expected 1 daily file, got %v", daily)
	}
	// The per-rotation file was consumed by the daily merge.
	all, _ := filepath.Glob(filepath.Join(dir, "structured_events_*.parquet"))
	if len(all) != 1 {
		t.Errorf("only the daily file should remain, got %v", all)
	}
}

// NDJSONEnabled also writes the raw events as network_<ts>.ndjson.
func TestParquetSinkNDJSONFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := NewParquetSink(ParquetOptions{
		OutputDir:       dir,
		RotateEvery:     5 * time.Minute,
		JournalEvery:    5 * time.Minute,
		DailyMergeEvery: 0,
		NDJSONEnabled:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), []byte(sampleNetlog)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "network_*.ndjson"))
	if len(files) != 1 {
		t.Fatalf("expected 1 ndjson file, got %v", files)
	}
	data, _ := os.ReadFile(files[0])
	if !strings.Contains(string(data), `"nodeId":"n123"`) {
		t.Errorf("ndjson content missing event: %q", data)
	}
}

func TestParquetSinkRequiresDirectory(t *testing.T) {
	if _, err := NewParquetSink(ParquetOptions{}); err == nil {
		t.Error("expected error for empty output directory")
	}
}

// Unrecognized items are skipped without error and produce no rows/files.
func TestParquetSinkSkipsUnrecognized(t *testing.T) {
	dir := t.TempDir()
	s, err := NewParquetSink(ParquetOptions{
		OutputDir:       dir,
		RotateEvery:     5 * time.Minute,
		JournalEvery:    5 * time.Minute,
		DailyMergeEvery: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(context.Background(), []byte(`{"time":"1","event":{"unrelated":true},"fields":{}}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "*.parquet")); len(files) != 0 {
		t.Errorf("unrecognized item should produce no parquet files, got %v", files)
	}
}
