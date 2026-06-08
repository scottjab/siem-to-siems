package destinations

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	pqio "github.com/scottjab/siem-to-siems/internal/parquet/io"
	"github.com/scottjab/siem-to-siems/internal/parquet/model"
)

// ParquetOptions configures the parquet sink. Durations mirror siem-to-parquet's
// rotate/journal/daily_merge cadence and are clamped identically.
type ParquetOptions struct {
	// OutputDir is the directory parquet (and optional NDJSON) files are written to. Created if missing.
	OutputDir string
	// RotateEvery is how often journals are rolled up into final parquet files. Default 5m.
	RotateEvery time.Duration
	// JournalEvery is how often buffered rows are flushed to small journal parquet files.
	// Only used when RotateEvery > JournalEvery. Minimum 1m, clamped to RotateEvery. Default 5m.
	JournalEvery time.Duration
	// DailyMergeEvery is how often per-rotation files are consolidated into daily files.
	// Minimum 1h when >0. Default 24h. Set to 0 to disable.
	DailyMergeEvery time.Duration
	// NDJSONEnabled also writes the raw netlog events as network_<ts>.ndjson files (as siem-to-parquet does).
	NDJSONEnabled bool
}

// buffer is a generic, threadsafe in-memory buffer (ported from siem-to-parquet utils.Buffer).
type buffer[T any] struct {
	mu    sync.Mutex
	items []T
}

func (b *buffer[T]) Add(xs []T) {
	b.mu.Lock()
	b.items = append(b.items, xs...)
	b.mu.Unlock()
}

func (b *buffer[T]) SwapAndClear() []T {
	b.mu.Lock()
	out := b.items
	b.items = nil
	b.mu.Unlock()
	return out
}

// ParquetSink is a Destination that parses incoming Splunk-HEC-style event streams
// into structured-netlog and configuration-audit parquet rows, buffering them and
// rolling them over to parquet files on the same journal/rotate/daily-merge cadence
// as the standalone siem-to-parquet service.
type ParquetSink struct {
	opts ParquetOptions

	structuredBuf buffer[model.ParquetStructuredNetlogRow]
	ndjsonBuf     buffer[string]
	cfgBuf        buffer[model.ParquetConfigLogRow]

	done     chan struct{}
	doneOnce sync.Once
	wg       sync.WaitGroup
}

func NewParquetSink(opts ParquetOptions) (*ParquetSink, error) {
	if opts.OutputDir == "" {
		return nil, errors.New("parquet: output directory required")
	}
	// Defaults and clamps mirror siem-to-parquet's config.Load + app.New.
	if opts.JournalEvery == 0 {
		opts.JournalEvery = 5 * time.Minute
	} else if opts.JournalEvery < time.Minute {
		opts.JournalEvery = time.Minute
	}
	if opts.RotateEvery <= 0 {
		opts.RotateEvery = 5 * time.Minute
	}
	if opts.JournalEvery > opts.RotateEvery {
		opts.JournalEvery = opts.RotateEvery
	}
	if opts.DailyMergeEvery > 0 && opts.DailyMergeEvery < time.Hour {
		opts.DailyMergeEvery = time.Hour
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return nil, err
	}
	s := &ParquetSink{opts: opts, done: make(chan struct{})}
	s.wg.Add(1)
	go s.rotationLoop()
	return s, nil
}

// Send parses one POST body. The body is a Splunk-HEC-style stream of JSON objects
// ({"time":..,"event":{..},"fields":{..}}), identical to siem-to-parquet's ingest path.
// Rows are buffered; the background loop writes them to parquet.
func (s *ParquetSink) Send(ctx context.Context, eventBytes []byte) error {
	dec := json.NewDecoder(bytes.NewReader(eventBytes))
	totalRows, totalCfg := 0, 0
	var ndjsonLines []string
	for {
		var item struct {
			Time   json.RawMessage `json:"time"`
			Event  json.RawMessage `json:"event"`
			Fields json.RawMessage `json:"fields"`
		}
		if err := dec.Decode(&item); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Printf("parquet ingest: decode error: %v", err)
			break
		}

		var ev model.EventJSON
		if err := json.Unmarshal(item.Event, &ev); err == nil {
			hasTraffic := len(ev.VirtualTraffic)+len(ev.SubnetTraffic)+len(ev.ExitTraffic)+len(ev.PhysicalTraffic) > 0
			if hasTraffic {
				s.structuredBuf.Add([]model.ParquetStructuredNetlogRow{model.StructureEvent(ev)})
				ndjsonLines = append(ndjsonLines, string(item.Event))
				totalRows++
				continue
			}
		}

		var fields struct {
			Recorded time.Time `json:"recorded"`
		}
		_ = json.Unmarshal(item.Fields, &fields)
		var cal model.ConfigAuditLog
		if err := json.Unmarshal(item.Event, &cal); err == nil {
			if cal.Action != "" || cal.EventGroupID != "" || cal.Origin != "" || !cal.DeferredAt.IsZero() {
				toJSON := func(v any) string {
					if v == nil {
						return ""
					}
					b, err := json.Marshal(v)
					if err != nil {
						return ""
					}
					return string(b)
				}
				row := model.ParquetConfigLogRow{
					TimeRaw:       strings.TrimSpace(string(item.Time)),
					RecordedMs:    fields.Recorded.UTC().UnixNano() / int64(time.Millisecond),
					DeferredAtMs:  cal.DeferredAt.UTC().UnixNano() / int64(time.Millisecond),
					EventGroupID:  cal.EventGroupID,
					Origin:        cal.Origin,
					Actor:         toJSON(cal.Actor),
					Target:        toJSON(cal.Target),
					Action:        cal.Action,
					OldJSON:       toJSON(cal.Old),
					NewJSON:       toJSON(cal.New),
					ActionDetails: cal.ActionDetails,
					Error:         cal.Error,
					EventJSON:     string(item.Event),
				}
				s.cfgBuf.Add([]model.ParquetConfigLogRow{row})
				totalCfg++
				continue
			}
		}
		log.Printf("parquet ingest: skipping unrecognized item; event=%s", string(item.Event))
	}
	if len(ndjsonLines) > 0 {
		s.ndjsonBuf.Add(ndjsonLines)
	}
	return nil
}

func (s *ParquetSink) Close() error {
	s.doneOnce.Do(func() { close(s.done) })
	s.wg.Wait()

	now := time.Now()
	s.flushAndRollup(now, true)
	if s.opts.DailyMergeEvery > 0 {
		s.performDailyMerge(now)
	}
	return nil
}

func (s *ParquetSink) rotationLoop() {
	defer s.wg.Done()

	rotateTicker := time.NewTicker(s.opts.RotateEvery)
	defer rotateTicker.Stop()
	log.Printf("parquet: starting rotation ticker: interval=%s", s.opts.RotateEvery)

	var journalTicker *time.Ticker
	if s.opts.RotateEvery > s.opts.JournalEvery {
		journalTicker = time.NewTicker(s.opts.JournalEvery)
		defer journalTicker.Stop()
		log.Printf("parquet: starting journal ticker: interval=%s", s.opts.JournalEvery)
	}

	var dailyTicker *time.Ticker
	if s.opts.DailyMergeEvery > 0 {
		dailyTicker = time.NewTicker(s.opts.DailyMergeEvery)
		defer dailyTicker.Stop()
		log.Printf("parquet: starting daily merge ticker: interval=%s", s.opts.DailyMergeEvery)
	}

	for {
		select {
		case <-s.done:
			return
		case now := <-rotateTicker.C:
			s.flushAndRollup(now, true)
		case now := <-tickerC(journalTicker):
			if !now.IsZero() {
				s.flushAndRollup(now, false)
			}
		case now := <-tickerC(dailyTicker):
			if !now.IsZero() {
				s.performDailyMerge(now)
			}
		}
	}
}

// flushAndRollup writes either journal or final rotated parquet, and rolls up journals on rotate.
func (s *ParquetSink) flushAndRollup(now time.Time, isRotate bool) {
	structuredBatch := s.structuredBuf.SwapAndClear()
	ndBatch := s.ndjsonBuf.SwapAndClear()
	cfgBatch := s.cfgBuf.SwapAndClear()
	log.Printf("parquet flush: rotate=%t structured_netlog_rows=%d ndjson_rows=%d config_rows=%d",
		isRotate, len(structuredBatch), len(ndBatch), len(cfgBatch))

	journaling := s.opts.RotateEvery > s.opts.JournalEvery

	// Structured Netlog: journal or final write.
	if journaling {
		if err := pqio.WriteStructuredNetlogParquetJournalBatch(s.opts.OutputDir, now, structuredBatch); err != nil {
			log.Printf("parquet: structured journal write failed: %v", err)
		}
		if isRotate {
			if out, n, rows, err := pqio.MergeStructuredNetlogJournalFiles(s.opts.OutputDir, now); err != nil {
				log.Printf("parquet: merge structured journals failed: %v", err)
			} else if n > 0 {
				log.Printf("parquet: merged %d structured journals (%d rows) into %s", n, rows, out)
			}
		}
	} else {
		if err := pqio.WriteStructuredNetlogParquetBatch(s.opts.OutputDir, now, structuredBatch); err != nil {
			log.Printf("parquet: structured write failed: %v", err)
		}
	}

	// Config logs: journal or final write.
	if journaling {
		if err := pqio.WriteConfigParquetJournalBatch(s.opts.OutputDir, now, cfgBatch); err != nil {
			log.Printf("parquet: config journal write failed: %v", err)
		}
		if isRotate {
			if out, n, rows, err := pqio.MergeConfigJournalFiles(s.opts.OutputDir, now); err != nil {
				log.Printf("parquet: merge config journals failed: %v", err)
			} else if n > 0 {
				log.Printf("parquet: merged %d config journals (%d rows) into %s", n, rows, out)
			}
		}
	} else {
		if err := pqio.WriteConfigParquetBatch(s.opts.OutputDir, now, cfgBatch); err != nil {
			log.Printf("parquet: config write failed: %v", err)
		}
	}

	if s.opts.NDJSONEnabled {
		if err := pqio.WriteNDJSONBatch(s.opts.OutputDir, now, ndBatch); err != nil {
			log.Printf("parquet: ndjson write failed: %v", err)
		}
	}
}

// performDailyMerge consolidates per-rotation parquet files into daily files.
func (s *ParquetSink) performDailyMerge(now time.Time) {
	log.Printf("parquet: performing daily merge at %s", now.Format("2006-01-02 15:04:05"))
	if out, n, rows, err := pqio.MergeDailyStructuredNetlogFiles(s.opts.OutputDir, now); err != nil {
		log.Printf("parquet: daily merge structured files failed: %v", err)
	} else if n > 0 {
		log.Printf("parquet: daily merged %d structured files (%d rows) into %s", n, rows, out)
	}
	if out, n, rows, err := pqio.MergeDailyConfigFiles(s.opts.OutputDir, now); err != nil {
		log.Printf("parquet: daily merge config files failed: %v", err)
	} else if n > 0 {
		log.Printf("parquet: daily merged %d config files (%d rows) into %s", n, rows, out)
	}
}

// tickerC returns a nil channel for a nil ticker so it never fires in select.
func tickerC(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
