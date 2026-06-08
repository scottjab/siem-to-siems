// Package io contains the parquet write/journal/merge helpers for the parquet
// destination. It is a port of the runtime write path from siem-to-parquet: same
// file names, same ZSTD compression, same journal/rotate/daily-merge semantics, and
// the same atomic-rename-on-merge behavior.
package io

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/scottjab/siem-to-siems/internal/parquet/model"
)

// writeParquetJournalBatch is a generic helper to write a small parquet journal file
// for later merge. Skips when rows is empty. The file name is `${prefix}_${ts}.parquet`.
func writeParquetJournalBatch[T any](outputDir, prefix string, now time.Time, rows []T, writerParallel int) error {
	if len(rows) == 0 {
		return nil
	}
	file := filepath.Join(outputDir, fmt.Sprintf("%s_%s.parquet", prefix, now.Format("20060102_150405")))
	fw, err := local.NewLocalFileWriter(file)
	if err != nil {
		return fmt.Errorf("file writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(T), int64(writerParallel))
	if err != nil {
		return fmt.Errorf("parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	for _, row := range rows {
		if err := pw.Write(row); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("parquet write: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("parquet close: %w", err)
	}
	return nil
}

// mergeParquetJournalFiles merges parquet journal files matching journalGlob into a single
// final parquet file using a temporary output file and atomic rename.
// Returns final path, number of merged input files, total merged row count, and error.
func mergeParquetJournalFiles[T any](outputDir, journalGlob, finalPrefix string, writerParallel, readerParallel int, now time.Time) (string, int, int, error) {
	matches, err := filepath.Glob(filepath.Join(outputDir, journalGlob))
	if err != nil {
		return "", 0, 0, fmt.Errorf("glob journals: %w", err)
	}
	if len(matches) == 0 {
		return "", 0, 0, nil
	}

	finalOut := filepath.Join(outputDir, fmt.Sprintf("%s_%s.parquet", finalPrefix, now.Format("20060102_150405")))
	rows, err := countParquetRows[T](matches, readerParallel)
	if err != nil {
		return "", 0, 0, err
	}
	if err := mergeAsType[T](matches, finalOut, writerParallel, readerParallel); err != nil {
		return "", 0, 0, err
	}
	for _, jf := range matches {
		_ = os.Remove(jf)
	}
	return finalOut, len(matches), rows, nil
}

func mergeAsType[T any](inputs []string, outPath string, writerParallel, readerParallel int) error {
	tmpOut := outPath + ".tmp"
	fw, err := local.NewLocalFileWriter(tmpOut)
	if err != nil {
		return fmt.Errorf("open tmp writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(T), int64(writerParallel))
	if err != nil {
		return fmt.Errorf("parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD

	for _, jf := range inputs {
		fr, err := local.NewLocalFileReader(jf)
		if err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("open reader: %w", err)
		}
		pr, err := reader.NewParquetReader(fr, new(T), int64(readerParallel))
		if err != nil {
			fr.Close()
			_ = pw.WriteStop()
			return fmt.Errorf("parquet reader: %w", err)
		}
		total := int(pr.GetNumRows())
		const chunk = 4096
		for readRows := 0; readRows < total; {
			n := min(total-readRows, chunk)
			recs, err := pr.ReadByNumber(n)
			if err != nil {
				pr.ReadStop()
				fr.Close()
				_ = pw.WriteStop()
				return fmt.Errorf("parquet read: %w", err)
			}
			for _, r := range recs {
				row, ok := r.(T)
				if !ok {
					pr.ReadStop()
					fr.Close()
					_ = pw.WriteStop()
					return fmt.Errorf("unexpected row type %T", r)
				}
				if err := pw.Write(row); err != nil {
					pr.ReadStop()
					fr.Close()
					_ = pw.WriteStop()
					return fmt.Errorf("parquet write merged: %w", err)
				}
			}
			readRows += len(recs)
		}
		pr.ReadStop()
		fr.Close()
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("close merged parquet: %w", err)
	}
	if err := os.Rename(tmpOut, outPath); err != nil {
		_ = os.Remove(tmpOut)
		return fmt.Errorf("rename merged parquet: %w", err)
	}
	return nil
}

// countParquetRows sums the number of rows across input parquet files for row type T.
func countParquetRows[T any](inputs []string, readerParallel int) (int, error) {
	totalRows := 0
	for _, jf := range inputs {
		fr, err := local.NewLocalFileReader(jf)
		if err != nil {
			return 0, fmt.Errorf("open reader: %w", err)
		}
		pr, err := reader.NewParquetReader(fr, new(T), int64(readerParallel))
		if err != nil {
			fr.Close()
			return 0, fmt.Errorf("parquet reader: %w", err)
		}
		totalRows += int(pr.GetNumRows())
		pr.ReadStop()
		fr.Close()
	}
	return totalRows, nil
}

func WriteStructuredNetlogParquetBatch(outputDir string, now time.Time, in []model.ParquetStructuredNetlogRow) error {
	if len(in) == 0 {
		return nil
	}
	file := filepath.Join(outputDir, fmt.Sprintf("structured_network_%s.parquet", now.Format("20060102_150405")))
	fw, err := local.NewLocalFileWriter(file)
	if err != nil {
		return fmt.Errorf("file writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(model.ParquetStructuredNetlogRow), 4)
	if err != nil {
		return fmt.Errorf("parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	for _, row := range in {
		if err := pw.Write(row); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("parquet write: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("parquet close: %w", err)
	}
	return nil
}

func WriteConfigParquetBatch(outputDir string, now time.Time, in []model.ParquetConfigLogRow) error {
	if len(in) == 0 {
		return nil
	}
	file := filepath.Join(outputDir, fmt.Sprintf("configuration_logs_%s.parquet", now.Format("20060102_150405")))
	fw, err := local.NewLocalFileWriter(file)
	if err != nil {
		return fmt.Errorf("file writer: %w", err)
	}
	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(model.ParquetConfigLogRow), 2)
	if err != nil {
		return fmt.Errorf("parquet writer: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	for _, row := range in {
		if err := pw.Write(row); err != nil {
			_ = pw.WriteStop()
			return fmt.Errorf("parquet write: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("parquet close: %w", err)
	}
	return nil
}

func WriteNDJSONBatch(outputDir string, now time.Time, lines []string) error {
	if len(lines) == 0 {
		return nil
	}
	file := filepath.Join(outputDir, fmt.Sprintf("network_%s.ndjson", now.Format("20060102_150405")))
	f, err := os.Create(file)
	if err != nil {
		return fmt.Errorf("ndjson create: %w", err)
	}
	defer f.Close()
	for _, line := range lines {
		if _, err := f.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("ndjson write: %w", err)
		}
	}
	return nil
}

// WriteStructuredNetlogParquetJournalBatch writes a small structured parquet file for later merge.
func WriteStructuredNetlogParquetJournalBatch(outputDir string, now time.Time, in []model.ParquetStructuredNetlogRow) error {
	return writeParquetJournalBatch[model.ParquetStructuredNetlogRow](outputDir, "structured_network_journal", now, in, 2)
}

// WriteConfigParquetJournalBatch writes a small configuration logs parquet file for later merge.
func WriteConfigParquetJournalBatch(outputDir string, now time.Time, in []model.ParquetConfigLogRow) error {
	return writeParquetJournalBatch[model.ParquetConfigLogRow](outputDir, "configuration_logs_journal", now, in, 2)
}

// MergeStructuredNetlogJournalFiles merges all structured journal parquet files in outputDir into a single
// rolled-up parquet file named like structured_events_YYYYMMDD_HHMMSS.parquet using an atomic rename.
func MergeStructuredNetlogJournalFiles(outputDir string, now time.Time) (string, int, int, error) {
	return mergeParquetJournalFiles[model.ParquetStructuredNetlogRow](outputDir,
		"structured_network_journal_*.parquet",
		"structured_events",
		4, 2, now,
	)
}

// MergeConfigJournalFiles merges all configuration log journal parquet files in outputDir into a single
// rolled-up parquet file named like configuration_logs_YYYYMMDD_HHMMSS.parquet using an atomic rename.
func MergeConfigJournalFiles(outputDir string, now time.Time) (string, int, int, error) {
	return mergeParquetJournalFiles[model.ParquetConfigLogRow](outputDir,
		"configuration_logs_journal_*.parquet",
		"configuration_logs",
		2, 1, now,
	)
}

// MergeDailyStructuredNetlogFiles merges all structured network parquet files (created from journals)
// into a single daily consolidated file. Returns final path, merged file count, total rows, and error.
func MergeDailyStructuredNetlogFiles(outputDir string, now time.Time) (string, int, int, error) {
	allMatches, err := filepath.Glob(filepath.Join(outputDir, "structured_events_*.parquet"))
	if err != nil {
		return "", 0, 0, fmt.Errorf("glob daily structured network files: %w", err)
	}

	// Filter out daily files (structured_events_daily_*) and journal files (structured_network_journal_*)
	var matches []string
	for _, match := range allMatches {
		basename := filepath.Base(match)
		if !strings.Contains(basename, "_daily_") && !strings.Contains(basename, "_journal_") {
			matches = append(matches, match)
		}
	}

	if len(matches) == 0 {
		return "", 0, 0, nil
	}

	finalOut := filepath.Join(outputDir, fmt.Sprintf("structured_events_daily_%s.parquet", now.Format("20060102_150405")))
	rows, err := countParquetRows[model.ParquetStructuredNetlogRow](matches, 2)
	if err != nil {
		return "", 0, 0, err
	}
	if err := mergeAsType[model.ParquetStructuredNetlogRow](matches, finalOut, 4, 2); err != nil {
		return "", 0, 0, err
	}
	for _, f := range matches {
		_ = os.Remove(f)
	}
	return finalOut, len(matches), rows, nil
}

// MergeDailyConfigFiles merges all configuration log parquet files (created from journals)
// into a single daily consolidated file. Returns final path, merged file count, total rows, and error.
func MergeDailyConfigFiles(outputDir string, now time.Time) (string, int, int, error) {
	allMatches, err := filepath.Glob(filepath.Join(outputDir, "configuration_logs_*.parquet"))
	if err != nil {
		return "", 0, 0, fmt.Errorf("glob daily config files: %w", err)
	}

	// Filter out daily files (configuration_logs_daily_*) and journal files (configuration_logs_journal_*)
	var matches []string
	for _, match := range allMatches {
		basename := filepath.Base(match)
		if !strings.Contains(basename, "_daily_") && !strings.Contains(basename, "_journal_") {
			matches = append(matches, match)
		}
	}

	if len(matches) == 0 {
		return "", 0, 0, nil
	}

	finalOut := filepath.Join(outputDir, fmt.Sprintf("configuration_logs_daily_%s.parquet", now.Format("20060102_150405")))
	rows, err := countParquetRows[model.ParquetConfigLogRow](matches, 1)
	if err != nil {
		return "", 0, 0, err
	}
	if err := mergeAsType[model.ParquetConfigLogRow](matches, finalOut, 2, 1); err != nil {
		return "", 0, 0, err
	}
	for _, f := range matches {
		_ = os.Remove(f)
	}
	return finalOut, len(matches), rows, nil
}
