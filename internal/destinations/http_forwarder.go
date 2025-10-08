package destinations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type HTTPForwarderOptions struct {
	// Optional: directory to journal failed events. Defaults to os.TempDir()+"/siem-to-siems-http-journal".
	JournalDirectory string
	// Maximum backoff between retries
	MaxBackoff time.Duration
	// Initial backoff duration
	InitialBackoff time.Duration
	// HTTP client override (nil uses default)
	Client *http.Client
	// Optional bearer token for Authorization header
	Token string
}

// HTTPForwarder forwards events to an HTTP endpoint with exponential backoff on failures.
// It ensures in-order delivery by serializing sends via an internal queue.
// On failure, it journals the event and retries the SAME event with backoff before
// moving on to subsequent events.
type HTTPForwarder struct {
	url    string
	client *http.Client

	journalDir  string
	bearerToken string

	// in-order delivery queue
	queueCh chan queuedItem
	closed  chan struct{}

	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// queuedItem represents a single event queued for delivery.
// journaled indicates whether this event has already been written to the durable journal
// to avoid duplicate lines on subsequent retries.
type queuedItem struct {
	data      []byte
	journaled bool
}

func NewHTTPForwarder(url string, opts HTTPForwarderOptions) (*HTTPForwarder, error) {
	if url == "" {
		return nil, fmt.Errorf("url required")
	}
	jf := opts.JournalDirectory
	if jf == "" {
		jf = filepath.Join(os.TempDir(), "siem-to-siems-http-journal")
	}
	if err := os.MkdirAll(jf, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir journal: %w", err)
	}
	c := opts.Client
	if c == nil {
		c = &http.Client{Timeout: 30 * time.Second}
	}
	initial := opts.InitialBackoff
	if initial <= 0 {
		initial = 1 * time.Second
	}
	max := opts.MaxBackoff
	if max <= 0 {
		max = 1 * time.Minute
	}
	h := &HTTPForwarder{
		url:            url,
		client:         c,
		journalDir:     jf,
		bearerToken:    opts.Token,
		queueCh:        make(chan queuedItem, 1024),
		closed:         make(chan struct{}),
		initialBackoff: initial,
		maxBackoff:     max,
	}
	go h.deliveryLoop()
	// Load any pre-existing journal on startup
	go h.requeueJournal()
	return h, nil
}

func (h *HTTPForwarder) Send(ctx context.Context, eventBytes []byte) error {
	// Enqueue for in-order delivery. Best-effort non-blocking; if full, journal and try once more.
	item := queuedItem{data: append([]byte(nil), eventBytes...), journaled: false}
	select {
	case h.queueCh <- item:
		return nil
	default:
		// queue full: write to journal to avoid loss and try once more
		_ = h.journal(eventBytes)
		item.journaled = true
		select {
		case h.queueCh <- item:
			return nil
		default:
			// If still full, report retryable error; the event is at least journaled.
			return RetryableError{Err: fmt.Errorf("queue full")}
		}
	}
}

func (h *HTTPForwarder) Close() error {
	close(h.closed)
	return nil
}

func (h *HTTPForwarder) deliveryLoop() {
	backoff := h.initialBackoff
	var current *queuedItem
	for {
		if current == nil {
			select {
			case <-h.closed:
				return
			case it := <-h.queueCh:
				// capture new item
				current = &queuedItem{data: it.data, journaled: it.journaled}
			}
		}
		// Try to deliver current
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := h.sendOnce(ctx, current.data)
		cancel()
		if err != nil {
			var r RetryableError
			if errors.As(err, &r) {
				// On first failure for this item, journal it
				if !current.journaled {
					_ = h.journal(current.data)
					current.journaled = true
				}
				time.Sleep(backoff)
				backoff *= 2
				if backoff > h.maxBackoff {
					backoff = h.maxBackoff
				}
				// retry the SAME current item (do not pull from queue to preserve order)
				continue
			}
			// Non-retryable: drop and move on
			backoff = h.initialBackoff
			current = nil
			continue
		}
		// success
		backoff = h.initialBackoff
		current = nil
	}
}

func (h *HTTPForwarder) sendOnce(ctx context.Context, b []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if h.bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.bearerToken)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return RetryableError{Err: err}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return RetryableError{Err: fmt.Errorf("downstream status %d", resp.StatusCode)}
	}
	return nil
}

func (h *HTTPForwarder) journal(b []byte) error {
	// simple append-only journal file per day
	day := time.Now().UTC().Format("20060102")
	path := filepath.Join(h.journalDir, fmt.Sprintf("failed-%s.ndjson", day))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return err
	}
	_, err = f.Write([]byte("\n"))
	return err
}

func (h *HTTPForwarder) requeueJournal() {
	entries, err := os.ReadDir(h.journalDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// best-effort: read whole file and enqueue lines
		path := filepath.Join(h.journalDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// split by lines
		start := 0
		for i := 0; i <= len(data); i++ {
			if i == len(data) || data[i] == '\n' {
				if i > start {
					// Re-enqueue as queuedItem without duplicating journal lines again.
					select {
					case h.queueCh <- queuedItem{data: append([]byte(nil), data[start:i]...), journaled: true}:
						// enqueued
					default:
						// queue full; drop on floor since journal retains data
					}
				}
				start = i + 1
			}
		}
		// do not delete journal; leave as durable log
	}
}
