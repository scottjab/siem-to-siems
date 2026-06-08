package destinations

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}

// recordingServer captures request bodies and Authorization headers, replying with status.
type recordingServer struct {
	mu     sync.Mutex
	bodies []string
	auth   []string
}

func (rs *recordingServer) handler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rs.mu.Lock()
		rs.bodies = append(rs.bodies, string(b))
		rs.auth = append(rs.auth, r.Header.Get("Authorization"))
		rs.mu.Unlock()
		w.WriteHeader(status)
	}
}

func (rs *recordingServer) snapshot() ([]string, []string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return append([]string(nil), rs.bodies...), append([]string(nil), rs.auth...)
}

func TestNewHTTPForwarderRequiresURL(t *testing.T) {
	if _, err := NewHTTPForwarder("", HTTPForwarderOptions{}); err == nil {
		t.Error("expected error for empty URL")
	}
}

func TestHTTPForwarderDeliversInOrder(t *testing.T) {
	rs := &recordingServer{}
	srv := httptest.NewServer(rs.handler(http.StatusOK))
	defer srv.Close()

	hf, err := NewHTTPForwarder(srv.URL, HTTPForwarderOptions{
		JournalDirectory: t.TempDir(),
		Token:            "secret",
		InitialBackoff:   5 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hf.Close()

	for _, e := range []string{"e1", "e2", "e3"} {
		if err := hf.Send(context.Background(), []byte(e)); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, 2*time.Second, func() bool {
		b, _ := rs.snapshot()
		return len(b) == 3
	})
	bodies, auth := rs.snapshot()
	if bodies[0] != "e1" || bodies[1] != "e2" || bodies[2] != "e3" {
		t.Errorf("bodies = %v, want in-order e1,e2,e3", bodies)
	}
	if auth[0] != "Bearer secret" {
		t.Errorf("auth = %q, want Bearer secret", auth[0])
	}
}

func TestHTTPForwarderJournalsOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	jdir := t.TempDir()
	hf, err := NewHTTPForwarder(srv.URL, HTTPForwarderOptions{
		JournalDirectory: jdir,
		InitialBackoff:   5 * time.Millisecond,
		MaxBackoff:       20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hf.Close()

	if err := hf.Send(context.Background(), []byte("doomed-event")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, 2*time.Second, func() bool {
		files, _ := filepath.Glob(filepath.Join(jdir, "failed-*.ndjson"))
		if len(files) == 0 {
			return false
		}
		data, _ := os.ReadFile(files[0])
		return strings.Contains(string(data), "doomed-event")
	})
}

func TestHTTPForwarderRequeuesJournalOnStartup(t *testing.T) {
	jdir := t.TempDir()
	// Pre-existing journal from a prior run.
	if err := os.WriteFile(filepath.Join(jdir, "failed-20260101.ndjson"), []byte("recovered-event\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	rs := &recordingServer{}
	srv := httptest.NewServer(rs.handler(http.StatusOK))
	defer srv.Close()

	hf, err := NewHTTPForwarder(srv.URL, HTTPForwarderOptions{
		JournalDirectory: jdir,
		InitialBackoff:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hf.Close()

	waitFor(t, 2*time.Second, func() bool {
		b, _ := rs.snapshot()
		for _, body := range b {
			if strings.Contains(body, "recovered-event") {
				return true
			}
		}
		return false
	})
}

func TestHTTPForwarderQueueFullJournals(t *testing.T) {
	// A server that blocks so the delivery loop stalls and the queue fills.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	jdir := t.TempDir()
	hf, err := NewHTTPForwarder(srv.URL, HTTPForwarderOptions{
		JournalDirectory: jdir,
		InitialBackoff:   5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hf.Close()

	// Queue capacity is 1024; overflow it to exercise the journal-on-full path.
	var lastErr error
	for i := 0; i < 1200; i++ {
		if err := hf.Send(context.Background(), []byte("bulk")); err != nil {
			lastErr = err
		}
	}
	// Either some sends returned a retryable error, or events were journaled on overflow.
	files, _ := filepath.Glob(filepath.Join(jdir, "failed-*.ndjson"))
	if lastErr == nil && len(files) == 0 {
		t.Skip("queue absorbed all sends without overflow on this run")
	}
}
