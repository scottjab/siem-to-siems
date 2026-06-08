package destinations

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// fakeSink is a test Destination that records sent payloads and close calls.
type fakeSink struct {
	mu      sync.Mutex
	got     [][]byte
	closed  int
	sendErr error
}

func (f *fakeSink) Send(_ context.Context, b []byte) error {
	f.mu.Lock()
	f.got = append(f.got, append([]byte(nil), b...))
	f.mu.Unlock()
	return f.sendErr
}

func (f *fakeSink) Close() error {
	f.mu.Lock()
	f.closed++
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func TestFanoutSendsToAll(t *testing.T) {
	a, b := &fakeSink{}, &fakeSink{}
	f := NewFanout(a, b)
	if err := f.Send(context.Background(), []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if a.count() != 1 || string(a.got[0]) != "hi" {
		t.Errorf("sink a got %v", a.got)
	}
	if b.count() != 1 || string(b.got[0]) != "hi" {
		t.Errorf("sink b got %v", b.got)
	}
}

func TestFanoutToleratesSinkErrors(t *testing.T) {
	a := &fakeSink{sendErr: errors.New("boom")}
	b := &fakeSink{}
	f := NewFanout(a, b)
	if err := f.Send(context.Background(), []byte("x")); err != nil {
		t.Errorf("Send should swallow sink errors, got %v", err)
	}
	if b.count() != 1 {
		t.Error("healthy sink should still receive when another fails")
	}
}

func TestFanoutClosesAll(t *testing.T) {
	a, b := &fakeSink{}, &fakeSink{}
	NewFanout(a, b).Close()
	if a.closed != 1 || b.closed != 1 {
		t.Errorf("close counts: a=%d b=%d", a.closed, b.closed)
	}
}

func TestNewFanoutCopiesSinkSlice(t *testing.T) {
	orig := &fakeSink{}
	sinks := []Destination{orig}
	f := NewFanout(sinks...)
	sinks[0] = &fakeSink{} // mutate caller's slice after construction
	if err := f.Send(context.Background(), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if orig.count() != 1 {
		t.Error("fanout should hold its own reference to the original sink")
	}
}
