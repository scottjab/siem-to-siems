package destinations

import (
	"context"
	"log"
	"sync"
)

// Fanout dispatches events to multiple destinations.
type Fanout struct {
	sinks []Destination
}

func NewFanout(sinks ...Destination) *Fanout {
	// copy to avoid external modifications
	copySinks := make([]Destination, len(sinks))
	copy(copySinks, sinks)
	return &Fanout{sinks: copySinks}
}

func (f *Fanout) Send(ctx context.Context, eventBytes []byte) error {
	var wg sync.WaitGroup
	wg.Add(len(f.sinks))
	for _, s := range f.sinks {
		sink := s
		go func() {
			defer wg.Done()
			if err := sink.Send(ctx, eventBytes); err != nil {
				log.Printf("fanout send error: %v", err)
			}
		}()
	}
	wg.Wait()
	return nil
}

func (f *Fanout) Close() error {
	for _, s := range f.sinks {
		if err := s.Close(); err != nil {
			log.Printf("fanout close error: %v", err)
		}
	}
	return nil
}
