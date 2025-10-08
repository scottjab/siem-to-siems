package destinations

import (
	"context"
)

// Destination is a sink that can receive events.
// Implementations must be safe for concurrent use.
type Destination interface {
	Send(ctx context.Context, eventBytes []byte) error
	Close() error
}

// RetryableError indicates a temporary failure that should be retried.
type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string { return e.Err.Error() }
func (e RetryableError) Unwrap() error { return e.Err }

// Note: error aggregation removed per user preference; errors are logged and ignored by fanout.
