package destinations

import (
	"errors"
	"testing"
)

func TestRetryableError(t *testing.T) {
	inner := errors.New("temporary")
	re := RetryableError{Err: inner}

	if re.Error() != "temporary" {
		t.Errorf("Error() = %q, want temporary", re.Error())
	}
	if !errors.Is(re, inner) {
		t.Error("errors.Is should unwrap to the inner error")
	}

	var target RetryableError
	if !errors.As(error(re), &target) {
		t.Error("errors.As should match RetryableError")
	}
}
