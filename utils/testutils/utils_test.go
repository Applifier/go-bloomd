package testutils

import (
	"errors"
	"testing"
	"time"
)

func TestEventually(t *testing.T) {
	t.Run("should stop if function successfull", func(t *testing.T) {
		f := func() error {
			return nil
		}
		if Eventually(f) != nil {
			t.Error("Should not fail")
		}
	})

	t.Run("should return last error if function was un successfull", func(t *testing.T) {
		timeout = 20 * time.Millisecond
		iterSleep = 1 * time.Millisecond
		d := time.Now().Add(10 * time.Millisecond)
		errBefore := errors.New("Before")
		errAfter := errors.New("After")
		f := func() error {
			if time.Now().Before(d) {
				return errBefore
			}
			return errAfter
		}
		if Eventually(f) != errAfter {
			t.Error("Should return last error")
		}
	})
}
