package testutils

import (
	"time"
)

var timeoutDefault = 1 * time.Second
var iterSleepDefault = 10 * time.Millisecond

var timeout = timeoutDefault
var iterSleep = iterSleepDefault

func Eventually(test func() error) (err error) {
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				break
			default:
				err = test()
				if err == nil {
					close(done)
					break
				}
				<-time.After(iterSleep)
			}
		}
	}()
	select {
	case <-done:
		return err
	case <-time.After(timeout):
		close(done)
		return err
	}
}
