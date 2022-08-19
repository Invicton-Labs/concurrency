package concurrency

import (
	"sync"
	"time"
)

type timeTracker struct {
	lock sync.Mutex
	//lastReset     *time.Time
	timerDuration time.Duration
	timer         *time.Timer
	threadSafe    bool
}

func newTimeTracker(timerDuration time.Duration, threadSafe bool) *timeTracker {
	tt := &timeTracker{
		timerDuration: timerDuration,
		timer:         &time.Timer{},
		threadSafe:    threadSafe,
	}
	if tt.timerDuration != 0 {
		tt.timer = time.NewTimer(timerDuration)
	}
	return tt
}

func (tt *timeTracker) Reset() {
	if tt.timerDuration != 0 {
		if tt.threadSafe {
			tt.lock.Lock()
			defer tt.lock.Unlock()
			// Stop the existing timer
			if !tt.timer.Stop() {
				// It was already expired or stopped, which
				// means there could be a value in the channel.
				// Drain it in a thread-safe manner.
				for {
					select {
					case <-tt.timer.C:
						continue
					default:
						goto done
					}
				}
			}
		} else {
			// If we don't need to handle thread safety,
			// this is a faster way to do it (select is slow).
			if !tt.timer.Stop() {
				for len(tt.timer.C) > 0 {
					<-tt.timer.C
				}
			}
		}
	done:
		tt.timer.Reset(tt.timerDuration)
	}
}

func (tt *timeTracker) TimerChan() <-chan time.Time {
	return tt.timer.C
}
