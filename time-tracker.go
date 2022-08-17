package concurrency

import (
	"sync"
	"time"
)

type timeTracker struct {
	lock          sync.Mutex
	lastReset     *time.Time
	timerDuration *time.Duration
	timer         *time.Timer
}

func newTimeTracker(timerDuration *time.Duration) *timeTracker {
	now := time.Now()
	tt := &timeTracker{
		lastReset:     &now,
		timerDuration: timerDuration,
		timer:         &time.Timer{},
	}
	if tt.timerDuration != nil {
		tt.timer = time.NewTimer(*timerDuration)
	}
	return tt
}

func (tt *timeTracker) GetLast() *time.Time {
	tt.lock.Lock()
	defer tt.lock.Unlock()
	if tt.lastReset == nil {
		t := time.Now()
		tt.lastReset = &t
	}
	return tt.lastReset
}

func (tt *timeTracker) Reset() {
	tt.lock.Lock()
	defer tt.lock.Unlock()
	t := time.Now()
	tt.lastReset = &t
	if tt.timerDuration != nil {
		// Stop the existing timer and drain
		// the channel.
		tt.timer.Stop()
		for {
			select {
			case <-tt.timer.C:
				continue
			default:
				goto done
			}
		}
	done:
		tt.timer.Reset(*tt.timerDuration)
	}
}

func (tt *timeTracker) TimerChan() <-chan time.Time {
	return tt.timer.C
}