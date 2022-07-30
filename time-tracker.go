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
	t := &timeTracker{
		lastReset:     &now,
		timerDuration: timerDuration,
		timer:         &time.Timer{},
	}
	if timerDuration != nil {
		t.timer = time.NewTimer(*timerDuration)
	}
	return t
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
		if !tt.timer.Stop() {
			for {
				select {
				case <-tt.timer.C:
					break
				default:
					goto done
				}
			}
		done:
		}
		tt.timer.Reset(*tt.timerDuration)
	}
}

func (tt *timeTracker) TimerChan() <-chan time.Time {
	return tt.timer.C
}
