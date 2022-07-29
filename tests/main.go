package main

import (
	"context"
	"fmt"
	"reflect"
	"time"
)

func prepareChan() chan int {
	var count int = 10000000

	c := make(chan int, count)

	for i := 0; i < count; i++ {
		c <- i
	}
	close(c)
	return c
}

func oneChan() int64 {
	c := prepareChan()

	foundVal := true
	start := time.Now()
	for {
		select {
		case _, foundVal = <-c:
			break
		}
		if !foundVal {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("1 Chan - Standard: %dms\n", ms)
	return ms
}

func twoChan() int64 {
	c := prepareChan()

	neverchan1 := make(chan struct{}, 0)

	foundVal := true
	start := time.Now()
	for {
		select {
		case _, foundVal = <-c:
			break
		case <-neverchan1:
			break
		}
		if !foundVal {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("2 Chan - Standard: %dms\n", ms)
	return ms
}

func threeChan() int64 {
	c := prepareChan()

	neverchan1 := make(chan struct{}, 0)
	neverchan2 := make(chan struct{}, 0)

	foundVal := true
	start := time.Now()
	for {
		select {
		case _, foundVal = <-c:
			break
		case <-neverchan1:
			break
		case <-neverchan2:
			break
		}
		if !foundVal {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("3 Chan - Standard: %dms\n", ms)
	return ms
}

func fourChan() int64 {
	c := prepareChan()

	neverchan1 := make(chan struct{}, 0)
	neverchan2 := make(chan struct{}, 0)
	neverchan3 := make(chan struct{}, 0)

	foundVal := true
	start := time.Now()
	for {
		select {
		case _, foundVal = <-c:
			break
		case <-neverchan1:
			break
		case <-neverchan2:
			break
		case <-neverchan3:
			break
		}
		if !foundVal {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("4 Chan - Standard: %dms\n", ms)
	return ms
}

func oneChanReflect() int64 {
	c := reflect.ValueOf(prepareChan())

	branches := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: c, Send: reflect.Value{}},
	}

	start := time.Now()
	for {
		_, _, recvOK := reflect.Select(branches)
		if !recvOK {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("1 Chan - Reflect: %dms\n", ms)
	return ms
}

func twoChanReflect() int64 {
	c := reflect.ValueOf(prepareChan())
	neverchan1 := reflect.ValueOf(make(chan struct{}, 0))

	branches := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: c, Send: reflect.Value{}},
		{Dir: reflect.SelectRecv, Chan: neverchan1, Send: reflect.Value{}},
	}

	start := time.Now()
	for {
		_, _, recvOK := reflect.Select(branches)
		if !recvOK {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("2 Chan - Reflect: %dms\n", ms)
	return ms
}

func threeChanReflect() int64 {
	c := reflect.ValueOf(prepareChan())
	neverchan1 := reflect.ValueOf(make(chan struct{}, 0))
	neverchan2 := reflect.ValueOf(make(chan struct{}, 0))

	branches := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: c, Send: reflect.Value{}},
		{Dir: reflect.SelectRecv, Chan: neverchan1, Send: reflect.Value{}},
		{Dir: reflect.SelectRecv, Chan: neverchan2, Send: reflect.Value{}},
	}

	start := time.Now()
	for {
		_, _, recvOK := reflect.Select(branches)
		if !recvOK {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("3 Chan - Reflect: %dms\n", ms)
	return ms
}

func fourChanReflect() int64 {
	c := reflect.ValueOf(prepareChan())
	neverchan1 := reflect.ValueOf(make(chan struct{}, 0))
	neverchan2 := reflect.ValueOf(make(chan struct{}, 0))
	neverchan3 := reflect.ValueOf(make(chan struct{}, 0))

	branches := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: c, Send: reflect.Value{}},
		{Dir: reflect.SelectRecv, Chan: neverchan1, Send: reflect.Value{}},
		{Dir: reflect.SelectRecv, Chan: neverchan2, Send: reflect.Value{}},
		{Dir: reflect.SelectRecv, Chan: neverchan3, Send: reflect.Value{}},
	}

	start := time.Now()
	for {
		_, _, recvOK := reflect.Select(branches)
		if !recvOK {
			break
		}
	}
	ms := time.Since(start).Milliseconds()
	fmt.Printf("4 Chan - Reflect: %dms\n", ms)
	return ms
}

func main() {
	oneChan()
	oneChanReflect()
	twoChan()
	twoChanReflect()
	threeChan()
	threeChanReflect()
	fourChan()
	fourChanReflect()
}

func myRoutine(ctx context.Context, c <-chan int, callbackInterval *time.Duration, callback func() error) {
	var timeoutChan <-chan time.Time
	var timer *time.Timer
	if callbackInterval != nil {
		// If we have a callback interval set, create a timer for it
		timer = time.NewTimer(*callbackInterval)
		timeoutChan = timer.C
	} else {
		// If we don't have a callback interval set, create
		// a channel that will never provide a value.
		timeoutChan = make(<-chan time.Time, 0)
	}

	for {
		select {

		// Handle context cancellation
		case <-ctx.Done():
			return

		// Handle timeouts
		case <-timeoutChan:
			callback()

		// Handle a value in the channel
		case v, ok := <-c:
			if !ok {
				// Channel is closed, exit out
				return
			}

			// Do something with v
			fmt.Println(v)
		}

		// Reset the timeout timer, if there is one
		if timer != nil {
			if !timer.Stop() {
				// See documentation for timer.Stop() for why this is needed
				<-timer.C
			}
			// Reset the timer
			timer.Reset(*callbackInterval)
		}
	}
}
