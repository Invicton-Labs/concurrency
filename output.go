package concurrency

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type saveOutputSettings[
	OutputChanType any,
] struct {
	ctxCancelledFunc                  func(inputIdx uint64) error
	internalCtx                       context.Context
	fullOutputChannelCallbackInterval time.Duration
	fullOutputChannelCallback         func(input *FullOutputChannelCallbackInput) error
	ignoreZeroValueOutputs            bool
	outputChan                        chan<- OutputChanType
	outputIndexCounter                *uint64
	getRoutineFunctionMetadata        func(inputIndex uint64) *RoutineFunctionMetadata
	lastOutputTimeTracker             *lastOutputTimeTracker
	maxBatchDelayInterval             *time.Duration
}

func saveOutput[OutputChanType any](
	settings *saveOutputSettings[OutputChanType],
	value OutputChanType,
	inputIndex uint64,
) (
	err error,
) {

	select {
	// If the internal context is done, that means that
	// a routine in this executor or downstream has failed,
	// so we always exit on that. We check this first so
	// that it has the highest priority.
	case <-settings.internalCtx.Done():
		return settings.ctxCancelledFunc(inputIndex)

	// The internal context is not done, so now wait for
	// the first thing to act on.
	default:

		// If we're supposed to ignore zero value outputs and the output is a
		// zero value, return without doing anything.
		if settings.ignoreZeroValueOutputs && reflect.ValueOf(value).IsZero() {
			return nil
		}

		// Get the index of this output insert attempt
		outputIndex := atomic.AddUint64(settings.outputIndexCounter, 1) - 1

		if settings.fullOutputChannelCallback == nil {
			select {
			// Check if the internal executor context is done
			case <-settings.internalCtx.Done():
				// If so, exit
				return settings.ctxCancelledFunc(inputIndex)

			case settings.outputChan <- value:
				// The insert into the output channel succeeded
				// Update the last output timestamp
				settings.lastOutputTimeTracker.SetNow()
				return nil
			}

		} else {
			// A timer for calling the full output callback on the desired interval,
			// if a callback was provided.
			var callbackTimer *time.Timer

			// A function that resets the output timer, called when an output is stored
			// or the timer expires and triggers a callback.
			callbackTimerResetFunc := func() {
				if callbackTimer != nil {
					callbackTimer.Stop()
				}
				callbackTimer = time.NewTimer(settings.fullOutputChannelCallbackInterval)
			}

			for {

				// Reset the callback timer
				callbackTimerResetFunc()

				select {

				// This will get a value from contextDoneChan when the context is cancelled.
				case <-settings.internalCtx.Done():
					return settings.ctxCancelledFunc(inputIndex)

				// Try to put the result in the output channel
				case settings.outputChan <- value:
					// The insert into the output channel succeeded
					// Update the last output timestamp
					settings.lastOutputTimeTracker.SetNow()
					return nil

				// This will trigger if the output channel is full for a specified
				// amount of time AND an FullOutputChannelCallback is provided. Otherwise,
				// it will never return.
				case <-callbackTimer.C:
					if err := settings.fullOutputChannelCallback(&FullOutputChannelCallbackInput{
						RoutineFunctionMetadata: settings.getRoutineFunctionMetadata(inputIndex),
						TimeSinceLastOutput:     time.Since(*(settings.lastOutputTimeTracker.Get())),
						OutputIndex:             outputIndex,
					}); err != nil {
						return err
					}
				}
			}
		}
	}
}

func saveOutputUnbatch[OutputType any](
	settings *saveOutputSettings[OutputType],
	values []OutputType,
	inputIndex uint64,
) (
	err error,
) {
	// Loop through each value in the result batch
	for _, value := range values {
		// Insert the value into the output channel
		err = saveOutput(settings, value, inputIndex)
		// Check if the result was a cancelled context or an error
		if err != nil {
			// If so, return right away
			return err
		}
	}
	return nil
}

func getSaveOutputBatchFunc[OutputType any](ctx context.Context, batchSize int, maxBatchInterval *time.Duration) func(
	settings *saveOutputSettings[[]OutputType],
	value OutputType,
	inputIndex uint64,
) (
	err error,
) {
	batch := make([]OutputType, batchSize)
	var batchIdx int = 0
	var batchLock sync.Mutex

	var outputTimer *time.Timer

	var timerResetChan chan struct{}

	timerFunc := func() {
		// A timer for sending an incomplete batch, if a certain amount of time
		// has passed.

		for {
			select {
			// If the context is done, exit out
			case <-ctx.Done():
				return

			// If we get a notification saying the timer was reset,
			// restart the loop so we wait on a new timer
			case <-timerResetChan:
				continue

			//
			case <-outputTimer.C:
				break
			}

			// The timer fired
			batchLock.Lock()
			if len(batch) > 0 {

			}
			if !outputTimer.Stop() {
				<-outputTimer.C
			}
			outputTimer.Reset(*maxBatchInterval)
			batchLock.Unlock()
		}
	}

	return func(
		settings *saveOutputSettings[[]OutputType],
		value OutputType,
		inputIndex uint64,
	) (
		err error,
	) {
		batchLock.Lock()
		defer batchLock.Unlock()

		// If it's the first output, start the batch max interval timer
		if maxBatchInterval != nil && outputTimer == nil {
			outputTimer = time.NewTimer(*maxBatchInterval)
			timerResetChan = make(chan struct{}, 10)
			go timerFunc()
		}

		// If we want zero values, or it's not a zero value anyways, save
		// the output value into the batch and increment the batch index.
		if !settings.ignoreZeroValueOutputs || !reflect.ValueOf(value).IsZero() {
			batch[batchIdx] = value
			batchIdx++

			// If the batch is full, output it
			if batchIdx == batchSize-1 {
				// Save the output
				err = saveOutput(settings, batch, inputIndex)

				// Clear the batch
				batch = make([]OutputType, batchSize)

				// Reset the batch timer if we have one
				if maxBatchInterval != nil {
					if !outputTimer.Stop() {
						<-outputTimer.C
					}
					outputTimer.Reset(*maxBatchInterval)
					// Send a signal that the timer was reset
					timerResetChan <- struct{}{}
				}
				return err
			}
		}

		return
	}
}
