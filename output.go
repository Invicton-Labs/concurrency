package concurrency

import (
	"reflect"
	"sync/atomic"
	"time"
)

type outputSettings[OutputType any] struct {
	CtxDoneChan                       <-chan struct{}
	OutputChan                        chan<- OutputType
	FullOutputChannelCallbackInterval time.Duration
	FullOutputChannelCallback         func(input *FullOutputChannelCallbackInput) error
	IgnoreZeroValueOutputs            bool
	CheckContextFirst                 bool
	GetRoutineFunctionMetadata        func() *RoutineFunctionMetadata
}

func output[OutputType any](
	input *outputSettings[OutputType],
	value OutputType,
	inputIndex uint64,
	outputIndexCounter *uint64,
	lastOutputTime *time.Time,
) (
	outputTime *time.Time,
	ctxCancelled bool,
	err error,
) {
	// Optionally, check to see if the context is done
	if input.CheckContextFirst {
		select {
		case <-input.CtxDoneChan:
			return nil, true, nil
		default:
			break
		}
	}

	// If we're supposed to ignore zero value outputs and the output is a
	// zero value, return without doing anything.
	if input.IgnoreZeroValueOutputs && reflect.ValueOf(value).IsZero() {
		return nil, false, nil
	}

	// Get the index of this output insert attempt
	outputIndex := atomic.AddUint64(outputIndexCounter, 1)

	if input.FullOutputChannelCallback == nil {
		// We have no full output callback, so just wait indefinitely
		// until either the context is cancelled or the value
		// is inserted into the output channel.
		select {
		// This will get a value from contextDoneChan when the context is cancelled.
		case <-input.CtxDoneChan:
			return nil, true, nil
		// Try to put the result in the output channel
		case input.OutputChan <- value:
			// The insert into the output channel succeeded
			t := time.Now()
			return &t, false, nil
		}

	} else {

		// A timer for calling the full output callback on the desired interval,
		// if a callback was provided.
		var callbackTimer *time.Timer

		// A function that resets the output timer, called when an output is stored
		// or the timer expires and triggers a callback.
		outputTimerResetFunc := func() {
			if callbackTimer != nil {
				callbackTimer.Stop()
			}
			callbackTimer = time.NewTimer(input.FullOutputChannelCallbackInterval)
		}

		// Set the initial callback timer
		outputTimerResetFunc()

		for {
			select {

			// This will get a value from contextDoneChan when the context is cancelled.
			case <-input.CtxDoneChan:
				return nil, true, nil

			// Try to put the result in the output channel
			case input.OutputChan <- value:
				// The insert into the output channel succeeded
				t := time.Now()
				return &t, false, nil

			// This will trigger if the output channel is full for a specified
			// amount of time AND an FullOutputChannelCallback is provided. Otherwise,
			// it will never return.
			case <-callbackTimer.C:
				if err := input.FullOutputChannelCallback(&FullOutputChannelCallbackInput{
					RoutineFunctionMetadata: input.GetRoutineFunctionMetadata(),
					TimeSinceLastOutput:     time.Since(*lastOutputTime),
					OutputIndex:             outputIndex,
				}); err != nil {
					return nil, false, err
				}
			}

			// Reset the callback timer
			outputTimerResetFunc()
		}
	}
}

func outputUnbatch[OutputType any](
	settings *outputSettings[OutputType],
	values []OutputType,
	inputIndex uint64,
	outputCounter *uint64,
	lastOutputTime *time.Time,
) (
	outputTime *time.Time,
	ctxCancelled bool,
	err error,
) {
	// Loop through each value in the result batch
	for _, value := range values {
		// Insert the value into the output channel
		lastOutputTime, ctxCancelled, err = output(settings, value, inputIndex, outputCounter, lastOutputTime)
		// Check if the result was a cancelled context or an error
		if ctxCancelled || err != nil {
			// If so, return right away
			return lastOutputTime, ctxCancelled, err
		}
	}
	return lastOutputTime, false, nil
}

func getOutputBatchFunc[OutputType any](batchSize int) func(
	settings *outputSettings[OutputType],
	value OutputType,
	inputIndex uint64,
	outputCounter *uint64,
	lastOutputTime *time.Time,
) (
	outputTime *time.Time,
	ctxCancelled bool,
	err error,
) {
	batch := make([]OutputType, batchSize)
	var batchIdx uint64 = 0
	return func(
		settings *outputSettings[OutputType],
		value OutputType,
		inputIndex uint64,
		outputCounter *uint64,
		lastOutputTime *time.Time,
	) (
		outputTime *time.Time,
		ctxCancelled bool,
		err error,
	) {

		batch[at]

		// Loop through each value in the result batch
		for _, value := range values {
			// Insert the value into the output channel
			lastOutputTime, ctxCancelled, err = output(settings, value, inputIndex, outputCounter, lastOutputTime)
			// Check if the result was a cancelled context or an error
			if ctxCancelled || err != nil {
				// If so, return right away
				return lastOutputTime, ctxCancelled, err
			}
		}
		return lastOutputTime, false, nil
	}
}
