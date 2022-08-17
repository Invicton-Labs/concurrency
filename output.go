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
	outputTimeTracker                 *timeTracker
}

func saveOutput[OutputChanType any](
	settings *saveOutputSettings[OutputChanType],
	value OutputChanType,
	inputIndex uint64,
	forceSendBatch bool,
) (
	err error,
) {
	if forceSendBatch {
		panic("Unexpected use of forceSendBatch")
	}

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

		callbackTimer := &time.Timer{}
		if settings.fullOutputChannelCallback != nil {
			callbackTimer = time.NewTimer(settings.fullOutputChannelCallbackInterval)
		}

		for {

			select {

			// This will get a value from contextDoneChan when the context is cancelled.
			case <-settings.internalCtx.Done():
				return settings.ctxCancelledFunc(inputIndex)

			// Try to put the result in the output channel
			case settings.outputChan <- value:
				// The insert into the output channel succeeded
				// Update the last output timestamp
				settings.outputTimeTracker.Reset()
				return nil

			// This will trigger if the output channel is full for a specified
			// amount of time AND an FullOutputChannelCallback is provided. Otherwise,
			// it will never return.
			case <-callbackTimer.C:
				if err := settings.fullOutputChannelCallback(&FullOutputChannelCallbackInput{
					RoutineFunctionMetadata: settings.getRoutineFunctionMetadata(inputIndex),
					TimeSinceLastOutput:     time.Since(*(settings.outputTimeTracker.GetLast())),
					OutputIndex:             outputIndex,
				}); err != nil {
					return err
				}
			}

			// Reset the callback timer
			if !callbackTimer.Stop() {
				for len(callbackTimer.C) > 0 {
					<-callbackTimer.C
				}
			}
			callbackTimer.Reset(settings.fullOutputChannelCallbackInterval)
		}
	}
}

func saveOutputUnbatch[OutputType any](
	settings *saveOutputSettings[OutputType],
	values []OutputType,
	inputIndex uint64,
	forceSendBatch bool,
) (
	err error,
) {
	if forceSendBatch {
		panic("Unexpected use of forceSendBatch")
	}
	// Loop through each value in the result batch
	for _, value := range values {
		// Insert the value into the output channel
		err = saveOutput(settings, value, inputIndex, false)
		// Check if the result was a cancelled context or an error
		if err != nil {
			// If so, return right away
			return err
		}
	}
	return nil
}

func getSaveOutputBatchFunc[OutputType any](batchSize int) func(
	settings *saveOutputSettings[[]OutputType],
	value OutputType,
	inputIndex uint64,
	forceSendBatch bool,
) (
	err error,
) {
	batch := make([]OutputType, batchSize)
	var batchIdx int = 0
	var batchLock sync.Mutex

	return func(
		settings *saveOutputSettings[[]OutputType],
		value OutputType,
		inputIndex uint64,
		forceSendBatch bool,
	) (
		err error,
	) {
		batchLock.Lock()
		defer batchLock.Unlock()

		// If we want zero values, or it's not a zero value anyways, save
		// the output value into the batch and increment the batch index.
		if !forceSendBatch && (!settings.ignoreZeroValueOutputs || !reflect.ValueOf(value).IsZero()) {
			batch[batchIdx] = value
			batchIdx++
		}

		// If we're force-sending a batch, or the batch is full, output it
		if (forceSendBatch && batchIdx > 0) || batchIdx == batchSize {
			// Save the output
			if batchIdx == batchSize-1 {
				// If it's a complete batch, send the entire slice
				err = saveOutput(settings, batch, inputIndex, false)
			} else {
				// If it's an incomplete batch, send a subslice since the slice
				// was pre-allocated and filled with zero-values (which we don't
				// want to send downstream)
				err = saveOutput(settings, batch[0:batchIdx], inputIndex, false)
			}

			// Clear the batch
			batch = make([]OutputType, batchSize)
			batchIdx = 0

			return err
		} else if forceSendBatch {
			// If it's a force batch send, but we didn't actually
			// send a batch, reset the output timer anyways
			settings.outputTimeTracker.Reset()
		}

		return nil
	}
}
