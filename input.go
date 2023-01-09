package concurrency

import (
	"context"
	"time"
)

type getInputSettings[
	InputType any,
	OutputType any,
	OutputChanType any,
	ProcessingFuncType ProcessingFuncTypes[InputType, OutputType],
] struct {
	ctxCancelledFunc                  func(executorInputIdx uint64, routineInputIdx uint64) error
	internalCtx                       context.Context
	executorInput                     *executorInput[InputType, OutputType, OutputChanType, ProcessingFuncType]
	emptyInputChannelCallbackInterval time.Duration
	inputChan                         <-chan InputType
	getRoutineFunctionMetadata        func(executorInputIndex uint64, routineInputIndex uint64) *RoutineFunctionMetadata
}

func getInput[
	InputType any,
	OutputType any,
	OutputChanType any,
	ProcessingFuncType ProcessingFuncTypes[InputType, OutputType],
](
	settings *getInputSettings[InputType, OutputType, OutputChanType, ProcessingFuncType],
	executorInputIndex uint64,
	routineInputIndex uint64,
	lastInputTime *time.Time,
	callbackTimer *timeTracker,
	batchTimer *timeTracker,
) (
	input InputType,
	channelClosed bool,
	forceSendBatch bool,
	err error,
) {

	inputReceived := false

	// If the internal context is done, that means that
	// a routine in this executor or downstream has failed,
	// so we always exit on that. We check this first so
	// that it has the highest priority.
	if settings.internalCtx.Err() != nil {
		return input, false, false, settings.ctxCancelledFunc(executorInputIndex, routineInputIndex)
	} else {
		// The internal context is not done, so now wait for
		// the first thing to act on.

		// Always check the batch timer first, if there is
		// a batch timer.
		if batchTimer.TimerChan() != nil {
			select {
			case <-batchTimer.TimerChan():
				return input, false, true, nil
			default:
			}
		}

		// We need a loop because a timeout will need to retry after running the callback.
		for {

			// Reset the callback timer, if there is one
			callbackTimer.Reset()

			select {
			// Check if the internal executor context is done
			case <-settings.internalCtx.Done():
				// If so, exit
				return input, false, false, settings.ctxCancelledFunc(executorInputIndex, routineInputIndex)

			// Try to get an input from the input channel
			case input, inputReceived = <-settings.inputChan:
				// If the channel is closed, exit out of the routine
				if !inputReceived {
					return input, true, false, nil
				}
				// Update the last input timestamp
				*lastInputTime = time.Now()
				return input, false, false, nil

			// This will trigger if there's a batch timer and it's ready
			case <-batchTimer.TimerChan():
				return input, false, true, nil

			// This will trigger if the output channel is full for a specified
			// amount of time AND an FullOutputChannelCallback is provided. Otherwise,
			// it will never return.
			case <-callbackTimer.TimerChan():
				if err := settings.executorInput.EmptyInputChannelCallback(&EmptyInputChannelCallbackInput{
					RoutineFunctionMetadata: settings.getRoutineFunctionMetadata(executorInputIndex, routineInputIndex),
					TimeSinceLastInput:      time.Since(*lastInputTime),
				}); err != nil {
					return input, false, false, err
				}
			}
		}
	}
}
