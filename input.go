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
	ctxCancelledFunc                  func(inputIdx uint64) error
	internalCtx                       context.Context
	executorInput                     *executorInput[InputType, OutputType, OutputChanType, ProcessingFuncType]
	emptyInputChannelCallbackInterval time.Duration
	inputChan                         <-chan InputType
	getRoutineFunctionMetadata        func(inputIndex uint64) *RoutineFunctionMetadata
}

func getInput[
	InputType any,
	OutputType any,
	OutputChanType any,
	ProcessingFuncType ProcessingFuncTypes[InputType, OutputType],
](
	settings *getInputSettings[InputType, OutputType, OutputChanType, ProcessingFuncType],
	inputIndex uint64,
	lastInputTime *time.Time,
	batchTimer *timeTracker,
) (
	input InputType,
	channelClosed bool,
	forceSendBatch bool,
	err error,
) {

	inputReceived := false

	select {
	// If the internal context is done, that means that
	// a routine in this executor or downstream has failed,
	// so we always exit on that. We check this first so
	// that it has the highest priority.
	case <-settings.internalCtx.Done():
		return input, false, false, settings.ctxCancelledFunc(inputIndex)

	// The internal context is not done, so now wait for
	// the first thing to act on.
	default:

		select {
		// This will trigger if there's a batch timer and it's ready
		case <-batchTimer.TimerChan():
			return input, false, true, nil

		default:

			// A timer for calling the full output callback on the desired interval,
			// if a callback was provided.
			var callbackTimer *time.Timer = &time.Timer{}
			if settings.executorInput.EmptyInputChannelCallback != nil {
				callbackTimer = time.NewTimer(settings.emptyInputChannelCallbackInterval)
			}

			// We need a loop because a timeout will need to retry after running the callback.
			for {

				select {
				// Check if the internal executor context is done
				case <-settings.internalCtx.Done():
					// If so, exit
					return input, false, false, settings.ctxCancelledFunc(inputIndex)

				// Try to get an input from the input channel
				case input, inputReceived = <-settings.inputChan:
					// If the channel is closed, exit out of the routine
					if !inputReceived {
						if settings.executorInput.RoutineSuccessCallback != nil {
							return input, true, false, settings.executorInput.RoutineSuccessCallback(&RoutineSuccessCallbackInput{
								RoutineFunctionMetadata: settings.getRoutineFunctionMetadata(inputIndex),
							})
						}
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
				case <-callbackTimer.C:
					if err := settings.executorInput.EmptyInputChannelCallback(&EmptyInputChannelCallbackInput{
						RoutineFunctionMetadata: settings.getRoutineFunctionMetadata(inputIndex),
						TimeSinceLastInput:      time.Since(*lastInputTime),
					}); err != nil {
						return input, false, false, err
					}
				}

				// Reset the timer, if there is one
				if callbackTimer != nil {
					if !callbackTimer.Stop() {
						for len(callbackTimer.C) > 0 {
							<-callbackTimer.C
						}
					}
					callbackTimer.Reset(settings.emptyInputChannelCallbackInterval)
				}
			}
		}
	}
}
