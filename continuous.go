package concurrency

import (
	"context"
	"time"
)

const zeroDuration time.Duration = 0 * time.Nanosecond

func scheduleCycle(ctx context.Context, nextCycleTime *time.Time, minCycleDuration time.Duration, inputChan chan struct{}) {
	// If there was no error, handle timing
	// Calculate the amount of time to wait until the next cycle should start
	*nextCycleTime = nextCycleTime.Add(minCycleDuration)
	waitTime := time.Until(*nextCycleTime)
	if waitTime > zeroDuration {
		// It's more than 0, so start a routine that will send something into our
		// channel when it's time to run.
		go func() error {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(waitTime):
				break
			}
			inputChan <- struct{}{}
			return nil
		}()
	} else {
		inputChan <- struct{}{}
	}
}

type ContinuousInput[OutputType any] executorInput[struct{}, OutputType, OutputType, ProcessingFuncWithoutInputWithOutput[OutputType]]

func Continuous[OutputType any](
	ctx context.Context,
	input ContinuousInput[OutputType],
	minCycleDuration time.Duration,
) *ExecutorOutput[OutputType] {
	inputChanSize := 2 * input.Concurrency
	if inputChanSize == 0 {
		inputChanSize = 2
	}

	forceWaitForInput := false

	// If we have a minimum cycle duration, we need to do many extra things
	if minCycleDuration > 0 {
		if input.Concurrency > 1 {
			panic("input.Concurrency cannot be > 1 when minCycleDuration > 0")
		}

		// If we have a minimum cycle duration, then we want to force the executor to wait for an input from the input channel
		// (this is how we signal it that it's time to run the function again).
		forceWaitForInput = true

		input.InputChannel = make(chan struct{}, inputChanSize)
		input.InputChannel <- struct{}{}

		nextCycleTime := time.Now()

		input.Func = func(ctx context.Context, metadata *RoutineFunctionMetadata) (OutputType, error) {
			output, err := input.Func(ctx, metadata)
			if err != nil {
				return output, err
			}
			scheduleCycle(ctx, &nextCycleTime, minCycleDuration, input.InputChannel)
			return output, err
		}
	}

	return new(ctx, (executorInput[struct{}, OutputType, OutputType, ProcessingFuncWithoutInputWithOutput[OutputType]])(input), saveOutput[OutputType], nil, forceWaitForInput)
}
