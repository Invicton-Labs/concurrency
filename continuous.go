package concurrency

import (
	"context"
	"time"
)

func getContinuousInputChannel(concurrency int, period time.Duration) (bool, <-chan time.Time) {
	if period == 0 {
		return false, nil
	}
	if concurrency > 1 {
		panic("input.Concurrency cannot be > 1 when minCycleDuration > 0")
	}
	// Create a ticker with a very short period so it ticks almost immediately
	ticker := time.NewTicker(1 * time.Nanosecond)
	// Wait for it to tick
	for len(ticker.C) == 0 {
		time.Sleep(1 * time.Nanosecond)
	}
	// Reset it to the desired period
	ticker.Reset(period)

	return true, ticker.C
}

type ContinuousInput[OutputType any] executorInput[time.Time, OutputType, OutputType, ProcessingFuncWithoutInputWithOutput[OutputType]]

func Continuous[OutputType any](
	ctx context.Context,
	input ContinuousInput[OutputType],
	period time.Duration,
) *ExecutorOutput[OutputType] {
	if input.InputChannel != nil {
		panic("Cannot provide input.InputChannel for Continuous")
	}
	forceWaitForInput, inputChannel := getContinuousInputChannel(input.Concurrency, period)
	input.InputChannel = inputChannel
	return new(ctx, (executorInput[time.Time, OutputType, OutputType, ProcessingFuncWithoutInputWithOutput[OutputType]])(input), saveOutput[OutputType], 0, forceWaitForInput)
}

type ContinuousBatchInput[OutputType any] executorInput[time.Time, OutputType, []OutputType, ProcessingFuncWithoutInputWithOutput[OutputType]]

func ContinuousBatch[OutputType any](
	ctx context.Context,
	input ContinuousBatchInput[OutputType],
	period time.Duration,
) *ExecutorOutput[[]OutputType] {
	if input.InputChannel != nil {
		panic("Cannot provide input.InputChannel for ContinuousBatch")
	}
	if input.BatchSize <= 0 {
		panic("input.BatchSize must be > 0 when using ContinuousBatch")
	}
	forceWaitForInput, inputChannel := getContinuousInputChannel(input.Concurrency, period)
	input.InputChannel = inputChannel
	return new(ctx, (executorInput[time.Time, OutputType, []OutputType, ProcessingFuncWithoutInputWithOutput[OutputType]])(input), getSaveOutputBatchFunc[OutputType](input.BatchSize), input.BatchMaxPeriod, forceWaitForInput)
}

type ContinuousUnbatchInput[OutputChanType any] executorInput[time.Time, []OutputChanType, OutputChanType, ProcessingFuncWithoutInputWithOutput[[]OutputChanType]]

func ContinuousUnbatch[OutputChanType any](
	ctx context.Context,
	input ContinuousUnbatchInput[OutputChanType],
	period time.Duration,
) *ExecutorOutput[OutputChanType] {
	if input.InputChannel != nil {
		panic("Cannot provide input.InputChannel for ContinuousUnbatch")
	}
	forceWaitForInput, inputChannel := getContinuousInputChannel(input.Concurrency, period)
	input.InputChannel = inputChannel
	return new(ctx, (executorInput[time.Time, []OutputChanType, OutputChanType, ProcessingFuncWithoutInputWithOutput[[]OutputChanType]])(input), saveOutputUnbatch[OutputChanType], 0, forceWaitForInput)
}

type ContinuousRebatchInput[OutputType any] executorInput[time.Time, []OutputType, []OutputType, ProcessingFuncWithoutInputWithOutput[[]OutputType]]

func ContinuousRebatch[OutputType any](
	ctx context.Context,
	input ContinuousRebatchInput[OutputType],
	period time.Duration,
) *ExecutorOutput[[]OutputType] {
	if input.InputChannel != nil {
		panic("Cannot provide input.InputChannel for ContinuousRebatch")
	}
	if input.BatchSize <= 0 {
		panic("input.BatchSize must be > 0 when using ContinuousRebatch")
	}
	forceWaitForInput, inputChannel := getContinuousInputChannel(input.Concurrency, period)
	input.InputChannel = inputChannel
	return new(ctx, (executorInput[time.Time, []OutputType, []OutputType, ProcessingFuncWithoutInputWithOutput[[]OutputType]])(input), getSaveOutputRebatchFunc[OutputType](input.BatchSize), input.BatchMaxPeriod, forceWaitForInput)
}

type ContinuousFinalInput executorInput[time.Time, any, any, ProcessingFuncWithoutInputWithoutOutput]

func ContinuousFinal(
	ctx context.Context,
	input ContinuousFinalInput,
	period time.Duration,
) *ExecutorOutput[any] {
	if input.InputChannel != nil {
		panic("Cannot provide input.InputChannel for ContinuousUnbatch")
	}
	forceWaitForInput, inputChannel := getContinuousInputChannel(input.Concurrency, period)
	input.InputChannel = inputChannel
	return new(ctx, (executorInput[time.Time, any, any, ProcessingFuncWithoutInputWithoutOutput])(input), nil, 0, forceWaitForInput)
}
