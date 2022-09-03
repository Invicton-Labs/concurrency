package concurrency

import (
	"context"
)

type ExecutorInput[InputType any, OutputType any] executorInput[InputType, OutputType, OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]]

func Executor[InputType any, OutputType any](ctx context.Context, input ExecutorInput[InputType, OutputType]) *ExecutorOutput[OutputType] {
	return new(ctx, (executorInput[InputType, OutputType, OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]])(input), saveOutput[OutputType], 0, false)
}

type ExecutorBatchInput[InputType any, OutputType any] executorInput[InputType, OutputType, []OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]]

func ExecutorBatch[InputType any, OutputType any](ctx context.Context, input ExecutorBatchInput[InputType, OutputType]) *ExecutorOutput[[]OutputType] {
	if input.BatchSize <= 0 {
		panic("input.BatchSize must be > 0 when using ExecutorBatch")
	}
	return new(ctx, (executorInput[InputType, OutputType, []OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]])(input), getSaveOutputBatchFunc[OutputType](input.BatchSize), input.BatchMaxPeriod, false)
}

type ExecutorUnbatchInput[InputType any, OutputChanType any] executorInput[InputType, []OutputChanType, OutputChanType, ProcessingFuncWithInputWithOutput[InputType, []OutputChanType]]

func ExecutorUnbatch[InputType any, OutputChanType any](ctx context.Context, input ExecutorUnbatchInput[InputType, OutputChanType]) *ExecutorOutput[OutputChanType] {
	return new(ctx, (executorInput[InputType, []OutputChanType, OutputChanType, ProcessingFuncWithInputWithOutput[InputType, []OutputChanType]])(input), saveOutputUnbatch[OutputChanType], 0, false)
}

type ExecutorFinalInput[InputType any] executorInput[InputType, any, any, ProcessingFuncWithInputWithoutOutput[InputType]]

func ExecutorFinal[InputType any](ctx context.Context, input ExecutorFinalInput[InputType]) *ExecutorOutput[any] {
	return new(ctx, (executorInput[InputType, any, any, ProcessingFuncWithInputWithoutOutput[InputType]])(input), nil, 0, false)
}
