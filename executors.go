package concurrency

import (
	"context"
	"fmt"
)

type ExecutorInput[InputType any, OutputType any] executorInput[InputType, OutputType, OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]]

func Executor[InputType any, OutputType any](ctx context.Context, input ExecutorInput[InputType, OutputType]) *ExecutorOutput[OutputType] {
	return new(ctx, (executorInput[InputType, OutputType, OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]])(input), saveOutput[OutputType], nil, false)
}

type ExecutorBatchInput[InputType any, OutputType any] executorInput[InputType, OutputType, []OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]]

func ExecutorBatch[InputType any, OutputType any](ctx context.Context, input ExecutorBatchInput[InputType, OutputType]) *ExecutorOutput[[]OutputType] {
	if input.BatchSize <= 0 {
		panic("input.BatchSize must be > 0 when using NewBatch")
	}
	return new(ctx, (executorInput[InputType, OutputType, []OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]])(input), getSaveOutputBatchFunc[OutputType](input.BatchSize), input.BatchMaxInterval, false)
}

type ExecutorUnbatchInput[InputType any, OutputChanType any] executorInput[InputType, []OutputChanType, OutputChanType, ProcessingFuncWithInputWithOutput[InputType, []OutputChanType]]

func ExecutorUnbatch[InputType any, OutputChanType any](ctx context.Context, input ExecutorUnbatchInput[InputType, OutputChanType]) *ExecutorOutput[OutputChanType] {
	return new(ctx, (executorInput[InputType, []OutputChanType, OutputChanType, ProcessingFuncWithInputWithOutput[InputType, []OutputChanType]])(input), saveOutputUnbatch[OutputChanType], nil, false)
}

type ExecutorFinalInput[InputType any] executorInput[InputType, any, any, ProcessingFuncWithInputWithoutOutput[InputType]]

func ExecutorFinal[InputType any](ctx context.Context, input ExecutorFinalInput[InputType]) *ExecutorOutput[any] {
	return new(ctx, (executorInput[InputType, any, any, ProcessingFuncWithInputWithoutOutput[InputType]])(input), nil, nil, false)
}

func Chain[InputType any, OutputType any](upstream *ExecutorOutput[InputType], input ExecutorInput[InputType, OutputType]) *ExecutorOutput[OutputType] {
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, OutputType, OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]])(input), saveOutput[OutputType], nil, false)
}

func ChainBatch[InputType any, OutputType any](upstream *ExecutorOutput[InputType], input ExecutorBatchInput[InputType, OutputType]) *ExecutorOutput[[]OutputType] {
	if input.BatchSize <= 0 {
		panic("input.BatchSize must be > 0 when using ChainBatch")
	}
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, OutputType, []OutputType, ProcessingFuncWithInputWithOutput[InputType, OutputType]])(input), getSaveOutputBatchFunc[OutputType](input.BatchSize), nil, false)
}

func ChainUnbatch[InputType any, OutputChanType any](upstream *ExecutorOutput[InputType], input ExecutorUnbatchInput[InputType, OutputChanType]) *ExecutorOutput[OutputChanType] {
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, []OutputChanType, OutputChanType, ProcessingFuncWithInputWithOutput[InputType, []OutputChanType]])(input), saveOutputUnbatch[OutputChanType], nil, false)
}

func ChainFinal[InputType any](upstream *ExecutorOutput[InputType], input ExecutorFinalInput[InputType]) *ExecutorOutput[any] {
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, any, any, ProcessingFuncWithInputWithoutOutput[InputType]])(input), nil, nil, false)
}

func IntoSlice[InputType any](upstream *ExecutorOutput[InputType], ctx context.Context) ([]InputType, error) {
	results := []InputType{}
	executor := new(upstream.Ctx, executorInput[InputType, any, any, ProcessingFuncWithInputWithoutOutput[InputType]]{
		Name:        fmt.Sprintf("%s-into-slice", upstream.Name),
		Concurrency: 1,
		Func: (ProcessingFuncWithInputWithoutOutput[InputType])(func(_ context.Context, input InputType, _ *RoutineFunctionMetadata) error {
			results = append(results, input)
			return nil
		}),
	}, nil, nil, false)
	return results, executor.Wait()
}
