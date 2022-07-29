package concurrency

import (
	"context"
	"fmt"
)

type NewInput[InputType any, OutputType any] executorInput[InputType, OutputType, OutputType]

func New[InputType any, OutputType any](ctx context.Context, input NewInput[InputType, OutputType]) *ExecutorOutput[OutputType] {
	return new(ctx, (executorInput[InputType, OutputType, OutputType])(input), saveOutput[OutputType])
}

func NewFinal[InputType any](ctx context.Context, input NewInput[InputType, any]) *ExecutorOutput[any] {
	return new(ctx, (executorInput[InputType, any, any])(input), nil)
}

type NewBatchInput[InputType any, OutputType any] executorInput[InputType, OutputType, []OutputType]

func NewBatch[InputType any, OutputType any](ctx context.Context, input NewBatchInput[InputType, OutputType], batchSize int) *ExecutorOutput[[]OutputType] {
	return new(ctx, (executorInput[InputType, OutputType, []OutputType])(input), getSaveOutputBatchFunc[OutputType](batchSize))
}

type NewUnbatchInput[InputType any, OutputChanType any] executorInput[InputType, []OutputChanType, OutputChanType]

func NewUnbatch[InputType any, OutputChanType any](ctx context.Context, input NewUnbatchInput[InputType, OutputChanType]) *ExecutorOutput[OutputChanType] {
	return new(ctx, (executorInput[InputType, []OutputChanType, OutputChanType])(input), saveOutputUnbatch[OutputChanType])
}

func Chain[InputType any, OutputType any](upstream *ExecutorOutput[InputType], input NewInput[InputType, OutputType]) *ExecutorOutput[OutputType] {
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, OutputType, OutputType])(input), saveOutput[OutputType])
}

func ChainBatch[InputType any, OutputType any](upstream *ExecutorOutput[InputType], input NewBatchInput[InputType, OutputType], batchSize int) *ExecutorOutput[[]OutputType] {
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, OutputType, []OutputType])(input), getSaveOutputBatchFunc[OutputType](batchSize))
}

func ChainUnbatch[InputType any, OutputChanType any](upstream *ExecutorOutput[InputType], input NewUnbatchInput[InputType, OutputChanType]) *ExecutorOutput[OutputChanType] {
	input.upstream = upstream
	return new(upstream.Ctx, (executorInput[InputType, []OutputChanType, OutputChanType])(input), saveOutputUnbatch[OutputChanType])
}

func IntoSlice[InputType any](upstream *ExecutorOutput[InputType], ctx context.Context) ([]InputType, error) {
	results := []InputType{}
	executor := new(upstream.Ctx, executorInput[InputType, any, any]{
		Name:        fmt.Sprintf("%s-into-slice", upstream.Name),
		Concurrency: 1,
		Func: func(_ context.Context, input InputType, _ *RoutineFunctionMetadata) (any, error) {
			results = append(results, input)
			return nil, nil
		},
	}, nil)
	return results, executor.Wait()
}
