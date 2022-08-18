package concurrency

import (
	"context"
	"fmt"
)

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

func (eo *ExecutorOutput[OutputType]) IntoSlice() ([]OutputType, error) {
	results := []OutputType{}
	executor := new(eo.Ctx, executorInput[OutputType, any, any, ProcessingFuncWithInputWithoutOutput[OutputType]]{
		Name:        fmt.Sprintf("%s-into-slice", eo.Name),
		Concurrency: 1,
		upstream:    eo,
		Func: (ProcessingFuncWithInputWithoutOutput[OutputType])(func(_ context.Context, input OutputType, _ *RoutineFunctionMetadata) error {
			results = append(results, input)
			return nil
		}),
	}, nil, nil, false)
	return results, executor.Wait()
}
