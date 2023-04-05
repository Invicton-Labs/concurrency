package concurrency

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/Invicton-Labs/go-stackerr"
)

func TestExecutorChain(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-chain", testExecutorChain)
}

func testExecutorChain(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	var processed int32 = 0
	var highestValueProcessed int = 0
	executor1 := Executor(ctx, ExecutorInput[int, uint]{
		Name:              "test-executor-chain-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output uint, err stackerr.Error) {
			return uint(input), nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	executor2 := Chain(executor1, ExecutorInput[uint, string]{
		Name:              "test-executor-chain-2",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount,
		Func: func(ctx context.Context, input uint, metadata *RoutineFunctionMetadata) (output string, err stackerr.Error) {
			return fmt.Sprintf("%d", input), nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	executor3 := ChainFinal(executor2, ExecutorFinalInput[string]{
		Name:              "test-executor-chain-3",
		Concurrency:       1,
		OutputChannelSize: inputCount,
		Func: func(ctx context.Context, input string, metadata *RoutineFunctionMetadata) (err stackerr.Error) {
			processed++
			intval, cerr := strconv.Atoi(input)
			if cerr != nil {
				return stackerr.Wrap(cerr)
			}
			if intval > highestValueProcessed {
				highestValueProcessed = intval
			}
			return nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	err := executor3.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if processed != int32(inputCount) {
		t.Fatalf("Processed %d values, but expected %d", processed, inputCount)
	}
	if highestValueProcessed != inputCount {
		t.Fatalf("Highest processed value was %d, but expected it to be %d", highestValueProcessed, inputCount)
	}
	testVerifyCleanup(t, executor1)
	testVerifyCleanup(t, executor2)
	testVerifyCleanup(t, executor3)
}

func TestExecutorChainBatch(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-chain-batch-unbatch", testExecutorChainBatchUnbatch)
}

func testExecutorChainBatchUnbatch(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	executor1 := Executor(ctx, ExecutorInput[int, uint]{
		Name:              "test-executor-chain-batch-unbatch-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output uint, err stackerr.Error) {
			return uint(input), nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	batchSize := inputCount / 10
	if batchSize < 1 {
		batchSize = 1
	}
	executor2 := ChainBatch(executor1, ExecutorBatchInput[uint, string]{
		Name:              "test-executor-chain-batch-unbatch-2",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount,
		BatchSize:         batchSize,
		Func: func(ctx context.Context, input uint, metadata *RoutineFunctionMetadata) (output string, err stackerr.Error) {
			return fmt.Sprintf("%d", input), nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	executor3 := ChainUnbatch(executor2, ExecutorUnbatchInput[[]string, int64]{
		Name:              "test-executor-chain-batch-unbatch-3",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount,
		Func: func(ctx context.Context, input []string, metadata *RoutineFunctionMetadata) (output []int64, err stackerr.Error) {
			r := make([]int64, len(input))
			for i, v := range input {
				conv, cerr := strconv.ParseInt(v, 10, 64)
				if cerr != nil {
					return nil, stackerr.Wrap(cerr)
				}
				r[i] = conv
			}
			return r, nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	var processed int32 = 0
	var highestValueProcessed int64 = 0
	executor4 := ChainFinal(executor3, ExecutorFinalInput[int64]{
		Name:              "test-executor-chain-batch-4",
		Concurrency:       1,
		OutputChannelSize: inputCount,
		Func: func(ctx context.Context, input int64, metadata *RoutineFunctionMetadata) (err stackerr.Error) {
			processed++
			if input > highestValueProcessed {
				highestValueProcessed = input
			}
			return nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	err := executor4.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if processed != int32(inputCount) {
		t.Fatalf("Processed %d values, but expected %d", processed, inputCount)
	}
	if int(highestValueProcessed) != inputCount {
		t.Fatalf("Highest processed value was %d, but expected it to be %d", highestValueProcessed, inputCount)
	}
	testVerifyCleanup(t, executor1)
	testVerifyCleanup(t, executor2)
	testVerifyCleanup(t, executor3)
	testVerifyCleanup(t, executor4)
}

func TestExecutorIntoSlice(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-into-slice", testExecutorIntoSlice)
}

func testExecutorIntoSlice(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	executor1 := Executor(ctx, ExecutorInput[int, uint]{
		Name:              "test-executor-into-slice-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output uint, err stackerr.Error) {
			return uint(input), nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	res, err := executor1.IntoSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != inputCount {
		t.Fatalf("Output slice had %d values, but expected %d", len(res), inputCount)
	}
	var highestValueProcessed uint = 0
	for _, v := range res {
		if v > highestValueProcessed {
			highestValueProcessed = v
		}
	}
	if int(highestValueProcessed) != inputCount {
		t.Fatalf("Highest processed value was %d, but expected it to be %d", highestValueProcessed, inputCount)
	}
	testVerifyCleanup(t, executor1)
}
