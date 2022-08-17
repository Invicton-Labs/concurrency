package concurrency

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutor(t *testing.T) {
	testMultiConcurrencies(t, "executor", executor)
}

func executor(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	var received int32 = 0
	executor := Executor(ctx, ExecutorInput[int, uint]{
		Name:              "test-executor-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output uint, err error) {
			atomic.AddInt32(&received, 1)
			return uint(input), nil
		},
		EmptyInputChannelCallback: emptyInput,
		FullOutputChannelCallback: fullOutput,
	})
	time.Sleep(100 * time.Millisecond)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	if err := executor.Wait(); err != nil {
		t.Error(err)
		return
	}
	if int(received) != 2*inputCount {
		t.Errorf("Received %d inputs, but expected %d\n", received, 2*inputCount)
		return
	}
	maxFound := uint(0)
	numOutput := 0
	for {
		v, open := <-executor.OutputChan
		if !open {
			break
		}
		if v > maxFound {
			maxFound = v
		}
		numOutput++
	}
	if maxFound != uint(inputCount) {
		t.Errorf("Expected max return value of %d, but received %d\n", inputCount, maxFound)
		return
	}
	if numOutput != 2*inputCount {
		t.Errorf("Received %d outputs, but expected %d\n", numOutput, 2*inputCount)
		return
	}
	verifyCleanup(t, executor)
}

func TestExecutorUnbatch(t *testing.T) {
	testMultiConcurrencies(t, "executor-unbatch", executorUnbatch)
}

func executorUnbatch(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	var received int32 = 0
	executor := ExecutorUnbatch(ctx, ExecutorUnbatchInput[int, uint]{
		Name:              "test-executor-unbatch-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output []uint, err error) {
			atomic.AddInt32(&received, 1)
			return []uint{uint(input), uint(input)}, nil
		},
		EmptyInputChannelCallback: emptyInput,
		FullOutputChannelCallback: fullOutput,
	})
	if err := executor.Wait(); err != nil {
		t.Error(err)
		return
	}
	if int(received) != inputCount {
		t.Errorf("Received %d inputs, but expected %d\n", received, 2*inputCount)
		return
	}
	maxFound := uint(0)
	numOutput := 0
	for {
		v, open := <-executor.OutputChan
		if !open {
			break
		}
		if v > maxFound {
			maxFound = v
		}
		numOutput++
	}
	if maxFound != uint(inputCount) {
		t.Errorf("Expected max return value of %d, but received %d\n", inputCount, maxFound)
		return
	}
	if numOutput != 2*inputCount {
		t.Errorf("Received %d outputs, but expected %d\n", numOutput, 2*inputCount)
		return
	}
	verifyCleanup(t, executor)
}

func TestExecutorBatchNoTimeout(t *testing.T) {
	testMultiConcurrencies(t, "executor-batch-no-timeout", executorBatchNoTimeout)
}

func executorBatchNoTimeout(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	batchSize := 100
	executor := ExecutorBatch(ctx, ExecutorBatchInput[int, uint]{
		Name:              "test-executor-batch-no-timeout-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (uint, error) {
			return uint(input), nil
		},
		BatchSize:                 batchSize,
		EmptyInputChannelCallback: emptyInput,
		FullOutputChannelCallback: fullOutput,
	})
	if err := executor.Wait(); err != nil {
		t.Error(err)
		return
	}
	batchCount := 0
	expectedBatchCount := int(math.Ceil(float64(inputCount)/float64(batchSize)) + 0.1)
	finalBatchSize := inputCount % batchSize
	if finalBatchSize == 0 {
		finalBatchSize = batchSize
	}
	for v := range executor.OutputChan {
		batchCount++
		if batchCount < expectedBatchCount && len(v) != batchSize {
			t.Errorf("Expected non-final output batch to be full with %d values, but it had %d values", batchSize, len(v))
			return
		}
		if batchCount == expectedBatchCount && len(v) != finalBatchSize {
			t.Errorf("Expected final output batch to have %d values, but it had %d values", finalBatchSize, len(v))
			return
		}
		if batchCount > expectedBatchCount {
			t.Errorf("Received more batches than expected (%d)", expectedBatchCount)
			return
		}
	}
	if batchCount != expectedBatchCount {
		t.Errorf("Received fewer batches (%d) than expected (%d)", batchCount, expectedBatchCount)
		return
	}
	verifyCleanup(t, executor)
}

func TestExecutorBatchTimeout(t *testing.T) {
	testMultiConcurrencies(t, "executor-batch-no-timeout", executorBatchTimeout)
}

func executorBatchTimeout(t *testing.T, numRoutines int, inputCount int) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	batchMaxInterval := 10 * time.Millisecond
	batchSize := 4000
	fullBatches := 2
	executor := ExecutorBatch(ctx, ExecutorBatchInput[int, uint]{
		Name:                           "test-executor-batch-timeout-1",
		Concurrency:                    numRoutines,
		OutputChannelSize:              inputCount * 2,
		InputChannel:                   inputChan,
		IncludeMetadataInFunctionCalls: true,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (uint, error) {
			if metadata.InputIndex > uint64(fullBatches*batchSize) && metadata.InputIndex%1000 == 0 {
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
			return uint(input), nil
		},
		BatchSize:                 batchSize,
		EmptyInputChannelCallback: emptyInput,
		FullOutputChannelCallback: fullOutput,
		BatchMaxInterval:          &batchMaxInterval,
	})
	batchIdx := 0
	totalOutputs := 0
	for {
		done := false
		select {
		case <-executor.Ctx.Done():
			done = true
		case v, open := <-executor.OutputChan:
			if !open {
				if totalOutputs < inputCount {
					t.Fatalf("Output channel closed with insufficient outputs")
				}
				done = true
				break
			}
			// If there's only a single routine, we can test to ensure the batches are being
			// sent before they're full.
			if numRoutines == 1 && batchIdx > fullBatches && len(v) >= batchSize {
				t.Fatalf("Output batch %d is full (%d elements), but expected an incomplete batch", batchIdx, len(v))
			}
			totalOutputs += len(v)
			batchIdx++
		}
		if done {
			break
		}
	}
	ctxCancel()
	if err := executor.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Expected a context cancelled error, but got %v", err)
	}
	verifyCleanup(t, executor)
}

func TestExecutorFinal(t *testing.T) {
	testMultiConcurrencies(t, "executor-final", executorFinal)
}
func executorFinal(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	var received int32 = 0
	executor := ExecutorFinal(ctx, ExecutorFinalInput[int]{
		Name:              "test-executor-final-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (err error) {
			atomic.AddInt32(&received, 1)
			return nil
		},
		EmptyInputChannelCallback: emptyInput,
		FullOutputChannelCallback: fullOutput,
	})

	if err := executor.Wait(); err != nil {
		t.Error(err)
		return
	}
	if int(received) != inputCount {
		t.Errorf("Received %d inputs, but expected %d\n", received, inputCount)
		return
	}
	verifyCleanup(t, executor)
}

func TestExecutorError(t *testing.T) {
	testMultiConcurrencies(t, "executor-error", executorError)
}
func executorError(t *testing.T, numRoutines int, inputCount int) {
	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	executor := Executor(ctx, ExecutorInput[int, uint]{
		Name:              "test-executor-error-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output uint, err error) {
			if input > inputCount/2 {
				return 0, fmt.Errorf("test-error")
			}
			return uint(input), nil
		},
		EmptyInputChannelCallback: emptyInput,
		FullOutputChannelCallback: fullOutput,
	})
	err := executor.Wait()
	if err == nil {
		t.Errorf("Expected an error, received none")
		return
	}
	if err.Error() != "test-error" {
		t.Errorf("Received unexpected error string: %s", err.Error())
		return
	}
	verifyCleanup(t, executor)
}
