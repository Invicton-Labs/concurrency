package concurrency

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func TestExecutorPerformance(t *testing.T) {
	inputCount := 10000000
	numRoutines := 1

	processingFunc := func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (output uint, err error) {
		//time.Sleep(1 * time.Millisecond)
		return uint(input), nil
	}

	ctx := context.Background()
	inputChan := make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)

	startTimeVanilla := time.Now()
	outputChan := make(chan uint, inputCount)
	errg := errgroup.Group{}
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				goto done
			case v, open := <-inputChan:
				if !open {
					goto done
				}
				r, err := processingFunc(ctx, v, nil)
				if err != nil {
					t.Fatal(err)
				}
				select {

				case <-ctx.Done():
					goto done
				case outputChan <- r:
				}
			}
		}
	done:
		return nil
	})
	if err := errg.Wait(); err != nil {
		t.Fatal(err)
	}
	durationVanilla := time.Since(startTimeVanilla)
	t.Logf("Vanilla duration: %dms", durationVanilla.Milliseconds())
	t.Error(durationVanilla.Milliseconds())

	inputChan = make(chan int, inputCount)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	startTimeConcurrency := time.Now()
	executor := Executor(ctx, ExecutorInput[int, uint]{
		Name:                      "test-executor-performance-1",
		Concurrency:               numRoutines,
		OutputChannelSize:         inputCount,
		InputChannel:              inputChan,
		Func:                      processingFunc,
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	if err := executor.Wait(); err != nil {
		t.Fatal(err)
	}
	durationConcurrency := time.Since(startTimeConcurrency)
	t.Logf("Concurrency duration: %dms", durationConcurrency.Milliseconds())
	testVerifyCleanup(t, executor)
}

func TestExecutor(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor", testExecutor)
}

func testExecutor(t *testing.T, numRoutines int, inputCount int) {
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
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	time.Sleep(100 * time.Millisecond)
	for i := 1; i <= inputCount; i++ {
		inputChan <- i
	}
	close(inputChan)
	if err := executor.Wait(); err != nil {
		t.Fatal(err)
	}
	if int(received) != 2*inputCount {
		t.Fatalf("Received %d inputs, but expected %d\n", received, 2*inputCount)
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
		t.Fatalf("Expected max return value of %d, but received %d\n", inputCount, maxFound)
	}
	if numOutput != 2*inputCount {
		t.Fatalf("Received %d outputs, but expected %d\n", numOutput, 2*inputCount)
	}
	testVerifyCleanup(t, executor)
}

func TestExecutorUnbatch(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-unbatch", testExecutorUnbatch)
}

func testExecutorUnbatch(t *testing.T, numRoutines int, inputCount int) {
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
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	if err := executor.Wait(); err != nil {
		t.Fatal(err)
	}
	if int(received) != inputCount {
		t.Fatalf("Received %d inputs, but expected %d\n", received, 2*inputCount)
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
		t.Fatalf("Expected max return value of %d, but received %d\n", inputCount, maxFound)
	}
	if numOutput != 2*inputCount {
		t.Fatalf("Received %d outputs, but expected %d\n", numOutput, 2*inputCount)
	}
	testVerifyCleanup(t, executor)
}

func TestExecutorBatchNoTimeout(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-batch-no-timeout", testExecutorBatchNoTimeout)
}

func testExecutorBatchNoTimeout(t *testing.T, numRoutines int, inputCount int) {
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
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	if err := executor.Wait(); err != nil {
		t.Fatal(err)
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
			t.Fatalf("Expected non-final output batch to be full with %d values, but it had %d values", batchSize, len(v))
		}
		if batchCount == expectedBatchCount && len(v) != finalBatchSize {
			t.Fatalf("Expected final output batch to have %d values, but it had %d values", finalBatchSize, len(v))
		}
		if batchCount > expectedBatchCount {
			t.Fatalf("Received more batches than expected (%d)", expectedBatchCount)
		}
	}
	if batchCount != expectedBatchCount {
		t.Fatalf("Received fewer batches (%d) than expected (%d)", batchCount, expectedBatchCount)
	}
	testVerifyCleanup(t, executor)
}

func TestExecutorBatchTimeout(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-batch-no-timeout", testExecutorBatchTimeout)
}

func testExecutorBatchTimeout(t *testing.T, numRoutines int, inputCount int) {
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
		Name:              "test-executor-batch-timeout-1",
		Concurrency:       numRoutines,
		OutputChannelSize: inputCount * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *RoutineFunctionMetadata) (uint, error) {
			if metadata.ExecutorInputIndex > uint64(fullBatches*batchSize) && metadata.ExecutorInputIndex%1000 == 0 {
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
			return uint(input), nil
		},
		BatchSize:                 batchSize,
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
		BatchMaxPeriod:            batchMaxInterval,
	})
	batchIdx := 0
	totalOutputs := 0
	for {
		done := false
		select {
		case <-executor.Ctx().Done():
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
	testVerifyCleanup(t, executor)
}

func TestExecutorFinal(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-final", testExecutorFinal)
}
func testExecutorFinal(t *testing.T, numRoutines int, inputCount int) {
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
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})

	if err := executor.Wait(); err != nil {
		t.Fatal(err)
	}
	if int(received) != inputCount {
		t.Fatalf("Received %d inputs, but expected %d\n", received, inputCount)
	}
	testVerifyCleanup(t, executor)
}

func TestExecutorError(t *testing.T) {
	testMultiConcurrenciesMultiInput(t, "executor-error", testExecutorError)
}
func testExecutorError(t *testing.T, numRoutines int, inputCount int) {
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
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	})
	err := executor.Wait()
	if err == nil {
		t.Fatalf("Expected an error, received none")
	}
	if err.Error() != "test-error" {
		t.Fatalf("Received unexpected error string: %s", err.Error())
	}
	testVerifyCleanup(t, executor)
}
