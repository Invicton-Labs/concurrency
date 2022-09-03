package concurrency

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestContinuous(t *testing.T) {
	testMultiConcurrencies(t, "continuous", testContinuous)
}
func testContinuous(t *testing.T, numRoutines int) {
	// This tests the continuous loop with multiple executors and
	// no wait period between the loops.
	{
		ctx, ctxCancel := context.WithCancel(context.Background())
		var generated int32 = 0
		var numOutputs int32 = 10000
		executor1 := Continuous(ctx, ContinuousInput[int32]{
			Name:                   "test-continuous-no-interval-1",
			Concurrency:            numRoutines,
			OutputChannelSize:      int(numOutputs),
			IgnoreZeroValueOutputs: true,
			Func: func(ctx context.Context, metadata *RoutineFunctionMetadata) (int32, error) {
				newval := atomic.AddInt32(&generated, 1)
				if newval > numOutputs {
					<-ctx.Done()
					return 0, nil
				}
				return newval, nil
			},
			EmptyInputChannelCallback: testEmptyInputCallback,
			FullOutputChannelCallback: testFullOutputCallback,
		}, 0)
		var highestValue, received int32 = 0, 0
		executor2 := ChainFinal(executor1, ExecutorFinalInput[int32]{
			Name:        "test-continuous-no-interval-2",
			Concurrency: 1,
			Func: func(ctx context.Context, input int32, metadata *RoutineFunctionMetadata) error {
				received++
				if input > highestValue {
					highestValue = input
				}
				if received == numOutputs {
					ctxCancel()
				}
				return nil
			},
			ProcessUpstreamOutputsAfterUpstreamError: true,
			EmptyInputChannelCallback:                testEmptyInputCallback,
			FullOutputChannelCallback:                testFullOutputCallback,
		})
		err := executor2.Wait()
		if err == nil {
			t.Fatalf("Expected a context cancelled error, but received no error")
		}
		if err := executor2.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if received != numOutputs {
			t.Fatalf("Received %d inputs, but expected %d\n", received, numOutputs)
		}
		testVerifyCleanup(t, executor1)
		testVerifyCleanup(t, executor2)
	}

	// This tests the continus loop with a single executor and a wait period
	if numRoutines == 1 {
		ctx, ctxCancel := context.WithCancel(context.Background())
		var generated int32 = 0
		var numOutputs int32 = 20
		executor1 := Continuous(ctx, ContinuousInput[int32]{
			Name:                   "test-continuous-interval-1",
			Concurrency:            numRoutines,
			OutputChannelSize:      int(numOutputs),
			IgnoreZeroValueOutputs: true,
			Func: func(ctx context.Context, metadata *RoutineFunctionMetadata) (int32, error) {
				newval := atomic.AddInt32(&generated, 1)
				if newval > numOutputs {
					<-ctx.Done()
					return 0, nil
				}
				return newval, nil
			},
			EmptyInputChannelCallback: testEmptyInputCallback,
			FullOutputChannelCallback: testFullOutputCallback,
		}, 100*time.Millisecond)
		var highestValue, received int32 = 0, 0
		executor2 := ChainFinal(executor1, ExecutorFinalInput[int32]{
			Name:        "test-continuous-interval-2",
			Concurrency: 1,
			Func: func(ctx context.Context, input int32, metadata *RoutineFunctionMetadata) error {
				received++
				if input > highestValue {
					highestValue = input
				}
				if received == numOutputs {
					ctxCancel()
				}
				return nil
			},
			ProcessUpstreamOutputsAfterUpstreamError: true,
			EmptyInputChannelCallback:                testEmptyInputCallback,
			FullOutputChannelCallback:                testFullOutputCallback,
		})
		err := executor2.Wait()
		if err == nil {
			t.Fatalf("Expected a context cancelled error, but received no error")
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if received != numOutputs {
			t.Fatalf("Received %d inputs, but expected %d\n", received, numOutputs)
		}
		testVerifyCleanup(t, executor1)
		testVerifyCleanup(t, executor2)
	}
}

func TestContinuousBatch(t *testing.T) {
	testMultiConcurrencies(t, "continuous-batch", testContinuousBatch)
}
func testContinuousBatch(t *testing.T, numRoutines int) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	var generated int32 = 0
	var numOutputs int32 = 10000
	batchSize := 199
	batchCount := int(numOutputs)/batchSize + 1
	executor1 := ContinuousBatch(ctx, ContinuousBatchInput[int32]{
		Name:                   "test-continuous-batch-1",
		Concurrency:            numRoutines,
		BatchSize:              batchSize,
		BatchMaxPeriod:         100 * time.Millisecond,
		OutputChannelSize:      batchCount,
		IgnoreZeroValueOutputs: true,
		Func: func(ctx context.Context, metadata *RoutineFunctionMetadata) (int32, error) {
			newval := atomic.AddInt32(&generated, 1)
			if newval > numOutputs {
				time.Sleep(10 * time.Millisecond)
				return 0, nil
			}
			return newval, nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	}, 0)
	var highestValue, received int32 = 0, 0
	executor2 := ChainFinal(executor1, ExecutorFinalInput[[]int32]{
		Name:        "test-continuous-batch-2",
		Concurrency: 1,
		Func: func(ctx context.Context, input []int32, metadata *RoutineFunctionMetadata) error {
			for _, v := range input {
				received++
				if v > highestValue {
					highestValue = v
				}
			}
			if received >= numOutputs {
				ctxCancel()
			}
			return nil
		},
		ProcessUpstreamOutputsAfterUpstreamError: true,
		EmptyInputChannelCallback:                testEmptyInputCallback,
		FullOutputChannelCallback:                testFullOutputCallback,
	})
	err := executor2.Wait()
	if err == nil {
		t.Fatalf("Expected a context cancelled error, but received no error")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if received != numOutputs {
		t.Fatalf("Received %d inputs, but expected %d\n", received, numOutputs)
	}
	testVerifyCleanup(t, executor1)
	testVerifyCleanup(t, executor2)
}

func TestContinuousUnbatch(t *testing.T) {
	testMultiConcurrencies(t, "continuous-unbatch", testContinuousUnbatch)
}
func testContinuousUnbatch(t *testing.T, numRoutines int) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	var generated int32 = 0
	var numOutputs int32 = 10000
	executor1 := ContinuousUnbatch(ctx, ContinuousUnbatchInput[int32]{
		Name:                   "test-continuous-unbatch-1",
		Concurrency:            numRoutines,
		BatchMaxPeriod:         100 * time.Millisecond,
		OutputChannelSize:      int(numOutputs),
		IgnoreZeroValueOutputs: true,
		Func: func(ctx context.Context, metadata *RoutineFunctionMetadata) ([]int32, error) {
			newval := atomic.AddInt32(&generated, 1)
			if newval > numOutputs {
				time.Sleep(10 * time.Millisecond)
				return []int32{}, nil
			}
			return []int32{newval, newval * 10, newval * 100}, nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	}, 0)
	var highestValue, received int32 = 0, 0
	executor2 := ChainFinal(executor1, ExecutorFinalInput[int32]{
		Name:        "test-continuous-unbatch-2",
		Concurrency: 1,
		Func: func(ctx context.Context, input int32, metadata *RoutineFunctionMetadata) error {
			received++
			if input > highestValue {
				highestValue = input
			}
			if received >= numOutputs*3 {
				ctxCancel()
			}
			return nil
		},
		ProcessUpstreamOutputsAfterUpstreamError: true,
		EmptyInputChannelCallback:                testEmptyInputCallback,
		FullOutputChannelCallback:                testFullOutputCallback,
	})
	err := executor2.Wait()
	if err == nil {
		t.Fatalf("Expected a context cancelled error, but received no error")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if received != numOutputs*3 {
		t.Fatalf("Received %d inputs, but expected %d\n", received, numOutputs*3)
	}
	testVerifyCleanup(t, executor1)
	testVerifyCleanup(t, executor2)
}

func TestContinuousFinal(t *testing.T) {
	testMultiConcurrencies(t, "continuous-final", testContinuousFinal)
}
func testContinuousFinal(t *testing.T, numRoutines int) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	var generated int32 = 0
	var numOutputs int32 = 10000
	executor1 := ContinuousFinal(ctx, ContinuousFinalInput{
		Name:                   "test-continuous-final-1",
		Concurrency:            numRoutines,
		BatchMaxPeriod:         100 * time.Millisecond,
		OutputChannelSize:      int(numOutputs),
		IgnoreZeroValueOutputs: true,
		Func: func(ctx context.Context, metadata *RoutineFunctionMetadata) error {
			newval := atomic.AddInt32(&generated, 1)
			if newval > numOutputs {
				ctxCancel()
				return nil
			}
			return nil
		},
		EmptyInputChannelCallback: testEmptyInputCallback,
		FullOutputChannelCallback: testFullOutputCallback,
	}, 0)

	err := executor1.Wait()
	if err == nil {
		t.Fatalf("Expected a context cancelled error, but received no error")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	testVerifyCleanup(t, executor1)
}
