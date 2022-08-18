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
		if err := executor2.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if received != numOutputs {
			t.Fatalf("Received %d inputs, but expected %d\n", received, numOutputs)
		}
		testVerifyCleanup(t, executor1)
		testVerifyCleanup(t, executor2)
	}
}
