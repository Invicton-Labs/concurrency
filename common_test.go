package concurrency

import (
	"fmt"
	"testing"
	"time"
)

func init() {
	DefaultEmptyInputChannelCallbackInterval = 1 * time.Second
	DefaultFullOutputChannelCallbackInterval = 1 * time.Second
}

func testEmptyInputCallback(input *EmptyInputChannelCallbackInput) error {
	fmt.Printf("%s routine %d has received no input after %dms\n", input.ExecutorName, input.RoutineIndex, input.TimeSinceLastInput.Milliseconds())
	return nil
}

func testFullOutputCallback(input *FullOutputChannelCallbackInput) error {
	fmt.Printf("%s routine %d has not been able to output for %dms\n", input.ExecutorName, input.RoutineIndex, input.TimeSinceLastOutput.Milliseconds())
	return nil
}

func testVerifyCleanup[OutputChanType any](t *testing.T, executor *ExecutorOutput[OutputChanType]) {
	// Drain the output channel
	for len(executor.OutputChan) > 0 {
		<-executor.OutputChan
	}
	if executor.OutputChan != nil {
		select {
		case _, open := <-executor.OutputChan:
			if open {
				t.Errorf("Executor channel should be closed, but is not")
				return
			}
		default:
			t.Errorf("Executor channel should be closed, but is not")
			return
		}
	}
	select {
	case _, open := <-executor.Ctx().Done():
		if open {
			t.Errorf("Executor context should be done, but is not")
			return
		}
	default:
		t.Errorf("Executor context should be done, but is not")
		return
	}
}

func testMultiConcurrencies(t *testing.T, testName string, f func(t *testing.T, numRoutines int)) {
	concurrencies := []int{
		1,
		10,
		100,
		1000,
		10000,
	}
	for _, numRoutines := range concurrencies {
		t.Run(testName, func(t *testing.T) {
			f(t, numRoutines)
		})
	}
}

func testMultiConcurrenciesMultiInput(t *testing.T, testName string, f func(t *testing.T, numRoutines int, inputCount int)) {
	concurrencies := []int{
		1,
		10,
		100,
		1000,
		10000,
	}
	inputCounts := []int{
		1,
		10,
		100,
		1000,
		10000,
		10010,
		100000,
	}

	for _, numRoutines := range concurrencies {
		for _, inputCount := range inputCounts {
			t.Run(testName, func(t *testing.T) {
				f(t, numRoutines, inputCount)
			})
		}
	}
}
