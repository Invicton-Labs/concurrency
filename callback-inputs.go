package concurrency

import (
	"time"

	"github.com/Invicton-Labs/go-stackerr"
)

// The common set of values for callbacks that are not specific to a single routine
type BaseExecutorCallbackInput struct {
	// The name of the executor
	ExecutorName string
}

type RoutineErrorCallbackInput struct {
	*RoutineFunctionMetadata
	// The error that was returned by the function
	Err stackerr.Error
}

type RoutineSuccessCallbackInput struct {
	*RoutineFunctionMetadata
}

type RoutineContextDoneCallbackInput struct {
	*RoutineFunctionMetadata
	// The error that killed the context
	Err stackerr.Error
}

type ExecutorErrorCallbackInput struct {
	*BaseExecutorCallbackInput
	// The error that caused one or more of the routines to fail
	Err stackerr.Error
}

type ExecutorSuccessCallbackInput struct {
	*BaseExecutorCallbackInput
}

type ExecutorContextDoneCallbackInput struct {
	*BaseExecutorCallbackInput
	// The context cancellation error (may wrap other info)
	Err stackerr.Error
}

type EmptyInputChannelCallbackInput struct {
	*RoutineFunctionMetadata
	// The duration since the last input was received
	TimeSinceLastInput time.Duration
}

type FullOutputChannelCallbackInput struct {
	*RoutineFunctionMetadata
	// The duration since the last output was stored
	TimeSinceLastOutput time.Duration
	// The output index, specific to the corresponding input
	// (i.e. resets at 0 for each input)
	OutputIndex uint64
}
