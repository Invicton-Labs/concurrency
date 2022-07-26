package concurrency

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

var (
	DefaultEmptyInputChannelCallbackInterval time.Duration = 1 * time.Second
	DefaultFullOutputChannelCallbackInterval time.Duration = 1 * time.Second
)

// The common set of values for callbacks that are specific to single routine
type RoutineFunctionMetadata struct {
	// The name of the executor
	ExecutorName string
	// The index of the routine (will be in [0-concurrency))
	RoutineIndex uint
	// The index of the input (the input value is the Nth value
	// from the input channel)
	InputIndex uint64
	// The status tracker for this executor
	RoutineStatusTracker *RoutineStatusTracker
	// The status trackers for all executors in this chain (by map)
	RoutineStatusTrackersMap map[string]*RoutineStatusTracker
	// The status trackers for all executors in this chain (by slice, in order of chaining)
	RoutineStatusTrackersSlice []*RoutineStatusTracker
	// A logger that is sweetened with additional data about the executor/routine
	//Log *zap.SugaredLogger
}

type ExecutorInput[InputType any, OutputType any] executorInput[InputType, OutputType, chan<- OutputType, func(ctx context.Context, input InputType, metadata *RoutineFunctionMetadata) (output OutputType, err error)]

type ExecutorUnbatchInput[InputType any, OutputType any] executorInput[InputType, OutputType, chan<- OutputType, func(ctx context.Context, input InputType, metadata *RoutineFunctionMetadata) (output []OutputType, err error)]

type ExecutorBatchInput[InputType any, OutputType any] executorInput[InputType, OutputType, chan<- []OutputType, func(ctx context.Context, input InputType, metadata *RoutineFunctionMetadata) (output OutputType, err error)]

type executorInput[
	InputType any,
	OutputType any,
	OutputChanType chan<- OutputType | chan<- []OutputType,
	FunctionType func(ctx context.Context, input InputType, metadata *RoutineFunctionMetadata) (output OutputType, err error) | func(ctx context.Context, input InputType, metadata *RoutineFunctionMetadata) (output []OutputType, err error),
] struct {
	// REQUIRED. A name that can be used in logs for this executor,
	// and for finding it in callback inputs.
	Name string

	// REQUIRED. The number of routines to run.
	Concurrency int

	// REQUIRED. The channel that has input values.
	InputChannel <-chan InputType

	// REQUIRED. The function that processes an input into an output.
	Func FunctionType

	// OPTIONAL. The size of the output channel that gets created.
	// Defaults to the twice the Concurrency value. Only applies if
	// this is a non-final executor.
	OutputChannelSize int

	// OPTIONAL. A channel to use for outputs, instead of creating a new
	// one internally.
	OutputChannel OutputChanType

	// OPTIONAL. Whether to ignore zero-value outputs from the processing function.
	// If true, zero-value outputs (the default value of the output type) will not
	// be sent downstream.
	IgnoreZeroValueOutputs bool

	// OPTIONAL. Whether to process all remaining upstream executor outputs with the
	// consumer after the upstream executor errors out. Default (false) is to immediately
	// kill the consumer if the upstream executor fails. Has no effect for the
	// top-level executor in a chain.
	ProcessUpstreamOutputsAfterUpstreamError bool

	// OPTIONAL. Whether to include a struct of metadata as an argument to the function
	// calls. This can be useful when coordinating shared resources between routines or
	// for debugging, but may impact high-performance applications.
	IncludeMetadataInFunctionCalls bool

	// OPTIONAL. How long to wait for an input before calling the empty input callback
	// function, IF one has been provided. Defaults to the
	// DefaultEmptyInputChannelCallbackInterval value.
	EmptyInputChannelCallbackInterval time.Duration
	// OPTIONAL. A function to call when the input channel is empty, but not closed.
	// Each routine has its own separate timer, so this could be called many times
	// concurrently by different routines.
	EmptyInputChannelCallback func(input *EmptyInputChannelCallbackInput) error

	// How long to wait to write an output before calling the full outputcallback
	// function, IF one has been provided.  Defaults to the
	// DefaultFullOutputChannelCallbackInterval value.
	FullOutputChannelCallbackInterval time.Duration
	// A function to call when the output channel is full and an output cannot
	// be written to it. Each routine has its own separate timer, so this could be called many
	// times concurrently by different routines.
	// Only applies if the these options are for a non-final executor.
	FullOutputChannelCallback func(input *FullOutputChannelCallbackInput) error

	// OPTIONAL. A function to call when the routine is about to exit due to an error.
	// Note that this is PER ROUTINE, not when the executor (group of routines) is about to exit.
	RoutineErrorCallback func(input *RoutineErrorCallbackInput) error
	// OPTIONAL. A function to call when the routine is about to exit due to the
	// input channel being closed. Note that this is PER ROUTINE,
	// not when the executor (group of routines) is about to exit.
	RoutineSuccessCallback func(input *RoutineSuccessCallbackInput) error
	// OPTIONAL. A function to call when a routine is about to exit due to the context
	// being done. Note that this is PER ROUTINE, not when the executor (group of routines)
	// is about to exit.
	RoutineContextDoneCallback func(input *RoutineContextDoneCallbackInput) error

	// OPTIONAL. A function to call ONCE when all routines in the executor are finished
	// and there was an error in one or more routines.
	ExecutorErrorCallback func(input *ExecutorErrorCallbackInput) error
	// OPTIONAL. A function to call ONCE when all routines in the executor are finished
	// and there were no errors.
	ExecutorSuccessCallback func(input *ExecutorSuccessCallbackInput) error
	// OPTIONAL. A function to call ONCE when all routines in the executor are finished
	// and the context was cancelled.
	ExecutorContextDoneCallback func(input *ExecutorContextDoneCallbackInput) error

	// Internal use only. Passthrough values from upstream executors.
	upstreamRoutineStatusTrackers []*RoutineStatusTracker
	// Internal use only. The errgroup.Group that is used for the upstream process.
	upstreamErrGroup *errgroup.Group
	// Internal use only. If set to true, results returned from the function will be
	// ignored and no output channel will be created.
	disableOutputChan bool
	// Internal use only. The context of the top-level executor.
	internalExecutorCtx context.Context
	// Internal use only. The cancellation function for the context of the top-level executor.
	internalExecutorCtxCancel context.CancelFunc
}

type ExecutorOutput[OutputType any] struct {

	// A context that is derived from the top-level executor's
	// input context and is cancelled if any of the executors in
	// a chain fail.
	Ctx context.Context

	// The status tracker for the routines.
	RoutineStatusTracker *RoutineStatusTracker

	// The channel that the outputs are written to
	OutputChan <-chan OutputType

	// Internal use only. A function to cancel the output (passthrough) context.
	passthroughCtxCancel context.CancelFunc

	// Internal use only. The error group that is used for the executor routines.
	errorGroup *errgroup.Group

	// Internal use only. The internal context used by the executor chain.
	internalExecutorCtx context.Context
	// Internal use only. The cancellation function for the internal context
	// used by the executor chain.
	internalExecutorCtxCancel context.CancelFunc

	// Internal use only. The original context that was passed into the
	// top-level executor in the chain.
	originalCtx context.Context
}

/*
func new[
	InputChanType any,
	OutputChanType any,
	OutputFuncType any,
](ctx context.Context, input *executorInput[InputChanType, OutputChanType, chan<- any, any]) {
	if ctx == nil {
		ctx = context.Background()
	}
	if input.Name == "" {
		panic("input.Name cannot be an empty string")
	}
	if input.Concurrency == 0 {
		panic("input.Concurrency must be greater than 0")
	}
	if input.InputChannel == nil {
		panic("input.InputChannel cannot be nil")
	}
	if input.Func == nil {
		panic("input.Func cannot be nil")
	}

	outputChannelSize := zeroDefault(input.OutputChannelSize, 2*input.Concurrency)

	emptyInputChannelCallbackInterval := zeroDefault(input.EmptyInputChannelCallbackInterval, DefaultEmptyInputChannelCallbackInterval)

	fullOutputChannelCallbackInterval := zeroDefault(input.FullOutputChannelCallbackInterval, DefaultFullOutputChannelCallbackInterval)

	outputChan := input.OutputChannel
	if outputChan == nil {
		outputChan := make(OutputChanType, outputChannelSize)
	}
}
*/
