package concurrency

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type routineStatus int

const (
	AwaitingInput routineStatus = iota
	Processing
	AwaitingOutput
	Errored
	ContextDone
	Finished
)

func (s routineStatus) String() string {
	switch s {
	case AwaitingInput:
		return "AwaitingInput"
	case Processing:
		return "Processing"
	case AwaitingOutput:
		return "AwaitingOutput"
	case Errored:
		return "Errored"
	case ContextDone:
		return "ContextDone"
	case Finished:
		return "Finished"
	default:
		panic(fmt.Errorf("unkown routineStatus: %d", s))
	}
}

type RoutineStatusTracker struct {
	// Internal use only. The name of the executor it came from.
	executorName string
	// Internal use only. A map for tracking the current state of each routine.
	routineStatuses sync.Map
	// Internal use only. A counter for the number of running routines.
	numRoutinesRunning int32
	// Internal use only. A counter for the number of routines awaiting an input.
	numRoutinesAwaitingInput int32
	// Internal use only. A counter for the number of routines that are currently processing an input.
	numRoutinesProcessingInput int32
	// Internal use only. A counter for the number of routines that are currently awaiting a slot to store an output.
	numRoutinesAwaitingOutput int32
	// Internal use only. A counter for the number of routines that have errored and exited.
	numRoutinesContextDone int32
	// Internal use only. A counter for the number of routines that have exited because the context was cancelled.
	numRoutinesErrored int32
	// Internal use only. A counter for the number of routines that have successfully finished and exited.
	numRoutinesFinished int32
	// Internal use only. A function that retrieves the length of the input channel. We
	// use a function instead of storing a reference to the channel itself because the channel
	// could have many different types, and we don't want to have to deal with those generics
	// here.
	getInputChanLength func() int
	// Internal use only. A function that retrieves the length of the output channel. We
	// use a function instead of storing a reference to the channel itself because the channel
	// could have many different types, and we don't want to have to deal with those generics
	// here.
	getOutputChanLength func() *int
}

func (upo *RoutineStatusTracker) updateRoutineStatus(routineIdx uint, newStatus routineStatus) (isLastRoutine bool) {
	previousStatusInterface, ok := upo.routineStatuses.Load(routineIdx)
	if ok {
		// If it already had a previously tracked state, decrement the corresponding counter for that state
		previousStatus := previousStatusInterface.(routineStatus)
		// If the status isn't actually changing, do nothing
		if previousStatus == newStatus {
			return
		}
		switch previousStatus {
		case AwaitingInput:
			atomic.AddInt32(&upo.numRoutinesAwaitingInput, -1)
		case Processing:
			atomic.AddInt32(&upo.numRoutinesProcessingInput, -1)
		case AwaitingOutput:
			atomic.AddInt32(&upo.numRoutinesAwaitingOutput, -1)
		case Errored:
			panic(fmt.Errorf("cannot update the status of routine with index %d to state %s after it has already been set to %s state", routineIdx, newStatus.String(), previousStatus.String()))
		case ContextDone:
			panic(fmt.Errorf("cannot update the status of routine with index %d to state %s after it has already been set to %s state", routineIdx, newStatus.String(), previousStatus.String()))
		case Finished:
			panic(fmt.Errorf("cannot update the status of routine with index %d to state %s after it has already been set to %s state", routineIdx, newStatus.String(), previousStatus.String()))
		default:
			panic(fmt.Errorf("unknown routine status: %d", previousStatus))
		}
	}
	upo.routineStatuses.Store(routineIdx, newStatus)
	// Now update the counter corresponding to the new state
	switch newStatus {
	case AwaitingInput:
		atomic.AddInt32(&upo.numRoutinesAwaitingInput, 1)
	case Processing:
		atomic.AddInt32(&upo.numRoutinesProcessingInput, 1)
	case AwaitingOutput:
		atomic.AddInt32(&upo.numRoutinesAwaitingOutput, 1)
	case Errored:
		atomic.AddInt32(&upo.numRoutinesErrored, 1)
		// If it's errored, it's done, so reduce the number of running routines
		if atomic.AddInt32(&upo.numRoutinesRunning, -1) == 0 {
			isLastRoutine = true
		}
	case ContextDone:
		atomic.AddInt32(&upo.numRoutinesContextDone, 1)
		// If it's errored, it's done, so reduce the number of running routines
		if atomic.AddInt32(&upo.numRoutinesRunning, -1) == 0 {
			isLastRoutine = true
		}
	case Finished:
		atomic.AddInt32(&upo.numRoutinesFinished, 1)
		// If it's finished, it's done, so reduce the number of running routines
		if atomic.AddInt32(&upo.numRoutinesRunning, -1) == 0 {
			isLastRoutine = true
		}
	default:
		panic(fmt.Errorf("unknown routine status: %d", newStatus))
	}
	return
}

func (rst *RoutineStatusTracker) GetExecutorName() string {
	return rst.executorName
}
func (rst *RoutineStatusTracker) GetNumRoutinesRunning() int32 {
	return atomic.LoadInt32(&rst.numRoutinesRunning)
}
func (rst *RoutineStatusTracker) GetNumRoutinesAwaitingInput() int32 {
	return atomic.LoadInt32(&rst.numRoutinesAwaitingInput)
}
func (rst *RoutineStatusTracker) GetNumRoutinesProcessing() int32 {
	return atomic.LoadInt32(&rst.numRoutinesProcessingInput)
}
func (rst *RoutineStatusTracker) GetNumRoutinesAwaitingOutput() int32 {
	return atomic.LoadInt32(&rst.numRoutinesAwaitingOutput)
}
func (rst *RoutineStatusTracker) GetNumRoutinesErrored() int32 {
	return atomic.LoadInt32(&rst.numRoutinesErrored)
}
func (rst *RoutineStatusTracker) GetNumRoutinesContextDone() int32 {
	return atomic.LoadInt32(&rst.numRoutinesContextDone)
}
func (rst *RoutineStatusTracker) GetNumRoutinesFinished() int32 {
	return atomic.LoadInt32(&rst.numRoutinesFinished)
}
func (rst *RoutineStatusTracker) GetInputChanLength() int {
	return rst.getInputChanLength()
}

// Returns nil if there is no output channel, or a pointer to an int if there is
func (rst *RoutineStatusTracker) GetOutputChanLength() *int {
	return rst.getOutputChanLength()
}
