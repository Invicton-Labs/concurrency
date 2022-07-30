# concurrency
Concurrent/parallel processing for Go.

Desired features:
- result batching (with timeout for sending incomplete batch)
- result unbatching
- single input object
- chaining
- final (no chaining)
- single internal context for all executors in a chain?
- multiple input to a single output (2-to-1 chaining)

Needs:
- Executor
- ExecutorBatch
- ExecutorUnbatch
- ExecutorFinal
- Chain
- ChainBatch
- ChainUnbatch
- ChainFinal
- IntoSlice
- Continuous
- ContinuousFinal

When an error occurs (no continued processing):
- Save the error internally for eventual exiting
- Cancel the internal context
- Close the output channel IF it was created internally
- Internal context cancellation causes all routines in all executors to exit
- Original error gets passed down and out through end of chain

When an error occurs (continued processing):
- Save the error internally for eventual exiting
- Cancel the internal context
- Keep processing upstream inputs until the channel is closed or empty
- Exit all routines in executor as inputs are 