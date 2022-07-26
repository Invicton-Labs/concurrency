# concurrency
Concurrent/parallel processing for Go.

Desired features:
- result batching
- result unbatching
- optional cancellation of function
- single input object
- chaining
- final (no chaining)
- single internal context for all executors in a chain?
- 

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
- 