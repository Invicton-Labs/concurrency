package concurrency

import "context"

// This is a context that allows accessing the values of the inner
// (wrapped) context, but does not get cancelled if the inner context
// gets cancelled.
type executorContext struct {
	context.Context
	WrappedCtx context.Context
}

func newExecutorContext(ctx context.Context) (context.Context, context.CancelFunc) {
	newCtx, newCtxCancel := context.WithCancel(context.Background())
	return &executorContext{
		Context:    newCtx,
		WrappedCtx: ctx,
	}, newCtxCancel
}

func (uc *executorContext) Value(key any) any {
	return uc.WrappedCtx.Value(key)
}
