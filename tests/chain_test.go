package tests

/*
func TestExecutorChain(t *testing.T) {
	ctx := context.Background()
	var count int = 1000
	inputChan := make(chan int, count)
	for i := 1; i <= count; i++ {
		inputChan <- i
	}
	close(inputChan)
	executor1 := concurrency.Executor(ctx, concurrency.ExecutorInput[int, uint]{
		Name:              "test-executor-chain-1",
		Concurrency:       1,
		OutputChannelSize: count * 2,
		InputChannel:      inputChan,
		Func: func(ctx context.Context, input int, metadata *concurrency.RoutineFunctionMetadata) (output uint, err error) {
			if input > 500 {
				return 0, fmt.Errorf("test-error")
			}
			return uint(input), nil
		},
		EmptyInputChannelCallbackInterval: 1 * time.Second,
		EmptyInputChannelCallback: func(input *concurrency.EmptyInputChannelCallbackInput) error {
			fmt.Printf("%s routine %d has received no input after %dms\n", input.ExecutorName, input.RoutineIndex, input.TimeSinceLastInput.Milliseconds())
			return nil
		},
		FullOutputChannelCallbackInterval: 1 * time.Second,
		FullOutputChannelCallback: func(input *concurrency.FullOutputChannelCallbackInput) error {
			fmt.Printf("%s routine %d has not been able to output for %dms\n", input.ExecutorName, input.RoutineIndex, input.TimeSinceLastOutput.Milliseconds())
			return nil
		},
	})
	//executor2 := concurrency.ChainFinal[InputType any](upstream *ExecutorOutput[InputType], input ExecutorInputFinal[InputType, any])
	err := executor.Wait()
	if err == nil {
		t.Errorf("Expected an error, received none")
		return
	}
	if err.Error() != "test-error" {
		t.Errorf("Received unexpected error string: %s", err.Error())
		return
	}
}
*/
