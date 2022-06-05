package watchback

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func mockCallableNoop(ctx context.Context) (err error) {
	return nil
}

type mockCallable1 struct {
	finished chan error
}

func newMockCallable1() (m *mockCallable1) {
	return &mockCallable1{
		finished: make(chan error, 1),
	}
}

/*
func (m *mockCallable1) wait() {
	select {
	case <-m.finished:
	case <-time.After(time.Second * 15):
		fmt.Println("WARN: wait over 15 seconds. left wait() method.")
	}
}
*/

func (m *mockCallable1) noop(ctx context.Context) (err error) {
	m.finished <- nil
	return nil
}

func (m *mockCallable1) sleep10_wo_context(ctx context.Context) (err error) {
	time.Sleep(time.Second * 10)
	m.finished <- nil
	return nil
}

func (m *mockCallable1) sleep10_w_context(ctx context.Context) (err error) {
	select {
	case <-time.After(time.Second * 10):
		fmt.Println("after 10 seconds (sleep10_w_context)")
	case <-ctx.Done():
		fmt.Println("context done")
		return errors.New("time-out")
	}
	m.finished <- nil
	return nil
}

func validateCallableInvokeResult(t *testing.T, callableName string, mockFinishCh <-chan error, ctx context.Context, resultCh <-chan error, expectErrorResult, expectMockNoFinish, expectContextDoneEarly, expectNoResult bool) {
	select {
	case err, ok := <-mockFinishCh:
		if nil != err {
			if !expectErrorResult {
				t.Errorf("result from %v not as expected at select-1: err=%v (not expecting error)", callableName, err)
			}
		} else {
			if expectErrorResult {
				t.Errorf("result from %v not as expected at select-1: err=%v (expecting error)", callableName, err)
			}
		}
		if true != ok {
			if !expectMockNoFinish {
				t.Errorf("result fetching of %v not as expected: ok=%v (expect false)", callableName, ok)
			}
		} else {
			if expectMockNoFinish {
				t.Errorf("result fetching of %v not as expected: ok=%v (expect true)", callableName, ok)
			}
		}
	case <-ctx.Done():
		if !expectContextDoneEarly {
			t.Errorf("context declares done before %v", callableName)
		}
	}
	select {
	case err := <-resultCh:
		if nil != err {
			if !expectErrorResult {
				t.Errorf("result from %v not as expected at select-2: err=%v (not expecting error)", callableName, err)
			}
		} else {
			if expectErrorResult {
				t.Errorf("result from %v not as expected at select-2: err=%v (expecting error)", callableName, err)
			}
		}
	case <-time.After(time.Second * 5):
		if !expectNoResult {
			t.Errorf("cannot have result from result-channel of %v bundle", callableName)
		}
	}
}

func validateCallableResult(t *testing.T, callableName string, resultCh <-chan error, within time.Duration, expectError bool) {
	select {
	case err := <-resultCh:
		if nil != err {
			if !expectError {
				t.Errorf("result from %v not as expected at select-1: err=%v (not expecting error)", callableName, err)
			}
		} else {
			if expectError {
				t.Errorf("result from %v not as expected at select-1: err=%v (expecting error)", callableName, err)
			}
		}
	case <-time.After(within):
		t.Errorf("result of %v no ready within %v", callableName, within)
	}
}

func TestRunner_Run_normal1(t *testing.T) {
	runner := newCallableBundleRunner(3)
	go runner.RunLoop()
	mock := newMockCallable1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	retCh, err := runner.AddCallableWithContext(mock.noop, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable: %v", err)
	}
	runner.Close()
	validateCallableInvokeResult(t, "Run_normal1:mock-noop", mock.finished, ctx, retCh, false, false, false, false)
}

func TestRunner_Run_normal3(t *testing.T) {
	runner := newCallableBundleRunner(3)
	mock := newMockCallable1()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	retCh1, err := runner.AddCallableWithContext(mock.noop, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 1: %v", err)
	}
	retCh2, err := runner.AddCallableWithContext(mock.noop, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 2: %v", err)
	}
	retCh3, err := runner.AddCallableWithContext(mock.noop, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 3: %v", err)
	}
	retCh4, err := runner.AddCallableWithContext(mock.noop, ctx)
	if nil == err {
		t.Errorf("shall not be able to feed callable 4: %v", err)
	}
	if nil != retCh4 {
		t.Errorf("result channel of callable 4 shall not be available: %v", retCh4)
	}
	go runner.RunLoop()
	runner.Close()
	validateCallableInvokeResult(t, "Run_normal3:mock-noop-1", mock.finished, ctx, retCh1, false, false, false, false)
	validateCallableInvokeResult(t, "Run_normal3:mock-noop-2", mock.finished, ctx, retCh2, false, false, false, false)
	validateCallableInvokeResult(t, "Run_normal3:mock-noop-3", mock.finished, ctx, retCh3, false, false, false, false)
}

func TestRunner_Run_endless1(t *testing.T) {
	runner := newCallableBundleRunner(3)
	go runner.RunLoop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	mock1 := newMockCallable1()
	retCh1, err := runner.AddCallableWithContext(mock1.noop, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 1: %v", err)
	}
	mock2 := newMockCallable1()
	retCh2, err := runner.AddCallableWithContext(mock2.sleep10_wo_context, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 2: %v", err)
	}
	mock3 := newMockCallable1()
	retCh3, err := runner.AddCallableWithContext(mock3.sleep10_w_context, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 3: %v", err)
	}
	runner.Close()
	validateCallableInvokeResult(t, "Run_endless1:mock-1-noop", mock1.finished, ctx, retCh1, false, false, false, false)
	validateCallableInvokeResult(t, "Run_endless1:mock-2-sleep10_wo_context", mock2.finished, ctx, retCh2, true, true, true, false)
	select {
	case err3 := <-retCh3:
		if nil == err3 {
			t.Errorf("expecting timeout failure: err=%v", err3)
		}
	case <-time.After(time.Second * 3):
		t.Errorf("not getting result on time")
	}
}

func TestRunner_Run_endless2(t *testing.T) {
	runner := newCallableBundleRunner(3)
	go runner.RunLoop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	mock1 := newMockCallable1()
	retCh1, err := runner.AddCallableWithContext(mock1.noop, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 1: %v", err)
	}
	mock2 := newMockCallable1()
	retCh2, err := runner.AddCallableWithContext(mock2.sleep10_wo_context, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 2: %v", err)
	}
	mock3 := newMockCallable1()
	retCh3, err := runner.AddCallableWithContext(mock3.sleep10_w_context, ctx)
	if nil != err {
		t.Fatalf("cannot feed callable 3: %v", err)
	}
	runner.Close()
	within := time.Second * 6 // context: 2 + runner-wait: 3 = take 5 seconds to get result
	validateCallableResult(t, "Run_endless2:mock-1-noop", retCh1, within, false)
	validateCallableResult(t, "Run_endless2:mock-2-sleep10_wo_context", retCh2, within, true)
	validateCallableResult(t, "Run_endless2:mock-3-sleep10_w_context", retCh3, within, true)
}

func TestRunner_Run_timeoutOverFeed(t *testing.T) {
	runner := newCallableBundleRunner(2)
	retCh1, cancel1, err := runner.AddCallableWithTimeout(mockCallableNoop, time.Second)
	if nil != err {
		t.Fatalf("cannot feed callable 1: %v", err)
	}
	defer cancel1()
	retCh2, cancel2, err := runner.AddCallableWithTimeout(mockCallableNoop, time.Second)
	if nil != err {
		t.Fatalf("cannot feed callable 2: %v", err)
	}
	defer cancel2()
	retCh3, cancel3, err := runner.AddCallableWithTimeout(mockCallableNoop, time.Second)
	if nil == err {
		t.Errorf("shall not be able to feed callable 3: %v", err)
		defer cancel3()
	}
	if nil != retCh3 {
		t.Errorf("result channel of callable 3 shall not be available")
	}
	if nil != cancel3 {
		t.Errorf("cancel function of callable 3 shall not be available")
	}
	go runner.RunLoop()
	runner.Close()
	within := time.Second * 3
	validateCallableResult(t, "Run_timeoutOverFeed:mock-1-noop", retCh1, within, false)
	validateCallableResult(t, "Run_timeoutOverFeed:mock-2-noop", retCh2, within, false)
	if nil != retCh3 {
		validateCallableResult(t, "Run_timeoutOverFeed:mock-3-noop", retCh3, within, false)
	}
}

func TestRunner_Run_timeout2(t *testing.T) {
	runner := newCallableBundleRunner(3)
	go runner.RunLoop()
	mock1 := newMockCallable1()
	retCh1, cancel1, err := runner.AddCallableWithTimeout(mock1.noop, time.Second)
	if nil != err {
		t.Fatalf("cannot feed callable 1: %v", err)
	}
	defer cancel1()
	mock2 := newMockCallable1()
	retCh2, cancel2, err := runner.AddCallableWithTimeout(mock2.sleep10_wo_context, time.Second)
	if nil != err {
		t.Fatalf("cannot feed callable 2: %v", err)
	}
	defer cancel2()
	mock3 := newMockCallable1()
	retCh3, cancel3, err := runner.AddCallableWithTimeout(mock3.sleep10_w_context, time.Second)
	if nil != err {
		t.Fatalf("cannot feed callable 3: %v", err)
	}
	defer cancel3()
	runner.Close()
	within := time.Second * 5 // timeout: 1 + runner-wait: 3 + extra-second-for-safe: 1
	validateCallableResult(t, "Run_timeout2:mock-1-noop", retCh1, within, false)
	validateCallableResult(t, "Run_timeout2:mock-2-sleep10_wo_context", retCh2, within, true)
	validateCallableResult(t, "Run_timeout2:mock-3-sleep10_w_context", retCh3, within, true)
}
