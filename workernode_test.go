package watchback

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"
)

func newDefaultNodeMessengingTimingConfigForTest_0() (cfg *NodeMessagingTimingConfig) {
	return &NodeMessagingTimingConfig{
		FlexOnServiceCheckPeriod:             0,
		ExpectOnServiceQueryWithin:           0,
		ExpectServiceActivationRequestWithin: 0,
		ExpectMessengerCloseWithin:           0,
	}
}

func newDefaultNodeMessengingTimingConfigForTest_1() (cfg *NodeMessagingTimingConfig) {
	return &NodeMessagingTimingConfig{
		FlexOnServiceCheckPeriod:             time.Second,
		ExpectOnServiceQueryWithin:           time.Second,
		ExpectServiceActivationRequestWithin: time.Second,
		ExpectMessengerCloseWithin:           time.Second,
	}
}

func newDefaultNodeMessengingTimingConfigForTest_2() (cfg *NodeMessagingTimingConfig) {
	return &NodeMessagingTimingConfig{
		FlexOnServiceCheckPeriod:             time.Second + (maxNodeMessagingOperationAttempt * (ExpiredCallableResultCollectPeriod + time.Second)),
		ExpectOnServiceQueryWithin:           time.Second,
		ExpectServiceActivationRequestWithin: time.Second,
		ExpectMessengerCloseWithin:           time.Second,
	}
}

func validate_Advices(t *testing.T, expectation map[string]bool, advices []string) {
	unexpected := make([]string, 0)
	for _, adv := range advices {
		_, ok := expectation[adv]
		if ok {
			expectation[adv] = true
		} else {
			unexpected = append(unexpected, adv)
		}
	}
	for adv, result := range expectation {
		if false == result {
			t.Errorf("expecting to have \"%v\" as one of advices", adv)
		}
	}
	if len(unexpected) > 0 {
		t.Errorf("caught unexpected advices: %v", unexpected)
	}
}

func TestNodeMessagingTimingConfig_Advice0(t *testing.T) {
	var expectation = map[string]bool{
		AdviceNodeMessagingTimingFlexOnServiceCheckPeriodTooSmall:                           false,
		AdviceNodeMessagingTimingExpectOnServiceQueryWithinTooSmall:                         false,
		AdviceNodeMessagingTimingExpectServiceActivationRequestWithinTooSmall:               false,
		AdviceNodeMessagingTimingExpectMessengerCloseWithinTooSmall:                         false,
		AdviceNodeMessagingTimingFlexOnServiceCheckLessThanSamllestMessagingFailureDuration: false,
	}
	advices := newDefaultNodeMessengingTimingConfigForTest_0().Advice()
	validate_Advices(t, expectation, advices)
}

func TestNodeMessagingTimingConfig_Advice1(t *testing.T) {
	cfg := &NodeMessagingTimingConfig{
		FlexOnServiceCheckPeriod:             time.Second,
		ExpectOnServiceQueryWithin:           time.Second * 2,
		ExpectServiceActivationRequestWithin: time.Second,
		ExpectMessengerCloseWithin:           time.Second,
	}
	var expectation = map[string]bool{
		AdviceNodeMessagingTimingFlexOnServiceCheckLessThanExpectOnServiceQuery:             false,
		AdviceNodeMessagingTimingFlexOnServiceCheckLessThanSamllestMessagingFailureDuration: false,
	}
	advices := cfg.Advice()
	validate_Advices(t, expectation, advices)
}

func TestNodeMessagingTimingConfig_Advice2(t *testing.T) {
	var expectation = map[string]bool{}
	advices := newDefaultNodeMessengingTimingConfigForTest_2().Advice()
	validate_Advices(t, expectation, advices)
}

func TestWorkerNode_NodeId(t *testing.T) {
	n := newWorkerNode(3, nil, newDefaultNodeMessengingTimingConfigForTest_0())
	if nodeId := n.NodeId(); nodeId != 3 {
		t.Errorf("unexpected node id: %v", nodeId)
	}
}

type mockNodeMessagingAdapter_AllError struct {
	errInstance error

	countHasMessagingFailure              int32
	countIsOnService                      int32
	countRequestServiceActivationApproval int32
	countClose                            int32
}

func newMockNodeMessagingAdapter_AllError(errInstanceText string) (m *mockNodeMessagingAdapter_AllError) {
	return &mockNodeMessagingAdapter_AllError{
		errInstance:              errors.New(errInstanceText),
		countHasMessagingFailure: 0,
	}
}

func (m *mockNodeMessagingAdapter_AllError) HasMessagingFailure(err error) {
	m.countHasMessagingFailure++
}

func (m *mockNodeMessagingAdapter_AllError) IsOnService(ctx context.Context) (onService bool, err error) {
	m.countIsOnService++
	return false, m.errInstance
}

func (m *mockNodeMessagingAdapter_AllError) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	m.countRequestServiceActivationApproval++
	return false, m.errInstance

}
func (m *mockNodeMessagingAdapter_AllError) Close(ctx context.Context) (err error) {
	m.countClose++
	return m.errInstance
}

func TestWorkerNode_IsOnService_allError1(t *testing.T) {
	mock := newMockNodeMessagingAdapter_AllError("allerror-1")
	n := newWorkerNode(3, mock, newDefaultNodeMessengingTimingConfigForTest_1())
	go n.RunMessagingLoop()
	running, err := n.IsOnService()
	if ErrExceedMaxWorkerNodeMessagingOperationAttempt != err {
		t.Errorf("expect ErrExceedMaxWorkerNodeMessagingOperationAttempt but get: %v", err)
	}
	if running {
		t.Errorf("expect false running: %v", running)
	}
	n.Close()
	if maxNodeMessagingOperationAttempt != mock.countIsOnService {
		t.Errorf("expect countIsOnService=%v: %v", maxNodeMessagingOperationAttempt, mock.countIsOnService)
	}
	if maxNodeMessagingOperationAttempt != mock.countHasMessagingFailure {
		t.Errorf("expect countHasMessagingFailure=%v: %v", maxNodeMessagingOperationAttempt, mock.countHasMessagingFailure)
	}
	if mock.countClose != 1 {
		t.Errorf("expect countClose=1: %v", mock.countClose)
	}
}

type mockNodeMessagingAdapter_C1 struct {
	sleepPeriod  time.Duration
	resultBool1  bool
	errInstance1 error

	paramBool1   bool
	paramInt32b1 int32

	countHasMessagingFailure int32
}

func (m *mockNodeMessagingAdapter_C1) doSleep() {
	if m.sleepPeriod > 0 {
		time.Sleep(m.sleepPeriod)
	}
}

func (m *mockNodeMessagingAdapter_C1) setError(enable bool) {
	if enable {
		m.errInstance1 = errors.New("mock error for mockNodeMessagingAdapter_C1")
	} else {
		m.errInstance1 = nil
	}
}

func (m *mockNodeMessagingAdapter_C1) HasMessagingFailure(err error) {
	m.countHasMessagingFailure++
}

func (m *mockNodeMessagingAdapter_C1) IsOnService(ctx context.Context) (onService bool, err error) {
	m.doSleep()
	return m.resultBool1, m.errInstance1
}

func (m *mockNodeMessagingAdapter_C1) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	m.paramInt32b1 = requesterNodeId
	m.paramBool1 = forceActivation
	m.doSleep()
	return m.resultBool1, m.errInstance1
}

func (m *mockNodeMessagingAdapter_C1) Close(ctx context.Context) (err error) {
	return nil
}

func newMockNodeMessagingAdapter_C1() (m *mockNodeMessagingAdapter_C1) {
	return &mockNodeMessagingAdapter_C1{
		sleepPeriod:  0,
		resultBool1:  false,
		errInstance1: nil,
	}
}

func validate_IsOnService(t *testing.T, n *WorkerNode, t0 time.Time, stepName string, expectRunning bool, expectError bool) {
	running, err := n.IsOnService()
	cost := time.Since(t0)
	log.Printf("cost until %v: %v", stepName, cost)
	if expectRunning != running {
		t.Fatalf("unexpect result of %v (expect running: %v): %v (err=%v)", stepName, expectRunning, running, err)
	}
	if expectError {
		if nil == err {
			t.Fatalf("expect error on %v: %v", stepName, err)
		}
	} else {
		if nil != err {
			t.Fatalf("expect no error on %v: %v", stepName, err)
		}
	}
}

func TestWorkerNode_IsOnService_c1a(t *testing.T) {
	mock := newMockNodeMessagingAdapter_C1()
	n := newWorkerNode(3, mock, newDefaultNodeMessengingTimingConfigForTest_2())
	go n.RunMessagingLoop()
	defer n.Close()
	t0 := time.Now()
	validate_IsOnService(t, n, t0, "IsOnService_onService1:1-not-running", false, false)
	mock.resultBool1 = true
	validate_IsOnService(t, n, t0, "IsOnService_onService1:2-running", true, false)
	mock.resultBool1 = false
	mock.setError(true)
	validate_IsOnService(t, n, t0, "IsOnService_onService1:3-on-error", true, true)
	mock.setError(false)
	mock.sleepPeriod = time.Second * 5
	validate_IsOnService(t, n, t0, "IsOnService_onService1:4-long-operation-but-in-flex", true, true)
	time.Sleep(time.Microsecond * 500)
	validate_IsOnService(t, n, t0, "IsOnService_onService1:5-long-operation-out-flex", false, true)
	mock.resultBool1 = true
	mock.setError(false)
	mock.sleepPeriod = 0
	validate_IsOnService(t, n, t0, "IsOnService_onService1:6-running", true, false)
	mock.resultBool1 = false
	validate_IsOnService(t, n, t0, "IsOnService_onService1:7-not-running", false, false)
	mock.resultBool1 = true
	mock.setError(true)
	validate_IsOnService(t, n, t0, "IsOnService_onService1:8-on-error", false, true)
}

func validate_RequestServiceActivationApproval(t *testing.T, n *WorkerNode, mock *mockNodeMessagingAdapter_C1, stepName string, sleepPeriod time.Duration, requesterNodeId int32, forceActivation, mockAccept, mockError, expectError bool) {
	mock.sleepPeriod = sleepPeriod
	mock.resultBool1 = mockAccept
	mock.setError(mockError)
	isApprove, err := n.RequestServiceActivationApproval(requesterNodeId, forceActivation)
	if mockAccept != isApprove {
		t.Errorf("unexpect accept status on %v: %v != %v", stepName, mockAccept, isApprove)
	}
	if (nil == err) && (true == expectError) {
		t.Errorf("expecting error but no error occur on %v.", stepName)
	}
	if (nil != err) && (false == expectError) {
		t.Errorf("expecting no error but error occurs on %v: %v.", stepName, err)
	}
	if mock.paramInt32b1 != requesterNodeId {
		t.Errorf("passed parameter 1 (requesterNodeId) not consist on %v: %v != %v", stepName, requesterNodeId, mock.paramInt32b1)
	}
	if mock.paramBool1 != forceActivation {
		t.Errorf("passed parameter 2 (forceActivation) not consist on %v: %v != %v", stepName, forceActivation, mock.paramBool1)
	}
}

func TestWorkerNode_RequestServiceActivationApproval_c1a(t *testing.T) {
	mock := newMockNodeMessagingAdapter_C1()
	n := newWorkerNode(3, mock, newDefaultNodeMessengingTimingConfigForTest_2())
	go n.RunMessagingLoop()
	defer n.Close()
	validate_RequestServiceActivationApproval(t, n, mock, "RequestServiceActivationApproval-1-success", 0, 2, false, true, false, false)
	validate_RequestServiceActivationApproval(t, n, mock, "RequestServiceActivationApproval-2-reject", 0, 2, false, false, false, false)
	validate_RequestServiceActivationApproval(t, n, mock, "RequestServiceActivationApproval-3-force", 0, 2, true, true, false, false)
	validate_RequestServiceActivationApproval(t, n, mock, "RequestServiceActivationApproval-4-exception", 0, 2, false, false, true, true)
	validate_RequestServiceActivationApproval(t, n, mock, "RequestServiceActivationApproval-5-timeout", time.Second*5, 2, false, false, false, true)
}
