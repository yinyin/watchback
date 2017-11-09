package watchback

import (
	"context"
	"testing"
	"time"
)

// Mock NodeMessagingAdapter for no operation
type mockNodeMessagerNoop struct {
	nodeId int32
}

func (m *mockNodeMessagerNoop) HasMessagingFailure(err error) {
}

func (m *mockNodeMessagerNoop) IsOnService(ctx context.Context) (onService bool, err error) {
	return false, nil
}

func (m *mockNodeMessagerNoop) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	return true, nil
}

func (m *mockNodeMessagerNoop) Close(ctx context.Context) (err error) {
	return nil
}

func newMockNodeMessagerNoop(nodeId int32) (m *mockNodeMessagerNoop) {
	return &mockNodeMessagerNoop{
		nodeId: nodeId,
	}
}

// Mock NodeMessagingAdapter for no operation
type mockNodeMessagerClock struct {
	nodeId int32

	lastHasMessagingFailureAt            time.Time
	lastIsOnService                      time.Time
	lastRequestServiceActivationApproval time.Time
	lastClose                            time.Time
}

func (m *mockNodeMessagerClock) HasMessagingFailure(err error) {
	m.lastHasMessagingFailureAt = time.Now()
}

func (m *mockNodeMessagerClock) IsOnService(ctx context.Context) (onService bool, err error) {
	m.lastIsOnService = time.Now()
	return false, nil
}

func (m *mockNodeMessagerClock) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	m.lastRequestServiceActivationApproval = time.Now()
	return true, nil
}

func (m *mockNodeMessagerClock) Close(ctx context.Context) (err error) {
	m.lastClose = time.Now()
	return nil
}

func newMockNodeMessagerClock(nodeId int32) (m *mockNodeMessagerClock) {
	return &mockNodeMessagerClock{
		nodeId: nodeId,
	}
}

func validate_ServiceRack_AddNode_result(t *testing.T, expNodeId int32, expError bool, workerNode *WorkerNode, resultErr error) {
	// validate Node-Id and Messenger
	if expNodeId < 0 {
		if workerNode != nil {
			t.Errorf("expect to have empty WorkerNode but having %v instead.")
		}
	} else if nil == workerNode {
		t.Errorf("expect to have WorkerNode with NodeId=%v but having empty one instead.", expNodeId)
	} else {
		if expNodeId != workerNode.nodeId {
			t.Errorf("expect to have WorkerNode with NodeId=%v but got NodeId=%v instead.", expNodeId, workerNode.nodeId)
		}
		if nil == workerNode.messenger {
			t.Errorf("expect to have WorkerNode NodeId=%v with given messenger but messenger is losted.", expNodeId)
		} else if resultMessenger, ok := workerNode.messenger.(*mockNodeMessagerNoop); false == ok {
			t.Errorf("cannot cast messenger to mock type: NodeId=%v", expNodeId)
		} else if nil != resultMessenger {
			if expNodeId != resultMessenger.nodeId {
				t.Errorf("expect to have messenger with NodeId=%v, but having %v instead.", expNodeId, resultMessenger)
			}
		}
	}
	// validate Error
	if expError {
		if nil == resultErr {
			t.Errorf("expecting to have error but resulted error is empty (Node-Id=%v).", expNodeId)
		}
	} else {
		if nil != resultErr {
			t.Errorf("expecting no error occurs but got error %v (Node-Id=%v).", resultErr, expNodeId)
		}
	}
}

func validate_ServiceRack_nodeCount(t *testing.T, serviceRack *ServiceRack, frontNodeCount int, allNodeCount int) {
	frontLen := len(serviceRack.frontNodes)
	if frontNodeCount != frontLen {
		t.Errorf("expect front node count %v but having %v", frontNodeCount, frontLen)
	}
	allLen := len(serviceRack.allNodes)
	if allNodeCount != allLen {
		t.Errorf("expect all node count %v but having %v", allNodeCount, allLen)
	}
}

func newDefaultServiceTimingConfigForTest_0() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{}
}

func newDefaultServiceTimingConfigForTest_1() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{
		AcceptablePreparePeriod:             time.Second,
		AcceptableOnServiceSelfCheckPeriod:  time.Second,
		AcceptableOffServiceSelfCheckPeriod: time.Second,
		AcceptableServiceActivationPeriod:   time.Second,
		AcceptableServiceReleasingPeriod:    time.Second,

		AcceptableOnServiceSelfCheckFailurePeriod:  time.Second,
		AcceptableOffServiceSelfCheckFailurePeriod: time.Second,
		AcceptableFrontNodeEmptyPeriod:             time.Second,

		OnServiceSelfCheckPeriod:  time.Second,
		OffServiceSelfCheckPeriod: time.Second,

		ServiceActivationFailureBlackoutPeriod: time.Second,
		ServiceReleaseSuccessBlackoutPeriod:    time.Second,
		ServiceReleaseFailureBlackoutPeriod:    time.Second,
	}
}

func newDefaultServiceTimingConfigForTest_2() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{
		AcceptablePreparePeriod:             time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod:  time.Second * 4,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 5,
		AcceptableServiceActivationPeriod:   time.Second * 5,
		AcceptableServiceReleasingPeriod:    time.Second * 7,

		AcceptableOnServiceSelfCheckFailurePeriod:  time.Second * 8,
		AcceptableOffServiceSelfCheckFailurePeriod: time.Second * 9,
		AcceptableFrontNodeEmptyPeriod:             time.Second * 10,

		OnServiceSelfCheckPeriod:  time.Second * 11,
		OffServiceSelfCheckPeriod: time.Second * 12,

		ServiceActivationFailureBlackoutPeriod: time.Second * 13,
		ServiceReleaseSuccessBlackoutPeriod:    time.Second * 14,
		ServiceReleaseFailureBlackoutPeriod:    time.Second * 15,
	}
}

func newDefaultServiceTimingConfigForTest_3() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{
		AcceptablePreparePeriod:             time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod:  time.Second * 4,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 5,
		AcceptableServiceActivationPeriod:   time.Second * 5,
		AcceptableServiceReleasingPeriod:    time.Second * 7,

		AcceptableOnServiceSelfCheckFailurePeriod:  time.Second * 8,
		AcceptableOffServiceSelfCheckFailurePeriod: time.Second * 9,
		AcceptableFrontNodeEmptyPeriod:             time.Second * 10,

		OnServiceSelfCheckPeriod:  time.Second * 23,
		OffServiceSelfCheckPeriod: time.Second * 33,

		ServiceActivationFailureBlackoutPeriod: time.Second * 13,
		ServiceReleaseSuccessBlackoutPeriod:    time.Second * 14,
		ServiceReleaseFailureBlackoutPeriod:    time.Second * 15,
	}
}

func newDefaultServiceTimingConfigForTest_A1() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{
		AcceptablePreparePeriod:             time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod:  time.Second * 4,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 5,
		AcceptableServiceActivationPeriod:   time.Second * 5,
		AcceptableServiceReleasingPeriod:    time.Second * 7,
		AcceptableControllerClosePeriod:     time.Second * 8,

		AcceptableOnServiceSelfCheckFailurePeriod:  time.Second * 2,
		AcceptableOffServiceSelfCheckFailurePeriod: time.Second * 2,
		AcceptableFrontNodeEmptyPeriod:             time.Second * 1,

		OnServiceSelfCheckPeriod:  time.Second * 11,
		OffServiceSelfCheckPeriod: time.Second * 12,

		ServiceActivationFailureBlackoutPeriod: time.Second * 13,
		ServiceReleaseSuccessBlackoutPeriod:    time.Second * 14,
		ServiceReleaseFailureBlackoutPeriod:    time.Second * 15,
	}
}

func newDefaultServiceTimingConfigForTest_A2() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{
		AcceptablePreparePeriod:             time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod:  time.Second * 4,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 5,
		AcceptableServiceActivationPeriod:   time.Second * 5,
		AcceptableServiceReleasingPeriod:    time.Second * 7,
		AcceptableControllerClosePeriod:     time.Second * 8,

		AcceptableOnServiceSelfCheckFailurePeriod:  time.Second * 2,
		AcceptableOffServiceSelfCheckFailurePeriod: time.Second * 2,
		AcceptableFrontNodeEmptyPeriod:             time.Second * 1,

		OnServiceSelfCheckPeriod:  time.Second * 2,
		OffServiceSelfCheckPeriod: time.Second * 2,

		ServiceActivationSuccessExemptionPeriod: time.Second * 1,
		ServiceActivationFailureBlackoutPeriod:  time.Second * 13,
		ServiceReleaseSuccessBlackoutPeriod:     time.Second * 14,
		ServiceReleaseFailureBlackoutPeriod:     time.Second * 15,
	}
}

func newDefaultServiceTimingConfigForTest_P1() (cfg *ServiceTimingConfig) {
	return &ServiceTimingConfig{
		AcceptablePreparePeriod:             time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod:  time.Second * 3,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 3,
		AcceptableServiceActivationPeriod:   time.Second * 3,
		AcceptableServiceReleasingPeriod:    time.Second * 3,

		AcceptableOnServiceSelfCheckFailurePeriod:  time.Second * 6,
		AcceptableOffServiceSelfCheckFailurePeriod: time.Second * 6,
		AcceptableFrontNodeEmptyPeriod:             time.Second * 10,

		OnServiceSelfCheckPeriod:  time.Second * 3,
		OffServiceSelfCheckPeriod: time.Second * 3,

		ServiceActivationSuccessExemptionPeriod: time.Second * 3,
		ServiceActivationFailureBlackoutPeriod:  time.Second * 10,
		ServiceReleaseSuccessBlackoutPeriod:     time.Second * 10,
		ServiceReleaseFailureBlackoutPeriod:     time.Second * 10,
	}
}

func validate_SameServiceTimingConfigContent(t *testing.T, cfg1 *ServiceTimingConfig, cfg2 *ServiceTimingConfig) {
	if cfg1.AcceptablePreparePeriod != cfg2.AcceptablePreparePeriod {
		t.Errorf("configuration value for AcceptablePreparePeriod is different: %v vs. %v",
			cfg1.AcceptablePreparePeriod, cfg2.AcceptablePreparePeriod)
	}
	if cfg1.AcceptableOnServiceSelfCheckPeriod != cfg2.AcceptableOnServiceSelfCheckPeriod {
		t.Errorf("configuration value for AcceptableOnServiceSelfCheckPeriod is different: %v vs. %v",
			cfg1.AcceptableOnServiceSelfCheckPeriod, cfg2.AcceptableOnServiceSelfCheckPeriod)
	}
	if cfg1.AcceptableOffServiceSelfCheckPeriod != cfg2.AcceptableOffServiceSelfCheckPeriod {
		t.Errorf("configuration value for AcceptableOffServiceSelfCheckPeriod is different: %v vs. %v",
			cfg1.AcceptableOffServiceSelfCheckPeriod, cfg2.AcceptableOffServiceSelfCheckPeriod)
	}
	if cfg1.AcceptableServiceActivationPeriod != cfg2.AcceptableServiceActivationPeriod {
		t.Errorf("configuration value for AcceptableServiceActivationPeriod is different: %v vs. %v",
			cfg1.AcceptableServiceActivationPeriod, cfg2.AcceptableServiceActivationPeriod)
	}
	if cfg1.AcceptableServiceReleasingPeriod != cfg2.AcceptableServiceReleasingPeriod {
		t.Errorf("configuration value for AcceptableServiceReleasingPeriod is different: %v vs. %v",
			cfg1.AcceptableServiceReleasingPeriod, cfg2.AcceptableServiceReleasingPeriod)
	}

	if cfg1.AcceptableOnServiceSelfCheckFailurePeriod != cfg2.AcceptableOnServiceSelfCheckFailurePeriod {
		t.Errorf("configuration value for AcceptableOnServiceSelfCheckFailurePeriod is different: %v vs. %v",
			cfg1.AcceptableOnServiceSelfCheckFailurePeriod, cfg2.AcceptableOnServiceSelfCheckFailurePeriod)
	}
	if cfg1.AcceptableOffServiceSelfCheckFailurePeriod != cfg2.AcceptableOffServiceSelfCheckFailurePeriod {
		t.Errorf("configuration value for AcceptableOffServiceSelfCheckFailurePeriod is different: %v vs. %v",
			cfg1.AcceptableOffServiceSelfCheckFailurePeriod, cfg2.AcceptableOffServiceSelfCheckFailurePeriod)
	}
	if cfg1.AcceptableFrontNodeEmptyPeriod != cfg2.AcceptableFrontNodeEmptyPeriod {
		t.Errorf("configuration value for AcceptableFrontNodeEmptyPeriod is different: %v vs. %v",
			cfg1.AcceptableFrontNodeEmptyPeriod, cfg2.AcceptableFrontNodeEmptyPeriod)
	}

	if cfg1.OnServiceSelfCheckPeriod != cfg2.OnServiceSelfCheckPeriod {
		t.Errorf("configuration value for OnServiceSelfCheckPeriod is different: %v vs. %v",
			cfg1.OnServiceSelfCheckPeriod, cfg2.OnServiceSelfCheckPeriod)
	}
	if cfg1.OffServiceSelfCheckPeriod != cfg2.OffServiceSelfCheckPeriod {
		t.Errorf("configuration value for OffServiceSelfCheckPeriod is different: %v vs. %v",
			cfg1.OffServiceSelfCheckPeriod, cfg2.OffServiceSelfCheckPeriod)
	}

	if cfg1.ServiceActivationFailureBlackoutPeriod != cfg2.ServiceActivationFailureBlackoutPeriod {
		t.Errorf("configuration value for ServiceActivationFailureBlackoutPeriod is different: %v vs. %v",
			cfg1.ServiceActivationFailureBlackoutPeriod, cfg2.ServiceActivationFailureBlackoutPeriod)
	}
	if cfg1.ServiceReleaseSuccessBlackoutPeriod != cfg2.ServiceReleaseSuccessBlackoutPeriod {
		t.Errorf("configuration value for ServiceReleaseSuccessBlackoutPeriod is different: %v vs. %v",
			cfg1.ServiceReleaseSuccessBlackoutPeriod, cfg2.ServiceReleaseSuccessBlackoutPeriod)
	}
	if cfg1.ServiceReleaseFailureBlackoutPeriod != cfg2.ServiceReleaseFailureBlackoutPeriod {
		t.Errorf("configuration value for ServiceReleaseFailureBlackoutPeriod is different: %v vs. %v",
			cfg1.ServiceReleaseFailureBlackoutPeriod, cfg2.ServiceReleaseFailureBlackoutPeriod)
	}
}

func TestServiceTimingConfig_CopyFrom(t *testing.T) {
	var cfg1 ServiceTimingConfig
	cfg2 := newDefaultServiceTimingConfigForTest_2()
	cfg1.copyFrom(cfg2)
	validate_SameServiceTimingConfigContent(t, &cfg1, cfg2)
}

func TestServiceTimingConfig_Advice0(t *testing.T) {
	var expectation = map[string]bool{
		AdviceAcceptablePreparePeriodTooShort:             false,
		AdviceAcceptableOnServiceSelfCheckPeriodTooShort:  false,
		AdviceAcceptableOffServiceSelfCheckPeriodTooShort: false,
		AdviceAcceptableServiceActivationPeriodTooShort:   false,
		AdviceAcceptableServiceReleasingPeriodTooShort:    false,
		AdviceAcceptableControllerClosePeriodTooShort:     false,
	}
	advices := newDefaultServiceTimingConfigForTest_0().Advice()
	validate_Advices(t, expectation, advices)
}

func TestServiceTimingConfig_Advice1(t *testing.T) {
	var expectation = map[string]bool{
		AdviceAcceptableOnServiceSelfCheckFailurePeriodShorterThanOperation:              false,
		AdviceAcceptableOffServiceSelfCheckFailurePeriodShorterThanOperation:             false,
		AdviceServiceActivationSuccessExemptionPeriodShorterThanCheck:                    false,
		AdviceServiceActivationFailureBlackoutPeriodShorterThanCheckAndActivateOperation: false,
		AdviceServiceReleaseSuccessBlackoutPeriodShorterThanCheckAndActivateOperation:    false,
		AdviceServiceReleaseFailureBlackoutPeriodShorterThanCheckAndActivateOperation:    false,
	}
	advices := newDefaultServiceTimingConfigForTest_A1().Advice()
	validate_Advices(t, expectation, advices)
}

func TestServiceTimingConfig_Advice2(t *testing.T) {
	var expectation = map[string]bool{
		AdviceAcceptableOnServiceSelfCheckFailurePeriodShorterThanOperation:  false,
		AdviceAcceptableOffServiceSelfCheckFailurePeriodShorterThanOperation: false,
		AdviceOnServiceSelfCheckPeriodShorterThanOperation:                   false,
		AdviceOffServiceSelfCheckPeriodShorterThanOperation:                  false,
		AdviceServiceActivationSuccessExemptionPeriodShorterThanCheck:        false,
	}
	advices := newDefaultServiceTimingConfigForTest_A2().Advice()
	validate_Advices(t, expectation, advices)
}

func TestServiceRack_AddNode_1normal(t *testing.T) {
	serviceRack := NewServiceRack(0, 2, nil, newDefaultServiceTimingConfigForTest_1())
	nodeMsgTimingCfg := newDefaultNodeMessengingTimingConfigForTest_0()
	node1, err1 := serviceRack.AddNode(1, newMockNodeMessagerNoop(1), nodeMsgTimingCfg)
	validate_ServiceRack_AddNode_result(t, 1, false, node1, err1)
	node2, err2 := serviceRack.AddNode(2, newMockNodeMessagerNoop(2), nodeMsgTimingCfg)
	validate_ServiceRack_AddNode_result(t, -1, false, node2, err2)
	node3, err3 := serviceRack.AddNode(3, newMockNodeMessagerNoop(3), nodeMsgTimingCfg)
	validate_ServiceRack_AddNode_result(t, 3, false, node3, err3)
	node3dup, err3dup := serviceRack.AddNode(3, newMockNodeMessagerNoop(3), nodeMsgTimingCfg)
	validate_ServiceRack_AddNode_result(t, -1, true, node3dup, err3dup)
	node2dup, err2dup := serviceRack.AddNode(2, newMockNodeMessagerNoop(2), nodeMsgTimingCfg)
	validate_ServiceRack_AddNode_result(t, -1, true, node2dup, err2dup)
	// check state of ServiceRack
	validate_ServiceRack_nodeCount(t, serviceRack, 1, 2)
}

var expect_RequestServiceActivationApprovalResult = [16]bool{
	true, true, true, true,
	false, true, true, true,
	true, true, true, true,
	false, false, true, true,
}

func demux_MockingRequestServiceActivationApprovalFactors(factorCode int32) (frontNode, forceActivation, localAvailable, isServicing, expectAccept bool) {
	if 0 != (factorCode & 0x01) {
		frontNode = true
	} else {
		frontNode = false
	}
	if 0 != (factorCode & 0x02) {
		forceActivation = true
	} else {
		forceActivation = false
	}
	if 0 != (factorCode & 0x04) {
		localAvailable = true
	} else {
		localAvailable = false
	}
	if 0 != (factorCode & 0x08) {
		isServicing = true
	} else {
		isServicing = false
	}
	expectAccept = expect_RequestServiceActivationApprovalResult[factorCode]
	return
}

func TestServiceRack_RequestServiceActivationApproval(t *testing.T) {
	serviceRack := NewServiceRack(0, 2, nil, newDefaultServiceTimingConfigForTest_1())
	var i int32
	for i = 1; i < 4; i++ {
		serviceRack.AddNode(i, newMockNodeMessagerNoop(i), newDefaultNodeMessengingTimingConfigForTest_0())
	}
	for i = 0; i < 0x10; i++ {
		var remoteNodeId int32
		frontNode, forceActivation, localAvailable, isServicing, expectAccept := demux_MockingRequestServiceActivationApprovalFactors(i)
		if frontNode {
			remoteNodeId = 1
		} else {
			remoteNodeId = 3
		}
		serviceRack.availability.Reset()
		if localAvailable {
			serviceRack.availability.AvailableWithin(time.Minute)
		}
		if isServicing {
			serviceRack.servicing.Store(true)
		} else {
			serviceRack.servicing.Store(false)
		}
		accept := serviceRack.RequestServiceActivationApproval(remoteNodeId, forceActivation)
		if expectAccept != accept {
			t.Errorf("unexpected approval on service activation request: remoteNodeId=%v, forceActivation=%v, localAvailable=%v, isServicing=%v, expectAccept=%v; resultApproval=%v",
				remoteNodeId, forceActivation, localAvailable, isServicing, expectAccept, accept)
		}
		if isServicing != serviceRack.servicing.Load() {
			t.Errorf("unmatched precondition: isServicing=%v", isServicing)
		}
		if localAvailable != serviceRack.availability.Availability() {
			t.Errorf("unmatched precondition: localAvailable=%v", localAvailable)
		}
		if frontNode != serviceRack.isFrontNodeId(remoteNodeId) {
			t.Errorf("unmatched precondition: frontNode=%v", frontNode)
		}
	}
}

func TestServiceRack_nodeLoopStartStop(t *testing.T) {
	var err error
	serviceRack := NewServiceRack(0, 2, nil, newDefaultServiceTimingConfigForTest_1())
	nodeMsgTimingCfg := newDefaultNodeMessengingTimingConfigForTest_1()
	nc1 := newMockNodeMessagerClock(1)
	_, err = serviceRack.AddNode(1, nc1, nodeMsgTimingCfg)
	if nil != err {
		t.Fatalf("cannot add node 1 to service rack: %v", err)
	}
	nc2 := newMockNodeMessagerClock(2)
	_, err = serviceRack.AddNode(2, nc2, nodeMsgTimingCfg)
	if nil != err {
		t.Fatalf("cannot add node 2 to service rack: $v", err)
	}
	nc3 := newMockNodeMessagerClock(3)
	_, err = serviceRack.AddNode(3, nc3, nodeMsgTimingCfg)
	if nil != err {
		t.Fatalf("cannot add node 3 to service rack: %v", err)
	}
	tStart := time.Now()
	serviceRack.startNodeLoops()
	if !nc1.lastClose.IsZero() {
		t.Error("initial last close time stamp is not zero (node-1)")
	}
	if !nc3.lastClose.IsZero() {
		t.Error("initial last close time stamp is not zero (node-3)")
	}
	serviceRack.stopNodeLoops()
	tEnd := time.Now()
	if nc1.lastClose.After(tEnd) || nc1.lastClose.Before(tStart) {
		t.Errorf("node 1 clock out of expected range: %v, %v, %v", tStart, nc1.lastClose, tEnd)
	}
	if !nc2.lastClose.IsZero() {
		t.Error("node 2 clock unexpected: %v", nc2.lastClose)
	}
	if nc3.lastClose.After(tEnd) || nc3.lastClose.Before(tStart) {
		t.Errorf("node 3 clock out of expected range: %v, %v, %v", tStart, nc3.lastClose, tEnd)
	}
}

func TestServiceRack_durationToNextSelfChecks(t *testing.T) {
	serviceRack := NewServiceRack(0, 2, nil, newDefaultServiceTimingConfigForTest_3())
	serviceRack.renewLastSelfCheckTimeStamp()
	durOn := serviceRack.durationToNextOnServiceSelfCheck()
	durOff := serviceRack.durationToNextOffServiceSelfCheck()
	if (durOn < time.Second*22) || (durOn > time.Second*23) {
		t.Errorf("duration to next on service check not within expected range (~23 sec): %v", durOn)
	}
	if (durOff < time.Second*32) || (durOff > time.Second*33) {
		t.Errorf("duration to next off service check not within expected range (~33 sec): %v", durOff)
	}
}

func prepare_ServiceRack_NilServCtl_TimingCfg3_6Nodes(t *testing.T, nodeMsgTimingCfg *NodeMessagingTimingConfig) (serviceRack *ServiceRack, nm [6]*mockNodeMessagingAdapter_C1, teardownFunc func()) {
	serviceRack = NewServiceRack(0,5, nil, newDefaultServiceTimingConfigForTest_3())
	for i := 0; i < 6; i++ {
		nodeId := int32(i + 1)
		aux := newMockNodeMessagingAdapter_C1()
		nm[i] = aux
		_, err := serviceRack.AddNode(nodeId, aux, nodeMsgTimingCfg)
		if nil != err {
			t.Fatalf("cannot add node %v to service rack: %v", nodeId, err)
		}
	}
	serviceRack.startNodeLoops()
	go serviceRack.controlRunner.RunLoop()
	teardownFunc = func() {
		serviceRack.stopNodeLoops()
		serviceRack.controlRunner.Close()
	}
	return serviceRack, nm, teardownFunc
}

func TestServiceRack_checkFrontNode(t *testing.T) {
	var err error
	serviceRack, nm, tearDownFunc := prepare_ServiceRack_NilServCtl_TimingCfg3_6Nodes(t, newDefaultNodeMessengingTimingConfigForTest_1())
	defer tearDownFunc()
	// case 1: all node return false
	for i := 0; i < 6; i++ {
		nm[i].resultBool1 = false
		nm[i].setError(false)
	}
	err = serviceRack.checkFrontNode()
	if nil == err {
		t.Errorf("expecting error for checkFrontNode but no error occurs (case-1)")
	}
	// case 2: all node raise exception
	for i := 0; i < 6; i++ {
		nm[i].resultBool1 = false
		nm[i].setError(true)
	}
	err = serviceRack.checkFrontNode()
	if nil == err {
		t.Errorf("expecting error for checkFrontNode but no error occurs (case-2)")
	}
	// case 3: all node return true
	for i := 0; i < 6; i++ {
		nm[i].resultBool1 = true
		nm[i].setError(false)
	}
	err = serviceRack.checkFrontNode()
	if nil != err {
		t.Errorf("expecting no error for checkFrontNode but error occurs (case-3): %v", err)
	}
	// case 4: nodes failed within check range
	for i := 0; i < 6; i++ {
		nm[i].resultBool1 = false
		nm[i].setError(true)
	}
	err = serviceRack.checkFrontNode()
	if nil != err {
		t.Errorf("expecting no error for checkFrontNode but error occurs (case-4): %v", err)
	}
	time.Sleep(2 * time.Second)
	// case 5: all nodes failed again
	for i := 0; i < 6; i++ {
		nm[i].resultBool1 = false
		nm[i].setError(true)
	}
	err = serviceRack.checkFrontNode()
	if nil == err {
		t.Errorf("expecting error for checkFrontNode but no error occurs (case-5)")
	}
	// case 6: one node works
	nm[3].resultBool1 = true
	nm[3].setError(false)
	err = serviceRack.checkFrontNode()
	if nil != err {
		t.Errorf("expecting no error for checkFrontNode but error occurs (case-6): %v", err)
	}
}

func TestServiceRack_concurServiceActivation(t *testing.T) {
	serviceRack, nm, tearDownFunc := prepare_ServiceRack_NilServCtl_TimingCfg3_6Nodes(t, newDefaultNodeMessengingTimingConfigForTest_1())
	defer tearDownFunc()
	t.Run("1-all-reject", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			nm[i].resultBool1 = false
			nm[i].setError(false)
		}
		success := serviceRack.concurServiceActivation(false)
		if success {
			t.Errorf("expecting not concur")
		}
	})
	t.Run("2-all-reject-except-1", func(t *testing.T) {
		nm[3].resultBool1 = true
		success := serviceRack.concurServiceActivation(false)
		if success {
			t.Errorf("expecting not concur")
		}
	})
	t.Run("3-all-accept", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			nm[i].resultBool1 = true
			nm[i].setError(false)
		}
		success := serviceRack.concurServiceActivation(false)
		if !success {
			t.Errorf("expecting concur")
		}
		for i := 0; i < 6; i++ {
			nodeId := i + 1
			if 5 == nodeId {
				continue // skip local node
			}
			if false != nm[i].paramBool1 {
				t.Errorf("expecting reciving force-activation false (node-id: %v): %v", nodeId, nm[i].paramBool1)
			}
			if 5 != nm[i].paramInt32b1 {
				t.Errorf("expecting reciving remote node id 5 (node-id: %v): %v", nodeId, nm[i].paramInt32b1)
			}
		}
	})
	t.Run("4-one-reject", func(t *testing.T) {
		nm[3].resultBool1 = false
		success := serviceRack.concurServiceActivation(false)
		if success {
			t.Errorf("expecting not concur")
		}
	})
	t.Run("5-one-in-exception", func(t *testing.T) {
		nm[3].resultBool1 = false
		nm[3].setError(true)
		success := serviceRack.concurServiceActivation(false)
		if !success {
			t.Errorf("expecting concur")
		}
	})
	t.Run("6-all-exception", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			nm[i].resultBool1 = true
			nm[i].setError(true)
		}
		success := serviceRack.concurServiceActivation(false)
		if !success {
			t.Errorf("expecting concur")
		}
	})
	t.Run("7-reject-w-force", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			nm[i].resultBool1 = false
			nm[i].setError(false)
			nm[i].paramBool1 = false
			nm[i].paramInt32b1 = -1
		}
		success := serviceRack.concurServiceActivation(true)
		if success {
			t.Errorf("expecting not concur")
		}
		for i := 0; i < 6; i++ {
			nodeId := i + 1
			if 5 == nodeId {
				continue
			}
			if 1 == nodeId {
				if true != nm[i].paramBool1 {
					t.Errorf("expecting reciving force-activation true (node-Id: %v): %v", nodeId, nm[i].paramBool1)
				}
				if 5 != nm[i].paramInt32b1 {
					t.Errorf("expecting reciving remote node id 5 (node-Id: %v): %v", nodeId, nm[i].paramInt32b1)
				}
			} else {
				if false != nm[i].paramBool1 {
					t.Errorf("expecting reciving force-activation false (not traversal) (node-Id: %v): %v", nodeId, nm[i].paramBool1)
				}
				if -1 != nm[i].paramInt32b1 {
					t.Errorf("expecting reciving remote node id -1 (not traversal) (node-Id: %v): %v", nodeId, nm[i].paramInt32b1)
				}
			}
		}
	})
}

type mockNodeMessagingAdapter_P1 struct {
	countHasMessagingFailure int32

	tqueueIsOnService                      chan time.Time
	tqueueRequestServiceActivationApproval chan time.Time
}

func newMockNodeMessagingAdapter_P1() (m *mockNodeMessagingAdapter_P1) {
	return &mockNodeMessagingAdapter_P1{
		countHasMessagingFailure:               0,
		tqueueIsOnService:                      make(chan time.Time, 8),
		tqueueRequestServiceActivationApproval: make(chan time.Time, 8),
	}
}

func (m *mockNodeMessagingAdapter_P1) HasMessagingFailure(err error) {
	m.countHasMessagingFailure++
}

func (m *mockNodeMessagingAdapter_P1) IsOnService(ctx context.Context) (onService bool, err error) {
	select {
	case m.tqueueIsOnService <- time.Now():
	default:
	}
	return false, nil
}

func (m *mockNodeMessagingAdapter_P1) expectIsOnService(t *testing.T) {
	select {
	case <-m.tqueueIsOnService:
	case <-time.After(time.Second * 10):
		t.Fatal("blocked at IsOnService")
	}
}

func (m *mockNodeMessagingAdapter_P1) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	select {
	case m.tqueueRequestServiceActivationApproval <- time.Now():
	default:
	}
	return true, nil
}

func (m *mockNodeMessagingAdapter_P1) expectRequestServiceActivationApproval(t *testing.T) {
	select {
	case <-m.tqueueRequestServiceActivationApproval:
	case <-time.After(time.Second * 10):
		t.Fatal("blocked at RequestServiceActivationApproval")
	}
}

func (m *mockNodeMessagingAdapter_P1) Close(ctx context.Context) (err error) {
	return nil
}

const (
	_ = iota
	testeventServiceControlPrepare
	testeventServiceControlOnServiceSelfCheck
	testeventServiceControlOffServiceSelfCheck
	testeventServiceControlActivateService
	testeventServiceControlReleaseService
	testeventServiceControlClose
)

type mockServiceControlAdapter_P1 struct {
	lastPrepareAt             time.Time
	lastOnServiceSelfCheckAt  time.Time
	lastOffServiceSelfCheckAt time.Time
	lastActivateServiceAt     time.Time
	lastReleaseServiceAt      time.Time
	lastCloseAt               time.Time

	eventCh chan int
}

func newMockServiceControlAdapter_P1() (m *mockServiceControlAdapter_P1) {
	return &mockServiceControlAdapter_P1{
		eventCh: make(chan int, 8),
	}
}

func (m *mockServiceControlAdapter_P1) Prepare(ctx context.Context) (err error) {
	m.lastPrepareAt = time.Now()
	m.eventCh <- testeventServiceControlPrepare
	return nil
}

func (m *mockServiceControlAdapter_P1) OnServiceSelfCheck(ctx context.Context) (err error) {
	m.lastOnServiceSelfCheckAt = time.Now()
	m.eventCh <- testeventServiceControlOnServiceSelfCheck
	return nil
}

func (m *mockServiceControlAdapter_P1) OffServiceSelfCheck(ctx context.Context) (err error) {
	m.lastOffServiceSelfCheckAt = time.Now()
	m.eventCh <- testeventServiceControlOffServiceSelfCheck
	return nil
}

func (m *mockServiceControlAdapter_P1) ActivateService(ctx context.Context) (err error) {
	m.lastActivateServiceAt = time.Now()
	m.eventCh <- testeventServiceControlActivateService
	return nil
}

func (m *mockServiceControlAdapter_P1) ReleaseService(ctx context.Context) (err error) {
	m.lastReleaseServiceAt = time.Now()
	m.eventCh <- testeventServiceControlReleaseService
	return nil
}

func (m *mockServiceControlAdapter_P1) Close(ctx context.Context) (err error) {
	m.lastCloseAt = time.Now()
	m.eventCh <- testeventServiceControlClose
	return nil
}

func (m *mockServiceControlAdapter_P1) validate_eventSequence(t *testing.T, eventSeq []int) {
	idx := 0
	for evnt := range m.eventCh {
		if evnt != eventSeq[idx] {
			t.Fatalf("unexpected event id: [index=%v] %v != %v", idx, evnt, eventSeq[idx])
		}
		idx++
		if -9 == eventSeq[idx] {
			break
		}
	}
}

func prepare_ServiceRack_P1ServCtl_TimingCfg3_3Nodes(t *testing.T, nodeMsgTimingCfg *NodeMessagingTimingConfig) (serviceController *mockServiceControlAdapter_P1, serviceRack *ServiceRack, nm [3]*mockNodeMessagingAdapter_P1, teardownFunc func()) {
	serviceController = newMockServiceControlAdapter_P1()
	serviceRack = NewServiceRack(0, 2, serviceController, newDefaultServiceTimingConfigForTest_P1())
	for i := 0; i < 3; i++ {
		nodeId := int32(i + 1)
		aux := newMockNodeMessagingAdapter_P1()
		nm[i] = aux
		_, err := serviceRack.AddNode(nodeId, aux, nodeMsgTimingCfg)
		if nil != err {
			t.Fatalf("cannot add node %v to service rack: %v", nodeId, err)
		}
	}
	go serviceRack.StateTransitionLoop()
	teardownFunc = func() {
		serviceRack.Close()
	}
	return serviceController, serviceRack, nm, teardownFunc
}

func TestServiceRack_StateTransitionLoop_flowNormal(t *testing.T) {
	serviceController, serviceRack, nm, teardownFunc := prepare_ServiceRack_P1ServCtl_TimingCfg3_3Nodes(t, newDefaultNodeMessengingTimingConfigForTest_1())
	defer teardownFunc()
	t.Run("1-prepare", func(t *testing.T) {
		var st = []int{
			testeventServiceControlPrepare,
			testeventServiceControlOffServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
	})
	t.Run("2-check_1-is-on-service", func(t *testing.T) {
		select {
		case <-nm[0].tqueueIsOnService:
		case <-nm[2].tqueueIsOnService:
		case <-time.After(time.Second * 10):
			t.Fatal("blocked at IsOnService stage.")
		}
	})
	t.Run("2-check_reqserv-n1", func(t *testing.T) {
		nm[0].expectRequestServiceActivationApproval(t)
	})
	t.Run("2-check_reqserv-n3", func(t *testing.T) {
		nm[2].expectRequestServiceActivationApproval(t)
	})
	t.Run("3-activate", func(t *testing.T) {
		var st = []int{
			testeventServiceControlActivateService,
			testeventServiceControlOnServiceSelfCheck,
			testeventServiceControlOnServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
	})
	t.Run("4-service", func(t *testing.T) {
		if false == serviceRack.servicing.Load() {
			t.Fatal("expect servicing flag on.")
		}
		if false == serviceRack.IsOnService() {
			t.Fatal("expect servicing check pass.")
		}
	})
	t.Run("5-req-activate", func(t *testing.T) {
		accept := serviceRack.RequestServiceActivationApproval(1, false)
		if true == accept {
			t.Fatalf("expect request rejected: %v", accept)
		}
	})
	t.Run("6-req-activate", func(t *testing.T) {
		accept := serviceRack.RequestServiceActivationApproval(1, true)
		if false == accept {
			t.Fatalf("expect request accept: %v", accept)
		}
		var st = []int{
			testeventServiceControlReleaseService,
			testeventServiceControlOffServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
	})
}

func TestServiceRack_StateTransitionLoop_flowTakeOver(t *testing.T) {
	serviceController, serviceRack, nm, teardownFunc := prepare_ServiceRack_P1ServCtl_TimingCfg3_3Nodes(t, newDefaultNodeMessengingTimingConfigForTest_1())
	defer teardownFunc()
	t.Run("1-prepare", func(t *testing.T) {
		var st = []int{
			testeventServiceControlPrepare,
			testeventServiceControlOffServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
	})
	t.Run("2-check_1-is-on-service", func(t *testing.T) {
		select {
		case <-nm[0].tqueueIsOnService:
		case <-nm[2].tqueueIsOnService:
		case <-time.After(time.Second * 10):
			t.Fatal("blocked at IsOnService stage.")
		}
	})
	t.Run("2-check_reqserv-n1", func(t *testing.T) {
		nm[0].expectRequestServiceActivationApproval(t)
	})
	t.Run("2-check_reqserv-n3", func(t *testing.T) {
		nm[2].expectRequestServiceActivationApproval(t)
	})
	t.Run("3-activate", func(t *testing.T) {
		var st = []int{
			testeventServiceControlActivateService,
			testeventServiceControlOnServiceSelfCheck,
			testeventServiceControlOnServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
	})
	t.Run("4-service", func(t *testing.T) {
		if false == serviceRack.servicing.Load() {
			t.Fatal("expect servicing flag on.")
		}
	})
	t.Run("5-takeover-fail", func(t *testing.T) {
		err := serviceRack.ServiceTakeOver()
		if nil != err {
			t.Fatalf("expect take over success: %v", err)
		}
		var st = []int{
			testeventServiceControlOnServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
		if false == serviceRack.servicing.Load() {
			t.Fatal("expect servicing flag on.")
		}
		if false == serviceRack.IsOnService() {
			t.Fatal("expect servicing check pass.")
		}
	})
	t.Run("6-req-activate", func(t *testing.T) {
		accept := serviceRack.RequestServiceActivationApproval(1, true)
		if false == accept {
			t.Fatalf("expect request accept: %v", accept)
		}
		var st = []int{
			testeventServiceControlReleaseService,
			testeventServiceControlOffServiceSelfCheck,
			-9,
		}
		serviceController.validate_eventSequence(t, st)
	})
	t.Run("7-takeover-ok", func(t *testing.T) {
		err := serviceRack.ServiceTakeOver()
		if nil != err {
			t.Fatalf("expect take over success: %v", err)
		}
		if false == serviceRack.servicing.Load() {
			t.Fatal("expect servicing flag on.")
		}
		if false == serviceRack.IsOnService() {
			t.Fatal("expect servicing check pass.")
		}
	})
}
