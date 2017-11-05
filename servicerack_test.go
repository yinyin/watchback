package watchback

import (
	"testing"
	"context"
	"time"
)

// Mock NodeMessagingAdapter for no operation
type mockNodeMessagerNoop struct {
	nodeId int32
}

func (m * mockNodeMessagerNoop) HasMessagingFailure(err error) {
}

func (m * mockNodeMessagerNoop) IsOnService(ctx context.Context) (onService bool, err error) {
	return false, nil
}

func (m * mockNodeMessagerNoop) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	return true, nil
}

func (m * mockNodeMessagerNoop) Close(ctx context.Context) (err error) {
	return nil
}

func newMockNodeMessagerNoop(nodeId int32) (m * mockNodeMessagerNoop) {
	return &mockNodeMessagerNoop {
		nodeId: nodeId,
	}
}

// Mock NodeMessagingAdapter for no operation
type mockNodeMessagerClock struct {
	nodeId int32

	lastHasMessagingFailureAt time.Time
	lastIsOnService time.Time
	lastRequestServiceActivationApproval time.Time
	lastClose time.Time
}

func (m * mockNodeMessagerClock) HasMessagingFailure(err error) {
	m.lastHasMessagingFailureAt = time.Now()
}

func (m * mockNodeMessagerClock) IsOnService(ctx context.Context) (onService bool, err error) {
	m.lastIsOnService = time.Now()
	return false, nil
}

func (m * mockNodeMessagerClock) RequestServiceActivationApproval(ctx context.Context, requesterNodeId int32, forceActivation bool) (isApproved bool, err error) {
	m.lastRequestServiceActivationApproval = time.Now()
	return true, nil
}

func (m * mockNodeMessagerClock) Close(ctx context.Context) (err error) {
	m.lastClose = time.Now()
	return nil
}

func newMockNodeMessagerClock(nodeId int32) (m * mockNodeMessagerClock) {
	return &mockNodeMessagerClock {
		nodeId: nodeId,
	}
}

func validate_ServiceRack_AddNode_result(t *testing.T, expNodeId int32, expError bool, workerNode * WorkerNode, resultErr error) {
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
		} else if resultMessenger, ok := workerNode.messenger.(* mockNodeMessagerNoop); false == ok {
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

func validate_ServiceRack_nodeCount(t *testing.T, serviceRack * ServiceRack, frontNodeCount int, allNodeCount int) {
	frontLen := len(serviceRack.frontNodes)
	if frontNodeCount != frontLen {
		t.Errorf("expect front node count %v but having %v", frontNodeCount, frontLen)
	}
	allLen := len(serviceRack.allNodes)
	if allNodeCount != allLen {
		t.Errorf("expect all node count %v but having %v", allNodeCount, allLen)
	}
}

func newDefaultServiceTimingConfigForTest_1() (cfg * ServiceTimingConfig) {
	return &ServiceTimingConfig {
		AcceptablePreparePeriod: time.Second,
		AcceptableOnServiceSelfCheckPeriod: time.Second,
		AcceptableOffServiceSelfCheckPeriod: time.Second,
		AcceptableServiceActivationPeriod: time.Second,
		AcceptableServiceReleasingPeriod   : time.Second,

		AcceptableOnServiceSelfCheckFailurePeriod  : time.Second,
		AcceptableOffServiceSelfCheckFailurePeriod : time.Second,
		AcceptableFrontNodeEmptyPeriod             : time.Second,

		OnServiceSelfCheckPeriod  : time.Second,
		OffServiceSelfCheckPeriod : time.Second,

		ServiceActivationFailureBlackoutPeriod : time.Second,
		ServiceReleaseSuccessBlackoutPeriod    : time.Second,
		ServiceReleaseFailureBlackoutPeriod: time.Second,
	}
}

func newDefaultServiceTimingConfigForTest_2() (cfg * ServiceTimingConfig) {
	return &ServiceTimingConfig {
		AcceptablePreparePeriod: time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod: time.Second * 4,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 5,
		AcceptableServiceActivationPeriod: time.Second * 5,
		AcceptableServiceReleasingPeriod   : time.Second * 7,

		AcceptableOnServiceSelfCheckFailurePeriod  : time.Second * 8,
		AcceptableOffServiceSelfCheckFailurePeriod : time.Second * 9,
		AcceptableFrontNodeEmptyPeriod             : time.Second * 10,

		OnServiceSelfCheckPeriod  : time.Second * 11,
		OffServiceSelfCheckPeriod : time.Second * 12,

		ServiceActivationFailureBlackoutPeriod : time.Second * 13,
		ServiceReleaseSuccessBlackoutPeriod    : time.Second * 14,
		ServiceReleaseFailureBlackoutPeriod: time.Second * 15,
	}
}

func newDefaultServiceTimingConfigForTest_3() (cfg * ServiceTimingConfig) {
	return &ServiceTimingConfig {
		AcceptablePreparePeriod: time.Second * 3,
		AcceptableOnServiceSelfCheckPeriod: time.Second * 4,
		AcceptableOffServiceSelfCheckPeriod: time.Second * 5,
		AcceptableServiceActivationPeriod: time.Second * 5,
		AcceptableServiceReleasingPeriod   : time.Second * 7,

		AcceptableOnServiceSelfCheckFailurePeriod  : time.Second * 8,
		AcceptableOffServiceSelfCheckFailurePeriod : time.Second * 9,
		AcceptableFrontNodeEmptyPeriod             : time.Second * 10,

		OnServiceSelfCheckPeriod  : time.Second * 23,
		OffServiceSelfCheckPeriod : time.Second * 33,

		ServiceActivationFailureBlackoutPeriod : time.Second * 13,
		ServiceReleaseSuccessBlackoutPeriod    : time.Second * 14,
		ServiceReleaseFailureBlackoutPeriod: time.Second * 15,
	}
}

func validate_SameServiceTimingConfigContent(t *testing.T, cfg1 * ServiceTimingConfig, cfg2 * ServiceTimingConfig) {
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

	if cfg1.ServiceActivationFailureBlackoutPeriod  != cfg2.ServiceActivationFailureBlackoutPeriod {
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

func TestServiceRack_AddNode_1normal(t *testing.T) {
	serviceRack := newServiceRack(2, nil, newDefaultServiceTimingConfigForTest_1())
	node1, err1 := serviceRack.AddNode(1, newMockNodeMessagerNoop(1), newDefaultNodeMessengingTimingConfigForTest_0())
	validate_ServiceRack_AddNode_result(t, 1, false, node1, err1)
	node2, err2 := serviceRack.AddNode(2, newMockNodeMessagerNoop(2), newDefaultNodeMessengingTimingConfigForTest_0())
	validate_ServiceRack_AddNode_result(t, -1, false, node2, err2)
	node3, err3 := serviceRack.AddNode(3, newMockNodeMessagerNoop(3), newDefaultNodeMessengingTimingConfigForTest_0())
	validate_ServiceRack_AddNode_result(t, 3, false, node3, err3)
	node3dup, err3dup := serviceRack.AddNode(3, newMockNodeMessagerNoop(3), newDefaultNodeMessengingTimingConfigForTest_0())
	validate_ServiceRack_AddNode_result(t, -1, true, node3dup, err3dup)
	node2dup, err2dup := serviceRack.AddNode(2, newMockNodeMessagerNoop(2), newDefaultNodeMessengingTimingConfigForTest_0())
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
		forceActivation =false
	}
	if 0 != (factorCode & 0x04) {
		localAvailable = true
	} else {
		localAvailable =false
	}
	if 0 != (factorCode & 0x08) {
		isServicing = true
	} else {
		isServicing =false
	}
	expectAccept = expect_RequestServiceActivationApprovalResult[factorCode]
	return
}

func TestServiceRack_RequestServiceActivationApproval(t *testing.T) {
	serviceRack := newServiceRack(2, nil,newDefaultServiceTimingConfigForTest_1())
	var i int32
	for i =1; i < 4; i++ {
		serviceRack.AddNode(i, newMockNodeMessagerNoop(i), newDefaultNodeMessengingTimingConfigForTest_0())
	}
	for i=0; i < 0x10; i++ {
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
	serviceRack := newServiceRack(2, nil, newDefaultServiceTimingConfigForTest_1())
	nc1 := newMockNodeMessagerClock(1)
	_, err = serviceRack.AddNode(1, nc1, newDefaultNodeMessengingTimingConfigForTest_1())
	if nil != err {
		t.Errorf("cannot add node 1 to service rack: %v", err)
	}
	nc2 := newMockNodeMessagerClock(2)
	_, err = serviceRack.AddNode(2, nc2, newDefaultNodeMessengingTimingConfigForTest_1())
	if nil != err {
		t.Errorf("cannot add node 2 to service rack: $v", err)
	}
	nc3 := newMockNodeMessagerClock(3)
	_, err = serviceRack.AddNode(3, nc3, newDefaultNodeMessengingTimingConfigForTest_1())
	if nil != err {
		t.Errorf("cannot add node 3 to service rack: %v", err)
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
	serviceRack := newServiceRack(2, nil, newDefaultServiceTimingConfigForTest_3())
	serviceRack.renewLastSelfCheckTimeStamp()
	durOn := serviceRack.durationToNextOnServiceSelfCheck()
	durOff := serviceRack.durationToNextOffServiceSelfCheck()
	if (durOn < time.Second * 22) || (durOn > time.Second * 23) {
		t.Errorf("duration to next on service check not within expected range (~23 sec): %v", durOn)
	}
	if (durOff < time.Second * 32) || (durOff > time.Second * 33) {
		t.Errorf("duration to next off service check not within expected range (~33 sec): %v", durOff)
	}
}
