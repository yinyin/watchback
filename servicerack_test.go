package watchback

import (
	"testing"
	"context"
	"time"
)

// Mock NodeMessang
type mockNodeMessagerNoop struct {
	nodeId int32
}

func (m * mockNodeMessagerNoop) HasMessagingFailure(err error) {
}

func (m * mockNodeMessagerNoop) IsOnService(ctx context.Context) (onService bool, err error) {
	return false, nil
}

func (m * mockNodeMessagerNoop) RequestServiceActivationApproval(ctx context.Context, forceActivation bool) (isApproved bool, err error) {
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

func TestServiceRack_AddNode_1normal(t *testing.T) {
	serviceRack := newServiceRack(2, nil,
		time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second)
	node1, err1 := serviceRack.AddNode(1, newMockNodeMessagerNoop(1), 0, 0, 0, 0)
	validate_ServiceRack_AddNode_result(t, 1, false, node1, err1)
	node2, err2 := serviceRack.AddNode(2, newMockNodeMessagerNoop(2), 0, 0, 0, 0)
	validate_ServiceRack_AddNode_result(t, -1, false, node2, err2)
	node3, err3 := serviceRack.AddNode(3, newMockNodeMessagerNoop(3), 0, 0, 0, 0)
	validate_ServiceRack_AddNode_result(t, 3, false, node3, err3)
	node3dup, err3dup := serviceRack.AddNode(3, newMockNodeMessagerNoop(3), 0, 0, 0, 0)
	validate_ServiceRack_AddNode_result(t, -1, true, node3dup, err3dup)
	node2dup, err2dup := serviceRack.AddNode(2, newMockNodeMessagerNoop(2), 0, 0, 0, 0)
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
func demux_MockingRequestServiceActivationApprovalFactors(factorCode int32) (remoteNodeId int32, forceActivation, localAvailable, isServicing, expectAccept bool) {
	if 0 != (factorCode & 0x01) {
		remoteNodeId = 1
	} else {
		remoteNodeId = 3
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
	serviceRack := newServiceRack(2, nil,
		time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second, time.Second)
	var i int32
	for i =1; i < 4; i++ {
		serviceRack.AddNode(i, newMockNodeMessagerNoop(i), 0, 0, 0, 0)
	}
	for i=0; i < 0x10; i++ {
		remoteNodeId, forceActivation, localAvailable, isServicing, expectAccept := demux_MockingRequestServiceActivationApprovalFactors(i)
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
	}
}
