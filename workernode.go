package watchback

import (
	"context"
	"errors"
	"log"
	"time"
)

const maxNodeMessagingOperationAttempt = 2

var ErrExceedMaxWorkerNodeMessagingOperationAttempt = errors.New("exceed max node messaging operation attempt")

// Callable content for performing ServiceActivationApproval request
type workerServiceActivationApprovalCallable struct {
	messenger NodeMessagingAdapter
	localNodeId int32
	forceActivation bool
	isApproved bool
}

func (c * workerServiceActivationApprovalCallable) requestServiceActivationApproval(ctx context.Context) (err error) {
	c.isApproved=false
	isApproved, err := c.messenger.RequestServiceActivationApproval(ctx, c.localNodeId, c.forceActivation)
	if nil != err {
		return err
	}
	c.isApproved = isApproved
	return nil
}

type NodeMessagingTimingConfig struct {
	flexOnServiceCheckPeriod             time.Duration

	expectOnServiceQueryWithin           time.Duration
	expectServiceActivationRequestWithin time.Duration
	expectMessengerCloseWithin           time.Duration
}

func (c * NodeMessagingTimingConfig) copyFrom(other * NodeMessagingTimingConfig) {
	*c = *other
}


type WorkerNode struct {
	nodeId    int32
	messenger NodeMessagingAdapter

	serviceOn       availabilityLogic
	messagingRunner callableBundleRunner

	messagingTimingConfig NodeMessagingTimingConfig
}

func newWorkerNode(nodeId int32, messenger NodeMessagingAdapter, messagingTimingConfig *NodeMessagingTimingConfig) (workerNode *WorkerNode) {
	workerNode = &WorkerNode{
		nodeId:          nodeId,
		messenger:       messenger,
		serviceOn:       newAvailabilityLogic(),
		messagingRunner: newCallableBundleRunner(1),
	}
	workerNode.messagingTimingConfig.copyFrom(messagingTimingConfig)
	return workerNode
}

func (n *WorkerNode) NodeId() (nodeId int32) {
	return n.nodeId
}

func (n *WorkerNode) Close() {
	c, cancel, err := n.messagingRunner.AddCallableWithTimeout(n.messenger.Close, n.messagingTimingConfig.expectMessengerCloseWithin)
	if nil != err {
		log.Printf("WARN: cannot invoke Close() of messenger (nodeId=%v): %v", n.nodeId, err)
	} else {
		defer cancel()
		<-c
		n.messagingRunner.Close()
	}
}

func (n *WorkerNode) RunMessagingLoop() {
	n.messagingRunner.RunLoop()
}

func (n *WorkerNode) invokeMessagingOperation(operationName string, callable callableFunc, timeout time.Duration) (err error) {
	for attempt := 0; attempt < maxNodeMessagingOperationAttempt; attempt++ {
		c, cancel, err := n.messagingRunner.AddCallableWithTimeout(callable, timeout)
		if nil != err {
			log.Printf("WARN: failed on invoke messaging operation %v (attempt=%v): %v", operationName, attempt, err)
		} else {
			defer cancel()
			err = <-c
			if nil != err {
				log.Printf("WARN: result into error on messaging operation %v (attempt=%v): %v", operationName, attempt, err)
				n.messenger.HasMessagingFailure(err)
			} else {
				return nil
			}
		}
	}
	return ErrExceedMaxWorkerNodeMessagingOperationAttempt
}

func (n *WorkerNode) requestIsOnServiceCheck(ctx context.Context) (err error) {
	onService, err := n.messenger.IsOnService(ctx)
	if nil != err {
		return err
	}
	if onService {
		n.serviceOn.AvailableWithin(n.messagingTimingConfig.flexOnServiceCheckPeriod)
	} else {
		select {
		case <-ctx.Done():
		default:
			n.serviceOn.Reset() // reset on context not expire yet
		}
	}
	return nil
}

func (n *WorkerNode) IsOnService() (running bool, err error) {
	err = n.invokeMessagingOperation("IsOnService", n.requestIsOnServiceCheck, n.messagingTimingConfig.expectOnServiceQueryWithin)
	return n.serviceOn.Availability(), err
}

func (n *WorkerNode) RequestServiceActivationApproval(localNodeId int32, forceActivation bool) (isApproved bool, err error) {
	callable := workerServiceActivationApprovalCallable {
		messenger: n.messenger,
		localNodeId: localNodeId,
		forceActivation: forceActivation,
	}
	err = n.invokeMessagingOperation("RequestServiceActivationApproval", callable.requestServiceActivationApproval, n.messagingTimingConfig.expectServiceActivationRequestWithin)
	if nil == err {
		return callable.isApproved, nil
	}
	return false, err
}
