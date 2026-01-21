package watchback

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

const maxNodeMessagingOperationAttempt = 2

var ErrExceedMaxWorkerNodeMessagingOperationAttempt = errors.New("exceed max node messaging operation attempt")

// Callable content for performing ServiceActivationApproval request
type workerServiceActivationApprovalCallable struct {
	messenger       NodeMessagingAdapter
	localNodeId     int32
	forceActivation bool
	isApproved      bool
}

func (c *workerServiceActivationApprovalCallable) requestServiceActivationApproval(ctx context.Context) (err error) {
	c.isApproved = false
	isApproved, err := c.messenger.RequestServiceActivationApproval(ctx, c.localNodeId, c.forceActivation)
	if nil != err {
		return err
	}
	c.isApproved = isApproved
	return nil
}

type NodeMessagingTimingConfig struct {
	FlexOnServiceCheckPeriod time.Duration // the node will consider on-service within given duration after a positive check

	ExpectOnServiceQueryWithin           time.Duration // time-out duration for on-service query
	ExpectServiceActivationRequestWithin time.Duration // time-out duration for service-activation-request
	ExpectMessengerCloseWithin           time.Duration // time-out duration for closing node messenger
}

func (c *NodeMessagingTimingConfig) copyFrom(other *NodeMessagingTimingConfig) {
	*c = *other
}

const AdviceNodeMessagingTimingFlexOnServiceCheckPeriodTooSmall = "Flex check period for on service check seems too small (< 1ms)"
const AdviceNodeMessagingTimingExpectOnServiceQueryWithinTooSmall = "Expected duration for on service query seems too small (< 1ns)"
const AdviceNodeMessagingTimingExpectServiceActivationRequestWithinTooSmall = "Expected duration for service activation request seems too small (< 1ns)"
const AdviceNodeMessagingTimingExpectMessengerCloseWithinTooSmall = "Expected duration for closing messenger seems too small (< 1ns)"
const AdviceNodeMessagingTimingFlexOnServiceCheckLessThanExpectOnServiceQuery = "Flex check period for on service check less than on service query"
const AdviceNodeMessagingTimingFlexOnServiceCheckLessThanSamllestMessagingFailureDuration = "Flex check period for on service check less than smallest messaging failure duration (< MAX_RETRY * (EXPIRE_COLLECT_PERIOD + EXPECT_ON_SERVICE_QUERY))"

func (c *NodeMessagingTimingConfig) Advice() (advices []string) {
	advices = make([]string, 0)
	if c.FlexOnServiceCheckPeriod < time.Millisecond {
		advices = append(advices, AdviceNodeMessagingTimingFlexOnServiceCheckPeriodTooSmall)
	}
	if c.ExpectOnServiceQueryWithin < time.Nanosecond {
		advices = append(advices, AdviceNodeMessagingTimingExpectOnServiceQueryWithinTooSmall)
	}
	if c.ExpectServiceActivationRequestWithin < time.Nanosecond {
		advices = append(advices, AdviceNodeMessagingTimingExpectServiceActivationRequestWithinTooSmall)
	}
	if c.ExpectMessengerCloseWithin < time.Nanosecond {
		advices = append(advices, AdviceNodeMessagingTimingExpectMessengerCloseWithinTooSmall)
	}
	if c.FlexOnServiceCheckPeriod < c.ExpectOnServiceQueryWithin {
		advices = append(advices, AdviceNodeMessagingTimingFlexOnServiceCheckLessThanExpectOnServiceQuery)
	}
	allErrorSmallestMessagingTime := maxNodeMessagingOperationAttempt * (ExpiredCallableResultCollectPeriod + c.ExpectOnServiceQueryWithin)
	if c.FlexOnServiceCheckPeriod < allErrorSmallestMessagingTime {
		advices = append(advices, AdviceNodeMessagingTimingFlexOnServiceCheckLessThanSamllestMessagingFailureDuration)
	}
	return advices
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
	c, cancel, err := n.messagingRunner.AddCallableWithTimeout(n.messenger.Close, n.messagingTimingConfig.ExpectMessengerCloseWithin)
	if nil != err {
		log.Printf("WARN: cannot invoke Close() of messenger (nodeId=%v): %v", n.nodeId, err)
		return
	}
	defer cancel()
	<-c
	n.messagingRunner.Close()
}

func (n *WorkerNode) RunMessagingLoop(waitGroup *sync.WaitGroup) {
	// defer waitGroup.Done()
	// waitGroup.Add(1)
	// The waitGroup will be done in `n.messagingRunner.RunLoop()`
	n.messagingRunner.RunLoop(waitGroup)
}

func (n *WorkerNode) attemptInvokeMessagingOperation(attempt int, operationName string, callable callableFunc, timeout time.Duration) (err error) {
	c, cancel, err := n.messagingRunner.AddCallableWithTimeout(callable, timeout)
	if nil != err {
		log.Printf("WARN: failed on invoke messaging operation %v (attempt=%v): %v", operationName, attempt, err)
	} else {
		defer cancel()
		err = <-c
		if nil == err {
			return nil
		}
		log.Printf("WARN: result into error on messaging operation %v (attempt=%v): %v", operationName, attempt, err)
		n.messenger.HasMessagingFailure(err)
	}
	return err
}

func (n *WorkerNode) invokeMessagingOperation(operationName string, callable callableFunc, timeout time.Duration) (err error) {
	for attempt := 0; attempt < maxNodeMessagingOperationAttempt; attempt++ {
		if err := n.attemptInvokeMessagingOperation(attempt, operationName, callable, timeout); nil == err {
			return nil
		}
	}
	return ErrExceedMaxWorkerNodeMessagingOperationAttempt
}

func (n *WorkerNode) requestIsOnServiceCheck(ctx context.Context) (err error) {
	onService, err := n.messenger.IsOnService(ctx)
	if nil != err {
		log.Printf("WARN: failed on IsOnService query (nodeId=%v): %v", n.nodeId, err)
		return err
	}
	if onService {
		n.serviceOn.AvailableWithin(n.messagingTimingConfig.FlexOnServiceCheckPeriod)
	} else {
		log.Printf("WARN: peer given off-service response on IsOnService query (nodeId=%v)", n.nodeId)
		select {
		case <-ctx.Done():
		default:
			n.serviceOn.Reset() // reset on context not expire yet
		}
	}
	return nil
}

func (n *WorkerNode) IsOnService() (running bool, err error) {
	err = n.invokeMessagingOperation("IsOnService", n.requestIsOnServiceCheck, n.messagingTimingConfig.ExpectOnServiceQueryWithin)
	return n.serviceOn.Availability(), err
}

func (n *WorkerNode) RequestServiceActivationApproval(localNodeId int32, forceActivation bool) (isApproved bool, err error) {
	callable := workerServiceActivationApprovalCallable{
		messenger:       n.messenger,
		localNodeId:     localNodeId,
		forceActivation: forceActivation,
	}
	err = n.invokeMessagingOperation("RequestServiceActivationApproval", callable.requestServiceActivationApproval, n.messagingTimingConfig.ExpectServiceActivationRequestWithin)
	if nil == err {
		return callable.isApproved, nil
	}
	return false, err
}
